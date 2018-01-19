create or replace package PKG_CIP_INTERFACE_VCP is

  PROCEDURE EXEC_ALL_FIVE_MIN;
  PROCEDURE EXEC_ALL_ONE_DAY;
  PROCEDURE P_IFR_RECORDDETAIL;
  PROCEDURE P_IFR_VCP_TERMCALL_EVENT_D;
  PROCEDURE P_IFR_VCP_AGENT_HALFHOUR;

  PROCEDURE P_IFR_VCP_RPT_CALLIN;
  PROCEDURE P_IFR_VCP_AGENT_ACTION_D;
  PROCEDURE P_IFR_VCP_AGENT_STAT;

  PROCEDURE P_IFR_VCP_AGENT;

  PROCEDURE P_IFS_VCP_BLACKNAME;

  PROCEDURE P_IFR_VCP_GROUP_QUEUE_INFO;
  PROCEDURE P_IFR_VCP_AGENT_GROUP_INFO;

  PROCEDURE P_CS_VCP_AUTO_COUNT_LONGER;

  PROCEDURE P_CS_VCP_IVRCalls_TO_LOSTCalls;

end PKG_CIP_INTERFACE_VCP;
/
create or replace package body PKG_CIP_INTERFACE_VCP is
  PROCEDURE EXEC_ALL_FIVE_MIN AS
  BEGIN
    P_IFR_RECORDDETAIL();
  
    P_IFR_VCP_TERMCALL_EVENT_D();
  --  P_IFR_VCP_AGENT_HALFHOUR();--40分钟
    P_IFS_VCP_BLACKNAME();
  END EXEC_ALL_FIVE_MIN;
  
  PROCEDURE EXEC_ALL_ONE_DAY AS
  BEGIN
 --   P_IFR_VCP_RPT_CALLIN();--40分钟
    P_IFR_VCP_AGENT_ACTION_D();
    P_IFR_VCP_AGENT_STAT();
    P_IFR_VCP_AGENT();
    P_IFR_VCP_GROUP_QUEUE_INFO();--
    P_IFR_VCP_AGENT_GROUP_INFO();--
  END EXEC_ALL_ONE_DAY;
  
  

  /*录音文件接口表 李征 2017/9/5*/
  PROCEDURE P_IFR_RECORDDETAIL AS
    NCOUNT        NUMBER;
    EXPLAINTYPENO VARCHAR2(50);
    V_SQLERRM     VARCHAR2(1000);
  BEGIN
    SELECT COUNT(A.RECORD_ID)
      INTO NCOUNT
      FROM T_IFR_VCP_RECORDDETAIL A
     WHERE A.STATUS IN (0, 1); --'接口处理状态接口处理状态，0待处理；1正在处理；2已处理；3处理失败；默认值为“0”'
  
    IF NCOUNT > 0 THEN
      BEGIN
        -- I := 1; --数组下标初始化
        UPDATE T_IFR_VCP_RECORDDETAIL A
           SET A.STATUS = 1 --1：正在传输
         WHERE A.STATUS = 0; --0:待处理
        COMMIT; --提交后方便出现异常后和业务表比对异常数据,比较业务表更新为2状态但是接口表因为回滚而不存在的数据就是异常数据
        --必须每条的传输，并且某条出现异常则记录日志然后进行一下条数据的传输。
        FOR J1 IN (SELECT *
                     FROM T_IFR_VCP_RECORDDETAIL A
                    WHERE A.STATUS = 1 --1：正在传输
                    ORDER BY A.CREATED_DATE ASC) LOOP
          BEGIN
            EXPLAINTYPENO := J1.ID; --
          
            MERGE INTO T_CS_VCP_RECORDDETAIL B
            USING (SELECT 1 FROM DUAL) I --不再重复透过DBLINK从接口表
            ON (B.RECORD_ID = J1.RECORD_ID) --操作业务表所以以业务关键字来做匹配条件
            WHEN MATCHED THEN
            -- 按主键找，找到了就更新
              UPDATE
                 SET B.REC_FILE          = J1.REC_FILE,
                     B.WEB_FILE_NAME     = J1.WEB_FILE_NAME,
                     B.AGENTNAME         = J1.AGENTNAME,
                     B.LONGER            = J1.LONGER,
                     B.CUSTOMER_NUM      = J1.CUSTOMER_NUM,
                     B.DIRECTION         = J1.DIRECTION,
                     B.LINE_TYPE         = J1.LINE_TYPE,
                     B.CREATED_DATE      = J1.CREATED_DATE,
                     B.LAST_UPDATED_DATE = SYSTIMESTAMP
              
               WHERE B.RECORD_ID = J1.RECORD_ID
              
            
            WHEN NOT MATCHED THEN
            --插入
              INSERT --INTO T_CS_VCP_RECORDDETAIL B
                (B.RECORD_ID,
                 B.REC_FILE,
                 B.WEB_FILE_NAME,
                 B.AGENTNAME,
                 B.LONGER,
                 B.CUSTOMER_NUM,
                 B.DIRECTION,
                 B.LINE_TYPE ,
                 B.CREATED_DATE)
              
              VALUES
                (J1.RECORD_ID,
                 J1.REC_FILE,
                 J1.WEB_FILE_NAME,
                 J1.AGENTNAME,
                 J1.LONGER,
                 J1.CUSTOMER_NUM,
                 J1.DIRECTION,
                 J1.LINE_TYPE,
                 J1.CREATED_DATE);
          
            UPDATE T_IFR_VCP_RECORDDETAIL A
               SET A.STATUS            = 2, --2：传输成功
                   A.LAST_UPDATED_DATE = SYSDATE
             WHERE A.ID = J1.ID
               AND A.STATUS = 1; -- 1 处理中
          
            COMMIT; --操作完整一条数据（包括更新或者插入成功然后并把传输状态改为传输成功状态才算完整操作一条记录）就commit，一旦出现异常则只回滚那一条异常的数据
            --COURSES(I) := J1.ID; --将接口表ID--INTF_ID加入数组
            -- I := I + 1; --数组下标+1
          EXCEPTION
            WHEN OTHERS THEN
              ROLLBACK; --回滚出现异常的某条记录，因为操作完整的一条数据包括2步，所以必须回滚，保证异常了不出出现在业务表中
              V_SQLERRM := SUBSTR(SQLERRM, 0, 1000);
              UPDATE T_IFR_VCP_RECORDDETAIL A
                 SET A.STATUS      = 3, -- 3 异常
                     A.SEND_REMARK = V_SQLERRM
               WHERE A.ID = EXPLAINTYPENO
                 AND A.STATUS = 1;
              COMMIT; --异常数据的操作，下面是写错误日志，所以分别作自己的事务提交
              INSERT INTO t_cs_db_log
                (LOG_ID,
                 log_name,
                 log_code,
                 log_desc,
                 log_flag,
                 creator,
                 beg_time,
                 end_time)
              VALUES
                (seq_cs_db_log.nextval,
                 '录音文件表_VCP',
                 'P_IFR_RECORDDETAIL',
                 V_SQLERRM,
                 '0',
                 'SYS',
                 SYSDATE,
                 SYSDATE);
              commit;
              --      PKG_CIP_COMMON.ADDOPERATIONLOG('P_IFR_RECORDDETAIL', 'VCP录音文件列表接口', V_SQLERRM, '0', '');
            
              COMMIT;
          END;
        END LOOP; --异常处理必须放在循环里面，保证异常之后不退出循环直接进行下步的处理，符合业务要求的逻辑
      
      END;
    END IF;
  
  END P_IFR_RECORDDETAIL;

  --通话记录明细表接口 李征 2017/9/5
  PROCEDURE P_IFR_VCP_TERMCALL_EVENT_D AS
  
    NCOUNT        NUMBER;
    EXPLAINTYPENO VARCHAR2(50);
    V_SQLERRM     VARCHAR2(1000);
  BEGIN
    SELECT COUNT(A.TABRECORD_ID)
      INTO NCOUNT
      FROM T_IFR_VCP_TERMCALL_DETAIL A
     WHERE A.STATUS IN (0, 1); --'接口处理状态接口处理状态，0待处理；1正在处理；2已处理；3处理失败；默认值为“0”'
  
    IF NCOUNT > 0 THEN
      BEGIN
        -- I := 1; --数组下标初始化
        UPDATE T_IFR_VCP_TERMCALL_DETAIL A
           SET A.STATUS = 1 --1：正在传输
         WHERE A.STATUS = 0; --0:待处理
        COMMIT; --提交后方便出现异常后和业务表比对异常数据,比较业务表更新为2状态但是接口表因为回滚而不存在的数据就是异常数据
        --必须每条的传输，并且某条出现异常则记录日志然后进行一下条数据的传输。
        FOR J1 IN (SELECT *
                     FROM T_IFR_VCP_TERMCALL_DETAIL A
                    WHERE A.STATUS = 1 --1：正在传输
                    ORDER BY A.CREATED_DATE ASC) LOOP
          BEGIN
            EXPLAINTYPENO := J1.ID;
          
            MERGE INTO T_CS_VCP_TERMCALL_EVENT_DETAIL B
            
            USING (SELECT 1 FROM DUAL) I --不再重复透过DBLINK从接口表
            ON (B.TABRECORD_ID = J1.TABRECORD_ID) --操作业务表所以以业务关键字来做匹配条件
            
            WHEN MATCHED THEN
            -- 按主键找，找到了就更新
          --  case when to_char(J1.ANSWERENDTIME,'yyyy')!='1970' then J1.ANSWERENDTIME end
            
              UPDATE
                 SET B.TERMINATIONCALL_ID        = J1.TERMINATIONCALL_ID,
                     B.DIRECTION                 = J1.DIRECTION,
                     B.CALLTYPE                  = J1.CALLTYPE,
                     B.DNIS                      = J1.DNIS,
                     B.ANI                       = J1.ANI,
                     B.CALLSTARTTIME             = case when to_char(J1.CALLSTARTTIME,'yyyy')!='1970' then J1.CALLSTARTTIME end,
                     B.ACDSTARTTIME              = case when to_char(J1.ACDSTARTTIME,'yyyy')!='1970' then J1.ACDSTARTTIME end,
                     B.RINGSTARTTIME             = case when to_char(J1.RINGSTARTTIME,'yyyy')!='1970' then J1.RINGSTARTTIME end,
                     B.ANSWERSTARTTIME           = case when to_char(J1.ANSWERSTARTTIME,'yyyy')!='1970' then J1.ANSWERSTARTTIME end,
                     B.ANSWERENDTIME             = case when to_char(J1.ANSWERENDTIME,'yyyy')!='1970' then J1.ANSWERENDTIME end,
                     B.DURATIONOFIVR             = J1.DURATIONOFIVR,
                     B.DURATIONOFHOLDING         = J1.DURATIONOFHOLDING,
                     B.DURATIONOFWORKREADY       = J1.DURATIONOFWORKREADY,
                     B.AGENTNAME                 = J1.AGENTNAME,
                     B.AFTERCALLDEALFINISHEDTIME = J1.AFTERCALLDEALFINISHEDTIME,
                     --      B.ACDNAME                   = J1.ACDNAME,
                     B.CALLDURATIONSECONDS = J1.CALLDURATIONSECONDS,
                     B.LINEDURATIONSECONDS = J1.LINEDURATIONSECONDS,
                     --     B.ACDTRANSNAME              = J1.ACDTRANSNAME,
                     B.ACDTIMELONG  = J1.ACDTIMELONG,
                     B.RINGTIMELONG = J1.RINGTIMELONG,
                     B.OUTNUM       = J1.OUTNUM,
                     B.CREATOR      = 'SYS',
                     B.MODIFIER     = 'SYS',
                     --变更后，修改
                     B.WRK_GROUP_CODE   = J1.WRK_GROUP_CODE,
                     B.TRANS_GROUP_CODE = J1.TRANS_GROUP_CODE,
                     B.LINE_TYPE        = J1.LINE_TYPE,
                     B.QUEUE_NO         = J1.QUEUE_NO,
                     
                     B.LAST_UPDATED_DATE = SYSTIMESTAMP
              
               WHERE B.TABRECORD_ID = J1.TABRECORD_ID
              
            
            WHEN NOT MATCHED THEN
            --插入
              INSERT --INTO T_CS_VCP_TERMCALL_EVENT_DETAIL B
                (B.TABRECORD_ID,
                 B.TERMINATIONCALL_ID,
                 B.DIRECTION,
                 B.CALLTYPE,
                 B.DNIS,
                 B.ANI,
                 B.CALLSTARTTIME,
                 B.ACDSTARTTIME,
                 B.RINGSTARTTIME,
                 B.ANSWERSTARTTIME,
                 B.ANSWERENDTIME,
                 B.DURATIONOFIVR,
                 B.DURATIONOFHOLDING,
                 B.DURATIONOFWORKREADY,
                 B.AGENTNAME,
                 B.AFTERCALLDEALFINISHEDTIME,
                 
                 B.CALLDURATIONSECONDS,
                 B.LINEDURATIONSECONDS,
                 
                 B.ACDTIMELONG,
                 B.RINGTIMELONG,
                 B.OUTNUM,
                 B.CREATOR,
                 B.MODIFIER,
                 --变更后，修改
                 B.WRK_GROUP_CODE,
                 B.TRANS_GROUP_CODE,
                 B.LINE_TYPE,
                 B.QUEUE_NO
                 
                 )
              
              VALUES
                (J1.TABRECORD_ID,
                 J1.TERMINATIONCALL_ID,
                 J1.DIRECTION,
                 J1.CALLTYPE,
                 J1.DNIS,
                 J1.ANI,
                 case when to_char(J1.CALLSTARTTIME,'yyyy')!='1970' then J1.CALLSTARTTIME end,
                 case when to_char(J1.ACDSTARTTIME,'yyyy')!='1970' then J1.ACDSTARTTIME end,
                 case when to_char(J1.RINGSTARTTIME,'yyyy')!='1970' then J1.RINGSTARTTIME end,
                 case when to_char(J1.ANSWERSTARTTIME,'yyyy')!='1970' then J1.ANSWERSTARTTIME end,
                 case when to_char(J1.ANSWERENDTIME,'yyyy')!='1970' then J1.ANSWERENDTIME end,
                 J1.DURATIONOFIVR,
                 J1.DURATIONOFHOLDING,
                 J1.DURATIONOFWORKREADY,
                 J1.AGENTNAME,
                 J1.AFTERCALLDEALFINISHEDTIME,
                 
                 J1.CALLDURATIONSECONDS,
                 J1.LINEDURATIONSECONDS,
                 
                 J1.ACDTIMELONG,
                 J1.RINGTIMELONG,
                 J1.OUTNUM,
                 'SYS',
                 'SYS',
                 --变更后，修改
                 J1.WRK_GROUP_CODE,
                 J1.TRANS_GROUP_CODE,
                 J1.LINE_TYPE,
                 J1.QUEUE_NO
                 
                 );
          
            UPDATE T_IFR_VCP_TERMCALL_DETAIL A
               SET A.STATUS            = 2, --2：传输成功
                   A.LAST_UPDATED_DATE = SYSDATE
             WHERE A.ID = J1.ID
               AND A.STATUS = 1; -- 1 处理中
          
            COMMIT; --操作完整一条数据（包括更新或者插入成功然后并把传输状态改为传输成功状态才算完整操作一条记录）就commit，一旦出现异常则只回滚那一条异常的数据
            --COURSES(I) := J1.ID; --将接口表ID--INTF_ID加入数组
            -- I := I + 1; --数组下标+1
          EXCEPTION
            WHEN OTHERS THEN
              ROLLBACK; --回滚出现异常的某条记录，因为操作完整的一条数据包括2步，所以必须回滚，保证异常了不出出现在业务表中
              V_SQLERRM := SUBSTR(SQLERRM, 0, 1000);
              UPDATE T_IFR_VCP_TERMCALL_DETAIL A
                 SET A.STATUS      = 3, -- 3 异常
                     A.SEND_REMARK = V_SQLERRM
               WHERE A.ID = EXPLAINTYPENO
                 AND A.STATUS = 1;
              COMMIT; --异常数据的操作，下面是写错误日志，所以分别作自己的事务提交
              INSERT INTO t_cs_db_log
                (LOG_ID,
                 log_name,
                 log_code,
                 log_desc,
                 log_flag,
                 creator,
                 beg_time,
                 end_time)
              VALUES
                (seq_cs_db_log.nextval,
                 '通话记录明细表_VCP',
                 'P_IFR_VCP_TERMCALL_EVENT_D',
                 V_SQLERRM,
                 '0',
                 'SYS',
                 SYSDATE,
                 SYSDATE);
              commit;
              --       PKG_CIP_COMMON.ADDOPERATIONLOG('P_IFR_VCP_TERMCALL_EVENT_D','通话记录明细表接口',V_SQLERRM,'0',                                               '');
            
              COMMIT;
          END;
        END LOOP; --异常处理必须放在循环里面，保证异常之后不退出循环直接进行下步的处理，符合业务要求的逻辑
      
      END;
    END IF;
  
  END P_IFR_VCP_TERMCALL_EVENT_D;

  /*坐席定时统计 李征 2017/9/6*/
  PROCEDURE P_IFR_VCP_AGENT_HALFHOUR AS
    NCOUNT        NUMBER;
    EXPLAINTYPENO VARCHAR2(50);
    V_SQLERRM     VARCHAR2(1000);
  BEGIN
    SELECT COUNT(A.TABRECORD_ID)
      INTO NCOUNT
      FROM T_IFR_VCP_AGENT_QUEUE_STATS A
     WHERE A.STATUS IN (0, 1); --'接口处理状态接口处理状态，0待处理；1正在处理；2已处理；3处理失败；默认值为“0”'
  
    IF NCOUNT > 0 THEN
      BEGIN
        -- I := 1; --数组下标初始化
        UPDATE T_IFR_VCP_AGENT_QUEUE_STATS A
           SET A.STATUS = 1 --1：正在传输
         WHERE A.STATUS = 0; --0:待处理
        COMMIT; --提交后方便出现异常后和业务表比对异常数据,比较业务表更新为2状态但是接口表因为回滚而不存在的数据就是异常数据
        --必须每条的传输，并且某条出现异常则记录日志然后进行一下条数据的传输。
        FOR J1 IN (SELECT *
                     FROM T_IFR_VCP_AGENT_QUEUE_STATS A
                    WHERE A.STATUS = 1 --1：正在传输
                    ORDER BY A.CREATED_DATE ASC) LOOP
          BEGIN
            EXPLAINTYPENO := J1.ID; --
          
            MERGE INTO T_CS_VCP_AGENT_QUEUE_STATS B
            USING (SELECT 1 FROM DUAL) I --不再重复透过DBLINK从接口表
            ON (B.TABRECORD_ID = J1.TABRECORD_ID) --操作业务表所以以业务关键字来做匹配条件
            WHEN MATCHED THEN
            -- 按主键找，找到了就更新
              UPDATE
                 SET B.CNAME             = J1.CNAME,
                     B.DINTERVALSTART    = J1.DINTERVALSTART,
                     B.NANSWEREDACD      = J1.NANSWEREDACD,
                     B.CREPORTGROUP      = J1.CREPORTGROUP,
                     B.LAST_UPDATED_DATE = SYSTIMESTAMP
               WHERE B.TABRECORD_ID = J1.TABRECORD_ID
              
            
            WHEN NOT MATCHED THEN
            --插入
              INSERT --INTO T_CS_VCP_AGENT_QUEUE_STATS B
                (B.TABRECORD_ID,
                 B.CNAME,
                 B.DINTERVALSTART,
                 B.NANSWEREDACD,
                 B.CREPORTGROUP)
              
              VALUES
                (J1.TABRECORD_ID,
                 J1.CNAME,
                 J1.DINTERVALSTART,
                 J1.NANSWEREDACD,
                 J1.CREPORTGROUP);
          
            UPDATE T_IFR_VCP_AGENT_QUEUE_STATS A
               SET A.STATUS            = 2, --2：传输成功
                   A.LAST_UPDATED_DATE = SYSDATE
             WHERE A.ID = J1.ID
               AND A.STATUS = 1; -- 1 处理中
          
            COMMIT; --操作完整一条数据（包括更新或者插入成功然后并把传输状态改为传输成功状态才算完整操作一条记录）就commit，一旦出现异常则只回滚那一条异常的数据
            --COURSES(I) := J1.ID; --将接口表ID--INTF_ID加入数组
            -- I := I + 1; --数组下标+1
          EXCEPTION
            WHEN OTHERS THEN
              ROLLBACK; --回滚出现异常的某条记录，因为操作完整的一条数据包括2步，所以必须回滚，保证异常了不出出现在业务表中
              V_SQLERRM := SUBSTR(SQLERRM, 0, 1000);
              UPDATE T_IFR_VCP_AGENT_QUEUE_STATS A
                 SET A.STATUS      = 3, -- 3 异常
                     A.SEND_REMARK = V_SQLERRM
               WHERE A.ID = EXPLAINTYPENO
                 AND A.STATUS = 1;
              COMMIT; --异常数据的操作，下面是写错误日志，所以分别作自己的事务提交
              INSERT INTO t_cs_db_log
                (LOG_ID,
                 log_name,
                 log_code,
                 log_desc,
                 log_flag,
                 creator,
                 beg_time,
                 end_time)
              VALUES
                (seq_cs_db_log.nextval,
                 ' 座席定时统计接口表_VCP',
                 'P_IFR_VCP_AGENT_HALFHOUR',
                 V_SQLERRM,
                 '0',
                 'SYS',
                 SYSDATE,
                 SYSDATE);
              commit;
              --      PKG_CIP_COMMON.ADDOPERATIONLOG('P_IFR_VCP_AGENT_HALFHOUR', 'VCP录音文件列表接口', V_SQLERRM, '0', '');
            
              COMMIT;
          END;
        END LOOP; --异常处理必须放在循环里面，保证异常之后不退出循环直接进行下步的处理，符合业务要求的逻辑
      
      END;
    END IF;
  
  END P_IFR_VCP_AGENT_HALFHOUR;

  /*呼入统计接口   李征 2017/9/6*/
  PROCEDURE P_IFR_VCP_RPT_CALLIN AS
    NCOUNT        NUMBER;
    EXPLAINTYPENO VARCHAR2(50);
    V_SQLERRM     VARCHAR2(1000);
  BEGIN
    SELECT COUNT(A.TABRECORD_ID)
      INTO NCOUNT
      FROM T_IFR_VCP_RPT_CALL_IN A
     WHERE A.STATUS IN (0, 1); --'接口处理状态接口处理状态，0待处理；1正在处理；2已处理；3处理失败；默认值为“0”'
  
    IF NCOUNT > 0 THEN
      BEGIN
        -- I := 1; --数组下标初始化
        UPDATE T_IFR_VCP_RPT_CALL_IN A
           SET A.STATUS = 1 --1：正在传输
         WHERE A.STATUS = 0; --0:待处理
        COMMIT; --提交后方便出现异常后和业务表比对异常数据,比较业务表更新为2状态但是接口表因为回滚而不存在的数据就是异常数据
        --必须每条的传输，并且某条出现异常则记录日志然后进行一下条数据的传输。
        FOR J1 IN (SELECT *
                     FROM T_IFR_VCP_RPT_CALL_IN A
                    WHERE A.STATUS = 1 --1：正在传输
                    ORDER BY A.CREATED_DATE ASC) LOOP
          BEGIN
            EXPLAINTYPENO := J1.ID; --
          
            MERGE INTO T_CS_VCP_RPT_CALL_IN B
            USING (SELECT 1 FROM DUAL) I --不再重复透过DBLINK从接口表
            ON (B.TABRECORD_ID = J1.TABRECORD_ID) --操作业务表所以以业务关键字来做匹配条件
            WHEN MATCHED THEN
            -- 按主键找，找到了就更新
              UPDATE
                 SET B.DINTERVAL_START         = J1.DINTERVAL_START,
                     B.CALL_IN                 = J1.CALL_IN,
                     B.CUSTOMER_ABANDON        = J1.CUSTOMER_ABANDON,
                     B.ANSWERED_IVR            = J1.ANSWERED_IVR,
                     B.ENTERED_ACD             = J1.ENTERED_ACD,
                     B.ANSWERED_ACD            = J1.ANSWERED_ACD,
                     B.HUANGUP_ACD             = J1.HUANGUP_ACD,
                     B.NOT_ANSWERED            = J1.NOT_ANSWERED,
                     B.ANSWERED_RATE           = J1.ANSWERED_RATE,
                     B.LOST_RATE               = J1.LOST_RATE,
                     B.ANSWER_TIMELONG_AGV     = J1.ANSWER_TIMELONG_AGV,
                     B.WAIT_TIMELONG_AGV       = J1.WAIT_TIMELONG_AGV,
                     B.AFTER_CALL_TIMELONG_AGV = J1.AFTER_CALL_TIMELONG_AGV,
                     B.ANSWERED_IN_10S         = J1.ANSWERED_IN_10S,
                     B.ANSWERED_IN_20S         = J1.ANSWERED_IN_20S,
                     B.ANSWERED_OUT_20S        = J1.ANSWERED_OUT_20S,
                     B.SERVICE_LEVEL_10S       = J1.SERVICE_LEVEL_10S,
                     B.SERVICE_LEVEL_20S       = J1.SERVICE_LEVEL_20S,
                     B.WRK_GROUP               = J1.WRK_GROUP,
                     B.QUEUE_NO                = J1.QUEUE_NO,
                     B.LAST_UPDATED_DATE       = SYSTIMESTAMP,
                     --变更后，
                     B.LINE_TYPE = J1.LINE_TYPE
              
               WHERE B.TABRECORD_ID = J1.TABRECORD_ID
              
            
            WHEN NOT MATCHED THEN
            --插入
              INSERT --INTO  T_CS_VCP_RPT_CALL_IN B
                (B.TABRECORD_ID,
                 B.DINTERVAL_START,
                 B.CALL_IN,
                 B.CUSTOMER_ABANDON,
                 B.ANSWERED_IVR,
                 B.ENTERED_ACD,
                 B.ANSWERED_ACD,
                 B.HUANGUP_ACD,
                 B.NOT_ANSWERED,
                 B.ANSWERED_RATE,
                 B.LOST_RATE,
                 B.ANSWER_TIMELONG_AGV,
                 B.WAIT_TIMELONG_AGV,
                 B.AFTER_CALL_TIMELONG_AGV,
                 B.ANSWERED_IN_10S,
                 B.ANSWERED_IN_20S,
                 B.ANSWERED_OUT_20S,
                 B.SERVICE_LEVEL_10S,
                 B.SERVICE_LEVEL_20S,
                 B.WRK_GROUP,
                 B.QUEUE_NO,
                 --变更后
                 B.LINE_TYPE)
              
              VALUES
                (J1.TABRECORD_ID,
                 J1.DINTERVAL_START,
                 J1.CALL_IN,
                 J1.CUSTOMER_ABANDON,
                 J1.ANSWERED_IVR,
                 J1.ENTERED_ACD,
                 J1.ANSWERED_ACD,
                 J1.HUANGUP_ACD,
                 J1.NOT_ANSWERED,
                 J1.ANSWERED_RATE,
                 J1.LOST_RATE,
                 J1.ANSWER_TIMELONG_AGV,
                 J1.WAIT_TIMELONG_AGV,
                 J1.AFTER_CALL_TIMELONG_AGV,
                 J1.ANSWERED_IN_10S,
                 J1.ANSWERED_IN_20S,
                 J1.ANSWERED_OUT_20S,
                 J1.SERVICE_LEVEL_10S,
                 J1.SERVICE_LEVEL_20S,
                 J1.WRK_GROUP,
                 J1.QUEUE_NO,
                 --变更后
                 J1.LINE_TYPE);
          
            UPDATE T_IFR_VCP_RPT_CALL_IN A
               SET A.STATUS            = 2, --2：传输成功
                   A.LAST_UPDATED_DATE = SYSDATE
             WHERE A.ID = J1.ID
               AND A.STATUS = 1; -- 1 处理中
          
            COMMIT; --操作完整一条数据（包括更新或者插入成功然后并把传输状态改为传输成功状态才算完整操作一条记录）就commit，一旦出现异常则只回滚那一条异常的数据
            --COURSES(I) := J1.ID; --将接口表ID--INTF_ID加入数组
            -- I := I + 1; --数组下标+1
          EXCEPTION
            WHEN OTHERS THEN
              ROLLBACK; --回滚出现异常的某条记录，因为操作完整的一条数据包括2步，所以必须回滚，保证异常了不出出现在业务表中
              V_SQLERRM := SUBSTR(SQLERRM, 0, 1000);
              UPDATE T_IFR_VCP_RPT_CALL_IN A
                 SET A.STATUS      = 3, -- 3 异常
                     A.SEND_REMARK = V_SQLERRM
               WHERE A.ID = EXPLAINTYPENO
                 AND A.STATUS = 1;
              COMMIT; --异常数据的操作，下面是写错误日志，所以分别作自己的事务提交
              INSERT INTO t_cs_db_log
                (LOG_ID,
                 log_name,
                 log_code,
                 log_desc,
                 log_flag,
                 creator,
                 beg_time,
                 end_time)
              VALUES
                (seq_cs_db_log.nextval,
                 '呼入统计接口报表_VCP',
                 'P_IFR_VCP_RPT_CALLIN',
                 V_SQLERRM,
                 '0',
                 'SYS',
                 SYSDATE,
                 SYSDATE);
              commit;
              --      PKG_CIP_COMMON.ADDOPERATIONLOG('P_IFR_VCP_RPT_CALLIN', 'VCP录音文件列表接口', V_SQLERRM, '0', '');
            
              COMMIT;
          END;
        END LOOP; --异常处理必须放在循环里面，保证异常之后不退出循环直接进行下步的处理，符合业务要求的逻辑
      
      END;
    END IF;
  
  END P_IFR_VCP_RPT_CALLIN;
  /*坐席状态明细接口   李征 2017/9/6*/
  PROCEDURE P_IFR_VCP_AGENT_ACTION_D AS
    NCOUNT        NUMBER;
    EXPLAINTYPENO VARCHAR2(50);
    V_SQLERRM     VARCHAR2(1000);
  BEGIN
    SELECT COUNT(A.AGENTEVENT_ID)
      INTO NCOUNT
      FROM T_IFR_VCP_AGENT_ACTION_COUNT A
     WHERE A.STATUS IN (0, 1); --'接口处理状态接口处理状态，0待处理；1正在处理；2已处理；3处理失败；默认值为“0”'
  
    IF NCOUNT > 0 THEN
      BEGIN
        -- I := 1; --数组下标初始化
        UPDATE T_IFR_VCP_AGENT_ACTION_COUNT A
           SET A.STATUS = 1 --1：正在传输
         WHERE A.STATUS = 0; --0:待处理
        COMMIT; --提交后方便出现异常后和业务表比对异常数据,比较业务表更新为2状态但是接口表因为回滚而不存在的数据就是异常数据
        --必须每条的传输，并且某条出现异常则记录日志然后进行一下条数据的传输。
        FOR J1 IN (SELECT *
                     FROM T_IFR_VCP_AGENT_ACTION_COUNT A
                    WHERE A.STATUS = 1 --1：正在传输
                    ORDER BY A.CREATED_DATE ASC) LOOP
          BEGIN
            EXPLAINTYPENO := J1.ID; --
          
            MERGE INTO T_CS_VCP_AGENT_ACTION_COUNT B
            USING (SELECT 1 FROM DUAL) I --不再重复透过DBLINK从接口表
            ON (B.AGENTEVENT_ID = J1.AGENTEVENT_ID) --操作业务表所以以业务关键字来做匹配条件
            WHEN MATCHED THEN
            -- 按主键找，找到了就更新
              UPDATE
                 SET B.AGENT_NAME        = J1.AGENT_NAME,
                     B.EVENT_TYPE        = J1.EVENT_TYPE,
                     B.EVENT_TIME_START  = J1.EVENT_TIME_START,
                     B.EVENT_TIME_END    = J1.EVENT_TIME_END,
                     B.TIME_LONG         = J1.TIME_LONG,
                     B.LAST_UPDATED_DATE = SYSTIMESTAMP
              
               WHERE B.AGENTEVENT_ID = J1.AGENTEVENT_ID
              
            
            WHEN NOT MATCHED THEN
            --插入
              INSERT --INTO  T_CS_VCP_AGENT_ACTION_COUNT B
                (B.AGENTEVENT_ID,
                 B.AGENT_NAME,
                 B.EVENT_TYPE,
                 B.EVENT_TIME_START,
                 B.EVENT_TIME_END,
                 B.TIME_LONG
                 
                 )
              
              VALUES
                (J1.AGENTEVENT_ID,
                 J1.AGENT_NAME,
                 J1.EVENT_TYPE,
                 J1.EVENT_TIME_START,
                 J1.EVENT_TIME_END,
                 J1.TIME_LONG
                 
                 );
          
            UPDATE T_IFR_VCP_AGENT_ACTION_COUNT A
               SET A.STATUS            = 2, --2：传输成功
                   A.LAST_UPDATED_DATE = SYSDATE
             WHERE A.ID = J1.ID
               AND A.STATUS = 1; -- 1 处理中
          
            COMMIT; --操作完整一条数据（包括更新或者插入成功然后并把传输状态改为传输成功状态才算完整操作一条记录）就commit，一旦出现异常则只回滚那一条异常的数据
            --COURSES(I) := J1.ID; --将接口表ID--INTF_ID加入数组
            -- I := I + 1; --数组下标+1
          EXCEPTION
            WHEN OTHERS THEN
              ROLLBACK; --回滚出现异常的某条记录，因为操作完整的一条数据包括2步，所以必须回滚，保证异常了不出出现在业务表中
              V_SQLERRM := SUBSTR(SQLERRM, 0, 1000);
              UPDATE T_IFR_VCP_AGENT_ACTION_COUNT A
                 SET A.STATUS      = 3, -- 3 异常
                     A.SEND_REMARK = V_SQLERRM
               WHERE A.ID = EXPLAINTYPENO
                 AND A.STATUS = 1;
              COMMIT; --异常数据的操作，下面是写错误日志，所以分别作自己的事务提交
              INSERT INTO t_cs_db_log
                (LOG_ID,
                 log_name,
                 log_code,
                 log_desc,
                 log_flag,
                 creator,
                 beg_time,
                 end_time)
              VALUES
                (seq_cs_db_log.nextval,
                 '坐席状态明细接口报表_VCP',
                 'P_IFR_VCP_AGENT_ACTION_D',
                 V_SQLERRM,
                 '0',
                 'SYS',
                 SYSDATE,
                 SYSDATE);
              commit;
              --      PKG_CIP_COMMON.ADDOPERATIONLOG('P_IFR_VCP_AGENT_ACTION_D', 'VCP录音文件列表接口', V_SQLERRM, '0', '');
            
              COMMIT;
          END;
        END LOOP; --异常处理必须放在循环里面，保证异常之后不退出循环直接进行下步的处理，符合业务要求的逻辑
      
      END;
    END IF;
  
  END P_IFR_VCP_AGENT_ACTION_D;

  /*坐席状态统计接口   李征 2017/9/6*/
  PROCEDURE P_IFR_VCP_AGENT_STAT AS
    NCOUNT        NUMBER;
    EXPLAINTYPENO VARCHAR2(50);
    V_SQLERRM     VARCHAR2(1000);
  BEGIN
    SELECT COUNT(A.RECORD_ID)
      INTO NCOUNT
      FROM T_IFR_VCP_AGENT_STAT_COUNT A
     WHERE A.STATUS IN (0, 1); --'接口处理状态接口处理状态，0待处理；1正在处理；2已处理；3处理失败；默认值为“0”'
  
    IF NCOUNT > 0 THEN
      BEGIN
        -- I := 1; --数组下标初始化
        UPDATE T_IFR_VCP_AGENT_STAT_COUNT A
           SET A.STATUS = 1 --1：正在传输
         WHERE A.STATUS = 0; --0:待处理
        COMMIT; --提交后方便出现异常后和业务表比对异常数据,比较业务表更新为2状态但是接口表因为回滚而不存在的数据就是异常数据
        --必须每条的传输，并且某条出现异常则记录日志然后进行一下条数据的传输。
        FOR J1 IN (SELECT *
                     FROM T_IFR_VCP_AGENT_STAT_COUNT A
                    WHERE A.STATUS = 1 --1：正在传输
                    ORDER BY A.CREATED_DATE ASC) LOOP
          BEGIN
            EXPLAINTYPENO := J1.ID; --
          
            MERGE INTO T_CS_VCP_AGENT_STAT_COUNT B
            USING (SELECT 1 FROM DUAL) I --不再重复透过DBLINK从接口表
            ON (B.RECORD_ID = J1.RECORD_ID) --操作业务表所以以业务关键字来做匹配条件
            WHEN MATCHED THEN
            -- 按主键找，找到了就更新
              UPDATE
                 SET B.AGENT_NAME             = J1.AGENT_NAME,
                     B.COUNTDATE              = J1.COUNTDATE,
                     B.MINILOGINTIME          = J1.MINILOGINTIME,
                     B.MAXLOGOUTTIME          = J1.MAXLOGOUTTIME,
                     B.LOGINTIMELONG          = J1.LOGINTIMELONG,
                     B.IDLETIMES              = J1.IDLETIMES,
                     B.IDLETIMELONG           = J1.IDLETIMELONG,
                     B.ANSWERTIMES            = J1.ANSWERTIMES,
                     B.CALLINRINGTIMELONG     = J1.CALLINRINGTIMELONG,
                     B.CALLINTIMELONG         = J1.CALLINTIMELONG,
                     B.AFTERCALLINTIMELONG    = J1.AFTERCALLINTIMELONG,
                     B.AFTERCALLOUTTIMELONG   = J1.AFTERCALLOUTTIMELONG,
                     B.CALLOUTTIMES           = J1.CALLOUTTIMES,
                     B.CALLOUTSUCCESTIMES     = J1.CALLOUTSUCCESTIMES,
                     B.CALLOUTRINGTIMELONG    = J1.CALLOUTRINGTIMELONG,
                     B.CALLOUTLIMELONG        = J1.CALLOUTLIMELONG,
                     B.HOLDTIMES              = J1.HOLDTIMES,
                     B.HOLDTIMELONG           = J1.HOLDTIMELONG,
                     B.WORKRATE               = J1.WORKRATE,
                     B.WAITTIMELONG           = J1.WAITTIMELONG,
                     B.TOTALAFTERCALLTIMELONG = J1.TOTALAFTERCALLTIMELONG,
                     B.LAST_UPDATED_DATE      = SYSTIMESTAMP
              
               WHERE B.RECORD_ID = J1.RECORD_ID
              
            
            WHEN NOT MATCHED THEN
            --插入
              INSERT --INTO  T_CS_VCP_AGENT_STAT_COUNT B
                (B.RECORD_ID,
                 B.AGENT_NAME,
                 B.COUNTDATE,
                 B.MINILOGINTIME,
                 B.MAXLOGOUTTIME,
                 B.LOGINTIMELONG,
                 B.IDLETIMES,
                 B.IDLETIMELONG,
                 B.ANSWERTIMES,
                 B.CALLINRINGTIMELONG,
                 B.CALLINTIMELONG,
                 B.AFTERCALLINTIMELONG,
                 B.AFTERCALLOUTTIMELONG,
                 B.CALLOUTTIMES,
                 B.CALLOUTSUCCESTIMES,
                 B.CALLOUTRINGTIMELONG,
                 B.CALLOUTLIMELONG,
                 B.HOLDTIMES,
                 B.HOLDTIMELONG,
                 B.WORKRATE,
                 B.WAITTIMELONG,
                 B.TOTALAFTERCALLTIMELONG)
              
              VALUES
                (J1.RECORD_ID,
                 J1.AGENT_NAME,
                 J1.COUNTDATE,
                 J1.MINILOGINTIME,
                 J1.MAXLOGOUTTIME,
                 J1.LOGINTIMELONG,
                 J1.IDLETIMES,
                 J1.IDLETIMELONG,
                 J1.ANSWERTIMES,
                 J1.CALLINRINGTIMELONG,
                 J1.CALLINTIMELONG,
                 J1.AFTERCALLINTIMELONG,
                 J1.AFTERCALLOUTTIMELONG,
                 J1.CALLOUTTIMES,
                 J1.CALLOUTSUCCESTIMES,
                 J1.CALLOUTRINGTIMELONG,
                 J1.CALLOUTLIMELONG,
                 J1.HOLDTIMES,
                 J1.HOLDTIMELONG,
                 J1.WORKRATE,
                 J1.WAITTIMELONG,
                 J1.TOTALAFTERCALLTIMELONG);
          
            UPDATE T_IFR_VCP_AGENT_STAT_COUNT A
               SET A.STATUS            = 2, --2：传输成功
                   A.LAST_UPDATED_DATE = SYSDATE
             WHERE A.ID = J1.ID
               AND A.STATUS = 1; -- 1 处理中
          
            COMMIT; --操作完整一条数据（包括更新或者插入成功然后并把传输状态改为传输成功状态才算完整操作一条记录）就commit，一旦出现异常则只回滚那一条异常的数据
            --COURSES(I) := J1.ID; --将接口表ID--INTF_ID加入数组
            -- I := I + 1; --数组下标+1
          EXCEPTION
            WHEN OTHERS THEN
              ROLLBACK; --回滚出现异常的某条记录，因为操作完整的一条数据包括2步，所以必须回滚，保证异常了不出出现在业务表中
              V_SQLERRM := SUBSTR(SQLERRM, 0, 1000);
              UPDATE T_IFR_VCP_AGENT_STAT_COUNT A
                 SET A.STATUS      = 3, -- 3 异常
                     A.SEND_REMARK = V_SQLERRM
               WHERE A.ID = EXPLAINTYPENO
                 AND A.STATUS = 1;
              COMMIT; --异常数据的操作，下面是写错误日志，所以分别作自己的事务提交
              INSERT INTO t_cs_db_log
                (LOG_ID,
                 log_name,
                 log_code,
                 log_desc,
                 log_flag,
                 creator,
                 beg_time,
                 end_time)
              VALUES
                (seq_cs_db_log.nextval,
                 '坐席状态统计接口报表_VCP',
                 'P_IFR_VCP_AGENT_STAT',
                 V_SQLERRM,
                 '0',
                 'SYS',
                 SYSDATE,
                 SYSDATE);
              commit;
              --      PKG_CIP_COMMON.ADDOPERATIONLOG('P_IFR_VCP_AGENT_STAT', 'VCP录音文件列表接口', V_SQLERRM, '0', '');
            
              COMMIT;
          END;
        END LOOP; --异常处理必须放在循环里面，保证异常之后不退出循环直接进行下步的处理，符合业务要求的逻辑
      
      END;
    END IF;
  
  END P_IFR_VCP_AGENT_STAT;

  /*坐席信息接口表 李征 2017/9/7*/
  PROCEDURE P_IFR_VCP_AGENT AS
    NCOUNT        NUMBER;
    EXPLAINTYPENO VARCHAR2(50);
    V_SQLERRM     VARCHAR2(1000);
  BEGIN
    SELECT COUNT(A.AGENT_NAME)
      INTO NCOUNT
      FROM T_IFR_VCP_DB_AGENT A
     WHERE A.STATUS IN (0, 1); --'接口处理状态接口处理状态，0待处理；1正在处理；2已处理；3处理失败；默认值为“0”'
  
    IF NCOUNT > 0 THEN
      BEGIN
        -- I := 1; --数组下标初始化
        UPDATE T_IFR_VCP_DB_AGENT A
           SET A.STATUS = 1 --1：正在传输
         WHERE A.STATUS = 0; --0:待处理
        COMMIT; --提交后方便出现异常后和业务表比对异常数据,比较业务表更新为2状态但是接口表因为回滚而不存在的数据就是异常数据
        --必须每条的传输，并且某条出现异常则记录日志然后进行一下条数据的传输。
        FOR J1 IN (SELECT *
                     FROM T_IFR_VCP_DB_AGENT A
                    WHERE A.STATUS = 1 --1：正在传输
                    ORDER BY A.CREATED_DATE ASC) LOOP
          BEGIN
            EXPLAINTYPENO := J1.ID; --
          
            MERGE INTO T_CS_VCP_DB_AGENT B
            USING (SELECT 1 FROM DUAL) I --不再重复透过DBLINK从接口表
            ON (B.AGENT_NAME = J1.AGENT_NAME) --操作业务表所以以业务关键字来做匹配条件
            WHEN MATCHED THEN
            -- 按主键找，找到了就更新
              UPDATE
                 SET B.PASSWORD          = J1.PASSWORD,
                     B.IS_ENABLE         = J1.IS_ENABLE,
                     B.LAST_UPDATED_DATE = SYSTIMESTAMP
               WHERE B.AGENT_NAME = J1.AGENT_NAME
              
            
            WHEN NOT MATCHED THEN
            --插入
              INSERT --INTO  T_CS_VCP_DB_AGENT B
                (B.AGENT_NAME, B.PASSWORD, B.IS_ENABLE)
              VALUES
                (J1.AGENT_NAME, J1.PASSWORD, J1.IS_ENABLE);
          
            UPDATE T_IFR_VCP_DB_AGENT A
               SET A.STATUS            = 2, --2：传输成功
                   A.LAST_UPDATED_DATE = SYSDATE
             WHERE A.ID = J1.ID
               AND A.STATUS = 1; -- 1 处理中
          
            COMMIT; --操作完整一条数据（包括更新或者插入成功然后并把传输状态改为传输成功状态才算完整操作一条记录）就commit，一旦出现异常则只回滚那一条异常的数据
            --COURSES(I) := J1.ID; --将接口表ID--INTF_ID加入数组
            -- I := I + 1; --数组下标+1
          EXCEPTION
            WHEN OTHERS THEN
              ROLLBACK; --回滚出现异常的某条记录，因为操作完整的一条数据包括2步，所以必须回滚，保证异常了不出出现在业务表中
              V_SQLERRM := SUBSTR(SQLERRM, 0, 1000);
              UPDATE T_IFR_VCP_DB_AGENT A
                 SET A.STATUS      = 3, -- 3 异常
                     A.SEND_REMARK = V_SQLERRM
               WHERE A.ID = EXPLAINTYPENO
                 AND A.STATUS = 1;
              COMMIT; --异常数据的操作，下面是写错误日志，所以分别作自己的事务提交
              INSERT INTO t_cs_db_log
                (LOG_ID,
                 log_name,
                 log_code,
                 log_desc,
                 log_flag,
                 creator,
                 beg_time,
                 end_time)
              VALUES
                (seq_cs_db_log.nextval,
                 ' 坐席信息接口表_VCP',
                 'P_IFR_VCP_AGENT',
                 V_SQLERRM,
                 '0',
                 'SYS',
                 SYSDATE,
                 SYSDATE);
              commit;
              --      PKG_CIP_COMMON.ADDOPERATIONLOG('坐席信息接口表', 'VCP录音文件列表接口', V_SQLERRM, '0', '');
            
              COMMIT;
          END;
        END LOOP; --异常处理必须放在循环里面，保证异常之后不退出循环直接进行下步的处理，符合业务要求的逻辑
      
      END;
    END IF;
  
  END P_IFR_VCP_AGENT;

  /*黑名单发送信息接口表 李征 2017/9/7*/
  PROCEDURE P_IFS_VCP_BLACKNAME AS
    NCOUNT        NUMBER;
    EXPLAINTYPENO VARCHAR2(50);
    V_SQLERRM     VARCHAR2(1000);
  BEGIN
    SELECT COUNT(A.BLACK_ID)
      INTO NCOUNT
      FROM T_CS_VCP_BLACKNAME A
     WHERE A.IS_SEND IN (0, 1); --'接口处理状态接口处理状态，0待处理；1正在处理；2已处理；3处理失败；默认值为“0”'
  
    IF NCOUNT > 0 THEN
      BEGIN
      
        UPDATE T_CS_VCP_BLACKNAME A
           SET A.IS_SEND = 1 --1：正在传输
         WHERE A.IS_SEND = 0; --0:待处理
        COMMIT; --提交后方便出现异常后和业务表比对异常数据,比较业务表更新为2状态但是接口表因为回滚而不存在的数据就是异常数据
        --必须每条的传输，并且某条出现异常则记录日志然后进行一下条数据的传输。
        FOR J1 IN (SELECT *
                     FROM T_CS_VCP_BLACKNAME A
                    WHERE A.IS_SEND = 1
                    ORDER BY A.CREATED_DATE ASC) LOOP
          BEGIN
            EXPLAINTYPENO := J1.BLACK_ID; --
          
            MERGE INTO T_IFS_VCP_BLACKNAME B
            USING (SELECT 1 FROM DUAL) I --不再重复透过DBLINK从接口表
            ON (B.BLACK_ID = J1.BLACK_ID) --操作业务表所以以业务关键字来做匹配条件
            WHEN MATCHED THEN
            -- 按主键找，找到了就更新
              UPDATE
                 SET B.PHONENUMBER = J1.PHONENUMBER,
                     B.CODETYPE    = J1.CODETYPE,
                     B.MARKER      = J1.MARKER,
                     B.USEDATE     = J1.USEDATE,
                     B.REMARK      = J1.REMARK,
                     B.Is_Enable   =J1.Is_Enable,
                     B.LINE_TYPE         = J1.LINE_TYPE,
                     B.LAST_UPDATED_DATE = SYSTIMESTAMP
               WHERE B.BLACK_ID = J1.BLACK_ID
              
            
            WHEN NOT MATCHED THEN
            --插入
              INSERT
                (B.BLACK_ID,
                 B.PHONENUMBER,
                 B.CODETYPE,
                 B.MARKER,
                 B.USEDATE,
                 B.REMARK,
                 B.LINE_TYPE,
                 B.SEND_DATE,
                 B.Is_Enable
                 )
              
              VALUES
                (J1.BLACK_ID,
                 J1.PHONENUMBER,
                 J1.CODETYPE,
                 J1.MARKER,
                 J1.USEDATE,
                 J1.REMARK,
                 J1.LINE_TYPE,
                 sysdate,
                 J1.Is_Enable);
          
            UPDATE T_CS_VCP_BLACKNAME A
               SET A.IS_SEND           = 2, --2：传输成功
                   A.LAST_UPDATED_DATE = SYSDATE
             WHERE A.BLACK_ID = J1.BLACK_ID
               AND A.IS_SEND = 1; -- 1 处理中
          
            COMMIT; --操作完整一条数据（包括更新或者插入成功然后并把传输状态改为传输成功状态才算完整操作一条记录）就commit，一旦出现异常则只回滚那一条异常的数据
            --COURSES(I) := J1.ID; --将接口表ID--INTF_ID加入数组
            -- I := I + 1; --数组下标+1
          EXCEPTION
            WHEN OTHERS THEN
              ROLLBACK; --回滚出现异常的某条记录，因为操作完整的一条数据包括2步，所以必须回滚，保证异常了不出出现在业务表中
              V_SQLERRM := SUBSTR(SQLERRM, 0, 1000);
              UPDATE T_CS_VCP_BLACKNAME A
                 SET A.IS_SEND = 3, -- 3 发送失败
                     A.REMARK  = V_SQLERRM
               WHERE A.BLACK_ID = EXPLAINTYPENO;
              --   AND A.STATUS = 1;
              COMMIT; --异常数据的操作，下面是写错误日志，所以分别作自己的事务提交
              INSERT INTO t_cs_db_log
                (LOG_ID,
                 log_name,
                 log_code,
                 log_desc,
                 log_flag,
                 creator,
                 beg_time,
                 end_time)
              VALUES
                (seq_cs_db_log.nextval,
                 ' 黑名单_VCP',
                 'P_IFS_VCP_BLACKNAME',
                 V_SQLERRM,
                 '0',
                 'SYS',
                 SYSDATE,
                 SYSDATE);
            
              COMMIT;
          END;
        END LOOP; --异常处理必须放在循环里面，保证异常之后不退出循环直接进行下步的处理，符合业务要求的逻辑
      
      END;
    END IF;
  
  END P_IFS_VCP_BLACKNAME;

  /*工作组队列接口表 李征 2017/9/8*/
  PROCEDURE P_IFR_VCP_GROUP_QUEUE_INFO AS
  
    NCOUNT        NUMBER;
    EXPLAINTYPENO VARCHAR2(50);
    V_SQLERRM     VARCHAR2(1000);
  BEGIN
    SELECT COUNT(A.GROUP_QUEUE_CODE)
      INTO NCOUNT
      FROM T_IFR_VCP_GROUP_QUEUE_INFO A
     WHERE A.STATUS IN (0, 1); --'接口处理状态接口处理状态，0待处理；1正在处理；2已处理；3处理失败；默认值为“0”'
  
    IF NCOUNT > 0 THEN
      BEGIN
        -- I := 1; --数组下标初始化
        UPDATE T_IFR_VCP_GROUP_QUEUE_INFO A
           SET A.STATUS = 1 --1：正在传输
         WHERE A.STATUS = 0; --0:待处理
        COMMIT; --提交后方便出现异常后和业务表比对异常数据,比较业务表更新为2状态但是接口表因为回滚而不存在的数据就是异常数据
        --必须每条的传输，并且某条出现异常则记录日志然后进行一下条数据的传输。
        FOR J1 IN (SELECT *
                     FROM T_IFR_VCP_GROUP_QUEUE_INFO A
                    WHERE A.STATUS = 1 --1：正在传输
                    ORDER BY A.CREATED_DATE ASC) LOOP
          BEGIN
            EXPLAINTYPENO := J1.ID; --
          
            MERGE INTO T_CS_VCP_GROUP_QUEUE_INFO B
            USING (SELECT 1 FROM DUAL) I --不再重复透过DBLINK从接口表
            ON (B.GROUP_QUEUE_CODE = J1.GROUP_QUEUE_CODE AND B.CODE_TYPE = J1.CODE_TYPE AND B.LINE_TYPE = J1.LINE_TYPE) --操作业务表所以以业务关键字来做匹配条件
            WHEN MATCHED THEN
            -- 按主键找，找到了就更新
              UPDATE
                 SET B.GROUP_QUEUE_NAME = J1.GROUP_QUEUE_NAME,
                     
                     B.IS_ENABLE = J1.IS_ENABLE,
                     
                     B.LAST_UPDATED_DATE = SYSTIMESTAMP
               WHERE B.GROUP_QUEUE_CODE = J1.GROUP_QUEUE_CODE
                 AND B.CODE_TYPE = J1.CODE_TYPE
                 AND B.LINE_TYPE = J1.LINE_TYPE
              
            
            WHEN NOT MATCHED THEN
            --插入
              INSERT --INTO  T_CS_VCP_GROUP_QUEUE_INFO B
                (B.GROUP_QUEUE_CODE,
                 B.GROUP_QUEUE_NAME,
                 B.CODE_TYPE,
                 B.LINE_TYPE,
                 B.IS_ENABLE)
              
              VALUES
                (J1.GROUP_QUEUE_CODE,
                 J1.GROUP_QUEUE_NAME,
                 J1.CODE_TYPE,
                 J1.LINE_TYPE,
                 J1.IS_ENABLE);
          
            UPDATE T_IFR_VCP_GROUP_QUEUE_INFO A
               SET A.STATUS            = 2, --2：传输成功
                   A.LAST_UPDATED_DATE = SYSDATE
             WHERE A.ID = J1.ID
               AND A.STATUS = 1; -- 1 处理中
          
            COMMIT; --操作完整一条数据（包括更新或者插入成功然后并把传输状态改为传输成功状态才算完整操作一条记录）就commit，一旦出现异常则只回滚那一条异常的数据
            --COURSES(I) := J1.ID; --将接口表ID--INTF_ID加入数组
            -- I := I + 1; --数组下标+1
          EXCEPTION
            WHEN OTHERS THEN
              ROLLBACK; --回滚出现异常的某条记录，因为操作完整的一条数据包括2步，所以必须回滚，保证异常了不出出现在业务表中
              V_SQLERRM := SUBSTR(SQLERRM, 0, 1000);
              UPDATE T_IFR_VCP_GROUP_QUEUE_INFO A
                 SET A.STATUS      = 3, -- 3 异常
                     A.SEND_REMARK = V_SQLERRM
               WHERE A.ID = EXPLAINTYPENO
                 AND A.STATUS = 1;
              COMMIT; --异常数据的操作，下面是写错误日志，所以分别作自己的事务提交
              INSERT INTO t_cs_db_log
                (LOG_ID,
                 log_name,
                 log_code,
                 log_desc,
                 log_flag,
                 creator,
                 beg_time,
                 end_time)
              VALUES
                (seq_cs_db_log.nextval,
                 ' 队列信息接口表_VCP',
                 'P_IFR_VCP_GROUP_QUEUE_INFO',
                 V_SQLERRM,
                 '0',
                 'SYS',
                 SYSDATE,
                 SYSDATE);
              commit;
              --      PKG_CIP_COMMON.ADDOPERATIONLOG('队列信息接口表', 'VCP录音文件列表接口', V_SQLERRM, '0', '');
            
              COMMIT;
          END;
        END LOOP; --异常处理必须放在循环里面，保证异常之后不退出循环直接进行下步的处理，符合业务要求的逻辑
      
      END;
    END IF;
  
  END P_IFR_VCP_GROUP_QUEUE_INFO;

  /*坐席工作组关系接口表 李征 2017/9/7*/
  PROCEDURE P_IFR_VCP_AGENT_GROUP_INFO AS
  
    NCOUNT        NUMBER;
    EXPLAINTYPENO VARCHAR2(50);
    V_SQLERRM     VARCHAR2(1000);
  BEGIN
    SELECT COUNT(A.AGENT_NAME)
      INTO NCOUNT
      FROM T_IFR_VCP_AGENT_GROUP_INFO A
    
     WHERE A.STATUS IN (0, 1); --'接口处理状态接口处理状态，0待处理；1正在处理；2已处理；3处理失败；默认值为“0”'
  
    IF NCOUNT > 0 THEN
      BEGIN
      
        --  删除业务表数据
        DELETE FROM T_CS_VCP_AGENT_GROUP_INFO B;
        COMMIT;
      
        -- I := 1; --数组下标初始化
        UPDATE T_IFR_VCP_AGENT_GROUP_INFO A
           SET A.STATUS = 1 --1：正在传输
         WHERE A.STATUS = 0; --0:待处理
        COMMIT; --提交后方便出现异常后和业务表比对异常数据,比较业务表更新为2状态但是接口表因为回滚而不存在的数据就是异常数据
        --必须每条的传输，并且某条出现异常则记录日志然后进行一下条数据的传输。
        FOR J1 IN (SELECT *
                     FROM T_IFR_VCP_AGENT_GROUP_INFO A
                    WHERE A.STATUS = 1 --1：正在传输
                    ORDER BY A.CREATED_DATE ASC) LOOP
          BEGIN
            EXPLAINTYPENO := J1.ID; --
          
            MERGE INTO T_CS_VCP_AGENT_GROUP_INFO B
            
            USING (SELECT 1 FROM DUAL) I --不再重复透过DBLINK从接口表
            ON (B.RF_ID = J1.ID) --操作业务表所以以业务关键字来做匹配条件
            WHEN MATCHED THEN
            -- 按主键找，找到了就更新
              UPDATE
                 SET B.AGENT_NAME        = J1.AGENT_NAME,
                     B.GROUP_CODE        = J1.GROUP_CODE,
                     B.LAST_UPDATED_DATE = SYSTIMESTAMP
               WHERE B.RF_ID = J1.ID
              
            
            WHEN NOT MATCHED THEN
            --插入
              INSERT --INTO   T_CS_VCP_AGENT_GROUP_INFO B
                (B.RF_ID, B.AGENT_NAME, B.GROUP_CODE)
              
              VALUES
                (J1.ID, J1.AGENT_NAME, J1.GROUP_CODE);
          
            UPDATE T_IFR_VCP_AGENT_GROUP_INFO A
               SET A.STATUS            = 2, --2：传输成功
                   A.LAST_UPDATED_DATE = SYSDATE
             WHERE A.AGENT_NAME = J1.AGENT_NAME
               AND A.STATUS = 1; -- 1 处理中
          
            COMMIT; --操作完整一条数据（包括更新或者插入成功然后并把传输状态改为传输成功状态才算完整操作一条记录）就commit，一旦出现异常则只回滚那一条异常的数据
            --COURSES(I) := J1.ID; --将接口表ID--INTF_ID加入数组
            -- I := I + 1; --数组下标+1
          EXCEPTION
            WHEN OTHERS THEN
              ROLLBACK; --回滚出现异常的某条记录，因为操作完整的一条数据包括2步，所以必须回滚，保证异常了不出出现在业务表中
              V_SQLERRM := SUBSTR(SQLERRM, 0, 1000);
              UPDATE T_IFR_VCP_AGENT_GROUP_INFO A
                 SET A.STATUS      = 3, -- 3 异常
                     A.SEND_REMARK = V_SQLERRM
               WHERE A.ID = EXPLAINTYPENO
                 AND A.STATUS = 1;
              COMMIT; --异常数据的操作，下面是写错误日志，所以分别作自己的事务提交
              INSERT INTO t_cs_db_log
                (LOG_ID,
                 log_name,
                 log_code,
                 log_desc,
                 log_flag,
                 creator,
                 beg_time,
                 end_time)
              VALUES
                (seq_cs_db_log.nextval,
                 ' 坐席工作组关系接口表_VCP',
                 'P_IFR_VCP_AGENT_GROUP_INFO',
                 V_SQLERRM,
                 '0',
                 'SYS',
                 SYSDATE,
                 SYSDATE);
              commit;
              --      PKG_CIP_COMMON.ADDOPERATIONLOG('队列信息接口表', 'VCP录音文件列表接口', V_SQLERRM, '0', '');
            
              COMMIT;
          END;
        END LOOP; --异常处理必须放在循环里面，保证异常之后不退出循环直接进行下步的处理，符合业务要求的逻辑
      
      END;
    END IF;
  
  END P_IFR_VCP_AGENT_GROUP_INFO;

  /*自动计算通话时长 李征 2017/9/15*/
  PROCEDURE P_CS_VCP_AUTO_COUNT_LONGER AS
  
    V_SQLERRM VARCHAR2(1000);
  BEGIN
  
    for i in (with A AS (SELECT
                 
                  row_number() over(partition by dd.bill_code order by dd.created_date asc) as XH,
                  Rec.RECORD_ID as RecordingID,
                  Rec.CREATED_DATE as RecordingDate,
                  Rec.CUSTOMER_NUM as PhoneCode,
                  Rec.Direction as CALLDIRECTION,
                  Rec.Longer as CallDurationSeconds,
                  DD.bill_code as bill_code
                   FROM T_CS_BU_CALL_INFO DD
                   left join T_CS_VCP_RECORDDETAIL Rec on DD.call_id =
                                                          Rec.RECORD_ID
                  where rec.direction = '1'
                       
                    and dd.created_date > sysdate - 3),
              
              B AS (select server_order
                      from t_cs_bu_server_info t
                     where t.Longer is null
                       and t.server_type in (1, 2, 3)
                       and t.created_date > sysdate - 3)
              
              select B.SERVER_ORDER, A.CallDurationSeconds
                from B
               INNER JOIN A ON B.SERVER_ORDER = A.bill_code
               where A.XH = 1)
    
     loop
    
      begin
        update t_cs_bu_server_info t
           set t.longer = i.CallDurationSeconds
         where t.server_order = i.SERVER_ORDER;
        commit;
      
      EXCEPTION
        WHEN OTHERS THEN
          ROLLBACK; --回滚出现异常的某条记录，因为操作完整的一条数据包括2步，所以必须回滚，保证异常了不出出现在业务表中
          V_SQLERRM := SUBSTR(SQLERRM, 0, 1000);
        
          COMMIT; --异常数据的操作，下面是写错误日志，所以分别作自己的事务提交
          INSERT INTO t_cs_db_log
            (LOG_ID,
             log_name,
             log_code,
             log_desc,
             log_flag,
             creator,
             beg_time,
             end_time)
          VALUES
            (seq_cs_db_log.nextval,
             ' 自动计算首次来电时长',
             'P_CS_VCP_AUTO_COUNT_LONGER',
             V_SQLERRM,
             '0',
             'SYS',
             SYSDATE,
             SYSDATE);
        
          COMMIT;
      END;
    
    end loop;
  
  END P_CS_VCP_AUTO_COUNT_LONGER;

  /*丢失电话回访单自动生成 */

  procedure P_CS_VCP_IVRCalls_TO_LOSTCalls as
  begin
    delete T_CS_DB_LOSTCalls;
    insert into T_CS_DB_LOSTCalls
      (CICCALLID,
       REMOTENUMBER,
       ACDNAME,
       CALLTYPE,
       DTSYSTEM,
       DTACD,
       ACDTIMES,
       CALLTIMES,
       AGENTID)
    
      select a.TERMINATIONCALL_ID as CICCALLID,
             a.ANI                as REMOTENUMBER,
             a.Queue_No           as ACDNAME,
             a.CallType           as CALLTYPE,
             sysdate              as DTSYSTEM,
             a.ACDSTARTTIME       as DTACD,
             a.ACDTIMELONG        as ACDTIMES,
             a.RINGTIMELONG       as CALLTIMES,
             a.AGENTNAME          as AGENTID
        from T_CS_VCP_TERMCALL_EVENT_DETAIL a
       where a.DIRECTION = '1'
         and a.line_type in('0','1')
         and a.CALLTYPE in ('2', '3', '5', '6')
         and a.last_updated_date > sysdate - 1
         and not exists
       (select 1
                from T_CS_VCP_TERMCALL_EVENT_DETAIL b
               where to_char(b.ANI) = to_char(a.ANI)
                 and b.CallType in ('2', '3', '5', '6')
                 and b.line_type in('0','1')
                 and a.DIRECTION = b.DIRECTION
                 AND b.last_updated_date > a.last_updated_date)
       order by a.last_updated_date desc;
    commit;
  end;

end PKG_CIP_INTERFACE_VCP;
/
