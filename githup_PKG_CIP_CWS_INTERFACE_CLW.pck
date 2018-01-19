create or replace package PKG_CIP_CWS_INTERFACE_CLW is

  -- Author  : LY-ZHENGLI
  -- Created : 2017/11/8 9:56:54
  PROCEDURE P_T_IFS_CWS_CGUARD_INFO;
  PROCEDURE P_T_IFS_CWS_CARPOSITION_INFO;

  PROCEDURE P_T_IFS_CWS_DEST_DOWNLOAD_INFO;
  PROCEDURE P_T_IFS_CWS_CARTRACKING_MAIN;
  PROCEDURE P_T_CWS_BU_CGUARD_INFO;
  PROCEDURE P_T_CWS_BU_DEST_DOWNLOAD_INFO;
  PROCEDURE P_T_CWS_BU_CARPOSITION_INFO;
  PROCEDURE P_T_CWS_BU_CARTRACKING_DETAIL;
  PROCEDURE P_T_CWS_BU_VEHNET_VISIT_S;

  PROCEDURE P_T_IFS_CWS_SERV_PKG_BUY;
  PROCEDURE P_T_IFS_CWS_CUST_SERVICE_INFO;

  PROCEDURE P_T_CWS_BU_SMS_SEND;

  FUNCTION FN_GET_SERVICETYPENAME_BY_CODE(F_SERVICE_TYPE_CODE in VARCHAR2) RETURN VARCHAR2;
  FUNCTION FN_GET_PROCEDURENAME(PROCEDURENAME in VARCHAR2) RETURN VARCHAR2;

end PKG_CIP_CWS_INTERFACE_CLW;
/
create or replace package body PKG_CIP_CWS_INTERFACE_CLW is
  /*T_IFS_CWS_CGUARD_INFO
  T_CWS_BU_CGUARD_INFO*/
  /*安防记录信息（同步） 李征 2017-11-10*/
  V_SQLERRM VARCHAR2(1000);
  PROCEDURE P_T_IFS_CWS_CGUARD_INFO AS
    NCOUNT        NUMBER;
    EXPLAINTYPENO VARCHAR2(50);
    --V_SQLERRM     VARCHAR2(1000);
  BEGIN
    SELECT COUNT(A.CGUARD_REQUEST_ID) INTO NCOUNT FROM T_CWS_BU_CGUARD_INFO A WHERE A.SEND_FLAG IN ('0', '1'); --'接口处理状态接口处理状态，0待处理；1正在处理；2已处理；3处理失败；默认值为“0”'
  
    IF NCOUNT > 0 THEN
      BEGIN
      
        UPDATE T_CWS_BU_CGUARD_INFO A
        SET    A.SEND_FLAG = '1' --1：正在传输
        WHERE  A.SEND_FLAG = '0'; --0:待处理
        COMMIT; --提交后方便出现异常后和业务表比对异常数据,比较业务表更新为2状态但是接口表因为回滚而不存在的数据就是异常数据
        --必须每条的传输，并且某条出现异常则记录日志然后进行一下条数据的传输。
        FOR J1 IN (SELECT * FROM T_CWS_BU_CGUARD_INFO A WHERE A.SEND_FLAG = '1' ORDER BY A.CREATED_DATE ASC)
        LOOP
          BEGIN
            EXPLAINTYPENO := J1.CGUARD_REQUEST_ID;
          
            INSERT into IFS.T_IFS_CWS_CGUARD_INFO B
              (B.CGUARD_REQUEST_ID,
               B.VIN,
               B.CALLIN_TYPE_CODE,
               B.CALLIN_TYPE_NAME,
               B.CGUARD_TYPE_CODE,
               B.CGUARD_TYPE_NAME,
               B.CAR_FORHEAD,
               B.GPS_TIME,
               B.CAR_POSITION,
               B.E_OR_W,
               B.S_OR_N,
               B.CREATE_DATE,
               B.CGUARD_CONTENT)
            
            VALUES
              (J1.CGUARD_REQUEST_ID,
               J1.VIN,
               J1.CALLIN_TYPE_CODE,
               J1.CALLIN_TYPE_NAME,
               J1.CGUARD_TYPE_CODE,
               J1.CGUARD_TYPE_NAME,
               J1.CAR_FORHEAD,
               J1.GPS_TIME,
               J1.CAR_POSITION,
               J1.E_OR_W,
               J1.S_OR_N,
               J1.CREATE_DATE,
               J1.CGUARD_CONTENT);
          
            UPDATE T_CWS_BU_CGUARD_INFO A
            SET    A.SEND_FLAG         = '2', --2：传输成功
                   A.LAST_UPDATED_DATE = SYSDATE
            WHERE  A.CGUARD_REQUEST_ID = J1.CGUARD_REQUEST_ID AND A.SEND_FLAG = '1'; -- 1 处理中
          
            COMMIT; --操作完整一条数据（包括更新或者插入成功然后并把传输状态改为传输成功状态才算完整操作一条记录）就commit，一旦出现异常则只回滚那一条异常的数据
            --COURSES(I) := J1.ID; --将接口表ID--INTF_ID加入数组
            -- I := I + 1; --数组下标+1
          
          EXCEPTION
          
            WHEN OTHERS THEN
              ROLLBACK; --回滚出现异常的某条记录，因为操作完整的一条数据包括2步，所以必须回滚，保证异常了不出出现在业务表中
              V_SQLERRM := SUBSTR(SQLERRM, 0, 1000);
              UPDATE T_CWS_BU_CGUARD_INFO A
              SET    A.SEND_FLAG = '3' -- 3 发送失败
              
              WHERE  A.CGUARD_REQUEST_ID = EXPLAINTYPENO AND A.SEND_FLAG = '1';
              COMMIT; --异常数据的操作，下面是写错误日志，所以分别作自己的事务提交
              PKG_COC_COMMON.ADDOPERATIONLOG('T_IFS_CWS_CGUARD_INFO', '安防记录信息（同步）', V_SQLERRM, '', '');
            
              COMMIT;
          END;
        END LOOP; --异常处理必须放在循环里面，保证异常之后不退出循环直接进行下步的处理，符合业务要求的逻辑
      
      END;
    END IF;
  
  END P_T_IFS_CWS_CGUARD_INFO;

  /*  
  IFS.T_IFS_CWS_CARPOSITION_INFO;
  T_CWS_BU_CARPOSITION_INFO;*/

  /*定位请求日志（同步） 李征 2017-11-10*/
  PROCEDURE P_T_IFS_CWS_CARPOSITION_INFO AS
    NCOUNT        NUMBER;
    EXPLAINTYPENO VARCHAR2(50);
    --V_SQLERRM     VARCHAR2(1000);
  BEGIN
    SELECT COUNT(A.REQUEST_ID) INTO NCOUNT FROM T_CWS_BU_CARPOSITION_INFO A WHERE A.SEND_FLAG IN ('0', '1'); --'接口处理状态接口处理状态，0待处理；1正在处理；2已处理；3处理失败；默认值为“0”'
  
    IF NCOUNT > 0 THEN
      BEGIN
      
        UPDATE T_CWS_BU_CARPOSITION_INFO A
        SET    A.SEND_FLAG = '1' --1：正在传输
        WHERE  A.SEND_FLAG = '0'; --0:待处理
        COMMIT; --提交后方便出现异常后和业务表比对异常数据,比较业务表更新为2状态但是接口表因为回滚而不存在的数据就是异常数据
        --必须每条的传输，并且某条出现异常则记录日志然后进行一下条数据的传输。
        FOR J1 IN (SELECT * FROM T_CWS_BU_CARPOSITION_INFO A WHERE A.SEND_FLAG = '1' ORDER BY A.CREATED_DATE ASC)
        LOOP
          BEGIN
            EXPLAINTYPENO := J1.REQUEST_ID;
          
            INSERT into IFS.T_IFS_CWS_CARPOSITION_INFO B
              (B.REQUEST_ID,
               B.VIN,
               B.TO_VHENET_TIME,
               B.TO_TCU_TIME,
               B.CAR_FORHEAD,
               B.GPS_TIME,
               B.CAR_POSITION,
               B.E_OR_W,
               B.S_OR_N,
               B.TCU_VEHNET_TIME,
               
               B.SEND_FLAG,
               
               B.REQUEST_STATUS_CODE,
               B.REQUEST_STATUS,
               B.ADDR,
               B.TCURESULTCODE,
               B.TCURESULTNAME)
            
            VALUES
              (J1.REQUEST_ID,
               J1.VIN,
               J1.TO_VHENET_TIME,
               J1.TO_TCU_TIME,
               J1.CAR_FORHEAD,
               J1.GPS_TIME,
               J1.CAR_POSITION,
               J1.E_OR_W,
               J1.S_OR_N,
               J1.TCU_VEHNET_TIME,
               '0',
               J1.REQUEST_STATUS_CODE,
               J1.REQUEST_STATUS,
               J1.ADDR,
               J1.TCURESULTCODE,
               J1.TCURESULTNAME);
          
            UPDATE T_CWS_BU_CARPOSITION_INFO A
            SET    A.SEND_FLAG         = '2', --2：传输成功
                   A.LAST_UPDATED_DATE = SYSDATE
            WHERE  A.REQUEST_ID = J1.REQUEST_ID AND A.SEND_FLAG = '1'; -- 1 处理中
          
            COMMIT; --操作完整一条数据（包括更新或者插入成功然后并把传输状态改为传输成功状态才算完整操作一条记录）就commit，一旦出现异常则只回滚那一条异常的数据
            --COURSES(I) := J1.ID; --将接口表ID--INTF_ID加入数组
            -- I := I + 1; --数组下标+1
          EXCEPTION
            WHEN OTHERS THEN
              ROLLBACK; --回滚出现异常的某条记录，因为操作完整的一条数据包括2步，所以必须回滚，保证异常了不出出现在业务表中
              V_SQLERRM := SUBSTR(SQLERRM, 0, 1000);
              UPDATE T_CWS_BU_CARPOSITION_INFO A
              SET    A.SEND_FLAG = '3' -- 3 发送失败
              
              WHERE  A.REQUEST_ID = EXPLAINTYPENO AND A.SEND_FLAG = '1';
              COMMIT; --异常数据的操作，下面是写错误日志，所以分别作自己的事务提交
              PKG_COC_COMMON.ADDOPERATIONLOG('P_T_IFS_CWS_CARPOSITION_INFO', '定位请求日志（同步）', V_SQLERRM, '', '');
            
              COMMIT;
          END;
        END LOOP; --异常处理必须放在循环里面，保证异常之后不退出循环直接进行下步的处理，符合业务要求的逻辑
      END;
    END IF;
  
  END P_T_IFS_CWS_CARPOSITION_INFO;

  /*
  IFS.T_IFS_CWS_DEST_DOWNLOAD_INFO;
  T_CWS_BU_DEST_DOWNLOAD_INFO;*/

  /*目的下载记录（同步） 李征 2017-11-10*/
  PROCEDURE P_T_IFS_CWS_DEST_DOWNLOAD_INFO AS
    NCOUNT        NUMBER;
    EXPLAINTYPENO VARCHAR2(50);
    --V_SQLERRM     VARCHAR2(1000);
  BEGIN
    SELECT COUNT(A.DEST_DW_LOGID) INTO NCOUNT FROM T_CWS_BU_DEST_DOWNLOAD_INFO A WHERE A.SEND_FLAG IN ('0', '1'); --'接口处理状态接口处理状态，0待处理；1正在处理；2已处理；3处理失败；默认值为“0”'
  
    IF NCOUNT > 0 THEN
      BEGIN
      
        UPDATE T_CWS_BU_DEST_DOWNLOAD_INFO A
        SET    A.SEND_FLAG = '1' --1：正在传输
        WHERE  A.SEND_FLAG = '0'; --0:待处理
        COMMIT; --提交后方便出现异常后和业务表比对异常数据,比较业务表更新为2状态但是接口表因为回滚而不存在的数据就是异常数据
        --必须每条的传输，并且某条出现异常则记录日志然后进行一下条数据的传输。
        FOR J1 IN (SELECT * FROM T_CWS_BU_DEST_DOWNLOAD_INFO A WHERE A.SEND_FLAG = '1' ORDER BY A.CREATED_DATE ASC)
        LOOP
          BEGIN
            EXPLAINTYPENO := J1.DEST_DW_LOGID;
          
            INSERT into IFS.T_IFS_CWS_DEST_DOWNLOAD_INFO B
              (B.DEST_DW_LOGID, B.VIN, B.NAVIGATION_CODE, B.NAVIGATION_NAME, B.CREATE_DATE, B.E_OR_W, B.S_OR_N, B.DEST_NAME, B.DA_REQEST_DATE, B.SERVER_ORDER, B.CREATOR)
            
            VALUES
              (J1.DEST_DW_LOGID,
               J1.VIN,
               DECODE(J1.NAVIGATION_CODE, '0', 'dst', '1', 'nml', '2', 'hwy', J1.NAVIGATION_CODE),
               J1.NAVIGATION_NAME,
               J1.CREATE_DATE,
               J1.E_OR_W,
               J1.S_OR_N,
               J1.DEST_NAME,
               J1.DA_REQEST_DATE,
               J1.SERVER_ORDER,
               J1.CREATOR);
          
            UPDATE T_CWS_BU_DEST_DOWNLOAD_INFO A
            SET    A.SEND_FLAG         = '2', --2：传输成功
                   A.LAST_UPDATED_DATE = SYSDATE
            WHERE  A.DEST_DW_LOGID = J1.DEST_DW_LOGID AND A.SEND_FLAG = '1'; -- 1 处理中
          
            COMMIT; --操作完整一条数据（包括更新或者插入成功然后并把传输状态改为传输成功状态才算完整操作一条记录）就commit，一旦出现异常则只回滚那一条异常的数据
            --COURSES(I) := J1.ID; --将接口表ID--INTF_ID加入数组
            -- I := I + 1; --数组下标+1
          EXCEPTION
            WHEN OTHERS THEN
              ROLLBACK; --回滚出现异常的某条记录，因为操作完整的一条数据包括2步，所以必须回滚，保证异常了不出出现在业务表中
              V_SQLERRM := SUBSTR(SQLERRM, 0, 1000);
              UPDATE T_CWS_BU_DEST_DOWNLOAD_INFO A
              SET    A.SEND_FLAG = '3' -- 3 发送失败
              
              WHERE  A.DEST_DW_LOGID = EXPLAINTYPENO AND A.SEND_FLAG = '1';
              COMMIT; --异常数据的操作，下面是写错误日志，所以分别作自己的事务提交
              PKG_COC_COMMON.ADDOPERATIONLOG('P_T_IFS_CWS_DEST_DOWNLOAD_INFO', '目的下载记录（同步）', V_SQLERRM, '', '');
              COMMIT;
          END;
        END LOOP; --异常处理必须放在循环里面，保证异常之后不退出循环直接进行下步的处理，符合业务要求的逻辑
      END;
    END IF;
  
  END P_T_IFS_CWS_DEST_DOWNLOAD_INFO;

  /*T_IFS_CWS_CARTRACKING_MAIN;
  T_CWS_BU_CARTRACKING_MAIN */

  /*追踪请求主表 李征 2017-11-10*/
  PROCEDURE P_T_IFS_CWS_CARTRACKING_MAIN AS
    NCOUNT        NUMBER;
    EXPLAINTYPENO VARCHAR2(50);
    --V_SQLERRM     VARCHAR2(1000);
  BEGIN
    SELECT COUNT(A.REQUEST_ID) INTO NCOUNT FROM T_CWS_BU_CARTRACKING_MAIN A WHERE A.SEND_FLAG IN ('0', '1'); --'接口处理状态接口处理状态，0待处理；1正在处理；2已处理；3处理失败；默认值为“0”'
  
    IF NCOUNT > 0 THEN
      BEGIN
      
        UPDATE T_CWS_BU_CARTRACKING_MAIN A
        SET    A.SEND_FLAG = '1' --1：正在传输
        WHERE  A.SEND_FLAG = '0'; --0:待处理
        COMMIT; --提交后方便出现异常后和业务表比对异常数据,比较业务表更新为2状态但是接口表因为回滚而不存在的数据就是异常数据
        --必须每条的传输，并且某条出现异常则记录日志然后进行一下条数据的传输。
        FOR J1 IN (SELECT * FROM T_CWS_BU_CARTRACKING_MAIN A WHERE A.SEND_FLAG = '1' ORDER BY A.CREATED_DATE ASC)
        LOOP
          BEGIN
            EXPLAINTYPENO := J1.REQUEST_ID;
          
            INSERT into IFS.T_IFS_CWS_CARTRACKING_MAIN B
              (B.REQUEST_ID,
               B.VIN,
               B.IS_BEGIN_TRACK,
               B.BEGIN_TRACK_TIME,
               B.END_TRACK_TIME,
               B.TIME_INTERVAL_VALUE,
               B.TIME_INTERVAL_TYPE,
               B.REQUEST_RESULT_CODE,
               B.REQUEST_RESULT_DESC,
               B.NOTIFY_STARTTIME,
               B.NOTIFYTYPE,
               B.NOTIFYTIME,
               B.NOTIFYFORMAT,
               B.NOTIFYINTERVAL,
               B.NOTIFYTO,
               B.NOTIFYTOTEL,
               B.IS_NOTIFY_MSG,
               B.IS_NOTIFY_MMS)
            
            VALUES
              (J1.REQUEST_ID,
               J1.VIN,
               J1.IS_BEGIN_TRACK,
               J1.BEGIN_TRACK_TIME,
               J1.END_TRACK_TIME,
               J1.TIME_INTERVAL_VALUE,
               J1.TIME_INTERVAL_TYPE,
               J1.REQUEST_RESULT_CODE,
               J1.REQUEST_RESULT_DESC,
               J1.NOTIFY_STARTTIME,
               J1.NOTIFYTYPE,
               J1.NOTIFYTIME,
               J1.NOTIFYFORMAT,
               J1.NOTIFYINTERVAL,
               J1.NOTIFYTO,
               J1.NOTIFYTOTEL,
               J1.IS_NOTIFY_MSG,
               J1.IS_NOTIFY_MMS);
          
            UPDATE T_CWS_BU_CARTRACKING_MAIN A
            SET    A.SEND_FLAG         = '2', --2：传输成功
                   A.LAST_UPDATED_DATE = SYSDATE
            WHERE  A.REQUEST_ID = J1.REQUEST_ID AND A.SEND_FLAG = '1'; -- 1 处理中
          
            COMMIT; --操作完整一条数据（包括更新或者插入成功然后并把传输状态改为传输成功状态才算完整操作一条记录）就commit，一旦出现异常则只回滚那一条异常的数据
            --COURSES(I) := J1.ID; --将接口表ID--INTF_ID加入数组
            -- I := I + 1; --数组下标+1
          EXCEPTION
            WHEN OTHERS THEN
              ROLLBACK; --回滚出现异常的某条记录，因为操作完整的一条数据包括2步，所以必须回滚，保证异常了不出出现在业务表中
              V_SQLERRM := SUBSTR(SQLERRM, 0, 1000);
              UPDATE T_CWS_BU_CARTRACKING_MAIN A
              SET    A.SEND_FLAG = '3' -- 3 发送失败
              
              WHERE  A.REQUEST_ID = EXPLAINTYPENO AND A.SEND_FLAG = '1';
              COMMIT; --异常数据的操作，下面是写错误日志，所以分别作自己的事务提交
              PKG_COC_COMMON.ADDOPERATIONLOG('P_T_IFS_CWS_CARTRACKING_MAIN', '追踪请求主表', V_SQLERRM, '', '');
            
              COMMIT;
          END;
        END LOOP; --异常处理必须放在循环里面，保证异常之后不退出循环直接进行下步的处理，符合业务要求的逻辑
      END;
    END IF;
  
  END P_T_IFS_CWS_CARTRACKING_MAIN;
  /*
  IFR.T_IFR_CWS_CGUARD_INFO
  T_CWS_BU_CGUARD_INFO
  */
  /*接口表_ 接收08IT安防_安防记录 李征 2017-11-8*/
  PROCEDURE P_T_CWS_BU_CGUARD_INFO AS
    NCOUNT        NUMBER;
    EXPLAINTYPENO VARCHAR2(50);
    --V_SQLERRM     VARCHAR2(1000);
  BEGIN
    SELECT COUNT(A.ID) INTO NCOUNT FROM IFR.T_IFR_CWS_CGUARD_INFO A WHERE A.SEND_FLAG IN ('0', '1'); --'接口处理状态接口处理状态，0待处理；1正在处理；2已处理；3处理失败；默认值为“0”'
  
    IF NCOUNT > 0 THEN
      BEGIN
        -- I := 1; --数组下标初始化
        UPDATE IFR.T_IFR_CWS_CGUARD_INFO A
        SET    A.SEND_FLAG = '1' --1：正在传输
        WHERE  A.SEND_FLAG = '0'; --0:待处理
        COMMIT; --提交后方便出现异常后和业务表比对异常数据,比较业务表更新为2状态但是接口表因为回滚而不存在的数据就是异常数据
        --必须每条的传输，并且某条出现异常则记录日志然后进行一下条数据的传输。
        FOR J1 IN (SELECT *
                   FROM   IFR.T_IFR_CWS_CGUARD_INFO A
                   WHERE  A.SEND_FLAG = '1' --1：正在传输
                   ORDER  BY A.CREATED_DATE ASC)
        LOOP
          BEGIN
            EXPLAINTYPENO := J1.ID; --
          
            MERGE INTO T_CWS_BU_CGUARD_INFO B
            USING (SELECT 1 FROM DUAL) I --不再重复透过DBLINK从接口表
            ON (B.CGUARD_REQUEST_ID = J1.REQID) --操作业务表所以以业务关键字来做匹配条件
            WHEN MATCHED THEN
            -- 按主键找，找到了就更新
              UPDATE
              SET    B.VIN              = J1.VIN, --VIN码
                     B.CALLIN_TYPE_CODE = J1.CALLTYPECODE, --呼入类型编码
                     B.CALLIN_TYPE_NAME = J1.CALLTYPENAME, --呼入类型名称
                     B.CGUARD_TYPE_CODE = J1.SECURITYTYPECODE, --安防类型编码
                     B.CGUARD_TYPE_NAME = J1.SECURITYTYPENAME, --安防类型名称
                     B.CAR_FORHEAD      = J1.FRONTDIRECTION,
                     /*传过来的时间格式为秒级时间戳，需要进行转换 ，下同*/
                     B.GPS_TIME       = J1.GPSDATE / (60 * 60 * 24) + TO_DATE('1970-01-01 08:00:00', 'YYYY-MM-DD HH24:MI:SS'),
                     B.CAR_POSITION   = J1.CARLOCATION,
                     B.CGUARD_CONTENT = decode(J1.CGUARDCONTENT,'null','',J1.CGUARDCONTENT),
                     B.E_OR_W         = J1.LONGITUDE, --经度
                     B.S_OR_N         = J1.LATITUDE, --纬度
                     B.CREATE_DATE    = J1.CREATETIME / (60 * 60 * 24) + TO_DATE('1970-01-01 08:00:00', 'YYYY-MM-DD HH24:MI:SS'), --建单时间
                     
                     B.LAST_UPDATED_DATE = SYSTIMESTAMP
              
              WHERE  B.CGUARD_REQUEST_ID = J1.REQID
              
            
            WHEN NOT MATCHED THEN
            --插入
              INSERT --INTO T_CWS_BU_CGUARD_INFO B
                (CGUARD_REQUEST_ID, VIN, CALLIN_TYPE_CODE, CALLIN_TYPE_NAME, CGUARD_TYPE_CODE, CGUARD_TYPE_NAME, CAR_FORHEAD, GPS_TIME, CAR_POSITION, E_OR_W, S_OR_N, CGUARD_CONTENT, CREATE_DATE)
              
              VALUES
                (J1.REQID,
                 J1.VIN,
                 J1.CALLTYPECODE,
                 J1.CALLTYPENAME,
                 J1.SECURITYTYPECODE,
                 J1.SECURITYTYPENAME,
                 J1.FRONTDIRECTION,
                 J1.GPSDATE / (60 * 60 * 24) + TO_DATE('1970-01-01 08:00:00', 'YYYY-MM-DD HH24:MI:SS'),
                 J1.CARLOCATION,
                 J1.LONGITUDE,
                 J1.LATITUDE,
                 decode(J1.CGUARDCONTENT,'null','',J1.CGUARDCONTENT),
                 J1.CREATETIME / (60 * 60 * 24) + TO_DATE('1970-01-01 08:00:00', 'YYYY-MM-DD HH24:MI:SS'));
          
            UPDATE IFR.T_IFR_CWS_CGUARD_INFO A
            SET    A.SEND_FLAG         = '2', --2：传输成功
                   A.SEND_DATE         = SYSDATE,
                   A.LAST_UPDATED_DATE = SYSDATE
            WHERE  A.ID = J1.ID AND A.SEND_FLAG = '1'; -- 1 处理中
          
            COMMIT; --操作完整一条数据（包括更新或者插入成功然后并把传输状态改为传输成功状态才算完整操作一条记录）就commit，一旦出现异常则只回滚那一条异常的数据
            --COURSES(I) := J1.ID; --将接口表ID--INTF_ID加入数组
            -- I := I + 1; --数组下标+1
          EXCEPTION
            WHEN OTHERS THEN
              ROLLBACK; --回滚出现异常的某条记录，因为操作完整的一条数据包括2步，所以必须回滚，保证异常了不出出现在业务表中
              V_SQLERRM := SUBSTR(SQLERRM, 0, 1000);
              UPDATE IFR.T_IFR_CWS_CGUARD_INFO A
              SET    A.SEND_FLAG   = '3', -- 3 异常
                     A.SEND_REMARK = V_SQLERRM
              WHERE  A.ID = EXPLAINTYPENO AND A.SEND_FLAG = '1';
              COMMIT; --异常数据的操作，下面是写错误日志，所以分别作自己的事务提交
              PKG_COC_COMMON.ADDOPERATIONLOG('P_T_CWS_BU_CGUARD_INFO', '接口表_ 接收08IT安防_安防记录', V_SQLERRM, '', '');
            
              COMMIT;
          END;
        END LOOP; --异常处理必须放在循环里面，保证异常之后不退出循环直接进行下步的处理，符合业务要求的逻辑
      
      END;
    END IF;
  
  END P_T_CWS_BU_CGUARD_INFO;

  /*  
  IFR.T_IFR_CWS_DEST_DOWNLOAD
  T_CWS_BU_DEST_DOWNLOAD_INFO
  
  COOK
  */

  /*接口表_推送给08IT安防_目的地下载记录（异步） 李征 2017-11-8*/
  PROCEDURE P_T_CWS_BU_DEST_DOWNLOAD_INFO AS
    NCOUNT        NUMBER;
    EXPLAINTYPENO VARCHAR2(50);
    --V_SQLERRM     VARCHAR2(1000);
  BEGIN
    SELECT COUNT(A.ID) INTO NCOUNT FROM IFR.T_IFR_CWS_DEST_DOWNLOAD A WHERE A.ISSENDFLAG IN ('0', '1'); --'接口处理状态接口处理状态，0待处理；1正在处理；2已处理；3处理失败；默认值为“0”'
  
    IF NCOUNT > 0 THEN
      BEGIN
        -- I := 1; --数组下标初始化
        UPDATE IFR.T_IFR_CWS_DEST_DOWNLOAD A
        SET    A.ISSENDFLAG = '1' --1：正在传输
        WHERE  A.ISSENDFLAG = '0'; --0:待处理
        COMMIT; --提交后方便出现异常后和业务表比对异常数据,比较业务表更新为2状态但是接口表因为回滚而不存在的数据就是异常数据
        --必须每条的传输，并且某条出现异常则记录日志然后进行一下条数据的传输。
      
        UPDATE IFR.T_IFR_CWS_DEST_DOWNLOAD A
        SET    A.ISSENDFLAG        = '3', --传输失败
               A.SEND_REMARK       = '在业务表没有匹配到数据',
               A.LAST_UPDATED_DATE = SYSDATE
        WHERE  A.DESTINATIONID not in (select B.DEST_DW_LOGID from T_CWS_BU_DEST_DOWNLOAD_INFO B);
        COMMIT;
      
        FOR J1 IN (SELECT *
                   FROM   IFR.T_IFR_CWS_DEST_DOWNLOAD A
                   WHERE  A.ISSENDFLAG = '1' --1：正在传输
                   ORDER  BY A.CREATED_DATE ASC)
        LOOP
          BEGIN
            EXPLAINTYPENO := J1.ID; --
          
            UPDATE T_CWS_BU_DEST_DOWNLOAD_INFO B
            SET    B.DA_REQEST_DATE = J1.VISITTIME / (60 * 60 * 24) + TO_DATE('1970-01-01 08:00:00', 'YYYY-MM-DD HH24:MI:SS'), --车载机访问时间
                   
                   B.LAST_UPDATED_DATE = SYSTIMESTAMP
            
            WHERE  B.DEST_DW_LOGID = J1.DESTINATIONID;
          
            UPDATE IFR.T_IFR_CWS_DEST_DOWNLOAD A
            SET    A.ISSENDFLAG        = '2', --2：传输成功
                   A.SENDTIME          = TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS'),
                   A.LAST_UPDATED_DATE = SYSDATE
            WHERE  A.ID = J1.ID AND A.ISSENDFLAG = '1'; -- 1 处理中
          
            COMMIT; --操作完整一条数据（包括更新或者插入成功然后并把传输状态改为传输成功状态才算完整操作一条记录）就commit，一旦出现异常则只回滚那一条异常的数据
            --COURSES(I) := J1.ID; --将接口表ID--INTF_ID加入数组
            -- I := I + 1; --数组下标+1
          EXCEPTION
            WHEN OTHERS THEN
              ROLLBACK; --回滚出现异常的某条记录，因为操作完整的一条数据包括2步，所以必须回滚，保证异常了不出出现在业务表中
              V_SQLERRM := SUBSTR(SQLERRM, 0, 1000);
              UPDATE IFR.T_IFR_CWS_DEST_DOWNLOAD A
              SET    A.ISSENDFLAG  = '3', -- 3 异常
                     A.SEND_REMARK = V_SQLERRM
              WHERE  A.ID = EXPLAINTYPENO AND A.ISSENDFLAG = '1';
              COMMIT; --异常数据的操作，下面是写错误日志，所以分别作自己的事务提交
              PKG_COC_COMMON.ADDOPERATIONLOG('P_T_CWS_BU_DEST_DOWNLOAD_INFO', '接口表_推送给08IT安防_目的地下载记录（异步）', V_SQLERRM, '', '');
            
              COMMIT;
          END;
        END LOOP; --异常处理必须放在循环里面，保证异常之后不退出循环直接进行下步的处理，符合业务要求的逻辑
      
      END;
    END IF;
  
  END P_T_CWS_BU_DEST_DOWNLOAD_INFO;

  /*  
  IFR.T_IFR_CWS_CARPOSITION_INFO
  T_CWS_BU_CARPOSITION_INFO
  */

  /*接口表_接收08IT安防_定位请求日志 李征 2017-11-8*/
  PROCEDURE P_T_CWS_BU_CARPOSITION_INFO AS
    NCOUNT        NUMBER;
    EXPLAINTYPENO VARCHAR2(50);
    --V_SQLERRM     VARCHAR2(1000);
  BEGIN
    SELECT COUNT(A.ID) INTO NCOUNT FROM IFR.T_IFR_CWS_CARPOSITION_INFO A WHERE A.SEND_FLAG IN ('0', '1'); --'接口处理状态接口处理状态，0待处理；1正在处理；2已处理；3处理失败；默认值为“0”'
  
    IF NCOUNT > 0 THEN
      BEGIN
        -- I := 1; --数组下标初始化
        UPDATE IFR.T_IFR_CWS_CARPOSITION_INFO A
        SET    A.SEND_FLAG = '1' --1：正在传输
        WHERE  A.SEND_FLAG = '0'; --0:待处理
        COMMIT; --提交后方便出现异常后和业务表比对异常数据,比较业务表更新为2状态但是接口表因为回滚而不存在的数据就是异常数据
        --必须每条的传输，并且某条出现异常则记录日志然后进行一下条数据的传输。
      
        UPDATE IFR.T_IFR_CWS_CARPOSITION_INFO A
        SET    A.SEND_FLAG         = '3', --传输失败
               A.SEND_REMARK       = '在业务表没有匹配到数据',
               A.LAST_UPDATED_DATE = SYSDATE
        WHERE  A.REQID not in (select B.REQUEST_ID from T_CWS_BU_CARPOSITION_INFO B);
        COMMIT;
      
        FOR J1 IN (SELECT *
                   FROM   IFR.T_IFR_CWS_CARPOSITION_INFO A
                   WHERE  A.SEND_FLAG = '1' --1：正在传输
                   ORDER  BY A.CREATED_DATE ASC)
        LOOP
          BEGIN
            EXPLAINTYPENO := J1.ID; --
          
            UPDATE T_CWS_BU_CARPOSITION_INFO B
            SET    B.CAR_FORHEAD     = J1.FRONTDIRECTION,
                   B.GPS_TIME        = J1.GPSDATE / (60 * 60 * 24) + TO_DATE('1970-01-01 08:00:00', 'YYYY-MM-DD HH24:MI:SS'),
                   B.CAR_POSITION    = J1.CARLOCATION,
                   B.E_OR_W          = J1.LONGITUDE,
                   B.S_OR_N          = J1.LATITUDE,
                   B.TCU_VEHNET_TIME = J1.RECTCUTIME / (60 * 60 * 24) + TO_DATE('1970-01-01 08:00:00', 'YYYY-MM-DD HH24:MI:SS'),
                   B.TO_TCU_TIME     = J1.SENDTCUTIME / (60 * 60 * 24) + TO_DATE('1970-01-01 08:00:00', 'YYYY-MM-DD HH24:MI:SS'),
                   B.TCURESULTCODE   = J1.TCURESULTCODE,
                   B.TCURESULTNAME   = J1.TCURESULTNAME,
                   
                   B.LAST_UPDATED_DATE = SYSTIMESTAMP
            
            WHERE  B.REQUEST_ID = J1.REQID;
          
            UPDATE IFR.T_IFR_CWS_CARPOSITION_INFO A
            SET    A.SEND_FLAG         = '2', --2：传输成功
                   A.SEND_DATE         = SYSDATE,
                   A.LAST_UPDATED_DATE = SYSDATE
            WHERE  A.ID = J1.ID AND A.SEND_FLAG = '1'; -- 1 处理中
          
            COMMIT; --操作完整一条数据（包括更新或者插入成功然后并把传输状态改为传输成功状态才算完整操作一条记录）就commit，一旦出现异常则只回滚那一条异常的数据
            --COURSES(I) := J1.ID; --将接口表ID--INTF_ID加入数组
            -- I := I + 1; --数组下标+1
          EXCEPTION
            WHEN OTHERS THEN
              ROLLBACK; --回滚出现异常的某条记录，因为操作完整的一条数据包括2步，所以必须回滚，保证异常了不出出现在业务表中
              V_SQLERRM := SUBSTR(SQLERRM, 0, 1000);
              UPDATE IFR.T_IFR_CWS_CARPOSITION_INFO A
              SET    A.SEND_FLAG   = '3', -- 3 异常
                     A.SEND_REMARK = V_SQLERRM
              WHERE  A.ID = EXPLAINTYPENO AND A.SEND_FLAG = '1';
              COMMIT; --异常数据的操作，下面是写错误日志，所以分别作自己的事务提交
              PKG_COC_COMMON.ADDOPERATIONLOG('P_T_CWS_BU_CARPOSITION_INFO', '接口表_接收08IT安防_定位请求日志', V_SQLERRM, '', '');
            
              COMMIT;
          END;
        END LOOP; --异常处理必须放在循环里面，保证异常之后不退出循环直接进行下步的处理，符合业务要求的逻辑
      
      END;
    END IF;
  
  END P_T_CWS_BU_CARPOSITION_INFO;

  /*
  IFR.T_IFR_CWS_CARTRACKING_DETAIL
  T_CWS_BU_CARTRACKING_DETAIL
  */

  /*接口表_追踪请求从表 李征 2017-11-8*/
  PROCEDURE P_T_CWS_BU_CARTRACKING_DETAIL AS
    NCOUNT        NUMBER;
    EXPLAINTYPENO VARCHAR2(50);
    --V_SQLERRM     VARCHAR2(1000);
  BEGIN
    SELECT COUNT(A.ID) INTO NCOUNT FROM IFR.T_IFR_CWS_CARTRACKING_DETAIL A WHERE A.SEND_FLAG IN ('0', '1'); --'接口处理状态接口处理状态，0待处理；1正在处理；2已处理；3处理失败；默认值为“0”'
  
    IF NCOUNT > 0 THEN
      BEGIN
        -- I := 1; --数组下标初始化
        UPDATE IFR.T_IFR_CWS_CARTRACKING_DETAIL A
        SET    A.SEND_FLAG = '1' --1：正在传输
        WHERE  A.SEND_FLAG = '0'; --0:待处理
        COMMIT; --提交后方便出现异常后和业务表比对异常数据,比较业务表更新为2状态但是接口表因为回滚而不存在的数据就是异常数据
        --必须每条的传输，并且某条出现异常则记录日志然后进行一下条数据的传输。
      
        /*     
        逻辑更换
         UPDATE IFR.T_IFR_CWS_CARTRACKING_DETAIL A
                 SET A.SEND_FLAG         = '3', --传输失败
                     A.SEND_REMARK       = '在业务表没有匹配到数据',
                     A.LAST_UPDATED_DATE = SYSDATE
               WHERE A.RESID not in
                     (select B.REQUEST_RESULT_ID
                        from T_CWS_BU_CARTRACKING_DETAIL B);
              COMMIT;*/
      
        FOR J1 IN (SELECT *
                   FROM   IFR.T_IFR_CWS_CARTRACKING_DETAIL A
                   WHERE  A.SEND_FLAG = '1' --1：正在传输
                   ORDER  BY A.CREATED_DATE ASC)
        LOOP
          BEGIN
            EXPLAINTYPENO := J1.ID; --
          
            MERGE INTO T_CWS_BU_CARTRACKING_DETAIL B
            USING (SELECT 1 FROM DUAL) I --不再重复透过DBLINK从接口表
            ON (B.REQUEST_RESULT_ID = J1.RESID) --操作业务表所以以业务关键字来做匹配条件
            WHEN MATCHED THEN
            
              UPDATE --T_CWS_BU_CARTRACKING_DETAIL B
              SET    B.CAR_FORHEAD  = J1.FRONTDIRECTION,
                     B.GPS_TIME     = J1.GPSDATE / (60 * 60 * 24) + TO_DATE('1970-01-01 08:00:00', 'YYYY-MM-DD HH24:MI:SS'),
                     B.CAR_POSITION = J1.CARLOCATION,
                     B.E_OR_W       = J1.LONGITUDE,
                     B.S_OR_N       = J1.LATITUDE,
                     B.CREATE_TIME  = J1.CREATETIME / (60 * 60 * 24) + TO_DATE('1970-01-01 08:00:00', 'YYYY-MM-DD HH24:MI:SS'),
                     
                     B.REQUEST_ID  = J1.REQID,
                     B.VIN         = J1.VIN,
                     B.SENDTCUTIME = J1.SENDTCUTIME / (60 * 60 * 24) + TO_DATE('1970-01-01 08:00:00', 'YYYY-MM-DD HH24:MI:SS'),
                     
                     B.TCURESULTCODE = J1.TCURESULTCODE,
                     B.TCURESULTNAME = J1.TCURESULTNAME,
                     
                     B.LAST_UPDATED_DATE = SYSTIMESTAMP
              
              WHERE  B.REQUEST_RESULT_ID = J1.RESID
              
            
            WHEN NOT MATCHED THEN
              INSERT
                (B.REQUEST_RESULT_ID,
                 B.CAR_FORHEAD,
                 B.GPS_TIME,
                 B.CAR_POSITION,
                 B.E_OR_W,
                 B.S_OR_N,
                 B.CREATE_TIME,
                 B.REQUEST_ID,
                 B.VIN,
                 
                 B.SENDTCUTIME,
                 B.TCURESULTCODE,
                 B.TCURESULTNAME
                 
                 )
              VALUES
                (J1.RESID,
                 J1.FRONTDIRECTION,
                 J1.GPSDATE / (60 * 60 * 24) + TO_DATE('1970-01-01 08:00:00', 'YYYY-MM-DD HH24:MI:SS'),
                 J1.CARLOCATION,
                 J1.LONGITUDE， J1.LATITUDE,
                 J1.CREATETIME / (60 * 60 * 24) + TO_DATE('1970-01-01 08:00:00', 'YYYY-MM-DD HH24:MI:SS'),
                 J1.REQID,
                 J1.VIN,
                 J1.SENDTCUTIME,
                 J1.TCURESULTCODE,
                 J1.TCURESULTNAME
                 
                 );
          
            UPDATE IFR.T_IFR_CWS_CARTRACKING_DETAIL A
            SET    A.SEND_FLAG         = '2', --2：传输成功
                   A.SEND_DATE         = SYSDATE,
                   A.LAST_UPDATED_DATE = SYSDATE
            WHERE  A.ID = J1.ID AND A.SEND_FLAG = '1'; -- 1 处理中
          
            COMMIT; --操作完整一条数据（包括更新或者插入成功然后并把传输状态改为传输成功状态才算完整操作一条记录）就commit，一旦出现异常则只回滚那一条异常的数据
            --COURSES(I) := J1.ID; --将接口表ID--INTF_ID加入数组
            -- I := I + 1; --数组下标+1
          EXCEPTION
            WHEN OTHERS THEN
              ROLLBACK; --回滚出现异常的某条记录，因为操作完整的一条数据包括2步，所以必须回滚，保证异常了不出出现在业务表中
              V_SQLERRM := SUBSTR(SQLERRM, 0, 1000);
              UPDATE IFR.T_IFR_CWS_CARTRACKING_DETAIL A
              SET    A.SEND_FLAG   = '3', -- 3 异常
                     A.SEND_REMARK = V_SQLERRM
              WHERE  A.ID = EXPLAINTYPENO AND A.SEND_FLAG = '1';
              COMMIT; --异常数据的操作，下面是写错误日志，所以分别作自己的事务提交
              PKG_COC_COMMON.ADDOPERATIONLOG('P_T_CWS_BU_CARTRACKING_DETAIL', '接口表_追踪请求从表', V_SQLERRM, '', '');
            
              COMMIT;
          END;
        END LOOP; --异常处理必须放在循环里面，保证异常之后不退出循环直接进行下步的处理，符合业务要求的逻辑
      
      END;
    END IF;
  
  END P_T_CWS_BU_CARTRACKING_DETAIL;

  /*
  IFR.T_IFR_CWS_VEHNET_VISIT_STATICS
  T_CWS_BU_VEHNET_VISIT_STATICS
  
  COOK
  */
  /*接口表_08IT服务访问日统计 李征 2017-11-9*/
  PROCEDURE P_T_CWS_BU_VEHNET_VISIT_S AS
    NCOUNT        NUMBER;
    EXPLAINTYPENO VARCHAR2(50);
    --V_SQLERRM     VARCHAR2(1000);
  BEGIN
    SELECT COUNT(A.DATE_STATICS_ID) INTO NCOUNT FROM IFR.T_IFR_CWS_VEHNET_VISIT_STATICS A WHERE A.SEND_FLAG IN ('0', '1'); --'接口处理状态接口处理状态，0待处理；1正在处理；2已处理；3处理失败；默认值为“0”'
  
    IF NCOUNT > 0 THEN
      BEGIN
        -- I := 1; --数组下标初始化
        UPDATE IFR.T_IFR_CWS_VEHNET_VISIT_STATICS A
        SET    A.SEND_FLAG = '1' --1：正在传输
        WHERE  A.SEND_FLAG = '0'; --0:待处理
        COMMIT; --提交后方便出现异常后和业务表比对异常数据,比较业务表更新为2状态但是接口表因为回滚而不存在的数据就是异常数据
        --必须每条的传输，并且某条出现异常则记录日志然后进行一下条数据的传输。
      
        FOR J1 IN (SELECT *
                   FROM   IFR.T_IFR_CWS_VEHNET_VISIT_STATICS A
                   WHERE  A.SEND_FLAG = '1' --1：正在传输
                   ORDER  BY A.CREATED_DATE ASC)
        LOOP
          BEGIN
            EXPLAINTYPENO := J1.DATE_STATICS_ID; --
          
            MERGE INTO T_CWS_BU_VEHNET_VISIT_STATICS B
            USING (SELECT 1 FROM DUAL) I --不再重复透过DBLINK从接口表
            ON (B.REQUEST_DATE = to_date(J1.VISITDATE, 'yyyy-mm-dd') and B.VIN = J1.VIN and B.VEHNET_SERVICE_CODE = J1.SERVICECODE) --操作业务表所以以业务关键字来做匹配条件
            WHEN MATCHED THEN
            
              UPDATE
              SET    B.QEQUEST_COUNT = J1.VISITNUMS,
                     
                     B.LAST_UPDATED_DATE = J1.LAST_UPDATED_DATE
              WHERE  B.REQUEST_DATE = to_date(J1.VISITDATE, 'yyyy-mm-dd') and B.VIN = J1.VIN and B.VEHNET_SERVICE_CODE = J1.SERVICECODE
              
            
            WHEN NOT MATCHED THEN
              INSERT --INTO  T_CWS_BU_VEHNET_VISIT_STATICS 
                (DATE_STATICS_ID, REQUEST_DATE, VIN, VEHNET_SERVICE_CODE, VEHNET_SERVICE_NAME, QEQUEST_COUNT)
              VALUES
                (J1.DATE_STATICS_ID, to_date(J1.VISITDATE, 'yyyy-mm-dd'), J1.VIN, J1.SERVICECODE, J1.SERVICENAME, J1.VISITNUMS);
          
            UPDATE IFR.T_IFR_CWS_VEHNET_VISIT_STATICS A
            SET    A.SEND_FLAG         = '2', --2：传输成功
                   A.SEND_DATE         = SYSDATE,
                   A.LAST_UPDATED_DATE = SYSDATE
            WHERE  A.DATE_STATICS_ID = J1.DATE_STATICS_ID AND A.SEND_FLAG = '1'; -- 1 处理中
          
            COMMIT; --操作完整一条数据（包括更新或者插入成功然后并把传输状态改为传输成功状态才算完整操作一条记录）就commit，一旦出现异常则只回滚那一条异常的数据
            --COURSES(I) := J1.ID; --将接口表ID--INTF_ID加入数组
            -- I := I + 1; --数组下标+1
          EXCEPTION
            WHEN OTHERS THEN
              ROLLBACK; --回滚出现异常的某条记录，因为操作完整的一条数据包括2步，所以必须回滚，保证异常了不出出现在业务表中
              V_SQLERRM := SUBSTR(SQLERRM, 0, 1000);
              UPDATE IFR.T_IFR_CWS_VEHNET_VISIT_STATICS A
              SET    A.SEND_FLAG   = '3', -- 3 异常
                     A.SEND_REMARK = V_SQLERRM
              WHERE  A.DATE_STATICS_ID = EXPLAINTYPENO AND A.SEND_FLAG = '1';
              COMMIT; --异常数据的操作，下面是写错误日志，所以分别作自己的事务提交
              PKG_COC_COMMON.ADDOPERATIONLOG('P_T_CWS_BU_VEHNET_VISIT_S', '接口表_08IT服务访问日统计', V_SQLERRM, '', '');
            
              COMMIT;
          END;
        END LOOP; --异常处理必须放在循环里面，保证异常之后不退出循环直接进行下步的处理，符合业务要求的逻辑
      
      END;
    END IF;
  
  END P_T_CWS_BU_VEHNET_VISIT_S;

  /*
  IFS.T_IFS_CWS_SERV_PKG_BUY
  T_CWS_BU_BASIC_SERV_BUY
  T_CWS_BU_ADDED_SERV_BUY
  
  COOK
  */

  /*接口表_推送给08IT安防_服务购买套餐明细信息 李征 2017-11-13*/
  PROCEDURE P_T_IFS_CWS_SERV_PKG_BUY AS
  
    --V_SQLERRM     VARCHAR2(1000);
  BEGIN
  
    update T_CWS_BU_BASIC_SERV_BUY A SET A.SEND_FLAG = '1' WHERE A.SEND_FLAG = '0';
    COMMIT;
  
    update T_CWS_BU_ADDED_SERV_BUY A SET A.SEND_FLAG = '1' WHERE A.SEND_FLAG = '0';
    COMMIT;
    INSERT INTO IFS.T_IFS_CWS_SERV_PKG_BUY
      (VIN, SERVICE_TYPE_CODE, SERVICE_TYPE_NAME, SERVICE_CODE, SERVICE_NAME, GUIDE_PRICE, YEARS, SERVICE_BEGIN_DATE, SERVICE_END_DATE, STATUS)
      select VIN,
             SERVICE_TYPE_CODE,
             SERVICE_TYPE_NAME,
             SERVICE_CODE,
             SERVICE_NAME,
             GUIDE_PRICE,
             YEARS,
             to_char(SERVICE_BEGIN_DATE, 'yyyy-mm-dd'),
             to_char(SERVICE_END_DATE, 'yyyy-mm-dd'),
             A.SERVICE_STATUS
      
      from   T_CWS_BU_BASIC_SERV_BUY A
      WHERE  A.SEND_FLAG = '1' AND A.SERVICE_STATUS IS NOT NULL;
  
    update T_CWS_BU_BASIC_SERV_BUY A SET A.SEND_FLAG = '2' WHERE A.SEND_FLAG = '1';
    COMMIT;
  
    INSERT INTO IFS.T_IFS_CWS_SERV_PKG_BUY
      (VIN, SERVICE_TYPE_CODE, SERVICE_TYPE_NAME, SERVICE_CODE, SERVICE_NAME, GUIDE_PRICE, YEARS, SERVICE_BEGIN_DATE, SERVICE_END_DATE, STATUS)
      select VIN, SERVICE_TYPE_CODE, SERVICE_TYPE_NAME, SERVICE_CODE, SERVICE_NAME, GUIDE_PRICE, YEARS, SERVICE_BEGIN_DATE, SERVICE_END_DATE, A.SERVICE_STATUS
      
      FROM   T_CWS_BU_ADDED_SERV_BUY A
      WHERE  A.SEND_FLAG = '1';
  
    UPDATE T_CWS_BU_ADDED_SERV_BUY A SET A.SEND_FLAG = '2' WHERE A.SEND_FLAG = '1';
    COMMIT;
  
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK; --回滚出现异常的某条记录，因为操作完整的一条数据包括2步，所以必须回滚，保证异常了不出出现在业务表中
      V_SQLERRM := SUBSTR(SQLERRM, 0, 1000);
      PKG_COC_COMMON.ADDOPERATIONLOG('P_T_IFS_CWS_SERV_PKG_BUY', '接口表_推送给08IT安防_服务购买套餐明细信息', V_SQLERRM, '', '');
  END P_T_IFS_CWS_SERV_PKG_BUY;

  /*T_IFS_CWS_CUST_SERVICE_INFO
  T_CWS_DB_CAR_MACH_INFO
  T_CWS_DB_MEM_SERVICE_INFO
  
  COOK
  */

  /*接口表_推送给08IT安防_车辆客户服务信息  李征 2017-11-13*/
  PROCEDURE P_T_IFS_CWS_CUST_SERVICE_INFO AS
  
  BEGIN
    update T_CWS_DB_MEM_SERVICE_INFO A SET A.SEND_FLAG = '1' WHERE A.SEND_FLAG = '0';
    COMMIT;
    update t_cws_db_car_mach_info A SET A.SEND_FLAG = '1' WHERE A.SEND_FLAG = '0';
    COMMIT;
    insert into IFS.T_IFS_CWS_CUST_SERVICE_INFO
      (VIN,
       NAVID,
       TCUID,
       ICCID,
       MSN,
       CUST_CODE,
       CUST_NAME,
       CUST_TYPE,
       PHONE,
       FIRST_CONTACT_PERSON,
       FIRST_CONTACT_TEL,
       FIRST_CONTACT_REALATION,
       SEC_CONTACT_PERSON,
       SEC_CONTACT_TEL,
       SEC_CONTACT_REALATION,
       THD_CONTACT_PERSON,
       THD_CONTACT_TEL,
       THD_CONTACT_REALATION,
       BASIC_SERVICE_CODE,
       BASIC_SERVICE_NAME,
       BASIC_SERVICE_YEAR,
       BASIC_SERVICE_BEGIN,
       BASIC_SERVICE_END,
       IS_MEMBER_ACCOUNT,
       IS_DYN_TRAFIC_SERV,
       CARSERIESSN,
       CNAME,
       ENAME,
       ANSWER18CARTYPE,
       CARNO,
       CUST_SEX,
       DCM_VERSION,
       NAVI_VERSION
       
       )
      SELECT B.VIN,
             B.NAVI_ID,
             B.DCM_ID,
             B.SIM_ICCID,
             A.MSN,
             A.CUST_CODE,
             A.CUST_NAME,
             A.CUST_TYPE,
             A.PHONE,
             A.CONTACTOR1,
             A.CONTACTOR1_TEL,
             A.RELATIONSHIP1,
             A.CONTACTOR2,
             A.CONTACTOR2_TEL,
             A.RELATIONSHIP2,
             A.CONTACTOR3,
             A.CONTACTOR3_TEL,
             A.RELATIONSHIP3,
             A.BASIC_SERVICE_CODE,
             A.BASIC_SERVICE_NAME,
             A.BASIC_SERVICE_YEAR,
             TO_CHAR(A.BASIC_SERVICE_BEGIN, 'YYYY-MM-DD'),
             TO_CHAR(A.BASIC_SERVICE_END, 'YYYY-MM-DD'),
             A.MEM_ACCOUNT,
             A.IS_DYN_TRAFIC_SERV,
             A.CAR_SERIES_CODE,
             A.CAR_SERIES_CN,
             A.CAR_SERIES_EN,
             A.CAR_CONFIG_CODE,
             A.CAR_NO,
             DECODE(A.GENDER, '未知', '', A.GENDER),
             B.DCM_VERSION,
             B.NAVI_VERSION
      FROM   T_CWS_DB_CAR_MACH_INFO B
      LEFT   JOIN T_CWS_DB_MEM_SERVICE_INFO A
      ON     B.VIN = A.VIN
      where  b.SEND_FLAG = '1' or A.SEND_FLAG = '1';
  
    update T_CWS_DB_MEM_SERVICE_INFO A SET A.SEND_FLAG = '2' WHERE A.SEND_FLAG = '1';
    COMMIT;
    update t_cws_db_car_mach_info A SET A.SEND_FLAG = '2' WHERE A.SEND_FLAG = '1';
    COMMIT;
  
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK; --回滚出现异常的某条记录，因为操作完整的一条数据包括2步，所以必须回滚，保证异常了不出出现在业务表中
      V_SQLERRM := SUBSTR(SQLERRM, 0, 1000);
      PKG_COC_COMMON.ADDOPERATIONLOG('P_T_IFS_CWS_CUST_SERVICE_INFO', '接口表_推送给08IT安防_车辆客户服务信息', V_SQLERRM, '', '');
    
  END P_T_IFS_CWS_CUST_SERVICE_INFO;

  /*
  T_CWS_BU_SMS_SEND
  
  IFR.T_IFR_CWS_SMS_SEND
  */

  /*接口表_接收车辆网短信发送情况 李征 2017-11-14*/
  PROCEDURE P_T_CWS_BU_SMS_SEND AS
    NCOUNT        NUMBER;
    EXPLAINTYPENO VARCHAR2(50);
    --V_SQLERRM     VARCHAR2(1000);
  BEGIN
    SELECT COUNT(A.TABLEID) INTO NCOUNT FROM IFR.T_IFR_CWS_SMS_SEND A WHERE A.SEND_FLAG IN ('0', '1'); --'接口处理状态接口处理状态，0待处理；1正在处理；2已处理；3处理失败；默认值为“0”'
  
    IF NCOUNT > 0 THEN
      BEGIN
        -- I := 1; --数组下标初始化
        UPDATE IFR.T_IFR_CWS_SMS_SEND A
        SET    A.SEND_FLAG = '1' --1：正在传输
        WHERE  A.SEND_FLAG = '0'; --0:待处理
        COMMIT; --提交后方便出现异常后和业务表比对异常数据,比较业务表更新为2状态但是接口表因为回滚而不存在的数据就是异常数据
        --必须每条的传输，并且某条出现异常则记录日志然后进行一下条数据的传输。
      
        FOR J1 IN (SELECT *
                   FROM   IFR.T_IFR_CWS_SMS_SEND A
                   WHERE  A.SEND_FLAG = '1' --1：正在传输
                   ORDER  BY A.CREATED_DATE ASC)
        LOOP
          BEGIN
            EXPLAINTYPENO := J1.TABLEID; --
          
            MERGE INTO T_CWS_BU_SMS_SEND B
            USING (SELECT 1 FROM DUAL) I --不再重复透过DBLINK从接口表
            ON (B.ID = J1.ID) --操作业务表所以以业务关键字来做匹配条件
            WHEN MATCHED THEN
            
              UPDATE
              SET    B.SOURCE_BILLNO = J1.SEQUENCEID,
                     
                     B.VIN             = J1.VINNUMBER,
                     B.SMSDATA         = convert((utl_raw.cast_to_varchar2(utl_encode.base64_decode(utl_raw.cast_to_raw(J1.SMSDATA)))), 'ZHS16GBK', 'UTF8'）,
                     B.PHONE           = J1.CALLTONUM,
                     B.SEND_TIME       = to_date(J1.SENDTIME, 'yyyy-mm-dd hh24:mi:ss'),
                     B.SERVICE_TYPE    = J1.SERVICETYPE,
                     --车联网发送状态2成功，3失败，1未知，翻译为carwings的短信状态1 成功，0失败
                     B.SEND_STATUS     = decode(J1.SENDSTATUS,'2','1','3','0',null),
                     B.DESCRIPTION     = J1.DESCRIPTION,
                     B.SMS_STATUS_TIME = to_date(J1.SMSSTATUSTIME, 'yyyy-mm-dd hh24:mi:ss'),
                 
                     
                     B.LAST_UPDATED_DATE = SYSDATE
              WHERE  B.ID = J1.ID
              
            
            WHEN NOT MATCHED THEN
              INSERT  
                (ID,
                 SOURCE_BILLNO,
                 VIN,
                 SMSDATA,
                 PHONE,
                 SEND_TIME,
                 SERVICE_TYPE,
                 SEND_STATUS,
                 DESCRIPTION,
                 SMS_STATUS_TIME,
                 SEND_MODEULE
                 
                 )
              VALUES
                (J1.ID,
                 J1.SEQUENCEID,
                 J1.VINNUMBER,
                 convert((utl_raw.cast_to_varchar2(utl_encode.base64_decode(utl_raw.cast_to_raw(J1.SMSDATA)))), 'ZHS16GBK', 'UTF8'）,
                 J1.CALLTONUM,
                 to_date(J1.SENDTIME, 'yyyy-mm-dd hh24:mi:ss'),
                 J1.SERVICETYPE,
                 --车联网发送状态2成功，3失败，1未知，翻译为carwings的短信状态1 成功，0失败
                decode(J1.SENDSTATUS,'2','1','3','0',null),
                 J1.DESCRIPTION,
                 to_date(J1.SMSSTATUSTIME, 'yyyy-mm-dd hh24:mi:ss'),
                 
                 'CGUARD'
                 
                 );
          
            UPDATE IFR.T_IFR_CWS_SMS_SEND A
            SET    A.SEND_FLAG         = '2', --2：传输成功
                   A.SEND_DATE         = SYSDATE,
                   A.LAST_UPDATED_DATE = SYSDATE
            WHERE  A.ID = J1.ID AND A.SEND_FLAG = '1'; -- 1 处理中
          
            COMMIT; --操作完整一条数据（包括更新或者插入成功然后并把传输状态改为传输成功状态才算完整操作一条记录）就commit，一旦出现异常则只回滚那一条异常的数据
            --COURSES(I) := J1.ID; --将接口表ID--INTF_ID加入数组
            -- I := I + 1; --数组下标+1
          EXCEPTION
            WHEN OTHERS THEN
              ROLLBACK; --回滚出现异常的某条记录，因为操作完整的一条数据包括2步，所以必须回滚，保证异常了不出出现在业务表中
              V_SQLERRM := SUBSTR(SQLERRM, 0, 1000);
              UPDATE IFR.T_IFR_CWS_SMS_SEND A
              SET    A.SEND_FLAG   = '3', -- 3 异常
                     A.SEND_REMARK = V_SQLERRM
              WHERE  A.TABLEID = EXPLAINTYPENO AND A.SEND_FLAG = '1';
              COMMIT; --异常数据的操作，下面是写错误日志，所以分别作自己的事务提交
              PKG_COC_COMMON.ADDOPERATIONLOG('P_T_CWS_BU_SMS_SEND', '接口表_接收车辆网短信发送情况 ', V_SQLERRM, '', '');
            
              COMMIT;
          END;
        END LOOP; --异常处理必须放在循环里面，保证异常之后不退出循环直接进行下步的处理，符合业务要求的逻辑
      
      END;
    END IF;
  
  END P_T_CWS_BU_SMS_SEND;

  /*******************************************************************************************
  *   版本:        1.00
  *   作者：       LIZHENG
  *   建立日期：   2017-11-20
  *   主要功能:    根据服务类别编码查询服务类别名称
                   传入参数：服务类别编码 P_SERVICE_TYPE_CODE
                   返回结果：服务类别名称
  *   修改日期：
  *   修改说明：
  ********************************************************************************************/

  FUNCTION FN_GET_SERVICETYPENAME_BY_CODE(F_SERVICE_TYPE_CODE in VARCHAR2) RETURN VARCHAR2 IS
  
    F_SERVICE_TYPE_NAME VARCHAR2(2000) := '';
  BEGIN
    select T.SERVICE_TYPE_NAME into F_SERVICE_TYPE_NAME from cws.t_cws_db_carwings_serv_type t WHERE T.SERVICE_TYPE_CODE = F_SERVICE_TYPE_CODE;
    return F_SERVICE_TYPE_NAME;
  END FN_GET_SERVICETYPENAME_BY_CODE;



  /*******************************************************************************************
  *   版本:        1.00
  *   作者：       denghq
  *   建立日期：   2017-11-20
  *   主要功能:    根据村存储过程名跳转到对应的存储过程
                   传入参数：存储过程名 PROCEDURENAME
                   返回结果：bool
  *   修改日期：
  *   修改说明：
  ********************************************************************************************/


 FUNCTION FN_GET_PROCEDURENAME(PROCEDURENAME in VARCHAR2) RETURN VARCHAR2 IS
  
    RESULTBOOL VARCHAR2(2000) := 'true';
  BEGIN
  IF PROCEDURENAME='P_T_IFS_CWS_CGUARD_INFO' THEN
     P_T_IFS_CWS_CGUARD_INFO;
  ELSIF  PROCEDURENAME='P_T_IFS_CWS_CARPOSITION_INFO' THEN
       P_T_IFS_CWS_CARPOSITION_INFO;
  ELSIF  PROCEDURENAME='P_T_IFS_CWS_DEST_DOWNLOAD_INFO' THEN
       P_T_IFS_CWS_DEST_DOWNLOAD_INFO;
  ELSIF  PROCEDURENAME='P_T_IFS_CWS_CARTRACKING_MAIN' THEN
       P_T_IFS_CWS_CARTRACKING_MAIN;
  ELSIF  PROCEDURENAME='P_T_CWS_BU_CGUARD_INFO' THEN
       P_T_CWS_BU_CGUARD_INFO;
  ELSIF  PROCEDURENAME='P_T_CWS_BU_DEST_DOWNLOAD_INFO' THEN
       P_T_CWS_BU_DEST_DOWNLOAD_INFO;
  ELSIF  PROCEDURENAME='P_T_CWS_BU_CARPOSITION_INFO' THEN
       P_T_CWS_BU_CARPOSITION_INFO;
  ELSIF  PROCEDURENAME='P_T_CWS_BU_CARTRACKING_DETAIL' THEN
       P_T_CWS_BU_CARTRACKING_DETAIL;
  ELSIF  PROCEDURENAME='P_T_CWS_BU_VEHNET_VISIT_S' THEN
       P_T_CWS_BU_VEHNET_VISIT_S;
  ELSIF  PROCEDURENAME='P_T_IFS_CWS_SERV_PKG_BUY' THEN
       P_T_IFS_CWS_SERV_PKG_BUY;
  ELSIF  PROCEDURENAME='P_T_IFS_CWS_CUST_SERVICE_INFO' THEN
       P_T_IFS_CWS_CUST_SERVICE_INFO;
  ELSIF PROCEDURENAME='P_T_CWS_BU_SMS_SEND' THEN
       P_T_CWS_BU_SMS_SEND;
  ELSE
       RESULTBOOL :='false';
  END IF;
  
  RETURN RESULTBOOL;
  
  END FN_GET_PROCEDURENAME;

end PKG_CIP_CWS_INTERFACE_CLW;
/
