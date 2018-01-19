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
  /*������¼��Ϣ��ͬ���� ���� 2017-11-10*/
  V_SQLERRM VARCHAR2(1000);
  PROCEDURE P_T_IFS_CWS_CGUARD_INFO AS
    NCOUNT        NUMBER;
    EXPLAINTYPENO VARCHAR2(50);
    --V_SQLERRM     VARCHAR2(1000);
  BEGIN
    SELECT COUNT(A.CGUARD_REQUEST_ID) INTO NCOUNT FROM T_CWS_BU_CGUARD_INFO A WHERE A.SEND_FLAG IN ('0', '1'); --'�ӿڴ���״̬�ӿڴ���״̬��0������1���ڴ���2�Ѵ���3����ʧ�ܣ�Ĭ��ֵΪ��0��'
  
    IF NCOUNT > 0 THEN
      BEGIN
      
        UPDATE T_CWS_BU_CGUARD_INFO A
        SET    A.SEND_FLAG = '1' --1�����ڴ���
        WHERE  A.SEND_FLAG = '0'; --0:������
        COMMIT; --�ύ�󷽱�����쳣���ҵ���ȶ��쳣����,�Ƚ�ҵ������Ϊ2״̬���ǽӿڱ���Ϊ�ع��������ڵ����ݾ����쳣����
        --����ÿ���Ĵ��䣬����ĳ�������쳣���¼��־Ȼ�����һ�������ݵĴ��䡣
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
            SET    A.SEND_FLAG         = '2', --2������ɹ�
                   A.LAST_UPDATED_DATE = SYSDATE
            WHERE  A.CGUARD_REQUEST_ID = J1.CGUARD_REQUEST_ID AND A.SEND_FLAG = '1'; -- 1 ������
          
            COMMIT; --��������һ�����ݣ��������»��߲���ɹ�Ȼ�󲢰Ѵ���״̬��Ϊ����ɹ�״̬������������һ����¼����commit��һ�������쳣��ֻ�ع���һ���쳣������
            --COURSES(I) := J1.ID; --���ӿڱ�ID--INTF_ID��������
            -- I := I + 1; --�����±�+1
          
          EXCEPTION
          
            WHEN OTHERS THEN
              ROLLBACK; --�ع������쳣��ĳ����¼����Ϊ����������һ�����ݰ���2�������Ա���ع�����֤�쳣�˲���������ҵ�����
              V_SQLERRM := SUBSTR(SQLERRM, 0, 1000);
              UPDATE T_CWS_BU_CGUARD_INFO A
              SET    A.SEND_FLAG = '3' -- 3 ����ʧ��
              
              WHERE  A.CGUARD_REQUEST_ID = EXPLAINTYPENO AND A.SEND_FLAG = '1';
              COMMIT; --�쳣���ݵĲ�����������д������־�����Էֱ����Լ��������ύ
              PKG_COC_COMMON.ADDOPERATIONLOG('T_IFS_CWS_CGUARD_INFO', '������¼��Ϣ��ͬ����', V_SQLERRM, '', '');
            
              COMMIT;
          END;
        END LOOP; --�쳣����������ѭ�����棬��֤�쳣֮���˳�ѭ��ֱ�ӽ����²��Ĵ�������ҵ��Ҫ����߼�
      
      END;
    END IF;
  
  END P_T_IFS_CWS_CGUARD_INFO;

  /*  
  IFS.T_IFS_CWS_CARPOSITION_INFO;
  T_CWS_BU_CARPOSITION_INFO;*/

  /*��λ������־��ͬ���� ���� 2017-11-10*/
  PROCEDURE P_T_IFS_CWS_CARPOSITION_INFO AS
    NCOUNT        NUMBER;
    EXPLAINTYPENO VARCHAR2(50);
    --V_SQLERRM     VARCHAR2(1000);
  BEGIN
    SELECT COUNT(A.REQUEST_ID) INTO NCOUNT FROM T_CWS_BU_CARPOSITION_INFO A WHERE A.SEND_FLAG IN ('0', '1'); --'�ӿڴ���״̬�ӿڴ���״̬��0������1���ڴ���2�Ѵ���3����ʧ�ܣ�Ĭ��ֵΪ��0��'
  
    IF NCOUNT > 0 THEN
      BEGIN
      
        UPDATE T_CWS_BU_CARPOSITION_INFO A
        SET    A.SEND_FLAG = '1' --1�����ڴ���
        WHERE  A.SEND_FLAG = '0'; --0:������
        COMMIT; --�ύ�󷽱�����쳣���ҵ���ȶ��쳣����,�Ƚ�ҵ������Ϊ2״̬���ǽӿڱ���Ϊ�ع��������ڵ����ݾ����쳣����
        --����ÿ���Ĵ��䣬����ĳ�������쳣���¼��־Ȼ�����һ�������ݵĴ��䡣
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
            SET    A.SEND_FLAG         = '2', --2������ɹ�
                   A.LAST_UPDATED_DATE = SYSDATE
            WHERE  A.REQUEST_ID = J1.REQUEST_ID AND A.SEND_FLAG = '1'; -- 1 ������
          
            COMMIT; --��������һ�����ݣ��������»��߲���ɹ�Ȼ�󲢰Ѵ���״̬��Ϊ����ɹ�״̬������������һ����¼����commit��һ�������쳣��ֻ�ع���һ���쳣������
            --COURSES(I) := J1.ID; --���ӿڱ�ID--INTF_ID��������
            -- I := I + 1; --�����±�+1
          EXCEPTION
            WHEN OTHERS THEN
              ROLLBACK; --�ع������쳣��ĳ����¼����Ϊ����������һ�����ݰ���2�������Ա���ع�����֤�쳣�˲���������ҵ�����
              V_SQLERRM := SUBSTR(SQLERRM, 0, 1000);
              UPDATE T_CWS_BU_CARPOSITION_INFO A
              SET    A.SEND_FLAG = '3' -- 3 ����ʧ��
              
              WHERE  A.REQUEST_ID = EXPLAINTYPENO AND A.SEND_FLAG = '1';
              COMMIT; --�쳣���ݵĲ�����������д������־�����Էֱ����Լ��������ύ
              PKG_COC_COMMON.ADDOPERATIONLOG('P_T_IFS_CWS_CARPOSITION_INFO', '��λ������־��ͬ����', V_SQLERRM, '', '');
            
              COMMIT;
          END;
        END LOOP; --�쳣����������ѭ�����棬��֤�쳣֮���˳�ѭ��ֱ�ӽ����²��Ĵ�������ҵ��Ҫ����߼�
      END;
    END IF;
  
  END P_T_IFS_CWS_CARPOSITION_INFO;

  /*
  IFS.T_IFS_CWS_DEST_DOWNLOAD_INFO;
  T_CWS_BU_DEST_DOWNLOAD_INFO;*/

  /*Ŀ�����ؼ�¼��ͬ���� ���� 2017-11-10*/
  PROCEDURE P_T_IFS_CWS_DEST_DOWNLOAD_INFO AS
    NCOUNT        NUMBER;
    EXPLAINTYPENO VARCHAR2(50);
    --V_SQLERRM     VARCHAR2(1000);
  BEGIN
    SELECT COUNT(A.DEST_DW_LOGID) INTO NCOUNT FROM T_CWS_BU_DEST_DOWNLOAD_INFO A WHERE A.SEND_FLAG IN ('0', '1'); --'�ӿڴ���״̬�ӿڴ���״̬��0������1���ڴ���2�Ѵ���3����ʧ�ܣ�Ĭ��ֵΪ��0��'
  
    IF NCOUNT > 0 THEN
      BEGIN
      
        UPDATE T_CWS_BU_DEST_DOWNLOAD_INFO A
        SET    A.SEND_FLAG = '1' --1�����ڴ���
        WHERE  A.SEND_FLAG = '0'; --0:������
        COMMIT; --�ύ�󷽱�����쳣���ҵ���ȶ��쳣����,�Ƚ�ҵ������Ϊ2״̬���ǽӿڱ���Ϊ�ع��������ڵ����ݾ����쳣����
        --����ÿ���Ĵ��䣬����ĳ�������쳣���¼��־Ȼ�����һ�������ݵĴ��䡣
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
            SET    A.SEND_FLAG         = '2', --2������ɹ�
                   A.LAST_UPDATED_DATE = SYSDATE
            WHERE  A.DEST_DW_LOGID = J1.DEST_DW_LOGID AND A.SEND_FLAG = '1'; -- 1 ������
          
            COMMIT; --��������һ�����ݣ��������»��߲���ɹ�Ȼ�󲢰Ѵ���״̬��Ϊ����ɹ�״̬������������һ����¼����commit��һ�������쳣��ֻ�ع���һ���쳣������
            --COURSES(I) := J1.ID; --���ӿڱ�ID--INTF_ID��������
            -- I := I + 1; --�����±�+1
          EXCEPTION
            WHEN OTHERS THEN
              ROLLBACK; --�ع������쳣��ĳ����¼����Ϊ����������һ�����ݰ���2�������Ա���ع�����֤�쳣�˲���������ҵ�����
              V_SQLERRM := SUBSTR(SQLERRM, 0, 1000);
              UPDATE T_CWS_BU_DEST_DOWNLOAD_INFO A
              SET    A.SEND_FLAG = '3' -- 3 ����ʧ��
              
              WHERE  A.DEST_DW_LOGID = EXPLAINTYPENO AND A.SEND_FLAG = '1';
              COMMIT; --�쳣���ݵĲ�����������д������־�����Էֱ����Լ��������ύ
              PKG_COC_COMMON.ADDOPERATIONLOG('P_T_IFS_CWS_DEST_DOWNLOAD_INFO', 'Ŀ�����ؼ�¼��ͬ����', V_SQLERRM, '', '');
              COMMIT;
          END;
        END LOOP; --�쳣����������ѭ�����棬��֤�쳣֮���˳�ѭ��ֱ�ӽ����²��Ĵ�������ҵ��Ҫ����߼�
      END;
    END IF;
  
  END P_T_IFS_CWS_DEST_DOWNLOAD_INFO;

  /*T_IFS_CWS_CARTRACKING_MAIN;
  T_CWS_BU_CARTRACKING_MAIN */

  /*׷���������� ���� 2017-11-10*/
  PROCEDURE P_T_IFS_CWS_CARTRACKING_MAIN AS
    NCOUNT        NUMBER;
    EXPLAINTYPENO VARCHAR2(50);
    --V_SQLERRM     VARCHAR2(1000);
  BEGIN
    SELECT COUNT(A.REQUEST_ID) INTO NCOUNT FROM T_CWS_BU_CARTRACKING_MAIN A WHERE A.SEND_FLAG IN ('0', '1'); --'�ӿڴ���״̬�ӿڴ���״̬��0������1���ڴ���2�Ѵ���3����ʧ�ܣ�Ĭ��ֵΪ��0��'
  
    IF NCOUNT > 0 THEN
      BEGIN
      
        UPDATE T_CWS_BU_CARTRACKING_MAIN A
        SET    A.SEND_FLAG = '1' --1�����ڴ���
        WHERE  A.SEND_FLAG = '0'; --0:������
        COMMIT; --�ύ�󷽱�����쳣���ҵ���ȶ��쳣����,�Ƚ�ҵ������Ϊ2״̬���ǽӿڱ���Ϊ�ع��������ڵ����ݾ����쳣����
        --����ÿ���Ĵ��䣬����ĳ�������쳣���¼��־Ȼ�����һ�������ݵĴ��䡣
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
            SET    A.SEND_FLAG         = '2', --2������ɹ�
                   A.LAST_UPDATED_DATE = SYSDATE
            WHERE  A.REQUEST_ID = J1.REQUEST_ID AND A.SEND_FLAG = '1'; -- 1 ������
          
            COMMIT; --��������һ�����ݣ��������»��߲���ɹ�Ȼ�󲢰Ѵ���״̬��Ϊ����ɹ�״̬������������һ����¼����commit��һ�������쳣��ֻ�ع���һ���쳣������
            --COURSES(I) := J1.ID; --���ӿڱ�ID--INTF_ID��������
            -- I := I + 1; --�����±�+1
          EXCEPTION
            WHEN OTHERS THEN
              ROLLBACK; --�ع������쳣��ĳ����¼����Ϊ����������һ�����ݰ���2�������Ա���ع�����֤�쳣�˲���������ҵ�����
              V_SQLERRM := SUBSTR(SQLERRM, 0, 1000);
              UPDATE T_CWS_BU_CARTRACKING_MAIN A
              SET    A.SEND_FLAG = '3' -- 3 ����ʧ��
              
              WHERE  A.REQUEST_ID = EXPLAINTYPENO AND A.SEND_FLAG = '1';
              COMMIT; --�쳣���ݵĲ�����������д������־�����Էֱ����Լ��������ύ
              PKG_COC_COMMON.ADDOPERATIONLOG('P_T_IFS_CWS_CARTRACKING_MAIN', '׷����������', V_SQLERRM, '', '');
            
              COMMIT;
          END;
        END LOOP; --�쳣����������ѭ�����棬��֤�쳣֮���˳�ѭ��ֱ�ӽ����²��Ĵ�������ҵ��Ҫ����߼�
      END;
    END IF;
  
  END P_T_IFS_CWS_CARTRACKING_MAIN;
  /*
  IFR.T_IFR_CWS_CGUARD_INFO
  T_CWS_BU_CGUARD_INFO
  */
  /*�ӿڱ�_ ����08IT����_������¼ ���� 2017-11-8*/
  PROCEDURE P_T_CWS_BU_CGUARD_INFO AS
    NCOUNT        NUMBER;
    EXPLAINTYPENO VARCHAR2(50);
    --V_SQLERRM     VARCHAR2(1000);
  BEGIN
    SELECT COUNT(A.ID) INTO NCOUNT FROM IFR.T_IFR_CWS_CGUARD_INFO A WHERE A.SEND_FLAG IN ('0', '1'); --'�ӿڴ���״̬�ӿڴ���״̬��0������1���ڴ���2�Ѵ���3����ʧ�ܣ�Ĭ��ֵΪ��0��'
  
    IF NCOUNT > 0 THEN
      BEGIN
        -- I := 1; --�����±��ʼ��
        UPDATE IFR.T_IFR_CWS_CGUARD_INFO A
        SET    A.SEND_FLAG = '1' --1�����ڴ���
        WHERE  A.SEND_FLAG = '0'; --0:������
        COMMIT; --�ύ�󷽱�����쳣���ҵ���ȶ��쳣����,�Ƚ�ҵ������Ϊ2״̬���ǽӿڱ���Ϊ�ع��������ڵ����ݾ����쳣����
        --����ÿ���Ĵ��䣬����ĳ�������쳣���¼��־Ȼ�����һ�������ݵĴ��䡣
        FOR J1 IN (SELECT *
                   FROM   IFR.T_IFR_CWS_CGUARD_INFO A
                   WHERE  A.SEND_FLAG = '1' --1�����ڴ���
                   ORDER  BY A.CREATED_DATE ASC)
        LOOP
          BEGIN
            EXPLAINTYPENO := J1.ID; --
          
            MERGE INTO T_CWS_BU_CGUARD_INFO B
            USING (SELECT 1 FROM DUAL) I --�����ظ�͸��DBLINK�ӽӿڱ�
            ON (B.CGUARD_REQUEST_ID = J1.REQID) --����ҵ���������ҵ��ؼ�������ƥ������
            WHEN MATCHED THEN
            -- �������ң��ҵ��˾͸���
              UPDATE
              SET    B.VIN              = J1.VIN, --VIN��
                     B.CALLIN_TYPE_CODE = J1.CALLTYPECODE, --�������ͱ���
                     B.CALLIN_TYPE_NAME = J1.CALLTYPENAME, --������������
                     B.CGUARD_TYPE_CODE = J1.SECURITYTYPECODE, --�������ͱ���
                     B.CGUARD_TYPE_NAME = J1.SECURITYTYPENAME, --������������
                     B.CAR_FORHEAD      = J1.FRONTDIRECTION,
                     /*��������ʱ���ʽΪ�뼶ʱ�������Ҫ����ת�� ����ͬ*/
                     B.GPS_TIME       = J1.GPSDATE / (60 * 60 * 24) + TO_DATE('1970-01-01 08:00:00', 'YYYY-MM-DD HH24:MI:SS'),
                     B.CAR_POSITION   = J1.CARLOCATION,
                     B.CGUARD_CONTENT = decode(J1.CGUARDCONTENT,'null','',J1.CGUARDCONTENT),
                     B.E_OR_W         = J1.LONGITUDE, --����
                     B.S_OR_N         = J1.LATITUDE, --γ��
                     B.CREATE_DATE    = J1.CREATETIME / (60 * 60 * 24) + TO_DATE('1970-01-01 08:00:00', 'YYYY-MM-DD HH24:MI:SS'), --����ʱ��
                     
                     B.LAST_UPDATED_DATE = SYSTIMESTAMP
              
              WHERE  B.CGUARD_REQUEST_ID = J1.REQID
              
            
            WHEN NOT MATCHED THEN
            --����
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
            SET    A.SEND_FLAG         = '2', --2������ɹ�
                   A.SEND_DATE         = SYSDATE,
                   A.LAST_UPDATED_DATE = SYSDATE
            WHERE  A.ID = J1.ID AND A.SEND_FLAG = '1'; -- 1 ������
          
            COMMIT; --��������һ�����ݣ��������»��߲���ɹ�Ȼ�󲢰Ѵ���״̬��Ϊ����ɹ�״̬������������һ����¼����commit��һ�������쳣��ֻ�ع���һ���쳣������
            --COURSES(I) := J1.ID; --���ӿڱ�ID--INTF_ID��������
            -- I := I + 1; --�����±�+1
          EXCEPTION
            WHEN OTHERS THEN
              ROLLBACK; --�ع������쳣��ĳ����¼����Ϊ����������һ�����ݰ���2�������Ա���ع�����֤�쳣�˲���������ҵ�����
              V_SQLERRM := SUBSTR(SQLERRM, 0, 1000);
              UPDATE IFR.T_IFR_CWS_CGUARD_INFO A
              SET    A.SEND_FLAG   = '3', -- 3 �쳣
                     A.SEND_REMARK = V_SQLERRM
              WHERE  A.ID = EXPLAINTYPENO AND A.SEND_FLAG = '1';
              COMMIT; --�쳣���ݵĲ�����������д������־�����Էֱ����Լ��������ύ
              PKG_COC_COMMON.ADDOPERATIONLOG('P_T_CWS_BU_CGUARD_INFO', '�ӿڱ�_ ����08IT����_������¼', V_SQLERRM, '', '');
            
              COMMIT;
          END;
        END LOOP; --�쳣����������ѭ�����棬��֤�쳣֮���˳�ѭ��ֱ�ӽ����²��Ĵ�������ҵ��Ҫ����߼�
      
      END;
    END IF;
  
  END P_T_CWS_BU_CGUARD_INFO;

  /*  
  IFR.T_IFR_CWS_DEST_DOWNLOAD
  T_CWS_BU_DEST_DOWNLOAD_INFO
  
  COOK
  */

  /*�ӿڱ�_���͸�08IT����_Ŀ�ĵ����ؼ�¼���첽�� ���� 2017-11-8*/
  PROCEDURE P_T_CWS_BU_DEST_DOWNLOAD_INFO AS
    NCOUNT        NUMBER;
    EXPLAINTYPENO VARCHAR2(50);
    --V_SQLERRM     VARCHAR2(1000);
  BEGIN
    SELECT COUNT(A.ID) INTO NCOUNT FROM IFR.T_IFR_CWS_DEST_DOWNLOAD A WHERE A.ISSENDFLAG IN ('0', '1'); --'�ӿڴ���״̬�ӿڴ���״̬��0������1���ڴ���2�Ѵ���3����ʧ�ܣ�Ĭ��ֵΪ��0��'
  
    IF NCOUNT > 0 THEN
      BEGIN
        -- I := 1; --�����±��ʼ��
        UPDATE IFR.T_IFR_CWS_DEST_DOWNLOAD A
        SET    A.ISSENDFLAG = '1' --1�����ڴ���
        WHERE  A.ISSENDFLAG = '0'; --0:������
        COMMIT; --�ύ�󷽱�����쳣���ҵ���ȶ��쳣����,�Ƚ�ҵ������Ϊ2״̬���ǽӿڱ���Ϊ�ع��������ڵ����ݾ����쳣����
        --����ÿ���Ĵ��䣬����ĳ�������쳣���¼��־Ȼ�����һ�������ݵĴ��䡣
      
        UPDATE IFR.T_IFR_CWS_DEST_DOWNLOAD A
        SET    A.ISSENDFLAG        = '3', --����ʧ��
               A.SEND_REMARK       = '��ҵ���û��ƥ�䵽����',
               A.LAST_UPDATED_DATE = SYSDATE
        WHERE  A.DESTINATIONID not in (select B.DEST_DW_LOGID from T_CWS_BU_DEST_DOWNLOAD_INFO B);
        COMMIT;
      
        FOR J1 IN (SELECT *
                   FROM   IFR.T_IFR_CWS_DEST_DOWNLOAD A
                   WHERE  A.ISSENDFLAG = '1' --1�����ڴ���
                   ORDER  BY A.CREATED_DATE ASC)
        LOOP
          BEGIN
            EXPLAINTYPENO := J1.ID; --
          
            UPDATE T_CWS_BU_DEST_DOWNLOAD_INFO B
            SET    B.DA_REQEST_DATE = J1.VISITTIME / (60 * 60 * 24) + TO_DATE('1970-01-01 08:00:00', 'YYYY-MM-DD HH24:MI:SS'), --���ػ�����ʱ��
                   
                   B.LAST_UPDATED_DATE = SYSTIMESTAMP
            
            WHERE  B.DEST_DW_LOGID = J1.DESTINATIONID;
          
            UPDATE IFR.T_IFR_CWS_DEST_DOWNLOAD A
            SET    A.ISSENDFLAG        = '2', --2������ɹ�
                   A.SENDTIME          = TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS'),
                   A.LAST_UPDATED_DATE = SYSDATE
            WHERE  A.ID = J1.ID AND A.ISSENDFLAG = '1'; -- 1 ������
          
            COMMIT; --��������һ�����ݣ��������»��߲���ɹ�Ȼ�󲢰Ѵ���״̬��Ϊ����ɹ�״̬������������һ����¼����commit��һ�������쳣��ֻ�ع���һ���쳣������
            --COURSES(I) := J1.ID; --���ӿڱ�ID--INTF_ID��������
            -- I := I + 1; --�����±�+1
          EXCEPTION
            WHEN OTHERS THEN
              ROLLBACK; --�ع������쳣��ĳ����¼����Ϊ����������һ�����ݰ���2�������Ա���ع�����֤�쳣�˲���������ҵ�����
              V_SQLERRM := SUBSTR(SQLERRM, 0, 1000);
              UPDATE IFR.T_IFR_CWS_DEST_DOWNLOAD A
              SET    A.ISSENDFLAG  = '3', -- 3 �쳣
                     A.SEND_REMARK = V_SQLERRM
              WHERE  A.ID = EXPLAINTYPENO AND A.ISSENDFLAG = '1';
              COMMIT; --�쳣���ݵĲ�����������д������־�����Էֱ����Լ��������ύ
              PKG_COC_COMMON.ADDOPERATIONLOG('P_T_CWS_BU_DEST_DOWNLOAD_INFO', '�ӿڱ�_���͸�08IT����_Ŀ�ĵ����ؼ�¼���첽��', V_SQLERRM, '', '');
            
              COMMIT;
          END;
        END LOOP; --�쳣����������ѭ�����棬��֤�쳣֮���˳�ѭ��ֱ�ӽ����²��Ĵ�������ҵ��Ҫ����߼�
      
      END;
    END IF;
  
  END P_T_CWS_BU_DEST_DOWNLOAD_INFO;

  /*  
  IFR.T_IFR_CWS_CARPOSITION_INFO
  T_CWS_BU_CARPOSITION_INFO
  */

  /*�ӿڱ�_����08IT����_��λ������־ ���� 2017-11-8*/
  PROCEDURE P_T_CWS_BU_CARPOSITION_INFO AS
    NCOUNT        NUMBER;
    EXPLAINTYPENO VARCHAR2(50);
    --V_SQLERRM     VARCHAR2(1000);
  BEGIN
    SELECT COUNT(A.ID) INTO NCOUNT FROM IFR.T_IFR_CWS_CARPOSITION_INFO A WHERE A.SEND_FLAG IN ('0', '1'); --'�ӿڴ���״̬�ӿڴ���״̬��0������1���ڴ���2�Ѵ���3����ʧ�ܣ�Ĭ��ֵΪ��0��'
  
    IF NCOUNT > 0 THEN
      BEGIN
        -- I := 1; --�����±��ʼ��
        UPDATE IFR.T_IFR_CWS_CARPOSITION_INFO A
        SET    A.SEND_FLAG = '1' --1�����ڴ���
        WHERE  A.SEND_FLAG = '0'; --0:������
        COMMIT; --�ύ�󷽱�����쳣���ҵ���ȶ��쳣����,�Ƚ�ҵ������Ϊ2״̬���ǽӿڱ���Ϊ�ع��������ڵ����ݾ����쳣����
        --����ÿ���Ĵ��䣬����ĳ�������쳣���¼��־Ȼ�����һ�������ݵĴ��䡣
      
        UPDATE IFR.T_IFR_CWS_CARPOSITION_INFO A
        SET    A.SEND_FLAG         = '3', --����ʧ��
               A.SEND_REMARK       = '��ҵ���û��ƥ�䵽����',
               A.LAST_UPDATED_DATE = SYSDATE
        WHERE  A.REQID not in (select B.REQUEST_ID from T_CWS_BU_CARPOSITION_INFO B);
        COMMIT;
      
        FOR J1 IN (SELECT *
                   FROM   IFR.T_IFR_CWS_CARPOSITION_INFO A
                   WHERE  A.SEND_FLAG = '1' --1�����ڴ���
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
            SET    A.SEND_FLAG         = '2', --2������ɹ�
                   A.SEND_DATE         = SYSDATE,
                   A.LAST_UPDATED_DATE = SYSDATE
            WHERE  A.ID = J1.ID AND A.SEND_FLAG = '1'; -- 1 ������
          
            COMMIT; --��������һ�����ݣ��������»��߲���ɹ�Ȼ�󲢰Ѵ���״̬��Ϊ����ɹ�״̬������������һ����¼����commit��һ�������쳣��ֻ�ع���һ���쳣������
            --COURSES(I) := J1.ID; --���ӿڱ�ID--INTF_ID��������
            -- I := I + 1; --�����±�+1
          EXCEPTION
            WHEN OTHERS THEN
              ROLLBACK; --�ع������쳣��ĳ����¼����Ϊ����������һ�����ݰ���2�������Ա���ع�����֤�쳣�˲���������ҵ�����
              V_SQLERRM := SUBSTR(SQLERRM, 0, 1000);
              UPDATE IFR.T_IFR_CWS_CARPOSITION_INFO A
              SET    A.SEND_FLAG   = '3', -- 3 �쳣
                     A.SEND_REMARK = V_SQLERRM
              WHERE  A.ID = EXPLAINTYPENO AND A.SEND_FLAG = '1';
              COMMIT; --�쳣���ݵĲ�����������д������־�����Էֱ����Լ��������ύ
              PKG_COC_COMMON.ADDOPERATIONLOG('P_T_CWS_BU_CARPOSITION_INFO', '�ӿڱ�_����08IT����_��λ������־', V_SQLERRM, '', '');
            
              COMMIT;
          END;
        END LOOP; --�쳣����������ѭ�����棬��֤�쳣֮���˳�ѭ��ֱ�ӽ����²��Ĵ�������ҵ��Ҫ����߼�
      
      END;
    END IF;
  
  END P_T_CWS_BU_CARPOSITION_INFO;

  /*
  IFR.T_IFR_CWS_CARTRACKING_DETAIL
  T_CWS_BU_CARTRACKING_DETAIL
  */

  /*�ӿڱ�_׷������ӱ� ���� 2017-11-8*/
  PROCEDURE P_T_CWS_BU_CARTRACKING_DETAIL AS
    NCOUNT        NUMBER;
    EXPLAINTYPENO VARCHAR2(50);
    --V_SQLERRM     VARCHAR2(1000);
  BEGIN
    SELECT COUNT(A.ID) INTO NCOUNT FROM IFR.T_IFR_CWS_CARTRACKING_DETAIL A WHERE A.SEND_FLAG IN ('0', '1'); --'�ӿڴ���״̬�ӿڴ���״̬��0������1���ڴ���2�Ѵ���3����ʧ�ܣ�Ĭ��ֵΪ��0��'
  
    IF NCOUNT > 0 THEN
      BEGIN
        -- I := 1; --�����±��ʼ��
        UPDATE IFR.T_IFR_CWS_CARTRACKING_DETAIL A
        SET    A.SEND_FLAG = '1' --1�����ڴ���
        WHERE  A.SEND_FLAG = '0'; --0:������
        COMMIT; --�ύ�󷽱�����쳣���ҵ���ȶ��쳣����,�Ƚ�ҵ������Ϊ2״̬���ǽӿڱ���Ϊ�ع��������ڵ����ݾ����쳣����
        --����ÿ���Ĵ��䣬����ĳ�������쳣���¼��־Ȼ�����һ�������ݵĴ��䡣
      
        /*     
        �߼�����
         UPDATE IFR.T_IFR_CWS_CARTRACKING_DETAIL A
                 SET A.SEND_FLAG         = '3', --����ʧ��
                     A.SEND_REMARK       = '��ҵ���û��ƥ�䵽����',
                     A.LAST_UPDATED_DATE = SYSDATE
               WHERE A.RESID not in
                     (select B.REQUEST_RESULT_ID
                        from T_CWS_BU_CARTRACKING_DETAIL B);
              COMMIT;*/
      
        FOR J1 IN (SELECT *
                   FROM   IFR.T_IFR_CWS_CARTRACKING_DETAIL A
                   WHERE  A.SEND_FLAG = '1' --1�����ڴ���
                   ORDER  BY A.CREATED_DATE ASC)
        LOOP
          BEGIN
            EXPLAINTYPENO := J1.ID; --
          
            MERGE INTO T_CWS_BU_CARTRACKING_DETAIL B
            USING (SELECT 1 FROM DUAL) I --�����ظ�͸��DBLINK�ӽӿڱ�
            ON (B.REQUEST_RESULT_ID = J1.RESID) --����ҵ���������ҵ��ؼ�������ƥ������
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
                 J1.LONGITUDE�� J1.LATITUDE,
                 J1.CREATETIME / (60 * 60 * 24) + TO_DATE('1970-01-01 08:00:00', 'YYYY-MM-DD HH24:MI:SS'),
                 J1.REQID,
                 J1.VIN,
                 J1.SENDTCUTIME,
                 J1.TCURESULTCODE,
                 J1.TCURESULTNAME
                 
                 );
          
            UPDATE IFR.T_IFR_CWS_CARTRACKING_DETAIL A
            SET    A.SEND_FLAG         = '2', --2������ɹ�
                   A.SEND_DATE         = SYSDATE,
                   A.LAST_UPDATED_DATE = SYSDATE
            WHERE  A.ID = J1.ID AND A.SEND_FLAG = '1'; -- 1 ������
          
            COMMIT; --��������һ�����ݣ��������»��߲���ɹ�Ȼ�󲢰Ѵ���״̬��Ϊ����ɹ�״̬������������һ����¼����commit��һ�������쳣��ֻ�ع���һ���쳣������
            --COURSES(I) := J1.ID; --���ӿڱ�ID--INTF_ID��������
            -- I := I + 1; --�����±�+1
          EXCEPTION
            WHEN OTHERS THEN
              ROLLBACK; --�ع������쳣��ĳ����¼����Ϊ����������һ�����ݰ���2�������Ա���ع�����֤�쳣�˲���������ҵ�����
              V_SQLERRM := SUBSTR(SQLERRM, 0, 1000);
              UPDATE IFR.T_IFR_CWS_CARTRACKING_DETAIL A
              SET    A.SEND_FLAG   = '3', -- 3 �쳣
                     A.SEND_REMARK = V_SQLERRM
              WHERE  A.ID = EXPLAINTYPENO AND A.SEND_FLAG = '1';
              COMMIT; --�쳣���ݵĲ�����������д������־�����Էֱ����Լ��������ύ
              PKG_COC_COMMON.ADDOPERATIONLOG('P_T_CWS_BU_CARTRACKING_DETAIL', '�ӿڱ�_׷������ӱ�', V_SQLERRM, '', '');
            
              COMMIT;
          END;
        END LOOP; --�쳣����������ѭ�����棬��֤�쳣֮���˳�ѭ��ֱ�ӽ����²��Ĵ�������ҵ��Ҫ����߼�
      
      END;
    END IF;
  
  END P_T_CWS_BU_CARTRACKING_DETAIL;

  /*
  IFR.T_IFR_CWS_VEHNET_VISIT_STATICS
  T_CWS_BU_VEHNET_VISIT_STATICS
  
  COOK
  */
  /*�ӿڱ�_08IT���������ͳ�� ���� 2017-11-9*/
  PROCEDURE P_T_CWS_BU_VEHNET_VISIT_S AS
    NCOUNT        NUMBER;
    EXPLAINTYPENO VARCHAR2(50);
    --V_SQLERRM     VARCHAR2(1000);
  BEGIN
    SELECT COUNT(A.DATE_STATICS_ID) INTO NCOUNT FROM IFR.T_IFR_CWS_VEHNET_VISIT_STATICS A WHERE A.SEND_FLAG IN ('0', '1'); --'�ӿڴ���״̬�ӿڴ���״̬��0������1���ڴ���2�Ѵ���3����ʧ�ܣ�Ĭ��ֵΪ��0��'
  
    IF NCOUNT > 0 THEN
      BEGIN
        -- I := 1; --�����±��ʼ��
        UPDATE IFR.T_IFR_CWS_VEHNET_VISIT_STATICS A
        SET    A.SEND_FLAG = '1' --1�����ڴ���
        WHERE  A.SEND_FLAG = '0'; --0:������
        COMMIT; --�ύ�󷽱�����쳣���ҵ���ȶ��쳣����,�Ƚ�ҵ������Ϊ2״̬���ǽӿڱ���Ϊ�ع��������ڵ����ݾ����쳣����
        --����ÿ���Ĵ��䣬����ĳ�������쳣���¼��־Ȼ�����һ�������ݵĴ��䡣
      
        FOR J1 IN (SELECT *
                   FROM   IFR.T_IFR_CWS_VEHNET_VISIT_STATICS A
                   WHERE  A.SEND_FLAG = '1' --1�����ڴ���
                   ORDER  BY A.CREATED_DATE ASC)
        LOOP
          BEGIN
            EXPLAINTYPENO := J1.DATE_STATICS_ID; --
          
            MERGE INTO T_CWS_BU_VEHNET_VISIT_STATICS B
            USING (SELECT 1 FROM DUAL) I --�����ظ�͸��DBLINK�ӽӿڱ�
            ON (B.REQUEST_DATE = to_date(J1.VISITDATE, 'yyyy-mm-dd') and B.VIN = J1.VIN and B.VEHNET_SERVICE_CODE = J1.SERVICECODE) --����ҵ���������ҵ��ؼ�������ƥ������
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
            SET    A.SEND_FLAG         = '2', --2������ɹ�
                   A.SEND_DATE         = SYSDATE,
                   A.LAST_UPDATED_DATE = SYSDATE
            WHERE  A.DATE_STATICS_ID = J1.DATE_STATICS_ID AND A.SEND_FLAG = '1'; -- 1 ������
          
            COMMIT; --��������һ�����ݣ��������»��߲���ɹ�Ȼ�󲢰Ѵ���״̬��Ϊ����ɹ�״̬������������һ����¼����commit��һ�������쳣��ֻ�ع���һ���쳣������
            --COURSES(I) := J1.ID; --���ӿڱ�ID--INTF_ID��������
            -- I := I + 1; --�����±�+1
          EXCEPTION
            WHEN OTHERS THEN
              ROLLBACK; --�ع������쳣��ĳ����¼����Ϊ����������һ�����ݰ���2�������Ա���ع�����֤�쳣�˲���������ҵ�����
              V_SQLERRM := SUBSTR(SQLERRM, 0, 1000);
              UPDATE IFR.T_IFR_CWS_VEHNET_VISIT_STATICS A
              SET    A.SEND_FLAG   = '3', -- 3 �쳣
                     A.SEND_REMARK = V_SQLERRM
              WHERE  A.DATE_STATICS_ID = EXPLAINTYPENO AND A.SEND_FLAG = '1';
              COMMIT; --�쳣���ݵĲ�����������д������־�����Էֱ����Լ��������ύ
              PKG_COC_COMMON.ADDOPERATIONLOG('P_T_CWS_BU_VEHNET_VISIT_S', '�ӿڱ�_08IT���������ͳ��', V_SQLERRM, '', '');
            
              COMMIT;
          END;
        END LOOP; --�쳣����������ѭ�����棬��֤�쳣֮���˳�ѭ��ֱ�ӽ����²��Ĵ�������ҵ��Ҫ����߼�
      
      END;
    END IF;
  
  END P_T_CWS_BU_VEHNET_VISIT_S;

  /*
  IFS.T_IFS_CWS_SERV_PKG_BUY
  T_CWS_BU_BASIC_SERV_BUY
  T_CWS_BU_ADDED_SERV_BUY
  
  COOK
  */

  /*�ӿڱ�_���͸�08IT����_�������ײ���ϸ��Ϣ ���� 2017-11-13*/
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
      ROLLBACK; --�ع������쳣��ĳ����¼����Ϊ����������һ�����ݰ���2�������Ա���ع�����֤�쳣�˲���������ҵ�����
      V_SQLERRM := SUBSTR(SQLERRM, 0, 1000);
      PKG_COC_COMMON.ADDOPERATIONLOG('P_T_IFS_CWS_SERV_PKG_BUY', '�ӿڱ�_���͸�08IT����_�������ײ���ϸ��Ϣ', V_SQLERRM, '', '');
  END P_T_IFS_CWS_SERV_PKG_BUY;

  /*T_IFS_CWS_CUST_SERVICE_INFO
  T_CWS_DB_CAR_MACH_INFO
  T_CWS_DB_MEM_SERVICE_INFO
  
  COOK
  */

  /*�ӿڱ�_���͸�08IT����_�����ͻ�������Ϣ  ���� 2017-11-13*/
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
             DECODE(A.GENDER, 'δ֪', '', A.GENDER),
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
      ROLLBACK; --�ع������쳣��ĳ����¼����Ϊ����������һ�����ݰ���2�������Ա���ع�����֤�쳣�˲���������ҵ�����
      V_SQLERRM := SUBSTR(SQLERRM, 0, 1000);
      PKG_COC_COMMON.ADDOPERATIONLOG('P_T_IFS_CWS_CUST_SERVICE_INFO', '�ӿڱ�_���͸�08IT����_�����ͻ�������Ϣ', V_SQLERRM, '', '');
    
  END P_T_IFS_CWS_CUST_SERVICE_INFO;

  /*
  T_CWS_BU_SMS_SEND
  
  IFR.T_IFR_CWS_SMS_SEND
  */

  /*�ӿڱ�_���ճ��������ŷ������ ���� 2017-11-14*/
  PROCEDURE P_T_CWS_BU_SMS_SEND AS
    NCOUNT        NUMBER;
    EXPLAINTYPENO VARCHAR2(50);
    --V_SQLERRM     VARCHAR2(1000);
  BEGIN
    SELECT COUNT(A.TABLEID) INTO NCOUNT FROM IFR.T_IFR_CWS_SMS_SEND A WHERE A.SEND_FLAG IN ('0', '1'); --'�ӿڴ���״̬�ӿڴ���״̬��0������1���ڴ���2�Ѵ���3����ʧ�ܣ�Ĭ��ֵΪ��0��'
  
    IF NCOUNT > 0 THEN
      BEGIN
        -- I := 1; --�����±��ʼ��
        UPDATE IFR.T_IFR_CWS_SMS_SEND A
        SET    A.SEND_FLAG = '1' --1�����ڴ���
        WHERE  A.SEND_FLAG = '0'; --0:������
        COMMIT; --�ύ�󷽱�����쳣���ҵ���ȶ��쳣����,�Ƚ�ҵ������Ϊ2״̬���ǽӿڱ���Ϊ�ع��������ڵ����ݾ����쳣����
        --����ÿ���Ĵ��䣬����ĳ�������쳣���¼��־Ȼ�����һ�������ݵĴ��䡣
      
        FOR J1 IN (SELECT *
                   FROM   IFR.T_IFR_CWS_SMS_SEND A
                   WHERE  A.SEND_FLAG = '1' --1�����ڴ���
                   ORDER  BY A.CREATED_DATE ASC)
        LOOP
          BEGIN
            EXPLAINTYPENO := J1.TABLEID; --
          
            MERGE INTO T_CWS_BU_SMS_SEND B
            USING (SELECT 1 FROM DUAL) I --�����ظ�͸��DBLINK�ӽӿڱ�
            ON (B.ID = J1.ID) --����ҵ���������ҵ��ؼ�������ƥ������
            WHEN MATCHED THEN
            
              UPDATE
              SET    B.SOURCE_BILLNO = J1.SEQUENCEID,
                     
                     B.VIN             = J1.VINNUMBER,
                     B.SMSDATA         = convert((utl_raw.cast_to_varchar2(utl_encode.base64_decode(utl_raw.cast_to_raw(J1.SMSDATA)))), 'ZHS16GBK', 'UTF8'��,
                     B.PHONE           = J1.CALLTONUM,
                     B.SEND_TIME       = to_date(J1.SENDTIME, 'yyyy-mm-dd hh24:mi:ss'),
                     B.SERVICE_TYPE    = J1.SERVICETYPE,
                     --����������״̬2�ɹ���3ʧ�ܣ�1δ֪������Ϊcarwings�Ķ���״̬1 �ɹ���0ʧ��
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
                 convert((utl_raw.cast_to_varchar2(utl_encode.base64_decode(utl_raw.cast_to_raw(J1.SMSDATA)))), 'ZHS16GBK', 'UTF8'��,
                 J1.CALLTONUM,
                 to_date(J1.SENDTIME, 'yyyy-mm-dd hh24:mi:ss'),
                 J1.SERVICETYPE,
                 --����������״̬2�ɹ���3ʧ�ܣ�1δ֪������Ϊcarwings�Ķ���״̬1 �ɹ���0ʧ��
                decode(J1.SENDSTATUS,'2','1','3','0',null),
                 J1.DESCRIPTION,
                 to_date(J1.SMSSTATUSTIME, 'yyyy-mm-dd hh24:mi:ss'),
                 
                 'CGUARD'
                 
                 );
          
            UPDATE IFR.T_IFR_CWS_SMS_SEND A
            SET    A.SEND_FLAG         = '2', --2������ɹ�
                   A.SEND_DATE         = SYSDATE,
                   A.LAST_UPDATED_DATE = SYSDATE
            WHERE  A.ID = J1.ID AND A.SEND_FLAG = '1'; -- 1 ������
          
            COMMIT; --��������һ�����ݣ��������»��߲���ɹ�Ȼ�󲢰Ѵ���״̬��Ϊ����ɹ�״̬������������һ����¼����commit��һ�������쳣��ֻ�ع���һ���쳣������
            --COURSES(I) := J1.ID; --���ӿڱ�ID--INTF_ID��������
            -- I := I + 1; --�����±�+1
          EXCEPTION
            WHEN OTHERS THEN
              ROLLBACK; --�ع������쳣��ĳ����¼����Ϊ����������һ�����ݰ���2�������Ա���ع�����֤�쳣�˲���������ҵ�����
              V_SQLERRM := SUBSTR(SQLERRM, 0, 1000);
              UPDATE IFR.T_IFR_CWS_SMS_SEND A
              SET    A.SEND_FLAG   = '3', -- 3 �쳣
                     A.SEND_REMARK = V_SQLERRM
              WHERE  A.TABLEID = EXPLAINTYPENO AND A.SEND_FLAG = '1';
              COMMIT; --�쳣���ݵĲ�����������д������־�����Էֱ����Լ��������ύ
              PKG_COC_COMMON.ADDOPERATIONLOG('P_T_CWS_BU_SMS_SEND', '�ӿڱ�_���ճ��������ŷ������ ', V_SQLERRM, '', '');
            
              COMMIT;
          END;
        END LOOP; --�쳣����������ѭ�����棬��֤�쳣֮���˳�ѭ��ֱ�ӽ����²��Ĵ�������ҵ��Ҫ����߼�
      
      END;
    END IF;
  
  END P_T_CWS_BU_SMS_SEND;

  /*******************************************************************************************
  *   �汾:        1.00
  *   ���ߣ�       LIZHENG
  *   �������ڣ�   2017-11-20
  *   ��Ҫ����:    ���ݷ����������ѯ�����������
                   ������������������� P_SERVICE_TYPE_CODE
                   ���ؽ���������������
  *   �޸����ڣ�
  *   �޸�˵����
  ********************************************************************************************/

  FUNCTION FN_GET_SERVICETYPENAME_BY_CODE(F_SERVICE_TYPE_CODE in VARCHAR2) RETURN VARCHAR2 IS
  
    F_SERVICE_TYPE_NAME VARCHAR2(2000) := '';
  BEGIN
    select T.SERVICE_TYPE_NAME into F_SERVICE_TYPE_NAME from cws.t_cws_db_carwings_serv_type t WHERE T.SERVICE_TYPE_CODE = F_SERVICE_TYPE_CODE;
    return F_SERVICE_TYPE_NAME;
  END FN_GET_SERVICETYPENAME_BY_CODE;



  /*******************************************************************************************
  *   �汾:        1.00
  *   ���ߣ�       denghq
  *   �������ڣ�   2017-11-20
  *   ��Ҫ����:    ���ݴ�洢��������ת����Ӧ�Ĵ洢����
                   ����������洢������ PROCEDURENAME
                   ���ؽ����bool
  *   �޸����ڣ�
  *   �޸�˵����
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
