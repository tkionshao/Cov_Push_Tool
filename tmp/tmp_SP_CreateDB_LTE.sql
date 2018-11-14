CREATE DEFINER=`covmo`@`%` PROCEDURE `SP_CreateDB_LTE`(IN RNC VARCHAR(50),IN FILEDATE VARCHAR(18),IN KIND VARCHAR(20),IN EVENT_NUM TINYINT(2),IN POS_NUM TINYINT(2))
BEGIN
	DECLARE GT_DB VARCHAR(50) DEFAULT CONCAT('gt_',RNC,'_',FILEDATE);
	DECLARE NT_DB VARCHAR(50) DEFAULT CONCAT('gt_',RNC,'_',CONCAT(LEFT(FILEDATE,8)),'_nt');
	DECLARE NT_DB_NAME VARCHAR(50) DEFAULT CONCAT('gt_nt_',CONCAT(LEFT(FILEDATE,8)),'');
	DECLARE FILE_STARTTIME VARCHAR(20) DEFAULT CONCAT(LEFT(FILEDATE,4),'-',SUBSTRING(FILEDATE,5,2),'-',SUBSTRING(FILEDATE,7,2),' ', SUBSTRING(FILEDATE,10,2),':',SUBSTRING(FILEDATE,12,2));
	DECLARE FILE_ENDTIME VARCHAR(20) DEFAULT CONCAT(LEFT(FILEDATE,4),'-',SUBSTRING(FILEDATE,5,2),'-',SUBSTRING(FILEDATE,7,2),' ', SUBSTRING(FILEDATE,15,2),':',SUBSTRING(FILEDATE,17,2),':00');	
	DECLARE EXISTFLAG INT;
	DECLARE EXISTSESSION INT;
	DECLARE EXISTSESSION_FLAG INT;
	DECLARE GT_SESSION_ID INT;
	DECLARE DAILY_JUDGE  BOOLEAN DEFAULT IF(RIGHT(FILEDATE,9)='0000_0000',TRUE,FALSE);
	
	DECLARE YEAR_DB_NAME VARCHAR(100);
	DECLARE WeekStart CHAR(20);
	DECLARE WeekEnd CHAR(20);
	DECLARE EXISTWKFLAG INT;
	DECLARE CUSTOMER_USER_FLAG VARCHAR(10);
	
	DECLARE WEEK_STARTTIME VARCHAR(20);
	DECLARE WEEK_ENDTIME VARCHAR(20);
	DECLARE	WEEK_DB VARCHAR(50);
	
	SET FILE_ENDTIME=REPLACE(FILE_ENDTIME,'00:00:00','23:59:59') ;
		
	SET WEEK_STARTTIME = CONCAT(FIRST_DAY_OF_WEEK(FILE_STARTTIME),' 00:00:00');
	SET WEEK_ENDTIME  = CONCAT(LAST_DAY_OF_WEEK(FILE_STARTTIME),' 23:59:59');
	SET WeekStart = FIRST_DAY_OF_WEEK(FILE_STARTTIME);
	SET WeekEnd = LAST_DAY_OF_WEEK(FILE_STARTTIME);
	
	
	SET WEEK_DB=CONCAT('gt_',RNC,'_',DATE_FORMAT(WeekStart,'%Y%m%d'),'_',DATE_FORMAT(WeekEnd,'%Y%m%d'));
	
	
	
	SELECT SESSION_ID INTO EXISTFLAG  FROM gt_gw_main.session_information WHERE SESSION_DB=GT_DB;
	SELECT LOWER(`value`) INTO CUSTOMER_USER_FLAG  FROM gt_gw_main.integration_param WHERE gt_group = 'sp' AND gt_name = 'grant' ;
	INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'SP_CreateDB_LTE','Create DB-Start', NOW());
	
	IF IFNULL(EXISTFLAG,0)=0 AND KIND != 'TMP' AND KIND != 'RERUN' AND KIND !='NT' AND KIND !='AP' AND KIND !='WEEK'  THEN
	BEGIN 
		INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'SP_CreateDB_LTE','Create DB-1', NOW());
	
		SELECT CONCAT(LTE_SCHEMA,'/',LTE_RPT,'/',SP_VERSION) INTO @SP_VERSION FROM gt_gw_main.sp_version LIMIT 1;
		SELECT IFNULL(MAX(SESSION_ID),0)+1  INTO GT_SESSION_ID FROM  gt_gw_main.session_information;
	
		SET @SqlCmd = CONCAT('SELECT GW_URI,AP_URI,DS_AP_URI,IFNULL(RNC_VERSION,'''') INTO @RNC_TO_SEC_GW_URI,@RNC_TO_SEC_AP_URI,@RNC_TO_SEC_DS_AP_URI,@RNC_VERSION FROM gt_gw_main.rnc_information WHERE rnc=',RNC,' and technology = ''LTE'' ;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
	
		SET @SqlCmd =CONCAT('INSERT INTO gt_gw_main.session_information 
			(SESSION_ID, SESSION_DB ,RNC,FILE_STARTTIME,FILE_ENDTIME,STATUS,IMPORT_TIME,SESSION_START,SESSION_END,SESSION_TYPE,TECHNOLOGY,SP_VERSION,GW_URI,AP_URI,DS_AP_URI,RNC_VERSION ) values 
			(',GT_SESSION_ID,',''',GT_DB,''',''',RNC,''',''',FILE_STARTTIME,''',''',FILE_ENDTIME,''',0, NOW(),''',FILE_STARTTIME,''',''',FILE_ENDTIME,'''
			,''',IF(KIND='DAILY','DAY','TEMP'),''',''LTE'',''',@SP_VERSION,''',''',@RNC_TO_SEC_GW_URI,''',''',@RNC_TO_SEC_AP_URI,''',''',@RNC_TO_SEC_DS_AP_URI,''',''',@RNC_VERSION,''')');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
	
		SET @SqlCmd =CONCAT('
				UPDATE `gt_gw_main`.`session_information` A,  `gt_gw_main`.`rnc_information` B
				SET 
					A.RNC_VERSION = B.RNC_VERSION
				WHERE A.`SESSION_DB` = ''',GT_DB,''' AND A.RNC = B.RNC;
				');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		IF KIND = 'DAILY' THEN
			CALL gt_gw_main.SP_CreateDB_Schema_LTE(GT_DB,2);
			CALL gt_gw_main.SP_CreateDB_Schema_Sub(GT_DB,'lte');
			CALL gt_gw_main.SP_Create_Merge_Rename_Table_LTE(GT_DB,'DY',EVENT_NUM,POS_NUM);
 			CALL gt_gw_main.SP_CreateDB_PM_LTE(GT_DB);
			CALL gt_gw_main.SP_Check_SysConfig('STEP1',FILEDATE,GT_DB,'');
			IF CUSTOMER_USER_FLAG = 'true' THEN
				CALL gt_gw_main.SP_Grant(GT_DB);
			END IF;
		ELSEIF KIND = 'HOURLY' THEN
			CALL gt_gw_main.SP_CreateDB_Schema_LTE(GT_DB,1);
			CALL gt_gw_main.SP_CreateDB_PM_LTE(GT_DB);
			CALL gt_gw_main.SP_Check_SysConfig('STEP1',FILEDATE,GT_DB,'');
		END IF;
	
	
	
		IF DAILY_JUDGE=TRUE THEN
	
			SET @SqlCmd =CONCAT('DROP TABLE IF EXISTS ',GT_DB,'.session_information;');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
			
			SET @SqlCmd =CONCAT('CREATE TABLE ',GT_DB,'.session_information ENGINE=MYISAM
					     SELECT * FROM gt_gw_main.session_information 
					     WHERE SESSION_DB=''',GT_DB,'''');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
		END IF;
	
		
	END;
	
	ELSEIF KIND = 'WEEK' THEN
		
		SELECT COUNT(SESSION_ID) INTO EXISTSESSION  FROM gt_gw_main.session_information WHERE SESSION_DB=WEEK_DB;
		IF IFNULL(EXISTSESSION,0)=0  THEN 
		SELECT CONCAT(LTE_SCHEMA,'/',LTE_RPT,'/',SP_VERSION) INTO @SP_VERSION FROM gt_gw_main.sp_version LIMIT 1;
		SELECT IFNULL(MAX(SESSION_ID),0)+1  INTO GT_SESSION_ID FROM  gt_gw_main.session_information;
		SET @SqlCmd =CONCAT('INSERT INTO gt_gw_main.session_information 
			(SESSION_ID, SESSION_DB ,RNC,FILE_STARTTIME,FILE_ENDTIME,STATUS,IMPORT_TIME,SESSION_START,SESSION_END,SESSION_TYPE,TECHNOLOGY,SP_VERSION) values 
			(',GT_SESSION_ID,',''',WEEK_DB,''',''',RNC,''',''',WEEK_STARTTIME,''',''',WEEK_ENDTIME,''',0, NOW(),''',WEEK_STARTTIME,''',''',WEEK_ENDTIME,'''
			,''WEEK'',''LTE'',''',@SP_VERSION,''')');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd =CONCAT('
				UPDATE `gt_gw_main`.`session_information` A,  `gt_gw_main`.`rnc_information` B
				SET 
					A.RNC_VERSION = B.RNC_VERSION
				WHERE A.`SESSION_DB` = ''',WEEK_DB,''' AND A.RNC = B.RNC;
				');
				
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
	
	
	
		CALL gt_gw_main.SP_CreateDB_Schema_LTE(WEEK_DB,3);
		CALL gt_gw_main.SP_Create_Merge_Rename_Table_LTE(WEEK_DB,'WK',EVENT_NUM,POS_NUM);
		END IF;
		
	ELSEIF KIND = 'NT' THEN
		INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'SP_CreateDB_LTE','Create DB-4', NOW());
		SET @SqlCmd =CONCAT('CREATE DATABASE IF NOT EXISTS ', NT_DB_NAME);
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		CALL gt_gw_main.SP_CreateDB_NT_Schema_LTE(NT_DB_NAME);
		CALL gt_gw_main.SP_CreateDB_NT2_Schema_LTE(NT_DB_NAME);
	
 	END IF;
	
	INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'SP_CreateDB_LTE','Create DB-End', NOW());
