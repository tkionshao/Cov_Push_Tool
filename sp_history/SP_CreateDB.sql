DELIMITER $$
USE `gt_gw_main`$$
DROP PROCEDURE IF EXISTS `SP_CreateDB_LTE`$$
CREATE DEFINER=`covmo`@`%` PROCEDURE `SP_CreateDB`(IN RNC VARCHAR(50),IN FILEDATE VARCHAR(18),IN KIND VARCHAR(20),IN EVENT_NUM TINYINT(2),IN POS_NUM TINYINT(2))
BEGIN
	DECLARE GT_DB VARCHAR(50) DEFAULT CONCAT('gt_',RNC,'_',FILEDATE);
	DECLARE NT_DB VARCHAR(50) DEFAULT CONCAT('gt_',RNC,'_',CONCAT(LEFT(FILEDATE,8)),'_nt');
	DECLARE NT_DB_NAME VARCHAR(50) DEFAULT CONCAT('gt_nt_',CONCAT(LEFT(FILEDATE,8)),'');
	DECLARE FILE_STARTTIME VARCHAR(18) DEFAULT CONCAT(LEFT(FILEDATE,4),'-',SUBSTRING(FILEDATE,5,2),'-',SUBSTRING(FILEDATE,7,2),' ', SUBSTRING(FILEDATE,10,2),':',SUBSTRING(FILEDATE,12,2));
	DECLARE FILE_ENDTIME VARCHAR(20) DEFAULT CONCAT(LEFT(FILEDATE,4),'-',SUBSTRING(FILEDATE,5,2),'-',SUBSTRING(FILEDATE,7,2),' ', SUBSTRING(FILEDATE,15,2),':',SUBSTRING(FILEDATE,17,2),':00');	
	DECLARE EXISTFLAG INT;
	DECLARE GT_SESSION_ID INT;
	DECLARE DAILY_JUDGE  BOOLEAN DEFAULT IF(RIGHT(FILEDATE,9)='0000_0000',TRUE,FALSE);
	DECLARE CUSTOMER_USER_FLAG VARCHAR(10);
	
	SELECT `value` INTO CUSTOMER_USER_FLAG  FROM gt_gw_main.integration_param WHERE gt_group = 'sp' AND gt_name = 'grant' ;
	SET FILE_ENDTIME=REPLACE(FILE_ENDTIME,'00:00:00','23:59:59') ;
	SELECT SESSION_ID INTO EXISTFLAG  FROM gt_gw_main.session_information WHERE SESSION_DB=GT_DB;
	SELECT LOWER(`value`) INTO CUSTOMER_USER_FLAG  FROM gt_gw_main.integration_param WHERE gt_group = 'sp' AND gt_name = 'grant' ;
	INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'SP_CreateDB','Create DB-Start', NOW());
	
	IF IFNULL(EXISTFLAG,0)=0 AND KIND != 'TMP' AND KIND != 'RERUN' AND KIND !='NT' AND KIND !='AP' AND KIND !='NT_DB_UMTS_LTE' THEN 
		INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'SP_CreateDB','Create DB-1', NOW());
		
		SELECT CONCAT(UMTS_SCHEMA,'/',UMTS_RPT,'/',SP_VERSION) INTO @SP_VERSION FROM gt_gw_main.sp_version LIMIT 1;
		SELECT IFNULL(MAX(SESSION_ID),0)+1  INTO GT_SESSION_ID FROM  gt_gw_main.session_information;
	
		SET @SqlCmd = CONCAT('SELECT GW_URI,AP_URI,DS_AP_URI,IFNULL(RNC_VERSION,'''') INTO @RNC_TO_SEC_GW_URI,@RNC_TO_SEC_AP_URI,@RNC_TO_SEC_DS_AP_URI,@RNC_VERSION FROM gt_gw_main.rnc_information WHERE rnc=',RNC,' and technology = ''UMTS'' ;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
	
		SET @SqlCmd =CONCAT('INSERT INTO gt_gw_main.session_information (SESSION_ID, SESSION_DB ,RNC,FILE_STARTTIME,FILE_ENDTIME,STATUS,IMPORT_TIME,SESSION_START,SESSION_END,SESSION_TYPE,TECHNOLOGY,SP_VERSION,GW_URI,AP_URI,DS_AP_URI,RNC_VERSION ) values 
		(',GT_SESSION_ID,',''',GT_DB,''',''',RNC,''',''',FILE_STARTTIME,''',''',FILE_ENDTIME,''',0, NOW(),''',FILE_STARTTIME,''',''',FILE_ENDTIME,'''
		,''',IF(KIND='DAILY','DAY','TEMP'),''',''UMTS'',''',@SP_VERSION,''',''',@RNC_TO_SEC_GW_URI,''',''',@RNC_TO_SEC_AP_URI,''',''',@RNC_TO_SEC_DS_AP_URI,''',''',@RNC_VERSION,''')');
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
			CALL gt_gw_main.SP_CreateDB_Schema(GT_DB,FILEDATE);
			
			CALL gt_gw_main.SP_CreateDB_Schema_Sub(GT_DB,'umts');
			CALL gt_gw_main.SP_Create_Merge_Rename_Table(GT_DB,'DY',EVENT_NUM,POS_NUM);
			IF CUSTOMER_USER_FLAG = 'true' THEN
				CALL gt_gw_main.SP_Grant(GT_DB);
			END IF;
		ELSEIF KIND = 'HOURLY' THEN
			CALL gt_gw_main.SP_CreateDB_Schema(GT_DB,FILEDATE);
			
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
	
		
	ELSEIF KIND = 'RERUN' THEN
		INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'SP_CreateDB','Create DB-2', NOW());
		CALL gt_gw_main.SP_Copy_Database(GT_DB,CONCAT(GT_DB,'_rerun'));
		
		IF DAILY_JUDGE=TRUE THEN
			SET @SqlCmd =CONCAT('DROP TABLE IF EXISTS ',GT_DB,'_rerun.session_information;');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
			
			SET @SqlCmd =CONCAT('CREATE TABLE ',GT_DB,'_rerun.session_information ENGINE=MYISAM
					     SELECT * FROM gt_gw_main.session_information 
					     WHERE SESSION_DB=''',GT_DB,'_rerun''');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
		END IF;
	ELSEIF KIND = 'TMP' THEN
 		INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'SP_CreateDB','Create DB-3', NOW());
		CALL gt_gw_main.SP_Copy_Database(GT_DB,CONCAT(GT_DB,'_tmp'));
	
		IF DAILY_JUDGE=TRUE THEN
			SET @SqlCmd =CONCAT('DROP TABLE IF EXISTS ',GT_DB,'_tmp.session_information;');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
			
			SET @SqlCmd =CONCAT('CREATE TABLE ',GT_DB,'_tmp.session_information ENGINE=MYISAM
					     SELECT * FROM gt_gw_main.session_information 
					     WHERE SESSION_DB=''',GT_DB,'_tmp''');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
		END IF;
	ELSEIF KIND = 'NT' THEN
		INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'SP_CreateDB','Create DB-4', NOW());
		SET @SqlCmd =CONCAT('CREATE DATABASE IF NOT EXISTS ', NT_DB_NAME);
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
	
		CALL gt_gw_main.SP_CreateDB_NT_Schema(NT_DB_NAME);
		CALL gt_gw_main.SP_CreateDB_NT2_Schema(NT_DB_NAME);
	
	ELSEIF KIND = 'AP' THEN
		INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'SP_CreateDB','Create DB-5', NOW());
		SET @SqlCmd =CONCAT('SELECT COUNT(*) INTO @DB_CNT FROM information_schema.SCHEMATA WHERE SCHEMA_NAME = ''gt_',RNC,'_',FILEDATE,''';');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
	
		IF @DB_CNT = 0 THEN
			CALL gt_gw_main.SP_CreateDB_Schema(GT_DB,FILEDATE);
		END IF;
	END IF;
	
	INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'SP_CreateDB','Create DB-End', NOW());
END$$
DELIMITER ;
