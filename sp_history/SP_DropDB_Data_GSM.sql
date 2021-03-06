DELIMITER $$
USE `gt_gw_main`$$
DROP PROCEDURE IF EXISTS `SP_CreateDB_LTE`$$
CREATE DEFINER=`covmo`@`%` PROCEDURE `SP_DropDB_Data_GSM`(IN GT_DB VARCHAR(100),IN WHICHONE VARCHAR(30),IN GT_COVMO VARCHAR(100))
BEGIN
	DECLARE GT_SESSION_ID INT;
	DECLARE NT_EXIST_FLAG INT;
	DECLARE REPLACE_NT_SESSION_ID INT;	
	DECLARE START_TIME DATETIME DEFAULT SYSDATE();
	DECLARE ORG_NT_DATE VARCHAR(20) DEFAULT SUBSTRING(RIGHT(GT_DB,18),1,8);
	DECLARE PU_ID VARCHAR(20);
	DECLARE NT_DATE VARCHAR(20) DEFAULT CONCAT(SUBSTRING(ORG_NT_DATE,1,4),'-',SUBSTRING(ORG_NT_DATE,5,2),'-',SUBSTRING(ORG_NT_DATE,7,2));
	DECLARE NT_DB VARCHAR(50);
	
	INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'SP_DropDB_Data','SP_DropDB_Data Start ', NOW());
	SELECT gt_strtok(GT_DB,2,'_') INTO PU_ID; 
	IF WHICHONE = 'DAILY' THEN 
		INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'SP_DropDB_Data','SP_DropDB_Data DAILY ', NOW());
		
		SET @SqlCmd=CONCAT('DELETE FROM gt_gw_main.session_information WHERE SESSION_DB=''',GT_DB,''';');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
	
		SET @SqlCmd=CONCAT('DELETE FROM gt_gw_main.table_call_cnt WHERE DATA_DATE = ''',NT_DATE,''';');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
			
		SET @SqlCmd =CONCAT('DROP DATABASE IF EXISTS ', GT_DB);
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
	
	ELSEIF WHICHONE = 'HOURLY' THEN 
		INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'SP_DropDB_Data','SP_DropDB_Data HOURLY ', NOW());
		
		SET @SqlCmd=CONCAT('DELETE FROM gt_gw_main.session_information WHERE SESSION_DB=''',GT_DB,''';');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
	
		SET @SqlCmd =CONCAT('DROP DATABASE IF EXISTS ', GT_DB);
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
	
	ELSEIF WHICHONE = 'WEEKLY' THEN 
		INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'SP_DropDB_Data','SP_DropDB_Data WEEKLY ', NOW());
		
		SET @SqlCmd=CONCAT('DELETE FROM gt_gw_main.session_information WHERE SESSION_DB=''',GT_DB,''';');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
	
		SET @SqlCmd =CONCAT('DROP DATABASE IF EXISTS ', GT_DB);
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
	
	ELSEIF WHICHONE = 'AP' THEN
		INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'SP_DropDB_Data','SP_DropDB_Data AP ', NOW());
	
		SET @SqlCmd=CONCAT('DELETE FROM ',GT_COVMO,'.session_information WHERE SESSION_DB=''',GT_DB,''';');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
	
		SET @SqlCmd=CONCAT('DELETE FROM ',GT_COVMO,'.table_call_cnt WHERE DATA_DATE = ''',NT_DATE,''' AND PU_ID = ',PU_ID,';');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
	
		SET @SqlCmd =CONCAT('DROP DATABASE IF EXISTS ', GT_DB);
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
	
	ELSEIF WHICHONE = 'REPORT_TMP' THEN
		INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'SP_DropDB_Data','SP_DropDB_Data REPORT_TMP ', NOW());
		
		SET @SqlCmd =CONCAT('DROP DATABASE IF EXISTS ', GT_DB , '_tmp');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
	ELSEIF WHICHONE = 'REPORT_RERUN' THEN
		INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'SP_DropDB_Data','SP_DropDB_Data REPORT_RERUN ', NOW());
		
		SET @SqlCmd =CONCAT('DROP DATABASE IF EXISTS ', GT_DB , '_rerun');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
	ELSEIF WHICHONE = 'NT' THEN
		INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'SP_DropDB_Data','SP_DropDB_Data NT DB ', NOW());
		
		SET NT_DB = GT_DB;
	
		SET @SqlCmd =CONCAT('DROP DATABASE IF EXISTS ', GT_DB );
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
	END IF;
	
	INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'SP_DropDB_Data',CONCAT('Done: ',TIMESTAMPDIFF(SECOND,START_TIME,SYSDATE()),' seconds.'), NOW());
END$$
DELIMITER ;
