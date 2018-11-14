DELIMITER $$
USE `gt_gw_main`$$
DROP PROCEDURE IF EXISTS `SP_CreateDB_LTE`$$
CREATE DEFINER=`covmo`@`%` PROCEDURE `SP_KPI_multi_split_E2E`(IN KPI_ID INT(11) ,IN SESSION_NAME VARCHAR(100),IN START_TIME VARCHAR(50),IN END_TIME VARCHAR(50), IN WORKER_ID VARCHAR(10)
							,IN IMSI MEDIUMTEXT,IN GT_COVMO VARCHAR(20),TECH_NAME VARCHAR(10)
							,IN TMP_DB VARCHAR(100),IN TARGET_TABLE VARCHAR(100)
							,IN COLUMN_CRT_STR VARCHAR(3000),IN COLUMN_COL_STR VARCHAR(2500),IN SP_NAME VARCHAR(100),IN DS_AP_IP VARCHAR(20),IN DS_AP_PORT VARCHAR(5)
							,IN DS_AP_USER VARCHAR(32),IN DS_AP_PASSWORD VARCHAR(32)
							)
BEGIN
	DECLARE SP_Process VARCHAR(100) DEFAULT NULL;	
	DECLARE SP_START_TIME DATETIME DEFAULT SYSDATE();
	DECLARE KPI_ERROR CONDITION FOR SQLSTATE '99999' ;
	DECLARE WORKER_ID VARCHAR(10) DEFAULT CONNECTION_ID();
	DECLARE IMSI_STR MEDIUMTEXT DEFAULT '';
	DECLARE IMSI_CONCAT MEDIUMTEXT DEFAULT '';
	
	DECLARE EXIT HANDLER FOR 1146	
	BEGIN
		SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS tmp_',WORKER_ID,';');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		SELECT 'No Table' AS IsSuccess;	
		
		SELECT '{tech:�ALL �, name:�SP-Report�, status:�2�,message_id: �null�, message: �SP_KPI_multi_split Failed Table does not exist. Check necessary table first�, log_path: ��}' AS message;
	END;		
		
	SET SESSION group_concat_max_len=@@max_allowed_packet;
				
	INSERT INTO gt_gw_main.sp_log VALUES('gt_gw_main','SP_KPI_multi_split_E2E',CONCAT(KPI_ID,' Start CREATE TABLE tbl_',SESSION_NAME), NOW());	
	
 	SET IMSI_STR=IMSI;
	
	SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS tmp_',WORKER_ID,';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('CREATE TEMPORARY TABLE tmp_',WORKER_ID,' (',COLUMN_CRT_STR,')'
			,' ENGINE=MYISAM DEFAULT CHARSET=latin1;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('SELECT spider_bg_direct_sql(CONCAT(''CALL gt_gw_main.SP_KPI_multi_remote_E2E(''''',SESSION_NAME,''''',',KPI_ID,',''''',START_TIME,''''',''''',END_TIME,''''','
				,CASE WHEN GT_COVMO='' THEN '''''gt_covmo''''' ELSE CONCAT('''''',GT_COVMO,'''''') END,','
				,CASE WHEN TECH_NAME='' THEN '''''''''' ELSE CONCAT('''''',TECH_NAME,'''''') END,','
 				,CASE WHEN IMSI_STR='' THEN '''''''''' ELSE CONCAT('''''',IMSI_STR,'''''') END
				,');'') 
	, ''tmp_',WORKER_ID,'''
	, CONCAT(''HOST ''''',DS_AP_IP,''''', PORT ''''',DS_AP_PORT,''''',USER ''''',DS_AP_USER,''''', PASSWORD ''''',DS_AP_PASSWORD,''''''')
	) INTO @bb ;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	IF @bb=1 THEN 	
		SET @SqlCmd=CONCAT('INSERT INTO ',TARGET_TABLE,' 
		SELECT ',COLUMN_COL_STR,'
		FROM
		tmp_',WORKER_ID,' ;');
	
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
	ELSE
		INSERT INTO `gt_gw_main`.`tbl_rpt_error` (`PID`,`CreateTime`,`error_str`) VALUES (PID,NOW(),'Spider SP Execute Failed - SP_KPI_multi_split');
		SIGNAL KPI_ERROR
			SET MESSAGE_TEXT = 'Spider SP Execute Failed - SP_KPI_multi_split';
	END IF;
	
	SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS tmp_',WORKER_ID,';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	INSERT INTO gt_gw_main.sp_log VALUES('gt_gw_main','SP_KPI_multi_split_E2E',CONCAT(KPI_ID,' Done: ',TIMESTAMPDIFF(SECOND,SP_START_TIME,SYSDATE()),' seconds.'), NOW());	
	
END$$
DELIMITER ;
