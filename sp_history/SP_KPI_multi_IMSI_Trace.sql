DELIMITER $$
USE `gt_gw_main`$$
DROP PROCEDURE IF EXISTS `SP_CreateDB_LTE`$$
CREATE DEFINER=`covmo`@`%` PROCEDURE `SP_KPI_multi_IMSI_Trace`(IN KPI_ID INT(11) ,IN SESSION_NAME VARCHAR(100),IN START_HOUR TINYINT(2),IN END_HOUR TINYINT(2), IN SOURCE_TYPE TINYINT(2), IN SERVICE TINYINT(2), IN WORKER_ID VARCHAR(10)
							,IN DATA_QUARTER VARCHAR(10),IN CELL_ID VARCHAR(100),IN TILE_ID VARCHAR(100)
							,IN IMSI VARCHAR(4096),IN CLUSTER_ID VARCHAR(50),IN CALL_TYPE VARCHAR(30),IN CALL_STATUS VARCHAR(10)
							,IN INDOOR VARCHAR(5),IN MOVING VARCHAR(5)
							,IN CELL_INDOOR VARCHAR(10),IN FREQUENCY VARCHAR(100) ,IN UARFCN VARCHAR(100),IN CELL_LON VARCHAR(50),IN CELL_LAT VARCHAR(50)
							,IN MSISDN VARCHAR(1024),IN IMEI_NEW VARCHAR(5000),IN APN VARCHAR(1024)
							,IN FILTER VARCHAR(1024),IN PID INT(11),IN POS_KIND VARCHAR(10),IN SITE_ID VARCHAR(100)
							,IN MAKE_ID VARCHAR(1024),IN MODEL_ID VARCHAR(1024),IN POLYGON_STR VARCHAR(250),IN WITHDUMP TINYINT(2),IN ALLRNC TINYINT(2),IN GT_COVMO VARCHAR(20),TECH_NAME VARCHAR(10),IN TMP_DB VARCHAR(100),IN TARGET_TABLE VARCHAR(100))
BEGIN
	DECLARE SP_Process VARCHAR(100) DEFAULT NULL;	
	DECLARE START_TIME DATETIME DEFAULT SYSDATE();
	DECLARE Spider_SP_ERROR CONDITION FOR SQLSTATE '99998';
	DECLARE KPI_ERROR CONDITION FOR SQLSTATE '99999' ;
	DECLARE WORKER_ID VARCHAR(10) DEFAULT CONNECTION_ID();
	DECLARE COLUMN_CRT_STR VARCHAR(1500) DEFAULT 
		' (CALL_ID BIGINT(20) DEFAULT NULL,
		RAB_SEQ_ID TINYINT(2) DEFAULT NULL,
		START_TIME DATETIME DEFAULT NULL,
		END_TIME DATETIME DEFAULT NULL,
		DURATION INT(11) DEFAULT NULL,
		IMSI CHAR(16) DEFAULT NULL,
		MSISDN VARCHAR(20) DEFAULT NULL,
		B_PARTY_NUMBER VARCHAR(15) DEFAULT NULL,
		POS_FIRST_CELL VARCHAR(20) DEFAULT NULL,
		POS_FIRST_LOC BIGINT(20) DEFAULT NULL,
		POS_LAST_CELL VARCHAR(20) DEFAULT NULL,
		POS_LAST_LOC BIGINT(20) DEFAULT NULL,
		START_CELL_ID VARCHAR(50) DEFAULT NULL,
		END_CELL_ID VARCHAR(50) DEFAULT NULL,
		CALL_TYPE VARCHAR(15) DEFAULT NULL,
		CALL_STATUS VARCHAR(11) DEFAULT NULL,
		RELEASE_CAUSE_STR VARCHAR(100) DEFAULT NULL,
		UL_VOLUME DOUBLE DEFAULT NULL,
		DL_VOLUME DOUBLE DEFAULT NULL,
		UL_THROUPUT DOUBLE DEFAULT NULL,
		DL_THROUPUT DOUBLE DEFAULT NULL,
		MANUFACTURER VARCHAR(32) DEFAULT NULL,
		MODEL VARCHAR(200) DEFAULT NULL,
		DS_DATE DATE DEFAULT NULL,
		PU SMALLINT(6) DEFAULT NULL,
		TECH_MASK TINYINT(4) DEFAULT NULL)';
	DECLARE COLUMN_COL_STR VARCHAR(500) DEFAULT 
		'CALL_ID,
		RAB_SEQ_ID,
		START_TIME,
		END_TIME,
		DURATION,
		IMSI,
		MSISDN,
		B_PARTY_NUMBER,
		POS_FIRST_CELL,
		POS_FIRST_LOC,
		POS_LAST_CELL,
		POS_LAST_LOC,
		START_CELL_ID,
		END_CELL_ID,
		CALL_TYPE,
		CALL_STATUS,
		RELEASE_CAUSE_STR,
		UL_VOLUME,
		DL_VOLUME,
		UL_THROUPUT,
		DL_THROUPUT,
		MANUFACTURER,
		MODEL,
		DS_DATE,
		PU,
		TECH_MASK';
	DECLARE EXIT HANDLER FOR 1146
	BEGIN 
		SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS tmp_',WORKER_ID,';');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		SELECT 'No Table' AS IsSuccess;		
	END;		
				
	INSERT INTO gt_gw_main.sp_log VALUES('gt_gw_main','SP_KPI_multi_IMSI_TRACE',CONCAT(KPI_ID,' Start CREATE TABLE tbl_',SESSION_NAME), NOW());	
		
	SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS tmp_',WORKER_ID,';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('CREATE TEMPORARY TABLE tmp_',WORKER_ID,' ',COLUMN_CRT_STR
			,' ENGINE=MYISAM DEFAULT CHARSET=latin1;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('SELECT spider_bg_direct_sql(CONCAT(''CALL gt_gw_main.SP_KPI_IMSI_TRACE_1_5_NEW(''''',SESSION_NAME,''''',',KPI_ID,',',START_HOUR,',',END_HOUR,',',SOURCE_TYPE,',',SERVICE,','
				,CASE WHEN DATA_QUARTER='' THEN '''''''''' ELSE DATA_QUARTER END,','
				,CASE WHEN CELL_ID='' THEN '''''''''' ELSE CONCAT('''''',CELL_ID,'''''') END,','
				,CASE WHEN TILE_ID='' THEN '''''''''' ELSE CONCAT('''''',TILE_ID,'''''') END,','
				,CASE WHEN IMSI='' THEN '''''''''' ELSE CONCAT('''''',IMSI,'''''') END,','
				,CASE WHEN CLUSTER_ID='' THEN '''''''''' ELSE CLUSTER_ID END,','
				,CASE WHEN CALL_TYPE='' THEN '''''''''' ELSE CONCAT('''''',CALL_TYPE,'''''') END,','
				,CASE WHEN CALL_STATUS='' THEN '''''''''' ELSE CONCAT('''''',CALL_STATUS,'''''') END,','
				,CASE WHEN INDOOR='' THEN '''''''''' ELSE CONCAT('''''',INDOOR,'''''') END,','
				,CASE WHEN MOVING='' THEN '''''''''' ELSE CONCAT('''''',MOVING,'''''') END,','
				,CASE WHEN CELL_INDOOR='' THEN '''''''''' ELSE CELL_INDOOR END,','
				,CASE WHEN FREQUENCY='' THEN '''''''''' ELSE CONCAT('''''',FREQUENCY,'''''') END,','
				,CASE WHEN UARFCN='' THEN '''''''''' ELSE CONCAT('''''',UARFCN,'''''') END,','
				,CASE WHEN CELL_LON='' THEN '''''''''' ELSE CELL_LON END,','
				,CASE WHEN CELL_LAT='' THEN '''''''''' ELSE CELL_LAT END,','
				,CASE WHEN MSISDN='' THEN '''''''''' ELSE CONCAT('''''',MSISDN,'''''') END,','
				,CASE WHEN IMEI_NEW='' THEN '''''''''' ELSE CONCAT('''''',IMEI_NEW,'''''') END,','
				,CASE WHEN APN='' THEN '''''''''' ELSE CONCAT('''''',APN,'''''') END,','
				,CASE WHEN FILTER='' THEN '''''''''' ELSE CONCAT('''''',FILTER,'''''') END,','
				,CASE WHEN PID='' THEN '''''''''' ELSE PID END,','
				,CASE WHEN POS_KIND='' THEN '''''''''' ELSE CONCAT('''''',POS_KIND,'''''') END,','
				,CASE WHEN SITE_ID='' THEN '''''''''' ELSE CONCAT('''''',SITE_ID,'''''') END,','
				,CASE WHEN MAKE_ID='' THEN '''''''''' ELSE CONCAT('''''',MAKE_ID,'''''') END,','
				,CASE WHEN MODEL_ID='' THEN '''''''''' ELSE CONCAT('''''',MODEL_ID,'''''') END,','
				,CASE WHEN POLYGON_STR='' THEN '''''''''' ELSE CONCAT('''''',POLYGON_STR,'''''') END,','
				,CASE WHEN WITHDUMP='' THEN '''''0''''' ELSE WITHDUMP END,','
				,CASE WHEN GT_COVMO='' THEN '''''gt_covmo''''' ELSE CONCAT('''''',GT_COVMO,'''''') END,','
				,CASE WHEN TECH_NAME='' THEN '''''''''' ELSE CONCAT('''''',TECH_NAME,'''''') END
				,');'') 
	, ''tmp_',WORKER_ID,'''
	, CONCAT(''HOST '''''',REPLACE(gt_strtok(DS_AP_URI,3,'':''),''/'',''''),'''''', PORT '''''',REPLACE(gt_strtok(DS_AP_URI,4,'':''),''/'',''''),'''''',USER '''''',`DS_AP_USER`,'''''', PASSWORD '''''',`DS_AP_PASSWORD`,'''''''')
	) INTO @bb FROM `',GT_COVMO,'`.`rnc_information` WHERE `RNC`=',gt_strtok(SESSION_NAME,2,'_'),' AND `TECHNOLOGY`=''',TECH_NAME,'''
	;');
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
		INSERT INTO `gt_gw_main`.`tbl_rpt_error` (`PID`,`CreateTime`,`error_str`) VALUES (PID,NOW(),'Spider SP Execute Failed - SP_KPI_multi_IMSI_TRACE');
		SIGNAL KPI_ERROR
			SET MESSAGE_TEXT = 'Spider SP Execute Failed - SP_KPI_multi_IMSI_TRACE';
	END IF;
	
	SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS tmp_',WORKER_ID,';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	INSERT INTO gt_gw_main.sp_log VALUES('gt_gw_main','SP_KPI_multi_IMSI_TRACE',CONCAT(KPI_ID,' Done: ',TIMESTAMPDIFF(SECOND,START_TIME,SYSDATE()),' seconds.'), NOW());	
	
END$$
DELIMITER ;
