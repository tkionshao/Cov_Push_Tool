DELIMITER $$
USE `gt_gw_main`$$
DROP PROCEDURE IF EXISTS `SP_CreateDB_LTE`$$
CREATE DEFINER=`covmo`@`%` PROCEDURE `SP_table_call_export_MI`(IN PU_ID VARCHAR(1000),IN START_DATE DATE)
a_label:
BEGIN
	DECLARE START_TIME DATETIME DEFAULT SYSDATE();
	DECLARE DATA_DATE VARCHAR(8);
	DECLARE PID VARCHAR(10) DEFAULT CONNECTION_ID();
	DECLARE FOLDER_PATH VARCHAR(50);
	DECLARE GT_DB VARCHAR(50);
	DECLARE EXIT HANDLER FOR 1146
	BEGIN 
		SELECT 'No data available.';
	
		SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS tmp_materialization_',PID,';');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
	
		SELECT '{tech:�ALL �, name:�SP-Report�, status:�2�,message_id: �null�, message: �SP_table_call_export_MI Failed Table does not exist. Check necessary table first.�, log_path: ��}' AS message;
	END;
	
	SET FOLDER_PATH='/data/mi/';
 	SET DATA_DATE=DATE_FORMAT(START_DATE,'%Y%m%d');
 	SET GT_DB=CONCAT('gt_',PU_ID,'_',DATA_DATE,'_0000_0000');
 	
	SET @SqlCmd=CONCAT('SELECT B.`TECHNOLOGY` INTO @PU_TECH 
				FROM `gt_covmo`.`rnc_information` B
				WHERE `RNC`=',PU_ID,';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	IF IFNULL(@PU_TECH,'')<>'' THEN 		
		IF @PU_TECH='UMTS' THEN 
			SET @SqlCmd=CONCAT('
						SELECT ''CALL_ID'' AS CALL_ID,''IMSI'' AS IMSI,''MAKE_ID'' AS MAKE_ID,''MODEL_ID'' AS MODEL_ID,''START_TIME'' AS START_TIME,''END_TIME'' AS END_TIME,
						''FIRST_POS_LAT'' AS FIRST_POS_LAT,''FIRST_POS_LON'' AS FIRST_POS_LON,''LAST_POS_LAT'' AS LAST_POS_LAT,''LAST_POS_LON'' AS LAST_POS_LON,''CALL_TYPE'' AS CALL_TYPE,''INDOOR'' AS INDOOR,''MOVING'' AS MOVING,
						''POS_FIRST_RSCP'' AS POS_FIRST_RSCP,
						''POS_FIRST_ECN0'' AS POS_FIRST_ECN0,
						''POS_FIRST_RSRP'' AS POS_FIRST_RSRP,
						''POS_FIRST_RSRQ'' AS POS_FIRST_RSRQ,
						''START_RNC_ID'' AS START_RNC_ID,
						''TECH_MASK'' AS TECH_MASK
						UNION
						SELECT CALL_ID,IMSI,IFNULL(MAKE_ID,0) AS MAKE_ID,IFNULL(MODEL_ID,0) AS MODEL_ID,UNIX_TIMESTAMP(START_TIME) AS START_TIME,UNIX_TIMESTAMP(END_TIME) AS END_TIME,
							GT_COVMO_PROJ_GEOHASH_TO_LAT(POS_FIRST_LOC) AS FIRST_POS_LAT,
							GT_COVMO_PROJ_GEOHASH_TO_LNG(POS_FIRST_LOC) AS FIRST_POS_LON,
							GT_COVMO_PROJ_GEOHASH_TO_LAT(POS_LAST_LOC) AS LAST_POS_LAT,
							GT_COVMO_PROJ_GEOHASH_TO_LNG(POS_LAST_LOC) AS LAST_POS_LON,								
							CALL_TYPE,INDOOR,MOVING,
							POS_FIRST_RSCP,
							POS_FIRST_ECN0,
							NULL AS POS_FIRST_RSRP,
							NULL AS POS_FIRST_RSRQ,
							START_RNC_ID,
							2 AS TECH_MASK 
						FROM ',GT_DB,'.`table_call` 
						WHERE IMSI IS NOT NULL AND `POS_FIRST_LOC` IS NOT NULL
						INTO OUTFILE "',FOLDER_PATH,'table_call_',PU_ID,'_',DATA_DATE,'_',@PU_TECH ,'.csv"
						FIELDS TERMINATED BY '',''
						OPTIONALLY ENCLOSED BY ''''
						LINES TERMINATED BY ''\\n'';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
			
			SET @SqlCmd=CONCAT('
						SELECT ''CALL_ID'' AS CALL_ID,''ConfiIndex'' AS ConfiIndex
						UNION
						SELECT CALL_ID, CAUSE AS ConfiIndex
						FROM ',GT_DB,'.table_position 
						WHERE SEQ_ID = 1
						INTO OUTFILE "',FOLDER_PATH,'table_position_',PU_ID,'_',DATA_DATE,'_',@PU_TECH ,'.csv"
						FIELDS TERMINATED BY '',''
						OPTIONALLY ENCLOSED BY ''''
						LINES TERMINATED BY ''\\n'';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
		ELSEIF @PU_TECH='LTE' THEN 
			SET @SqlCmd=CONCAT('
						SELECT ''CALL_ID'' AS CALL_ID,''IMSI'' AS IMSI,''MAKE_ID'' AS MAKE_ID,''MODEL_ID'' AS MODEL_ID,''START_TIME'' AS START_TIME,''END_TIME'' AS END_TIME,
						''FIRST_POS_LAT'' AS FIRST_POS_LAT,''FIRST_POS_LON'' AS FIRST_POS_LON,''LAST_POS_LAT'' AS LAST_POS_LAT,''LAST_POS_LON'' AS LAST_POS_LON,''CALL_TYPE'' AS CALL_TYPE,''INDOOR'' AS INDOOR,''MOVING'' AS MOVING,
						''POS_FIRST_RSCP'' AS POS_FIRST_RSCP,
						''POS_FIRST_ECN0'' AS POS_FIRST_ECN0,
						''POS_FIRST_RSRP'' AS POS_FIRST_RSRP,
						''POS_FIRST_RSRQ'' AS POS_FIRST_RSRQ,
						''START_RNC_ID'' AS START_RNC_ID,
						''TECH_MASK'' AS TECH_MASK
						UNION
						SELECT CALL_ID,IMSI,IFNULL(MAKE_ID,0) AS MAKE_ID,IFNULL(MODEL_ID,0) AS MODEL_ID,UNIX_TIMESTAMP(START_TIME) AS START_TIME,UNIX_TIMESTAMP(END_TIME) AS END_TIME,
							GT_COVMO_PROJ_GEOHASH_TO_LAT(POS_FIRST_LOC) AS FIRST_POS_LAT,
							GT_COVMO_PROJ_GEOHASH_TO_LNG(POS_FIRST_LOC) AS FIRST_POS_LON,
							GT_COVMO_PROJ_GEOHASH_TO_LAT(POS_LAST_LOC) AS LAST_POS_LAT,
							GT_COVMO_PROJ_GEOHASH_TO_LNG(POS_LAST_LOC) AS LAST_POS_LON,
							CALL_TYPE,INDOOR,MOVING,
							NULL AS POS_FIRST_RSCP,
							NULL AS POS_FIRST_ECN0,
							POS_FIRST_RSRP,
							POS_FIRST_RSRQ,
							NULL AS START_RNC_ID,
							4 AS TECH_MASK 
						FROM ',GT_DB,'.`table_call_lte` 
						WHERE IMSI IS NOT NULL AND `POS_FIRST_LOC` IS NOT NULL
						INTO OUTFILE "',FOLDER_PATH,'table_call_',PU_ID,'_',DATA_DATE,'_',@PU_TECH ,'.csv"
						FIELDS TERMINATED BY '',''
						OPTIONALLY ENCLOSED BY ''''
						LINES TERMINATED BY ''\\n'';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
			
			SET @SqlCmd=CONCAT('
						SELECT ''CALL_ID'' AS CALL_ID,''ConfiIndex'' AS ConfiIndex
						UNION
						SELECT CALL_ID, CAUSE
						FROM ',GT_DB,'.table_position_convert_serving_lte 
						WHERE SEQ_ID = 1
						INTO OUTFILE "',FOLDER_PATH,'table_position_',PU_ID,'_',DATA_DATE,'_',@PU_TECH ,'.csv"
						FIELDS TERMINATED BY '',''
						OPTIONALLY ENCLOSED BY ''''
						LINES TERMINATED BY ''\\n'';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
		END IF;
	END IF;
	INSERT INTO gt_gw_main.sp_log VALUES('gt_gw_main','SP_table_call_export',CONCAT(CONNECTION_ID(),' Done: ',TIMESTAMPDIFF(SECOND,START_TIME,SYSDATE()),' seconds.'), NOW());
END$$
DELIMITER ;
