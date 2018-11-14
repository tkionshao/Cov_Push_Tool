DELIMITER $$
USE `gt_gw_main`$$
DROP PROCEDURE IF EXISTS `SP_CreateDB_LTE`$$
CREATE DEFINER=`covmo`@`%` PROCEDURE `SP_table_position_export_Webe`(IN GT_DB VARCHAR(100),IN PATH VARCHAR(100),IN TECH_MASK TINYINT(4),IN GT_COVMO VARCHAR(100))
a_label:
BEGIN
	DECLARE START_TIME DATETIME DEFAULT SYSDATE();
	DECLARE RNC_ID INT;
	DECLARE DATA_DATE VARCHAR(8);
	DECLARE DATA_QRT VARCHAR(4);
	DECLARE FOLDER_PATH VARCHAR(20);
	
	SET FOLDER_PATH='/data/Mysql_Export/';	
 	SELECT gt_strtok(GT_DB,2,'_') INTO RNC_ID;
 	SELECT gt_strtok(GT_DB,3,'_') INTO DATA_DATE;
 	SELECT gt_strtok(GT_DB,4,'_') INTO DATA_QRT;
	SET @GT_DB = CONCAT('gt_',RNC_ID,'_',DATA_DATE,'_0000_0000');
	
	SET @SqlCmd=CONCAT('
				SELECT ''IMSI'', ''IMEI'',''DATE_TIME'', ''MAKE_ID'',''MODEL_ID'' ,''CELL_ID'' , ''ENODEB_ID'', ''LATITUDE'', ''LONGITUDE'', ''RSRP'', ''RSRQ'', ''Call_TYPE'', ''Call_STATUS'', ''TECHNOLOGY'', ''EVENT_ID'', ''INDOOR''
				UNION ALL
				SELECT IMSI AS IMSI, 
				IMEI AS IMEI,
				DATE_TIME AS DATE_TIME,
				MAKE_ID AS MAKE_ID,
				MODEL_ID AS MODEL_ID,
				CELL_ID AS CELL_ID,
				ENODEB_ID AS ENODEB_ID,
				gt_covmo_proj_geohash_to_lat(LOC_ID) AS LATITUDE,
				gt_covmo_proj_geohash_to_lng(LOC_ID) AS LONGITUDE,
				RSRP AS RSRP,
				RSRQ AS RSRQ,
				Call_TYPE AS Call_TYPE,
				Call_STATUS AS Call_STATUS,
				''LTE'' AS TECHNOLOGY,
				EVENT_ID AS EVENT_ID,
				INDOOR AS INDOOR 
				FROM ',@GT_DB,'.table_position_convert_serving_lte_',DATA_QRT,' 
				WHERE IMSI IS NOT NULL AND LOC_ID IS NOT NULL AND event_id IN (0,1,200)
				INTO OUTFILE ''',FOLDER_PATH,'gt_table_pos_',RNC_ID,'_',DATA_DATE,'_',DATA_QRT,'_lte.csv ''
				FIELDS TERMINATED BY '',''
				OPTIONALLY ENCLOSED BY ''''
				LINES TERMINATED BY ''\n'';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	INSERT INTO gt_gw_main.sp_log VALUES('gt_gw_main','SP_table_position_export_Webe',CONCAT(CONNECTION_ID(),' Done: ',TIMESTAMPDIFF(SECOND,START_TIME,SYSDATE()),' seconds.'), NOW());
END$$
DELIMITER ;
