DELIMITER $$
USE `gt_gw_main`$$
DROP PROCEDURE IF EXISTS `SP_CreateDB_LTE`$$
CREATE DEFINER=`covmo`@`%` PROCEDURE `SP_table_call_export`(IN GT_DB VARCHAR(100),IN TECH_MASK TINYINT(4),IN GT_COVMO VARCHAR(100))
a_label:
BEGIN
	DECLARE START_TIME DATETIME DEFAULT SYSDATE();
	DECLARE RNC_ID INT;
	DECLARE DATA_DATE VARCHAR(8);
	DECLARE DATA_QRT VARCHAR(4);
	DECLARE FOLDER_PATH VARCHAR(20);
	
	SET FOLDER_PATH='//data//Mysql_Export//';	
 	SELECT gt_strtok(GT_DB,2,'_') INTO RNC_ID;
 	SELECT gt_strtok(GT_DB,3,'_') INTO DATA_DATE;
 	SELECT gt_strtok(GT_DB,4,'_') INTO DATA_QRT;
	
	IF TECH_MASK=1 THEN 
		SET @SqlCmd=CONCAT('
					SELECT ''IMSI'' AS IMSI,''IMEI'' AS IMEI,''MSISDN'' AS MSISDN,''START_TIME'' AS START_TIME,''END_TIME'' AS END_TIME,''call_type'' AS call_type,''END_LONG'' AS END_LONG,''END_LAT'' AS END_LAT
					UNION
					SELECT A.IMSI, A.IMEI, B.`MSISDN`, A.`START_TIME`, A.`END_TIME`, A.`CALL_TYPE`
						,gt_covmo_proj_geohash_to_lng(A.POS_LAST_LOC) AS END_LONG
						,gt_covmo_proj_geohash_to_lat(A.POS_LAST_LOC) AS END_LAT
					FROM `table_call_gsm` A
					JOIN ',GT_COVMO,'.`dim_msisdn` B
					ON A.IMSI=B.IMSI
					WHERE A.IMSI IS NOT NULL AND A.`POS_FIRST_LOC` IS NOT NULL AND A.`POS_LAST_LOC` IS NOT NULL AND A.`CALL_STATUS` = ''2''
					INTO OUTFILE ',FOLDER_PATH,'table_call_',RNC_ID,'_',DATA_DATE,'_',DATA_QRT,'_',TECH_MASK,'.csv
					FIELDS TERMINATED BY ''\t''
					OPTIONALLY ENCLOSED BY ''''
					LINES TERMINATED BY ''\n'';');
	ELSEIF TECH_MASK=2 THEN 
		SET @SqlCmd=CONCAT('
					SELECT ''IMSI'' AS IMSI,''IMEI'' AS IMEI,''MSISDN'' AS MSISDN,''START_TIME'' AS START_TIME,''END_TIME'' AS END_TIME,''call_type'' AS call_type,''END_LONG'' AS END_LONG,''END_LAT'' AS END_LAT
					UNION
					SELECT A.IMSI, A.IMEI, B.`MSISDN`, A.`START_TIME`, A.`END_TIME`, A.`CALL_TYPE`
						,gt_covmo_proj_geohash_to_lng(A.POS_LAST_LOC) AS END_LONG
						,gt_covmo_proj_geohash_to_lat(A.POS_LAST_LOC) AS END_LAT
					FROM `table_call`  A
					JOIN ',GT_COVMO,'.`dim_msisdn` B
					ON A.IMSI=B.IMSI
					WHERE A.IMSI IS NOT NULL AND `POS_FIRST_LOC` IS NOT NULL AND `POS_LAST_LOC` IS NOT NULL AND call_status = ''2''
					INTO OUTFILE ',FOLDER_PATH,'table_call_',RNC_ID,'_',DATA_DATE,'_',DATA_QRT,'_',TECH_MASK,'.csv
					FIELDS TERMINATED BY ''\t''
					OPTIONALLY ENCLOSED BY ''''
					LINES TERMINATED BY ''\n'';');
	ELSEIF TECH_MASK=4 THEN 
		SET @SqlCmd=CONCAT('
					SELECT ''IMSI'' AS IMSI,''IMEI'' AS IMEI,''MSISDN'' AS MSISDN,''START_TIME'' AS START_TIME,''END_TIME'' AS END_TIME,''call_type'' AS call_type,''END_LONG'' AS END_LONG,''END_LAT'' AS END_LAT
					UNION
					SELECT A.IMSI, A.IMEI, B.`MSISDN`, A.`START_TIME`, A.`END_TIME`, A.`CALL_TYPE`
						,gt_covmo_proj_geohash_to_lng(A.POS_LAST_LOC) AS END_LONG
						,gt_covmo_proj_geohash_to_lat(A.POS_LAST_LOC) AS END_LAT
					FROM `table_call_gsm` A
					JOIN ',GT_COVMO,'.`dim_msisdn` B
					ON A.IMSI=B.IMSI
					WHERE A.IMSI IS NOT NULL AND A.`POS_FIRST_LOC` IS NOT NULL AND A.`POS_LAST_LOC` IS NOT NULL AND A.`CALL_STATUS` = ''2''
					INTO OUTFILE ',FOLDER_PATH,'table_call_',RNC_ID,'_',DATA_DATE,'_',DATA_QRT,'_',TECH_MASK,'.csv
					FIELDS TERMINATED BY ''\t''
					OPTIONALLY ENCLOSED BY ''''
					LINES TERMINATED BY ''\n'';');
	END IF;
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	INSERT INTO gt_gw_main.sp_log VALUES('gt_gw_main','SP_table_call_export',CONCAT(CONNECTION_ID(),' Done: ',TIMESTAMPDIFF(SECOND,START_TIME,SYSDATE()),' seconds.'), NOW());
END$$
DELIMITER ;
