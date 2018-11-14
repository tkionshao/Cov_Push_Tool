CREATE DEFINER=`covmo`@`%` PROCEDURE `SP_Generate_NT_GSM`(IN GT_DB VARCHAR(100),IN GT_COVMO VARCHAR(100))
BEGIN
	
	DECLARE START_TIME DATETIME DEFAULT SYSDATE();
	DECLARE SESSION_DATE CHAR(10);
	DECLARE FILEDATE CHAR(8) DEFAULT NULL;
	DECLARE WORKER_ID VARCHAR(10) DEFAULT CONNECTION_ID();
	
	SELECT gt_strtok(GT_DB,3,'_') INTO FILEDATE;
	SET SESSION_DATE = CONCAT(LEFT(FILEDATE,4),'-',SUBSTRING(FILEDATE,5,2),'-',SUBSTRING(FILEDATE,7,2));
	
	INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'SP_Generate_NT_GSM','Start', NOW());
	
	INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'SP_Generate_NT_GSM','Insert Data to nt_cell_current_gsm', NOW());
	
	SET @SqlCmd=CONCAT('SELECT max_longitude,max_latitude,min_longitude,min_latitude INTO @max_long,@max_lat,@min_long,@min_lat 
			FROM ',GT_COVMO,'.dim_mcc WHERE mcc IN (
				SELECT DISTINCT mcc FROM ',GT_COVMO,'.sys_mnc
			);');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt; 
	
	SET @SqlCmd=CONCAT('CREATE TABLE ',GT_DB,'.nt_cell_current_gsm_dump LIKE ',GT_DB,'.nt_cell_current_gsm');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt; 
	
	INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'SP_Generate_NT_GSM','nt_cell_current_gsm - dump', NOW());
	
	CALL gt_gw_main.SP_Generate_NT_Sub_Dump(GT_DB,'nt_cell_gsm','nt_cell_current_gsm_dump','BSC_ID','GSM');
	CALL gt_gw_main.SP_Generate_NT_Sub_Dump(GT_DB,'nt_cell_gsm','nt_cell_current_gsm_dump','SITE_ID','GSM');
	CALL gt_gw_main.SP_Generate_NT_Sub_Dump(GT_DB,'nt_cell_gsm','nt_cell_current_gsm_dump','LAC','GSM');
	CALL gt_gw_main.SP_Generate_NT_Sub_Dump(GT_DB,'nt_cell_gsm','nt_cell_current_gsm_dump','CELL_ID','GSM');
	CALL gt_gw_main.SP_Generate_NT_Sub_Dump(GT_DB,'nt_cell_gsm','nt_cell_current_gsm_dump','BCCH_ARFCN','GSM');
	CALL gt_gw_main.SP_Generate_NT_Sub_Dump(GT_DB,'nt_cell_gsm','nt_cell_current_gsm_dump','BSIC','GSM');
	CALL gt_gw_main.SP_Generate_NT_Sub_Dump(GT_DB,'nt_cell_gsm','nt_cell_current_gsm_dump','BANDINDEX','GSM');
	CALL gt_gw_main.SP_Generate_NT_Sub_Dump_Special(GT_DB,'nt_cell_gsm','nt_cell_current_gsm_dump','','GSM','location');
	
	INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'SP_Generate_NT','nt_cell_current_gsm - check', NOW());
	
	SET @SqlCmd=CONCAT('INSERT INTO ',GT_DB,'.nt_cell_current_gsm SELECT * FROM ',GT_DB,'.nt_cell_gsm;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt; 
	
	CALL gt_gw_main.SP_Generate_NT_Sub_Check(GT_DB,'nt_cell_current_gsm','CELL_NAME','','CELL_ID','GSM');
	CALL gt_gw_main.SP_Generate_NT_Sub_Check(GT_DB,'nt_cell_current_gsm','INDOOR','0to1','0','GSM');
	CALL gt_gw_main.SP_Generate_NT_Sub_Check(GT_DB,'nt_cell_current_gsm','SITE_TYPE','1to6','2','GSM');
	
	
	INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'SP_Generate_NT_GSM','Insert Data to nt_antenna_current_gsm', NOW());
	
	SET @SqlCmd=CONCAT('CREATE TABLE ',GT_DB,'.nt_antenna_current_gsm_dump LIKE ',GT_DB,'.nt_antenna_current_gsm');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt; 
	
	SET @SqlCmd=CONCAT('
	UPDATE ',GT_DB,'.nt_antenna_gsm A
	SET antenna_model = "DEFAULT_SECTOR" 
	WHERE A.antenna_model IS NULL AND A.ANTENNA_TYPE=2;
	');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt; 
	
	SET @SqlCmd=CONCAT('
	UPDATE ',GT_DB,'.nt_antenna_gsm A
	SET antenna_model = "DEFAULT_OMNI" 
	WHERE A.antenna_model IS NULL AND A.ANTENNA_TYPE=1;
	');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt; 
	
	SET @SqlCmd=CONCAT('
	UPDATE ',GT_DB,'.nt_antenna_gsm A
	SET antenna_model = "DEFAULT_OMNI" 
	WHERE A.antenna_model IS NULL AND A.ANTENNA_TYPE=3;
	');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt; 
	
	INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'SP_Generate_NT','nt_antenna_current_gsm - dump', NOW());
	
	CALL gt_gw_main.SP_Generate_NT_Sub_Dump(GT_DB,'nt_antenna_gsm','nt_antenna_current_gsm_dump','BSC_ID','GSM');
	CALL gt_gw_main.SP_Generate_NT_Sub_Dump(GT_DB,'nt_antenna_gsm','nt_antenna_current_gsm_dump','CELL_ID','GSM');
	CALL gt_gw_main.SP_Generate_NT_Sub_Dump(GT_DB,'nt_antenna_gsm','nt_antenna_current_gsm_dump','LAC','GSM');
	CALL gt_gw_main.SP_Generate_NT_Sub_Dump(GT_DB,'nt_antenna_gsm','nt_antenna_current_gsm_dump','ANTENNA_MODEL','GSM');
	CALL gt_gw_main.SP_Generate_NT_Sub_Dump(GT_DB,'nt_antenna_gsm','nt_antenna_current_gsm_dump','AZIMUTH','GSM');
	CALL gt_gw_main.SP_Generate_NT_Sub_Dump_Special(GT_DB,'nt_antenna_gsm','nt_antenna_current_gsm_dump','','GSM','location');	
	CALL gt_gw_main.SP_Generate_NT_Sub_Dump_Special(GT_DB,'nt_antenna_gsm','nt_antenna_current_gsm_dump','','GSM','mapping_with_cell');
	
	INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'SP_Generate_NT','nt_antenna_current_gsm - check', NOW());
	
	SET @SqlCmd=CONCAT('INSERT INTO ',GT_DB,'.nt_antenna_current_gsm
				SELECT * FROM ',GT_DB,'.nt_antenna_gsm A;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt; 
	
	SET @SqlCmd=CONCAT('UPDATE ',GT_DB,'.nt_antenna_current_gsm
						SET FLAG = 0;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt; 	
	
	CALL gt_gw_main.SP_Generate_NT_Sub_Check(GT_DB,'nt_antenna_current_gsm','CELL_NAME','','CELL_ID','GSM');
	CALL gt_gw_main.SP_Generate_NT_Sub_Check(GT_DB,'nt_antenna_current_gsm','AZIMUTH','>=360','mod(AZIMUTH, 360)','GSM');
	CALL gt_gw_main.SP_Generate_NT_Sub_Check(GT_DB,'nt_antenna_current_gsm','BEAMWIDTH_V','1to360','7','GSM');
	CALL gt_gw_main.SP_Generate_NT_Sub_Check(GT_DB,'nt_antenna_current_gsm','BCCH_POWER','1to100','33','GSM');
	CALL gt_gw_main.SP_Generate_NT_Sub_Check(GT_DB,'nt_antenna_current_gsm','DOWNTILT_EL','-90to90','0','GSM');
	CALL gt_gw_main.SP_Generate_NT_Sub_Check(GT_DB,'nt_antenna_current_gsm','DOWNTILT_MEC','-90to90','0','GSM');
	CALL gt_gw_main.SP_Generate_NT_Sub_Check_Special(GT_DB,'nt_antenna_current_gsm','ANTENNA_TYPE','1to3','GSM');
	CALL gt_gw_main.SP_Generate_NT_Sub_Check_Special(GT_DB,'nt_antenna_current_gsm','HEIGHT','0to1000','GSM');
	CALL gt_gw_main.SP_Generate_NT_Sub_Check_Special(GT_DB,'nt_antenna_current_gsm','BEAMWIDTH_H','1to360','GSM');
	CALL gt_gw_main.SP_Generate_NT_Sub_Check_Special(GT_DB,'nt_antenna_current_gsm','ANTENNA_GAIN','0to30','GSM');
	CALL gt_gw_main.SP_Generate_NT_Sub_Check_Special(GT_DB,'nt_antenna_current_gsm','INDOOR_TYPE','','GSM');
	
	SET @SqlCmd=CONCAT('UPDATE ',GT_DB,'.nt_antenna_current_gsm SET EIRP = BCCH_POWER+ANTENNA_GAIN-FEEDER_LOSS;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	CALL gt_gw_main.SP_Generate_NT_Sub_Check(GT_DB,'nt_antenna_current_gsm','EIRP','0to100','48','GSM');
	
	INSERT INTO gt_gw_main.SP_LOG VALUES(GT_DB,'SP_Generate_NT_GSM','dump cell if not in antenna', NOW());
	CALL gt_gw_main.SP_Generate_NT_Sub_Dump_Special(GT_DB,'nt_cell_current_gsm','nt_cell_current_gsm_dump','','GSM','mapping_with_antenna');
	
	INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'SP_Generate_NT_GSM','Update nt_antenna_current_gsm ANTENNA_TYPE', NOW());
	
	
	INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'SP_Generate_NT_GSM','Insert Data to nt_neighbor_current_gsm', NOW());
	SET @SqlCmd=CONCAT('	INSERT INTO ',GT_DB,'.nt_neighbor_current_gsm
				SELECT DISTINCT * FROM ',GT_DB,'.nt_neighbor_gsm 
				WHERE BSC_ID IS NOT NULL AND CELL_ID IS NOT NULL AND NBR_BSC_ID IS NOT NULL AND NBR_CELL_ID IS NOT NULL
			; ');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt; 
	INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'SP_Generate_NT_GSM','Insert Data to nt_bsc_current', NOW());
	
	SET @SqlCmd=CONCAT('	INSERT INTO ',GT_DB,'.nt_bsc_current
				(BSC_ID,BSC_NAME,MCC,MNC,TECH_MASK,VENDOR,REGION,SUB_REGION,BSC_MODEL,SW_VERSION)
				SELECT DISTINCT 
				BSC_ID,BSC_NAME,MCC,
				CASE WHEN LENGTH(MNC)=1 THEN CONCAT(0,MNC) ELSE MNC END
				,TECH_MASK,VENDOR,REGION,SUB_REGION,BSC_MODEL,SW_VERSION
				FROM ',GT_DB,'.nt_bsc 
				WHERE BSC_ID IS NOT NULL AND BSC_ID <> ''''
				; ');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt; 
	
	
	
	SET @SqlCmd=CONCAT('	UPDATE ',GT_DB,'.nt_bsc_current A
				, (SELECT BSC_ID,COUNT(DISTINCT CELL_ID) AS CELL_CNT
				   FROM  ',GT_DB,'.nt_cell_current_gsm 
				   GROUP BY BSC_ID ) B 
				SET A.CELL_CNT=B.CELL_CNT  
				WHERE A.BSC_ID=B.BSC_ID;');
				
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt; 
	
	INSERT INTO gt_gw_main.SP_LOG VALUES(GT_DB,'SP_Generate_NT','Update nt_cell_current_gsm.NBR_AVG_DISTANCE_VORONOI', NOW());
	
	SET @SqlCmd=CONCAT('DROP TABLE IF EXISTS ',GT_DB,'.tmp_avg_voronoi_distance;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('CREATE TABLE ',GT_DB,'.tmp_avg_voronoi_distance
			SELECT BSC_ID,SITE_ID,AVG(REFINE_DISTANCE) AS DISTANCE_AVG 
			FROM ',GT_DB,'.nt_neighbor_voronoi_gsm  
			GROUP BY BSC_ID,SITE_ID
	;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('ALTER TABLE ',GT_DB,'.tmp_avg_voronoi_distance ADD INDEX `RNC_SITE`(BSC_ID,SITE_ID);');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('UPDATE ',GT_DB,'.nt_cell_current_gsm A, ',GT_DB,'.tmp_avg_voronoi_distance B
		SET A.NBR_AVG_DISTANCE_VORONOI = B.DISTANCE_AVG
		WHERE A.BSC_ID = B.BSC_ID AND A.SITE_ID = B.SITE_ID;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
-- 	SET @SqlCmd=CONCAT('Drop table if exists ',GT_DB,'.nt_antenna_gsm;');
-- 	PREPARE Stmt FROM @SqlCmd;
-- 	EXECUTE Stmt;
-- 	DEALLOCATE PREPARE Stmt;
	
-- 	SET @SqlCmd=CONCAT('Drop table if exists ',GT_DB,'.nt_cell_gsm;');
-- 	PREPARE Stmt FROM @SqlCmd;
-- 	EXECUTE Stmt;
-- 	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('Drop table if exists ',GT_DB,'.nt_neighbor_gsm;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('Drop table if exists ',GT_DB,'.nt_bsc;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	CALL gt_gw_main.SP_Generate_NT_Sub_Density(GT_DB,'gt_covmo','gsm');
	CALL gt_gw_main.SP_Generate_NT_Sub_Pathloss(GT_DB,'gt_covmo','gsm');
	SET @SqlCmd=CONCAT('UPDATE ',GT_DB,'.nt_cell_current_gsm
			SET 
			  NBR_AVG_DISTNCE_GSM = CASE WHEN NBR_AVG_DISTNCE_GSM < 100 THEN 100 ELSE NBR_AVG_DISTNCE_GSM END,
			  NBR_AVG_DISTANCE_VORONOI = CASE WHEN NBR_AVG_DISTANCE_VORONOI < 100 THEN 100 WHEN NBR_AVG_DISTANCE_VORONOI IS NULL THEN 3000 ELSE NBR_AVG_DISTANCE_VORONOI END
			;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	CALL gt_gw_main.SP_Sub_Generate_Sys_Config(GT_DB,'gt_covmo','gsm');
	
	INSERT INTO gt_gw_main.SP_LOG VALUES(GT_DB,'SP_Generate_NT_GSM',CONCAT('Done: ',TIMESTAMPDIFF(SECOND,START_TIME,SYSDATE()),' seconds.'), NOW());
