DELIMITER $$
USE `gt_gw_main`$$
DROP PROCEDURE IF EXISTS `SP_CreateDB_LTE`$$
CREATE DEFINER=`covmo`@`%` PROCEDURE `SP_Generate_NT_LTE`(IN GT_DB VARCHAR(100),IN GT_COVMO VARCHAR(100))
a_label:
BEGIN
	DECLARE START_TIME DATETIME DEFAULT SYSDATE();
	DECLARE v_cnt INT;
	DECLARE PM_COUNTER_FLAG VARCHAR(10);	
	
	SELECT LOWER(`value`) INTO PM_COUNTER_FLAG FROM gt_gw_main.integration_param WHERE gt_group = 'sp' AND gt_name = 'pm_counter';
  	INSERT INTO gt_gw_main.SP_LOG VALUES(GT_DB,'SP_Generate_NT_LTE','nt_cell_current_lte', NOW());
	
	CALL gt_gw_main.SP_Sub_Generate_Sys_Config(GT_DB,'gt_covmo','lte');
	
	IF PM_COUNTER_FLAG = 'true' THEN
		CALL gt_gw_main.SP_Sub_Generate_Dim_PM_Counter(GT_DB,'gt_covmo');
-- 		CALL gt_gw_main.SP_Alter_PM_Schema(GT_DB);
	END IF;
	
	SET @SqlCmd=CONCAT('CREATE TABLE ',GT_DB,'.nt_cell_current_lte_dump LIKE ',GT_DB,'.nt_cell_current_lte');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt; 
	
	INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'SP_Generate_NT_LTE','nt_cell_current_lte - dump', NOW());
	CALL gt_gw_main.SP_Generate_NT_Sub_Dump(GT_DB,'nt_cell_lte','nt_cell_current_lte_dump','ENODEB_ID','LTE');
	CALL gt_gw_main.SP_Generate_NT_Sub_Dump(GT_DB,'nt_cell_lte','nt_cell_current_lte_dump','CELL_ID','LTE');
	CALL gt_gw_main.SP_Generate_NT_Sub_Dump(GT_DB,'nt_cell_lte','nt_cell_current_lte_dump','EUTRABAND','LTE');
	CALL gt_gw_main.SP_Generate_NT_Sub_Dump(GT_DB,'nt_cell_lte','nt_cell_current_lte_dump','PCI','LTE');
	CALL gt_gw_main.SP_Generate_NT_Sub_Dump(GT_DB,'nt_cell_lte','nt_cell_current_lte_dump','BWCHANNEL','LTE');
	CALL gt_gw_main.SP_Generate_NT_Sub_Dump(GT_DB,'nt_cell_lte','nt_cell_current_lte_dump','DL_EARFCN','LTE');
	CALL gt_gw_main.SP_Generate_NT_Sub_Dump(GT_DB,'nt_cell_lte','nt_cell_current_lte_dump','CLUSTER_NAME_SUB_REGION','LTE');
	INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'SP_Generate_NT_LTE','nt_cell_current_lte - check', NOW());
	
	SET @SqlCmd=CONCAT('INSERT INTO ',GT_DB,'.nt_cell_current_lte SELECT * FROM ',GT_DB,'.nt_cell_lte;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt; 
	
	CALL gt_gw_main.SP_Generate_NT_Sub_Check(GT_DB,'nt_cell_current_lte','ENODEB_NAME','','ENODEB_ID','LTE');
	CALL gt_gw_main.SP_Generate_NT_Sub_Check(GT_DB,'nt_cell_current_lte','CELL_NAME','','CELL_ID','LTE');
	CALL gt_gw_main.SP_Generate_NT_Sub_Check(GT_DB,'nt_cell_current_lte','ENODEB_TYPE','1to9','1','LTE');
	CALL gt_gw_main.SP_Generate_NT_Sub_Check(GT_DB,'nt_cell_current_lte','INDOOR','0to2','0','LTE');
	
	SET @SqlCmd=CONCAT('UPDATE ',GT_DB,'.nt_cell_current_lte SET ACT_STATE=''COMMERCIAL'';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	INSERT INTO gt_gw_main.SP_LOG VALUES(GT_DB,'SP_Generate_NT_LTE','nt_antenna_current_lte', NOW());
	SET @SqlCmd=CONCAT('SELECT max_longitude,max_latitude,min_longitude,min_latitude INTO @max_long,@max_lat,@min_long,@min_lat 
			FROM ',GT_COVMO,'.dim_mcc WHERE mcc IN (
				SELECT DISTINCT mcc FROM ',GT_COVMO,'.sys_mnc
			);');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt; 
	
	SET @SqlCmd=CONCAT('CREATE TABLE ',GT_DB,'.nt_antenna_current_lte_dump LIKE ',GT_DB,'.nt_antenna_current_lte;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt; 
	
	INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'SP_Generate_NT_LTE','nt_antenna_current_lte - dump', NOW());
	CALL gt_gw_main.SP_Generate_NT_Sub_Dump(GT_DB,'nt_antenna_lte','nt_antenna_current_lte_dump','ENODEB_ID','LTE');
	CALL gt_gw_main.SP_Generate_NT_Sub_Dump(GT_DB,'nt_antenna_lte','nt_antenna_current_lte_dump','CELL_ID','LTE');
	CALL gt_gw_main.SP_Generate_NT_Sub_Dump(GT_DB,'nt_antenna_lte','nt_antenna_current_lte_dump','LONGITUDE','LTE');
	CALL gt_gw_main.SP_Generate_NT_Sub_Dump(GT_DB,'nt_antenna_lte','nt_antenna_current_lte_dump','LATITUDE','LTE');
	CALL gt_gw_main.SP_Generate_NT_Sub_Dump_Special(GT_DB,'nt_antenna_lte','nt_antenna_current_lte_dump','','LTE','mapping_with_cell');
	CALL gt_gw_main.SP_Generate_NT_Sub_Dump_Special(GT_DB,'nt_antenna_lte','nt_antenna_current_lte_dump','AZIMUTH','LTE','azimuth_with_outdoor');
	IF @max_long IS NULL OR @max_lat IS NULL OR @min_long IS NULL OR @min_lat IS NULL THEN
		CALL gt_gw_main.SP_Generate_NT_Sub_Dump_Special(GT_DB,'nt_antenna_lte','nt_antenna_current_lte_dump','','LTE','location2');
	ELSE
		CALL gt_gw_main.SP_Generate_NT_Sub_Dump_Special(GT_DB,'nt_antenna_lte','nt_antenna_current_lte_dump','','LTE','location');
	END IF;
	INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'SP_Generate_NT_LTE','nt_antenna_current_lte - check', NOW());
	
	SET @SqlCmd=CONCAT('INSERT INTO ',GT_DB,'.nt_antenna_current_lte 
	SELECT *   FROM ',GT_DB,'.nt_antenna_lte;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt; 
	
	SET @SqlCmd=CONCAT('UPDATE ',GT_DB,'.nt_antenna_current_lte
						SET FLAG = 0;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt; 	
	
	CALL gt_gw_main.SP_Generate_NT_Sub_Check(GT_DB,'nt_antenna_current_lte','AZIMUTH','<0','IF(MOD(AZIMUTH,360)=0,0,MOD(AZIMUTH,360)+360)','LTE');
	CALL gt_gw_main.SP_Generate_NT_Sub_Check(GT_DB,'nt_antenna_current_lte','AZIMUTH','>359','MOD(AZIMUTH, 360)','LTE');
	CALL gt_gw_main.SP_Generate_NT_Sub_Check(GT_DB,'nt_antenna_current_lte','BEAM_WIDTH_VERTICAL','1to360','7','LTE');
	CALL gt_gw_main.SP_Generate_NT_Sub_Check(GT_DB,'nt_antenna_current_lte','DOWN_TILT_MECHANICAL','-90to90','0','LTE');
	CALL gt_gw_main.SP_Generate_NT_Sub_Check(GT_DB,'nt_antenna_current_lte','DOWN_TILT_ELECTRICAL','-90to90','0','LTE');
	CALL gt_gw_main.SP_Generate_NT_Sub_Check(GT_DB,'nt_antenna_current_lte','FEEDER_ATTEN','0to20','3','LTE');
	CALL gt_gw_main.SP_Generate_NT_Sub_Check_Special(GT_DB,'nt_antenna_current_lte','ANTENNA_TYPE','1to3','LTE');
	CALL gt_gw_main.SP_Generate_NT_Sub_Check_Special(GT_DB,'nt_antenna_current_lte','ANTENNA_HEIGHT','3to300','LTE');
	CALL gt_gw_main.SP_Generate_NT_Sub_Check_Special(GT_DB,'nt_antenna_current_lte','ANTENNA_GAIN','0to30','LTE');
	CALL gt_gw_main.SP_Generate_NT_Sub_Check_Special(GT_DB,'nt_antenna_current_lte','BEAM_WIDTH_HORIZONTAL','1to360','LTE');
	CALL gt_gw_main.SP_Generate_NT_Sub_Check_Special(GT_DB,'nt_antenna_current_lte','REFERENCE_SIGNAL_POWER','-60to50','LTE');
	
	INSERT INTO gt_gw_main.SP_LOG VALUES(GT_DB,'SP_Generate_NT_LTE','dump cell if not in antenna', NOW());
	CALL gt_gw_main.SP_Generate_NT_Sub_Dump_Special(GT_DB,'nt_cell_current_lte','nt_cell_current_lte_dump','','LTE','mapping_with_antenna');
	
	INSERT INTO gt_gw_main.SP_LOG VALUES(GT_DB,'SP_Generate_NT_LTE','nt_tac_cell_current_lte', NOW());
	SET @SqlCmd=CONCAT(' INSERT INTO ',GT_DB,'.nt_tac_cell_current_lte(ENODEB_ID,CELL_ID,TAC,TAC_TYPE)
				SELECT DISTINCT ENODEB_ID,CELL_ID,TAC,TAC_TYPE FROM ',GT_DB,'.nt_tac_cell_lte; ');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt; 
	
	INSERT INTO gt_gw_main.SP_LOG VALUES(GT_DB,'SP_Generate_NT_LTE','nt_mme_current_lte', NOW());
	SET @SqlCmd=CONCAT('INSERT INTO ',GT_DB,'.nt_mme_current_lte
				(MME_ID,MME_NAME,MME_NE_ID,S_GW_ID,S_GW_NAME,USER_LABEL,MCC,MNC,VENDOR)
			SELECT MME_ID,MME_NAME,MME_NE_ID,S_GW_ID,S_GW_NAME,USER_LABEL,MCC,MNC,VENDOR 
			FROM ',GT_DB,'.nt_mme_lte; ');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	INSERT INTO gt_gw_main.SP_LOG VALUES(GT_DB,'SP_Generate_NT_LTE','nt_nbr_4_2_current_lte', NOW());
	SET @SqlCmd=CONCAT('INSERT INTO ',GT_DB,'.nt_nbr_4_2_current_lte
			(ENODEB_ID,CELL_ID,NBR_BSC_ID,NBR_CELL_ID,PRIORITY,ARFCN,BCC,NCC,LAC)
			SELECT DISTINCT 
				ENODEB_ID,CELL_ID,NBR_BSC_ID,NBR_CELL_ID,PRIORITY,ARFCN,BCC,NCC,LAC 
			FROM ',GT_DB,'.nt_nbr_4_2_lte; ');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt; 
	
	INSERT INTO gt_gw_main.SP_LOG VALUES(GT_DB,'SP_Generate_NT_LTE','nt_nbr_4_3_current_lte', NOW());
	SET @SqlCmd=CONCAT('INSERT INTO ',GT_DB,'.nt_nbr_4_3_current_lte
			(ENODEB_ID,CELL_ID,NBR_RNC_ID,NBR_CELL_ID,PRIORITY,PSC)
			SELECT DISTINCT 
				ENODEB_ID,CELL_ID,NBR_RNC_ID,NBR_CELL_ID,PRIORITY,PSC 
			FROM ',GT_DB,'.nt_nbr_4_3_lte; ');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt; 
	
	INSERT INTO gt_gw_main.SP_LOG VALUES(GT_DB,'SP_Generate_NT_LTE','nt_nbr_4_4_current_lte', NOW());
	SET @SqlCmd=CONCAT('INSERT INTO ',GT_DB,'.nt_nbr_4_4_current_lte
			(ENODEB_ID,CELL_ID,NBR_ENODEB_ID,NBR_CELL_ID,PRIORITY,NBR_TYPE)
			SELECT  DISTINCT 
				ENODEB_ID,CELL_ID,NBR_ENODEB_ID,NBR_CELL_ID,PRIORITY, NBR_TYPE 
			FROM ',GT_DB,'.nt_nbr_4_4_lte; ');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt; 	
	
	INSERT INTO gt_gw_main.SP_LOG VALUES(GT_DB,'SP_Generate_NT_LTE','NT_CELL_CURRENT_LTE.NBR_DISTANCE_4G_CM', NOW());
	
	SET @SqlCmd=CONCAT('CREATE TABLE ',GT_DB,'.tmp_nt_current_nbr_distance_LTE_fixDataType(
		`enodeb_id` MEDIUMINT(9) DEFAULT NULL,
		`cell_id` TINYINT(4) DEFAULT NULL,
		`CELL_LON` DECIMAL (9, 6) DEFAULT NULL,
		`CELL_LAT` DECIMAL (9, 6) DEFAULT NULL,
		`NBR_LON` DECIMAL (9, 6) DEFAULT NULL,
		`NBR_LAT` DECIMAL (9, 6) DEFAULT NULL,
		KEY `enodeb_id` (`enodeb_id`),
		KEY `cell_id` (`cell_id`)
	) ENGINE=MYISAM DEFAULT CHARSET=utf8;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('INSERT INTO ',GT_DB,'.tmp_nt_current_nbr_distance_LTE_fixDataType
		SELECT DISTINCT a.enodeb_id,a.cell_id,B.LONGITUDE CELL_LON,B.LATITUDE CELL_LAT ,C.LONGITUDE NBR_LON,C.LATITUDE NBR_LAT
		FROM ',GT_DB,'.nt_nbr_4_4_current_lte A
				INNER JOIN ',GT_DB,'.NT_ANTENNA_CURRENT_LTE  B
				ON a.enodeb_id=b.enodeb_id AND a.cell_id=b.cell_id 
				INNER JOIN ',GT_DB,'.NT_ANTENNA_CURRENT_LTE  C 
				ON A.nbr_enodeb_id=C.enodeb_id AND A.nbr_cell_id=C.cell_id
				WHERE EXISTS
					(SELECT 1 FROM ',GT_DB,'.NT_CELL_CURRENT_LTE D
						WHERE A.nbr_enodeb_id=D.enodeb_id
						AND A.nbr_cell_id=D.cell_id
						AND D.INDOOR=0
					); 
	');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('CREATE TABLE ',GT_DB,'.tmp_nt_current_nbr_distance_LTE
				SELECT DISTINCT B.enodeb_id,B.cell_id,GT_COVMO_DISTANCE(B.CELL_LON,B.CELL_LAT ,B.NBR_LON,B.NBR_LAT) AS NBR_DISTANCE_4G_CM
				FROM ',GT_DB,'.tmp_nt_current_nbr_distance_LTE_fixDataType  B
				;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('CREATE TABLE ',GT_DB,'.tmp_update_data_LTE
				SELECT enodeb_id,cell_id,AVG(NBR_DISTANCE_4G_CM) AS NBR_AVG_DISTANCE_4G_CM
				FROM ',GT_DB,'.tmp_nt_current_nbr_distance_LTE
				WHERE NBR_DISTANCE_4G_CM >3
				GROUP BY enodeb_id,cell_id
				HAVING COUNT(*) > 1
	;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	SET @SqlCmd=CONCAT('CREATE INDEX idx_tmp_update_data_LTE ON ',GT_DB,'.tmp_update_data_LTE (enodeb_id,cell_id);');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT(' UPDATE ',GT_DB,'.NT_CELL_CURRENT_LTE A
			JOIN 
			(
				SELECT ENODEB_ID,CELL_ID ,NBR_AVG_DISTANCE_4G_CM
				FROM (
					SELECT a.enodeb_id,a.cell_id,b.NBR_AVG_DISTANCE_4G_CM
					 FROM  ',GT_DB,'.tmp_nt_current_nbr_distance_LTE a
					JOIN  ',GT_DB,'.tmp_update_data_LTE  b
					ON a.enodeb_id=b.enodeb_id AND a.cell_id=b.cell_id 
					WHERE a.NBR_DISTANCE_4G_CM > 3
				) AA
				GROUP BY ENODEB_ID,CELL_ID 
			) B
			ON A.ENODEB_ID=B.ENODEB_ID
			AND A.CELL_ID=B.CELL_ID
			SET A.NBR_DISTANCE_4G_CM=B.NBR_AVG_DISTANCE_4G_CM ');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt; 
	
	SET @SqlCmd=CONCAT('DROP TABLE IF EXISTS ',GT_DB,'.tmp_nt_current_nbr_distance_LTE;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('DROP TABLE IF EXISTS ',GT_DB,'.tmp_update_data_LTE;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
			
	INSERT INTO gt_gw_main.SP_LOG VALUES(GT_DB,'SP_Generate_NT_LTE','Update NT_CELL_CURRENT_LTE.NBR_DISTANCE_4G_VORONOI', NOW());
	
	SET @SqlCmd=CONCAT('DROP TABLE IF EXISTS ',GT_DB,'.tmp_avg_voronoi_distance;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('CREATE TABLE ',GT_DB,'.tmp_avg_voronoi_distance ENGINE=MYISAM AS
		SELECT ENODEB_ID,AVG(REFINE_DISTANCE) AS DISTANCE_AVG 
		FROM ',GT_DB,'.`nt_neighbor_voronoi_lte` 
		GROUP BY ENODEB_ID
		;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('ALTER TABLE ',GT_DB,'.tmp_avg_voronoi_distance ADD INDEX `ix_enodeb_id`(ENODEB_ID);');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('UPDATE ',GT_DB,'.NT_CELL_CURRENT_LTE A, ',GT_DB,'.tmp_avg_voronoi_distance B
		SET A.`NBR_DISTANCE_4G_VORONOI` = B.DISTANCE_AVG
		WHERE A.ENODEB_ID = B.ENODEB_ID;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;	
	
	INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'SP_Generate_NT_LTE','DISTANCE', NOW());
	SET @SqlCmd=CONCAT('Drop TABLE IF EXISTS ',GT_DB,'.tmp_nt_cell_u;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('CREATE TABLE ',GT_DB,'.tmp_nt_cell_u (
		`ENODEB_ID` MEDIUMINT(9) DEFAULT NULL,
		`CELL_ID` TINYINT(4) DEFAULT NULL,
		`LONGITUDE` DECIMAL (9, 6) DEFAULT NULL,
		`LATITUDE` DECIMAL (9, 6) DEFAULT NULL,
		`AZIMUTH` SMALLINT(6) DEFAULT NULL
	)ENGINE=MYISAM DEFAULT CHARSET=utf8;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
		
	
	SET @SqlCmd=CONCAT('INSERT INTO ',GT_DB,'.tmp_nt_cell_u
                                SELECT A.ENODEB_ID,A.CELL_ID,B.LONGITUDE,B.LATITUDE,B.AZIMUTH
                                FROM ',GT_DB,'.nt_cell_current_lte A
                                JOIN ',GT_DB,'.nt_antenna_current_lte B
                                ON A.CELL_ID=B.CELL_ID AND A.ENODEB_ID=B.ENODEB_ID;');
        PREPARE Stmt FROM @SqlCmd;
        EXECUTE Stmt;
        DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('CREATE INDEX idx_tmp_nt_cell_u ON ',GT_DB,'.tmp_nt_cell_u (ENODEB_ID,CELL_ID);');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt; 
	
	SET @SqlCmd=CONCAT('UPDATE ',GT_DB,'.`nt_nbr_4_4_current_lte` A
				JOIN ',GT_DB,'.tmp_nt_cell_u B ON A.CELL_ID=B.CELL_ID AND A.ENODEB_ID=B.ENODEB_ID 
				JOIN ',GT_DB,'.tmp_nt_cell_u C ON A.NBR_CELL_ID=C.CELL_ID AND A.NBR_ENODEB_ID=C.ENODEB_ID
				SET NBR_ANGLE=gt_covmo_angle(B.LONGITUDE,B.LATITUDE,C.LONGITUDE,C.LATITUDE)
				,NBR_AZIMUTH_ANGLE=gt_covmo_azimuth_angle(B.LONGITUDE,B.LATITUDE,B.AZIMUTH,C.LONGITUDE,C.LATITUDE,C.AZIMUTH)
				,NBR_DISTANCE=CASE
				WHEN B.LONGITUDE=C.LONGITUDE AND B.LATITUDE=C.LATITUDE THEN 0
				ELSE gt_covmo_distance(B.LONGITUDE,B.LATITUDE,C.LONGITUDE,C.LATITUDE)
				END
				WHERE NBR_TYPE<3;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt; 
	
	INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'SP_Generate_NT_LTE','Update NBR_TYPE by DL_EARFCN', NOW());
	
	SET @SqlCmd=CONCAT('UPDATE ',GT_DB,'.nt_nbr_4_4_current_lte a,  (
			SELECT a.*, b.DL_EARFCN AS S_EAR, c.DL_EARFCN AS T_EAR FROM (
				SELECT ENODEB_ID,CELL_ID,NBR_ENODEB_ID,NBR_CELL_ID,NBR_TYPE FROM ',GT_DB,'.nt_nbr_4_4_current_lte WHERE NBR_TYPE=0
			) a, ',GT_DB,'.nt_cell_current_lte b, ',GT_DB,'.nt_cell_current_lte c
			WHERE a.ENODEB_ID=b.ENODEB_ID AND a.CELL_ID=b.CELL_ID
			AND a.NBR_ENODEB_ID=c.ENODEB_ID AND a.NBR_CELL_ID=c.CELL_ID
			)t
		SET a.NBR_TYPE = (CASE WHEN t.S_EAR=t.T_EAR THEN 2 ELSE 1 END)
		WHERE a.ENODEB_ID=t.ENODEB_ID AND a.CELL_ID=t.CELL_ID AND a.NBR_ENODEB_ID=t.NBR_ENODEB_ID AND a.NBR_CELL_ID=t.NBR_CELL_ID;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt; 
	
	SET @SqlCmd=CONCAT('Drop TABLE IF EXISTS ',GT_DB,'.tmp_nt_current_nbr_distance_LTE_fixDataType;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	SET @SqlCmd=CONCAT('Drop TABLE IF EXISTS ',GT_DB,'.tmp_nt_cell_u;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	SET @SqlCmd=CONCAT(' Drop table if exists ',GT_DB,'.nt_antenna_lte;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT(' Drop table if exists ',GT_DB,'.nt_cell_lte;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT(' Drop table if exists ',GT_DB,'.nt_mme_lte;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT(' Drop table if exists ',GT_DB,'.nt_nbr_4_2_lte;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT(' Drop table if exists ',GT_DB,'.nt_nbr_4_3_lte;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT(' Drop table if exists ',GT_DB,'.nt_nbr_4_4_lte;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT(' Drop table if exists ',GT_DB,'.nt_tac_cell_lte;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('DROP TABLE IF EXISTS ',GT_DB,'.tmp_avg_voronoi_distance;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	CALL gt_gw_main.SP_Generate_NT_Sub_Density(GT_DB,'gt_covmo','lte');
	CALL gt_gw_main.SP_Generate_NT_Sub_Pathloss(GT_DB,'gt_covmo','lte');
	
	INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'SP_Generate_NT_LTE','DIST rule', NOW());
	
	SET @SqlCmd=CONCAT('
	UPDATE ',GT_DB,'.nt_cell_current_lte
	SET 
	  NBR_DISTANCE_4G_CM = CASE WHEN NBR_DISTANCE_4G_CM < 100 OR NBR_DISTANCE_4G_CM IS NULL THEN 100 ELSE NBR_DISTANCE_4G_CM END,
	  NBR_DISTANCE_4G_VORONOI = CASE WHEN NBR_DISTANCE_4G_VORONOI < 100 THEN 100 WHEN NBR_DISTANCE_4G_VORONOI IS NULL THEN 3000 ELSE NBR_DISTANCE_4G_VORONOI END
	;	
	');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt; 
	INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'SP_Generate_NT_LTE','ANTENNA_RADIUS rule', NOW());
	SET @SqlCmd=CONCAT('
	UPDATE ',GT_DB,'.nt_antenna_current_lte A, ',GT_DB,'.nt_cell_current_lte B
	SET 
	  ANTENNA_RADIUS = CASE WHEN indoor_type = 0 AND (NBR_DISTANCE_4G_VORONOI*(1.5-SITE_DENSITY_TYPE/10)) > PATHLOSS_DISTANCE 
			THEN (NBR_DISTANCE_4G_VORONOI*(1.5-SITE_DENSITY_TYPE/10))
		WHEN indoor_type = 0 AND (NBR_DISTANCE_4G_VORONOI*(1.5-SITE_DENSITY_TYPE/10)) < PATHLOSS_DISTANCE 
			THEN PATHLOSS_DISTANCE
	ELSE 50 END	
	WHERE A.enodeb_id = B.enodeb_id and A.cell_id = B.cell_id;	
	');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt; 
	
	INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'SP_Generate_NT_LTE','CLOSED_RADIUS rule', NOW());
	SET @SqlCmd=CONCAT('
	UPDATE ',GT_DB,'.nt_antenna_current_lte A, ',GT_DB,'.nt_cell_current_lte B
	SET 
	  CLOSED_RADIUS = CASE WHEN indoor_type = 0 THEN ( 1+ FLOOR(ANTENNA_HEIGHT/ 50 )) * NBR_DISTANCE_4G_VORONOI / 5
		ELSE 50 END
	WHERE A.enodeb_id = B.enodeb_id and A.cell_id = B.cell_id;	
	;	
	');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt; 	
	
	INSERT INTO gt_gw_main.SP_LOG VALUES(GT_DB,'SP_Generate_NT_LTE',CONCAT('Done: ',TIMESTAMPDIFF(SECOND,START_TIME,SYSDATE()),' seconds.'), NOW());
	
	
END$$
DELIMITER ;
