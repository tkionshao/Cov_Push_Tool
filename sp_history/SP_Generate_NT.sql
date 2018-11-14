DELIMITER $$
USE `gt_gw_main`$$
DROP PROCEDURE IF EXISTS `SP_CreateDB_LTE`$$
CREATE DEFINER=`covmo`@`%` PROCEDURE `SP_Generate_NT`(IN GT_DB VARCHAR(100),IN GT_COVMO VARCHAR(100))
BEGIN
	
	DECLARE START_TIME DATETIME DEFAULT SYSDATE();
	DECLARE SESSION_DATE CHAR(10);
	DECLARE FILEDATE CHAR(8) DEFAULT NULL;
	DECLARE WORKER_ID VARCHAR(10) DEFAULT CONNECTION_ID();
	
	SELECT gt_strtok(GT_DB,3,'_') INTO FILEDATE;
	
	SET SESSION_DATE = CONCAT(LEFT(FILEDATE,4),'-',SUBSTRING(FILEDATE,5,2),'-',SUBSTRING(FILEDATE,7,2));
	
	INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'SP_Generate_NT','Update NT Date', NOW());
	
	CALL gt_gw_main.SP_Sub_Generate_Sys_Config(GT_DB,'gt_covmo','umts');
	
	SET @SqlCmd=CONCAT('UPDATE ',GT_DB,'.nt_cell SET NT_DATE=''',SESSION_DATE,' 00:00:00'';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt; 
		
	SET @SqlCmd=CONCAT('UPDATE ',GT_DB,'.nt_antenna SET NT_DATE=''',SESSION_DATE,' 00:00:00'';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt; 
		
	SET @SqlCmd=CONCAT('UPDATE ',GT_DB,'.nt_neighbor SET NT_DATE=''',SESSION_DATE,' 00:00:00'';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt; 
		
	SET @SqlCmd=CONCAT('UPDATE ',GT_DB,'.nt_rnc SET NT_DATE=''',SESSION_DATE,' 00:00:00'';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt; 
	
	INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'SP_Generate_NT','Insert Data to nt_rnc_current', NOW());
	
	SET @SqlCmd=CONCAT('	INSERT INTO ',GT_DB,'.nt_rnc_current
				(NT_DATE,RNC_ID,RNC_NAME,MCC,MNC,TECH_MASK,VENDOR,RNC_MODEL)
				SELECT 
					DISTINCT NT_DATE,RNC_ID,RNC_NAME,MCC,CASE WHEN LENGTH(MNC)=1 THEN CONCAT(0,MNC) ELSE MNC END ,TECH_MASK,VENDOR,RNC_MODEL
				FROM ',GT_DB,'.nt_rnc 
				WHERE RNC_ID IS NOT NULL AND RNC_ID <> ''''
				; ');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt; 
	SET @SqlCmd=CONCAT('	UPDATE ',GT_DB,'.nt_rnc_current A
				, (SELECT RNC_ID,COUNT(DISTINCT CELL_ID) AS CELL_CNT, COUNT(DISTINCT SITE_ID) AS SITE_CNT 
				   FROM  ',GT_DB,'.nt_cell 
				   GROUP BY RNC_ID ) B 
				SET A.SITE_CNT=B.SITE_CNT , A.CELL_CNT=B.CELL_CNT  
				WHERE A.RNC_ID=B.RNC_ID;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt; 	
	
	INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'SP_Generate_NT','Insert Data to nt_current', NOW());
	
	SET @SqlCmd=CONCAT('SELECT max_longitude,max_latitude,min_longitude,min_latitude INTO @max_long,@max_lat,@min_long,@min_lat 
			FROM ',GT_COVMO,'.dim_mcc WHERE mcc IN (
				SELECT DISTINCT mcc FROM ',GT_COVMO,'.sys_mnc
			);');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt; 
	
	
	
	SET @SqlCmd=CONCAT('CREATE TABLE ',GT_DB,'.nt_current_dump LIKE ',GT_DB,'.nt_current');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt; 
	
	INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'SP_Generate_NT','nt_current - dump', NOW());
	
	CALL gt_gw_main.SP_Generate_NT_Sub_Dump(GT_DB,'nt_cell','nt_current_dump','RNC_ID','UMTS');
	CALL gt_gw_main.SP_Generate_NT_Sub_Dump(GT_DB,'nt_cell','nt_current_dump','SITE_ID','UMTS');
	CALL gt_gw_main.SP_Generate_NT_Sub_Dump(GT_DB,'nt_cell','nt_current_dump','CELL_ID','UMTS');
	CALL gt_gw_main.SP_Generate_NT_Sub_Dump(GT_DB,'nt_cell','nt_current_dump','SCRAMBLING_CODE','UMTS');
	CALL gt_gw_main.SP_Generate_NT_Sub_Dump(GT_DB,'nt_cell','nt_current_dump','FREQUENCY','UMTS');
	CALL gt_gw_main.SP_Generate_NT_Sub_Dump(GT_DB,'nt_cell','nt_current_dump','LONGITUDE','UMTS');
	CALL gt_gw_main.SP_Generate_NT_Sub_Dump(GT_DB,'nt_cell','nt_current_dump','LATITUDE','UMTS');
	CALL gt_gw_main.SP_Generate_NT_Sub_Dump(GT_DB,'nt_cell','nt_current_dump','DL_UARFCN','UMTS');
	CALL gt_gw_main.SP_Generate_NT_Sub_Dump_Special(GT_DB,'nt_cell','nt_current_dump','','UMTS','location');
	
	INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'SP_Generate_NT','nt_current - check', NOW());
	
	SET @SqlCmd=CONCAT('INSERT INTO ',GT_DB,'.nt_current SELECT * FROM ',GT_DB,'.nt_cell;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt; 
	
	CALL gt_gw_main.SP_Generate_NT_Sub_Check(GT_DB,'nt_current','SITE_NAME','','SITE_ID','UMTS');
	CALL gt_gw_main.SP_Generate_NT_Sub_Check(GT_DB,'nt_current','CELL_NAME','','CELL_ID','UMTS');
 	CALL gt_gw_main.SP_Generate_NT_Sub_Check(GT_DB,'nt_current','INDOOR','NOT IN (0,1)','0','UMTS');
	CALL gt_gw_main.SP_Generate_NT_Sub_Check(GT_DB,'nt_current','SITE_TYPE','NOT IN (1,2,3,4,5,6)','2','UMTS');
	INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'SP_Generate_NT','Insert Data to nt_antenna_current', NOW());
	
	SET @SqlCmd=CONCAT('CREATE TABLE ',GT_DB,'.nt_antenna_current_dump LIKE ',GT_DB,'.nt_antenna_current');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt; 
	
	INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'SP_Generate_NT','nt_antenna_current - dump', NOW());
	
	CALL gt_gw_main.SP_Generate_NT_Sub_Dump(GT_DB,'nt_antenna','nt_antenna_current_dump','RNC_ID','UMTS');
	CALL gt_gw_main.SP_Generate_NT_Sub_Dump(GT_DB,'nt_antenna','nt_antenna_current_dump','CELL_ID','UMTS');
	CALL gt_gw_main.SP_Generate_NT_Sub_Dump_Special(GT_DB,'nt_antenna','nt_antenna_current_dump','','UMTS','location');
	CALL gt_gw_main.SP_Generate_NT_Sub_Dump_Special(GT_DB,'nt_antenna','nt_antenna_current_dump','','UMTS','mapping_with_cell');
	CALL gt_gw_main.SP_Generate_NT_Sub_Dump_Special(GT_DB,'nt_antenna','nt_antenna_current_dump','AZIMUTH','UMTS','azimuth_with_outdoor');
	
	INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'SP_Generate_NT','nt_antenna_current - check', NOW());
	
	SET @SqlCmd=CONCAT('INSERT INTO ',GT_DB,'.nt_antenna_current
						SELECT * FROM ',GT_DB,'.nt_antenna A;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt; 
	
	SET @SqlCmd=CONCAT('UPDATE ',GT_DB,'.nt_antenna_current
						SET FLAG = 0;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt; 	
	
	CALL gt_gw_main.SP_Generate_NT_Sub_Check(GT_DB,'nt_antenna_current','CELL_NAME','','CELL_ID','UMTS');
	CALL gt_gw_main.SP_Generate_NT_Sub_Check(GT_DB,'nt_antenna_current','AZIMUTH','','0','UMTS');
	CALL gt_gw_main.SP_Generate_NT_Sub_Check(GT_DB,'nt_antenna_current','AZIMUTH','>360','360','UMTS');
	CALL gt_gw_main.SP_Generate_NT_Sub_Check_Special(GT_DB,'nt_antenna_current','ANTENNA_TYPE','1to3','UMTS');
 	CALL gt_gw_main.SP_Generate_NT_Sub_Check_Special(GT_DB,'nt_antenna_current','BEAMWIDTH_H','1to360','UMTS');
	CALL gt_gw_main.SP_Generate_NT_Sub_Check_Special(GT_DB,'nt_antenna_current','HEIGHT','0to1000','UMTS');
 	CALL gt_gw_main.SP_Generate_NT_Sub_Check(GT_DB,'nt_antenna_current','BEAMWIDTH_V','1to360','7','UMTS');
	CALL gt_gw_main.SP_Generate_NT_Sub_Check(GT_DB,'nt_antenna_current','CPICH_POWER','<-10','-10','UMTS'); 
	CALL gt_gw_main.SP_Generate_NT_Sub_Check(GT_DB,'nt_antenna_current','CPICH_POWER','>50','50','UMTS'); 
	CALL gt_gw_main.SP_Generate_NT_Sub_Check_Special(GT_DB,'nt_antenna_current','ANTENNA_GAIN','0to30','UMTS');
	CALL gt_gw_main.SP_Generate_NT_Sub_Check(GT_DB,'nt_antenna_current','FEEDER_LOSS','0to30','3','UMTS');
	CALL gt_gw_main.SP_Generate_NT_Sub_Check(GT_DB,'nt_antenna_current','DOWNTILT_EL','-90to90','0','UMTS');
	CALL gt_gw_main.SP_Generate_NT_Sub_Check(GT_DB,'nt_antenna_current','DOWNTILT_MEC','-90to90','0','UMTS');
	
	SET @SqlCmd=CONCAT('UPDATE ',GT_DB,'.nt_antenna_current SET EIRP = CPICH_POWER+ANTENNA_GAIN-FEEDER_LOSS;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	CALL gt_gw_main.SP_Generate_NT_Sub_Check(GT_DB,'nt_antenna_current','EIRP','-0to100','48','UMTS');
	
	INSERT INTO gt_gw_main.SP_LOG VALUES(GT_DB,'SP_Generate_NT','dump cell if not in antenna', NOW());
	CALL gt_gw_main.SP_Generate_NT_Sub_Dump_Special(GT_DB,'nt_current','nt_current_dump','','UMTS','mapping_with_antenna');
	
	INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'SP_Generate_NT','Insert Data to nt_neighbor_current', NOW());
	
	SET @SqlCmd=CONCAT('CREATE TABLE ',GT_DB,'.nt_neighbor_current_dump LIKE ',GT_DB,'.nt_neighbor_current');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt; 
	SET @SqlCmd=CONCAT('CREATE INDEX del ON ',GT_DB,'.nt_neighbor (RNC_ID,CELL_ID);');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt; 
	INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'SP_Generate_NT','nt_neighbor - dump', NOW());
	CALL gt_gw_main.SP_Generate_NT_Sub_Dump_Special(GT_DB,'nt_neighbor','nt_neighbor_current_dump','','UMTS','neighbor_source_check_with_cell');
	CALL gt_gw_main.SP_Generate_NT_Sub_Dump_Special(GT_DB,'nt_neighbor','nt_neighbor_current_dump','','UMTS','neighbor_target_check_with_cell');
	
	CALL gt_gw_main.SP_Generate_NT_Sub_Dump_Special(GT_DB,'nt_neighbor','nt_neighbor_current_dump','','UMTS','umts_neighbor_nbr_type_2');
	SET @SqlCmd=CONCAT('	INSERT INTO ',GT_DB,'.nt_neighbor_current
				SELECT 
					DISTINCT * FROM ',GT_DB,'.nt_neighbor 
				WHERE RNC_ID IS NOT NULL AND CELL_ID IS NOT NULL AND NBR_RNC_ID IS NOT NULL AND NBR_CELL_ID IS NOT NULL
			; ');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt; 
	INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'SP_Generate_NT','Insert Data to nt_cell_attribute_current', NOW());
	SET @SqlCmd=CONCAT('	INSERT INTO ',GT_DB,'.nt_cell_attribute_current
				SELECT DISTINCT * FROM ',GT_DB,'.nt_cell_attribute 
				WHERE CELL_ID <>'''' and RNC_ID<>'''' and CELL_ID IS NOT NULL and RNC_ID IS NOT NULL;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt; 
	
	INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'SP_Generate_NT','Update  nt_current', NOW());
	SET @SqlCmd=CONCAT('	Update  ',GT_DB,'.nt_current A, 
					(SELECT RNC_ID,CELL_ID, COUNT(*) AS NBR_CNT 
					 FROM ',GT_DB,'.nt_neighbor_current
					 WHERE `NBR_TYPE`=1
					 GROUP BY RNC_ID,CELL_ID) B
				SET     A.INAFREQ_NBCNT=B.NBR_CNT 
				WHERE A.CELL_ID=B.CELL_ID
				AND A.RNC_ID=B.RNC_ID;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt; 
	
	
	SET @SqlCmd=CONCAT('	Update  ',GT_DB,'.nt_current A, 
					(SELECT RNC_ID,CELL_ID,COUNT(*) AS NBR_CNT 
					 FROM ',GT_DB,'.nt_neighbor_current
					 WHERE `NBR_TYPE`=2
					 GROUP BY RNC_ID,CELL_ID, NBR_TYPE	) B
				SET     A.INTFREQ_NBCNT=B.NBR_CNT 
				WHERE A.CELL_ID=B.CELL_ID
				AND A.RNC_ID=B.RNC_ID;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt; 
	SET @SqlCmd=CONCAT('	Update  ',GT_DB,'.nt_current A, 
					(SELECT RNC_ID,CELL_ID, COUNT(*) AS NBR_CNT 
					 FROM ',GT_DB,'.nt_neighbor_current
					 WHERE `NBR_TYPE`=3
					 GROUP BY RNC_ID,CELL_ID) B
				SET     A.GSM_NBCNT=B.NBR_CNT 
				WHERE A.CELL_ID=B.CELL_ID
				AND A.RNC_ID=B.RNC_ID;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt; 
	SET @SqlCmd=CONCAT('Drop table if exists ',GT_DB,'.tmp_nt_cell_u;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
		SET @SqlCmd=CONCAT('CREATE TABLE ',GT_DB,'.tmp_nt_cell_u (
		  `RNC_ID` MEDIUMINT(9) DEFAULT NULL,
		  `CELL_ID` MEDIUMINT(9) DEFAULT NULL,
		  `LONGITUDE` DECIMAL (9, 6) DEFAULT NULL,
		  `LATITUDE` DECIMAL (9, 6) DEFAULT NULL,
		  `AZIMUTH` SMALLINT(6) DEFAULT NULL
	) ENGINE=MYISAM DEFAULT CHARSET=utf8;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('INSERT INTO ',GT_DB,'.tmp_nt_cell_u
				SELECT A.RNC_ID,A.CELL_ID,A.LONGITUDE,A.LATITUDE,AZIMUTH
				FROM ',GT_DB,'.nt_current A
				JOIN ',GT_DB,'.nt_antenna_current B
				ON A.CELL_ID=B.CELL_ID AND A.RNC_ID=B.RNC_ID;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	SET @SqlCmd=CONCAT('CREATE INDEX idx_tmp_nt_cell_u ON ',GT_DB,'.tmp_nt_cell_u (RNC_ID,CELL_ID);');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt; 
	SET @SqlCmd=CONCAT('UPDATE ',GT_DB,'.`nt_neighbor_current` A
				JOIN ',GT_DB,'.tmp_nt_cell_u B ON A.CELL_ID=B.CELL_ID AND A.RNC_ID=B.RNC_ID 
				JOIN ',GT_DB,'.tmp_nt_cell_u C ON A.NBR_CELL_ID=C.CELL_ID AND A.NBR_RNC_ID=C.RNC_ID
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
	
	SET @SqlCmd=CONCAT('Drop table if exists ',GT_DB,'.tmp_nt_nbr_u;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	SET @SqlCmd=CONCAT('CREATE TABLE ',GT_DB,'.tmp_nt_nbr_u (
		`RNC_ID` INT(11) DEFAULT NULL,
		`CELL_ID` MEDIUMINT(9) DEFAULT NULL,
		`LONGITUDE` DECIMAL (9, 6) DEFAULT NULL,
		`LATITUDE` DECIMAL (9, 6) DEFAULT NULL
	)ENGINE=MYISAM DEFAULT CHARSET=utf8;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	SET @SqlCmd=CONCAT('INSERT INTO ',GT_DB,'.tmp_nt_nbr_u
				SELECT DISTINCT A.RNC_ID,A.CELL_ID,C.LONGITUDE,C.LATITUDE
				FROM ',GT_DB,'.`nt_neighbor_current` A
				JOIN ',GT_DB,'.tmp_nt_cell_u B ON A.CELL_ID=B.CELL_ID AND A.RNC_ID=B.RNC_ID 
				JOIN ',GT_DB,'.tmp_nt_cell_u C ON A.NBR_CELL_ID=C.CELL_ID AND A.NBR_RNC_ID=C.RNC_ID
				WHERE NBR_TYPE<3;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	SET @SqlCmd=CONCAT('CREATE INDEX idx_tmp_nt_nbr_u ON ',GT_DB,'.tmp_nt_nbr_u (RNC_ID,CELL_ID);');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('CREATE TABLE ',GT_DB,'.tmp_nt_current_nbr_distance
				 SELECT DISTINCT A.CELL_ID, A.RNC_ID
				,gt_covmo_distance(A.LONGITUDE,A.LATITUDE,B.LONGITUDE,B.LATITUDE) AS NB_DISTANCE_UMTS
				FROM ',GT_DB,'.nt_current A
				JOIN ',GT_DB,'.tmp_nt_nbr_u B
				ON A.CELL_ID=B.CELL_ID AND A.RNC_ID=B.RNC_ID
			;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('CREATE TABLE ',GT_DB,'.tmp_update_data
				SELECT CELL_ID, RNC_ID, AVG(NB_DISTANCE_UMTS) AS NB_AVG_DISTANCE_UMTS
				FROM ',GT_DB,'.tmp_nt_current_nbr_distance
				WHERE NB_DISTANCE_UMTS > 3
				GROUP BY CELL_ID,RNC_ID
				HAVING COUNT(*)>1
	;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	SET @SqlCmd=CONCAT('CREATE INDEX idx_tmp_update_data ON ',GT_DB,'.tmp_update_data (RNC_ID,CELL_ID);');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	
	SET @SqlCmd=CONCAT('UPDATE ',GT_DB,'.nt_current A
				JOIN (
					SELECT AA.CELL_ID, AA.RNC_ID,NB_AVG_DISTANCE_UMTS
					FROM (
						SELECT A.CELL_ID, A.RNC_ID, B.NB_AVG_DISTANCE_UMTS
						FROM ',GT_DB,'.tmp_nt_current_nbr_distance A
						JOIN ',GT_DB,'.tmp_update_data B
						ON A.CELL_ID=B.CELL_ID AND A.RNC_ID=B.RNC_ID
						WHERE A.NB_DISTANCE_UMTS > 3
					) AA
					GROUP BY AA.RNC_ID,AA.CELL_ID
				) B
				ON A.CELL_ID=B.CELL_ID AND A.RNC_ID=B.RNC_ID
				SET A.NB_AVG_DISTANCE_UMTS = B.NB_AVG_DISTANCE_UMTS;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
		
	SET @SqlCmd=CONCAT('DROP TABLE IF EXISTS ',GT_DB,'.tmp_nt_current_nbr_distance;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;	
	
	SET @SqlCmd=CONCAT('DROP TABLE IF EXISTS ',GT_DB,'.tmp_update_data;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	
	INSERT INTO gt_gw_main.SP_LOG VALUES(GT_DB,'SP_Generate_NT','Update nt_current.NB_AVG_DISTANCE_VORONOI', NOW());
	
	SET @SqlCmd=CONCAT('DROP TABLE IF EXISTS ',GT_DB,'.tmp_avg_voronoi_distance;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('CREATE TABLE ',GT_DB,'.tmp_avg_voronoi_distance
		SELECT RNC_ID,SITE_ID,AVG(REFINE_DISTANCE) AS DISTANCE_AVG 
		FROM ',GT_DB,'.nt_neighbor_voronoi 
		GROUP BY RNC_ID,SITE_ID
	;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('ALTER TABLE ',GT_DB,'.tmp_avg_voronoi_distance ADD INDEX `RNC_SITE`(RNC_ID,SITE_ID);');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('UPDATE ',GT_DB,'.nt_current A, ',GT_DB,'.tmp_avg_voronoi_distance B
		SET A.NB_AVG_DISTANCE_VORONOI = B.DISTANCE_AVG
		WHERE A.RNC_ID = B.RNC_ID AND A.SITE_ID = B.SITE_ID;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	INSERT INTO gt_gw_main.SP_LOG VALUES(GT_DB,'SP_Generate_NT','Update NT_RNC_CURRENT.AVG_VORONOI ', NOW());
	
	SET @SqlCmd=CONCAT('DROP TABLE IF EXISTS ',GT_DB,'.tmp_rnc_avg_voronoi;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('CREATE TABLE ',GT_DB,'.tmp_rnc_avg_voronoi
		SELECT RNC_ID,AVG(NB_AVG_DISTANCE_VORONOI) AS AVG_VORONOI
		FROM ',GT_DB,'.nt_current 
		GROUP BY RNC_ID
	;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('UPDATE ',GT_DB,'.nt_rnc_current A, ',GT_DB,'.tmp_rnc_avg_voronoi B
		SET A.AVG_VORONOI = 
			CASE WHEN B.AVG_VORONOI <= 3000 THEN 
				CASE WHEN (5 * B.AVG_VORONOI ) > 3000 THEN (5 * B.AVG_VORONOI ) END
			WHEN B.AVG_VORONOI > 3000 THEN (3 * B.AVG_VORONOI ) END
		WHERE A.RNC_ID = B.RNC_ID;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('UPDATE ',GT_DB,'.nt_rnc_current A
		SET A.AVG_VORONOI = 3000
		WHERE A.AVG_VORONOI IS NULL;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'SP_Generate_NT','INSERT INTO gt_covmo.`dim_cluster_group`', NOW());
	
	SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS `',GT_DB,'`.`dim_cluster_group_',WORKER_ID,'`;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt; 
	
	SET @SqlCmd=CONCAT('CREATE TEMPORARY TABLE ',GT_DB,'.`dim_cluster_group_',WORKER_ID,'` 
				SELECT * FROM gt_covmo.dim_cluster_group;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS `',GT_DB,'`.`tmp_dim_cluster_group_',WORKER_ID,'`;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt; 
	
	SET @SqlCmd=CONCAT('CREATE TEMPORARY TABLE ',GT_DB,'.`tmp_dim_cluster_group_',WORKER_ID,'`
				SELECT 
					DISTINCT A.CLUSTER_NAME AS CLUSTER_NAME,1 AS ENABLED
				FROM ',GT_DB,'.nt_current A LEFT JOIN `',GT_DB,'`.`dim_cluster_group_',WORKER_ID,'`  B
				ON A.CLUSTER_NAME=B.CLUSTER_NAME COLLATE utf8_swedish_ci
				WHERE B.CLUSTER_ID IS NULL AND A.CLUSTER_NAME<>'''' AND A.CLUSTER_NAME IS NOT NULL;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	SET @SqlCmd=CONCAT('INSERT INTO gt_covmo.dim_cluster_group
			(CLUSTER_NAME,ENABLED)
			SELECT  
				CLUSTER_NAME,ENABLED
			FROM ',GT_DB,'.`tmp_dim_cluster_group_',WORKER_ID,'`;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS `',GT_DB,'`.`cur_dim_cluster_group_',WORKER_ID,'`;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt; 
	
	SET @SqlCmd=CONCAT('CREATE TEMPORARY TABLE ',GT_DB,'.`cur_dim_cluster_group_',WORKER_ID,'` 
				SELECT * FROM gt_covmo.dim_cluster_group;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('UPDATE ',GT_DB,'.nt_current A ,`',GT_DB,'`.`cur_dim_cluster_group_',WORKER_ID,'`  B
				SET A.CLUSTER_ID=B.CLUSTER_ID
				WHERE A.CLUSTER_NAME=B.CLUSTER_NAME COLLATE utf8_swedish_ci;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('INSERT INTO ',GT_DB,'.rule_nt_nbr_umts
			SELECT *
			FROM ',GT_COVMO,'.rule_nt_nbr_umts;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt; 
	
	SET @SqlCmd=CONCAT('Drop table if exists ',GT_DB,'.tmp_nt_cell_u;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('Drop table if exists ',GT_DB,'.tmp_nt_nbr_u;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
-- 	SET @SqlCmd=CONCAT('Drop table if exists ',GT_DB,'.nt_antenna;');
-- 	PREPARE Stmt FROM @SqlCmd;
-- 	EXECUTE Stmt;
-- 	DEALLOCATE PREPARE Stmt;
-- 	
-- 	SET @SqlCmd=CONCAT('Drop table if exists ',GT_DB,'.nt_cell;');
-- 	PREPARE Stmt FROM @SqlCmd;
-- 	EXECUTE Stmt;
-- 	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('Drop table if exists ',GT_DB,'.nt_cell_attribute;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('Drop table if exists ',GT_DB,'.nt_neighbor;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('Drop table if exists ',GT_DB,'.nt_rnc;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('DROP TABLE IF EXISTS ',GT_DB,'.tmp_avg_voronoi_distance;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('DROP TABLE IF EXISTS ',GT_DB,'.tmp_rnc_avg_voronoi;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	CALL gt_gw_main.SP_Generate_NT_Sub_Density(GT_DB,'gt_covmo','umts');
	CALL gt_gw_main.SP_Generate_NT_Sub_Pathloss(GT_DB,'gt_covmo','umts');
	INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'SP_Generate_NT','DIST rule', NOW());
	
	SET @SqlCmd=CONCAT('UPDATE ',GT_DB,'.nt_current
			SET 
			  NB_AVG_DISTANCE_UMTS = CASE WHEN NB_AVG_DISTANCE_UMTS < 100 THEN 100 ELSE NB_AVG_DISTANCE_UMTS END,
			  NB_AVG_DISTANCE_VORONOI = CASE WHEN NB_AVG_DISTANCE_VORONOI < 100 THEN 100 WHEN NB_AVG_DISTANCE_VORONOI IS NULL THEN 3000 ELSE NB_AVG_DISTANCE_VORONOI END
			;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt; 
	
	INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'SP_Generate_NT','ANTENNA_RADIUS rule', NOW());
	
	SET @SqlCmd=CONCAT('UPDATE ',GT_DB,'.nt_antenna_current A, ',GT_DB,'.nt_current B
			SET 
			  ANTENNA_RADIUS = CASE WHEN INDOOR = 0 AND (NB_AVG_DISTANCE_VORONOI*(1.5-SITE_DENSITY_TYPE/10)) > PATHLOSS_DISTANCE
					THEN (NB_AVG_DISTANCE_VORONOI*(1.5-SITE_DENSITY_TYPE/10)) 
				WHEN INDOOR = 0 AND (NB_AVG_DISTANCE_VORONOI*(1.5-SITE_DENSITY_TYPE/10)) < PATHLOSS_DISTANCE
					THEN PATHLOSS_DISTANCE
			ELSE 50 END
			WHERE A.rnc_id = B.rnc_id and A.cell_id = B.cell_id
			;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt; 
	INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'SP_Generate_NT','CLOSED_RADIUS rule', NOW());
	
	SET @SqlCmd=CONCAT('UPDATE ',GT_DB,'.nt_antenna_current A, ',GT_DB,'.nt_current B
			SET 
			  CLOSED_RADIUS = CASE WHEN INDOOR = 0 THEN ( 1+ FLOOR(HEIGHT/ 50 )) * NB_AVG_DISTANCE_VORONOI / 5
				ELSE 50 END
			WHERE A.rnc_id = B.rnc_id and A.cell_id = B.cell_id
			;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt; 	
		
	
	
	INSERT INTO gt_gw_main.SP_LOG VALUES(GT_DB,'SP_Generate_NT',CONCAT('Done: ',TIMESTAMPDIFF(SECOND,START_TIME,SYSDATE()),' seconds.'), NOW());
	
END$$
DELIMITER ;
