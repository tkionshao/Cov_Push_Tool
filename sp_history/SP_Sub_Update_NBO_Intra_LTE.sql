DELIMITER $$
USE `gt_gw_main`$$
DROP PROCEDURE IF EXISTS `SP_CreateDB_LTE`$$
CREATE DEFINER=`covmo`@`%` PROCEDURE `SP_Sub_Update_NBO_Intra_LTE`(IN GT_DB VARCHAR(100),IN GT_COVMO VARCHAR(100))
BEGIN
	DECLARE START_TIME DATETIME DEFAULT SYSDATE();
	DECLARE WORKER_ID VARCHAR(10) DEFAULT CONNECTION_ID();
	DECLARE CURRENT_NT_DB VARCHAR(50) DEFAULT CONCAT('gt_nt_',gt_strtok(GT_DB,3,'_'));
	
	SET @SqlCmd=CONCAT('SELECT `att_value` INTO @STANDARD_RSRP FROM ',CURRENT_NT_DB,'.`sys_config` WHERE `group_name`=''NBO'' AND att_name = ''STANDARD_RSRP'';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	SET @SqlCmd=CONCAT('SELECT `att_value` INTO @MAX_INTRA_NBR_LTE FROM ',CURRENT_NT_DB,'.`sys_config` WHERE `group_name`=''NBO'' AND att_name = ''MAX_INTRA_NBR_LTE'';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;	
	
	INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'SP_Sub_Update_NBO_Intra_LTE','START ', NOW());
	
	SET @SqlCmd=CONCAT('TRUNCATE TABLE ',GT_DB,'.opt_nbr_result_intra_lte;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
		
	INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'SP_Sub_Update_NBO_Intra_LTE','Start', NOW());
	SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS `',GT_DB,'`.`opt_nbr_result_intra_lte_',WORKER_ID,'`;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt; 
	SET @SqlCmd=CONCAT('CREATE TEMPORARY TABLE ',GT_DB,'.`opt_nbr_result_intra_lte_',WORKER_ID,'` 
			LIKE ',GT_DB,'.opt_nbr_result_intra_lte;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('ALTER TABLE ',GT_DB,'.`opt_nbr_result_intra_lte_',WORKER_ID,'`
			ADD COLUMN `REF_AVG_DISTANCE` DOUBLE NULL,
			ADD COLUMN `TMP_RSRP_CNT` DOUBLE NULL;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('INSERT INTO ',GT_DB,'.`opt_nbr_result_intra_lte_',WORKER_ID,'`
			(ENODEB_ID,CELL_ID,NBR_ENODEB_ID,NBR_CELL_ID,NBR_TYPE,
			HO_COUNT,PINGPONG_HO_CNT,HO_FAIL_CNT,HO_RE_EST_CNT,MEAS_COUNT,
			RSRP_SUM,RSRQ_SUM,RSRP_CNT,RSRQ_CNT,BEST_MEAS_CNT,SERVING_RSRP_SUM,
			SERVING_RSRQ_SUM,SERVING_RSRP_CNT,SERVING_RSRQ_CNT,TA_HO_SUM,
			TA_HO_CNT,HO_INTERRUP_TIME,HO_INTERRUP_CNT)
			SELECT
				ENODEB_ID,
				CELL_ID,
				NBR_ENODEB_ID,
				NBR_CELL_ID,
				NBR_TYPE,
				SUM(HO_COUNT),
				SUM(PINGPONG_HO_CNT),
				SUM(HO_FAIL_CNT),
				SUM(HO_RE_EST_CNT),
				SUM(MEAS_COUNT),
				SUM(RSRP_SUM),
				SUM(RSRQ_SUM),
				SUM(RSRP_CNT),
				SUM(RSRQ_CNT),
				SUM(BEST_MEAS_CNT),
				SUM(SERVING_RSRP_SUM),
				SUM(SERVING_RSRQ_SUM),
				SUM(SERVING_RSRP_CNT),
				SUM(SERVING_RSRQ_CNT),
				SUM(TA_HO_SUM),
				SUM(TA_HO_CNT),
				SUM(HO_INTERRUP_TIME),
				SUM(HO_INTERRUP_CNT)
			FROM ',GT_DB,'.opt_nbr_inter_intra_lte
			WHERE NBR_TYPE IN (2,20)
			GROUP BY ENODEB_ID,CELL_ID,NBR_ENODEB_ID,NBR_CELL_ID,NBR_TYPE;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	SET @SqlCmd=CONCAT('ALTER TABLE `',GT_DB,'`.`opt_nbr_result_intra_lte_',WORKER_ID,'` ADD INDEX `IX_KEY` (`ENODEB_ID`, `CELL_ID`, `NBR_ENODEB_ID`, `NBR_CELL_ID`);;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt; 
	
	INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'SP_Sub_Update_NBO_Intra_LTE','STEP0', NOW());
	
	SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS `',GT_DB,'`.`opt_intra_lte_',WORKER_ID,'`;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt; 
	
	SET @SqlCmd=CONCAT('CREATE TEMPORARY TABLE ',GT_DB,'.`opt_intra_lte_',WORKER_ID,'` (
				`ENODEB_ID` MEDIUMINT(9) DEFAULT NULL,
				`CELL_ID` MEDIUMINT(9) DEFAULT NULL,
				`NBR_ENODEB_ID` INT(11) DEFAULT NULL,
				`NBR_CELL_ID` MEDIUMINT(9) DEFAULT NULL,
				`NBR_TYPE` VARCHAR(10) DEFAULT NULL
				) ENGINE=MYISAM DEFAULT CHARSET=latin1 DELAY_KEY_WRITE=1;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;	
	
	SET @SqlCmd=CONCAT('
			INSERT INTO ',GT_DB,'.`opt_intra_lte_',WORKER_ID,'`
				(ENODEB_ID,CELL_ID,NBR_ENODEB_ID,NBR_CELL_ID,NBR_TYPE
				)
			SELECT 
				B.ENODEB_ID,
				B.CELL_ID,
				B.NBR_ENODEB_ID,
				B.NBR_CELL_ID,
				B.NBR_TYPE
			FROM ',GT_DB,'.`opt_nbr_result_intra_lte_',WORKER_ID,'` A RIGHT JOIN ',CURRENT_NT_DB,'.nt_nbr_4_4_current_lte B
			ON A.enodeb_id = B.enodeb_id AND A.cell_id = B.cell_id AND A.nbr_enodeb_id = B.nbr_enodeb_id AND A.nbr_cell_id = B.nbr_cell_id
			WHERE A.enodeb_id IS NULL AND B.NBR_TYPE = 2;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('
			INSERT INTO ',GT_DB,'.`opt_nbr_result_intra_lte_',WORKER_ID,'`
				(ENODEB_ID,CELL_ID,NBR_ENODEB_ID,NBR_CELL_ID,NBR_TYPE)
			SELECT * FROM ',GT_DB,'.`opt_intra_lte_',WORKER_ID,'`;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('ALTER TABLE ',GT_DB,'.`opt_nbr_result_intra_lte_',WORKER_ID,'`
			ADD INDEX `ENODEB_ID` (`ENODEB_ID`, `CELL_ID`, `NBR_ENODEB_ID`, `NBR_CELL_ID`, `NBR_TYPE`);');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'SP_Sub_Update_NBO_Intra_LTE','STEP1 - DISTANCE_METER & PRI_ANG', NOW());
	
	SET @SqlCmd=CONCAT('UPDATE ',GT_DB,'.`opt_nbr_result_intra_lte_',WORKER_ID,'` A, ',CURRENT_NT_DB,'.nt_antenna_current_lte B ,',CURRENT_NT_DB,'.nt_antenna_current_lte C
			SET 
			A.LONGITUDE = B.LONGITUDE,
			A.LATITUDE = B.LATITUDE,
			A.HEIGHT = B.ANTENNA_HEIGHT,
			A.DISTANCE_METER = gt_covmo_distance(B.LONGITUDE,B.LATITUDE, C.LONGITUDE,C.LATITUDE),
			A.PRI_ANG = floor(gt_covmo_azimuth_angle(B.LONGITUDE,B.LATITUDE,B.AZIMUTH,C.LONGITUDE,C.LATITUDE,C.AZIMUTH)/60)/10+1
			WHERE A.ENODEB_ID = B.ENODEB_ID AND A.CELL_ID = B.CELL_ID AND A.NBR_ENODEB_ID = C.ENODEB_ID AND A.NBR_CELL_ID =  C.CELL_ID;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('UPDATE ',GT_DB,'.`opt_nbr_result_intra_lte_',WORKER_ID,'`
			SET PRI_ANG = 1.2
			WHERE PRI_ANG IS NULL ;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'SP_Sub_Update_NBO_Intra_LTE','STEP1 - UPDATE PRI_DISTANCE', NOW());
	
	SET @SqlCmd=CONCAT('UPDATE ',GT_DB,'.`opt_nbr_result_intra_lte_',WORKER_ID,'`
			SET PRI_DISTANCE = (DISTANCE_METER + 1500) / 500
			WHERE DISTANCE_METER IS NOT NULL;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS `',GT_DB,'`.`opt_pri_dist_intra_lte_',WORKER_ID,'`;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt; 
	
	SET @SqlCmd=CONCAT('CREATE TEMPORARY TABLE ',GT_DB,'.`opt_pri_dist_intra_lte_',WORKER_ID,'` (
				`ENODEB_ID` MEDIUMINT(9) DEFAULT NULL,
				`CELL_ID` MEDIUMINT(9) DEFAULT NULL,
				`NBR_ENODEB_ID` INT(11) DEFAULT NULL,
				`NBR_CELL_ID` MEDIUMINT(9) DEFAULT NULL,
				`NBR_TYPE` VARCHAR(10) DEFAULT NULL,
				`PRI_DISTANCE` DOUBLE DEFAULT NULL,
				KEY `IX_ENODEB_CELL` (`CELL_ID`,`ENODEB_ID`, `NBR_CELL_ID`, `NBR_ENODEB_ID`,`NBR_TYPE`)
				) ENGINE=MYISAM DEFAULT CHARSET=latin1 DELAY_KEY_WRITE=1;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	SET @SqlCmd=CONCAT('INSERT INTO ',GT_DB,'.`opt_pri_dist_intra_lte_',WORKER_ID,'`
			SELECT
				A.ENODEB_ID,
				A.CELL_ID,
				A.NBR_ENODEB_ID,
				A.NBR_CELL_ID,
				A.NBR_TYPE,
				CASE WHEN B.NBR_DISTANCE_4G_VORONOI IS NOT NULL THEN 1.5*(B.NBR_DISTANCE_4G_VORONOI+1500)/500 
				     ELSE 1.5* (2000 +1500) /500 END AS PRI_DISTANCE
			FROM ',GT_DB,'.`opt_nbr_result_intra_lte_',WORKER_ID,'` A LEFT JOIN ',CURRENT_NT_DB,'.nt_cell_current_lte B
			ON A.enodeb_id = B.enodeb_id AND A.cell_id = B.cell_id
			WHERE
			A.DISTANCE_METER IS NULL AND A.NBR_TYPE = 2;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('UPDATE ',GT_DB,'.`opt_nbr_result_intra_lte_',WORKER_ID,'` A, ',GT_DB,'.`opt_pri_dist_intra_lte_',WORKER_ID,'` B
			SET A.PRI_DISTANCE = B.PRI_DISTANCE
			WHERE A.ENODEB_ID = B.ENODEB_ID AND A.CELL_ID = B.CELL_ID AND A.NBR_ENODEB_ID = B.NBR_ENODEB_ID AND A.NBR_CELL_ID =  B.NBR_CELL_ID 
				AND A.NBR_TYPE = B.NBR_TYPE;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'SP_Sub_Update_NBO_Intra_LTE','STEP1 - UPDATE PRO_HO', NOW());
	
	SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS `',GT_DB,'`.`opt_pri_ho_intra_lte_',WORKER_ID,'`;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt; 
	
	SET @SqlCmd=CONCAT('CREATE TEMPORARY TABLE ',GT_DB,'.`opt_pri_ho_intra_lte_',WORKER_ID,'` (
				`ENODEB_ID` MEDIUMINT(9) DEFAULT NULL,
				`CELL_ID` MEDIUMINT(9) DEFAULT NULL,
				`NBR_ENODEB_ID` INT(11) DEFAULT NULL,
				`NBR_CELL_ID` MEDIUMINT(9) DEFAULT NULL,
				`NBR_TYPE` VARCHAR(10) DEFAULT NULL,
				`PRI_HO` MEDIUMINT(9) DEFAULT NULL,
				`tmp` MEDIUMINT(9) DEFAULT NULL,
				`tmp2` MEDIUMINT(9) DEFAULT NULL,
				KEY `IX_ENODEB_CELL` (`CELL_ID`,`ENODEB_ID`, `NBR_CELL_ID`, `NBR_ENODEB_ID`,`NBR_TYPE`)
				) ENGINE=MYISAM DEFAULT CHARSET=latin1 DELAY_KEY_WRITE=1;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('INSERT INTO ',GT_DB,'.`opt_pri_ho_intra_lte_',WORKER_ID,'`
			SELECT
			  ENODEB_ID,
			  CELL_ID,
			  NBR_ENODEB_ID,
			  NBR_CELL_ID,
			  NBR_TYPE,
			  @curRank := IF(@ENODEB_ID=ENODEB_ID AND @CELL_ID = CELL_ID, @curRank + 1, 1) AS `PRI_HO`,
			  @ENODEB_ID := ENODEB_ID AS tmp,
			  @CELL_ID := CELL_ID AS tmp2
			FROM ',GT_DB,'.`opt_nbr_result_intra_lte_',WORKER_ID,'`, (SELECT @curRank := 0) r,(SELECT @ENODEB_ID:='''') s,(SELECT @CELL_ID:='''') w 
			ORDER BY ENODEB_ID,CELL_ID,HO_COUNT DESC, PRI_DISTANCE ASC;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('UPDATE ',GT_DB,'.`opt_nbr_result_intra_lte_',WORKER_ID,'` A, ',GT_DB,'.`opt_pri_ho_intra_lte_',WORKER_ID,'` B
			SET A.PRI_HO = B.PRI_HO
			WHERE A.ENODEB_ID = B.ENODEB_ID AND A.CELL_ID = B.CELL_ID AND A.NBR_ENODEB_ID = B.NBR_ENODEB_ID AND A.NBR_CELL_ID =  B.NBR_CELL_ID 
				AND A.NBR_TYPE = B.NBR_TYPE;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'SP_Sub_Update_NBO_Intra_LTE','STEP1 - UPDATE PRI_MEAS', NOW());
	
	SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS `',GT_DB,'`.`opt_pri_meas_intra_lte_',WORKER_ID,'`;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt; 
	
	SET @SqlCmd=CONCAT('CREATE TEMPORARY TABLE ',GT_DB,'.`opt_pri_meas_intra_lte_',WORKER_ID,'` (
				`ENODEB_ID` MEDIUMINT(9) DEFAULT NULL,
				`CELL_ID` MEDIUMINT(9) DEFAULT NULL,
				`NBR_ENODEB_ID` INT(11) DEFAULT NULL,
				`NBR_CELL_ID` MEDIUMINT(9) DEFAULT NULL,
				`NBR_TYPE` VARCHAR(10) DEFAULT NULL,
				`PRI_MEAS` MEDIUMINT(9) DEFAULT NULL,
				`tmp` MEDIUMINT(9) DEFAULT NULL,
				`tmp2` MEDIUMINT(9) DEFAULT NULL,
				KEY `IX_ENODEB_CELL` (`CELL_ID`,`ENODEB_ID`, `NBR_CELL_ID`, `NBR_ENODEB_ID`,`NBR_TYPE`)
				) ENGINE=MYISAM DEFAULT CHARSET=latin1 DELAY_KEY_WRITE=1;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('INSERT INTO ',GT_DB,'.`opt_pri_meas_intra_lte_',WORKER_ID,'`
			SELECT
			  ENODEB_ID,
			  CELL_ID,
			  NBR_ENODEB_ID,
			  NBR_CELL_ID,
			  NBR_TYPE,
			  @curRank := IF(@ENODEB_ID=ENODEB_ID AND @CELL_ID = CELL_ID, @curRank + 1, 1) AS PRI_MEAS,
			  @ENODEB_ID := ENODEB_ID AS tmp,
			  @CELL_ID := CELL_ID AS tmp2
			FROM ',GT_DB,'.`opt_nbr_result_intra_lte_',WORKER_ID,'`, (SELECT @curRank := 0) r,(SELECT @ENODEB_ID:='''') s,(SELECT @CELL_ID:='''') w 
			ORDER BY ENODEB_ID,CELL_ID,(BEST_MEAS_CNT + MEAS_COUNT) DESC, BEST_MEAS_CNT DESC, PRI_DISTANCE ASC;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('UPDATE ',GT_DB,'.`opt_nbr_result_intra_lte_',WORKER_ID,'` A, ',GT_DB,'.`opt_pri_meas_intra_lte_',WORKER_ID,'` B
			SET A.PRI_MEAS = B.PRI_MEAS
			WHERE A.ENODEB_ID = B.ENODEB_ID AND A.CELL_ID = B.CELL_ID AND A.NBR_ENODEB_ID = B.NBR_ENODEB_ID AND A.NBR_CELL_ID =  B.NBR_CELL_ID 
				AND A.NBR_TYPE = B.NBR_TYPE;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'SP_Sub_Update_NBO_Intra_LTE','STEP1 - UPDATE PRI_RSRP', NOW());
	
	SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS `',GT_DB,'`.`opt_pri_rsrp_intra_lte_',WORKER_ID,'`;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt; 
	
	SET @SqlCmd=CONCAT('CREATE TEMPORARY TABLE ',GT_DB,'.`opt_pri_rsrp_intra_lte_',WORKER_ID,'` (
				`ENODEB_ID` MEDIUMINT(9) DEFAULT NULL,
				`CELL_ID` MEDIUMINT(9) DEFAULT NULL,
				`NBR_ENODEB_ID` INT(11) DEFAULT NULL,
				`NBR_CELL_ID` MEDIUMINT(9) DEFAULT NULL,
				`NBR_TYPE` VARCHAR(10) DEFAULT NULL,
				`PRI_RSRP` MEDIUMINT(9) DEFAULT NULL,
				`tmp` MEDIUMINT(9) DEFAULT NULL,
				`tmp2` MEDIUMINT(9) DEFAULT NULL,
				KEY `IX_ENODEB_CELL` (`CELL_ID`,`ENODEB_ID`, `NBR_CELL_ID`, `NBR_ENODEB_ID`,`NBR_TYPE`)
				) ENGINE=MYISAM DEFAULT CHARSET=latin1 DELAY_KEY_WRITE=1;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('UPDATE ',GT_DB,'.`opt_nbr_result_intra_lte_',WORKER_ID,'` 
			SET TMP_RSRP_CNT = CASE WHEN RSRP_CNT > 0 THEN 0 ELSE 1 END;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('INSERT INTO ',GT_DB,'.`opt_pri_rsrp_intra_lte_',WORKER_ID,'`
			SELECT
			  ENODEB_ID,
			  CELL_ID,
			  NBR_ENODEB_ID,
			  NBR_CELL_ID,
			  NBR_TYPE,
			  @curRank := IF(@ENODEB_ID=ENODEB_ID AND @CELL_ID = CELL_ID, @curRank + 1, 1) AS PRI_RSRP,
			  @ENODEB_ID := ENODEB_ID AS tmp,
			  @CELL_ID := CELL_ID AS tmp2
			FROM ',GT_DB,'.`opt_nbr_result_intra_lte_',WORKER_ID,'`, (SELECT @curRank := 0) r,(SELECT @ENODEB_ID:='''') s,(SELECT @CELL_ID:='''') w 
			ORDER BY ENODEB_ID,CELL_ID,TMP_RSRP_CNT,(RSRP_SUM / RSRP_CNT) DESC, PRI_DISTANCE ASC;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	SET @SqlCmd=CONCAT('UPDATE ',GT_DB,'.`opt_nbr_result_intra_lte_',WORKER_ID,'` A, ',GT_DB,'.`opt_pri_rsrp_intra_lte_',WORKER_ID,'` B
			SET A.PRI_RSRP = B.PRI_RSRP
			WHERE A.ENODEB_ID = B.ENODEB_ID AND A.CELL_ID = B.CELL_ID AND A.NBR_ENODEB_ID = B.NBR_ENODEB_ID AND A.NBR_CELL_ID =  B.NBR_CELL_ID 
				AND A.NBR_TYPE = B.NBR_TYPE;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'SP_Sub_Update_NBO_Intra_LTE','STEP1 - UPDATE PRI_RSRQ', NOW());
	
	SET @SqlCmd=CONCAT('UPDATE ',GT_DB,'.`opt_nbr_result_intra_lte_',WORKER_ID,'`
			SET PRI_RSRQ = 9;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	
	INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'SP_Sub_Update_NBO_Intra_LTE','STEP2 - UPDATE PRI_HO', NOW());
	
	SET @SqlCmd=CONCAT('UPDATE ',GT_DB,'.`opt_nbr_result_intra_lte_',WORKER_ID,'`
			SET PRI_HO = 999
			WHERE NBR_TYPE = 20 AND (HO_FAIL_CNT/HO_COUNT) > 0.9 AND HO_COUNT > 1;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'SP_Sub_Update_NBO_Intra_LTE','STEP2 - UPDATE PRI_RSRP', NOW());
	
	SET @SqlCmd=CONCAT('UPDATE ',GT_DB,'.`opt_nbr_result_intra_lte_',WORKER_ID,'`
			SET PRI_RSRP = 999
			WHERE NBR_TYPE = 20 AND @STANDARD_RSRP > (RSRP_SUM / RSRP_CNT) AND HO_COUNT = 0;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'SP_Sub_Update_NBO_Intra_LTE','STEP2 - UPDATE PRI_DISTANCE', NOW());
	
	SET @SqlCmd=CONCAT('UPDATE ',GT_DB,'.`opt_nbr_result_intra_lte_',WORKER_ID,'` A, ',CURRENT_NT_DB,'.nt_cell_current_lte B 
			SET A.REF_AVG_DISTANCE = CASE WHEN NBR_DISTANCE_4G_CM > NBR_DISTANCE_4G_VORONOI THEN NBR_DISTANCE_4G_VORONOI ELSE NBR_DISTANCE_4G_CM END
			WHERE A.ENODEB_ID = B.ENODEB_ID AND A.CELL_ID = B.CELL_ID;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('UPDATE ',GT_DB,'.`opt_nbr_result_intra_lte_',WORKER_ID,'`
			SET PRI_DISTANCE = 999
			WHERE NBR_TYPE = 20 AND DISTANCE_METER > 3*REF_AVG_DISTANCE;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'SP_Sub_Update_NBO_Intra_LTE','STEP3 - UPDATE PRI_WEIGHTED', NOW());
	
	SET @SqlCmd=CONCAT('UPDATE ',GT_DB,'.`opt_nbr_result_intra_lte_',WORKER_ID,'`
			SET PRI_WEIGHTED = (3*PRI_HO + PRI_MEAS + PRI_RSRP + PRI_RSRQ + POWER(PRI_DISTANCE,1.5)*PRI_ANG);');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'SP_Sub_Update_NBO_Intra_LTE','STEP3 - UPDATE RANK', NOW());
	
	SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS `',GT_DB,'`.`opt_rank_intra_lte_',WORKER_ID,'`;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt; 
	
	SET @SqlCmd=CONCAT('CREATE TEMPORARY TABLE ',GT_DB,'.`opt_rank_intra_lte_',WORKER_ID,'` (
				`ENODEB_ID` MEDIUMINT(9) DEFAULT NULL,
				`CELL_ID` MEDIUMINT(9) DEFAULT NULL,
				`NBR_ENODEB_ID` INT(11) DEFAULT NULL,
				`NBR_CELL_ID` MEDIUMINT(9) DEFAULT NULL,
				`RANK` MEDIUMINT(9) DEFAULT NULL,
				`tmp` MEDIUMINT(9) DEFAULT NULL,
				`tmp2` MEDIUMINT(9) DEFAULT NULL,
				KEY `IX_ENODEB_CELL` (`CELL_ID`,`ENODEB_ID`, `NBR_CELL_ID`, `NBR_ENODEB_ID`)
				) ENGINE=MYISAM DEFAULT CHARSET=latin1 DELAY_KEY_WRITE=1;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	SET @SqlCmd=CONCAT('INSERT INTO ',GT_DB,'.`opt_rank_intra_lte_',WORKER_ID,'`
			SELECT
			  ENODEB_ID,
			  CELL_ID,
			  NBR_ENODEB_ID,
			  NBR_CELL_ID,
			  @curRank := IF(@ENODEB_ID=ENODEB_ID AND @CELL_ID = CELL_ID, @curRank + 1, 1) AS RANK,
			  @ENODEB_ID := ENODEB_ID AS tmp,
			  @CELL_ID := CELL_ID AS tmp2
			FROM ',GT_DB,'.`opt_nbr_result_intra_lte_',WORKER_ID,'`, (SELECT @curRank := 0) r,(SELECT @ENODEB_ID:='''') s,(SELECT @CELL_ID:='''') w 
			ORDER BY ENODEB_ID,CELL_ID,(CASE WHEN ENODEB_ID = NBR_ENODEB_ID THEN 0 ELSE 1 END) , PRI_WEIGHTED, 
			PRI_DISTANCE ASC
			;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('UPDATE ',GT_DB,'.`opt_nbr_result_intra_lte_',WORKER_ID,'` A, ',GT_DB,'.`opt_rank_intra_lte_',WORKER_ID,'` B
			SET A.RANK = B.RANK
			WHERE A.ENODEB_ID = B.ENODEB_ID AND A.CELL_ID = B.CELL_ID AND A.NBR_ENODEB_ID = B.NBR_ENODEB_ID AND A.NBR_CELL_ID =  B.NBR_CELL_ID;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'SP_Sub_Update_NBO_Intra_LTE','STEP4', NOW());
	
	SET @SqlCmd=CONCAT('INSERT INTO ',GT_DB,'.`opt_nbr_result_intra_lte`
			    (`ENODEB_ID`,
			     `CELL_ID`,
			     `NBR_ENODEB_ID`,
			     `NBR_CELL_ID`,
			     `NBR_TYPE`,
			     `DISTANCE_METER`,
			     `HO_COUNT`,
			     `PINGPONG_HO_CNT`,
			     `HO_FAIL_CNT`,
			     `HO_RE_EST_CNT`,
			     `MEAS_COUNT`,
			     `RSRP_SUM`,
			     `RSRQ_SUM`,
			     `RSRP_CNT`,
			     `RSRQ_CNT`,
			     `BEST_MEAS_CNT`,
			     `SERVING_RSRP_SUM`,
			     `SERVING_RSRQ_SUM`,
			     `SERVING_RSRP_CNT`,
			     `SERVING_RSRQ_CNT`,
			     `TA_HO_SUM`,
			     `TA_HO_CNT`,
			     `HO_INTERRUP_TIME`,
			     `HO_INTERRUP_CNT`,
			     `PRI_WEIGHTED`,
			     `RANK`,
			     `ACTION`,
			     `LONGITUDE`,
			     `LATITUDE`,
			     `HEIGHT`,
			     `PRI_HO`,
			     `PRI_MEAS`,
			     `PRI_RSRP`,
			     `PRI_RSRQ`,
			     `PRI_DISTANCE`,
			     `PRI_ANG`
			)
			SELECT
			  `ENODEB_ID`,
			  `CELL_ID`,
			  `NBR_ENODEB_ID`,
			  `NBR_CELL_ID`,
			  IF(`NBR_TYPE` = 2, ''CM Intra'', ''N/A'') AS `NBR_TYPE`,
			  `DISTANCE_METER`,
			  `HO_COUNT`,
			  `PINGPONG_HO_CNT`,
			  `HO_FAIL_CNT`,
			  `HO_RE_EST_CNT`,
			  `MEAS_COUNT`,
			  `RSRP_SUM`,
			  `RSRQ_SUM`,
			  `RSRP_CNT`,
			  `RSRQ_CNT`,
			  `BEST_MEAS_CNT`,
			  `SERVING_RSRP_SUM`,
			  `SERVING_RSRQ_SUM`,
			  `SERVING_RSRP_CNT`,
			  `SERVING_RSRQ_CNT`,
			  `TA_HO_SUM`,
			  `TA_HO_CNT`,
			  `HO_INTERRUP_TIME`,
			  `HO_INTERRUP_CNT`,
			  `PRI_WEIGHTED`,
			  `RANK`,
			  CASE WHEN (`RANK`<=@MAX_INTRA_NBR_LTE) AND `NBR_TYPE`=2 THEN ''Keep''
				WHEN (`RANK`>@MAX_INTRA_NBR_LTE) AND `NBR_TYPE`=2 THEN ''Remove''
				WHEN (`RANK`<@MAX_INTRA_NBR_LTE) AND `NBR_TYPE`=20 AND PRI_WEIGHTED<1000 THEN ''Add''
				WHEN (`RANK`>=@MAX_INTRA_NBR_LTE) AND `NBR_TYPE`=20 THEN ''None''
				WHEN PRI_WEIGHTED>=1000 AND `NBR_TYPE`=20 THEN ''Ignore''
			  END AS ACTION,
			  `LONGITUDE`,
			  `LATITUDE`,
			  `HEIGHT`,
			  `PRI_HO`,
			  `PRI_MEAS`,
			  `PRI_RSRP`,
			  `PRI_RSRQ`,
			  `PRI_DISTANCE`,
			  `PRI_ANG`
			FROM ',GT_DB,'.`opt_nbr_result_intra_lte_',WORKER_ID,'`;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'SP_Sub_Update_NBO_Intra_LTE','STEP5 - update PU_ID and SUB_REGION_ID', NOW());
	
	SET @SqlCmd=CONCAT('UPDATE ',GT_DB,'.`opt_nbr_result_intra_lte` A, ',CURRENT_NT_DB,'.nt_cell_current_lte B
			SET 
				A.PU_ID = B.PU_ID,
				A.SUB_REGION_ID = B.SUB_REGION_ID
			WHERE A.ENODEB_ID = B.ENODEB_ID AND A.CELL_ID = B.CELL_ID;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'SP_Sub_Update_NBO_Intra_LTE',CONCAT('Done: ',TIMESTAMPDIFF(SECOND,START_TIME,SYSDATE()),' seconds.'), NOW());
	
END$$
DELIMITER ;
