CREATE DEFINER=`covmo`@`%` PROCEDURE `SP_Generate_Global_Statistic_tile_umts`(IN DATA_DATE DATE,IN DATA_HOUR TINYINT(2),IN PU_ID MEDIUMINT(9),IN TECH_MASK TINYINT(2), IN GT_DB VARCHAR(100),IN TileResolution VARCHAR(30))
BEGIN
	DECLARE WORKER_ID VARCHAR(10) DEFAULT CONNECTION_ID();
	DECLARE NT_DATE VARCHAR(50) DEFAULT DATE_FORMAT(DATE(DATA_DATE),'%Y%m%d');
	DECLARE NT_DB VARCHAR(50) DEFAULT CONCAT('gt_nt_',NT_DATE,'');
	
	DECLARE TILE_UMTS_COLUMN_SUM_STR VARCHAR(6500) DEFAULT 
		'SUM(IFNULL(INIT_CALL_CNT,0)) AS INIT_CALL_CNT,
		SUM(IFNULL(END_CALL_CNT,0)) AS END_CALL_CNT,
		SUM(IFNULL(VOICE_CNT,0)) AS VOICE_CNT,
		SUM(IFNULL(VIDEO_CNT,0)) AS VIDEO_CNT,
		SUM(IFNULL(PS_R99_CNT,0)) AS PS_R99_CNT,
		SUM(IFNULL(PS_HSPA_CNT,0)) AS PS_HSPA_CNT,
		SUM(IFNULL(M_RAB_CNT,0)) AS M_RAB_CNT,
		SUM(IFNULL(SIGNAL_CNT,0)) AS SIGNAL_CNT,
		SUM(IFNULL(SMS_CNT,0)) AS SMS_CNT,
		SUM(IFNULL(PS_OTHER_CNT,0)) AS PS_OTHER_CNT,
		SUM(IFNULL(CALL_DUR_SUM,0)) AS CALL_DUR_SUM,
		SUM(IFNULL(CS_DUR_SUM,0)) AS CS_DUR_SUM,
		SUM(IFNULL(DROP_CNT,0)) AS DROP_CNT,
		SUM(IFNULL(BLOCK_CNT,0)) AS BLOCK_CNT,
		SUM(IFNULL(DROP_VOICE_CNT,0)) AS DROP_VOICE_CNT,
		SUM(IFNULL(DROP_VIDEO_CNT,0)) AS DROP_VIDEO_CNT,
		SUM(IFNULL(DROP_PS_R99_CNT,0)) AS DROP_PS_R99_CNT,
		SUM(IFNULL(DROP_PS_HSPA_CNT,0)) AS DROP_PS_HSPA_CNT,
		SUM(IFNULL(DROP_M_RAB_CNT,0)) AS DROP_M_RAB_CNT,
		SUM(IFNULL(DROP_SIGNAL_CNT,0)) AS DROP_SIGNAL_CNT,
		SUM(IFNULL(DROP_SMS_CNT,0)) AS DROP_SMS_CNT,
		SUM(IFNULL(DROP_PS_OTHER_CNT,0)) AS DROP_PS_OTHER_CNT,
		SUM(IFNULL(SHO_ATTEMPT_CNT,0)) AS SHO_ATTEMPT_CNT,
		SUM(IFNULL(SHO_FAILURE_CNT,0)) AS SHO_FAILURE_CNT,
		SUM(IFNULL(IFHO_ATTEMPT_CNT,0)) AS IFHO_ATTEMPT_CNT,
		SUM(IFNULL(IFHO_FAILURE_CNT,0)) AS IFHO_FAILURE_CNT,
		SUM(IFNULL(IRAT_ATTEMPT_CNT,0)) AS IRAT_ATTEMPT_CNT,
		SUM(IFNULL(IRAT_FAILURE_CNT,0)) AS IRAT_FAILURE_CNT,
		SUM(IFNULL(PS_UL_VOLUME_SUM,0)) AS PS_UL_VOLUME_SUM,
		SUM(IFNULL(PS_DL_VOLUME_SUM,0)) AS PS_DL_VOLUME_SUM,
		MAX(IFNULL(PS_UL_SPEED_MAX,0)) AS PS_UL_SPEED_MAX,
		MAX(IFNULL(PS_DL_SPEED_MAX,0)) AS PS_DL_SPEED_MAX,
		SUM(IFNULL(RSCP_SUM,0)) AS RSCP_SUM,
		SUM(IFNULL(RSCP_CNT,0)) AS RSCP_CNT,
		SUM(IFNULL(ECNO_SUM,0)) AS ECNO_SUM,
		SUM(IFNULL(ECNO_CNT,0)) AS ECNO_CNT,
		SUM(IFNULL(ACTIVE_SET_SUM,0)) AS ACTIVE_SET_SUM,
		SUM(IFNULL(ACTIVE_SET_CNT,0)) AS ACTIVE_SET_CNT,
		SUM(IFNULL(POLLUTED_PILOT_CNT,0)) AS POLLUTED_PILOT_CNT,
		SUM(IFNULL(PILOT_DOM_SUM,0)) AS PILOT_DOM_SUM,
		SUM(IFNULL(PILOT_CNT,0)) AS PILOT_CNT,
		SUM(IFNULL(T19_CNT,0)) AS T19_CNT,
		SUM(IFNULL(UL_THROUPUT_SUM,0)) AS UL_THROUPUT_SUM,
		SUM(IFNULL(UL_THROUPUT_CNT,0)) AS UL_THROUPUT_CNT,
		SUM(IFNULL(DL_THROUPUT_SUM,0)) AS DL_THROUPUT_SUM,
		SUM(IFNULL(DL_THROUPUT_CNT,0)) AS DL_THROUPUT_CNT,
		SUM(IFNULL(NON_BLOCK_VOICE_CNT,0)) AS NON_BLOCK_VOICE_CNT,
		SUM(IFNULL(NON_BLOCK_VIDEO_CNT,0)) AS NON_BLOCK_VIDEO_CNT,
		SUM(IFNULL(NON_BLOCK_PS_R99_CNT,0)) AS NON_BLOCK_PS_R99_CNT,
		SUM(IFNULL(NON_BLOCK_PS_HSPA_CNT,0)) AS NON_BLOCK_PS_HSPA_CNT,
		SUM(IFNULL(NON_BLOCK_M_RAB_CNT,0)) AS NON_BLOCK_M_RAB_CNT,
		SUM(IFNULL(NON_BLOCK_SIGNAL_CNT,0)) AS NON_BLOCK_SIGNAL_CNT,
		SUM(IFNULL(NON_BLOCK_SMS_CNT,0)) AS NON_BLOCK_SMS_CNT,
		SUM(IFNULL(NON_BLOCK_PS_OTHER_CNT,0)) AS NON_BLOCK_PS_OTHER_CNT,
		SUM(IFNULL(PS_CNT,0)) AS PS_CNT,
		SUM(IFNULL(DROP_PS_CNT,0)) AS DROP_PS_CNT,
		SUM(IFNULL(FP_RSCP_1,0)) AS FP_RSCP_1,
		SUM(IFNULL(FP_ECN0_1,0)) AS FP_ECN0_1,
		SUM(IFNULL(BEST_CNT,0)) AS BEST_CNT,
		MAX(IFNULL(UL_THROUPUT_MAX,0)) AS UL_THROUPUT_MAX,
		MAX(IFNULL(DL_THROUPUT_MAX,0)) AS DL_THROUPUT_MAX,
		MAX(IFNULL(CS_RRC_DURATION,0)) AS CS_RRC_DURATION,
		MAX(IFNULL(PS_RRC_DURATION,0)) AS PS_RRC_DURATION,
		SUM(IFNULL(CAUSE_14_CNT,0)) AS CAUSE_14_CNT,
		SUM(IFNULL(CAUSE_15_CNT,0)) AS CAUSE_15_CNT,
		SUM(IFNULL(CAUSE_46_CNT,0)) AS CAUSE_46_CNT,
		SUM(IFNULL(CAUSE_115_CNT,0)) AS CAUSE_115_CNT,
		SUM(IFNULL(CAUSE_OTHERS_CNT,0)) AS CAUSE_OTHERS_CNT,
		SUM(IFNULL(CAUSE_53_CNT,0)) AS CAUSE_53_CNT,
		SUM(IFNULL(CAUSE_65_CNT,0)) AS CAUSE_65_CNT,
		SUM(IFNULL(CAUSE_114_CNT,0)) AS CAUSE_114_CNT,
		SUM(IFNULL(CAUSE_263_CNT,0)) AS CAUSE_263_CNT,
		SUM(IFNULL(CAUSE_CAPACITY,0)) AS CAUSE_CAPACITY,
		SUM(IFNULL(`RSCP_THRESHOLD_1_CNT`,0)) AS RSCP_THRESHOLD_1_CNT,
		SUM(IFNULL(`RSCP_THRESHOLD_2_CNT`,0)) AS RSCP_THRESHOLD_2_CNT,
		SUM(IFNULL(`RSCP_THRESHOLD_3_CNT`,0)) AS RSCP_THRESHOLD_3_CNT,
		SUM(IFNULL(`ECN0_THRESHOLD_1_CNT`,0)) AS ECN0_THRESHOLD_1_CNT,
		SUM(IFNULL(`ECN0_THRESHOLD_2_CNT`,0)) AS ECN0_THRESHOLD_2_CNT,
		SUM(IFNULL(`ECN0_THRESHOLD_3_CNT`,0)) AS ECN0_THRESHOLD_3_CNT,
		SUM(IFNULL(`DL_TPT_THRESHOLD_SUM`,0)) AS DL_TPT_THRESHOLD_SUM,
		SUM(IFNULL(`DL_TPT_THRESHOLD_CNT`,0)) AS DL_TPT_THRESHOLD_CNT,
		SUM(IFNULL(`UL_TPT_THRESHOLD_SUM`,0)) AS UL_TPT_THRESHOLD_SUM,
		SUM(IFNULL(`UL_TPT_THRESHOLD_CNT`,0)) AS UL_TPT_THRESHOLD_CNT,
		SUM(IFNULL(`DL_VOLUME_THRESHOLD`,0)) AS DL_VOLUME_THRESHOLD,
		SUM(IFNULL(`UL_VOLUME_THRESHOLD`,0)) AS UL_VOLUME_THRESHOLD,
		SUM(IFNULL(BLOCK_VOICE_CNT,0)) AS BLOCK_VOICE_CNT,
		SUM(IFNULL(BLOCK_VIDEO_CNT,0)) AS BLOCK_VIDEO_CNT,
		SUM(IFNULL(BLOCK_PS_R99_CNT,0)) AS BLOCK_PS_R99_CNT,
		SUM(IFNULL(BLOCK_PS_HSPA_CNT,0)) AS BLOCK_PS_HSPA_CNT,
		SUM(IFNULL(BLOCK_M_RAB_CNT,0)) AS BLOCK_M_RAB_CNT,
		SUM(IFNULL(BLOCK_SIGNAL_CNT,0)) AS BLOCK_SIGNAL_CNT,
		SUM(IFNULL(BLOCK_SMS_CNT,0)) AS BLOCK_SMS_CNT,
		SUM(IFNULL(BLOCK_PS_OTHER_CNT,0)) AS BLOCK_PS_OTHER_CNT';
	
	INSERT INTO gt_gw_main.sp_log VALUES('gt_gw_main','SP_Generate_Global_Statistic_tile_umts',CONCAT(GT_DB,' CREATE TEMPORARY TABLE tmp_materialization_',WORKER_ID), NOW());	
	
	SET @ZOOM_LEVEL1 = gt_covmo_csv_get(TileResolution,1);
	SET @ZOOM_LEVEL2 = gt_covmo_csv_get(TileResolution,2);
	SET @ZOOM_LEVEL3 = gt_covmo_csv_get(TileResolution,3);
	
	SET @TILE_ID_LVL1 = CONCAT('TILE_ID_',gt_covmo_csv_get(TileResolution,1));
	SET @TILE_ID_LVL2 = CONCAT('TILE_ID_',gt_covmo_csv_get(TileResolution,2));
	
	SET @SqlCmd=CONCAT('SELECT COUNT(*) INTO @V_EXIST_Rscp_threshold_1 FROM ',NT_DB,'.`sys_config`
				WHERE att_name=''Rscp_threshold_1'';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	IF @V_EXIST_Rscp_threshold_1 >0 THEN 
		SET @SqlCmd=CONCAT('SELECT `att_value` INTO @Rscp_threshold_1 FROM ',NT_DB,'.`sys_config` WHERE att_name=''Rscp_threshold_1'';');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt; 
	ELSE 
		SET @Rscp_threshold_1=-100;
	END IF;
	
	SET @SqlCmd=CONCAT('SELECT COUNT(*) INTO @V_EXIST_Rscp_threshold_2 FROM ',NT_DB,'.`sys_config`
				WHERE att_name=''Rscp_threshold_2'';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	IF @V_EXIST_Rscp_threshold_2 >0 THEN 
		SET @SqlCmd=CONCAT('SELECT `att_value` INTO @Rscp_threshold_2 FROM ',NT_DB,'.`sys_config` WHERE att_name=''Rscp_threshold_2'';');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt; 
	ELSE 
		SET @Rscp_threshold_2=-110;
	END IF;
	
	SET @SqlCmd=CONCAT('SELECT COUNT(*) INTO @V_EXIST_Ecn0_threshold_1 FROM ',NT_DB,'.`sys_config`
				WHERE att_name=''Ecn0_threshold_1'';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	IF @V_EXIST_Ecn0_threshold_1 >0 THEN 
		SET @SqlCmd=CONCAT('SELECT `att_value` INTO @Ecn0_threshold_1 FROM ',NT_DB,'.`sys_config` WHERE att_name=''Ecn0_threshold_1'';');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt; 
	ELSE 
		SET @Ecn0_threshold_1=-10;
	END IF;
	
	SET @SqlCmd=CONCAT('SELECT COUNT(*) INTO @V_EXIST_Ecn0_threshold_2 FROM ',NT_DB,'.`sys_config`
				WHERE att_name=''Ecn0_threshold_2'';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	IF @V_EXIST_Ecn0_threshold_2 >0 THEN 
		SET @SqlCmd=CONCAT('SELECT `att_value` INTO @Ecn0_threshold_2 FROM ',NT_DB,'.`sys_config` WHERE att_name=''Ecn0_threshold_2'';');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt; 
	ELSE 
		SET @Ecn0_threshold_2=-14;
	END IF;
	
	SET @SqlCmd=CONCAT('SELECT COUNT(*) INTO @V_EXIST_Dl_vol_threshold_3G FROM ',NT_DB,'.`sys_config`
				WHERE att_name=''Dl_vol_threshold_3G'';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	IF @V_EXIST_Dl_vol_threshold_3G >0 THEN 
		SET @SqlCmd=CONCAT('SELECT `att_value` INTO @Dl_vol_threshold_3G FROM ',NT_DB,'.`sys_config` WHERE att_name=''Dl_vol_threshold_3G'';');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt; 
	ELSE 
		SET @Dl_vol_threshold_3G=0;
	END IF;
	
	SET @SqlCmd=CONCAT('SELECT COUNT(*) INTO @V_EXIST_Ul_vol_threshold_3G FROM ',NT_DB,'.`sys_config`
				WHERE att_name=''Ul_vol_threshold_3G'';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	IF @V_EXIST_Ul_vol_threshold_3G >0 THEN 
		SET @SqlCmd=CONCAT('SELECT `att_value` INTO @Ul_vol_threshold_3G FROM ',NT_DB,'.`sys_config` WHERE att_name=''Ul_vol_threshold_3G'';');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt; 
	ELSE 
		SET @Ul_vol_threshold_3G=0;
	END IF;
	
	SET SESSION group_concat_max_len=102400; 
	
	SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS ',GT_DB,'.tmp_materialization_',WORKER_ID,';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('CREATE TEMPORARY TABLE ',GT_DB,'.tmp_materialization_',WORKER_ID,' (
				  `TILE_ID` BIGINT(20) NOT NULL,
				  `FREQUENCY` SMALLINT(6) NOT NULL,
				  `UARFCN` SMALLINT(6) DEFAULT NULL,
				  `DATA_DATE` DATETIME NOT NULL,
				  `DATA_HOUR` TINYINT(2) NOT NULL,
				  `',@TILE_ID_LVL2,'` BIGINT(20) DEFAULT NULL,
				  `',@TILE_ID_LVL1,'` BIGINT(20) DEFAULT NULL,
				  `INIT_CALL_CNT` INT(11) DEFAULT NULL,
				  `END_CALL_CNT` INT(11) DEFAULT NULL,
				  `VOICE_CNT` INT(11) DEFAULT NULL,
				  `VIDEO_CNT` INT(11) DEFAULT NULL,
				  `PS_R99_CNT` INT(11) DEFAULT NULL,
				  `PS_HSPA_CNT` INT(11) DEFAULT NULL,
				  `M_RAB_CNT` INT(11) DEFAULT NULL, 
				  `SIGNAL_CNT` INT(11) DEFAULT NULL,
				  `SMS_CNT` INT(11) DEFAULT NULL,
				  `PS_OTHER_CNT` INT(11) DEFAULT NULL,
				  `CALL_DUR_SUM` DOUBLE DEFAULT NULL,
				  `CS_DUR_SUM` DOUBLE DEFAULT NULL,
				  `DROP_CNT` INT(11) DEFAULT NULL,
				  `BLOCK_CNT` INT(11) DEFAULT NULL,
				  `DROP_VOICE_CNT` INT(11) DEFAULT NULL,
				  `DROP_VIDEO_CNT` INT(11) DEFAULT NULL,
				  `DROP_PS_R99_CNT` INT(11) DEFAULT NULL,
				  `DROP_PS_HSPA_CNT` INT(11) DEFAULT NULL,
				  `DROP_M_RAB_CNT` INT(11) DEFAULT NULL,
				  `DROP_SIGNAL_CNT` INT(11) DEFAULT NULL,
				  `DROP_SMS_CNT` INT(11) DEFAULT NULL,
				  `DROP_PS_OTHER_CNT` INT(11) DEFAULT NULL,
				  `SHO_ATTEMPT_CNT` INT(11) DEFAULT NULL,
				  `SHO_FAILURE_CNT` INT(11) DEFAULT NULL,
				  `IFHO_ATTEMPT_CNT` INT(11) DEFAULT NULL,
				  `IFHO_FAILURE_CNT` INT(11) DEFAULT NULL,
				  `IRAT_ATTEMPT_CNT` INT(11) DEFAULT NULL,
				  `IRAT_FAILURE_CNT` INT(11) DEFAULT NULL,
				  `PS_UL_VOLUME_SUM` DOUBLE DEFAULT NULL,
				  `PS_DL_VOLUME_SUM` DOUBLE DEFAULT NULL,
				  `PS_UL_SPEED_MAX` DOUBLE DEFAULT NULL,
				  `PS_DL_SPEED_MAX` DOUBLE DEFAULT NULL,
				  `RSCP_SUM` DOUBLE DEFAULT NULL,
				  `RSCP_CNT` INT(11) DEFAULT NULL,
				  `ECNO_SUM` DOUBLE DEFAULT NULL,
				  `ECNO_CNT` INT(11) DEFAULT NULL,
				  `ACTIVE_SET_SUM` DOUBLE DEFAULT NULL,
				  `ACTIVE_SET_CNT` INT(11) DEFAULT NULL,
				  `POLLUTED_PILOT_CNT` INT(11) DEFAULT NULL,
				  `PILOT_DOM_SUM` DOUBLE DEFAULT NULL,
				  `PILOT_CNT` INT(11) DEFAULT NULL,
				  `T19_CNT` INT(11) DEFAULT NULL,
				  `UL_THROUPUT_SUM` DOUBLE DEFAULT NULL,
				  `UL_THROUPUT_CNT` INT(11) DEFAULT NULL,
				  `DL_THROUPUT_SUM` DOUBLE DEFAULT NULL,
				  `DL_THROUPUT_CNT` INT(11) DEFAULT NULL,
				  `NON_BLOCK_VOICE_CNT` INT(11) DEFAULT NULL,
				  `NON_BLOCK_VIDEO_CNT` INT(11) DEFAULT NULL,
				  `NON_BLOCK_PS_R99_CNT` INT(11) DEFAULT NULL,
				  `NON_BLOCK_PS_HSPA_CNT` INT(11) DEFAULT NULL,
				  `NON_BLOCK_M_RAB_CNT` INT(11) DEFAULT NULL,
				  `NON_BLOCK_SIGNAL_CNT` INT(11) DEFAULT NULL,
				  `NON_BLOCK_SMS_CNT` INT(11) DEFAULT NULL,
				  `NON_BLOCK_PS_OTHER_CNT` INT(11) DEFAULT NULL,
				  `PS_CNT` INT(11) DEFAULT NULL,
				  `DROP_PS_CNT` INT(11) DEFAULT NULL,
				  `FP_RSCP_1` DOUBLE DEFAULT NULL,
				  `FP_ECN0_1` DOUBLE DEFAULT NULL,
				  `BEST_CNT` INT(11) DEFAULT NULL,
				  `UL_THROUPUT_MAX` DOUBLE DEFAULT NULL,
				  `DL_THROUPUT_MAX` DOUBLE DEFAULT NULL,
				  `CS_RRC_DURATION` DOUBLE DEFAULT NULL,
				  `PS_RRC_DURATION` DOUBLE DEFAULT NULL,
				  `CAUSE_14_CNT` SMALLINT(6) DEFAULT NULL,
				  `CAUSE_15_CNT` SMALLINT(6) DEFAULT NULL,
				  `CAUSE_46_CNT` SMALLINT(6) DEFAULT NULL,
				  `CAUSE_115_CNT` SMALLINT(6) DEFAULT NULL,
				  `CAUSE_OTHERS_CNT` SMALLINT(6) DEFAULT NULL,
				  `CAUSE_53_CNT` SMALLINT(6) DEFAULT NULL,
				  `CAUSE_65_CNT` SMALLINT(6) DEFAULT NULL,
				  `CAUSE_114_CNT` SMALLINT(6) DEFAULT NULL,
				  `CAUSE_263_CNT` SMALLINT(6) DEFAULT NULL,
				  `CAUSE_CAPACITY` SMALLINT(6) DEFAULT NULL,
				  `RSCP_THRESHOLD_1_CNT` int(11) DEFAULT NULL,
				  `RSCP_THRESHOLD_2_CNT` int(11) DEFAULT NULL,
				  `RSCP_THRESHOLD_3_CNT` int(11) DEFAULT NULL,
				  `ECN0_THRESHOLD_1_CNT` int(11) DEFAULT NULL,
				  `ECN0_THRESHOLD_2_CNT` int(11) DEFAULT NULL,
				  `ECN0_THRESHOLD_3_CNT` int(11) DEFAULT NULL,
				  `DL_TPT_THRESHOLD_SUM` DOUBLE DEFAULT NULL,
				  `DL_TPT_THRESHOLD_CNT` int(11) DEFAULT NULL,
				  `UL_TPT_THRESHOLD_SUM` DOUBLE DEFAULT NULL,
				  `UL_TPT_THRESHOLD_CNT` int(11) DEFAULT NULL,
				  `DL_VOLUME_THRESHOLD` DOUBLE DEFAULT NULL,
				  `UL_VOLUME_THRESHOLD` DOUBLE DEFAULT NULL,
				  `BLOCK_VOICE_CNT` INT(11) DEFAULT NULL,
				  `BLOCK_VIDEO_CNT` INT(11) DEFAULT NULL,
				  `BLOCK_PS_R99_CNT` INT(11) DEFAULT NULL,
				  `BLOCK_PS_HSPA_CNT` INT(11) DEFAULT NULL,
				  `BLOCK_M_RAB_CNT` INT(11) DEFAULT NULL,
				  `BLOCK_SIGNAL_CNT` INT(11) DEFAULT NULL,
				  `BLOCK_SMS_CNT` INT(11) DEFAULT NULL,
				  `BLOCK_PS_OTHER_CNT` INT(11) DEFAULT NULL,
				  `PU_ID` MEDIUMINT(9) DEFAULT NULL,
				  `REG_1_ID` BIGINT(20) DEFAULT NULL,
				  `REG_2_ID` BIGINT(20) DEFAULT NULL,
				  `REG_3_ID` BIGINT(20) DEFAULT NULL
			--	  PRIMARY KEY (`TILE_ID`,FREQUENCY,`UARFCN`,`DATA_DATE`,`DATA_HOUR`)
				) ENGINE=MYISAM DEFAULT CHARSET=latin1;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	INSERT INTO gt_gw_main.sp_log VALUES('gt_gw_main','SP_Generate_Global_Statistic_tile_umts',CONCAT(GT_DB,' INSERT INTO tbl_',GT_DB,'_',WORKER_ID), NOW());	
	 
	
	SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS ',GT_DB,'.tmp_tile_start_',WORKER_ID,';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS ',GT_DB,'.tmp_tile_end_',WORKER_ID,';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS ',GT_DB,'.tmp_tile_fp_',WORKER_ID,';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('CREATE TEMPORARY TABLE ',GT_DB,'.tmp_tile_start_',WORKER_ID,' ENGINE=MYISAM AS
				SELECT
					gt_geohash_ext(POS_FIRST_LOC,',@ZOOM_LEVEL3,') AS TILE_ID
 					,gt_geohash_ext(POS_FIRST_LOC,',@ZOOM_LEVEL2,') AS ',@TILE_ID_LVL2,'
 					,gt_geohash_ext(POS_FIRST_LOC,',@ZOOM_LEVEL1,') AS ',@TILE_ID_LVL1,'
 					,POS_FIRST_FREQUENCY AS FREQUENCY
 					,POS_FIRST_UARFCN AS UARFCN
					,DATA_DATE
					,DATA_HOUR
					,COUNT(*) AS `INIT_CALL_CNT`
					,NULL AS `END_CALL_CNT`
					,SUM(IF(CALL_TYPE=10,1,0)) AS VOICE_CNT
					,SUM(IF(CALL_TYPE=11,1,0)) AS VIDEO_CNT
					,SUM(IF(CALL_TYPE=12,1,0)) AS PS_R99_CNT
					,SUM(IF(CALL_TYPE=13,1,0)) AS PS_HSPA_CNT
					,SUM(IF(CALL_TYPE=14,1,0)) AS M_RAB_CNT
					,SUM(IF(CALL_TYPE=15,1,0)) AS SIGNAL_CNT
					,SUM(IF(CALL_TYPE=16,1,0)) AS SMS_CNT
					,SUM(IF(CALL_TYPE=18,1,0)) AS PS_OTHER_CNT
					,SUM(RRC_CONNECT_DURATION/1000) AS CALL_DUR_SUM
					,SUM(CS_CALL_DURA/1000) AS CS_DUR_SUM
					,NULL AS `DROP_CNT`
					,SUM(IF(CALL_STATUS=3,1,0)) AS BLOCK_CNT
					,NULL AS `DROP_VOICE_CNT`
					,NULL AS `DROP_VIDEO_CNT`
					,NULL AS `DROP_PS_R99_CNT`
					,NULL AS `DROP_PS_HSPA_CNT`
					,NULL AS `DROP_M_RAB_CNT`
					,NULL AS `DROP_SIGNAL_CNT`
					,NULL AS `DROP_SMS_CNT`
					,NULL AS `DROP_PS_OTHER_CNT`
					,SUM(SHO) AS SHO_ATTEMPT_CNT
					,0 AS SHO_FAILURE_CNT
					,NULL AS `IFHO_ATTEMPT_CNT`
					,NULL AS `IFHO_FAILURE_CNT`
					,NULL AS `IRAT_ATTEMPT_CNT`
					,NULL AS `IRAT_FAILURE_CNT`
					,NULL AS `PS_UL_VOLUME_SUM`
					,NULL AS `PS_DL_VOLUME_SUM`
					,NULL AS `PS_UL_SPEED_MAX`
					,NULL AS `PS_DL_SPEED_MAX`
					,SUM(POS_FIRST_RSCP) AS RSCP_SUM
					,COUNT(POS_FIRST_RSCP) AS RSCP_CNT
					,SUM(POS_FIRST_ECN0) AS ECNO_SUM
					,COUNT(POS_FIRST_ECN0) AS ECNO_CNT
					,NULL AS `ACTIVE_SET_SUM`
					,NULL AS `ACTIVE_SET_CNT`
					,NULL AS `POLLUTED_PILOT_CNT`
					,NULL AS `PILOT_DOM_SUM`
					,NULL AS `PILOT_CNT`
					,NULL AS `T19_CNT`
					,NULL AS `UL_THROUPUT_SUM`
					,NULL AS `UL_THROUPUT_CNT`
					,NULL AS `DL_THROUPUT_SUM`
					,NULL AS `DL_THROUPUT_CNT`
					,SUM(IF(CALL_STATUS IN (1,2,4,5) AND CALL_TYPE=10,1,0)) AS NON_BLOCK_VOICE_CNT
					,SUM(IF(CALL_STATUS IN (1,2,4,5) AND CALL_TYPE=11,1,0)) AS NON_BLOCK_VIDEO_CNT
					,SUM(IF(CALL_STATUS IN (1,2,4,5) AND CALL_TYPE=12,1,0)) AS NON_BLOCK_PS_R99_CNT
					,SUM(IF(CALL_STATUS IN (1,2,4,5) AND CALL_TYPE=13,1,0)) AS NON_BLOCK_PS_HSPA_CNT
					,SUM(IF(CALL_STATUS IN (1,2,4,5) AND CALL_TYPE=14,1,0)) AS NON_BLOCK_M_RAB_CNT
					,SUM(IF(CALL_STATUS IN (1,2,4,5) AND CALL_TYPE=15,1,0)) AS NON_BLOCK_SIGNAL_CNT
					,SUM(IF(CALL_STATUS IN (1,2,4,5) AND CALL_TYPE=16,1,0)) AS NON_BLOCK_SMS_CNT
					,SUM(IF(CALL_STATUS IN (1,2,4,5) AND CALL_TYPE=18,1,0)) AS NON_BLOCK_PS_OTHER_CNT
					,SUM(IF(CALL_TYPE IN (12,13,14),1,0)) AS PS_CNT
					,NULL AS `DROP_PS_CNT`
					,NULL AS `FP_RSCP_1`
					,NULL AS `FP_ECN0_1`
					,NULL AS `BEST_CNT`
					,NULL AS `UL_THROUPUT_MAX`
					,NULL AS `DL_THROUPUT_MAX`
					,SUM(IF(CALL_TYPE IN (10,11), RRC_CONNECT_DURATION/1000, 0)) AS CS_RRC_DURATION
					,SUM(IF(CALL_TYPE IN (12,13,14,18), RRC_CONNECT_DURATION/1000, 0)) AS PS_RRC_DURATION
					,NULL AS `CAUSE_14_CNT`
					,NULL AS `CAUSE_15_CNT`
					,NULL AS `CAUSE_46_CNT`
					,NULL AS `CAUSE_115_CNT`
					,NULL AS `CAUSE_OTHERS_CNT`
					,NULL AS `CAUSE_53_CNT`
					,NULL AS `CAUSE_65_CNT`
					,NULL AS `CAUSE_114_CNT`
					,NULL AS `CAUSE_263_CNT`
					,NULL AS `CAUSE_CAPACITY`
					
					,SUM(IF (POS_FIRST_RSCP > ',@Rscp_threshold_1,' , 1,0)) AS `RSCP_THRESHOLD_1_CNT`
					,SUM(IF (POS_FIRST_RSCP <=',@Rscp_threshold_1,' AND POS_FIRST_RSCP >',@Rscp_threshold_2,', 1,0)) AS `RSCP_THRESHOLD_2_CNT`
					,SUM(IF (POS_FIRST_RSCP <= ',@Rscp_threshold_2,' , 1,0)) AS `RSCP_THRESHOLD_3_CNT`
	
					,SUM(IF (POS_FIRST_ECN0 > ',@Ecn0_threshold_1,' , 1,0))AS `ECN0_THRESHOLD_1_CNT`
					,SUM(IF (POS_FIRST_ECN0 <= ',@Ecn0_threshold_1,' AND POS_FIRST_ECN0 >',@Ecn0_threshold_2,', 1,0)) AS `ECN0_THRESHOLD_2_CNT`
					,SUM(IF (POS_FIRST_ECN0 <= ',@Ecn0_threshold_2,' , 1,0)) AS `ECN0_THRESHOLD_3_CNT`
					
					,SUM(IF (DL_TRAFFIC_VOLUME > ',@Dl_vol_threshold_3G,' , DL_THROUGHPUT_AVG,0)) AS `DL_TPT_THRESHOLD_SUM`
					,SUM(IF (IFNULL(0,DL_TRAFFIC_VOLUME) > ',@Dl_vol_threshold_3G,' , 1,0)) AS `DL_TPT_THRESHOLD_CNT`
					,SUM(IF (UL_TRAFFIC_VOLUME > ',@UL_vol_threshold_3G,' , UL_THROUGHPUT_AVG, 0)) AS `UL_TPT_THRESHOLD_SUM`
					,SUM(IF (IFNULL(0,UL_TRAFFIC_VOLUME) > ',@UL_vol_threshold_3G,' , 1,0)) AS `UL_TPT_THRESHOLD_CNT`
					,SUM(IF (DL_TRAFFIC_VOLUME > ',@Dl_vol_threshold_3G,' , DL_TRAFFIC_VOLUME,0)) AS `DL_VOLUME_THRESHOLD`
					,SUM(IF (UL_TRAFFIC_VOLUME > ',@UL_vol_threshold_3G,' , UL_TRAFFIC_VOLUME,0)) AS `UL_VOLUME_THRESHOLD`
					,SUM(IF(CALL_STATUS=3 AND CALL_TYPE=10,1,0)) AS BLOCK_VOICE_CNT
					,SUM(IF(CALL_STATUS=3 AND CALL_TYPE=11,1,0)) AS BLOCK_VIDEO_CNT
					,SUM(IF(CALL_STATUS=3 AND CALL_TYPE=12,1,0)) AS BLOCK_PS_R99_CNT
					,SUM(IF(CALL_STATUS=3 AND CALL_TYPE=13,1,0)) AS BLOCK_PS_HSPA_CNT
					,SUM(IF(CALL_STATUS=3 AND CALL_TYPE=14,1,0)) AS BLOCK_M_RAB_CNT
					,SUM(IF(CALL_STATUS=3 AND CALL_TYPE=15,1,0)) AS BLOCK_SIGNAL_CNT
					,SUM(IF(CALL_STATUS=3 AND CALL_TYPE=16,1,0)) AS BLOCK_SMS_CNT
					,SUM(IF(CALL_STATUS=3 AND CALL_TYPE=18,1,0)) AS BLOCK_PS_OTHER_CNT
					,',PU_ID,' AS `PU_ID`
				FROM ',GT_DB,'.table_call
				WHERE DATA_HOUR =',DATA_HOUR,' AND POS_FIRST_LOC IS NOT NULL 
-- 				AND POS_FIRST_RNC=',PU_ID,' 
				GROUP BY gt_geohash_ext(POS_FIRST_LOC,',@ZOOM_LEVEL3,'),POS_FIRST_FREQUENCY,POS_FIRST_UARFCN
				ORDER BY NULL;
				');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('CREATE TEMPORARY TABLE ',GT_DB,'.tmp_tile_end_',WORKER_ID,' ENGINE=MYISAM AS
				SELECT
					gt_geohash_ext(POS_LAST_LOC,',@ZOOM_LEVEL3,') AS TILE_ID
 					,gt_geohash_ext(POS_LAST_LOC,',@ZOOM_LEVEL2,') AS ',@TILE_ID_LVL2,'
 					,gt_geohash_ext(POS_LAST_LOC,',@ZOOM_LEVEL1,') AS ',@TILE_ID_LVL1,'
 					,POS_LAST_FREQUENCY AS FREQUENCY
 					,POS_LAST_UARFCN AS UARFCN
					,DATA_DATE
					,DATA_HOUR
					,NULL AS `INIT_CALL_CNT`
					,COUNT(*) AS `END_CALL_CNT`
					,NULL AS `VOICE_CNT`
					,NULL AS `VIDEO_CNT`
					,NULL AS `PS_R99_CNT`
					,NULL AS `PS_HSPA_CNT`
					,NULL AS `M_RAB_CNT`
					,NULL AS `SIGNAL_CNT`
					,NULL AS `SMS_CNT`
					,NULL AS `PS_OTHER_CNT`
					,NULL AS `CALL_DUR_SUM`
					,NULL AS `CS_DUR_SUM`
					,SUM(IF(CALL_STATUS=2,1,0)) `DROP_CNT`
					,NULL AS `BLOCK_CNT`
					,SUM(IF(CALL_STATUS=2 AND CALL_TYPE=10,1,0)) AS DROP_VOICE_CNT
					,SUM(IF(CALL_STATUS=2 AND CALL_TYPE=11,1,0)) AS DROP_VIDEO_CNT
					,SUM(IF(CALL_STATUS=2 AND CALL_TYPE=12,1,0)) AS DROP_PS_R99_CNT
					,SUM(IF(CALL_STATUS=2 AND CALL_TYPE=13,1,0)) AS DROP_PS_HSPA_CNT
					,SUM(IF(CALL_STATUS=2 AND CALL_TYPE=14,1,0)) AS DROP_M_RAB_CNT
					,SUM(IF(CALL_STATUS=2 AND CALL_TYPE=15,1,0)) AS DROP_SIGNAL_CNT
					,SUM(IF(CALL_STATUS=2 AND CALL_TYPE=16,1,0)) AS DROP_SMS_CNT
					,SUM(IF(CALL_STATUS=2 AND CALL_TYPE=18,1,0)) AS DROP_PS_OTHER_CNT
					,NULL AS `SHO_ATTEMPT_CNT`
					,NULL AS `SHO_FAILURE_CNT`
					,NULL AS `IFHO_ATTEMPT_CNT`
					,NULL AS `IFHO_FAILURE_CNT`
					,IFNULL(SUM(IRAT_HHO_ATTEMPT),0) AS IRAT_ATTEMPT_CNT
					,IFNULL(SUM(IFNULL(`IRAT_HHO_ATTEMPT`,0)-IFNULL(`IRAT_HHO_SUCCESS`,0)),0) AS IRAT_FAILURE_CNT
					,NULL AS `PS_UL_VOLUME_SUM`
					,NULL AS `PS_DL_VOLUME_SUM`
					,NULL AS `PS_UL_SPEED_MAX`
					,NULL AS `PS_DL_SPEED_MAX`
					,NULL AS `RSCP_SUM`
					,NULL AS `RSCP_CNT`
					,NULL AS `ECNO_SUM`
					,NULL AS `ECNO_CNT`
					,NULL AS `ACTIVE_SET_SUM`
					,NULL AS `ACTIVE_SET_CNT`
					,NULL AS `POLLUTED_PILOT_CNT`
					,NULL AS `PILOT_DOM_SUM`
					,NULL AS `PILOT_CNT`
					,NULL AS `T19_CNT`
					,NULL AS `UL_THROUPUT_SUM`
					,NULL AS `UL_THROUPUT_CNT`
					,NULL AS `DL_THROUPUT_SUM`
					,NULL AS `DL_THROUPUT_CNT`
					,NULL AS `NON_BLOCK_VOICE_CNT`
					,NULL AS `NON_BLOCK_VIDEO_CNT`
					,NULL AS `NON_BLOCK_PS_R99_CNT`
					,NULL AS `NON_BLOCK_PS_HSPA_CNT`
					,NULL `NON_BLOCK_M_RAB_CNT`
					,NULL `NON_BLOCK_SIGNAL_CNT`
					,NULL `NON_BLOCK_SMS_CNT`
					,NULL `NON_BLOCK_PS_OTHER_CNT`
					,NULL AS `PS_CNT`
					,SUM(IF(CALL_STATUS=2 AND CALL_TYPE IN(12,13,14),1,0)) AS DROP_PS_CNT
					,NULL AS `FP_RSCP_1`
					,NULL AS `FP_ECN0_1`
					,NULL AS `BEST_CNT`
					,NULL AS `UL_THROUPUT_MAX`
					,NULL AS `DL_THROUPUT_MAX`
					,NULL AS `CS_RRC_DURATION`
					,NULL AS `PS_RRC_DURATION`
					,SUM(IF(IU_RELEASE_CAUSE=14 AND CALL_STATUS=2,1,0)) AS CAUSE_14_CNT
					,SUM(IF(IU_RELEASE_CAUSE=15 AND CALL_STATUS=2,1,0)) AS CAUSE_15_CNT
					,SUM(IF(IU_RELEASE_CAUSE=46 AND CALL_STATUS=2,1,0)) AS CAUSE_46_CNT
					,SUM(IF(IU_RELEASE_CAUSE=115 AND CALL_STATUS=2,1,0)) AS CAUSE_115_CNT
					,SUM(IF(IU_RELEASE_CAUSE NOT IN (14,15,46,115) AND CALL_STATUS=2,1,0)) AS CAUSE_OTHERS_CNT
					,SUM(IF(IU_RELEASE_CAUSE=53 AND CALL_STATUS=2,1,0)) AS CAUSE_53_CNT
					,SUM(IF(IU_RELEASE_CAUSE=65 AND CALL_STATUS=2,1,0)) AS CAUSE_65_CNT
					,SUM(IF(IU_RELEASE_CAUSE=114 AND CALL_STATUS=2,1,0)) AS CAUSE_114_CNT
					,SUM(IF(IU_RELEASE_CAUSE=263 AND CALL_STATUS=2,1,0)) AS CAUSE_263_CNT
					,SUM(IF(IU_RELEASE_CAUSE IN (53,65,114,263) AND CALL_STATUS=2,1,0)) AS CAUSE_CAPACITY
					,NULL AS `RSCP_THRESHOLD_1_CNT`
					,NULL AS `RSCP_THRESHOLD_2_CNT`
					,NULL AS `RSCP_THRESHOLD_3_CNT`
					,NULL AS `ECN0_THRESHOLD_1_CNT`
					,NULL AS `ECN0_THRESHOLD_2_CNT`
					,NULL AS `ECN0_THRESHOLD_3_CNT`
					,NULL AS `DL_TPT_THRESHOLD_SUM`
					,NULL AS `DL_TPT_THRESHOLD_CNT`
					,NULL AS `UL_TPT_THRESHOLD_SUM`
					,NULL AS `UL_TPT_THRESHOLD_CNT`
					,NULL AS `DL_VOLUME_THRESHOLD`
					,NULL AS `UL_VOLUME_THRESHOLD`
					,NULL AS BLOCK_VOICE_CNT
					,NULL AS BLOCK_VIDEO_CNT
					,NULL AS BLOCK_PS_R99_CNT
					,NULL AS BLOCK_PS_HSPA_CNT
					,NULL AS BLOCK_M_RAB_CNT
					,NULL AS BLOCK_SIGNAL_CNT
					,NULL AS BLOCK_SMS_CNT
					,NULL AS BLOCK_PS_OTHER_CNT
					,',PU_ID,' AS `PU_ID`
					
				FROM ',GT_DB,'.table_call
				WHERE DATA_HOUR =',DATA_HOUR,' AND POS_LAST_LOC IS NOT NULL 
-- 				AND POS_LAST_RNC=',PU_ID,' 
 				GROUP BY gt_geohash_ext(POS_LAST_LOC,',@ZOOM_LEVEL3,'),POS_LAST_FREQUENCY,POS_LAST_UARFCN
				ORDER BY NULL;
				');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('CREATE TEMPORARY TABLE ',GT_DB,'.tmp_tile_fp_',WORKER_ID,' ENGINE=MYISAM AS
				SELECT
					`TILE_ID`
 					,gt_geohash_ext(`TILE_ID`,',@ZOOM_LEVEL2,') AS ',@TILE_ID_LVL2,'
 					,gt_geohash_ext(`TILE_ID`,',@ZOOM_LEVEL1,') AS ',@TILE_ID_LVL1,'
 					,`FREQUENCY`
 					,`UARFCN`
					,`DATA_DATE`
					,`DATA_HOUR`
					,NULL AS `INIT_CALL_CNT`
					,NULL AS `END_CALL_CNT`
					,NULL AS `VOICE_CNT`
					,NULL AS `VIDEO_CNT`
					,NULL AS `PS_R99_CNT`
					,NULL AS `PS_HSPA_CNT`
					,NULL AS `M_RAB_CNT`
					,NULL AS `SIGNAL_CNT`
					,NULL AS `SMS_CNT`
					,NULL AS `PS_OTHER_CNT`
					,NULL AS `CALL_DUR_SUM`
					,NULL AS `CS_DUR_SUM`
					,NULL AS `DROP_CNT`
					,NULL AS `BLOCK_CNT`
					,NULL AS `DROP_VOICE_CNT`
					,NULL AS `DROP_VIDEO_CNT`
					,NULL AS `DROP_PS_R99_CNT`
					,NULL AS `DROP_PS_HSPA_CNT`
					,NULL AS `DROP_M_RAB_CNT`
					,NULL AS `DROP_SIGNAL_CNT`
					,NULL AS `DROP_SMS_CNT`
					,NULL AS `DROP_PS_OTHER_CNT`
					,NULL AS `SHO_ATTEMPT_CNT`
					,NULL AS `SHO_FAILURE_CNT`
					,NULL AS `IFHO_ATTEMPT_CNT`
					,NULL AS `IFHO_FAILURE_CNT`
					,NULL AS `IRAT_ATTEMPT_CNT`
					,NULL AS `IRAT_FAILURE_CNT`
					,IFNULL(SUM(IF(CALL_TYPE IN (12,13,14,18),UL_TRAFFIC_VOLUME,0)),0) AS PS_UL_VOLUME_SUM
					,IFNULL(SUM(IF(CALL_TYPE IN (12,13,14,18),DL_TRAFFIC_VOLUME,0)),0) AS PS_DL_VOLUME_SUM
					,MAX(IF(CALL_TYPE IN (12,13,14,18),(UL_THROUGHPUT_SUM/UL_THROUGHPUT_EVENT_CNT),0)) AS PS_UL_SPEED_MAX
  					,MAX(IF(CALL_TYPE IN (12,13,14,18),(DL_THROUGHPUT_SUM/DL_THROUGHPUT_EVENT_CNT),0)) AS PS_DL_SPEED_MAX
					,NULL AS `RSCP_SUM`
					,NULL AS `RSCP_CNT`
					,NULL AS `ECNO_SUM`
					,NULL AS `ECNO_CNT`
					,SUM(`BEST_CNT`) AS `ACTIVE_SET_SUM`
					,COUNT(`BEST_CNT`) AS `ACTIVE_SET_CNT`
					,SUM(`PP_CNT`) AS `POLLUTED_PILOT_CNT`
					,SUM(`RSCP_GAP`) AS `PILOT_DOM_SUM`
					,SUM(`PILOT_CNT`) AS `PILOT_CNT`
					,NULL AS `T19_CNT`
					,IFNULL(SUM(IF(CALL_TYPE IN (12,13,14,18),UL_THROUGHPUT_SUM,0)),0) AS UL_THROUPUT_SUM
					,IFNULL(SUM(IF(CALL_TYPE IN (12,13,14,18),UL_THROUGHPUT_EVENT_CNT,0)),0) AS UL_THROUPUT_CNT
					,IFNULL(SUM(IF(CALL_TYPE IN (12,13,14,18),DL_THROUGHPUT_SUM,0)),0) AS DL_THROUPUT_SUM
					,IFNULL(SUM(IF(CALL_TYPE IN (12,13,14,18),DL_THROUGHPUT_EVENT_CNT,0)),0) AS DL_THROUPUT_CNT
					,NULL AS `NON_BLOCK_VOICE_CNT`
					,NULL AS `NON_BLOCK_VIDEO_CNT`
					,NULL AS `NON_BLOCK_PS_R99_CNT`
					,NULL AS `NON_BLOCK_PS_HSPA_CNT`
					,NULL AS `NON_BLOCK_M_RAB_CNT`
					,NULL AS `NON_BLOCK_SIGNAL_CNT`
					,NULL AS `NON_BLOCK_SMS_CNT`
					,NULL AS `NON_BLOCK_PS_OTHER_CNT`
					,NULL AS `PS_CNT`
					,NULL AS `DROP_PS_CNT`
					,SUM(`FP_RSCP_1`) AS `FP_RSCP_1`
					,SUM(`FP_ECN0_1`) AS `FP_ECN0_1`
					,SUM(`BEST_CNT`) AS `BEST_CNT`
					,MAX(IF(CALL_TYPE IN (12,13,14,18),(UL_THROUGHPUT_SUM/UL_THROUGHPUT_EVENT_CNT),0)) AS UL_THROUPUT_MAX
					,MAX(IF(CALL_TYPE IN (12,13,14,18),(DL_THROUGHPUT_SUM/DL_THROUGHPUT_EVENT_CNT),0)) AS DL_THROUPUT_MAX
					,NULL AS `CS_RRC_DURATION`
					,NULL AS `PS_RRC_DURATION`
					,NULL AS `CAUSE_14_CNT`
					,NULL AS `CAUSE_15_CNT`
					,NULL AS `CAUSE_46_CNT`
					,NULL AS `CAUSE_115_CNT`
					,NULL AS `CAUSE_OTHERS_CNT`
					,NULL AS `CAUSE_53_CNT`
					,NULL AS `CAUSE_65_CNT`
					,NULL AS `CAUSE_114_CNT`
					,NULL AS `CAUSE_263_CNT`
					,NULL AS `CAUSE_CAPACITY`
					,NULL AS `RSCP_THRESHOLD_1_CNT`
					,NULL AS `RSCP_THRESHOLD_2_CNT`
					,NULL AS `RSCP_THRESHOLD_3_CNT`
					,NULL AS `ECN0_THRESHOLD_1_CNT`
					,NULL AS `ECN0_THRESHOLD_2_CNT`
					,NULL AS `ECN0_THRESHOLD_3_CNT`
					,NULL AS `DL_TPT_THRESHOLD_SUM`
					,NULL AS `DL_TPT_THRESHOLD_CNT`
					,NULL AS `UL_TPT_THRESHOLD_SUM`
					,NULL AS `UL_TPT_THRESHOLD_CNT`
					,NULL AS `DL_VOLUME_THRESHOLD`
					,NULL AS `UL_VOLUME_THRESHOLD`
					,NULL AS BLOCK_VOICE_CNT
					,NULL AS BLOCK_VIDEO_CNT
					,NULL AS BLOCK_PS_R99_CNT
					,NULL AS BLOCK_PS_HSPA_CNT
					,NULL AS BLOCK_M_RAB_CNT
					,NULL AS BLOCK_SIGNAL_CNT
					,NULL AS BLOCK_SMS_CNT
					,NULL AS BLOCK_PS_OTHER_CNT
					,',PU_ID,' AS `PU_ID`
				FROM ',GT_DB,'.`table_tile_fp_t`
				WHERE DATA_HOUR =',DATA_HOUR,' 
-- 				AND RNC_ID=',PU_ID,'  
				GROUP BY `TILE_ID`,FREQUENCY,UARFCN
				HAVING SUM(`BEST_CNT`)>1
				ORDER BY NULL;
				');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	SET @SqlCmd=CONCAT('CREATE INDEX `ix_tile_fq` ON ',GT_DB,'.tmp_tile_start_',WORKER_ID,' (TILE_ID,FREQUENCY,UARFCN);');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('CREATE INDEX `ix_tile_fq` ON ',GT_DB,'.tmp_tile_end_',WORKER_ID,' (TILE_ID,FREQUENCY,UARFCN);');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('CREATE INDEX `ix_tile_fq` ON ',GT_DB,'.tmp_tile_fp_',WORKER_ID,' (TILE_ID,FREQUENCY,UARFCN);');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	
	SET @SqlCmd=CONCAT('INSERT INTO ',GT_DB,'.`tmp_materialization_',WORKER_ID,'` (
				`TILE_ID`,
				`',@TILE_ID_LVL2,'`,
				`',@TILE_ID_LVL1,'`,	
				`FREQUENCY`,
				`UARFCN`,
				`DATA_DATE`,
				`DATA_HOUR`,
				`INIT_CALL_CNT`,
				`END_CALL_CNT`,
				`VOICE_CNT`,
				`VIDEO_CNT`,
				`PS_R99_CNT`,
				`PS_HSPA_CNT`,
				`M_RAB_CNT`,
				`SIGNAL_CNT`,
				`SMS_CNT`,
				`PS_OTHER_CNT`,
				`CALL_DUR_SUM`,
				`CS_DUR_SUM`,
				`DROP_CNT`,
				`BLOCK_CNT`,
				`DROP_VOICE_CNT`,
				`DROP_VIDEO_CNT`,
				`DROP_PS_R99_CNT`,
				`DROP_PS_HSPA_CNT`,
				`DROP_M_RAB_CNT`,
				`DROP_SIGNAL_CNT`,
				`DROP_SMS_CNT`,
				`DROP_PS_OTHER_CNT`,
				`SHO_ATTEMPT_CNT`,
				`SHO_FAILURE_CNT`,
				`IFHO_ATTEMPT_CNT`,
				`IFHO_FAILURE_CNT`,
				`IRAT_ATTEMPT_CNT`,
				`IRAT_FAILURE_CNT`,
				`PS_UL_VOLUME_SUM`,
				`PS_DL_VOLUME_SUM`,
				`PS_UL_SPEED_MAX`,
				`PS_DL_SPEED_MAX`,
				`RSCP_SUM`,
				`RSCP_CNT`,
				`ECNO_SUM`,
				`ECNO_CNT`,
				`ACTIVE_SET_SUM`,
				`ACTIVE_SET_CNT`,
				`POLLUTED_PILOT_CNT`,
				`PILOT_DOM_SUM`,
				`PILOT_CNT`,
				`T19_CNT`,
				`UL_THROUPUT_SUM`,
				`UL_THROUPUT_CNT`,
				`DL_THROUPUT_SUM`,
				`DL_THROUPUT_CNT`,
				`NON_BLOCK_VOICE_CNT`,
				`NON_BLOCK_VIDEO_CNT`,
				`NON_BLOCK_PS_R99_CNT`,
				`NON_BLOCK_PS_HSPA_CNT`,
				`NON_BLOCK_M_RAB_CNT`,
				`NON_BLOCK_SIGNAL_CNT`,
				`NON_BLOCK_SMS_CNT`,
				`NON_BLOCK_PS_OTHER_CNT`,
				`PS_CNT`,
				`DROP_PS_CNT`,
				`FP_RSCP_1`,
				`FP_ECN0_1`,
				`BEST_CNT`,
				`UL_THROUPUT_MAX`,
				`DL_THROUPUT_MAX`,
				`CS_RRC_DURATION`,
				`PS_RRC_DURATION`,
				`CAUSE_14_CNT`,
				`CAUSE_15_CNT`,
				`CAUSE_46_CNT`,
				`CAUSE_115_CNT`,
				`CAUSE_OTHERS_CNT`,
				`CAUSE_53_CNT`,
				`CAUSE_65_CNT`,
				`CAUSE_114_CNT`,
				`CAUSE_263_CNT`,
				`CAUSE_CAPACITY`,
				`RSCP_THRESHOLD_1_CNT`,
				`RSCP_THRESHOLD_2_CNT`,
				`RSCP_THRESHOLD_3_CNT`,
				`ECN0_THRESHOLD_1_CNT`,
				`ECN0_THRESHOLD_2_CNT`,
				`ECN0_THRESHOLD_3_CNT`,
				`DL_TPT_THRESHOLD_SUM`,
				`DL_TPT_THRESHOLD_CNT`,
				`UL_TPT_THRESHOLD_SUM`,
				`UL_TPT_THRESHOLD_CNT`,
				`DL_VOLUME_THRESHOLD`,
				`UL_VOLUME_THRESHOLD`,
				`BLOCK_VOICE_CNT`,
				`BLOCK_VIDEO_CNT`,
				`BLOCK_PS_R99_CNT`,
				`BLOCK_PS_HSPA_CNT`,
				`BLOCK_M_RAB_CNT`,
				`BLOCK_SIGNAL_CNT`,
				`BLOCK_SMS_CNT`,
				`BLOCK_PS_OTHER_CNT`,
				`PU_ID`)
				
				SELECT * FROM ',GT_DB,'.tmp_tile_start_',WORKER_ID,'
				UNION 
				SELECT * FROM ',GT_DB,'.tmp_tile_end_',WORKER_ID,'
				UNION 
				SELECT * FROM ',GT_DB,'.tmp_tile_fp_',WORKER_ID,'
				;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('SELECT 
				`TILE_ID`, 
				`FREQUENCY`,
				  `UARFCN`,
				 `DATA_DATE`,
				  `DATA_HOUR`,
				',@TILE_ID_LVL2,' AS ',@TILE_ID_LVL2,',
				  ',@TILE_ID_LVL1,' AS ',@TILE_ID_LVL1,',
				  ',TILE_UMTS_COLUMN_SUM_STR,' ,
				 `PU_ID`
				FROM ',GT_DB,'.tmp_materialization_',WORKER_ID,' WHERE TILE_ID >0
				GROUP BY `TILE_ID`,FREQUENCY,`UARFCN`,`DATA_DATE`,`DATA_HOUR`;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS ',GT_DB,'.tmp_materialization_',WORKER_ID,';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS ',GT_DB,'.tmp_tile_start_',WORKER_ID,';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS ',GT_DB,'.tmp_tile_end_',WORKER_ID,';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS ',GT_DB,'.tmp_tile_fp_',WORKER_ID,';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	INSERT INTO gt_gw_main.sp_log VALUES('gt_gw_main','SP_Generate_Global_Statistic_tile_umts',CONCAT(GT_DB,' END'), NOW());
	
