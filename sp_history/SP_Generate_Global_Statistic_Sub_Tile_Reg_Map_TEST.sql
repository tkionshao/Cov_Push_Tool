DELIMITER $$
USE `gt_global_statistic`$$
DROP PROCEDURE IF EXISTS `SP_CreateDB_LTE`$$
CREATE DEFINER=`covmo`@`%` PROCEDURE `SP_Generate_Global_Statistic_Sub_Tile_Reg_Map_TEST`(IN TBL_NAME VARCHAR(100),IN WORKER_ID VARCHAR(10),IsIMSI BIT,GROUP_ID TINYINT(4),IN TECH_MASK TINYINT(2),IN RPT_TYPE TINYINT(2),IN DS_FLAG TINYINT(2))
a_label:
BEGIN
	DECLARE START_TIME DATETIME DEFAULT SYSDATE();
	DECLARE STEP_START_TIME DATETIME DEFAULT SYSDATE();
	DECLARE SUB_WORKER_ID VARCHAR(10) DEFAULT CONNECTION_ID();
	
	
	DECLARE TILE_UMTS_COLUMN_STR VARCHAR(2000) DEFAULT 
		' A.`TILE_ID`,
		  A.`FREQUENCY`,
		  A.`UARFCN`,
		  A.`DATA_DATE`,
		  A.`DATA_HOUR`,
		  A.`INIT_CALL_CNT`,
		  A.`END_CALL_CNT`,
		  A.`VOICE_CNT`,
		  A.`VIDEO_CNT`,
		  A.`PS_R99_CNT`,
		  A.`PS_HSPA_CNT`,
		  A.`M_RAB_CNT`,
		  A.`SIGNAL_CNT`,
		  A.`SMS_CNT`,
		  A.`PS_OTHER_CNT`,
		  A.`CALL_DUR_SUM`,
		  A.`CS_DUR_SUM`,
		  A.`DROP_CNT`,
		  A.`BLOCK_CNT`,
		  A.`DROP_VOICE_CNT`,
		  A.`DROP_VIDEO_CNT`,
		  A.`DROP_PS_R99_CNT`,
		  A.`DROP_PS_HSPA_CNT`,
		  A.`DROP_M_RAB_CNT`,
		  A.`DROP_SIGNAL_CNT`,
		  A.`DROP_SMS_CNT`,
		  A.`DROP_PS_OTHER_CNT`,
		  A.`SHO_ATTEMPT_CNT`,
		  A.`SHO_FAILURE_CNT`,
		  A.`IFHO_ATTEMPT_CNT`,
		  A.`IFHO_FAILURE_CNT`,
		  A.`IRAT_ATTEMPT_CNT`,
		  A.`IRAT_FAILURE_CNT`,
		  A.`PS_UL_VOLUME_SUM`,
		  A.`PS_DL_VOLUME_SUM`,
		  A.`PS_UL_SPEED_MAX`,
		  A.`PS_DL_SPEED_MAX`,
		  A.`RSCP_SUM`,
		  A.`RSCP_CNT`,
		  A.`ECNO_SUM`,
		  A.`ECNO_CNT`,
		  A.`ACTIVE_SET_SUM`,
		  A.`ACTIVE_SET_CNT`,
		  A.`POLLUTED_PILOT_CNT`,
		  A.`PILOT_DOM_SUM`,
		  A.`PILOT_CNT`,
		  A.`T19_CNT`,
		  A.`UL_THROUPUT_SUM`,
		  A.`UL_THROUPUT_CNT`,
		  A.`DL_THROUPUT_SUM`,
		  A.`DL_THROUPUT_CNT`,
		  A.`NON_BLOCK_VOICE_CNT`,
		  A.`NON_BLOCK_VIDEO_CNT`,
		  A.`NON_BLOCK_PS_R99_CNT`,
		  A.`NON_BLOCK_PS_HSPA_CNT`,
		  A.`NON_BLOCK_M_RAB_CNT`,
		  A.`NON_BLOCK_SIGNAL_CNT`,
		  A.`NON_BLOCK_SMS_CNT`,
		  A.`NON_BLOCK_PS_OTHER_CNT`,
		  A.`PS_CNT`,
		  A.`DROP_PS_CNT`,
		  A.`FP_RSCP_1`,
		  A.`FP_ECN0_1`,
		  A.`BEST_CNT`,
		  A.`UL_THROUPUT_MAX`,
		  A.`DL_THROUPUT_MAX`,
		  A.`CS_RRC_DURATION`,
		  A.`PS_RRC_DURATION`,
		  A.`CAUSE_14_CNT`,
		  A.`CAUSE_15_CNT`,
		  A.`CAUSE_46_CNT`,
		  A.`CAUSE_115_CNT`,
		  A.`CAUSE_OTHERS_CNT`,
		  A.`CAUSE_53_CNT`,
		  A.`CAUSE_65_CNT`,
		  A.`CAUSE_114_CNT`,
		  A.`CAUSE_263_CNT`,
		  A.`CAUSE_CAPACITY`,
		  A.MAKE_MODEL,
		  A.`TILE_ID_16`,
		  A.`TILE_ID_13`,
		  B.`REG_1_ID`,
		  B.`REG_2_ID`,
		  B.`REG_3_ID`';
	
	DECLARE TILE_LTE_COLUMN_STR VARCHAR(2000) DEFAULT 
		' A.`TILE_ID`,
		   A.`EARFCN`,
		   A.`EUTRABAND`,
		   A.`DATA_DATE`,
		   A.`DATA_HOUR`,
		   A.`INIT_CALL_CNT`,
		   A.`END_CALL_CNT`,
		   A.`SIGNAL_CNT`,
		   A.`DATA_CNT`,
		   A.`UNSPECIFIED_CNT`,
		   A.`CALL_DUR_SUM`,
		   A.`BLOCK_CNT`,
		   A.`DROP_CNT`,
		   A.`CSFB_CNT`,
		   A.`INTER_FREQ_ATTEMPT_CNT`,
		   A.`INTER_FREQ_FAILURE_CNT`,
		   A.`INTRA_FREQ_ATTEMPT_CNT`,
		   A.`INTRA_FREQ_FAILURE_CNT`,
		   A.`4G_3G_ATTEMPT_CNT`,
		   A.`4G_3G_FAILURE_CNT`,
		   A.`4G_2G_ATTEMPT_CNT`,
		   A.`4G_2G_FAILURE_CNT`,
		   A.`PS_UL_VOLUME_SUM`,
		   A.`PS_DL_VOLUME_SUM`,
		   A.`PS_UL_SPEED_MAX`,
		   A.`PS_DL_SPEED_MAX`,
		   A.`RSRP_SUM`,
		   A.`RSRP_CNT`,
		   A.`RSRQ_SUM`,
		   A.`RSRQ_CNT`,
		   A.`PILOT_DOMINANCE_SUM`,
		   A.`PILOT_DOMINANCE_CNT`,
		   A.`DROP_SIGNAL_CNT`,
		   A.`DROP_DATA_CNT`,
		   A.`DROP_SMS_CNT`,
		   A.`DROP_VOLTE_CNT`,
		   A.`DROP_UNSPECIFIED_CNT`,
		   A.`END_NON_BLOCK_CALL_CNT`,
		   B.`REG_1_ID`,
		   B.`REG_2_ID`,
		   B.`REG_3_ID`,
		   A.`UL_THROUPUT_SUM`,
		   A.`UL_THROUPUT_CNT`,
		   A.`DL_THROUPUT_SUM`,
		   A.`DL_THROUPUT_CNT`,
		   A.`END_NON_BLOCK_SIGNAL_CNT`,
		   A.`END_NON_BLOCK_DATA_CNT`,
		   A.`MR_4G_RSRP_SERVING_SUM`,
		   A.`MR_4G_RSRP_SERVING_CNT`,
		   A.`MR_4G_RSRQ_SERVING_SUM`,
		   A.`MR_4G_RSRQ_SERVING_CNT`,
		   A.`DATA_DUR_SUM`,
		   A.`SIGNAL_DUR_SUM`,
		   A.`VOLTE_DUR_SUM`,
		   A.`SMS_DUR_SUM`,
		   A.`UNSP_DUR_SUM`,
		   A.`VOLTE_CNT`,
		   A.`SMS_CNT`';
	DECLARE TILE_GSM_COLUMN_STR VARCHAR(2000) DEFAULT 
		' A.`TILE_ID`,
		   A.`FREQUENCY`,
		   A.`BCCH_ARFCN`,
		   A.`DATA_DATE`,
		   A.`DATA_HOUR`,
		   A.`INIT_CALL_CNT`,
		   A.`END_CALL_CNT`,
		   A.`VOICE_CNT`,
		   A.`SIGNAL_CNT`,
		   A.`SMS_CNT`,
		   A.`GPRS_CNT`,
		   A.`OTHER_CNT`,
		   A.`BLOCK_CNT`,
		   A.`DROP_VOICE_CNT`,
		   A.`DROP_SIGNAL_CNT`,
		   A.`DROP_SMS_CNT`,
		   A.`DROP_GPRS_CNT`,
		   A.`DROP_OTHER_CNT`,
		   A.`NON_BLOCK_VOICE_CNT`,
		   A.`NON_BLOCK_SIGNAL_CNT`,
		   A.`NON_BLOCK_SMS_CNT`,
		   A.`NON_BLOCK_GPRS_CNT`,
		   A.`NON_BLOCK_OTHER_CNT`,
		   A.`CALL_DUR_SUM`,
		   A.`RXLEV_SUM`,
		   A.`RXLEV_CNT`,
		   A.`RXQUAL_SUM`,
		   A. `RXQUAL_CNT`,
		   A.`CALL_PS_DUR`,
		  A.`TILE_ID_16`,
		  A.`TILE_ID_13`,
		  B. `REG_1_ID`,
		  B.`REG_2_ID`,
		  B.`REG_3_ID`';
	
	DECLARE ROMER_UMTS_COLUMN_STR VARCHAR(2000) DEFAULT 
		' A.`TILE_ID`,
		  A.`MCC`,
		  A.`MNC`,
		  A.`FREQUENCY`,
		  A.`UARFCN`,
		  A.`DATA_DATE`,
		  A.`DATA_HOUR`,
		  A.`INIT_CALL_CNT`,
		  A.`END_CALL_CNT`,
		  A.`VOICE_CNT`,
		  A.`VIDEO_CNT`,
		  A.`PS_R99_CNT`,
		  A.`PS_HSPA_CNT`,
		  A.`M_RAB_CNT`,
		  A.`SIGNAL_CNT`,
		  A.`SMS_CNT`,
		  A.`PS_OTHER_CNT`,
		  A.`CALL_DUR_SUM`,
		  A.`CS_DUR_SUM`,
		  A.`DROP_CNT`,
		  A.`BLOCK_CNT`,
		  A.`DROP_VOICE_CNT`,
		  A.`DROP_VIDEO_CNT`,
		  A.`DROP_PS_R99_CNT`,
		  A.`DROP_PS_HSPA_CNT`,
		  A.`DROP_M_RAB_CNT`,
		  A.`DROP_SIGNAL_CNT`,
		  A.`DROP_SMS_CNT`,
		  A.`DROP_PS_OTHER_CNT`,
		  A.`PS_UL_VOLUME_SUM`,
		  A.`PS_DL_VOLUME_SUM`,
		  A.`PS_UL_SPEED_MAX`,
		  A.`PS_DL_SPEED_MAX`,
		  A.`UL_THROUPUT_SUM`,
		  A.`UL_THROUPUT_CNT`,
		  A.`DL_THROUPUT_SUM`,
		  A.`DL_THROUPUT_CNT`,
		  A.`NON_BLOCK_VOICE_CNT`,
		  A.`NON_BLOCK_VIDEO_CNT`,
		  A.`NON_BLOCK_PS_R99_CNT`,
		  A.`NON_BLOCK_PS_HSPA_CNT`,
		  A.`NON_BLOCK_M_RAB_CNT`,
		  A.`NON_BLOCK_SIGNAL_CNT`,
		  A.`NON_BLOCK_SMS_CNT`,
		  A.`NON_BLOCK_PS_OTHER_CNT`,
		  A.`PS_CNT`,
		  A.`DROP_PS_CNT`,
		  A.`CALL_SETUP_TIME_SUM`,
		  A.`CALL_SETUP_TIME_CNT`,
		  A.`CALL_SETUP_TIME_VOICE_SUM`,
		  A.`CALL_SETUP_TIME_VOICE_CNT`,
		  A.`CALL_SETUP_TIME_VEDIO_SUM`,
		  A.`CALL_SETUP_TIME_VEDIO_CNT`,
		  A.`CALL_SETUP_TIME_R99_SUM`,
		  A.`CALL_SETUP_TIME_R99_CNT`,
		  A.`CALL_SETUP_TIME_HSPA_SUM`,
		  A.`CALL_SETUP_TIME_HSPA_CNT`,
		  A.`CALL_SETUP_TIME_MRAB_SUM`,
		  A.`CALL_SETUP_TIME_MRAB_CNT`,
		  A.`CALL_SETUP_TIME_SIG_SUM`,
		  A.`CALL_SETUP_TIME_SIG_CNT`,
		  A.`CALL_SETUP_TIME_OTH_SUM`,
		  A.`CALL_SETUP_TIME_OTH_CNT`,
		  A.`CALL_SETUP_TIME_SMS_SUM`,
		  A.`CALL_SETUP_TIME_SMS_CNT`,
		  B.`REG_1_ID`,
		  B.`REG_2_ID`,
		  B.`REG_3_ID`';
	
	DECLARE ROMER_LTE_COLUMN_STR VARCHAR(2000) DEFAULT 
		' A.`TILE_ID`,
		  A.`MCC`,
		  A.`MNC`,
		  A.`EARFCN`,
		  A.`EUTRABAND`,
		  A.`DATA_DATE`,
		  A.`DATA_HOUR`,
		  A.`INIT_CALL_CNT`,
		  A.`END_CALL_CNT`,
		  A.`SIGNAL_CNT`,
		  A.`DATA_CNT`,
		  A.`UNSPECIFIED_CNT`,
		  A.`CALL_DUR_SUM`,
		  A.`BLOCK_CNT`,
		  A.`DROP_CNT`,
		  A.`CSFB_CNT`,
		  A.`PS_UL_VOLUME_SUM`,
		  A.`PS_DL_VOLUME_SUM`,
		  A.`PS_UL_SPEED_MAX`,
		  A.`PS_DL_SPEED_MAX`,
		  A.`DROP_SIGNAL_CNT`,
		  A.`DROP_DATA_CNT`,
		  A.`DROP_SMS_CNT`,
		  A.`DROP_VOLTE_CNT`,
		  A.`DROP_UNSPECIFIED_CNT`,
		  A.`END_NON_BLOCK_CALL_CNT`,
		  A.`UL_THROUPUT_SUM`,
		  A.`UL_THROUPUT_CNT`,
		  A.`DL_THROUPUT_SUM`,
		  A.`DL_THROUPUT_CNT`,
		  A.`END_NON_BLOCK_SIGNAL_CNT`,
		  A.`END_NON_BLOCK_DATA_CNT`,
		  A.`CALL_SETUP_TIME_SUM`,
		  A.`CALL_SETUP_TIME_CNT`,
		  A.`CALL_SETUP_TIME_SIG_SUM`,
		  A.`CALL_SETUP_TIME_SIG_CNT`,
		  A.`CALL_SETUP_TIME_DATA_SUM`,
		  A.`CALL_SETUP_TIME_DATA_CNT`,
		  A.`CALL_SETUP_TIME_SMS_SUM`,
		  A.`CALL_SETUP_TIME_SMS_CNT`,
		  A.`CALL_SETUP_TIME_VOLTE_SUM`,
		  A.`CALL_SETUP_TIME_VOLTE_CNT`,
		  A.`CALL_SETUP_TIME_UNSP_SUM`,
		  A.`CALL_SETUP_TIME_UNSP_CNT`,
		  A.`VOLTE_CNT`,
		  A.`SMS_CNT`,
		  B.`REG_1_ID`,
		  B.`REG_2_ID`,
		  B.`REG_3_ID`';
	
	DECLARE ROMER_GSM_COLUMN_STR VARCHAR(2000) DEFAULT 
		' A.`TILE_ID`,
		  A.`MCC`,
		  A.`MNC`,
		  A.`FREQUENCY`,
		  A.`BCCH_ARFCN`,
		  A.`DATA_DATE`,
		  A.`DATA_HOUR`,
		  A.`INIT_CALL_CNT`,
		  A.`END_CALL_CNT`,
		  A.`VOICE_CNT`,
		  A.`SIGNAL_CNT`,
		  A.`SMS_CNT`,
		  A.`GPRS_CNT`,
		  A.`OTHER_CNT`,
		  A.`BLOCK_CNT`,
		  A.`DROP_VOICE_CNT`,
		  A.`DROP_SIGNAL_CNT`,
		  A.`DROP_SMS_CNT`,
		  A.`DROP_GPRS_CNT`,
		  A.`DROP_OTHER_CNT`,
		  A.`NON_BLOCK_VOICE_CNT`,
		  A.`NON_BLOCK_SIGNAL_CNT`,
		  A.`NON_BLOCK_SMS_CNT`,
		  A.`NON_BLOCK_GPRS_CNT`,
		  A.`NON_BLOCK_OTHER_CNT`,
		  A.`CALL_DUR_SUM`,
		  A.`CALL_SETUP_TIME_SUM`,
		  A.`CALL_SETUP_TIME_CNT`,
		  A.`CALL_SETUP_TIME_VOICE_SUM`,
		  A.`CALL_SETUP_TIME_VOICE_CNT`,
		  A.`CALL_SETUP_TIME_SIG_SUM`,
		  A.`CALL_SETUP_TIME_SIG_CNT`,
		  A.`CALL_SETUP_TIME_SMS_SUM`,
		  A.`CALL_SETUP_TIME_SMS_CNT`,
		  A.`CALL_SETUP_TIME_GPRS_SUM`,
		  A.`CALL_SETUP_TIME_GPRS_CNT`,
		  A.`CALL_SETUP_TIME_OTH_SUM`,
		  A.`CALL_SETUP_TIME_OTH_CNT`,
		  B.`REG_1_ID`,
		  B.`REG_2_ID`,
		  B.`REG_3_ID`';
	
	DECLARE DS_COLUMN_STR VARCHAR(3000) DEFAULT 
		' A.`TILE_ID`,
		  A.`DATA_DATE`,
		  A.`DATA_HOUR`,
		  A.`RSRP_SUM`,
		  A.`RSRP_CNT`,
		  A.`RSCP_SUM`,
		  A.`RSCP_CNT`,
		  A.`RXLEV_SUM`,
		  A.`RXLEV_CNT`,
		  A.`RSRQ_SUM`,
		  A.`RSRQ_CNT`,
		  A.`ECNO_SUM`,
		  A.`ECNO_CNT`,
		  A.`RXQUAL_SUM`,
		  A.`RXQUAL_CNT`,
		  A.`GSM_CS_CALL_CNT`,
		  A.`GSM_PS_CALL_CNT`,
		  A.`GSM_VOICE_DROP_CNT`,
		  A.`GSM_GPRS_DROP_CNT`,
		  A.`GSM_SMS_DROP_CNT`,
		  A.`GSM_CS_BLOCK_CNT`,
		  A.`GSM_PS_BLOCK_CNT`,
		  A.`GSM_CS_CALL_DURATION`,
		  A.`GSM_CALL_SETUP_TIME_SUM`,
		  A.`GSM_CALL_SETUP_TIME_CNT`,
		  A.`GSM_CS_CALL_SETUP_TIME_SUM`,
		  A.`GSM_CS_CALL_SETUP_TIME_CNT`,
		  A.`GSM_PS_CALL_SETUP_TIME_SUM`,
		  A.`GSM_PS_CALL_SETUP_TIME_CNT`,
		  A.`GSM_CS_SETUP_FAILURE_CNT`,
		  A.`GSM_PS_SETUP_FAILURE_CNT`,
		  A.`GSM_PS_CALL_DURATION`,
		  A.`UMTS_CS_CALL_CNT`,
		  A.`UMTS_PS_CALL_CNT`,
		  A.`UMTS_VOICE_DROP_CNT`,
		  A.`UMTS_PS_DROP_CNT`,
		  A.`UMTS_CS_CALL_DURATION`,
		  A.`UMTS_CS_BLOCK_CNT`,
		  A.`UMTS_PS_BLOCK_CNT`,
		  A.`UMTS_UL_VOLUME`,
		  A.`UMTS_DL_VOLUME`,
		  A.`UMTS_UL_THROUPUT_SUM`,
		  A.`UMTS_UL_THROUPUT_CNT`,
		  A.`UMTS_DL_THROUPUT_SUM`,
		  A.`UMTS_DL_THROUPUT_CNT`,
		  A.`UMTS_CALL_SETUP_TIME_SUM`,
		  A.`UMTS_CALL_SETUP_TIME_CNT`,
		  A.`UMTS_CS_CALL_SETUP_TIME_SUM`,
		  A.`UMTS_CS_CALL_SETUP_TIME_CNT`,
		  A.`UMTS_PS_CALL_SETUP_TIME_SUM`,
		  A.`UMTS_PS_CALL_SETUP_TIME_CNT`,
		  A.`UMTS_CS_SETUP_FAILURE_CNT`,
		  A.`UMTS_PS_SETUP_FAILURE_CNT`,
		  A.`UMTS_IRAT_HO_ATMP`,
		  A.`UMTS_IRAT_HO_FAIL`,
		  A.`UMTS_CS_RRC_DURATION`,
		  A.`UMTS_PS_RRC_DURATION`,
		  A.`LTE_CALL_CNT`,
		  A.`LTE_DROP_CNT`,
		  A.`LTE_BLOCK_CNT`,
		  A.`LTE_DURATION`,
		  A.`LTE_VOLTE_DURATION`,
		  A.`LTE_UL_VOLUME`,
		  A.`LTE_DL_VOLUME`,
		  A.`LTE_UL_THROUPUT_SUM`,
		  A.`LTE_UL_THROUPUT_CNT`,
		  A.`LTE_DL_THROUPUT_SUM`,
		  A.`LTE_DL_THROUPUT_CNT`,
		  A.`LATENCY_SUM`,
		  A.`LATENCY_CNT`,
		  A.`LTE_CALL_SETUP_TIME_SUM`,
		  A.`LTE_CALL_SETUP_TIME_CNT`,
		  A.`LTE_SETUP_FAILURE_CNT`,
		  A.`SRVCC_ATTEMPT_CNT`,
		  A.`SRVCC_FAILURE_CNT`,
		  A.`S1_HO_ATTEMPT`,
		  A.`S1_HO_FAILURE`,
		  A.`X2_HO_ATTEMPT`,
		  A.`X2_HO_FAILURE`,
		  A.`UMTS_MAX_UL_THROUPUT`,
		  A.`UMTS_MAX_DL_THROUPUT`,
		  A.`LTE_MAX_UL_THROUPUT`,
		  A.`LTE_MAX_DL_THROUPUT`,
		  A.`LTE_IRAT_TO_UMTS_ATMP`,
		  A.`LTE_IRAT_TO_GERAN_ATMP`,
		  A.`LTE_IRAT_TO_CDMA_ATMP`,
		  A.`LTE_IRAT_TO_UMTS_FAIL`,
		  A.`LTE_IRAT_TO_GERAN_FAIL`,
		  A.`LTE_IRAT_TO_CDMA_FAIL`,
		  A.`COMPLETED_PU_CNT`,
		  A.`TOTAL_PU_CNT`,
		  B.`REG_1_ID`,
		  B.`REG_2_ID`,
		  B.`REG_3_ID`';
	DECLARE DS_GSM_COLUMN_STR VARCHAR(2000) DEFAULT 
		' A.`TILE_ID`,
		  A.`DATA_DATE`,
		  A.`DATA_HOUR`,
		  A.`TILE_ID_16`,
		  A.`TILE_ID_13`,
		  A.`RXLEV_SUM`,
		  A.`RXLEV_CNT`,
		  A.`RXQUAL_SUM`,
		  A.`RXQUAL_CNT`,
		  A.`GSM_CS_CALL_CNT`,
		  A.`GSM_PS_CALL_CNT`,
		  A.`GSM_VOICE_DROP_CNT`,
		  A.`GSM_GPRS_DROP_CNT`,
		  A.`GSM_SMS_DROP_CNT`,
		  A.`GSM_CS_BLOCK_CNT`,
		  A.`GSM_PS_BLOCK_CNT`,
		  A.`GSM_CS_CALL_DURATION`,
		  A.`GSM_CALL_SETUP_TIME_SUM`,
		  A.`GSM_CALL_SETUP_TIME_CNT`,
		  A.`GSM_CS_CALL_SETUP_TIME_SUM`,
		  A.`GSM_CS_CALL_SETUP_TIME_CNT`,
		  A.`GSM_PS_CALL_SETUP_TIME_SUM`,
		  A.`GSM_PS_CALL_SETUP_TIME_CNT`,
		  A.`GSM_CS_SETUP_FAILURE_CNT`,
		  A.`GSM_PS_SETUP_FAILURE_CNT`,
		  A.`GSM_PS_CALL_DURATION`,
		  B.`REG_1_ID`,
		  B.`REG_2_ID`,
		  B.`REG_3_ID`';
	
	CALL gt_gw_main.`SP_Sub_Set_Session_Param_LTE`('');
	
	SET @global_db='gt_global_statistic';
	INSERT INTO gt_gw_main.SP_LOG VALUES(@global_db,'SP_Generate_Global_Statistic_Sub_Tile_Reg_Map',CONCAT(TBL_NAME,',',WORKER_ID,',',SUB_WORKER_ID,', START'), START_TIME);
	SET STEP_START_TIME := SYSDATE();
	
	
	SET @SqlCmd=CONCAT('SELECT COUNT(*) INTO @V_REGION_TILE_GID FROM information_schema.`TABLES`
								WHERE TABLE_SCHEMA=''',@global_db,''' 
								AND TABLE_NAME=''table_region_tile_g',GROUP_ID,''';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
				
	IF @V_REGION_TILE_GID=0 THEN 
		SET @SqlCmd=CONCAT('CREATE TABLE  ',@global_db,'.table_region_tile_g',GROUP_ID,'(
			  `tile_id` BIGINT(20) NOT NULL DEFAULT ''0'',
			  `reg_1_id` BIGINT(20) DEFAULT ''0'',
			  `reg_2_id` INT(20) DEFAULT ''0'',
			  `reg_3_id` BIGINT(20) DEFAULT ''0'',
			  `reg_level` TINYINT(4) DEFAULT NULL,
			  `group_id` TINYINT(4) DEFAULT NULL,
			   KEY `idx_reg_tile` (`tile_id`,`group_id`,`reg_1_id`,`reg_2_id`,`reg_3_id`))
			   ENGINE=MYISAM DEFAULT CHARSET=utf8 DELAY_KEY_WRITE=1;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
	
		IF GROUP_ID =1 THEN 
			SET @SqlCmd=CONCAT('INSERT IGNORE INTO ',@global_db,'.table_region_tile_g',GROUP_ID,'
					SELECT tile_id,reg_1_id,reg_2_id,reg_3_id,reg_level,group_id 
					FROM ',@global_db,'.table_region_tile_g
					WHERE group_id IN (0,1);');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
		ELSE 
			SET @SqlCmd=CONCAT('INSERT IGNORE INTO ',@global_db,'.table_region_tile_g',GROUP_ID,'
					SELECT tile_id,reg_1_id,reg_2_id,reg_3_id,reg_level,group_id 
					FROM ',@global_db,'.table_region_tile_g
					WHERE group_id =',GROUP_ID,';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
		END IF;	
	END IF;	
	IF IsIMSI THEN 
		SET @SqlCmd=CONCAT('UPDATE ',TBL_NAME,' A,',@global_db,'.`table_region_tile_g',GROUP_ID,'` B
					SET A.START_REG_1_ID=B.REG_1_ID,
						A.START_REG_2_ID=B.REG_2_ID,
						A.START_REG_3_ID=B.REG_3_ID
					WHERE A.`START_TILE_ID`=B.`TILE_ID` 
					AND B.group_id=',GROUP_ID,'
					AND B.reg_level IN (1,2,3);
			;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		INSERT INTO gt_gw_main.SP_LOG VALUES(@global_db,'SP_Generate_Global_Statistic_Sub_Tile_Reg_Map',CONCAT(TBL_NAME,' UPDATE start_reg_id cost:',TIMESTAMPDIFF(SECOND,STEP_START_TIME,SYSDATE()),' sec.'), NOW());
		SET STEP_START_TIME := SYSDATE();
		SET @SqlCmd=CONCAT('UPDATE ',TBL_NAME,' A,',@global_db,'.`table_region_tile_g',GROUP_ID,'` B
					SET A.END_REG_1_ID=B.REG_1_ID,
						A.END_REG_2_ID=B.REG_2_ID,
						A.END_REG_3_ID=B.REG_3_ID
					WHERE A.`END_TILE_ID`=B.`TILE_ID`  
					AND B.group_id=',GROUP_ID,' 
					AND B.reg_level IN (1,2,3);
			;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		INSERT INTO gt_gw_main.SP_LOG VALUES(@global_db,'SP_Generate_Global_Statistic_Sub_Tile_Reg_Map',CONCAT(TBL_NAME,' UPDATE end_reg_id cost:',TIMESTAMPDIFF(SECOND,STEP_START_TIME,SYSDATE()),' sec.'), NOW());
		SET STEP_START_TIME := SYSDATE();
	
	ELSE 
	
		SET @SqlCmd=CONCAT('SELECT att_value INTO @SYS_CONFIG_TILE FROM gt_covmo.`sys_config`
							WHERE group_name=''system'' AND att_name=''MapResolution'';');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		
		IF gt_covmo_csv_count(@SYS_CONFIG_TILE,',') =3 THEN
		
			SET @SqlCmd=CONCAT('SELECT gt_covmo_csv_get(att_value,3) INTO @ZOOM_LEVEL FROM gt_covmo.`sys_config` WHERE `group_name`=''system'' AND att_name = ''MapResolution'';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
		ELSE 
			SET @SqlCmd=CONCAT('SELECT att_value INTO @ZOOM_LEVEL FROM gt_global_statistic.`nw_config` WHERE `group_name`=''system'' AND att_name = ''MapResolution'';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
		END IF;
		
		
		SET @SqlCmd=CONCAT('SELECT COUNT(*) INTO @v_REG3 FROM ',@global_db,'.`usr_polygon_reg_3` WHERE group_id=',GROUP_ID,';');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt; 
		
		IF @v_REG3=0 THEN 
			LEAVE a_label;
		END IF;
			
		SET @SqlCmd=CONCAT('SELECT COUNT(*) INTO @v_REG2 FROM ',@global_db,'.`usr_polygon_reg_2` WHERE group_id=',GROUP_ID,';');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt; 
		
		SET @SqlCmd=CONCAT('SELECT COUNT(*) INTO @v_REG1 FROM ',@global_db,'.`usr_polygon_reg_1` WHERE group_id=',GROUP_ID,';');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt; 
				
		SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS ',@global_db,'.`tmp_tile_3_lte_',WORKER_ID,'_',SUB_WORKER_ID,'_g',GROUP_ID,'`;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt; 
		
		SET @SqlCmd=CONCAT('CREATE TEMPORARY TABLE ',@global_db,'.`tmp_tile_3_lte_',WORKER_ID,'_',SUB_WORKER_ID,'_g',GROUP_ID,'` (
					  `tile_id` BIGINT(20) NOT NULL,
					  `lon_lat` VARCHAR(70) DEFAULT NULL,
					  PRIMARY KEY (`tile_id`)
				) ENGINE=MYISAM DEFAULT CHARSET=latin1;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		
		SET @SqlCmd =CONCAT('ALTER TABLE ',@global_db,'.`tmp_tile_3_lte_',WORKER_ID,'_',SUB_WORKER_ID,'_g',GROUP_ID,'` DISABLE KEYS;') ;
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		INSERT INTO gt_gw_main.SP_LOG VALUES(@global_db,'SP_Generate_Global_Statistic_Sub_Tile_Reg_Map',CONCAT('tmp_tile_3_',WORKER_ID,'_',SUB_WORKER_ID,'_g',GROUP_ID,' DISABLE KEYS cost:',TIMESTAMPDIFF(SECOND,STEP_START_TIME,SYSDATE()),' sec.'), NOW());
		SET STEP_START_TIME := SYSDATE();
				
		SET @SqlCmd=CONCAT('INSERT INTO ',@global_db,'.`tmp_tile_3_lte_',WORKER_ID,'_',SUB_WORKER_ID,'_g',GROUP_ID,'`  
					(`tile_id` ,`lon_lat` )
					SELECT tile_id ,CONCAT(''POINT('',(gt_covmo_proj_geohash_to_lng(gt_covmo_hex_geohash_upperbound(TILE_ID,',@ZOOM_LEVEL,'))+gt_covmo_proj_geohash_to_lng(gt_covmo_hex_geohash_lowerbound(TILE_ID,',@ZOOM_LEVEL,')))/2,'' '',(gt_covmo_proj_geohash_to_lat(gt_covmo_hex_geohash_upperbound(TILE_ID,',@ZOOM_LEVEL,'))+gt_covmo_proj_geohash_to_lat(gt_covmo_hex_geohash_lowerbound(TILE_ID,',@ZOOM_LEVEL,')))/2,'')'') AS lon_lat 
					FROM 
					(
						SELECT DISTINCT tile_id FROM ',TBL_NAME,' A 
						WHERE NOT EXISTS 
							(
								SELECT NULL FROM ',@global_db,'.`table_region_tile_g',GROUP_ID,'` C FORCE INDEX(idx_reg_tile)
								WHERE C.`tile_id`=A.`tile_id` 
							)
					) A;
					');
	
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
	
		INSERT INTO gt_gw_main.SP_LOG VALUES(@global_db,'SP_Generate_Global_Statistic_Sub_Tile_Reg_Map',CONCAT('tmp_tile_3_',WORKER_ID,'_',SUB_WORKER_ID,'_g',GROUP_ID,' INSERT cost:',TIMESTAMPDIFF(SECOND,STEP_START_TIME,SYSDATE()),' sec.'), NOW());
		SET STEP_START_TIME := SYSDATE();
		
		SET @SqlCmd =CONCAT('ALTER TABLE ',@global_db,'.`tmp_tile_3_lte_',WORKER_ID,'_',SUB_WORKER_ID,'_g',GROUP_ID,'` ENABLE KEYS;') ;
	
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		INSERT INTO gt_gw_main.SP_LOG VALUES(@global_db,'SP_Generate_Global_Statistic_Sub_Tile_Reg_Map',CONCAT('tmp_tile_3_',WORKER_ID,'_',SUB_WORKER_ID,'_g',GROUP_ID,' ENABLE KEYS cost:',TIMESTAMPDIFF(SECOND,STEP_START_TIME,SYSDATE()),' sec.'), NOW());
		SET STEP_START_TIME := SYSDATE();
		
		SET @SqlCmd =CONCAT('INSERT IGNORE INTO ',@global_db,'.table_region_tile_g',GROUP_ID,' 
					    (`tile_id`,
					     `reg_1_id`,
					     `reg_2_id`,
					     `reg_3_id`,
					     `reg_level`,
					     `group_id`)
					SELECT A.`tile_id`,
						CASE WHEN (@v_REG1=0 OR @v_REG2=0) THEN 0 ELSE FLOOR(B.`parent_id`/1000) END AS reg_1_id,
						CASE WHEN @v_REG2=0 THEN 0 ELSE B.`parent_id` END AS reg_2_id,
						B.`id` AS reg_3_id,3,',GROUP_ID,' 
					FROM ',@global_db,'.tmp_tile_3_lte_',WORKER_ID,'_',SUB_WORKER_ID,'_g',GROUP_ID,' A
					,',@global_db,'.`usr_polygon_reg_3` B
					WHERE gt_covmo_pointinpoly(
							ASWKB(GEOMFROMTEXT(A.`lon_lat`)),
							ASWKB(GEOMFROMTEXT(B.`polygon_str`)))=1 
						AND B.`group_id`=',GROUP_ID,'
					;') ;
	
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		INSERT INTO gt_gw_main.SP_LOG VALUES(@global_db,'SP_Generate_Global_Statistic_Sub_Tile_Reg_Map',CONCAT('table_region_tile_g',GROUP_ID,',3,',WORKER_ID,'_',SUB_WORKER_ID,' INSERT cost:',TIMESTAMPDIFF(SECOND,STEP_START_TIME,SYSDATE()),' sec.'), NOW());
		SET STEP_START_TIME := SYSDATE();
/*		IF @v_REG2>0 THEN 
			SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS ',@global_db,'.`tmp_tile_2_lte_',WORKER_ID,'_',SUB_WORKER_ID,'`;');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt; 
			SET @SqlCmd=CONCAT('CREATE TEMPORARY TABLE ',@global_db,'.`tmp_tile_2_lte_',WORKER_ID,'_',SUB_WORKER_ID,'` (
						  `tile_id` BIGINT(20) NOT NULL,
						  `lon_lat` VARCHAR(70) DEFAULT NULL,
						  PRIMARY KEY (`tile_id`)
					) ENGINE=MYISAM DEFAULT CHARSET=latin1;');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
			
			SET @SqlCmd =CONCAT('ALTER TABLE ',@global_db,'.`tmp_tile_2_lte_',WORKER_ID,'_',SUB_WORKER_ID,'` DISABLE KEYS;') ;
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
			INSERT INTO gt_gw_main.SP_LOG VALUES(@global_db,'SP_Generate_Global_Statistic_Sub_Tile_Reg_Map',CONCAT('tmp_tile_2_',WORKER_ID,'_',SUB_WORKER_ID,' DISABLE KEYS cost:',TIMESTAMPDIFF(SECOND,STEP_START_TIME,SYSDATE()),' sec.'), NOW());
			SET STEP_START_TIME := SYSDATE();
					
			SET @SqlCmd=CONCAT('INSERT INTO ',@global_db,'.`tmp_tile_2_lte_',WORKER_ID,'_',SUB_WORKER_ID,'`  
						(`tile_id` ,`lon_lat` )
						SELECT tile_id ,CONCAT(''POINT('',(gt_covmo_proj_geohash_to_lng(gt_covmo_hex_geohash_upperbound(TILE_ID,',@ZOOM_LEVEL,'))+gt_covmo_proj_geohash_to_lng(gt_covmo_hex_geohash_lowerbound(TILE_ID,',@ZOOM_LEVEL,')))/2,'' '',(gt_covmo_proj_geohash_to_lat(gt_covmo_hex_geohash_upperbound(TILE_ID,',@ZOOM_LEVEL,'))+gt_covmo_proj_geohash_to_lat(gt_covmo_hex_geohash_lowerbound(TILE_ID,',@ZOOM_LEVEL,')))/2,'')'') AS lon_lat 
						FROM 
						(
							SELECT tile_id FROM ',@global_db,'.tmp_tile_3_lte_',WORKER_ID,'_',SUB_WORKER_ID,'_g',GROUP_ID,' A 
							WHERE NOT EXISTS 
								(
									SELECT NULL FROM ',@global_db,'.`table_region_tile_g',GROUP_ID,'` C FORCE INDEX(idx_reg_tile)
									WHERE C.`tile_id`=A.`tile_id`
								)
						) A;
						');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
			INSERT INTO gt_gw_main.SP_LOG VALUES(@global_db,'SP_Generate_Global_Statistic_Sub_Tile_Reg_Map',CONCAT('tmp_tile_2_',WORKER_ID,'_',SUB_WORKER_ID,' INSERT cost:',TIMESTAMPDIFF(SECOND,STEP_START_TIME,SYSDATE()),' sec.'), NOW());
			SET STEP_START_TIME := SYSDATE();
			
			SET @SqlCmd =CONCAT('ALTER TABLE ',@global_db,'.`tmp_tile_2_lte_',WORKER_ID,'_',SUB_WORKER_ID,'` ENABLE KEYS;') ;
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
			INSERT INTO gt_gw_main.SP_LOG VALUES(@global_db,'SP_Generate_Global_Statistic_Sub_Tile_Reg_Map',CONCAT('tmp_tile_2_',WORKER_ID,'_',SUB_WORKER_ID,' ENABLE KEYS cost:',TIMESTAMPDIFF(SECOND,STEP_START_TIME,SYSDATE()),' sec.'), NOW());
			SET STEP_START_TIME := SYSDATE();
					
			SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS ',@global_db,'.`tmp_tile_3_lte_',WORKER_ID,'_',SUB_WORKER_ID,'_g',GROUP_ID,'`;');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt; 
						
			SET @SqlCmd =CONCAT('INSERT IGNORE INTO ',@global_db,'.table_region_tile_g',GROUP_ID,'  
						    (`tile_id`,
						     `reg_1_id`,
						     `reg_2_id`,
						     `reg_3_id`,
						     `reg_level`,
						     `group_id`)
						SELECT A.`tile_id`,
							B.`parent_id` AS reg_1_id,
							B.`id` AS reg_2_id,
							NULL AS reg_3_id,2,',GROUP_ID,' 
						FROM ',@global_db,'.tmp_tile_2_lte_',WORKER_ID,'_',SUB_WORKER_ID,' A
						,',@global_db,'.`usr_polygon_reg_2` B
						WHERE gt_covmo_pointinpoly(
								ASWKB(GEOMFROMTEXT(A.`lon_lat`)),
								ASWKB(GEOMFROMTEXT(B.`polygon_str`)))=1 
							AND B.`group_id`=',GROUP_ID,'
					;') ;
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
			INSERT INTO gt_gw_main.SP_LOG VALUES(@global_db,'SP_Generate_Global_Statistic_Sub_Tile_Reg_Map',CONCAT('table_region_tile_g,2,',WORKER_ID,'_',SUB_WORKER_ID,' INSERT cost:',TIMESTAMPDIFF(SECOND,STEP_START_TIME,SYSDATE()),' sec.'), NOW());
			SET STEP_START_TIME := SYSDATE();
			
			IF @v_REG1>0 THEN 		
				SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS ',@global_db,'.`tmp_tile_1_lte_',WORKER_ID,'_',SUB_WORKER_ID,'`;');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt; 
				
				SET @SqlCmd=CONCAT('CREATE TEMPORARY TABLE ',@global_db,'.`tmp_tile_1_lte_',WORKER_ID,'_',SUB_WORKER_ID,'` (
							  `tile_id` BIGINT(20) NOT NULL,
							  `lon_lat` VARCHAR(70) DEFAULT NULL,
							  PRIMARY KEY (`tile_id`)
						) ENGINE=MYISAM DEFAULT CHARSET=latin1;');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				
				SET @SqlCmd =CONCAT('ALTER TABLE ',@global_db,'.`tmp_tile_1_lte_',WORKER_ID,'_',SUB_WORKER_ID,'` DISABLE KEYS;') ;
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				INSERT INTO gt_gw_main.SP_LOG VALUES(@global_db,'SP_Generate_Global_Statistic_Sub_Tile_Reg_Map',CONCAT('tmp_tile_1_',WORKER_ID,'_',SUB_WORKER_ID,' DISABLE KEYS cost:',TIMESTAMPDIFF(SECOND,STEP_START_TIME,SYSDATE()),' sec.'), NOW());
				SET STEP_START_TIME := SYSDATE();
						
				SET @SqlCmd=CONCAT('INSERT INTO ',@global_db,'.`tmp_tile_1_lte_',WORKER_ID,'_',SUB_WORKER_ID,'`  
							(`tile_id` ,`lon_lat` )
							SELECT tile_id ,CONCAT(''POINT('',(gt_covmo_proj_geohash_to_lng(gt_covmo_hex_geohash_upperbound(TILE_ID,',@ZOOM_LEVEL,'))+gt_covmo_proj_geohash_to_lng(gt_covmo_hex_geohash_lowerbound(TILE_ID,',@ZOOM_LEVEL,')))/2,'' '',(gt_covmo_proj_geohash_to_lat(gt_covmo_hex_geohash_upperbound(TILE_ID,',@ZOOM_LEVEL,'))+gt_covmo_proj_geohash_to_lat(gt_covmo_hex_geohash_lowerbound(TILE_ID,',@ZOOM_LEVEL,')))/2,'')'') AS lon_lat 
							FROM 
							(
								SELECT tile_id FROM ',@global_db,'.tmp_tile_2_lte_',WORKER_ID,'_',SUB_WORKER_ID,' A 
								WHERE NOT EXISTS 
									(
										SELECT NULL FROM ',@global_db,'.`table_region_tile_g',GROUP_ID,'` C FORCE INDEX(idx_reg_tile)
										WHERE C.`tile_id`=A.`tile_id` 
									)
							) A;
							');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				INSERT INTO gt_gw_main.SP_LOG VALUES(@global_db,'SP_Generate_Global_Statistic_Sub_Tile_Reg_Map',CONCAT('tmp_tile_1_',WORKER_ID,'_',SUB_WORKER_ID,' INSERT cost:',TIMESTAMPDIFF(SECOND,STEP_START_TIME,SYSDATE()),' sec.'), NOW());
				SET STEP_START_TIME := SYSDATE();
				
				SET @SqlCmd =CONCAT('ALTER TABLE ',@global_db,'.`tmp_tile_1_lte_',WORKER_ID,'_',SUB_WORKER_ID,'` ENABLE KEYS;') ;
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				INSERT INTO gt_gw_main.SP_LOG VALUES(@global_db,'SP_Generate_Global_Statistic_Sub_Tile_Reg_Map',CONCAT('tmp_tile_1_',WORKER_ID,'_',SUB_WORKER_ID,' ENABLE KEYS cost:',TIMESTAMPDIFF(SECOND,STEP_START_TIME,SYSDATE()),' sec.'), NOW());
				SET STEP_START_TIME := SYSDATE();
				
				SET @SqlCmd =CONCAT('INSERT IGNORE INTO ',@global_db,'.table_region_tile_g',GROUP_ID,' 
							    (`tile_id`,
							     `reg_1_id`,
							     `reg_2_id`,
							     `reg_3_id`,
							     `reg_level`,
							     `group_id`)
							SELECT A.`tile_id`,
								`id` AS reg_1_id,
								NULL AS reg_2_id,
								NULL AS reg_3_id,1,',GROUP_ID,' 
							FROM ',@global_db,'.tmp_tile_1_lte_',WORKER_ID,'_',SUB_WORKER_ID,' A
							,',@global_db,'.`usr_polygon_reg_1` B
							WHERE gt_covmo_pointinpoly(
									ASWKB(GEOMFROMTEXT(A.`lon_lat`)),
									ASWKB(GEOMFROMTEXT(B.`polygon_str`)))=1 
								AND B.`group_id`=',GROUP_ID,'
						;') ;
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				INSERT INTO gt_gw_main.SP_LOG VALUES(@global_db,'SP_Generate_Global_Statistic_Sub_Tile_Reg_Map',CONCAT('table_region_tile_g,1,',WORKER_ID,'_',SUB_WORKER_ID,' INSERT cost:',TIMESTAMPDIFF(SECOND,STEP_START_TIME,SYSDATE()),' sec.'), NOW());
				SET STEP_START_TIME := SYSDATE();
						
				SET @SqlCmd=CONCAT('INSERT IGNORE INTO ',@global_db,'.table_region_tile_g',GROUP_ID,'  
							    (`tile_id`,
							     `reg_1_id`,
							     `reg_2_id`,
							     `reg_3_id`,
							     `reg_level`,
							     `group_id`)
							SELECT A.`tile_id`,
								NULL AS reg_1_id,
								NULL AS reg_2_id,
								NULL AS reg_3_id,0,0
							FROM ',@global_db,'.tmp_tile_1_lte_',WORKER_ID,'_',SUB_WORKER_ID,' A 
							WHERE NOT EXISTS 
									(
										SELECT NULL FROM ',@global_db,'.`table_region_tile_g',GROUP_ID,'` C FORCE INDEX(idx_reg_tile)
										WHERE C.`tile_id`=A.`tile_id` 
									)
							;');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				INSERT INTO gt_gw_main.SP_LOG VALUES(@global_db,'SP_Generate_Global_Statistic_Sub_Tile_Reg_Map',CONCAT('tmp_tile_0_',WORKER_ID,'_',SUB_WORKER_ID,' INSERT cost:',TIMESTAMPDIFF(SECOND,STEP_START_TIME,SYSDATE()),' sec.'), NOW());
				SET STEP_START_TIME := SYSDATE();
									
				SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS ',@global_db,'.`tmp_tile_1_lte_',WORKER_ID,'_',SUB_WORKER_ID,'`;');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
			ELSE
				SET @SqlCmd=CONCAT('INSERT IGNORE INTO ',@global_db,'.table_region_tile_g',GROUP_ID,' 
							    (`tile_id`,
							     `reg_1_id`,
							     `reg_2_id`,
							     `reg_3_id`,
							     `reg_level`,
							     `group_id`)
							SELECT A.`tile_id`,
								0 AS reg_1_id,
								0 AS reg_2_id,
								0 AS reg_3_id,0,0
							FROM ',@global_db,'.tmp_tile_2_lte_',WORKER_ID,'_',SUB_WORKER_ID,' A 
							WHERE NOT EXISTS 
									(
										SELECT NULL FROM ',@global_db,'.`table_region_tile_g',GROUP_ID,'` C FORCE INDEX(idx_reg_tile)
										WHERE C.`tile_id`=A.`tile_id`
									)
							;');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				INSERT INTO gt_gw_main.SP_LOG VALUES(@global_db,'SP_Generate_Global_Statistic_Sub_Tile_Reg_Map',CONCAT('tmp_tile_0_',WORKER_ID,'_',SUB_WORKER_ID,' INSERT cost:',TIMESTAMPDIFF(SECOND,STEP_START_TIME,SYSDATE()),' sec.'), NOW());
				SET STEP_START_TIME := SYSDATE();			
			END IF;
									
			SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS ',@global_db,'.`tmp_tile_2_lte_',WORKER_ID,'_',SUB_WORKER_ID,'`;');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
		ELSE*/
		SET @SqlCmd=CONCAT('INSERT IGNORE INTO ',@global_db,'.table_region_tile_g',GROUP_ID,' 
			    (`tile_id`,
			     `reg_1_id`,
			     `reg_2_id`,
			     `reg_3_id`,
			     `reg_level`,
			     `group_id`)
			SELECT A.`tile_id`,
				0 AS reg_1_id,
				0 AS reg_2_id,
				0 AS reg_3_id,0,0
			FROM ',@global_db,'.`tmp_tile_3_lte_',WORKER_ID,'_',SUB_WORKER_ID,'_g',GROUP_ID,'` A 
			WHERE NOT EXISTS 
					(
						SELECT NULL FROM ',@global_db,'.`table_region_tile_g',GROUP_ID,'` C FORCE INDEX(idx_reg_tile)
						WHERE C.`tile_id`=A.`tile_id`
					)
			;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		INSERT INTO gt_gw_main.SP_LOG VALUES(@global_db,'SP_Generate_Global_Statistic_Sub_Tile_Reg_Map',CONCAT('tmp_tile_0_',WORKER_ID,'_',SUB_WORKER_ID,' INSERT cost:',TIMESTAMPDIFF(SECOND,STEP_START_TIME,SYSDATE()),' sec.'), NOW());
		SET STEP_START_TIME := SYSDATE();
		
		SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS ',@global_db,'.`tmp_tile_3_lte_',WORKER_ID,'_',SUB_WORKER_ID,'_g',GROUP_ID,'`;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt; 
	
	
	IF DS_FLAG=0 THEN 
		IF TECH_MASK =4 AND RPT_TYPE= 1 THEN 
			SET @SqlCmd=CONCAT('DROP TABLE IF EXISTS ',@global_db,'.`tmp_update_regid_tile_lte_',WORKER_ID,'`;');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt; 
		
			SET @SqlCmd =CONCAT('CREATE TABLE IF NOT EXISTS ',@global_db,'.`tmp_update_regid_tile_lte_',WORKER_ID,'` select * from ',TBL_NAME,' where 1<>1;');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd =CONCAT('INSERT INTO ',@global_db,'.`tmp_update_regid_tile_lte_',WORKER_ID,'` 
						SELECT
						 ',TILE_LTE_COLUMN_STR,'
						FROM ',TBL_NAME,' A LEFT JOIN ',@global_db,'.`table_region_tile_g',GROUP_ID,'` B
						ON A.`tile_id`=B.`tile_id` AND B.reg_level IN (1,2,3,0)
						;
			');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
		
			SET @SqlCmd=CONCAT('TRUNCATE TABLE ',TBL_NAME,';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
		
			SET @SqlCmd=CONCAT('INSERT INTO ',TBL_NAME,'
						SELECT * FROM ',@global_db,'.`tmp_update_regid_tile_lte_',WORKER_ID,'`;');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
		
			SET @SqlCmd=CONCAT('DROP TABLE IF EXISTS ',@global_db,'.`tmp_update_regid_tile_lte_',WORKER_ID,'`;');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt; 
		END IF;
	
		IF TECH_MASK =4 AND RPT_TYPE= 8 THEN 
			SET @SqlCmd=CONCAT('DROP TABLE IF EXISTS ',@global_db,'.`tmp_update_regid_roamer_lte_',WORKER_ID,'`;');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt; 
		
			SET @SqlCmd =CONCAT('CREATE TABLE IF NOT EXISTS ',@global_db,'.`tmp_update_regid_roamer_lte_',WORKER_ID,'` select * from ',TBL_NAME,' where 1 <> 1;');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd =CONCAT('INSERT INTO ',@global_db,'.`tmp_update_regid_roamer_lte_',WORKER_ID,'`
						SELECT
						 ',ROMER_LTE_COLUMN_STR,'
						FROM ',TBL_NAME,' A LEFT JOIN ',@global_db,'.`table_region_tile_g',GROUP_ID,'` B
						ON A.`tile_id`=B.`tile_id` AND B.reg_level IN (1,2,3,0)
						;
			');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
		
			SET @SqlCmd=CONCAT('TRUNCATE TABLE ',TBL_NAME,';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
		
			SET @SqlCmd=CONCAT('INSERT INTO ',TBL_NAME,'
						SELECT * FROM ',@global_db,'.`tmp_update_regid_roamer_lte_',WORKER_ID,'` ;');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
		
			SET @SqlCmd=CONCAT('DROP TABLE IF EXISTS ',@global_db,'.`tmp_update_regid_roamer_lte_',WORKER_ID,'`;');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt; 
		END IF;
	
		IF TECH_MASK =2 AND RPT_TYPE= 1 THEN 
			SET @SqlCmd=CONCAT('DROP TABLE IF EXISTS ',@global_db,'.`tmp_update_regid_tile_umts_',WORKER_ID,'`;');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt; 
		
			SET @SqlCmd =CONCAT('CREATE TABLE IF NOT EXISTS ',@global_db,'.`tmp_update_regid_tile_umts_',WORKER_ID,'` select * from ',TBL_NAME,' where 1<>1;');
		
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			
	
			SET @SqlCmd =CONCAT('INSERT INTO ',@global_db,'.`tmp_update_regid_tile_umts_',WORKER_ID,'`
						SELECT
						 ',TILE_UMTS_COLUMN_STR,'
						FROM ',TBL_NAME,' A LEFT JOIN ',@global_db,'.`table_region_tile_g',GROUP_ID,'` B
						ON A.`tile_id`=B.`tile_id` AND B.reg_level IN (1,2,3,0)
						;
	
			');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
		
			SET @SqlCmd=CONCAT('TRUNCATE TABLE ',TBL_NAME,';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
		
			SET @SqlCmd=CONCAT('INSERT INTO ',TBL_NAME,'
						SELECT * FROM ',@global_db,'.`tmp_update_regid_tile_umts_',WORKER_ID,'` ;');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
		
			SET @SqlCmd=CONCAT('DROP TABLE IF EXISTS ',@global_db,'.`tmp_update_regid_tile_umts_',WORKER_ID,'`;');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt; 
		END IF;
	
		IF TECH_MASK =2 AND RPT_TYPE= 8 THEN 
			SET @SqlCmd=CONCAT('DROP TABLE IF EXISTS ',@global_db,'.`tmp_update_regid_roamer_umts_',WORKER_ID,'`;');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt; 
		
			SET @SqlCmd =CONCAT('CREATE TABLE IF NOT EXISTS ',@global_db,'.`tmp_update_regid_roamer_umts_',WORKER_ID,'` select * from ',TBL_NAME,' where 1<>1;');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd =CONCAT('INSERT INTO ',@global_db,'.`tmp_update_regid_roamer_umts_',WORKER_ID,'`
						SELECT
						 ',ROMER_UMTS_COLUMN_STR,'
						FROM ',TBL_NAME,' A LEFT JOIN ',@global_db,'.`table_region_tile_g',GROUP_ID,'` B
						ON A.`tile_id`=B.`tile_id` AND B.reg_level IN (1,2,3,0)
						;
			');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
		
			SET @SqlCmd=CONCAT('TRUNCATE TABLE ',TBL_NAME,';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
		
			SET @SqlCmd=CONCAT('INSERT INTO ',TBL_NAME,'
						SELECT * FROM ',@global_db,'.`tmp_update_regid_roamer_umts_',WORKER_ID,'` ;');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
		
			SET @SqlCmd=CONCAT('DROP TABLE IF EXISTS ',@global_db,'.`tmp_update_regid_roamer_umts_',WORKER_ID,'`;');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt; 
		END IF;
	
		IF TECH_MASK =1 AND RPT_TYPE= 1 THEN 
			SET @SqlCmd=CONCAT('DROP TABLE IF EXISTS ',@global_db,'.`tmp_update_regid_tile_gsm_',WORKER_ID,'`;');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt; 
		
			SET @SqlCmd =CONCAT('CREATE TABLE IF NOT EXISTS ',@global_db,'.`tmp_update_regid_tile_gsm_',WORKER_ID,'` select * from ',TBL_NAME,' where 1<>1;');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd =CONCAT('INSERT INTO ',@global_db,'.`tmp_update_regid_tile_gsm_',WORKER_ID,'`
						SELECT
						 ',TILE_GSM_COLUMN_STR,'
						FROM ',TBL_NAME,' A LEFT JOIN ',@global_db,'.`table_region_tile_g',GROUP_ID,'` B
						ON A.`tile_id`=B.`tile_id` AND B.reg_level IN (1,2,3,0)
						;
			');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
		
			SET @SqlCmd=CONCAT('TRUNCATE TABLE ',TBL_NAME,';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
		
			SET @SqlCmd=CONCAT('INSERT INTO ',TBL_NAME,'
						SELECT * FROM ',@global_db,'.`tmp_update_regid_tile_gsm_',WORKER_ID,'` ;');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
		
			SET @SqlCmd=CONCAT('DROP TABLE IF EXISTS ',@global_db,'.`tmp_update_regid_tile_gsm_',WORKER_ID,'`;');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt; 
		END IF;
	
		IF TECH_MASK =1 AND RPT_TYPE= 8 THEN 
			SET @SqlCmd=CONCAT('DROP TABLE IF EXISTS ',@global_db,'.`tmp_update_regid_roamer_gsm_',WORKER_ID,'`;');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt; 
		
			SET @SqlCmd =CONCAT('CREATE TABLE IF NOT EXISTS ',@global_db,'.`tmp_update_regid_roamer_gsm_',WORKER_ID,'` select * from ',TBL_NAME,' where 1<>1;');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd =CONCAT('INSERT INTO ',@global_db,'.`tmp_update_regid_roamer_gsm_',WORKER_ID,'`
						SELECT
						 ',ROMER_GSM_COLUMN_STR,'
						FROM ',TBL_NAME,' A LEFT JOIN ',@global_db,'.`table_region_tile_g',GROUP_ID,'` B
						ON A.`tile_id`=B.`tile_id` AND B.reg_level IN (1,2,3,0)
						;
			');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
		
			SET @SqlCmd=CONCAT('TRUNCATE TABLE ',TBL_NAME,';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
		
			SET @SqlCmd=CONCAT('INSERT INTO ',TBL_NAME,'
						SELECT * FROM ',@global_db,'.`tmp_update_regid_roamer_gsm_',WORKER_ID,'` ;');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
		
			SET @SqlCmd=CONCAT('DROP TABLE IF EXISTS ',@global_db,'.`tmp_update_regid_roamer_gsm_',WORKER_ID,'`;');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt; 
		END IF;
	ELSE 
		IF TECH_MASK =4 THEN 
			SET @SqlCmd=CONCAT('DROP TABLE IF EXISTS ',@global_db,'.`tmp_update_regid_ds_lte_',WORKER_ID,'`;');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt; 
		
			SET @SqlCmd =CONCAT('CREATE TABLE IF NOT EXISTS ',@global_db,'.`tmp_update_regid_ds_lte_',WORKER_ID,'` select * from ',TBL_NAME,' where 1<>1;');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd =CONCAT('INSERT INTO ',@global_db,'.`tmp_update_regid_ds_lte_',WORKER_ID,'`
						SELECT
						 ',DS_COLUMN_STR,'
						FROM ',TBL_NAME,' A LEFT JOIN ',@global_db,'.`table_region_tile_g',GROUP_ID,'` B
						ON A.`tile_id`=B.`tile_id` AND B.reg_level IN (1,2,3,0)
						;
			');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
		
			SET @SqlCmd=CONCAT('TRUNCATE TABLE ',TBL_NAME,';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
		
			SET @SqlCmd=CONCAT('INSERT INTO ',TBL_NAME,'
						SELECT * FROM ',@global_db,'.`tmp_update_regid_ds_lte_',WORKER_ID,'` ;');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
		
			SET @SqlCmd=CONCAT('DROP TABLE IF EXISTS ',@global_db,'.`tmp_update_regid_ds_lte_',WORKER_ID,'`;');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt; 
		END IF;
		IF TECH_MASK =2 THEN 
			SET @SqlCmd=CONCAT('DROP TABLE IF EXISTS ',@global_db,'.`tmp_update_regid_ds_umts_',WORKER_ID,'`;');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt; 
		
			SET @SqlCmd =CONCAT('CREATE TABLE IF NOT EXISTS ',@global_db,'.`tmp_update_regid_ds_umts_',WORKER_ID,'` select * from  ',TBL_NAME,' where 1<>1;');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd =CONCAT('INSERT INTO ',@global_db,'.`tmp_update_regid_ds_umts_',WORKER_ID,'` 
						SELECT
						 ',DS_COLUMN_STR,'
						FROM ',TBL_NAME,' A LEFT JOIN ',@global_db,'.`table_region_tile_g',GROUP_ID,'` B
						ON A.`tile_id`=B.`tile_id` AND B.reg_level IN (1,2,3,0)
						;
			');
	
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
		
			SET @SqlCmd=CONCAT('TRUNCATE TABLE ',TBL_NAME,';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
		
			SET @SqlCmd=CONCAT('INSERT INTO ',TBL_NAME,'
						SELECT * FROM ',@global_db,'.`tmp_update_regid_ds_umts_',WORKER_ID,'` ;');
						
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
		
			SET @SqlCmd=CONCAT('DROP TABLE IF EXISTS ',@global_db,'.`tmp_update_regid_ds_umts_',WORKER_ID,'`;');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;	
		END IF;
		IF TECH_MASK =1 THEN 
			SET @SqlCmd=CONCAT('DROP TABLE IF EXISTS ',@global_db,'.`tmp_update_regid_ds_gsm_',WORKER_ID,'`;');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt; 
		
			SET @SqlCmd =CONCAT('CREATE TABLE IF NOT EXISTS ',@global_db,'.`tmp_update_regid_ds_gsm_',WORKER_ID,'` select * from ',TBL_NAME,' where 1<>1;');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd =CONCAT('INSERT INTO ',@global_db,'.`tmp_update_regid_ds_gsm_',WORKER_ID,'`
						SELECT
						 ',DS_COLUMN_STR,'
						FROM ',TBL_NAME,' A LEFT JOIN ',@global_db,'.`table_region_tile_g',GROUP_ID,'` B
						ON A.`tile_id`=B.`tile_id` AND B.reg_level IN (1,2,3,0)
						;
			');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
		
			SET @SqlCmd=CONCAT('TRUNCATE TABLE ',TBL_NAME,';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
		
			SET @SqlCmd=CONCAT('INSERT INTO ',TBL_NAME,'
						SELECT * FROM ',@global_db,'.`tmp_update_regid_ds_gsm_',WORKER_ID,'` ;');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
		
			SET @SqlCmd=CONCAT('DROP TABLE IF EXISTS ',@global_db,'.`tmp_update_regid_ds_gsm_',WORKER_ID,'`;');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;	
		END IF;
	END IF;
	
	
	
/*		
		
		SET @SqlCmd=CONCAT('UPDATE ',TBL_NAME,' A,',@global_db,'.`table_region_tile_g',GROUP_ID,'` B
					SET A.REG_1_ID=B.REG_1_ID,
						A.REG_2_ID=B.REG_2_ID,
						A.REG_3_ID=B.REG_3_ID
					WHERE A.`tile_id`=B.`tile_id` AND B.reg_level IN (1,2,3);
			;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
*/		INSERT INTO gt_gw_main.SP_LOG VALUES(@global_db,'SP_Generate_Global_Statistic_Sub_Tile_Reg_Map',CONCAT(TBL_NAME,' UPDATE reg_id cost:',TIMESTAMPDIFF(SECOND,STEP_START_TIME,SYSDATE()),' sec.'), NOW());
		SET STEP_START_TIME := SYSDATE();
	END IF;
	
	INSERT INTO gt_gw_main.SP_LOG VALUES(@global_db,'SP_Generate_Global_Statistic_Sub_Tile_Reg_Map',CONCAT(TBL_NAME,',',WORKER_ID,'_',SUB_WORKER_ID,' Done: ',TIMESTAMPDIFF(SECOND,START_TIME,SYSDATE()),' seconds.'), NOW());
END$$
DELIMITER ;
