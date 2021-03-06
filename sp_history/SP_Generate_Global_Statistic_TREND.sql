DELIMITER $$
USE `gt_global_statistic`$$
DROP PROCEDURE IF EXISTS `SP_CreateDB_LTE`$$
CREATE DEFINER=`covmo`@`%` PROCEDURE `SP_Generate_Global_Statistic_TREND`(IN v_TECH_MASK TINYINT(2),IN FLAG TINYINT(2),IN exDate VARCHAR(10),IN RPT_TYPE VARCHAR(10))
BEGIN
	DECLARE START_TIME DATETIME DEFAULT SYSDATE();
	DECLARE STEP_START_TIME DATETIME DEFAULT SYSDATE();
	DECLARE SP_Process VARCHAR(100) DEFAULT NULL;
	DECLARE KPI_ERROR CONDITION FOR SQLSTATE '99999' ;
	DECLARE FIRSTDAY_OF_WEEK TINYINT(4) DEFAULT 6;
	DECLARE v_DATA_DATE VARCHAR(10) DEFAULT NULL;
	DECLARE v_DATA_HOUR TINYINT(2) DEFAULT NULL;
	DECLARE v_PU_ID MEDIUMINT(9) DEFAULT NULL;
	DECLARE v_group_db_name VARCHAR(100) DEFAULT '';
			
	DECLARE CELL_AGG_UMTS_COLUMN_STR VARCHAR(2000) DEFAULT 
		'`INIT_CALL_CNT`,
		`END_CALL_CNT`,
		`VOICE_CNT`,
		`VIDEO_CNT`,
		`PS_R99_CNT`,
		`PS_HSPA_CNT`,
		`M_RAB_CNT`,
		`SIGNAL_CNT`,
		`SMS_CNT`,
		`PS_OTHER_CNT`,
		`END_VOICE_CNT`,
		`END_VIDEO_CNT`,
		`END_PS_R99_CNT`,
		`END_PS_HSPA_CNT`,
		`END_M_RAB_CNT`,
		`END_SIGNAL_CNT`,
		`END_SMS_CNT`,
		`END_PS_OTHER_CNT`,
		`DROP_VOICE_CNT`,
		`DROP_VIDEO_CNT`,
		`DROP_PS_R99_CNT`,
		`DROP_PS_HSPA_CNT`,
		`DROP_M_RAB_CNT`,
		`DROP_SIGNAL_CNT`,
		`DROP_SMS_CNT`,
		`DROP_PS_OTHER_CNT`,
		`NON_BLOCK_VOICE_CNT`,
		`NON_BLOCK_VIDEO_CNT`,
		`NON_BLOCK_PS_R99_CNT`,
		`NON_BLOCK_PS_HSPA_CNT`,
		`NON_BLOCK_M_RAB_CNT`,
		`NON_BLOCK_SIGNAL_CNT`,
		`NON_BLOCK_SMS_CNT`,
		`NON_BLOCK_PS_OTHER_CNT`,
		`BLOCK_VOICE_CNT`,
		`BLOCK_VEDIO_CNT`,
		`BLOCK_R99_CNT`,
		`BLOCK_HSPA_CNT`,
		`BLOCK_MRAB_CNT`,
		`BLOCK_SIGNAL_CNT`,
		`BLOCK_SMS_CNT`,
		`BLOCK_PS_OTHER_CNT`,
		`RSCP_SUM`,
		`RSCP_CNT`,
		`ECN0_SUM`,
		`ECN0_CNT`,
		`UL_VOLUME_SUM`,
		`DL_VOLUME_SUM`,
		`UL_THROUPUT_MAX`,
		`DL_THROUPUT_MAX`,
		`UL_THROUPUT_SUM`,
		`UL_THROUPUT_CNT`,
		`DL_THROUPUT_SUM`,
		`DL_THROUPUT_CNT`,
		`FP_RSCP_1`,
		`FP_ECN0_1`,
		`BEST_CNT`,
		`CALL_SETUP_TIME_VOICE_SUM`,
		`CALL_SETUP_TIME_VOICE_CNT`,
		`CALL_SETUP_TIME_VEDIO_SUM`,
		`CALL_SETUP_TIME_VEDIO_CNT`,
		`CALL_SETUP_TIME_R99_SUM`,
		`CALL_SETUP_TIME_R99_CNT`,
		`CALL_SETUP_TIME_HSPA_SUM`,
		`CALL_SETUP_TIME_HSPA_CNT`,
		`CALL_SETUP_TIME_MRAB_SUM`,
		`CALL_SETUP_TIME_MRAB_CNT`,
		`CALL_SETUP_TIME_SIG_SUM`,
		`CALL_SETUP_TIME_SIG_CNT`,
		`CALL_SETUP_TIME_OTH_SUM`,
		`CALL_SETUP_TIME_OTH_CNT`,
		`CALL_SETUP_TIME_SMS_SUM`,
		`CALL_SETUP_TIME_SMS_CNT`,
		`CALL_SETUP_TIME_SUM`,
		`CALL_SETUP_TIME_CNT`,
		`SF_VOICE_CNT`,
		`SF_VEDIO_CNT`,
		`SF_R99_CNT`,
		`SF_HSPA_CNT`,
		`SF_MRAB_CNT`,
		`SF_OTHER_CNT`,
		`SF_SMS_CNT`,
		`SF_SIGNAL_CNT`,
		`CALL_DUR_SUM`,
		`VOICE_DUR_SUM`,
		`VIDEO_DUR_SUM`,
		`R99_DUR_SUM`,
		`HSPA_DUR_SUM`,
		`MRAB_DUR_SUM`';
	
	DECLARE TILE_UMTS_COLUMN_STR VARCHAR(1500) DEFAULT 
		'`INIT_CALL_CNT`,
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
		  `DL_THROUPUT_MAX`';	  
			
	DECLARE CELL_AGG_LTE_COLUMN_STR VARCHAR(2500) DEFAULT 
		'`INIT_CALL_CNT`,
		`END_CALL_CNT`,
		`SIGNAL_CNT`,
		`DATA_CNT`,
		`SMS_CNT`,
		`VOLTE_CNT`,
		`UNSPECIFIED_CNT`,
	
		`END_SIGNAL_CNT`,
		`END_DATA_CNT`,
		`END_SMS_CNT`,
		`END_VOLTE_CNT`,
		`END_UNSPECIFIED_CNT`,
	
		`DROP_SIGNAL_CNT`,
		`DROP_DATA_CNT`,
		`DROP_SMS_CNT`,
		`DROP_VOLTE_CNT`,
		`DROP_UNSPECIFIED_CNT`,
		`BLOCK_SIGNAL_CNT`,
		`BLOCK_DATA_CNT`,
		`BLOCK_SMS_CNT`,
		`BLOCK_VOLTE_CNT`,
		`BLOCK_UNSPECIFIED_CNT`,
		`CSFB_SIGNAL_CNT`,
		`CSFB_DATA_CNT`,
		`CSFB_SMS_CNT`,
		`CSFB_VOLTE_CNT`,
		`CSFB_UNSPECIFIED_CNT`,
		`NON_BLOCK_SIGNAL_CNT`,
		`NON_BLOCK_DATA_CNT`,
		`NON_BLOCK_SMS_CNT`,
		`NON_BLOCK_VOLTE_CNT`,
		`NON_BLOCK_UNSPECIFIED_CNT`,
		`RSRP_SUM`,
		`RSRP_CNT`,
		`RSRQ_SUM`,
		`RSRQ_CNT`,
		`UL_VOLUME_SUM`,
		`DL_VOLUME_SUM`,
		`UL_THROUPUT_MAX`,
		`DL_THROUPUT_MAX`,
		`UL_THROUPUT_SUM`,
		`UL_THROUPUT_CNT`,
		`DL_THROUPUT_SUM`,
		`DL_THROUPUT_CNT`,
		`INTER_FREQ_ATTEMPT_CNT`,
		`INTER_FREQ_FAILURE_CNT`,
		`INTRA_FREQ_ATTEMPT_CNT`,
		`INTRA_FREQ_FAILURE_CNT`,
		`4G_3G_ATTEMPT_CNT`,
		`4G_3G_FAILURE_CNT`,
		`4G_2G_ATTEMPT_CNT`,
		`4G_2G_FAILURE_CNT`,
		`MR_4G_RSRP_SERVING_SUM`,
		`MR_4G_RSRP_SERVING_CNT`,
		`MR_4G_RSRQ_SERVING_SUM`,
		`MR_4G_RSRQ_SERVING_CNT`,
		`CALL_SETUP_TIME_SUM`,
		`CALL_SETUP_TIME_CNT`,
		
		`CALL_SETUP_TIME_SIG_SUM`,
		`CALL_SETUP_TIME_SIG_CNT`,
		`CALL_SETUP_TIME_DATA_SUM`,
		`CALL_SETUP_TIME_DATA_CNT`,
		`CALL_SETUP_TIME_SMS_SUM`,
		`CALL_SETUP_TIME_SMS_CNT`,
		`CALL_SETUP_TIME_VOLTE_SUM`,
		`CALL_SETUP_TIME_VOLTE_CNT`,
		`CALL_SETUP_TIME_UNSP_SUM`,
		`CALL_SETUP_TIME_UNSP_CNT`,
	
		`SF_SIGNAL_CNT`,
		`SF_DATA_CNT`,
		`SF_SMS_CNT`,
		`SF_VOLTE_CNT`,
		`SF_UNSPECIFIED_CNT`,
		`SRVCC_ATTEMPT_CNT`,
		`SRVCC_FAILURE_CNT`,
		`S1_HO_ATTEMPT`,
		`S1_HO_FAILURE`,
		`X2_HO_ATTEMPT`,
		`X2_HO_FAILURE`,
		`LATENCY_SUM`,
		`LATENCY_CNT`';
	
	
	DECLARE TILE_LTE_COLUMN_STR VARCHAR(3000) DEFAULT 
		'`INIT_CALL_CNT`,
		`END_CALL_CNT`,
		`SIGNAL_CNT`,
		`DATA_CNT`,
		`UNSPECIFIED_CNT`,
		`CALL_DUR_SUM`,
		`BLOCK_CNT`,
		`DROP_CNT`,
		`CSFB_CNT`,
		`INTER_FREQ_ATTEMPT_CNT`,
		`INTER_FREQ_FAILURE_CNT`,
		`INTRA_FREQ_ATTEMPT_CNT`,
		`INTRA_FREQ_FAILURE_CNT`,
		`4G_3G_ATTEMPT_CNT`,
		`4G_3G_FAILURE_CNT`,
		`4G_2G_ATTEMPT_CNT`,
		`4G_2G_FAILURE_CNT`,
		`PS_UL_VOLUME_SUM`,
		`PS_DL_VOLUME_SUM`,
		`PS_UL_SPEED_MAX`,
		`PS_DL_SPEED_MAX`,
		`RSRP_SUM`,
		`RSRP_CNT`,
		`RSRQ_SUM`,
		`RSRQ_CNT`,
		`PILOT_DOMINANCE_SUM`,
		`PILOT_DOMINANCE_CNT`,
		`DROP_SIGNAL_CNT`,
		`DROP_DATA_CNT`,
		`DROP_SMS_CNT`,
		`DROP_VOLTE_CNT`,
		`DROP_UNSPECIFIED_CNT`,
		`END_NON_BLOCK_CALL_CNT`,
		`UL_THROUPUT_SUM`,
		`UL_THROUPUT_CNT`,
		`DL_THROUPUT_SUM`,
		`DL_THROUPUT_CNT`,
		`END_NON_BLOCK_SIGNAL_CNT`,
		`END_NON_BLOCK_DATA_CNT`,
		`MR_4G_RSRP_SERVING_SUM`,
		`MR_4G_RSRP_SERVING_CNT`,
		`MR_4G_RSRQ_SERVING_SUM`,
		`MR_4G_RSRQ_SERVING_CNT`';
	
	DECLARE CELL_AGG_GSM_COLUMN_STR VARCHAR(1500) DEFAULT 
		'`INIT_CALL_CNT`,
		`END_CALL_CNT`,
		`VOICE_CNT`,
		`SIGNAL_CNT`,
		`SMS_CNT`,
		`GPRS_CNT`,
		`OTHER_CNT`,
	
		`END_VOICE_CNT`,
		`END_SIGNAL_CNT`,
		`END_SMS_CNT`,
		`END_GPRS_CNT`,
		`END_OTHER_CNT`,
	
		`BLOCK_VOICE_CNT`,
		`BLOCK_GPRS_CNT`,
		`BLOCK_SMS_CNT`,
		`BLOCK_SIGNAL_CNT`,
		`BLOCK_OTHER_CNT`,
		`DROP_VOICE_CNT`,
		`DROP_SIGNAL_CNT`,
		`DROP_SMS_CNT`,
		`DROP_GPRS_CNT`,
		`DROP_OTHER_CNT`,
		`NON_BLOCK_VOICE_CNT`,
		`NON_BLOCK_SIGNAL_CNT`,
		`NON_BLOCK_SMS_CNT`,
		`NON_BLOCK_GPRS_CNT`,
		`NON_BLOCK_OTHER_CNT`,
		`RXLEV_SUM`,
		`RXLEV_CNT`,
		`RXQUAL_SUM`,
		`RXQUAL_CNT`,
		`CALL_SETUP_TIME_SUM`,
		`CALL_SETUP_TIME_CNT`,
		`CALL_SETUP_TIME_VOICE_SUM`,
		`CALL_SETUP_TIME_VOICE_CNT`,
		`CALL_SETUP_TIME_SIG_SUM`,
		`CALL_SETUP_TIME_SIG_CNT`,
		`CALL_SETUP_TIME_SMS_SUM`,
		`CALL_SETUP_TIME_SMS_CNT`,
		`CALL_SETUP_TIME_GPRS_SUM`,
		`CALL_SETUP_TIME_GPRS_CNT`,	
		`CALL_SETUP_TIME_OTH_SUM`,
		`CALL_SETUP_TIME_OTH_CNT`,
		`SF_VOICE_CNT`,
		`SF_DATA_CNT`,
		`SF_SMS_CNT`,
		`SF_SIGNAL_CNT`,
		`SF_OTHER_CNT`,
		`CALL_DUR_SUM`,
		`VOICE_DUR_SUM`,
		`DATA_DUR_SUM`';
	
	DECLARE TILE_GSM_COLUMN_STR VARCHAR(1500) DEFAULT 
		'INIT_CALL_CNT, 
		  END_CALL_CNT,
		  VOICE_CNT,
		  SIGNAL_CNT,
		  SMS_CNT,
		  GPRS_CNT,
		  OTHER_CNT,
		  BLOCK_CNT,
		  DROP_VOICE_CNT,
		  DROP_SIGNAL_CNT,
		  DROP_SMS_CNT,
		  DROP_GPRS_CNT,
		  DROP_OTHER_CNT,
		  NON_BLOCK_VOICE_CNT,
		  NON_BLOCK_SIGNAL_CNT,
		  NON_BLOCK_SMS_CNT,
		  NON_BLOCK_GPRS_CNT,
		  NON_BLOCK_OTHER_CNT,
		  CALL_DUR_SUM,
		  RXLEV_SUM,
		  RXLEV_CNT,
		  RXQUAL_SUM,
		  RXQUAL_CNT';
	
	SET @global_db='gt_global_statistic';
				
	INSERT INTO gt_gw_main.SP_LOG VALUES(@global_db,'SP_Generate_Global_Statistic_TREND','START ', START_TIME);
	SET STEP_START_TIME := exDate;	
		
	SET @SqlCmd=CONCAT('SELECT GROUP_CONCAT(DISTINCT `group_id` ORDER BY `group_id` SEPARATOR ''|'' ) INTO @REG_GROUP FROM ',@global_db,'.`usr_polygon_reg_3`;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;	
		
	SET @v_reg_m=1;
	SET @v_reg_Max=gt_covmo_csv_count(@REG_GROUP,'|');
	WHILE @v_reg_m <= @v_reg_Max DO
	BEGIN
		SET v_group_db_name=CONCAT('gt_global_statistic_g',gt_strtok(@REG_GROUP, @v_reg_m, '|'));
				
		SET v_DATA_DATE:=exDate;
	
	
		SELECT `DAY_OF_WEEK`(v_DATA_DATE) INTO @DAY_OF_WEEK;
		
		SET @FIRST_DAY=gt_strtok(@DAY_OF_WEEK, 1, '|');
		SET @END_DAY=gt_strtok(@DAY_OF_WEEK, 2, '|');
		SET @DATE_WK=CONCAT(DATE_FORMAT(@FIRST_DAY,'%Y%m%d'),'_',DATE_FORMAT(@END_DAY,'%Y%m%d'));
		SET @DATE_MN=DATE_FORMAT(v_DATA_DATE,'%Y%m');
		SET @DATE_DY=DATE_FORMAT(v_DATA_DATE,'%Y%m%d');
		SET @EX_DATE=CONCAT(DATE_FORMAT(v_DATA_DATE,'%Y%m%d_'),v_DATA_HOUR);
		
		SET @FIRST_DAY_WK=DATE_FORMAT(@FIRST_DAY,'%Y%m%d');
		SET @END_DAY_WK=DATE_FORMAT(@END_DAY,'%Y%m%d');
			
		SET @FIRST_DAY_MN=DATE(CONCAT(YEAR(v_DATA_DATE),'-',MONTH(v_DATA_DATE),'-','01'));
		SET @FIRST_DAY_MN=DATE_FORMAT(@FIRST_DAY_MN,'%Y%m%d');		
		SET @END_DAY_MN=LAST_DAY(v_DATA_DATE);	
		SET @END_DAY_MN=DATE_FORMAT(@END_DAY_MN,'%Y%m%d');
	
		SET @WHERE_DAY=DATE_FORMAT(v_DATA_DATE,'%Y-%m-%d');
		SET @WHERE_FIRST_DAY_WK=DATE_FORMAT(@FIRST_DAY_WK,'%Y-%m-%d');
		SET @WHERE_END_DAY_WK=DATE_FORMAT(@END_DAY_WK,'%Y-%m-%d');
		SET @WHERE_FIRST_DAY_MN=DATE(CONCAT(YEAR(v_DATA_DATE),'-',MONTH(v_DATA_DATE),'-','01'));
		SET @WHERE_END_DAY_MN=DATE_FORMAT(@END_DAY_MN,'%Y-%m-%d');
		
		SET @table_cell_agg_umts_hr=CONCAT(v_group_db_name,'.table_cell_agg_umts_hr_',@DATE_DY);
		SET @table_cell_agg_umts_dy=CONCAT(v_group_db_name,'.table_cell_agg_umts_dy_',@DATE_DY);
		SET @table_cell_agg_umts_wk=CONCAT(v_group_db_name,'.table_cell_agg_umts_wk_',@DATE_WK);
		SET @table_cell_agg_umts_mn=CONCAT(v_group_db_name,'.table_cell_agg_umts_mn_',@DATE_MN);			
		
		SET @table_cell_agg_lte_hr=CONCAT(v_group_db_name,'.table_cell_agg_lte_hr_',@DATE_DY);
		SET @table_cell_agg_lte_dy=CONCAT(v_group_db_name,'.table_cell_agg_lte_dy_',@DATE_DY);
		SET @table_cell_agg_lte_wk=CONCAT(v_group_db_name,'.table_cell_agg_lte_wk_',@DATE_WK);
		SET @table_cell_agg_lte_mn=CONCAT(v_group_db_name,'.table_cell_agg_lte_mn_',@DATE_MN);
		
		SET @table_cell_agg_gsm_hr=CONCAT(v_group_db_name,'.table_cell_agg_gsm_hr_',@DATE_DY);
		SET @table_cell_agg_gsm_dy=CONCAT(v_group_db_name,'.table_cell_agg_gsm_dy_',@DATE_DY);
		SET @table_cell_agg_gsm_wk=CONCAT(v_group_db_name,'.table_cell_agg_gsm_wk_',@DATE_WK);
		SET @table_cell_agg_gsm_mn=CONCAT(v_group_db_name,'.table_cell_agg_gsm_mn_',@DATE_MN);
		
		SET @table_reg_3_umts_hr=CONCAT(v_group_db_name,'.table_reg_3_umts_hr_',@DATE_DY);
		SET @table_reg_2_umts_hr=CONCAT(v_group_db_name,'.table_reg_2_umts_hr_',@DATE_DY);
		SET @table_reg_1_umts_hr=CONCAT(v_group_db_name,'.table_reg_1_umts_hr_',@DATE_DY);
		SET @table_reg_3_umts_dy=CONCAT(v_group_db_name,'.table_reg_3_umts_dy_',@DATE_DY);
		SET @table_reg_2_umts_dy=CONCAT(v_group_db_name,'.table_reg_2_umts_dy_',@DATE_DY);
		SET @table_reg_1_umts_dy=CONCAT(v_group_db_name,'.table_reg_1_umts_dy_',@DATE_DY);
		SET @table_reg_3_umts_wk=CONCAT(v_group_db_name,'.table_reg_3_umts_wk_',@DATE_WK);
		SET @table_reg_2_umts_wk=CONCAT(v_group_db_name,'.table_reg_2_umts_wk_',@DATE_WK);
		SET @table_reg_1_umts_wk=CONCAT(v_group_db_name,'.table_reg_1_umts_wk_',@DATE_WK);
		SET @table_reg_3_umts_mn=CONCAT(v_group_db_name,'.table_reg_3_umts_mn_',@DATE_MN);
		SET @table_reg_2_umts_mn=CONCAT(v_group_db_name,'.table_reg_2_umts_mn_',@DATE_MN);
		SET @table_reg_1_umts_mn=CONCAT(v_group_db_name,'.table_reg_1_umts_mn_',@DATE_MN);	
		
		SET @table_reg_3_lte_hr=CONCAT(v_group_db_name,'.table_reg_3_lte_hr_',@DATE_DY);
		SET @table_reg_2_lte_hr=CONCAT(v_group_db_name,'.table_reg_2_lte_hr_',@DATE_DY);
		SET @table_reg_1_lte_hr=CONCAT(v_group_db_name,'.table_reg_1_lte_hr_',@DATE_DY);
		SET @table_reg_3_lte_dy=CONCAT(v_group_db_name,'.table_reg_3_lte_dy_',@DATE_DY);
		SET @table_reg_2_lte_dy=CONCAT(v_group_db_name,'.table_reg_2_lte_dy_',@DATE_DY);
		SET @table_reg_1_lte_dy=CONCAT(v_group_db_name,'.table_reg_1_lte_dy_',@DATE_DY);
		SET @table_reg_3_lte_wk=CONCAT(v_group_db_name,'.table_reg_3_lte_wk_',@DATE_WK);
		SET @table_reg_2_lte_wk=CONCAT(v_group_db_name,'.table_reg_2_lte_wk_',@DATE_WK);
		SET @table_reg_1_lte_wk=CONCAT(v_group_db_name,'.table_reg_1_lte_wk_',@DATE_WK);
		SET @table_reg_3_lte_mn=CONCAT(v_group_db_name,'.table_reg_3_lte_mn_',@DATE_MN);
		SET @table_reg_2_lte_mn=CONCAT(v_group_db_name,'.table_reg_2_lte_mn_',@DATE_MN);
		SET @table_reg_1_lte_mn=CONCAT(v_group_db_name,'.table_reg_1_lte_mn_',@DATE_MN);
		SET @table_reg_3_gsm_hr=CONCAT(v_group_db_name,'.table_reg_3_gsm_hr_',@DATE_DY);
		SET @table_reg_2_gsm_hr=CONCAT(v_group_db_name,'.table_reg_2_gsm_hr_',@DATE_DY);
		SET @table_reg_1_gsm_hr=CONCAT(v_group_db_name,'.table_reg_1_gsm_hr_',@DATE_DY);
		SET @table_reg_3_gsm_dy=CONCAT(v_group_db_name,'.table_reg_3_gsm_dy_',@DATE_DY);
		SET @table_reg_2_gsm_dy=CONCAT(v_group_db_name,'.table_reg_2_gsm_dy_',@DATE_DY);
		SET @table_reg_1_gsm_dy=CONCAT(v_group_db_name,'.table_reg_1_gsm_dy_',@DATE_DY);
		SET @table_reg_3_gsm_wk=CONCAT(v_group_db_name,'.table_reg_3_gsm_wk_',@DATE_WK);
		SET @table_reg_2_gsm_wk=CONCAT(v_group_db_name,'.table_reg_2_gsm_wk_',@DATE_WK);
		SET @table_reg_1_gsm_wk=CONCAT(v_group_db_name,'.table_reg_1_gsm_wk_',@DATE_WK);
		SET @table_reg_3_gsm_mn=CONCAT(v_group_db_name,'.table_reg_3_gsm_mn_',@DATE_MN);
		SET @table_reg_2_gsm_mn=CONCAT(v_group_db_name,'.table_reg_2_gsm_mn_',@DATE_MN);
		SET @table_reg_1_gsm_mn=CONCAT(v_group_db_name,'.table_reg_1_gsm_mn_',@DATE_MN);
				 
	IF RPT_TYPE='CELL' THEN
		IF v_TECH_MASK= 2 THEN 
			IF FLAG= 2 THEN 
				INSERT INTO gt_gw_main.SP_LOG VALUES(@global_db,'SP_Generate_Global_Statistic_TREND','INSERT INTO table_cell_agg_umts_daily', START_TIME);
					
				SET @SqlCmd=CONCAT('SELECT COUNT(*) INTO @P_CELL_AGG_UMTS_DY FROM information_schema.`PARTITIONS`
						WHERE TABLE_SCHEMA=''',v_group_db_name,''' 
						AND TABLE_NAME=''table_cell_agg_umts_daily'' AND PARTITION_NAME= ''p',@DATE_DY,''';');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
	
				IF @P_CELL_AGG_UMTS_DY=0 THEN
				SET @SqlCmd=CONCAT('ALTER TABLE ',v_group_db_name,'.table_cell_agg_umts_daily ADD PARTITION (PARTITION p',@DATE_DY,' VALUES IN (TO_DAYS(''',STEP_START_TIME,''')));');
				
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				END IF;
			
				SET @SqlCmd=CONCAT('ALTER TABLE ',v_group_db_name,'.table_cell_agg_umts_daily 
							TRUNCATE PARTITION p',@DATE_DY,'
						   ;');	
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
	
				SET @SqlCmd=CONCAT('INSERT INTO ',v_group_db_name,'.table_cell_agg_umts_daily 
							(  `DATA_DATE`,	
							   `CELL_ID`,
								`SITE_ID`,
								`CLUSTER_ID`,
								`RNC_ID`,
								`FREQUENCY`,
								`UARFCN`,
								`CELL_NAME`,
							',CELL_AGG_umts_COLUMN_STR,')
							SELECT
							',@DATE_DY,',	
							`CELL_ID`,
							`SITE_ID`,
							`CLUSTER_ID`,
							`RNC_ID`,
							`FREQUENCY`,
							`UARFCN`,
							`CELL_NAME`,
							',CELL_AGG_umts_COLUMN_STR,'
							FROM ',@table_cell_agg_umts_dy,'
							
						;');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				
				SET @SqlCmd=CONCAT('REPLACE INTO ',@global_db,'.table_call_cnt_history_trend 
							(  `START_DATE`,
							   `END_DATE`,
							   `TECH_MASK`,
							   `CREATE_TIME`,
							   `FLAG`,
						           `RPT_TYPE`
							)
							SELECT
							',@DATE_DY,',	
							',@DATE_DY,',
							',v_TECH_MASK,',
							 ''',NOW(),''' AS `CREATE_TIME`,
							''DAILY'',
							''CELL''
							
						;');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				
			END IF;
			
			IF FLAG=3 THEN 
				INSERT INTO gt_gw_main.SP_LOG VALUES(@global_db,'SP_Generate_Global_Statistic_TREND','INSERT INTO table_cell_agg_umts_weekly', START_TIME);
				
				SET @SqlCmd=CONCAT('SELECT COUNT(*) INTO @P_CELL_AGG_UMTS_WK FROM information_schema.`PARTITIONS`
							WHERE TABLE_SCHEMA=''',v_group_db_name,''' 
							AND TABLE_NAME=''table_cell_agg_umts_weekly'' AND PARTITION_NAME= ''p',@FIRST_DAY_WK,''';');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
			
				IF @P_CELL_AGG_UMTS_WK=0 THEN
	
				SET @SqlCmd=CONCAT('ALTER TABLE ',v_group_db_name,'.table_cell_agg_umts_weekly ADD PARTITION (PARTITION p',@FIRST_DAY_WK,' VALUES IN (TO_DAYS(''',@FIRST_DAY_WK,''')));');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				END IF;
				
				SET @SqlCmd=CONCAT('ALTER TABLE ',v_group_db_name,'.table_cell_agg_umts_weekly 
							TRUNCATE  PARTITION p',@FIRST_DAY_WK,'
						   ;');
						
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
			
				SET @SqlCmd=CONCAT('INSERT INTO ',v_group_db_name,'.table_cell_agg_umts_weekly 
							(  `START_DATE`,
							   `END_DATE`,
								`CELL_ID`,
								`SITE_ID`,
								`CLUSTER_ID`,
								`RNC_ID`,
								`FREQUENCY`,
								`UARFCN`,
								`CELL_NAME`,
							',CELL_AGG_umts_COLUMN_STR,')
							SELECT
							',@FIRST_DAY_WK,',	 
							',@END_DAY_WK,',
							`CELL_ID`,
								`SITE_ID`,
								`CLUSTER_ID`,
								`RNC_ID`,
								`FREQUENCY`,
								`UARFCN`,
								`CELL_NAME`,
							',CELL_AGG_umts_COLUMN_STR,'
							FROM ',@table_cell_agg_umts_wk,'
							
							  
						;');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				
				SET @SqlCmd=CONCAT('REPLACE INTO ',@global_db,'.table_call_cnt_history_trend 
							(  `START_DATE`,
							   `END_DATE`,
							   `TECH_MASK`,
							   `CREATE_TIME`,
							   `FLAG`,
						           `RPT_TYPE`
							)
							SELECT
							',@FIRST_DAY_WK,',	
							',@END_DAY_WK,',
							',v_TECH_MASK,',
							 ''',NOW(),''' AS `CREATE_TIME`,
							''WEEKLY'',
							''CELL''
						;');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
			END IF;
			IF FLAG=4 THEN 
				INSERT INTO gt_gw_main.SP_LOG VALUES(@global_db,'SP_Generate_Global_Statistic_TREND','INSERT INTO table_cell_agg_umts_monthly', START_TIME);
				
				SET @SqlCmd=CONCAT('SELECT COUNT(*) INTO @P_CELL_AGG_UMTS_MN FROM information_schema.`PARTITIONS`
							WHERE TABLE_SCHEMA=''',v_group_db_name,''' 
							AND TABLE_NAME=''table_cell_agg_umts_monthly'' AND PARTITION_NAME= ''p',@FIRST_DAY_MN,''';');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				
				IF @P_CELL_AGG_UMTS_MN=0 THEN 
	
				SET @SqlCmd=CONCAT('ALTER TABLE ',v_group_db_name,'.table_cell_agg_umts_monthly ADD PARTITION (PARTITION p',@FIRST_DAY_MN,' VALUES IN (TO_DAYS(''',@FIRST_DAY_MN,''')));');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				END IF;
			
				SET @SqlCmd=CONCAT('ALTER TABLE ',v_group_db_name,'.table_cell_agg_umts_monthly 
							TRUNCATE PARTITION p',@FIRST_DAY_MN,'
						   ;');
						
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				SET @SqlCmd=CONCAT('INSERT INTO ',v_group_db_name,'.table_cell_agg_umts_monthly 
							(  `START_DATE`,
							   `END_DATE`,
								`CELL_ID`,
								`SITE_ID`,
								`CLUSTER_ID`,
								`RNC_ID`,
								`FREQUENCY`,
								`UARFCN`,
								`CELL_NAME`,
							',CELL_AGG_umts_COLUMN_STR,')
							SELECT
							',@FIRST_DAY_MN,',	 
							',@END_DAY_MN,',
							`CELL_ID`,
								`SITE_ID`,
								`CLUSTER_ID`,
								`RNC_ID`,
								`FREQUENCY`,
								`UARFCN`,
								`CELL_NAME`,
							',CELL_AGG_umts_COLUMN_STR,'
							FROM ',@table_cell_agg_umts_mn,'
							
						;');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				
				SET @SqlCmd=CONCAT('REPLACE INTO ',@global_db,'.table_call_cnt_history_trend 
							(  `START_DATE`,
							   `END_DATE`,
							   `TECH_MASK`,
							   `CREATE_TIME`,
							   `FLAG`,
						           `RPT_TYPE`
							)
							SELECT
							',@FIRST_DAY_MN,',	
							',@END_DAY_MN,',
							',v_TECH_MASK,',
							 ''',NOW(),''' AS `CREATE_TIME`,
							''MONTHLY'',
							''CELL''
						;');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
			END IF;
		END IF;
		IF v_TECH_MASK= 4 THEN 
			IF FLAG= 2 THEN 
				INSERT INTO gt_gw_main.SP_LOG VALUES(@global_db,'SP_Generate_Global_Statistic_TREND','INSERT INTO table_cell_agg_LTE_daily', START_TIME);
			
				SET @SqlCmd=CONCAT('SELECT COUNT(*) INTO @P_CELL_AGG_LTE_DY FROM information_schema.`PARTITIONS`
							WHERE TABLE_SCHEMA=''',v_group_db_name,''' 
							AND TABLE_NAME=''table_cell_agg_LTE_daily'' AND PARTITION_NAME= ''p',@DATE_DY,''';');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
			
				IF @P_CELL_AGG_LTE_DY=0 THEN
				SET @SqlCmd=CONCAT('ALTER TABLE ',v_group_db_name,'.table_cell_agg_LTE_daily ADD PARTITION (PARTITION p',@DATE_DY,' VALUES IN (TO_DAYS(''',@DATE_DY,''')));');	
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				END IF;
	
				SET @SqlCmd=CONCAT('ALTER TABLE ',v_group_db_name,'.table_cell_agg_LTE_daily 
							TRUNCATE PARTITION p',@DATE_DY,'
						   ;');		
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
			
				SET @SqlCmd=CONCAT('INSERT INTO ',v_group_db_name,'.table_cell_agg_LTE_daily 
							(  `DATA_DATE`,	
							   `CELL_ID`,
							   `ENODEB_ID`,
						           `PU_ID`,
							   `EARFCN`,
							   `EUTRABAND`,
							   `CELL_NAME`,
							',CELL_AGG_LTE_COLUMN_STR,')
							SELECT
							',@DATE_DY,',	
							`CELL_ID`,
							   `ENODEB_ID`,
						           `PU_ID`,
							   `EARFCN`,
							   `EUTRABAND`,
							   `CELL_NAME`,
							',CELL_AGG_LTE_COLUMN_STR,'
							FROM ',@table_cell_agg_LTE_dy,'
							
						;');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
	
				SET @SqlCmd=CONCAT('REPLACE INTO ',@global_db,'.table_call_cnt_history_trend 
							(  `START_DATE`,
							   `END_DATE`,
							   `TECH_MASK`,
							   `CREATE_TIME`,
							   `FLAG`,
						           `RPT_TYPE`
							)
							SELECT
							',@DATE_DY,',	
							',@DATE_DY,',
							',v_TECH_MASK,',
							 ''',NOW(),''' AS `CREATE_TIME`,
							''DAILY'',
							''CELL''
						;');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
			END IF;
			IF FLAG= 3 THEN
				INSERT INTO gt_gw_main.SP_LOG VALUES(@global_db,'SP_Generate_Global_Statistic_TREND','INSERT INTO table_cell_agg_LTE_weekly', START_TIME);
				
				SET @SqlCmd=CONCAT('SELECT COUNT(*) INTO @P_CELL_AGG_LTE_WK FROM information_schema.`PARTITIONS`
							WHERE TABLE_SCHEMA=''',v_group_db_name,''' 
							AND TABLE_NAME=''table_cell_agg_LTE_weekly'' AND PARTITION_NAME= ''p',@FIRST_DAY_WK,''';');
				
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
			
				IF @P_CELL_AGG_LTE_WK=0 THEN
				SET @SqlCmd=CONCAT('ALTER TABLE ',v_group_db_name,'.table_cell_agg_LTE_weekly ADD PARTITION (PARTITION p',@FIRST_DAY_WK,' VALUES IN (TO_DAYS(''',@FIRST_DAY_WK,''')));');
				
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				END IF;
	
				SET @SqlCmd=CONCAT('ALTER TABLE ',v_group_db_name,'.table_cell_agg_LTE_weekly 
							TRUNCATE PARTITION p',@FIRST_DAY_WK,'
						   ;');
					
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
			
				SET @SqlCmd=CONCAT('INSERT INTO ',v_group_db_name,'.table_cell_agg_LTE_weekly 
							(  `START_DATE`,
							   `END_DATE`,
							   `CELL_ID`,
							   `ENODEB_ID`,
						           `PU_ID`,
							   `EARFCN`,
							   `EUTRABAND`,
							   `CELL_NAME`,
							',CELL_AGG_LTE_COLUMN_STR,')
							SELECT
							',@FIRST_DAY_WK,',	 
							',@END_DAY_WK,',
							`CELL_ID`,
							   `ENODEB_ID`,
						           `PU_ID`,
							   `EARFCN`,
							   `EUTRABAND`,
							   `CELL_NAME`,
							',CELL_AGG_LTE_COLUMN_STR,'
							FROM ',@table_cell_agg_LTE_wk,'
							
						;');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
	
				SET @SqlCmd=CONCAT('REPLACE INTO ',@global_db,'.table_call_cnt_history_trend 
							(  `START_DATE`,
							   `END_DATE`,
							   `TECH_MASK`,
							   `CREATE_TIME`,
							   `FLAG`,
						           `RPT_TYPE`
							)
							SELECT
							',@FIRST_DAY_WK,',	
							',@END_DAY_WK,',
							',v_TECH_MASK,',
							 ''',NOW(),''' AS `CREATE_TIME`,
							''WEEKLY'',
							''CELL''
						;');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
			END IF;
			IF FLAG =4 THEN 
				INSERT INTO gt_gw_main.SP_LOG VALUES(@global_db,'SP_Generate_Global_Statistic_TREND','INSERT INTO table_cell_agg_LTE_monthly', START_TIME);
				
				SET @SqlCmd=CONCAT('SELECT COUNT(*) INTO @P_CELL_AGG_LTE_MN FROM information_schema.`PARTITIONS`
							WHERE TABLE_SCHEMA=''',v_group_db_name,''' 
							AND TABLE_NAME=''table_cell_agg_LTE_monthly'' AND PARTITION_NAME= ''p',@FIRST_DAY_MN,''';');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				
				IF @P_CELL_AGG_LTE_MN=0 THEN 
				SET @SqlCmd=CONCAT('ALTER TABLE ',v_group_db_name,'.table_cell_agg_LTE_monthly ADD PARTITION (PARTITION p',@FIRST_DAY_MN,' VALUES IN (TO_DAYS(''',@FIRST_DAY_MN,''')));');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				END IF;
	
				SET @SqlCmd=CONCAT('ALTER TABLE ',v_group_db_name,'.table_cell_agg_LTE_monthly 
							TRUNCATE PARTITION p',@FIRST_DAY_MN,'
						   ;');
						
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
	
				SET @SqlCmd=CONCAT('INSERT INTO ',v_group_db_name,'.table_cell_agg_LTE_monthly 
							(  `START_DATE`,
							   `END_DATE`,
							   `CELL_ID`,
							   `ENODEB_ID`,
						           `PU_ID`,
							   `EARFCN`,
							   `EUTRABAND`,
							   `CELL_NAME`,
							',CELL_AGG_LTE_COLUMN_STR,')
							SELECT
							',@FIRST_DAY_MN,',	 
							',@END_DAY_MN,',
							`CELL_ID`,
							   `ENODEB_ID`,
						           `PU_ID`,
							   `EARFCN`,
							   `EUTRABAND`,
							   `CELL_NAME`,
							',CELL_AGG_LTE_COLUMN_STR,'
							FROM ',@table_cell_agg_LTE_mn,'
							
						;');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				
				SET @SqlCmd=CONCAT('REPLACE INTO ',@global_db,'.table_call_cnt_history_trend 
							(  `START_DATE`,
							   `END_DATE`,
							   `TECH_MASK`,
							   `CREATE_TIME`,
							   `FLAG`,
						           `RPT_TYPE`
							)
							SELECT
							',@FIRST_DAY_MN,',	
							',@END_DAY_MN,',
							',v_TECH_MASK,',
							 ''',NOW(),''' AS `CREATE_TIME`,
							''MONTHLY'',
							''CELL''
						;');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
			END IF;
		END IF;
	END IF;
	
	IF RPT_TYPE='TILE' THEN	
		IF v_TECH_MASK= 2 THEN 
			IF FLAG= 2 THEN					
				INSERT INTO gt_gw_main.SP_LOG VALUES(@global_db,'SP_Generate_Global_Statistic_TREND','INSERT INTO table_reg_3_umts_daily', START_TIME);
				
				SET @SqlCmd=CONCAT('SELECT COUNT(*) INTO @P_REG_3_UMTS_DY FROM information_schema.`PARTITIONS`
							WHERE TABLE_SCHEMA=''',v_group_db_name,''' 
							AND TABLE_NAME=''table_reg_3_umts_daily'' AND PARTITION_NAME= ''p',@DATE_DY,''';');
				
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
			
				IF @P_REG_3_UMTS_DY=0 THEN
				SET @SqlCmd=CONCAT('ALTER TABLE ',v_group_db_name,'.table_reg_3_umts_daily ADD PARTITION (PARTITION p',@DATE_DY,' VALUES IN (TO_DAYS(''',@DATE_DY,''')));');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				END IF;
				
				SET @SqlCmd=CONCAT('ALTER TABLE ',v_group_db_name,'.table_reg_3_umts_daily 
							TRUNCATE PARTITION p',@DATE_DY,'
						   ;');
						
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
	
				SET @SqlCmd=CONCAT('INSERT INTO ',v_group_db_name,'.table_reg_3_umts_daily 
							(  `DATA_DATE`,	
							`REG_ID`,
							`FREQUENCY`,
							`UARFCN`,
							',TILE_UMTS_COLUMN_STR,')
						   SELECT
							',@DATE_DY,',
							`REG_ID`,
							`FREQUENCY`,
							`UARFCN`,
							',TILE_UMTS_COLUMN_STR,'
						   FROM ',@table_reg_3_umts_dy,'
						
						;');
				
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				INSERT INTO gt_gw_main.SP_LOG VALUES(@global_db,'SP_Generate_Global_Statistic_TREND','INSERT INTO table_reg_2_umts_daily', START_TIME);
			
				SET @SqlCmd=CONCAT('SELECT COUNT(*) INTO @P_REG_2_UMTS_DY FROM information_schema.`PARTITIONS`
							WHERE TABLE_SCHEMA=''',v_group_db_name,''' 
							AND TABLE_NAME=''table_reg_2_umts_daily'' AND PARTITION_NAME= ''p',@DATE_DY,''';');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
			
				IF @P_REG_2_UMTS_DY=0 THEN
				SET @SqlCmd=CONCAT('ALTER TABLE ',v_group_db_name,'.table_reg_2_umts_daily ADD PARTITION (PARTITION p',@DATE_DY,' VALUES IN (TO_DAYS(''',@DATE_DY,''')));');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				END IF;
	
				SET @SqlCmd=CONCAT('ALTER TABLE ',v_group_db_name,'.table_reg_2_umts_daily 
							TRUNCATE PARTITION p',@DATE_DY,'
						   ;');
						
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
			
				SET @SqlCmd=CONCAT('INSERT INTO ',v_group_db_name,'.table_reg_2_umts_daily 
							(  `DATA_DATE`,	
								`REG_ID`,
							`FREQUENCY`,
							`UARFCN`,
							',TILE_UMTS_COLUMN_STR,')
						   SELECT
							',@DATE_DY,',	 
							`REG_ID`,
							`FREQUENCY`,
							`UARFCN`,
							',TILE_UMTS_COLUMN_STR,'
						   FROM ',@table_reg_2_umts_dy,'
						  
						;');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				INSERT INTO gt_gw_main.SP_LOG VALUES(@global_db,'SP_Generate_Global_Statistic_TREND','INSERT INTO table_reg_1_umts_daily', START_TIME);
			
				SET @SqlCmd=CONCAT('SELECT COUNT(*) INTO @P_REG_1_UMTS_DY FROM information_schema.`PARTITIONS`
							WHERE TABLE_SCHEMA=''',v_group_db_name,''' 
							AND TABLE_NAME=''table_reg_1_umts_daily'' AND PARTITION_NAME= ''p',@DATE_DY,''';');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
			
				IF @P_REG_1_UMTS_DY=0 THEN
				SET @SqlCmd=CONCAT('ALTER TABLE ',v_group_db_name,'.table_reg_1_umts_daily ADD PARTITION (PARTITION p',@DATE_DY,' VALUES IN (TO_DAYS(''',@DATE_DY,''')));');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				END IF;
	
				SET @SqlCmd=CONCAT('ALTER TABLE ',v_group_db_name,'.table_reg_1_umts_daily 
							TRUNCATE PARTITION p',@DATE_DY,'
						   ;');
						
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
			
				SET @SqlCmd=CONCAT('INSERT INTO ',v_group_db_name,'.table_reg_1_umts_daily 
							(  `DATA_DATE`,	
								`REG_ID`,
							`FREQUENCY`,
							`UARFCN`,
							',TILE_UMTS_COLUMN_STR,')
						   SELECT
							',@DATE_DY,',
							`REG_ID`,
							`FREQUENCY`,
							`UARFCN`,
							',TILE_UMTS_COLUMN_STR,'
						   FROM ',@table_reg_1_umts_dy,'
						  
						;');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
			
				SET @SqlCmd=CONCAT('REPLACE INTO ',@global_db,'.table_call_cnt_history_trend 
							(  `START_DATE`,
							   `END_DATE`,
							   `TECH_MASK`,
							   `CREATE_TIME`,
							   `FLAG`,
						           `RPT_TYPE`
							)
							SELECT
							',@DATE_DY,',	
							',@DATE_DY,',
							',v_TECH_MASK,',
							 ''',NOW(),''' AS `CREATE_TIME`,
							''DAILY'',
							''TILE''
						;');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
			END IF;
			IF FLAG=3 THEN 
				INSERT INTO gt_gw_main.SP_LOG VALUES(@global_db,'SP_Generate_Global_Statistic_TREND','INSERT INTO table_reg_3_umts_weekly', START_TIME);
				
				SET @SqlCmd=CONCAT('SELECT COUNT(*) INTO @P_REG_3_UMTS_WK FROM information_schema.`PARTITIONS`
							WHERE TABLE_SCHEMA=''',v_group_db_name,''' 
							AND TABLE_NAME=''table_reg_3_umts_weekly'' AND PARTITION_NAME= ''p',@FIRST_DAY_WK,''';');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
			
				IF @P_REG_3_UMTS_WK=0 THEN
				SET @SqlCmd=CONCAT('ALTER TABLE ',v_group_db_name,'.table_reg_3_umts_weekly ADD PARTITION (PARTITION p',@FIRST_DAY_WK,' VALUES IN (TO_DAYS(''',@FIRST_DAY_WK,''')));');
			
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				END IF;
	
				SET @SqlCmd=CONCAT('ALTER TABLE ',v_group_db_name,'.table_reg_3_umts_weekly 
							TRUNCATE PARTITION p',@FIRST_DAY_WK,'
						   ;');
						
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
			
				SET @SqlCmd=CONCAT('INSERT INTO ',v_group_db_name,'.table_reg_3_umts_weekly 
							(  `START_DATE`,	
							    `END_DATE`,	
								`REG_ID`,
							`FREQUENCY`,
							`UARFCN`,
							',TILE_UMTS_COLUMN_STR,')
						   SELECT
							',@FIRST_DAY_WK,',	 
							',@END_DAY_WK,',
							`REG_ID`,
							`FREQUENCY`,
							`UARFCN`,
							',TILE_UMTS_COLUMN_STR,'
						   FROM ',@table_reg_3_umts_wk,'
						
						;');
	
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				SET @SqlCmd=CONCAT('SELECT COUNT(*) INTO @P_REG_2_UMTS_WK FROM information_schema.`PARTITIONS`
							WHERE TABLE_SCHEMA=''',v_group_db_name,''' 
							AND TABLE_NAME=''table_reg_2_umts_weekly'' AND PARTITION_NAME= ''p',@FIRST_DAY_WK,''';');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
			
				IF @P_REG_2_UMTS_WK=0 THEN
				SET @SqlCmd=CONCAT('ALTER TABLE ',v_group_db_name,'.table_reg_2_umts_weekly ADD PARTITION (PARTITION p',@FIRST_DAY_WK,' VALUES IN (TO_DAYS(''',@FIRST_DAY_WK,''')));');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				END IF;
			
				SET @SqlCmd=CONCAT('ALTER TABLE ',v_group_db_name,'.table_reg_2_umts_weekly 
							TRUNCATE PARTITION p',@FIRST_DAY_WK,'
						   ;');
						
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
	
				SET @SqlCmd=CONCAT('INSERT INTO ',v_group_db_name,'.table_reg_2_umts_weekly 
							(  `START_DATE`,	
							    `END_DATE`,	
								`REG_ID`,
							`FREQUENCY`,
							`UARFCN`,
							',TILE_UMTS_COLUMN_STR,')
						   SELECT
							',@FIRST_DAY_WK,',	 
							',@END_DAY_WK,',
							`REG_ID`,
							`FREQUENCY`,
							`UARFCN`,
							',TILE_UMTS_COLUMN_STR,'
						   FROM ',@table_reg_2_umts_wk,'
						  
						;');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				INSERT INTO gt_gw_main.SP_LOG VALUES(@global_db,'SP_Generate_Global_Statistic_TREND','INSERT INTO table_reg_1_umts_weekly', START_TIME);
				
				SET @SqlCmd=CONCAT('SELECT COUNT(*) INTO @P_REG_1_UMTS_WK FROM information_schema.`PARTITIONS`
							WHERE TABLE_SCHEMA=''',v_group_db_name,''' 
							AND TABLE_NAME=''table_reg_1_umts_weekly'' AND PARTITION_NAME= ''p',@FIRST_DAY_WK,''';');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
			
				IF @P_REG_1_UMTS_WK=0 THEN
				SET @SqlCmd=CONCAT('ALTER TABLE ',v_group_db_name,'.table_reg_1_umts_weekly ADD PARTITION (PARTITION p',@FIRST_DAY_WK,' VALUES IN (TO_DAYS(''',@FIRST_DAY_WK,''')));');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				END IF;
	
				SET @SqlCmd=CONCAT('ALTER TABLE ',v_group_db_name,'.table_reg_1_umts_weekly 
							TRUNCATE PARTITION p',@FIRST_DAY_WK,'
						   ;');
						
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
			
				SET @SqlCmd=CONCAT('INSERT INTO ',v_group_db_name,'.table_reg_1_umts_weekly 
							(  `START_DATE`,	
							    `END_DATE`,	
								`REG_ID`,
							`FREQUENCY`,
							`UARFCN`,
							',TILE_UMTS_COLUMN_STR,')
						   SELECT
							',@FIRST_DAY_WK,',	 
							',@END_DAY_WK,',
							`REG_ID`,
							`FREQUENCY`,
							`UARFCN`,
							',TILE_UMTS_COLUMN_STR,'
						   FROM ',@table_reg_1_umts_wk,'
						  
						;');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				
				SET @SqlCmd=CONCAT('REPLACE INTO ',@global_db,'.table_call_cnt_history_trend 
							(  `START_DATE`,
							   `END_DATE`,
							   `TECH_MASK`,
							   `CREATE_TIME`,
							   `FLAG`,
						           `RPT_TYPE`
							)
							SELECT
							',@FIRST_DAY_WK,',	
							',@END_DAY_WK,',
							',v_TECH_MASK,',
							 ''',NOW(),''' AS `CREATE_TIME`,
							''WEEKLY'',
							''TILE''
						;');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				
			END IF;
			IF FLAG= 4 THEN
				INSERT INTO gt_gw_main.SP_LOG VALUES(@global_db,'SP_Generate_Global_Statistic_TREND','INSERT INTO table_reg_3_umts_monthly', START_TIME);
				
				SET @SqlCmd=CONCAT('SELECT COUNT(*) INTO @P_REG_3_UMTS_MN FROM information_schema.`PARTITIONS`
							WHERE TABLE_SCHEMA=''',v_group_db_name,''' 
							AND TABLE_NAME=''table_reg_3_umts_monthly'' AND PARTITION_NAME= ''p',@FIRST_DAY_MN,''';');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
			
				IF @P_REG_3_UMTS_MN=0 THEN
				SET @SqlCmd=CONCAT('ALTER TABLE ',v_group_db_name,'.table_reg_3_umts_monthly ADD PARTITION (PARTITION p',@FIRST_DAY_MN,' VALUES IN (TO_DAYS(''',@FIRST_DAY_MN,''')));');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				END IF;
	
				SET @SqlCmd=CONCAT('ALTER TABLE ',v_group_db_name,'.table_reg_3_umts_monthly 
							TRUNCATE PARTITION p',@FIRST_DAY_MN,'
						   ;');	
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
	
				SET @SqlCmd=CONCAT('INSERT INTO ',v_group_db_name,'.table_reg_3_umts_monthly 
					(  `START_DATE`,	
					   `END_DATE`,
						`REG_ID`,
							`FREQUENCY`,
							`UARFCN`,
					',TILE_UMTS_COLUMN_STR,')
					SELECT
					',@FIRST_DAY_MN,',	 
					',@END_DAY_MN,',
					`REG_ID`,
							`FREQUENCY`,
							`UARFCN`,
					',TILE_UMTS_COLUMN_STR,'
					FROM ',@table_reg_3_umts_mn,'
					
				;');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
						
				INSERT INTO gt_gw_main.SP_LOG VALUES(@global_db,'SP_Generate_Global_Statistic_TREND','INSERT INTO table_reg_2_umts_weekly', START_TIME);
				
				
				INSERT INTO gt_gw_main.SP_LOG VALUES(@global_db,'SP_Generate_Global_Statistic_TREND','INSERT INTO table_reg_2_umts_monthly', START_TIME);
				
				SET @SqlCmd=CONCAT('SELECT COUNT(*) INTO @P_REG_2_UMTS_MN FROM information_schema.`PARTITIONS`
							WHERE TABLE_SCHEMA=''',v_group_db_name,''' 
							AND TABLE_NAME=''table_reg_2_umts_monthly'' AND PARTITION_NAME= ''p',@FIRST_DAY_MN,''';');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
			
				IF @P_REG_2_UMTS_MN=0 THEN
				SET @SqlCmd=CONCAT('ALTER TABLE ',v_group_db_name,'.table_reg_2_umts_monthly ADD PARTITION (PARTITION p',@FIRST_DAY_MN,' VALUES IN (TO_DAYS(''',@FIRST_DAY_MN,''')));');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				END IF;
			
				SET @SqlCmd=CONCAT('ALTER TABLE ',v_group_db_name,'.table_reg_2_umts_monthly 
							TRUNCATE PARTITION p',@FIRST_DAY_MN,'
						   ;');
						
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
	
	
				SET @SqlCmd=CONCAT('INSERT INTO ',v_group_db_name,'.table_reg_2_umts_monthly 
						(  `START_DATE`,	
						   `END_DATE`,	
							`REG_ID`,
							`FREQUENCY`,
							`UARFCN`,
						',TILE_UMTS_COLUMN_STR,')
						SELECT
						',@FIRST_DAY_MN,',	 
						',@END_DAY_MN,',
							`REG_ID`,
							`FREQUENCY`,
							`UARFCN`,
						',TILE_UMTS_COLUMN_STR,'
						FROM ',@table_reg_2_umts_mn,'
						
					;');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				INSERT INTO gt_gw_main.SP_LOG VALUES(@global_db,'SP_Generate_Global_Statistic_TREND','INSERT INTO table_reg_1_umts_monthly', START_TIME);
				
				SET @SqlCmd=CONCAT('SELECT COUNT(*) INTO @P_REG_1_UMTS_MN FROM information_schema.`PARTITIONS`
							WHERE TABLE_SCHEMA=''',v_group_db_name,''' 
							AND TABLE_NAME=''table_reg_1_umts_monthly'' AND PARTITION_NAME= ''p',@FIRST_DAY_MN,''';');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
			
				IF @P_REG_1_UMTS_MN=0 THEN
				SET @SqlCmd=CONCAT('ALTER TABLE ',v_group_db_name,'.table_reg_1_umts_monthly ADD PARTITION (PARTITION p',@FIRST_DAY_MN,' VALUES IN (TO_DAYS(''',@FIRST_DAY_MN,''')));');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				END IF;
	
				SET @SqlCmd=CONCAT('ALTER TABLE ',v_group_db_name,'.table_reg_1_umts_monthly 
							TRUNCATE PARTITION p',@FIRST_DAY_MN,'
						   ;');
						
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
	
				SET @SqlCmd=CONCAT('INSERT INTO ',v_group_db_name,'.table_reg_1_umts_monthly 
						(  `START_DATE`,	
						   `END_DATE`,	
						`REG_ID`,
							`FREQUENCY`,
							`UARFCN`,
						',TILE_UMTS_COLUMN_STR,')
						SELECT
						',@FIRST_DAY_MN,',	 
						',@END_DAY_MN,',
						`REG_ID`,
							`FREQUENCY`,
							`UARFCN`,
						',TILE_UMTS_COLUMN_STR,'
						FROM ',@table_reg_1_umts_mn,'
						
					;');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
	
				SET @SqlCmd=CONCAT('REPLACE INTO ',@global_db,'.table_call_cnt_history_trend 
							(  `START_DATE`,
							   `END_DATE`,
							   `TECH_MASK`,
							   `CREATE_TIME`,
							   `FLAG`,
						           `RPT_TYPE`
							)
							SELECT
							',@FIRST_DAY_MN,',	
							',@END_DAY_MN,',
							',v_TECH_MASK,',
							 ''',NOW(),''' AS `CREATE_TIME`,
							''MONTHLY'',
							''TILE''
						;');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
			END IF;
		END IF;
	
		IF v_TECH_MASK= 4 THEN 
			IF FLAG= 2 THEN 						
				INSERT INTO gt_gw_main.SP_LOG VALUES(@global_db,'SP_Generate_Global_Statistic_TREND','INSERT INTO table_reg_3_LTE_daily', START_TIME);
				
				SET @SqlCmd=CONCAT('SELECT COUNT(*) INTO @P_REG_3_LTE_DY FROM information_schema.`PARTITIONS`
							WHERE TABLE_SCHEMA=''',v_group_db_name,''' 
							AND TABLE_NAME=''table_reg_3_LTE_daily'' AND PARTITION_NAME= ''p',@DATE_DY,''';');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
			
				IF @P_REG_3_LTE_DY=0 THEN
				SET @SqlCmd=CONCAT('ALTER TABLE ',v_group_db_name,'.table_reg_3_LTE_daily ADD PARTITION (PARTITION p',@DATE_DY,' VALUES IN (TO_DAYS(''',@DATE_DY,''')));');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				END IF;
	
				SET @SqlCmd=CONCAT('ALTER TABLE ',v_group_db_name,'.table_reg_3_LTE_daily 
							TRUNCATE PARTITION p',@DATE_DY,'
						   ;');
						
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
			
				SET @SqlCmd=CONCAT('INSERT INTO ',v_group_db_name,'.table_reg_3_LTE_daily 
							(  `DATA_DATE`,	
							`REG_ID`,
							`EARFCN`,
							`EUTRABAND`,
							',TILE_LTE_COLUMN_STR,')
						   SELECT
							',@DATE_DY,',	
							`REG_ID`,
							`EARFCN`,
							`EUTRABAND`,
							',TILE_LTE_COLUMN_STR,'
						   FROM ',@table_reg_3_LTE_dy,'
						  
						;');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;	
				INSERT INTO gt_gw_main.SP_LOG VALUES(@global_db,'SP_Generate_Global_Statistic_TREND','INSERT INTO table_reg_2_LTE_daily', START_TIME);
			
				SET @SqlCmd=CONCAT('SELECT COUNT(*) INTO @P_REG_2_LTE_DY FROM information_schema.`PARTITIONS`
							WHERE TABLE_SCHEMA=''',v_group_db_name,''' 
							AND TABLE_NAME=''table_reg_2_LTE_daily'' AND PARTITION_NAME= ''p',@DATE_DY,''';');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
			
				IF @P_REG_2_LTE_DY=0 THEN
				SET @SqlCmd=CONCAT('ALTER TABLE ',v_group_db_name,'.table_reg_2_LTE_daily ADD PARTITION (PARTITION p',@DATE_DY,' VALUES IN (TO_DAYS(''',@DATE_DY,''')));');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				END IF;
	
				SET @SqlCmd=CONCAT('ALTER TABLE ',v_group_db_name,'.table_reg_2_LTE_daily 
							TRUNCATE PARTITION p',@DATE_DY,'
						   ;');
						
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
			
				SET @SqlCmd=CONCAT('INSERT INTO ',v_group_db_name,'.table_reg_2_LTE_daily 
							(  `DATA_DATE`,	
								`REG_ID`,
							`EARFCN`,
							`EUTRABAND`,
							',TILE_LTE_COLUMN_STR,')
						   SELECT
							',@DATE_DY,',
							`REG_ID`,
							`EARFCN`,
							`EUTRABAND`,
							',TILE_LTE_COLUMN_STR,'
						   FROM ',@table_reg_2_LTE_dy,'
						  
						;');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				
				INSERT INTO gt_gw_main.SP_LOG VALUES(@global_db,'SP_Generate_Global_Statistic_TREND','INSERT INTO table_reg_1_LTE_daily', START_TIME);
			
				SET @SqlCmd=CONCAT('SELECT COUNT(*) INTO @P_REG_1_LTE_DY FROM information_schema.`PARTITIONS`
							WHERE TABLE_SCHEMA=''',v_group_db_name,''' 
							AND TABLE_NAME=''table_reg_1_LTE_daily'' AND PARTITION_NAME= ''p',@DATE_DY,''';');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
			
				IF @P_REG_1_LTE_DY=0 THEN
				SET @SqlCmd=CONCAT('ALTER TABLE ',v_group_db_name,'.table_reg_1_LTE_daily ADD PARTITION (PARTITION p',@DATE_DY,' VALUES IN (TO_DAYS(''',@DATE_DY,''')));');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				END IF;
			
				SET @SqlCmd=CONCAT('ALTER TABLE ',v_group_db_name,'.table_reg_1_LTE_daily 
							TRUNCATE PARTITION p',@DATE_DY,'
						   ;');
						
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
			
				SET @SqlCmd=CONCAT('INSERT INTO ',v_group_db_name,'.table_reg_1_LTE_daily 
							(  `DATA_DATE`,	
								`REG_ID`,
							`EARFCN`,
							`EUTRABAND`,
							',TILE_LTE_COLUMN_STR,')
						   SELECT
							',@DATE_DY,',	
							`REG_ID`,
							`EARFCN`,
							`EUTRABAND`,
							',TILE_LTE_COLUMN_STR,'
						   FROM ',@table_reg_1_LTE_dy,'
						  
						;');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
	
				SET @SqlCmd=CONCAT('REPLACE INTO ',@global_db,'.table_call_cnt_history_trend 
							(  `START_DATE`,
							   `END_DATE`,
							   `TECH_MASK`,
							   `CREATE_TIME`,
							   `FLAG`,
						           `RPT_TYPE`
							)
							SELECT
							',@DATE_DY,',	
							',@DATE_DY,',
							',v_TECH_MASK,',
							 ''',NOW(),''' AS `CREATE_TIME`,
							''DAILY'',
							''TILE''
						;');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
			END IF;
			IF FLAG= 3 THEN
				INSERT INTO gt_gw_main.SP_LOG VALUES(@global_db,'SP_Generate_Global_Statistic_TREND','INSERT INTO table_reg_3_LTE_weekly', START_TIME);
				
				SET @SqlCmd=CONCAT('SELECT COUNT(*) INTO @P_REG_3_LTE_WK FROM information_schema.`PARTITIONS`
							WHERE TABLE_SCHEMA=''',v_group_db_name,''' 
							AND TABLE_NAME=''table_reg_3_LTE_weekly'' AND PARTITION_NAME= ''p',@FIRST_DAY_WK,''';');
				
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
			
				IF @P_REG_3_LTE_WK=0 THEN
				SET @SqlCmd=CONCAT('ALTER TABLE ',v_group_db_name,'.table_reg_3_LTE_weekly ADD PARTITION (PARTITION p',@FIRST_DAY_WK,' VALUES IN (TO_DAYS(''',@FIRST_DAY_WK,''')));');
				
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				END IF;
				
				SET @SqlCmd=CONCAT('ALTER TABLE ',v_group_db_name,'.table_reg_3_LTE_weekly 
							TRUNCATE PARTITION p',@FIRST_DAY_WK,'
						   ;');
					
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
	
				SET @SqlCmd=CONCAT('INSERT INTO ',v_group_db_name,'.table_reg_3_LTE_weekly 
							(  `START_DATE`,	
							    `END_DATE`,	
							`REG_ID`,
							`EARFCN`,
							`EUTRABAND`,
							',TILE_LTE_COLUMN_STR,')
						   SELECT
							',@FIRST_DAY_WK,',	 
							',@END_DAY_WK,',
							`REG_ID`,
							`EARFCN`,
							`EUTRABAND`,
							',TILE_LTE_COLUMN_STR,'
						   FROM ',@table_reg_3_LTE_wk,'
						  
						;');
		
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
	
				INSERT INTO gt_gw_main.SP_LOG VALUES(@global_db,'SP_Generate_Global_Statistic_TREND','INSERT INTO table_reg_2_LTE_weekly', START_TIME);
				
				SET @SqlCmd=CONCAT('SELECT COUNT(*) INTO @P_REG_2_LTE_WK FROM information_schema.`PARTITIONS`
							WHERE TABLE_SCHEMA=''',v_group_db_name,''' 
							AND TABLE_NAME=''table_reg_2_LTE_weekly'' AND PARTITION_NAME= ''p',@FIRST_DAY_WK,''';');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
			
				IF @P_REG_2_LTE_WK=0 THEN
				SET @SqlCmd=CONCAT('ALTER TABLE ',v_group_db_name,'.table_reg_2_LTE_weekly ADD PARTITION (PARTITION p',@FIRST_DAY_WK,' VALUES IN (TO_DAYS(''',@FIRST_DAY_WK,''')));');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				END IF;
	
				SET @SqlCmd=CONCAT('ALTER TABLE ',v_group_db_name,'.table_reg_2_LTE_weekly 
							TRUNCATE PARTITION p',@FIRST_DAY_WK,'
						   ;');
						
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
			
				SET @SqlCmd=CONCAT('INSERT INTO ',v_group_db_name,'.table_reg_2_LTE_weekly 
							(  `START_DATE`,	
							    `END_DATE`,	
							`REG_ID`,
							`EARFCN`,
							`EUTRABAND`,
							',TILE_LTE_COLUMN_STR,')
						   SELECT
							',@FIRST_DAY_WK,',	 
							',@END_DAY_WK,',
							`REG_ID`,
							`EARFCN`,
							`EUTRABAND`,
							',TILE_LTE_COLUMN_STR,'
						   FROM ',@table_reg_2_LTE_wk,'
						 
						;');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				INSERT INTO gt_gw_main.SP_LOG VALUES(@global_db,'SP_Generate_Global_Statistic_TREND','INSERT INTO table_reg_1_LTE_weekly', START_TIME);
				
				SET @SqlCmd=CONCAT('SELECT COUNT(*) INTO @P_REG_1_LTE_WK FROM information_schema.`PARTITIONS`
							WHERE TABLE_SCHEMA=''',v_group_db_name,''' 
							AND TABLE_NAME=''table_reg_1_LTE_weekly'' AND PARTITION_NAME= ''p',@FIRST_DAY_WK,''';');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
			
				IF @P_REG_1_LTE_WK=0 THEN
				SET @SqlCmd=CONCAT('ALTER TABLE ',v_group_db_name,'.table_reg_1_LTE_weekly ADD PARTITION (PARTITION p',@FIRST_DAY_WK,' VALUES IN (TO_DAYS(''',@FIRST_DAY_WK,''')));');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				END IF;
	
				SET @SqlCmd=CONCAT('ALTER TABLE ',v_group_db_name,'.table_reg_1_LTE_weekly 
							TRUNCATE PARTITION p',@FIRST_DAY_WK,'
						   ;');
						
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
			
				SET @SqlCmd=CONCAT('INSERT INTO ',v_group_db_name,'.table_reg_1_LTE_weekly 
							(  `START_DATE`,	
							    `END_DATE`,	
							`REG_ID`,
							`EARFCN`,
							`EUTRABAND`,
							',TILE_LTE_COLUMN_STR,')
						   SELECT
							',@FIRST_DAY_WK,',	 
							',@END_DAY_WK,',
							`REG_ID`,
							`EARFCN`,
							`EUTRABAND`,
							',TILE_LTE_COLUMN_STR,'
						   FROM ',@table_reg_1_LTE_wk,'
						  
						;');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
	
				SET @SqlCmd=CONCAT('REPLACE INTO ',@global_db,'.table_call_cnt_history_trend 
							(  `START_DATE`,
							   `END_DATE`,
							   `TECH_MASK`,
							   `CREATE_TIME`,
							   `FLAG`,
						           `RPT_TYPE`
							)
							SELECT
							',@FIRST_DAY_WK,',	
							',@END_DAY_WK,',
							',v_TECH_MASK,',
							 ''',NOW(),''' AS `CREATE_TIME`,
							''WEEKLY'',
							''TILE''
						;');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
			END IF;
			IF FLAG= 4 THEN
				INSERT INTO gt_gw_main.SP_LOG VALUES(@global_db,'SP_Generate_Global_Statistic_TREND','INSERT INTO table_reg_3_LTE_monthly', START_TIME);
				
				SET @SqlCmd=CONCAT('SELECT COUNT(*) INTO @P_REG_3_LTE_MN FROM information_schema.`PARTITIONS`
							WHERE TABLE_SCHEMA=''',v_group_db_name,''' 
							AND TABLE_NAME=''table_reg_3_LTE_monthly'' AND PARTITION_NAME= ''p',@FIRST_DAY_MN,''';');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
			
				IF @P_REG_3_LTE_MN=0 THEN
				SET @SqlCmd=CONCAT('ALTER TABLE ',v_group_db_name,'.table_reg_3_LTE_monthly ADD PARTITION (PARTITION p',@FIRST_DAY_MN,' VALUES IN (TO_DAYS(''',@FIRST_DAY_MN,''')));');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				END IF;
	
				SET @SqlCmd=CONCAT('ALTER TABLE ',v_group_db_name,'.table_reg_3_LTE_monthly 
							TRUNCATE PARTITION p',@FIRST_DAY_MN,'
						   ;');
						
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
			
				SET @SqlCmd=CONCAT('INSERT INTO ',v_group_db_name,'.table_reg_3_LTE_monthly 
						(  `START_DATE`,	
						   `END_DATE`,	
							`REG_ID`,
							`EARFCN`,
							`EUTRABAND`,
						',TILE_LTE_COLUMN_STR,')
						SELECT
						',@FIRST_DAY_MN,',	 
						',@END_DAY_MN,',
						`REG_ID`,
							`EARFCN`,
							`EUTRABAND`,
						',TILE_LTE_COLUMN_STR,'
						FROM ',@table_reg_3_LTE_mn,'
						
					;');
		
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;		
				INSERT INTO gt_gw_main.SP_LOG VALUES(@global_db,'SP_Generate_Global_Statistic_TREND','INSERT INTO table_reg_2_LTE_monthly', START_TIME);
				
				SET @SqlCmd=CONCAT('SELECT COUNT(*) INTO @P_REG_2_LTE_MN FROM information_schema.`PARTITIONS`
							WHERE TABLE_SCHEMA=''',v_group_db_name,''' 
							AND TABLE_NAME=''table_reg_2_LTE_monthly'' AND PARTITION_NAME= ''p',@FIRST_DAY_MN,''';');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
			
				IF @P_REG_2_LTE_MN=0 THEN
				SET @SqlCmd=CONCAT('ALTER TABLE ',v_group_db_name,'.table_reg_2_LTE_monthly ADD PARTITION (PARTITION p',@FIRST_DAY_MN,' VALUES IN (TO_DAYS(''',@FIRST_DAY_MN,''')));');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				END IF;
				
				SET @SqlCmd=CONCAT('ALTER TABLE  ',v_group_db_name,'.table_reg_2_LTE_monthly 
							TRUNCATE PARTITION p',@FIRST_DAY_MN,'
						   ;');
						
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
	
				SET @SqlCmd=CONCAT('INSERT INTO ',v_group_db_name,'.table_reg_2_LTE_monthly 
						(  `START_DATE`,	
						   `END_DATE`,	
						`REG_ID`,
							`EARFCN`,
							`EUTRABAND`,
						',TILE_LTE_COLUMN_STR,')
						SELECT
						',@FIRST_DAY_MN,',	 
						',@END_DAY_MN,',
						`REG_ID`,
							`EARFCN`,
							`EUTRABAND`,
						',TILE_LTE_COLUMN_STR,'
						FROM ',@table_reg_2_LTE_mn,'
						
					;');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				INSERT INTO gt_gw_main.SP_LOG VALUES(@global_db,'SP_Generate_Global_Statistic_TREND','INSERT INTO table_reg_1_LTE_monthly', START_TIME);
				
				SET @SqlCmd=CONCAT('SELECT COUNT(*) INTO @P_REG_1_LTE_MN FROM information_schema.`PARTITIONS`
							WHERE TABLE_SCHEMA=''',v_group_db_name,''' 
							AND TABLE_NAME=''table_reg_1_LTE_monthly'' AND PARTITION_NAME= ''p',@FIRST_DAY_MN,''';');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
			
				IF @P_REG_1_LTE_MN=0 THEN
				SET @SqlCmd=CONCAT('ALTER TABLE ',v_group_db_name,'.table_reg_1_LTE_monthly ADD PARTITION (PARTITION p',@FIRST_DAY_MN,' VALUES IN (TO_DAYS(''',@FIRST_DAY_MN,''')));');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				END IF;
	
				SET @SqlCmd=CONCAT('ALTER TABLE ',v_group_db_name,'.table_reg_1_LTE_monthly 
							TRUNCATE PARTITION p',@FIRST_DAY_MN,'
						   ;');
						
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
		
				SET @SqlCmd=CONCAT('INSERT INTO ',v_group_db_name,'.table_reg_1_LTE_monthly 
						(  `START_DATE`,	
						   `END_DATE`,	
						`REG_ID`,
							`EARFCN`,
							`EUTRABAND`,
						',TILE_LTE_COLUMN_STR,')
						SELECT
						',@FIRST_DAY_MN,',	 
						',@END_DAY_MN,',
						`REG_ID`,
							`EARFCN`,
							`EUTRABAND`,
						',TILE_LTE_COLUMN_STR,'
						FROM ',@table_reg_1_LTE_mn,'
						
					;');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				
				SET @SqlCmd=CONCAT('REPLACE INTO ',@global_db,'.table_call_cnt_history_trend 
							(  `START_DATE`,
							   `END_DATE`,
							   `TECH_MASK`,
							   `CREATE_TIME`,
							   `FLAG`,
						           `RPT_TYPE`
							)
							SELECT
							',@FIRST_DAY_MN,',	
							',@END_DAY_MN,',
							',v_TECH_MASK,',
							 ''',NOW(),''' AS `CREATE_TIME`,
							''MONTHLY'',
							''TILE''
						;');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
			END IF;			
		END IF;
	END IF;
	
	SET @v_reg_m=@v_reg_m+1;
	
	END;
	END WHILE;
	
	
END$$
DELIMITER ;
