DELIMITER $$
USE `gt_global_statistic`$$
DROP PROCEDURE IF EXISTS `SP_CreateDB_LTE`$$
CREATE DEFINER=`covmo`@`%` PROCEDURE `SP_Generate_Global_Statistic_Sub_TmpTbl_Cell`(IN DATA_DATE DATE,IN DATA_HOUR TINYINT(2),IN PU_ID MEDIUMINT(9),IN TECH_MASK TINYINT(2), IN GT_DB VARCHAR(100),IN WORKER_ID VARCHAR(10))
BEGIN
	DECLARE START_TIME DATETIME DEFAULT SYSDATE();
	DECLARE STEP_START_TIME DATETIME DEFAULT SYSDATE();
	DECLARE SP_Process VARCHAR(100) DEFAULT NULL;
	DECLARE SUB_WORKER_ID VARCHAR(10) DEFAULT CONNECTION_ID();
	DECLARE EX_DATE VARCHAR(100) DEFAULT NULL;
	DECLARE KPI_ERROR CONDITION FOR SQLSTATE '99999' ;
	DECLARE EXIT HANDLER FOR 1146
	BEGIN 
		SET @SqlCmd=CONCAT('INSERT INTO `gt_gw_main`.`tbl_rpt_error`
				    (`PID`,
				     `CreateTime`,
				     `error_str`)
			VALUES (',WORKER_ID,',''',NOW(),''',''No Table Available.'');');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		SELECT 'Temp table not exist.' AS IsSuccess;		
	END;
		
	INSERT INTO gt_gw_main.SP_LOG VALUES(GT_DB,'SP_Generate_Global_Statistic_Sub_TmpTbl_Cell',CONCAT(DATA_DATE,',',DATA_HOUR,',',PU_ID,',',TECH_MASK,',',PU_ID,'_',SUB_WORKER_ID,' Start'), NOW());
	SET STEP_START_TIME := SYSDATE();
	
	SET EX_DATE=CONCAT(DATE_FORMAT(DATA_DATE,'%Y%m%d_'),DATA_HOUR,'_',WORKER_ID);
	SET @SqlCmd=CONCAT('SELECT `att_value` INTO @ZOOM_LEVEL FROM gt_covmo.`sys_config` WHERE `group_name`=''system'' AND att_name = ''MapResolution'';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt; 
	
  	CALL gt_gw_main.`SP_Sub_Set_Session_Param_LTE`('');
	
	IF TECH_MASK=2 THEN 	
		SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS gt_global_statistic.tmp_cell_agg_umts_',PU_ID,'_',SUB_WORKER_ID,';');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		
		SET SP_Process = 'SP_Generate_Global_Statistic_cell_agg_umts';
		SET @SqlCmd=CONCAT('CREATE TEMPORARY TABLE gt_global_statistic.tmp_cell_agg_umts_',PU_ID,'_',SUB_WORKER_ID,' (
					`DATA_DATE` datetime NOT NULL,
					`DATA_HOUR` tinyint(4) NOT NULL,
					`CELL_ID` mediumint(9) NOT NULL,
					`SITE_ID` varchar(20) DEFAULT NULL,
					`CLUSTER_ID` mediumint(9) DEFAULT NULL,
					`RNC_ID` mediumint(9) NOT NULL,
					`FREQUENCY` smallint(6) DEFAULT NULL,
					`UARFCN` smallint(6) DEFAULT NULL,
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
					`END_VOICE_CNT` INT(11) DEFAULT NULL,
					`END_VIDEO_CNT` INT(11) DEFAULT NULL,
					`END_PS_R99_CNT` INT(11) DEFAULT NULL,
					`END_PS_HSPA_CNT` INT(11) DEFAULT NULL,
					`END_M_RAB_CNT` INT(11) DEFAULT NULL,
					`END_SIGNAL_CNT` INT(11) DEFAULT NULL,
					`END_SMS_CNT` INT(11) DEFAULT NULL,
					`END_PS_OTHER_CNT` INT(11) DEFAULT NULL,
					`DROP_VOICE_CNT` INT(11) DEFAULT NULL,
					`DROP_VIDEO_CNT` INT(11) DEFAULT NULL,
					`DROP_PS_R99_CNT` INT(11) DEFAULT NULL,
					`DROP_PS_HSPA_CNT` INT(11) DEFAULT NULL,
					`DROP_M_RAB_CNT` INT(11) DEFAULT NULL,
					`DROP_SIGNAL_CNT` INT(11) DEFAULT NULL,
					`DROP_SMS_CNT` INT(11) DEFAULT NULL,
					`DROP_PS_OTHER_CNT` INT(11) DEFAULT NULL,
					`NON_BLOCK_VOICE_CNT` INT(11) DEFAULT NULL,
					`NON_BLOCK_VIDEO_CNT` INT(11) DEFAULT NULL,
					`NON_BLOCK_PS_R99_CNT` INT(11) DEFAULT NULL,
					`NON_BLOCK_PS_HSPA_CNT` INT(11) DEFAULT NULL,
					`NON_BLOCK_M_RAB_CNT` INT(11) DEFAULT NULL,
					`NON_BLOCK_SIGNAL_CNT` INT(11) DEFAULT NULL,
					`NON_BLOCK_SMS_CNT` INT(11) DEFAULT NULL,
					`NON_BLOCK_PS_OTHER_CNT` INT(11) DEFAULT NULL,
					`BLOCK_VOICE_CNT` INT(11) DEFAULT NULL,
					`BLOCK_VEDIO_CNT` INT(11) DEFAULT NULL,
					`BLOCK_R99_CNT` INT(11) DEFAULT NULL,
					`BLOCK_HSPA_CNT` INT(11) DEFAULT NULL,
					`BLOCK_MRAB_CNT` INT(11) DEFAULT NULL,
					`BLOCK_SIGNAL_CNT` INT(11) DEFAULT NULL,
					`BLOCK_SMS_CNT` INT(11) DEFAULT NULL,
					`BLOCK_PS_OTHER_CNT` INT(11) DEFAULT NULL,
					`RSCP_SUM` DOUBLE DEFAULT NULL,
					`RSCP_CNT` INT(11) DEFAULT NULL,
					`ECN0_SUM` DOUBLE DEFAULT NULL,
					`ECN0_CNT` INT(11) DEFAULT NULL,
					`UL_VOLUME_SUM` DOUBLE DEFAULT NULL,
					`DL_VOLUME_SUM` DOUBLE DEFAULT NULL,
					`UL_THROUPUT_MAX` DOUBLE DEFAULT NULL,
					`DL_THROUPUT_MAX` DOUBLE DEFAULT NULL,
					`UL_THROUPUT_SUM` DOUBLE DEFAULT NULL,
					`UL_THROUPUT_CNT` INT(11) DEFAULT NULL,
					`DL_THROUPUT_SUM` DOUBLE DEFAULT NULL,
					`DL_THROUPUT_CNT` INT(11) DEFAULT NULL,
					`FP_RSCP_1` double DEFAULT NULL,
					`FP_ECN0_1` double DEFAULT NULL,
					`BEST_CNT` INT(11) DEFAULT NULL,
					`CALL_SETUP_TIME_VOICE_SUM` int(11) DEFAULT NULL,
					`CALL_SETUP_TIME_VOICE_CNT` int(11) DEFAULT NULL,
					`CALL_SETUP_TIME_VEDIO_SUM` int(11) DEFAULT NULL,
					`CALL_SETUP_TIME_VEDIO_CNT` int(11) DEFAULT NULL,
					`CALL_SETUP_TIME_R99_SUM` int(11) DEFAULT NULL,
					`CALL_SETUP_TIME_R99_CNT` int(11) DEFAULT NULL,
					`CALL_SETUP_TIME_HSPA_SUM` int(11) DEFAULT NULL,
					`CALL_SETUP_TIME_HSPA_CNT` int(11) DEFAULT NULL,
					`CALL_SETUP_TIME_MRAB_SUM` int(11) DEFAULT NULL,
					`CALL_SETUP_TIME_MRAB_CNT` int(11) DEFAULT NULL,
					`CALL_SETUP_TIME_SIG_SUM` int(11) DEFAULT NULL,
					`CALL_SETUP_TIME_SIG_CNT` int(11) DEFAULT NULL,
					`CALL_SETUP_TIME_OTH_SUM` int(11) DEFAULT NULL,
					`CALL_SETUP_TIME_OTH_CNT` int(11) DEFAULT NULL,
					`CALL_SETUP_TIME_SMS_SUM` int(11) DEFAULT NULL,
					`CALL_SETUP_TIME_SMS_CNT` int(11) DEFAULT NULL,
					`CALL_SETUP_TIME_SUM` int(11) DEFAULT NULL,
					`CALL_SETUP_TIME_CNT` int(11) DEFAULT NULL,
					`SF_VOICE_CNT` int(11) DEFAULT NULL,
					`SF_VEDIO_CNT` int(11) DEFAULT NULL,
					`SF_R99_CNT` int(11) DEFAULT NULL,
					`SF_HSPA_CNT` int(11) DEFAULT NULL,
					`SF_MRAB_CNT` int(11) DEFAULT NULL,
					`SF_OTHER_CNT` int(11) DEFAULT NULL,
					`SF_SMS_CNT` int(11) DEFAULT NULL,
					`SF_SIGNAL_CNT` int(11) DEFAULT NULL,
					`CALL_DUR_SUM` DOUBLE DEFAULT NULL,
					`VOICE_DUR_SUM` DOUBLE DEFAULT NULL,
					`VIDEO_DUR_SUM` DOUBLE DEFAULT NULL,
					`R99_DUR_SUM` DOUBLE DEFAULT NULL,
					`HSPA_DUR_SUM` DOUBLE DEFAULT NULL,
					`MRAB_DUR_SUM` DOUBLE DEFAULT NULL
					,PRIMARY KEY (`CELL_ID`,RNC_ID,`DATA_DATE`,`DATA_HOUR`)
					) ENGINE=MYISAM DEFAULT CHARSET=latin1;');
		
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		
		SET STEP_START_TIME := SYSDATE();
		SET @SqlCmd=CONCAT('SELECT spider_bg_direct_sql(CONCAT(''CALL gt_global_statistic.',SP_Process,'(''''',DATA_DATE,''''',',DATA_HOUR,',',PU_ID,',',TECH_MASK,',''''',GT_DB,''''');'') 
			, ''gt_global_statistic.tmp_cell_agg_umts_',PU_ID,'_',SUB_WORKER_ID,'''
			, CONCAT(''HOST '''''',REPLACE(gt_strtok(DS_AP_URI,3,'':''),''/'',''''),'''''', PORT '''''',REPLACE(gt_strtok(DS_AP_URI,4,'':''),''/'',''''),'''''',USER '''''',`DS_AP_USER`,'''''', PASSWORD '''''',`DS_AP_PASSWORD`,'''''''')
			) INTO @bb FROM `gt_covmo`.`rnc_information` WHERE `RNC`=',PU_ID,' AND `TECHNOLOGY`=''UMTS''
			;');
		
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
	 	INSERT INTO gt_gw_main.SP_LOG VALUES(GT_DB,'SP_Generate_Global_Statistic_Sub_TmpTbl_Cell',CONCAT('SELECT spider_bg_direct_sql,umts:',GT_DB,'_',WORKER_ID), NOW());
		SET STEP_START_TIME := SYSDATE();
		
		IF @bb=1 THEN 			
			SET @SqlCmd=CONCAT('INSERT INTO gt_global_statistic.tmp_cell_agg_umts_',EX_DATE,'
						SELECT * FROM gt_global_statistic.tmp_cell_agg_umts_',PU_ID,'_',SUB_WORKER_ID,';');
			
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
			INSERT INTO gt_gw_main.SP_LOG VALUES(GT_DB,'SP_Generate_Global_Statistic_Sub_TmpTbl_Cell',CONCAT('INSERT tmp_cell_agg_umts_',EX_DATE,' cost:',TIMESTAMPDIFF(SECOND,STEP_START_TIME,SYSDATE()),' sec.'), NOW());
			SET STEP_START_TIME := SYSDATE();
	
			SET @SqlCmd=CONCAT('UPDATE gt_global_statistic.tmp_table_call_cnt_',WORKER_ID,' 
						SET `IsSuccess`=1
						WHERE `DATA_DATE`=''',DATA_DATE,''' AND `DATA_HOUR`=',DATA_HOUR,'
							AND `PU_ID`=',PU_ID,' AND `TECH_MASK`=',TECH_MASK,'
						;');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
			INSERT INTO gt_gw_main.SP_LOG VALUES(GT_DB,'SP_Generate_Global_Statistic_Sub_TmpTbl_Cell',CONCAT('UPDATE tmp_table_call_cnt_',PU_ID,'_',SUB_WORKER_ID,' ,umts cost:',TIMESTAMPDIFF(SECOND,STEP_START_TIME,SYSDATE()),' sec.'), NOW());
		
		ELSE
			INSERT INTO gt_gw_main.SP_LOG VALUES(GT_DB,'SP_Generate_Global_Statistic_Sub_TmpTbl_Cell',CONCAT('INSERT ERROR,',WORKER_ID), NOW());
			INSERT INTO `gt_gw_main`.`tbl_rpt_error` (`PID`,`CreateTime`,`error_str`) VALUES (WORKER_ID,NOW(),'Spider SP EXECUTE Failed - SP_Generate_Global_Statistic_Sub_TmpTbl_Cell, UMTS');
			SIGNAL KPI_ERROR
				SET MESSAGE_TEXT = 'Spider SP EXECUTE Failed - SP_Generate_Global_Statistic_Sub_TmpTbl_Cell, UMTS';
		END IF;	
			
		SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS gt_global_statistic.tmp_cell_agg_umts_',PU_ID,'_',SUB_WORKER_ID,';');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		
	ELSEIF TECH_MASK=1 THEN 	
		SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS gt_global_statistic.tmp_cell_agg_gsm_',PU_ID,'_',SUB_WORKER_ID,';');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		
		SET SP_Process = 'SP_Generate_Global_Statistic_cell_agg_gsm';
		SET @SqlCmd=CONCAT('CREATE TEMPORARY TABLE gt_global_statistic.tmp_cell_agg_gsm_',PU_ID,'_',SUB_WORKER_ID,' (
					`DATA_DATE` datetime NOT NULL,
					`DATA_HOUR` tinyint(4) NOT NULL,
					`CELL_ID` mediumint(9) NOT NULL,
					`SITE_ID` varchar(20) DEFAULT NULL,
					`BSC_ID` mediumint(9) NOT NULL,
					`BCCH_ARFCN` smallint(5) unsigned DEFAULT NULL,
					`BANDINDEX` varchar(10) DEFAULT NULL,
					`INIT_CALL_CNT` INT(11) DEFAULT NULL,
					`END_CALL_CNT` INT(11) DEFAULT NULL,
					`VOICE_CNT` int(11) DEFAULT NULL,
					`SIGNAL_CNT` int(11) DEFAULT NULL,
					`SMS_CNT` int(11) DEFAULT NULL,
					`GPRS_CNT` int(11) DEFAULT NULL,
					`OTHER_CNT` int(11) DEFAULT NULL,
					`END_VOICE_CNT` int(11) DEFAULT NULL,
					`END_SIGNAL_CNT` int(11) DEFAULT NULL,
					`END_SMS_CNT` int(11) DEFAULT NULL,
					`END_GPRS_CNT` int(11) DEFAULT NULL,
					`END_OTHER_CNT` int(11) DEFAULT NULL,
					`BLOCK_VOICE_CNT` int(11) DEFAULT NULL,
					`BLOCK_GPRS_CNT` int(11) DEFAULT NULL,
					`BLOCK_SMS_CNT` int(11) DEFAULT NULL,
					`BLOCK_SIGNAL_CNT` int(11) DEFAULT NULL,
					`BLOCK_OTHER_CNT` int(11) DEFAULT NULL,
					`DROP_VOICE_CNT` int(11) DEFAULT NULL,
					`DROP_SIGNAL_CNT` int(11) DEFAULT NULL,
					`DROP_SMS_CNT` int(11) DEFAULT NULL,
					`DROP_GPRS_CNT` int(11) DEFAULT NULL,
					`DROP_OTHER_CNT` int(11) DEFAULT NULL,
					`NON_BLOCK_VOICE_CNT` int(11) DEFAULT NULL,
					`NON_BLOCK_SIGNAL_CNT` int(11) DEFAULT NULL,
					`NON_BLOCK_SMS_CNT` int(11) DEFAULT NULL,
					`NON_BLOCK_GPRS_CNT` int(11) DEFAULT NULL,
					`NON_BLOCK_OTHER_CNT` int(11) DEFAULT NULL,
					`RXLEV_SUM` double DEFAULT NULL,
					`RXLEV_CNT` int(11) DEFAULT NULL,
					`RXQUAL_SUM` double DEFAULT NULL,
					`RXQUAL_CNT` int(11) DEFAULT NULL,
					`CALL_SETUP_TIME_SUM` int(11) DEFAULT NULL,
					`CALL_SETUP_TIME_CNT` int(11) DEFAULT NULL,
					`CALL_SETUP_TIME_VOICE_SUM` int(11) DEFAULT NULL,
					`CALL_SETUP_TIME_VOICE_CNT` int(11) DEFAULT NULL,
					`CALL_SETUP_TIME_SIG_SUM` int(11) DEFAULT NULL,
					`CALL_SETUP_TIME_SIG_CNT` int(11) DEFAULT NULL,
					`CALL_SETUP_TIME_SMS_SUM` int(11) DEFAULT NULL,
					`CALL_SETUP_TIME_SMS_CNT` int(11) DEFAULT NULL,
					`CALL_SETUP_TIME_GPRS_SUM` int(11) DEFAULT NULL,
					`CALL_SETUP_TIME_GPRS_CNT` int(11) DEFAULT NULL,
					`CALL_SETUP_TIME_OTH_SUM` int(11) DEFAULT NULL,
					`CALL_SETUP_TIME_OTH_CNT` int(11) DEFAULT NULL,
					`SF_VOICE_CNT` int(11) DEFAULT NULL,
					`SF_DATA_CNT` int(11) DEFAULT NULL,
					`SF_SMS_CNT` int(11) DEFAULT NULL,
					`SF_SIGNAL_CNT` int(11) DEFAULT NULL,
					`SF_OTHER_CNT` int(11) DEFAULT NULL,
					`CALL_DUR_SUM` double DEFAULT NULL,
					`VOICE_DUR_SUM` double DEFAULT NULL,
					`DATA_DUR_SUM` double DEFAULT NULL
					 ,PRIMARY KEY (`CELL_ID`,`BSC_ID`,`DATA_DATE`,`DATA_HOUR`)
					) ENGINE=MYISAM DEFAULT CHARSET=latin1;');
		
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		
		SET STEP_START_TIME := SYSDATE();
		SET @SqlCmd=CONCAT('SELECT spider_bg_direct_sql(CONCAT(''CALL gt_global_statistic.',SP_Process,'(''''',DATA_DATE,''''',',DATA_HOUR,',',PU_ID,',',TECH_MASK,',''''',GT_DB,''''');'') 
			, ''gt_global_statistic.tmp_cell_agg_gsm_',PU_ID,'_',SUB_WORKER_ID,'''
			, CONCAT(''HOST '''''',REPLACE(gt_strtok(DS_AP_URI,3,'':''),''/'',''''),'''''', PORT '''''',REPLACE(gt_strtok(DS_AP_URI,4,'':''),''/'',''''),'''''',USER '''''',`DS_AP_USER`,'''''', PASSWORD '''''',`DS_AP_PASSWORD`,'''''''')
			) INTO @bb FROM `gt_covmo`.`RNC_information` WHERE `RNC`=',PU_ID,' AND `TECHNOLOGY`=''GSM''
			;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
	 	INSERT INTO gt_gw_main.SP_LOG VALUES(GT_DB,'SP_Generate_Global_Statistic_Sub_TmpTbl_Cell',CONCAT('SELECT spider_bg_direct_sql,gsm:',GT_DB,'_',WORKER_ID), NOW());
		SET STEP_START_TIME := SYSDATE();
		
		IF @bb=1 THEN 
			SET @SqlCmd=CONCAT('INSERT INTO gt_global_statistic.tmp_cell_agg_gsm_',EX_DATE,'
						SELECT * FROM gt_global_statistic.tmp_cell_agg_gsm_',PU_ID,'_',SUB_WORKER_ID,';');						
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
			INSERT INTO gt_gw_main.SP_LOG VALUES(GT_DB,'SP_Generate_Global_Statistic_Sub_TmpTbl_Cell',CONCAT('INSERT tmp_cell_agg_gsm_',EX_DATE,' cost:',TIMESTAMPDIFF(SECOND,STEP_START_TIME,SYSDATE()),' sec.'), NOW());
			SET STEP_START_TIME := SYSDATE();
			
			SET @SqlCmd=CONCAT('UPDATE gt_global_statistic.tmp_table_call_cnt_',WORKER_ID,' 
						SET `IsSuccess`=1
						WHERE `DATA_DATE`=''',DATA_DATE,''' AND `DATA_HOUR`=',DATA_HOUR,'
							AND `PU_ID`=',PU_ID,' AND `TECH_MASK`=',TECH_MASK,'
						;');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
			INSERT INTO gt_gw_main.SP_LOG VALUES(GT_DB,'SP_Generate_Global_Statistic_Sub_TmpTbl_Cell',CONCAT('UPDATE tmp_table_call_cnt_',PU_ID,'_',SUB_WORKER_ID,' ,gsm cost:',TIMESTAMPDIFF(SECOND,STEP_START_TIME,SYSDATE()),' sec.'), NOW());
		ELSE
			INSERT INTO gt_gw_main.SP_LOG VALUES(GT_DB,'SP_Generate_Global_Statistic_Sub_TmpTbl_Cell',CONCAT('INSERT ERROR,',WORKER_ID), NOW());
			INSERT INTO `gt_gw_main`.`tbl_rpt_error` (`PID`,`CreateTime`,`error_str`) VALUES (WORKER_ID,NOW(),'Spider SP EXECUTE Failed - SP_Generate_Global_Statistic_Sub_TmpTbl_Cell,GSM');
			SIGNAL KPI_ERROR
				SET MESSAGE_TEXT = 'Spider SP EXECUTE Failed - SP_Generate_Global_Statistic_Sub_TmpTbl_Cell,GSM';
		END IF;	
			
		SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS gt_global_statistic.tmp_cell_agg_gsm_',PU_ID,'_',SUB_WORKER_ID,';');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
	ELSEIF TECH_MASK=4 THEN 
		
		SET SP_Process = 'SP_Generate_Global_Statistic_cell_lte';
		SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS gt_global_statistic.tmp_cell_lte_',PU_ID,'_',SUB_WORKER_ID,';');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
	
		SET @SqlCmd=CONCAT('CREATE TEMPORARY TABLE gt_global_statistic.tmp_cell_lte_',PU_ID,'_',SUB_WORKER_ID,' 
					(
					`DATA_DATE` DATE NOT NULL DEFAULT ''0000-00-00'',
					`DATA_HOUR` TINYINT(4) NOT NULL DEFAULT ''0'',
					`PU_ID` MEDIUMINT(9) NOT NULL DEFAULT ''0'',
					`ENODEB_ID` MEDIUMINT(9) NOT NULL DEFAULT ''0'',
					`CELL_ID` MEDIUMINT(9) NOT NULL DEFAULT ''0'',
					`CELL_NAME` VARCHAR(50) CHARACTER SET utf8 DEFAULT NULL COMMENT ''lookup'',
					`SUB_REGION_ID` MEDIUMINT(9) NOT NULL DEFAULT ''0'',
					`EUTRABAND` SMALLINT(6) DEFAULT NULL,
					`EARFCN` MEDIUMINT(9) DEFAULT NULL,
					`CELL_OSS_NODE_ID` INT(20) DEFAULT NULL,
					`RRC_CONN_SETUP_ATTEMPT` MEDIUMINT(8) DEFAULT ''0'',
					`RRC_CONN_SETUP_REJECT` MEDIUMINT(8) DEFAULT ''0'',
					`RRC_CONN_SETUP_MISSING_COMPLETE` MEDIUMINT(8) DEFAULT ''0'',
					`ERAB_SETUP_ATTEMPT` MEDIUMINT(8) DEFAULT ''0'',
					`ERAB_SETUP_REJECT` MEDIUMINT(8) DEFAULT ''0'',
					`ERAB_SETUP_ACCS_DELAY` MEDIUMINT(8) DEFAULT ''0'',
					`UE_CNXT_DROP_SETUP` MEDIUMINT(8) DEFAULT ''0'',
					`UE_CNXT_DROP_DROP` MEDIUMINT(8) DEFAULT ''0'',
					`RRC_RE_ESTABLISHMENT_ATTEMPT` MEDIUMINT(8) DEFAULT ''0'',
					`RRC_RE_ESTABLISHMENT_FAILURE` MEDIUMINT(8) DEFAULT ''0'',
					`MIMO_USAGE_RATIO_NUM` MEDIUMINT(8) DEFAULT NULL,
					`MIMO_USAGE_RATIO_DEN` MEDIUMINT(8) DEFAULT NULL,
					`SUBSC_PET_CELL_PER_HR` FLOAT DEFAULT NULL,
					`CALL_DURATION_SUM` MEDIUMINT(8) DEFAULT ''0'',
					`CALL_DURATION_CNT` MEDIUMINT(8) DEFAULT ''0'',
					`4G_4G_HO_ATTEMPT` MEDIUMINT(8) DEFAULT ''0'',
					`4G_4G_HO_FAILURE` MEDIUMINT(8) DEFAULT ''0'',
					`RRC_RECONFIGURATION_ATTEMPT` MEDIUMINT(8) DEFAULT ''0'',
					`RRC_RECONFIGURATION_FAILURE` MEDIUMINT(8) DEFAULT ''0'',
					`CSFB_EXECUTIONS` MEDIUMINT(8) DEFAULT ''0'',
					`UE_CNXT_REL_TOT` MEDIUMINT(8) DEFAULT ''0'',
					`IRAT_PING_PONG` float DEFAULT NULL,
					`IRAT_RSRP` float DEFAULT NULL,
					`IRAT_RSRQ` float DEFAULT NULL,
					`IRAT_TA` float DEFAULT NULL,
					`IRAT_RSCP` float DEFAULT NULL,
					`IRAT_ECN0` float DEFAULT NULL,
					`IRAT_RSSI` float DEFAULT NULL,
					`4G_3G_HO_ATTEMPT` MEDIUMINT(8) DEFAULT ''0'',
					`4G_3G_HO_FAILURE` MEDIUMINT(8) DEFAULT ''0'',
					`4G_2G_HO_ATTEMPT` MEDIUMINT(8) DEFAULT ''0'',
					`4G_2G_HO_FAILURE` MEDIUMINT(8)  DEFAULT ''0'',
					`BLIND_REDIR_4G_3G` MEDIUMINT(8)  DEFAULT ''0'',
					`BLIND_REDIR_4G_2G` MEDIUMINT(8)  DEFAULT ''0'',
					`4G_3G_CSFB_HO_ATT`  MEDIUMINT(8)  DEFAULT ''0'',
					`4G_3G_CSFB_HO_SUCC` MEDIUMINT(8)  DEFAULT ''0'',
					`4G_3G_CSFB_HO_FAILURE` MEDIUMINT(8)  DEFAULT ''0'',
					`VOLTE_ATTEMPT`  MEDIUMINT(8)  DEFAULT ''0'',
					`VOLTE_SUCCESS` MEDIUMINT(8)  DEFAULT ''0'',
					`VOLTE_FAILURE` MEDIUMINT(8)  DEFAULT ''0'',
					`PROCEDURE_TIME_SUM` MEDIUMINT(8)  DEFAULT ''0'',
					`PROCEDURE_TIME_CNT` MEDIUMINT(8)  DEFAULT ''0'',
					`ACCESSIBILITY_TIME_SUM` MEDIUMINT(8)  DEFAULT ''0'',
					`ACCESSIBILITY_TIME_CNT` MEDIUMINT(8)  DEFAULT ''0'',
					`IRAT_RSRP_CNT` MEDIUMINT(8) DEFAULT ''0'',
					`IRAT_RSRQ_CNT` MEDIUMINT(8) DEFAULT ''0'',
					`IRAT_TA_CNT` MEDIUMINT(8) DEFAULT ''0'',
					`IRAT_RSCP_CNT` MEDIUMINT(8) DEFAULT ''0'',
					`IRAT_ECN0_CNT` MEDIUMINT(8) DEFAULT ''0'',
					`IRAT_RSSI_CNT` MEDIUMINT(8) DEFAULT ''0'',
					`PEAK_SUBSCRIBER` MEDIUMINT(8) DEFAULT ''0'',
					`PEAK_QUARTER` CHAR(4) DEFAULT ''0''
					`VOLTE_DROP` MEDIUMINT(8) DEFAULT ''0'',
					`VOLTE_END` MEDIUMINT(8) DEFAULT ''0'',
					`DL_VOLUME_SUM` DOUBLE DEFAULT ''0'',
					`UL_VOLUME_SUM` DOUBLE DEFAULT ''0'',
					`DL_THROUGHPUT_SUM` DOUBLE DEFAULT ''0'',
					`DL_THROUGHPUT_CNT` MEDIUMINT(9) DEFAULT ''0'',
					`UL_THROUGHPUT_SUM` DOUBLE DEFAULT ''0'',
					`UL_THROUGHPUT_CNT` MEDIUMINT(9) DEFAULT ''0'',
					`DL_THROUGHPUT_MAX` DOUBLE DEFAULT ''0'',
					`UL_THROUGHPUT_MAX` DOUBLE DEFAULT ''0'',
					`DL_TRAFFIC_DUR` DOUBLE DEFAULT ''0'',
					`UL_TRAFFIC_DUR` DOUBLE DEFAULT ''0'',
					PRIMARY KEY(`ENODEB_ID`,`CELL_ID`,PU_ID,SUB_REGION_ID,`DATA_DATE`,`DATA_HOUR`)
				) ENGINE=MYISAM DEFAULT CHARSET=utf8;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
	
		SET STEP_START_TIME := SYSDATE();
		SET @SqlCmd=CONCAT('SELECT spider_bg_direct_sql(CONCAT(''CALL gt_global_statistic.',SP_Process,'(''''',DATA_DATE,''''',',DATA_HOUR,',',PU_ID,',',TECH_MASK,',''''',GT_DB,''''');'') 
					, ''gt_global_statistic.tmp_cell_lte_',PU_ID,'_',SUB_WORKER_ID,'''
					, CONCAT(''HOST '''''',REPLACE(gt_strtok(DS_AP_URI,3,'':''),''/'',''''),'''''', PORT '''''',REPLACE(gt_strtok(DS_AP_URI,4,'':''),''/'',''''),'''''',USER '''''',`DS_AP_USER`,'''''', PASSWORD '''''',`DS_AP_PASSWORD`,'''''''')
					) INTO @bb FROM `gt_covmo`.`rnc_information` WHERE `RNC`=',PU_ID,' AND `TECHNOLOGY`=''LTE''
					;');
		
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		INSERT INTO gt_gw_main.SP_LOG VALUES(GT_DB,'SP_Generate_Global_Statistic_Sub_TmpTbl_Cell',CONCAT('spider_bg_direct_sql tmp_cell_lte_',PU_ID,'_',SUB_WORKER_ID,' cost:',TIMESTAMPDIFF(SECOND,STEP_START_TIME,SYSDATE()),' sec.'), NOW());
		SET STEP_START_TIME := SYSDATE();
	
		IF @bb=1 THEN 
			SET @SqlCmd=CONCAT('INSERT INTO gt_global_statistic.tmp_cell_lte_',EX_DATE,'
						SELECT * FROM gt_global_statistic.tmp_cell_lte_',PU_ID,'_',SUB_WORKER_ID,';');
			
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
			INSERT INTO gt_gw_main.SP_LOG VALUES(GT_DB,'SP_Generate_Global_Statistic_Sub_TmpTbl_Cell',CONCAT('tmp_cell_lte_',PU_ID,'_',SUB_WORKER_ID,' INSERT tmp_cell_tile_lte cost:',TIMESTAMPDIFF(SECOND,STEP_START_TIME,SYSDATE()),' sec.'), NOW());
			SET STEP_START_TIME := SYSDATE();
			SET @SqlCmd=CONCAT('UPDATE gt_global_statistic.tmp_table_call_cnt_',WORKER_ID,' 
						SET `IsSuccess`=1
						WHERE `DATA_DATE`=''',DATA_DATE,''' AND `DATA_HOUR`=',DATA_HOUR,'
							AND `PU_ID`=',PU_ID,' AND `TECH_MASK`=',TECH_MASK,'
						;');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
			INSERT INTO gt_gw_main.SP_LOG VALUES(GT_DB,'SP_Generate_Global_Statistic_Sub_TmpTbl_Cell',CONCAT('UPDATE tmp_table_call_cnt_',PU_ID,'_',SUB_WORKER_ID,' ,lte cost:',TIMESTAMPDIFF(SECOND,STEP_START_TIME,SYSDATE()),' sec.'), NOW());
		ELSE
			INSERT INTO `gt_gw_main`.`tbl_rpt_error` (`PID`,`CreateTime`,`error_str`) VALUES (WORKER_ID,NOW(),'Spider SP EXECUTE Failed - SP_Generate_Global_Statistic_Sub_TmpTbl_Cell');
			SIGNAL KPI_ERROR
				SET MESSAGE_TEXT = 'Spider SP EXECUTE Failed - SP_Generate_Global_Statistic_Sub_TmpTbl_Cell';
	 		INSERT INTO gt_gw_main.SP_LOG VALUES(GT_DB,'SP_Generate_Global_Statistic_Sub_TmpTbl_Cell',CONCAT('INSERT ERROR,',PU_ID,'_',SUB_WORKER_ID,' cost:',TIMESTAMPDIFF(SECOND,STEP_START_TIME,SYSDATE()),' sec.'), NOW());
			SET STEP_START_TIME := SYSDATE();
		END IF;	
			
		SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS gt_global_statistic.tmp_cell_lte_',PU_ID,'_',SUB_WORKER_ID,';');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
					
	 	INSERT INTO gt_gw_main.SP_LOG VALUES(GT_DB,'SP_Generate_Global_Statistic_Sub_TmpTbl_Cell',CONCAT('DROP TEMPORARY,',PU_ID,'_',SUB_WORKER_ID,' cost:',TIMESTAMPDIFF(SECOND,STEP_START_TIME,SYSDATE()),' sec.'), NOW());
		SET STEP_START_TIME := SYSDATE();
			
		
		SET SP_Process = 'SP_Generate_Global_Statistic_cell_agg_lte';					
		SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS gt_global_statistic.tmp_cell_agg_lte_',PU_ID,'_',SUB_WORKER_ID,';');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		
		SET @SqlCmd=CONCAT('CREATE TEMPORARY TABLE gt_global_statistic.tmp_cell_agg_lte_',PU_ID,'_',SUB_WORKER_ID,' 
					(
						`DATA_DATE` datetime NOT NULL,
						`DATA_HOUR` tinyint(4) NOT NULL,
						`CELL_ID` tinyint(4) unsigned,
						`ENODEB_ID` mediumint(9) DEFAULT NULL,
						`PU_ID` mediumint(9) NOT NULL,
						`EARFCN` SMALLINT(6) NOT NULL,
						`EUTRABAND` SMALLINT(6) NOT NULL,
						`INIT_CALL_CNT` INT(11) DEFAULT NULL,
						`END_CALL_CNT` INT(11) DEFAULT NULL,
						`SIGNAL_CNT` INT(11) DEFAULT NULL,
						`DATA_CNT` INT(11) DEFAULT NULL,
						`SMS_CNT` INT(11) DEFAULT NULL,
						`VOLTE_CNT` INT(11) DEFAULT NULL,
						`UNSPECIFIED_CNT` INT(11) DEFAULT NULL,
						`END_SIGNAL_CNT` INT(11) DEFAULT NULL,
						`END_DATA_CNT` INT(11) DEFAULT NULL,
						`END_SMS_CNT` INT(11) DEFAULT NULL,
						`END_VOLTE_CNT` INT(11) DEFAULT NULL,
						`END_UNSPECIFIED_CNT` INT(11) DEFAULT NULL,
						`DROP_SIGNAL_CNT` INT(11) DEFAULT NULL,
						`DROP_DATA_CNT` INT(11) DEFAULT NULL,
						`DROP_SMS_CNT` INT(11) DEFAULT NULL,
						`DROP_VOLTE_CNT` INT(11) DEFAULT NULL,
						`DROP_UNSPECIFIED_CNT` INT(11) DEFAULT NULL,
						`BLOCK_SIGNAL_CNT` INT(11) DEFAULT NULL,
						`BLOCK_DATA_CNT` INT(11) DEFAULT NULL,
						`BLOCK_SMS_CNT` INT(11) DEFAULT NULL,
						`BLOCK_VOLTE_CNT` INT(11) DEFAULT NULL,
						`BLOCK_UNSPECIFIED_CNT` INT(11) DEFAULT NULL,
						`CSFB_SIGNAL_CNT` INT(11) DEFAULT NULL,
						`CSFB_DATA_CNT` INT(11) DEFAULT NULL,
						`CSFB_SMS_CNT` INT(11) DEFAULT NULL,
						`CSFB_VOLTE_CNT` INT(11) DEFAULT NULL,
						`CSFB_UNSPECIFIED_CNT` INT(11) DEFAULT NULL,
						`NON_BLOCK_SIGNAL_CNT` INT(11) DEFAULT NULL,
						`NON_BLOCK_DATA_CNT` INT(11) DEFAULT NULL,
						`NON_BLOCK_SMS_CNT` INT(11) DEFAULT NULL,
						`NON_BLOCK_VOLTE_CNT` INT(11) DEFAULT NULL,
						`NON_BLOCK_UNSPECIFIED_CNT` INT(11) DEFAULT NULL,
						`RSRP_SUM` DOUBLE DEFAULT NULL,
						`RSRP_CNT` INT(11) DEFAULT NULL,
						`RSRQ_SUM` DOUBLE DEFAULT NULL,
						`RSRQ_CNT` INT(11) DEFAULT NULL,
						`UL_VOLUME_SUM` DOUBLE DEFAULT NULL,
						`DL_VOLUME_SUM` DOUBLE DEFAULT NULL,
						`UL_THROUPUT_MAX` DOUBLE DEFAULT NULL,
						`DL_THROUPUT_MAX` DOUBLE DEFAULT NULL,
						`UL_THROUPUT_SUM` DOUBLE DEFAULT NULL,
						`UL_THROUPUT_CNT` INT(11) DEFAULT NULL,
						`DL_THROUPUT_SUM` DOUBLE DEFAULT NULL,
						`DL_THROUPUT_CNT` INT(11) DEFAULT NULL,
						`INTER_FREQ_ATTEMPT_CNT` INT(11) DEFAULT NULL,
						`INTER_FREQ_FAILURE_CNT` INT(11) DEFAULT NULL,
						`INTRA_FREQ_ATTEMPT_CNT` INT(11) DEFAULT NULL,
						`INTRA_FREQ_FAILURE_CNT` INT(11) DEFAULT NULL,
						`4G_3G_ATTEMPT_CNT` INT(11) DEFAULT NULL,
						`4G_3G_FAILURE_CNT` INT(11) DEFAULT NULL,
						`4G_2G_ATTEMPT_CNT` INT(11) DEFAULT NULL,
						`4G_2G_FAILURE_CNT` INT(11) DEFAULT NULL,
						`MR_4G_RSRP_SERVING_SUM` DOUBLE DEFAULT NULL,
						`MR_4G_RSRP_SERVING_CNT` INT(11) DEFAULT NULL,
						`MR_4G_RSRQ_SERVING_SUM` DOUBLE DEFAULT NULL,
						`MR_4G_RSRQ_SERVING_CNT` INT(11) DEFAULT NULL,
						`CALL_SETUP_TIME_SUM` INT(11) DEFAULT NULL,
						`CALL_SETUP_TIME_CNT` INT(11) DEFAULT NULL,
						`CALL_SETUP_TIME_SIG_SUM` INT(11) DEFAULT NULL,
						`CALL_SETUP_TIME_SIG_CNT` INT(11) DEFAULT NULL,
						`CALL_SETUP_TIME_DATA_SUM` INT(11) DEFAULT NULL,
						`CALL_SETUP_TIME_DATA_CNT` INT(11) DEFAULT NULL,
						`CALL_SETUP_TIME_SMS_SUM` INT(11) DEFAULT NULL,
						`CALL_SETUP_TIME_SMS_CNT` INT(11) DEFAULT NULL,
						`CALL_SETUP_TIME_VOLTE_SUM` INT(11) DEFAULT NULL,
						`CALL_SETUP_TIME_VOLTE_CNT` INT(11) DEFAULT NULL,
						`CALL_SETUP_TIME_UNSP_SUM` INT(11) DEFAULT NULL,
						`CALL_SETUP_TIME_UNSP_CNT` INT(11) DEFAULT NULL,
						`SF_SIGNAL_CNT` INT(11) DEFAULT NULL,
						`SF_DATA_CNT` INT(11) DEFAULT NULL,
						`SF_SMS_CNT` INT(11) DEFAULT NULL,
						`SF_VOLTE_CNT` INT(11) DEFAULT NULL,
						`SF_UNSPECIFIED_CNT` INT(11) DEFAULT NULL,
						`SRVCC_ATTEMPT_CNT` INT(11) DEFAULT NULL,
						`SRVCC_FAILURE_CNT` INT(11) DEFAULT NULL,
						`S1_HO_ATTEMPT` INT(11) DEFAULT NULL,
						`S1_HO_FAILURE` INT(11) DEFAULT NULL,
						`X2_HO_ATTEMPT` INT(11) DEFAULT NULL,
						`X2_HO_FAILURE` INT(11) DEFAULT NULL,
						`LATENCY_SUM` DOUBLE DEFAULT NULL,
						`LATENCY_CNT` INT(11) DEFAULT NULL
						,PRIMARY KEY (`CELL_ID`,ENODEB_ID,`DATA_DATE`,`DATA_HOUR`)
					) ENGINE=MYISAM DEFAULT CHARSET=latin1;');
		
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		INSERT INTO gt_gw_main.SP_LOG VALUES(GT_DB,'SP_Generate_Global_Statistic_Sub_TmpTbl_Cell',CONCAT(DATA_DATE,',',DATA_HOUR,',',PU_ID,',',TECH_MASK,',',PU_ID,'_',SUB_WORKER_ID,' create table: ',TIMESTAMPDIFF(SECOND,STEP_START_TIME,SYSDATE()),' seconds.'), NOW());
		SET STEP_START_TIME := SYSDATE();
		
		SET @SqlCmd=CONCAT('SELECT spider_bg_direct_sql(CONCAT(''CALL gt_global_statistic.',SP_Process,'(''''',DATA_DATE,''''',',DATA_HOUR,',',PU_ID,',',TECH_MASK,',''''',GT_DB,''''');'') 
					, ''gt_global_statistic.tmp_cell_agg_lte_',PU_ID,'_',SUB_WORKER_ID,'''
					, CONCAT(''HOST '''''',REPLACE(gt_strtok(DS_AP_URI,3,'':''),''/'',''''),'''''', PORT '''''',REPLACE(gt_strtok(DS_AP_URI,4,'':''),''/'',''''),'''''',USER '''''',`DS_AP_USER`,'''''', PASSWORD '''''',`DS_AP_PASSWORD`,'''''''')
					) INTO @bb FROM `gt_covmo`.`rnc_information` WHERE `RNC`=',PU_ID,' AND `TECHNOLOGY`=''LTE''
					;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		
	 	INSERT INTO gt_gw_main.SP_LOG VALUES(GT_DB,'SP_Generate_Global_Statistic_Sub_TmpTbl_Cell',CONCAT('spider_bg_direct_sql tmp_materialization_lte_',PU_ID,'_',SUB_WORKER_ID,' ,lte cost:',TIMESTAMPDIFF(SECOND,STEP_START_TIME,SYSDATE()),' sec.'), NOW());
		SET STEP_START_TIME := SYSDATE();
	
		IF @bb=1 THEN 
			SET @SqlCmd=CONCAT('INSERT INTO gt_global_statistic.tmp_cell_agg_lte_',EX_DATE,'
						SELECT * FROM gt_global_statistic.tmp_cell_agg_lte_',PU_ID,'_',SUB_WORKER_ID,';');
			
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
			INSERT INTO gt_gw_main.SP_LOG VALUES(GT_DB,'SP_Generate_Global_Statistic_Sub_TmpTbl_Cell',CONCAT('INSERT tmp_cell_lte_',EX_DATE,' cost:',TIMESTAMPDIFF(SECOND,STEP_START_TIME,SYSDATE()),' sec.'), NOW());
			SET STEP_START_TIME := SYSDATE();
	
			SET @SqlCmd=CONCAT('UPDATE gt_global_statistic.tmp_table_call_cnt_',WORKER_ID,' 
						SET `IsSuccess`=1
						WHERE `DATA_DATE`=''',DATA_DATE,''' AND `DATA_HOUR`=',DATA_HOUR,'
							AND `PU_ID`=',PU_ID,' AND `TECH_MASK`=',TECH_MASK,'
						;');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
			INSERT INTO gt_gw_main.SP_LOG VALUES(GT_DB,'SP_Generate_Global_Statistic_Sub_TmpTbl_Cell',CONCAT('UPDATE tmp_table_call_cnt_',PU_ID,'_',SUB_WORKER_ID,' ,gsm cost:',TIMESTAMPDIFF(SECOND,STEP_START_TIME,SYSDATE()),' sec.'), NOW());
		
		ELSE
			INSERT INTO `gt_gw_main`.`tbl_rpt_error` (`PID`,`CreateTime`,`error_str`) VALUES (WORKER_ID,NOW(),'Spider SP EXECUTE Failed - SP_Generate_Global_Statistic_Sub_TmpTbl_Cell');
			SIGNAL KPI_ERROR
				SET MESSAGE_TEXT = 'Spider SP EXECUTE Failed - SP_Generate_Global_Statistic_Sub_TmpTbl_Cell';
	 		INSERT INTO gt_gw_main.SP_LOG VALUES(GT_DB,'SP_Generate_Global_Statistic_Sub_TmpTbl_Cell',CONCAT('INSERT ERROR,',PU_ID,'_',SUB_WORKER_ID,' cost:',TIMESTAMPDIFF(SECOND,STEP_START_TIME,SYSDATE()),' sec.'), NOW());
			SET STEP_START_TIME := SYSDATE();
		END IF;	
			
		SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS gt_global_statistic.tmp_cell_lte_',PU_ID,'_',SUB_WORKER_ID,';');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
					
	 	INSERT INTO gt_gw_main.SP_LOG VALUES(GT_DB,'SP_Generate_Global_Statistic_Sub_TmpTbl_Cell',CONCAT('DROP TEMPORARY,',PU_ID,'_',SUB_WORKER_ID,' cost:',TIMESTAMPDIFF(SECOND,STEP_START_TIME,SYSDATE()),' sec.'), NOW());
		SET STEP_START_TIME := SYSDATE();
	END IF;	
	INSERT INTO gt_gw_main.SP_LOG VALUES(GT_DB,'SP_Generate_Global_Statistic_Sub_TmpTbl_Cell',CONCAT(DATA_DATE,',',DATA_HOUR,',',PU_ID,',',TECH_MASK,',',PU_ID,'_',SUB_WORKER_ID,' Done: ',TIMESTAMPDIFF(SECOND,START_TIME,SYSDATE()),' seconds.'), NOW());
END$$
DELIMITER ;
