DELIMITER $$
USE `gt_global_statistic`$$
DROP PROCEDURE IF EXISTS `SP_CreateDB_LTE`$$
CREATE DEFINER=`covmo`@`%` PROCEDURE `SP_Generate_Global_Statistic_Sub_TmpTbl_Roamer`(IN DATA_DATE DATE,IN DATA_HOUR TINYINT(2),IN PU_ID MEDIUMINT(9),IN TECH_MASK TINYINT(2), IN GT_DB VARCHAR(100),IN WORKER_ID VARCHAR(10),IN TileResolution VARCHAR(10))
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
		SELECT 'No Table Available' AS IsSuccess;		
	END;
		
	INSERT INTO gt_gw_main.SP_LOG VALUES(GT_DB,'SP_Generate_Global_Statistic_Sub_TmpTbl_Roamer',CONCAT(DATA_DATE,',',DATA_HOUR,',',PU_ID,',',TECH_MASK,',',PU_ID,'_',SUB_WORKER_ID,' Start'), NOW());
	SET STEP_START_TIME := SYSDATE();
	
	SET EX_DATE=CONCAT(DATE_FORMAT(DATA_DATE,'%Y%m%d_'),DATA_HOUR,'_',WORKER_ID);
	SET @SqlCmd=CONCAT('SELECT `att_value` INTO @ZOOM_LEVEL FROM gt_covmo.`sys_config` WHERE `group_name`=''system'' AND att_name = ''MapResolution'';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt; 
	
  	CALL gt_gw_main.`SP_Sub_Set_Session_Param_LTE`('');
	
	SET @TILE_ID_LVL1 = CONCAT('TILE_ID_',gt_covmo_csv_get(TileResolution,1));
	SET @TILE_ID_LVL2 = CONCAT('TILE_ID_',gt_covmo_csv_get(TileResolution,2));
  	
	IF TECH_MASK=2 THEN 	
		SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS gt_global_statistic.tmp_romer_tile_umts_',PU_ID,'_',SUB_WORKER_ID,';');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		
		SET SP_Process = 'SP_Generate_Global_Statistic_roamer_umts';
		SET @SqlCmd=CONCAT('CREATE TEMPORARY TABLE gt_global_statistic.tmp_romer_tile_umts_',PU_ID,'_',SUB_WORKER_ID,' (
					  `TILE_ID` BIGINT(20) NOT NULL,
					  `MCC` char(3) DEFAULT NULL,
					  `MNC` varchar(3) DEFAULT NULL,
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
					  `PS_UL_VOLUME_SUM` DOUBLE DEFAULT NULL,
					  `PS_DL_VOLUME_SUM` DOUBLE DEFAULT NULL,
					  `PS_UL_SPEED_MAX` DOUBLE DEFAULT NULL,
					  `PS_DL_SPEED_MAX` DOUBLE DEFAULT NULL,
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
					  `CALL_SETUP_TIME_SUM` int(11) DEFAULT NULL,
					  `CALL_SETUP_TIME_CNT` int(11) DEFAULT NULL,
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
					  `REG_1_ID` BIGINT(20) DEFAULT NULL,
					  `REG_2_ID` BIGINT(20) DEFAULT NULL,
					  `REG_3_ID` BIGINT(20) DEFAULT NULL
-- 					  ,PRIMARY KEY (`TILE_ID`,MCC,MNC,`FREQUENCY`,`UARFCN`,`DATA_DATE`,`DATA_HOUR`)
					) ENGINE=MYISAM DEFAULT CHARSET=latin1;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		SET STEP_START_TIME := SYSDATE();
		SET @SqlCmd=CONCAT('SELECT spider_bg_direct_sql(CONCAT(''CALL gt_global_statistic.',SP_Process,'(''''',DATA_DATE,''''',',DATA_HOUR,',',PU_ID,',',TECH_MASK,',''''',GT_DB,''''',''''',TileResolution,''''');'') 
			, ''gt_global_statistic.tmp_romer_tile_umts_',PU_ID,'_',SUB_WORKER_ID,'''
			, CONCAT(''HOST '''''',REPLACE(gt_strtok(DS_AP_URI,3,'':''),''/'',''''),'''''', PORT '''''',REPLACE(gt_strtok(DS_AP_URI,4,'':''),''/'',''''),'''''',USER '''''',`DS_AP_USER`,'''''', PASSWORD '''''',`DS_AP_PASSWORD`,'''''''')
			) INTO @bb FROM `gt_covmo`.`rnc_information` WHERE `RNC`=',PU_ID,' AND `TECHNOLOGY`=''UMTS''
			;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
	 	INSERT INTO gt_gw_main.SP_LOG VALUES(GT_DB,'SP_Generate_Global_Statistic_Sub_TmpTbl_Roamer',CONCAT('SELECT spider_bg_direct_sql,umts:',GT_DB,'_',WORKER_ID), NOW());
		SET STEP_START_TIME := SYSDATE();
		
		IF @bb=1 THEN 			
			SET @SqlCmd=CONCAT('INSERT INTO gt_global_statistic.tmp_romer_tile_umts_',EX_DATE,'
						SELECT * FROM gt_global_statistic.tmp_romer_tile_umts_',PU_ID,'_',SUB_WORKER_ID,';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
			INSERT INTO gt_gw_main.SP_LOG VALUES(GT_DB,'SP_Generate_Global_Statistic_Sub_TmpTbl_Roamer',CONCAT('INSERT tmp_romer_tile_umts_',EX_DATE,' cost:',TIMESTAMPDIFF(SECOND,STEP_START_TIME,SYSDATE()),' sec.'), NOW());
			
			SET STEP_START_TIME := SYSDATE();
	
			SET @SqlCmd=CONCAT('UPDATE gt_global_statistic.tmp_table_call_cnt_',WORKER_ID,' 
						SET `IsSuccess`=1
						WHERE `DATA_DATE`=''',DATA_DATE,''' AND `DATA_HOUR`=',DATA_HOUR,'
							AND `PU_ID`=',PU_ID,' AND `TECH_MASK`=',TECH_MASK,'
						;');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
			INSERT INTO gt_gw_main.SP_LOG VALUES(GT_DB,'SP_Generate_Global_Statistic_Sub_TmpTbl_Roamer',CONCAT('UPDATE tmp_table_call_cnt_',PU_ID,'_',SUB_WORKER_ID,' ,umts cost:',TIMESTAMPDIFF(SECOND,STEP_START_TIME,SYSDATE()),' sec.'), NOW());
		ELSE
			INSERT INTO gt_gw_main.SP_LOG VALUES(GT_DB,'SP_Generate_Global_Statistic_Sub_TmpTbl_Roamer',CONCAT('INSERT ERROR,',WORKER_ID), NOW());
			INSERT INTO `gt_gw_main`.`tbl_rpt_error` (`PID`,`CreateTime`,`error_str`) VALUES (WORKER_ID,NOW(),'Spider SP EXECUTE Failed - SP_Generate_Global_Statistic_Sub_TmpTbl_Roamer, UMTS');
			SIGNAL KPI_ERROR
				SET MESSAGE_TEXT = 'Spider SP EXECUTE Failed - SP_Generate_Global_Statistic_Sub_TmpTbl_Roamer, UMTS';
		END IF;	
			
		SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS gt_global_statistic.tmp_romer_tile_umts_',PU_ID,'_',SUB_WORKER_ID,';');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		
	ELSEIF TECH_MASK=1 THEN 	
		SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS gt_global_statistic.tmp_romer_tile_gsm_',PU_ID,'_',SUB_WORKER_ID,';');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		
		SET SP_Process = 'SP_Generate_Global_Statistic_roamer_gsm';
		SET @SqlCmd=CONCAT('CREATE TEMPORARY TABLE gt_global_statistic.tmp_romer_tile_gsm_',PU_ID,'_',SUB_WORKER_ID,' (
					  `TILE_ID` BIGINT(20) NOT NULL,
					  `MCC` char(3) DEFAULT NULL,
					  `MNC` varchar(3) DEFAULT NULL,
					  `FREQUENCY` SMALLINT(6) NOT NULL,
					  `BCCH_ARFCN` varchar(10) DEFAULT NULL,
					  `DATA_DATE` DATETIME NOT NULL,
					  `DATA_HOUR` TINYINT(2) NOT NULL,
					  `',@TILE_ID_LVL2,'` BIGINT(20) DEFAULT NULL,
					  `',@TILE_ID_LVL1,'` BIGINT(20) DEFAULT NULL,
					  INIT_CALL_CNT int(11) DEFAULT NULL, 
					  END_CALL_CNT int(11) DEFAULT NULL,
					  VOICE_CNT int(11) DEFAULT NULL,
					  SIGNAL_CNT int(11) DEFAULT NULL,
					  SMS_CNT int(11) DEFAULT NULL,
					  GPRS_CNT int(11) DEFAULT NULL,
					  OTHER_CNT int(11) DEFAULT NULL,
					  BLOCK_CNT int(11) DEFAULT NULL,
					  DROP_VOICE_CNT int(11) DEFAULT NULL,
					  DROP_SIGNAL_CNT int(11) DEFAULT NULL,
					  DROP_SMS_CNT int(11) DEFAULT NULL,
					  DROP_GPRS_CNT int(11) DEFAULT NULL,
					  DROP_OTHER_CNT int(11) DEFAULT NULL,
					  NON_BLOCK_VOICE_CNT int(11) DEFAULT NULL,
					  NON_BLOCK_SIGNAL_CNT int(11) DEFAULT NULL,
					  NON_BLOCK_SMS_CNT int(11) DEFAULT NULL,
					  NON_BLOCK_GPRS_CNT int(11) DEFAULT NULL,
					  NON_BLOCK_OTHER_CNT int(11) DEFAULT NULL,
					  CALL_DUR_SUM double DEFAULT NULL,
					  CALL_SETUP_TIME_SUM int(11) DEFAULT NULL,
					  CALL_SETUP_TIME_CNT int(11) DEFAULT NULL,
					  CALL_SETUP_TIME_VOICE_SUM int(11) DEFAULT NULL,
					  CALL_SETUP_TIME_VOICE_CNT int(11) DEFAULT NULL,
					  CALL_SETUP_TIME_SIG_SUM int(11) DEFAULT NULL,
					  CALL_SETUP_TIME_SIG_CNT int(11) DEFAULT NULL,
					  CALL_SETUP_TIME_SMS_SUM int(11) DEFAULT NULL,
					  CALL_SETUP_TIME_SMS_CNT int(11) DEFAULT NULL,
					  CALL_SETUP_TIME_GPRS_SUM int(11) DEFAULT NULL,
					  CALL_SETUP_TIME_GPRS_CNT int(11) DEFAULT NULL,
					  CALL_SETUP_TIME_OTH_SUM int(11) DEFAULT NULL,
					  CALL_SETUP_TIME_OTH_CNT int(11) DEFAULT NULL,
					  `REG_1_ID` BIGINT(20) DEFAULT NULL,
					  `REG_2_ID` BIGINT(20) DEFAULT NULL,
					  `REG_3_ID` BIGINT(20) DEFAULT NULL
-- 					  ,PRIMARY KEY (`TILE_ID`,MCC,MNC,`FREQUENCY`,`BCCH_ARFCN`,`DATA_DATE`,`DATA_HOUR`)
					) ENGINE=MYISAM DEFAULT CHARSET=latin1;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		
		SET STEP_START_TIME := SYSDATE();
		SET @SqlCmd=CONCAT('SELECT spider_bg_direct_sql(CONCAT(''CALL gt_global_statistic.',SP_Process,'(''''',DATA_DATE,''''',',DATA_HOUR,',',PU_ID,',',TECH_MASK,',''''',GT_DB,''''',''''',TileResolution,''''');'') 
			, ''gt_global_statistic.tmp_romer_tile_gsm_',PU_ID,'_',SUB_WORKER_ID,'''
			, CONCAT(''HOST '''''',REPLACE(gt_strtok(DS_AP_URI,3,'':''),''/'',''''),'''''', PORT '''''',REPLACE(gt_strtok(DS_AP_URI,4,'':''),''/'',''''),'''''',USER '''''',`DS_AP_USER`,'''''', PASSWORD '''''',`DS_AP_PASSWORD`,'''''''')
			) INTO @bb FROM `gt_covmo`.`RNC_information` WHERE `RNC`=',PU_ID,' AND `TECHNOLOGY`=''GSM''
			;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
	 	INSERT INTO gt_gw_main.SP_LOG VALUES(GT_DB,'SP_Generate_Global_Statistic_Sub_TmpTbl_Roamer',CONCAT('SELECT spider_bg_direct_sql,gsm:',GT_DB,'_',WORKER_ID), NOW());
		SET STEP_START_TIME := SYSDATE();
		
		IF @bb=1 THEN 
			SET @SqlCmd=CONCAT('INSERT INTO gt_global_statistic.tmp_romer_tile_gsm_',EX_DATE,'
						SELECT * FROM gt_global_statistic.tmp_romer_tile_gsm_',PU_ID,'_',SUB_WORKER_ID,';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
			INSERT INTO gt_gw_main.SP_LOG VALUES(GT_DB,'SP_Generate_Global_Statistic_Sub_TmpTbl_Roamer',CONCAT('INSERT tmp_romer_tile_gsm_',EX_DATE,' cost:',TIMESTAMPDIFF(SECOND,STEP_START_TIME,SYSDATE()),' sec.'), NOW());
			SET STEP_START_TIME := SYSDATE();
			
			SET @SqlCmd=CONCAT('UPDATE gt_global_statistic.tmp_table_call_cnt_',WORKER_ID,' 
						SET `IsSuccess`=1
						WHERE `DATA_DATE`=''',DATA_DATE,''' AND `DATA_HOUR`=',DATA_HOUR,'
							AND `PU_ID`=',PU_ID,' AND `TECH_MASK`=',TECH_MASK,'
						;');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
			INSERT INTO gt_gw_main.SP_LOG VALUES(GT_DB,'SP_Generate_Global_Statistic_Sub_TmpTbl_Roamer',CONCAT('UPDATE tmp_table_call_cnt_',PU_ID,'_',SUB_WORKER_ID,' ,gsm cost:',TIMESTAMPDIFF(SECOND,STEP_START_TIME,SYSDATE()),' sec.'), NOW());
		ELSE
			INSERT INTO gt_gw_main.SP_LOG VALUES(GT_DB,'SP_Generate_Global_Statistic_Sub_TmpTbl_Roamer',CONCAT('INSERT ERROR,',WORKER_ID), NOW());
			INSERT INTO `gt_gw_main`.`tbl_rpt_error` (`PID`,`CreateTime`,`error_str`) VALUES (WORKER_ID,NOW(),'Spider SP EXECUTE Failed - SP_Generate_Global_Statistic_Sub_TmpTbl_Roamer,GSM');
			SIGNAL KPI_ERROR
				SET MESSAGE_TEXT = 'Spider SP EXECUTE Failed - SP_Generate_Global_Statistic_Sub_TmpTbl_Roamer,GSM';
		END IF;	
			
		SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS gt_global_statistic.tmp_romer_tile_gsm_',PU_ID,'_',SUB_WORKER_ID,';');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
	ELSEIF TECH_MASK=4 THEN 
		SET SP_Process = 'SP_Generate_Global_Statistic_roamer_lte';					
		SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS gt_global_statistic.tmp_romer_tile_lte_',PU_ID,'_',SUB_WORKER_ID,';');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		
		SET @SqlCmd=CONCAT('CREATE TEMPORARY TABLE gt_global_statistic.tmp_romer_tile_lte_',PU_ID,'_',SUB_WORKER_ID,' 
					(
					  `TILE_ID` BIGINT(20) NOT NULL,
					  `MCC` char(3) DEFAULT NULL,
					  `MNC` varchar(3) DEFAULT NULL,
					  `',@TILE_ID_LVL2,'` BIGINT(20)  NULL,
					  `',@TILE_ID_LVL1,'` BIGINT(20)  NULL,
					  `EARFCN` SMALLINT(6) NOT NULL,
					  `EUTRABAND` SMALLINT(6) NOT NULL,
					  `DATA_DATE` DATETIME NOT NULL,
					  `DATA_HOUR` TINYINT(4) NOT NULL,
					  `INIT_CALL_CNT` INT(11) DEFAULT NULL,
					  `END_CALL_CNT` INT(11) DEFAULT NULL,
					  `SIGNAL_CNT` INT(11) DEFAULT NULL,
					  `DATA_CNT` INT(11) DEFAULT NULL,
					  `UNSPECIFIED_CNT` INT(11) DEFAULT NULL,
					  `CALL_DUR_SUM` DOUBLE DEFAULT NULL,
					  `BLOCK_CNT` INT(11) DEFAULT NULL,
					  `DROP_CNT` INT(11) DEFAULT NULL,
					  `CSFB_CNT` INT(11) DEFAULT NULL,
					  `PS_UL_VOLUME_SUM` DOUBLE DEFAULT NULL,
					  `PS_DL_VOLUME_SUM` DOUBLE DEFAULT NULL,
					  `PS_UL_SPEED_MAX` DOUBLE DEFAULT NULL,
					  `PS_DL_SPEED_MAX` DOUBLE DEFAULT NULL,
					  `DROP_SIGNAL_CNT` INT(11) DEFAULT NULL,
					  `DROP_DATA_CNT` INT(11) DEFAULT NULL,
					  `DROP_SMS_CNT` INT(11) DEFAULT NULL,
					  `DROP_VOLTE_CNT` INT(11) DEFAULT NULL,
					  `DROP_UNSPECIFIED_CNT` INT(11) DEFAULT NULL,
					  `END_NON_BLOCK_CALL_CNT` INT(11) DEFAULT NULL,
					  `UL_THROUPUT_SUM` DOUBLE DEFAULT NULL,
					  `UL_THROUPUT_CNT` INT(11) DEFAULT NULL,
					  `DL_THROUPUT_SUM` DOUBLE DEFAULT NULL,
					  `DL_THROUPUT_CNT` INT(11) DEFAULT NULL,
					  `END_NON_BLOCK_SIGNAL_CNT` INT(11) DEFAULT NULL,
					  `END_NON_BLOCK_DATA_CNT` INT(11) DEFAULT NULL,
					  `CALL_SETUP_TIME_SUM`  int(11) DEFAULT NULL,
					  `CALL_SETUP_TIME_CNT` int(11) DEFAULT NULL,
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
					  `REG_1_ID` BIGINT(20) DEFAULT NULL,
					  `REG_2_ID` BIGINT(20) DEFAULT NULL,
					  `REG_3_ID` BIGINT(20) DEFAULT NULL
-- 					  ,PRIMARY KEY (`TILE_ID`,MCC,MNC,`EARFCN`,`EUTRABAND`,`DATA_DATE`,`DATA_HOUR`)
					) ENGINE=MYISAM DEFAULT CHARSET=latin1;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		INSERT INTO gt_gw_main.SP_LOG VALUES(GT_DB,'SP_Generate_Global_Statistic_Sub_TmpTbl_Roamer',CONCAT(DATA_DATE,',',DATA_HOUR,',',PU_ID,',',TECH_MASK,',',PU_ID,'_',SUB_WORKER_ID,' create table: ',TIMESTAMPDIFF(SECOND,STEP_START_TIME,SYSDATE()),' seconds.'), NOW());
		SET STEP_START_TIME := SYSDATE();
		SET @SqlCmd=CONCAT('SELECT spider_bg_direct_sql(CONCAT(''CALL gt_global_statistic.',SP_Process,'(''''',DATA_DATE,''''',',DATA_HOUR,',',PU_ID,',',TECH_MASK,',''''',GT_DB,''''',''''',TileResolution,''''');'') 
					, ''gt_global_statistic.tmp_romer_tile_lte_',PU_ID,'_',SUB_WORKER_ID,'''
					, CONCAT(''HOST '''''',REPLACE(gt_strtok(DS_AP_URI,3,'':''),''/'',''''),'''''', PORT '''''',REPLACE(gt_strtok(DS_AP_URI,4,'':''),''/'',''''),'''''',USER '''''',`DS_AP_USER`,'''''', PASSWORD '''''',`DS_AP_PASSWORD`,'''''''')
					) INTO @bb FROM `gt_covmo`.`rnc_information` WHERE `RNC`=',PU_ID,' AND `TECHNOLOGY`=''LTE''
					;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
	 	INSERT INTO gt_gw_main.SP_LOG VALUES(GT_DB,'SP_Generate_Global_Statistic_Sub_TmpTbl_Roamer',CONCAT('spider_bg_direct_sql tmp_materialization_lte_',PU_ID,'_',SUB_WORKER_ID,' ,lte cost:',TIMESTAMPDIFF(SECOND,STEP_START_TIME,SYSDATE()),' sec.'), NOW());
		SET STEP_START_TIME := SYSDATE();
	
		IF @bb=1 THEN 
			SET @SqlCmd=CONCAT('INSERT INTO gt_global_statistic.tmp_romer_tile_lte_',EX_DATE,'
						SELECT * FROM gt_global_statistic.tmp_romer_tile_lte_',PU_ID,'_',SUB_WORKER_ID,';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
			INSERT INTO gt_gw_main.SP_LOG VALUES(GT_DB,'SP_Generate_Global_Statistic_Sub_TmpTbl_Roamer',CONCAT('INSERT tmp_romer_tile_lte_',EX_DATE,' cost:',TIMESTAMPDIFF(SECOND,STEP_START_TIME,SYSDATE()),' sec.'), NOW());
			SET STEP_START_TIME := SYSDATE();
			
			SET @SqlCmd=CONCAT('UPDATE gt_global_statistic.tmp_table_call_cnt_',WORKER_ID,' 
						SET `IsSuccess`=1
						WHERE `DATA_DATE`=''',DATA_DATE,''' AND `DATA_HOUR`=',DATA_HOUR,'
							AND `PU_ID`=',PU_ID,' AND `TECH_MASK`=',TECH_MASK,'
						;');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
			INSERT INTO gt_gw_main.SP_LOG VALUES(GT_DB,'SP_Generate_Global_Statistic_Sub_TmpTbl_Roamer',CONCAT('UPDATE tmp_table_call_cnt_',PU_ID,'_',SUB_WORKER_ID,' ,lte cost:',TIMESTAMPDIFF(SECOND,STEP_START_TIME,SYSDATE()),' sec.'), NOW());
		ELSE
			INSERT INTO `gt_gw_main`.`tbl_rpt_error` (`PID`,`CreateTime`,`error_str`) VALUES (WORKER_ID,NOW(),'Spider SP EXECUTE Failed - SP_Generate_Global_Statistic_Sub_TmpTbl_Roamer');
			SIGNAL KPI_ERROR
				SET MESSAGE_TEXT = 'Spider SP EXECUTE Failed - SP_Generate_Global_Statistic_Sub_TmpTbl_Roamer';
	 		INSERT INTO gt_gw_main.SP_LOG VALUES(GT_DB,'SP_Generate_Global_Statistic_Sub_TmpTbl_Roamer',CONCAT('INSERT ERROR,',PU_ID,'_',SUB_WORKER_ID,' cost:',TIMESTAMPDIFF(SECOND,STEP_START_TIME,SYSDATE()),' sec.'), NOW());
			SET STEP_START_TIME := SYSDATE();
		END IF;	
			
		SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS gt_global_statistic.tmp_romer_tile_lte_',PU_ID,'_',SUB_WORKER_ID,';');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
					
	 	INSERT INTO gt_gw_main.SP_LOG VALUES(GT_DB,'SP_Generate_Global_Statistic_Sub_TmpTbl_Roamer',CONCAT('DROP TEMPORARY,',PU_ID,'_',SUB_WORKER_ID,' cost:',TIMESTAMPDIFF(SECOND,STEP_START_TIME,SYSDATE()),' sec.'), NOW());
		SET STEP_START_TIME := SYSDATE();
	END IF;	
	INSERT INTO gt_gw_main.SP_LOG VALUES(GT_DB,'SP_Generate_Global_Statistic_Sub_TmpTbl_Roamer',CONCAT(DATA_DATE,',',DATA_HOUR,',',PU_ID,',',TECH_MASK,',',PU_ID,'_',SUB_WORKER_ID,' Done: ',TIMESTAMPDIFF(SECOND,START_TIME,SYSDATE()),' seconds.'), NOW());
END$$
DELIMITER ;
