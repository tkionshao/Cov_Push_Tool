CREATE DEFINER=`covmo`@`%` PROCEDURE `SP_Generate_Global_Statistic_DS_Sub_TmpTbl_Tile`(IN DATA_DATE DATE,IN DATA_HOUR TINYINT(2),IN PU_ID MEDIUMINT(9),IN TECH_MASK TINYINT(2), IN GT_DB VARCHAR(100),IN WORKER_ID VARCHAR(10),in DS_FLAG TINYINT(2),IN TileResolution VARCHAR(10))
BEGIN
	DECLARE START_TIME DATETIME DEFAULT SYSDATE();
	DECLARE STEP_START_TIME DATETIME DEFAULT SYSDATE();
	DECLARE SP_Process VARCHAR(100) DEFAULT NULL;
	DECLARE SUB_WORKER_ID VARCHAR(10) DEFAULT CONNECTION_ID();
	DECLARE EX_DATE VARCHAR(100) DEFAULT NULL;
	DECLARE KPI_ERROR CONDITION FOR SQLSTATE '99999' ;
		
	INSERT INTO gt_gw_main.SP_LOG VALUES(GT_DB,'SP_Generate_Global_Statistic_DS_Sub_TmpTbl_Tile',CONCAT(DATA_DATE,',',DATA_HOUR,',',PU_ID,',',TECH_MASK,',',PU_ID,'_',SUB_WORKER_ID,' Start'), NOW());
	SET STEP_START_TIME := SYSDATE();
	
	SET EX_DATE=CONCAT(DATE_FORMAT(DATA_DATE,'%Y%m%d_'),DATA_HOUR,'_',WORKER_ID);
	
  	CALL gt_gw_main.`SP_Sub_Set_Session_Param_LTE`('');
	
	SET @TILE_ID_LVL1 = CONCAT('TILE_ID_',gt_covmo_csv_get(TileResolution,1));
	SET @TILE_ID_LVL2 = CONCAT('TILE_ID_',gt_covmo_csv_get(TileResolution,2));
	
	IF TECH_MASK=2 THEN 	
		SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS gt_global_statistic.tmp_ds_tile_umts_',PU_ID,'_',SUB_WORKER_ID,';');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		
		SET SP_Process = 'SP_Generate_Global_Statistic_DS_tile_umts';
		SET @SqlCmd=CONCAT('CREATE TEMPORARY TABLE gt_global_statistic.tmp_ds_tile_umts_',PU_ID,'_',SUB_WORKER_ID,' (
					  `TILE_ID` BIGINT(20) NOT NULL,
					  `DATA_DATE` DATETIME NOT NULL,
					  `DATA_HOUR` TINYINT(2) NOT NULL,
					  `',@TILE_ID_LVL2,'` BIGINT(20) DEFAULT NULL,
					  `',@TILE_ID_LVL1,'` BIGINT(20) DEFAULT NULL,
					  `RSCP_SUM` DOUBLE DEFAULT NULL,
					  `RSCP_CNT` INT(11) DEFAULT NULL,
					  `ECNO_SUM` DOUBLE DEFAULT NULL,
					  `ECNO_CNT` INT(11) DEFAULT NULL,
					  `UMTS_CS_CALL_CNT` INT(11) DEFAULT NULL,
					  `UMTS_PS_CALL_CNT` INT(11) DEFAULT NULL,
					  `UMTS_VOICE_DROP_CNT` INT(11) DEFAULT NULL,
					  `UMTS_PS_DROP_CNT` INT(11) DEFAULT NULL,
					  `UMTS_CS_CALL_DURATION` DOUBLE DEFAULT NULL,
					  `UMTS_CS_BLOCK_CNT` INT(11) DEFAULT NULL,
					  `UMTS_PS_BLOCK_CNT` INT(11) DEFAULT NULL,
					  `UMTS_UL_VOLUME` DOUBLE DEFAULT NULL,
					  `UMTS_DL_VOLUME` DOUBLE DEFAULT NULL,
					  `UMTS_UL_THROUPUT_SUM` DOUBLE DEFAULT NULL,
					  `UMTS_UL_THROUPUT_CNT` INT(11) DEFAULT NULL,
					  `UMTS_DL_THROUPUT_SUM` DOUBLE DEFAULT NULL,
					  `UMTS_DL_THROUPUT_CNT` INT(11) DEFAULT NULL,
					  `UMTS_CALL_SETUP_TIME_SUM` double DEFAULT NULL,
					  `UMTS_CALL_SETUP_TIME_CNT` int(11) DEFAULT NULL,
					  `UMTS_CS_CALL_SETUP_TIME_SUM` INT(11) DEFAULT NULL,
					  `UMTS_CS_CALL_SETUP_TIME_CNT` INT(11) DEFAULT NULL,
					  `UMTS_PS_CALL_SETUP_TIME_SUM` INT(11) DEFAULT NULL,
					  `UMTS_PS_CALL_SETUP_TIME_CNT` INT(11) DEFAULT NULL,
					  `UMTS_CS_SETUP_FAILURE_CNT` int(11) DEFAULT NULL,
					  `UMTS_PS_SETUP_FAILURE_CNT` int(11) DEFAULT NULL,
					  `UMTS_MAX_UL_THROUPUT` DOUBLE DEFAULT NULL,
					  `UMTS_MAX_DL_THROUPUT` DOUBLE DEFAULT NULL,
					  `UMTS_IRAT_HO_ATMP` int(11) DEFAULT NULL,
					  `UMTS_IRAT_HO_FAIL` int(11) DEFAULT NULL,
					  `REG_1_ID` BIGINT(20) DEFAULT NULL,
					  `REG_2_ID` BIGINT(20) DEFAULT NULL,
					  `REG_3_ID` BIGINT(20) DEFAULT NULL,
					  PRIMARY KEY (`TILE_ID`,`DATA_DATE`,`DATA_HOUR`)
					) ENGINE=MYISAM DEFAULT CHARSET=latin1;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		
		SET STEP_START_TIME := SYSDATE();
		SET @SqlCmd=CONCAT('SELECT spider_bg_direct_sql(CONCAT(''CALL gt_global_statistic.',SP_Process,'(''''',DATA_DATE,''''',',DATA_HOUR,',',PU_ID,',',TECH_MASK,',''''',GT_DB,''''',''''',TileResolution,''''');'') 
			, ''gt_global_statistic.tmp_ds_tile_umts_',PU_ID,'_',SUB_WORKER_ID,'''
			, CONCAT(''HOST '''''',REPLACE(gt_strtok(DS_AP_URI,3,'':''),''/'',''''),'''''', PORT '''''',REPLACE(gt_strtok(DS_AP_URI,4,'':''),''/'',''''),'''''',USER '''''',`DS_AP_USER`,'''''', PASSWORD '''''',`DS_AP_PASSWORD`,'''''''')
			) INTO @bb FROM `gt_covmo`.`rnc_information` WHERE `RNC`=',PU_ID,' AND `TECHNOLOGY`=''UMTS''
			;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
	 	INSERT INTO gt_gw_main.SP_LOG VALUES(GT_DB,'SP_Generate_Global_Statistic_DS_Sub_TmpTbl_Tile',CONCAT('SELECT spider_bg_direct_sql,umts:',GT_DB,'_',WORKER_ID), NOW());
		SET STEP_START_TIME := SYSDATE();
		
		IF @bb=1 THEN 			
			SET @SqlCmd=CONCAT('INSERT INTO gt_global_statistic.tmp_ds_tile_umts_',EX_DATE,'
						SELECT * FROM gt_global_statistic.tmp_ds_tile_umts_',PU_ID,'_',SUB_WORKER_ID,';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
			INSERT INTO gt_gw_main.SP_LOG VALUES(GT_DB,'SP_Generate_Global_Statistic_DS_Sub_TmpTbl_Tile',CONCAT('INSERT tmp_tile_umts_',EX_DATE,' cost:',TIMESTAMPDIFF(SECOND,STEP_START_TIME,SYSDATE()),' sec.'), NOW());
			SET STEP_START_TIME := SYSDATE();
		ELSE
			INSERT INTO gt_gw_main.SP_LOG VALUES(GT_DB,'SP_Generate_Global_Statistic_DS_Sub_TmpTbl_Tile',CONCAT('INSERT ERROR,',WORKER_ID), NOW());
			INSERT INTO `gt_gw_main`.`tbl_rpt_error` (`PID`,`CreateTime`,`error_str`) VALUES (WORKER_ID,NOW(),'Spider SP EXECUTE Failed - SP_Generate_Global_Statistic_DS_Sub_TmpTbl_Tile, UMTS');
			SIGNAL KPI_ERROR
				SET MESSAGE_TEXT = 'Spider SP EXECUTE Failed - SP_Generate_Global_Statistic_DS_Sub_TmpTbl_Tile, UMTS';
		END IF;	
			
		SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS gt_global_statistic.tmp_ds_tile_umts_',PU_ID,'_',SUB_WORKER_ID,';');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		
	ELSEIF TECH_MASK=1 THEN 	
		SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS gt_global_statistic.tmp_ds_tile_gsm_',PU_ID,'_',SUB_WORKER_ID,';');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		
		SET SP_Process = 'SP_Generate_Global_Statistic_DS_tile_gsm';
		SET @SqlCmd=CONCAT('CREATE TEMPORARY TABLE gt_global_statistic.tmp_ds_tile_gsm_',PU_ID,'_',SUB_WORKER_ID,' (
					  `TILE_ID` BIGINT(20) NOT NULL,
					  `DATA_DATE` DATETIME NOT NULL,
					  `DATA_HOUR` TINYINT(2) NOT NULL,
					  `',@TILE_ID_LVL2,'` BIGINT(20) DEFAULT NULL,
					  `',@TILE_ID_LVL1,'` BIGINT(20) DEFAULT NULL,
					  `RXLEV_SUM` double DEFAULT NULL,
					  `RXLEV_CNT` int(11) DEFAULT NULL,
					  `RXQUAL_SUM` double DEFAULT NULL,
					  `RXQUAL_CNT` int(11) DEFAULT NULL,
					  `GSM_CS_CALL_CNT` int(11) DEFAULT NULL, 
					  `GSM_PS_CALL_CNT` int(11) DEFAULT NULL,
					  `GSM_VOICE_DROP_CNT` int(11) DEFAULT NULL,
					  `GSM_GPRS_DROP_CNT` int(11) DEFAULT NULL,
					  `GSM_SMS_DROP_CNT` int(11) DEFAULT NULL,
					  `GSM_CS_BLOCK_CNT` int(11) DEFAULT NULL,
					  `GSM_PS_BLOCK_CNT` int(11) DEFAULT NULL,
					  `GSM_CS_CALL_DURATION` double DEFAULT NULL,
					  `GSM_CALL_SETUP_TIME_SUM` double DEFAULT NULL,
					  `GSM_CALL_SETUP_TIME_CNT` int(11) DEFAULT NULL,
					  `GSM_CS_CALL_SETUP_TIME_SUM` INT(11) DEFAULT NULL,
					  `GSM_CS_CALL_SETUP_TIME_CNT` INT(11) DEFAULT NULL,
					  `GSM_PS_CALL_SETUP_TIME_SUM` INT(11) DEFAULT NULL,
					  `GSM_PS_CALL_SETUP_TIME_CNT` INT(11) DEFAULT NULL,
					  `GSM_CS_SETUP_FAILURE_CNT` int(11) DEFAULT NULL,
					  `GSM_PS_SETUP_FAILURE_CNT` int(11) DEFAULT NULL,
					  `REG_1_ID` BIGINT(20) DEFAULT NULL,
					  `REG_2_ID` BIGINT(20) DEFAULT NULL,
					  `REG_3_ID` BIGINT(20) DEFAULT NULL,
					  PRIMARY KEY (`TILE_ID`,`DATA_DATE`,`DATA_HOUR`)
					) ENGINE=MYISAM DEFAULT CHARSET=latin1;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		
		SET STEP_START_TIME := SYSDATE();
		SET @SqlCmd=CONCAT('SELECT spider_bg_direct_sql(CONCAT(''CALL gt_global_statistic.',SP_Process,'(''''',DATA_DATE,''''',',DATA_HOUR,',',PU_ID,',',TECH_MASK,',''''',GT_DB,''''',''''',TileResolution,''''');'') 
			, ''gt_global_statistic.tmp_ds_tile_gsm_',PU_ID,'_',SUB_WORKER_ID,'''
			, CONCAT(''HOST '''''',REPLACE(gt_strtok(DS_AP_URI,3,'':''),''/'',''''),'''''', PORT '''''',REPLACE(gt_strtok(DS_AP_URI,4,'':''),''/'',''''),'''''',USER '''''',`DS_AP_USER`,'''''', PASSWORD '''''',`DS_AP_PASSWORD`,'''''''')
			) INTO @bb FROM `gt_covmo`.`RNC_information` WHERE `RNC`=',PU_ID,' AND `TECHNOLOGY`=''GSM''
			;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
	 	INSERT INTO gt_gw_main.SP_LOG VALUES(GT_DB,'SP_Generate_Global_Statistic_DS_Sub_TmpTbl_Tile',CONCAT('SELECT spider_bg_direct_sql,gsm:',GT_DB,'_',WORKER_ID), NOW());
		SET STEP_START_TIME := SYSDATE();
		
		IF @bb=1 THEN 
			SET @SqlCmd=CONCAT('INSERT INTO gt_global_statistic.tmp_ds_tile_gsm_',EX_DATE,'
						SELECT * FROM gt_global_statistic.tmp_ds_tile_gsm_',PU_ID,'_',SUB_WORKER_ID,';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
			INSERT INTO gt_gw_main.SP_LOG VALUES(GT_DB,'SP_Generate_Global_Statistic_DS_Sub_TmpTbl_Tile',CONCAT('INSERT tmp_tile_gsm_',EX_DATE,' cost:',TIMESTAMPDIFF(SECOND,STEP_START_TIME,SYSDATE()),' sec.'), NOW());
			SET STEP_START_TIME := SYSDATE();
		ELSE
			INSERT INTO gt_gw_main.SP_LOG VALUES(GT_DB,'SP_Generate_Global_Statistic_DS_Sub_TmpTbl_Tile',CONCAT('INSERT ERROR,',WORKER_ID), NOW());
			INSERT INTO `gt_gw_main`.`tbl_rpt_error` (`PID`,`CreateTime`,`error_str`) VALUES (WORKER_ID,NOW(),'Spider SP EXECUTE Failed - SP_Generate_Global_Statistic_DS_Sub_TmpTbl_Tile,GSM');
			SIGNAL KPI_ERROR
				SET MESSAGE_TEXT = 'Spider SP EXECUTE Failed - SP_Generate_Global_Statistic_DS_Sub_TmpTbl_Tile,GSM';
		END IF;	
			
		SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS gt_global_statistic.tmp_ds_tile_gsm_',PU_ID,'_',SUB_WORKER_ID,';');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		
	ELSEIF TECH_MASK=4 THEN 	
		SET SP_Process = 'SP_Generate_Global_Statistic_DS_tile_lte';					
		SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS gt_global_statistic.tmp_ds_tile_lte_',PU_ID,'_',SUB_WORKER_ID,';');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		
		SET @SqlCmd=CONCAT('CREATE TEMPORARY TABLE gt_global_statistic.tmp_ds_tile_lte_',PU_ID,'_',SUB_WORKER_ID,' 
					(
					  `TILE_ID` BIGINT(20) NOT NULL,
					  `DATA_DATE` DATETIME NOT NULL,
					  `DATA_HOUR` TINYINT(4) NOT NULL,
					  `',@TILE_ID_LVL2,'` BIGINT(20)  NULL,
					  `',@TILE_ID_LVL1,'` BIGINT(20)  NULL,
					  `RSRP_SUM` DOUBLE DEFAULT NULL,
					  `RSRP_CNT` INT(11) DEFAULT NULL,
					  `RSRQ_SUM` DOUBLE DEFAULT NULL,
					  `RSRQ_CNT` INT(11) DEFAULT NULL,
					  `LTE_CALL_CNT` INT(11) DEFAULT NULL,
					  `LTE_DROP_CNT` INT(11) DEFAULT NULL,
					  `LTE_BLOCK_CNT` INT(11) DEFAULT NULL,
					  `LTE_UL_VOLUME_SUM` DOUBLE DEFAULT NULL,
					  `LTE_DL_VOLUME_SUM` DOUBLE DEFAULT NULL,
					  `LTE_UL_THROUPUT_SUM` DOUBLE DEFAULT NULL,
					  `LTE_UL_THROUPUT_CNT` INT(11) DEFAULT NULL,
					  `LTE_DL_THROUPUT_SUM` DOUBLE DEFAULT NULL,
					  `LTE_DL_THROUPUT_CNT` INT(11) DEFAULT NULL,
					  `LATENCY_SUM` DOUBLE DEFAULT NULL,
					  `LATENCY_CNT` INT(11) DEFAULT NULL,
					  `LTE_CALL_SETUP_TIME_SUM` INT(11) DEFAULT NULL,
					  `LTE_CALL_SETUP_TIME_CNT` INT(11) DEFAULT NULL,
					  `LTE_SETUP_FAILURE_CNT` INT(11) DEFAULT NULL,
					  `SRVCC_ATTEMPT_CNT` INT(11) DEFAULT NULL,
					  `SRVCC_FAILURE_CNT` INT(11) DEFAULT NULL,
					  `S1_HO_ATTEMPT` INT(11) DEFAULT NULL,
					  `S1_HO_FAILURE` INT(11) DEFAULT NULL,
					  `X2_HO_ATTEMPT` INT(11) DEFAULT NULL,
					  `X2_HO_FAILURE` INT(11) DEFAULT NULL,
					  `LTE_MAX_UL_THROUPUT` DOUBLE DEFAULT NULL,
					  `LTE_MAX_DL_THROUPUT` DOUBLE DEFAULT NULL,
					  `LTE_IRAT_TO_UMTS_ATMP` INT(11) DEFAULT NULL,
					  `LTE_IRAT_TO_GERAN_ATMP` INT(11) DEFAULT NULL,
					  `LTE_IRAT_TO_CDMA_ATMP` INT(11) DEFAULT NULL,
					  `LTE_IRAT_TO_UMTS_FAIL` INT(11) DEFAULT NULL,
					  `LTE_IRAT_TO_GERAN_FAIL` INT(11) DEFAULT NULL,
					  `LTE_IRAT_TO_CDMA_FAIL` INT(11) DEFAULT NULL,
					  `REG_1_ID` BIGINT(20) DEFAULT NULL,
					  `REG_2_ID` BIGINT(20) DEFAULT NULL,
					  `REG_3_ID` BIGINT(20) DEFAULT NULL
					  ,PRIMARY KEY (`TILE_ID`,`DATA_DATE`,`DATA_HOUR`)
					) ENGINE=MYISAM DEFAULT CHARSET=latin1;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		INSERT INTO gt_gw_main.SP_LOG VALUES(GT_DB,'SP_Generate_Global_Statistic_DS_Sub_TmpTbl_Tile',CONCAT(DATA_DATE,',',DATA_HOUR,',',PU_ID,',',TECH_MASK,',',PU_ID,'_',SUB_WORKER_ID,' create table: ',TIMESTAMPDIFF(SECOND,STEP_START_TIME,SYSDATE()),' seconds.'), NOW());
		SET STEP_START_TIME := SYSDATE();
		
		SET @SqlCmd=CONCAT('SELECT spider_bg_direct_sql(CONCAT(''CALL gt_global_statistic.',SP_Process,'(''''',DATA_DATE,''''',',DATA_HOUR,',',PU_ID,',',TECH_MASK,',''''',GT_DB,''''',''''',TileResolution,''''');'') 
					, ''gt_global_statistic.tmp_ds_tile_lte_',PU_ID,'_',SUB_WORKER_ID,'''
					, CONCAT(''HOST '''''',REPLACE(gt_strtok(DS_AP_URI,3,'':''),''/'',''''),'''''', PORT '''''',REPLACE(gt_strtok(DS_AP_URI,4,'':''),''/'',''''),'''''',USER '''''',`DS_AP_USER`,'''''', PASSWORD '''''',`DS_AP_PASSWORD`,'''''''')
					) INTO @bb FROM `gt_covmo`.`rnc_information` WHERE `RNC`=',PU_ID,' AND `TECHNOLOGY`=''LTE''
					;');
	SELECT @SqlCmd;
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
	 	INSERT INTO gt_gw_main.SP_LOG VALUES(GT_DB,'SP_Generate_Global_Statistic_DS_Sub_TmpTbl_Tile',CONCAT('spider_bg_direct_sql tmp_materialization_lte_',PU_ID,'_',SUB_WORKER_ID,' ,lte cost:',TIMESTAMPDIFF(SECOND,STEP_START_TIME,SYSDATE()),' sec.'), NOW());
		SET STEP_START_TIME := SYSDATE();
	
		IF @bb=1 THEN 
			SET @SqlCmd=CONCAT('INSERT INTO gt_global_statistic.tmp_ds_tile_lte_',EX_DATE,'
						SELECT * FROM gt_global_statistic.tmp_ds_tile_lte_',PU_ID,'_',SUB_WORKER_ID,';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
			INSERT INTO gt_gw_main.SP_LOG VALUES(GT_DB,'SP_Generate_Global_Statistic_DS_Sub_TmpTbl_Tile',CONCAT('INSERT tmp_tile_lte_',EX_DATE,' cost:',TIMESTAMPDIFF(SECOND,STEP_START_TIME,SYSDATE()),' sec.'), NOW());
			SET STEP_START_TIME := SYSDATE();
		ELSE
			INSERT INTO `gt_gw_main`.`tbl_rpt_error` (`PID`,`CreateTime`,`error_str`) VALUES (WORKER_ID,NOW(),'Spider SP EXECUTE Failed - SP_Generate_Global_Statistic_DS_Sub_TmpTbl_Tile');
			SIGNAL KPI_ERROR
				SET MESSAGE_TEXT = 'Spider SP EXECUTE Failed - SP_Generate_Global_Statistic_DS_Sub_TmpTbl_Tile';
	 		INSERT INTO gt_gw_main.SP_LOG VALUES(GT_DB,'SP_Generate_Global_Statistic_DS_Sub_TmpTbl_Tile',CONCAT('INSERT ERROR,',PU_ID,'_',SUB_WORKER_ID,' cost:',TIMESTAMPDIFF(SECOND,STEP_START_TIME,SYSDATE()),' sec.'), NOW());
			SET STEP_START_TIME := SYSDATE();
		END IF;	
			
		SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS gt_global_statistic.tmp_ds_tile_lte_',PU_ID,'_',SUB_WORKER_ID,';');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
					
	 	INSERT INTO gt_gw_main.SP_LOG VALUES(GT_DB,'SP_Generate_Global_Statistic_DS_Sub_TmpTbl_Tile',CONCAT('DROP TEMPORARY,',PU_ID,'_',SUB_WORKER_ID,' cost:',TIMESTAMPDIFF(SECOND,STEP_START_TIME,SYSDATE()),' sec.'), NOW());
		SET STEP_START_TIME := SYSDATE();
	END IF;	
	INSERT INTO gt_gw_main.SP_LOG VALUES(GT_DB,'SP_Generate_Global_Statistic_DS_Sub_TmpTbl_Tile',CONCAT(DATA_DATE,',',DATA_HOUR,',',PU_ID,',',TECH_MASK,',',PU_ID,'_',SUB_WORKER_ID,' Done: ',TIMESTAMPDIFF(SECOND,START_TIME,SYSDATE()),' seconds.'), NOW());
