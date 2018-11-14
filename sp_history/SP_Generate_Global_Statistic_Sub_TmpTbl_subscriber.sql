DELIMITER $$
USE `gt_global_statistic`$$
DROP PROCEDURE IF EXISTS `SP_CreateDB_LTE`$$
CREATE DEFINER=`covmo`@`%` PROCEDURE `SP_Generate_Global_Statistic_Sub_TmpTbl_subscriber`(IN DATA_DATE DATE,IN DATA_HOUR TINYINT(2),IN PU_ID MEDIUMINT(9),IN TECH_MASK TINYINT(2), IN GT_DB VARCHAR(100), WORKER_ID VARCHAR(10),IN IMSI_CELL TINYINT(2))
BEGIN
	DECLARE START_TIME DATETIME DEFAULT SYSDATE();
	DECLARE STEP_START_TIME DATETIME DEFAULT SYSDATE();
	DECLARE SP_Process VARCHAR(100) DEFAULT NULL;
	DECLARE SUB_WORKER_ID VARCHAR(10) DEFAULT CONNECTION_ID();
	DECLARE KPI_ERROR CONDITION FOR SQLSTATE '99999' ;
	
	SET @EX_DATE=CONCAT(DATE_FORMAT(DATA_DATE,'%Y%m%d_'),DATA_HOUR,'_',WORKER_ID);
	
	SET @SqlCmd=CONCAT('SELECT `att_value` INTO @ZOOM_LEVEL FROM gt_covmo.`sys_config` WHERE `group_name`=''system'' AND att_name = ''MapResolution'';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
  	CALL gt_gw_main.`SP_Sub_Set_Session_Param_LTE`('');
 
	
	IF TECH_MASK=2 THEN 
		SET SP_Process = 'SP_Generate_Global_Statistic_subscriber_umts';
		SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS gt_global_statistic.tmp_subscriber_umts_',PU_ID,'_',SUB_WORKER_ID,';');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		
		SET @SqlCmd=CONCAT('CREATE TEMPORARY TABLE gt_global_statistic.tmp_subscriber_umts_',PU_ID,'_',SUB_WORKER_ID,' 
					(
					`DATA_DATE` date DEFAULT NULL,
							 `END_TIME` DATETIME DEFAULT NULL,							
							 `DURATION` MEDIUMINT(9) DEFAULT NULL,
							 `IMSI` varchar(20) DEFAULT NULL,
							 `CELL_ID` VARCHAR(50) DEFAULT NULL,
							 `DROP_REASON` MEDIUMINT(9) DEFAULT NULL,
							 `LAST_RSCP` DOUBLE DEFAULT NULL,
							 `LAST_ECN0` DOUBLE DEFAULT NULL,
							 `FREQ_BAND` SMALLINT(6) DEFAULT NULL,
							 `ILE_ID` BIGINT(20) DEFAULT NULL	
					) ENGINE=MYISAM DEFAULT CHARSET=latin1 DELAY_KEY_WRITE=1;');
	
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		
		INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'SP_Generate_Global_Statistic_Sub_TmpTbl_SUBSCRIBER',CONCAT(GT_DB,' INSERT INTO tbl_',GT_DB,'_',WORKER_ID), NOW());	
		SET @SqlCmd=CONCAT('SELECT spider_bg_direct_sql(CONCAT(''CALL gt_global_statistic.',SP_Process,'(''''',DATA_DATE,''''',',DATA_HOUR,',',PU_ID,',',TECH_MASK,',''''',GT_DB,''''',',IMSI_CELL,');'') 
					, ''gt_global_statistic.tmp_subscriber_umts_',PU_ID,'_',SUB_WORKER_ID,'''
					, CONCAT(''HOST '''''',REPLACE(gt_strtok(DS_AP_URI,3,'':''),''/'',''''),'''''', PORT '''''',REPLACE(gt_strtok(DS_AP_URI,4,'':''),''/'',''''),'''''',USER '''''',`DS_AP_USER`,'''''', PASSWORD '''''',`DS_AP_PASSWORD`,'''''''')
					) INTO @bb FROM `gt_covmo`.`rnc_information` WHERE `RNC`=',PU_ID,' AND `TECHNOLOGY`=''UMTS''
					;');
		
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
	 	INSERT INTO gt_gw_main.SP_LOG VALUES(GT_DB,'SP_Generate_Global_Statistic_Sub_TmpTbl_SUBSCRIBER',CONCAT('SELECT spider_bg_direct_sql,umts:',GT_DB,'_',WORKER_ID), NOW());
		SET STEP_START_TIME := SYSDATE();
		
		IF @bb=1 THEN 			
			SET @SqlCmd=CONCAT('INSERT INTO gt_global_statistic.tmp_subscriber_umts_',@EX_DATE,'
						SELECT * FROM gt_global_statistic.tmp_subscriber_umts_',PU_ID,'_',SUB_WORKER_ID,';');
		
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
			INSERT INTO gt_gw_main.SP_LOG VALUES(GT_DB,'SP_Generate_Global_Statistic_Sub_TmpTbl_SUBSCRIBER',CONCAT('tmp_imsi_umts_',PU_ID,'_',SUB_WORKER_ID,' INSERT tmp_cell_tile_umts cost:',TIMESTAMPDIFF(SECOND,STEP_START_TIME,SYSDATE()),' sec.'), NOW());
			SET STEP_START_TIME := SYSDATE();		
			
			SET @SqlCmd=CONCAT('UPDATE gt_global_statistic.tmp_table_call_cnt_',WORKER_ID,' 
						SET `IsSuccess`=1
						WHERE `DATA_DATE`=''',DATA_DATE,''' AND `DATA_HOUR`=',DATA_HOUR,'
							AND `PU_ID`=',PU_ID,' AND `TECH_MASK`=',TECH_MASK,'
						;');
		
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
			INSERT INTO gt_gw_main.SP_LOG VALUES(GT_DB,'SP_Generate_Global_Statistic_Sub_TmpTbl_SUBSCRIBER',CONCAT('UPDATE tmp_table_call_cnt_',PU_ID,'_',SUB_WORKER_ID,' INSERT tmp_cell_tile_lte cost:',TIMESTAMPDIFF(SECOND,STEP_START_TIME,SYSDATE()),' sec.'), NOW());
		ELSE
			INSERT INTO gt_gw_main.SP_LOG VALUES(GT_DB,'SP_Generate_Global_Statistic_Sub_TmpTbl_SUBSCRIBER',CONCAT('INSERT ERROR,tmp_subsciber_umts_',PU_ID,'_',SUB_WORKER_ID), NOW());
			INSERT INTO `gt_gw_main`.`tbl_rpt_error` (`PID`,`CreateTime`,`error_str`) VALUES (WORKER_ID,NOW(),'Spider SP EXECUTE Failed - SP_Generate_Global_Statistic_Sub_TmpTbl_SUBSCRIBER');
			SIGNAL KPI_ERROR
				SET MESSAGE_TEXT = 'Spider SP EXECUTE Failed - SP_Generate_Global_Statistic_Sub_TmpTbl_SUBSCRIBER';
		END IF;	
	ELSEIF TECH_MASK=4 THEN 	
		SET SP_Process = 'SP_Generate_Global_Statistic_subscriber_lte';
		SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS gt_global_statistic.tmp_subscriber_lte_',PU_ID,'_',SUB_WORKER_ID,';');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;	
	
		SET @SqlCmd=CONCAT('CREATE TEMPORARY TABLE gt_global_statistic.tmp_subscriber_lte_',PU_ID,'_',SUB_WORKER_ID,' 
				(
					 `DATA_DATE` date DEFAULT NULL,
							 `END_TIME` DATETIME DEFAULT NULL,							
							 `DURATION` MEDIUMINT(9) DEFAULT NULL,
							 `IMSI` varchar(20) DEFAULT NULL,
							 `CELL_ID` VARCHAR(50) DEFAULT NULL,
							 `DROP_REASON` MEDIUMINT(9) DEFAULT NULL,
							 `LAST_RSRP` DOUBLE DEFAULT NULL,
							 `LAST_RSRQ` DOUBLE DEFAULT NULL,
							 `FREQ_BAND` SMALLINT(6) DEFAULT NULL,
							 `TILE_ID` BIGINT(20) DEFAULT NULL	
				) ENGINE=MYISAM DEFAULT CHARSET=utf8;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		
		SET @SqlCmd=CONCAT('SELECT spider_bg_direct_sql(CONCAT(''CALL gt_global_statistic.',SP_Process,'(''''',DATA_DATE,''''',',DATA_HOUR,',',PU_ID,',',TECH_MASK,',''''',GT_DB,''''',',IMSI_CELL,');'') 
					, ''gt_global_statistic.tmp_subscriber_lte_',PU_ID,'_',SUB_WORKER_ID,'''
					, CONCAT(''HOST '''''',REPLACE(gt_strtok(DS_AP_URI,3,'':''),''/'',''''),'''''', PORT '''''',REPLACE(gt_strtok(DS_AP_URI,4,'':''),''/'',''''),'''''',USER '''''',`DS_AP_USER`,'''''', PASSWORD '''''',`DS_AP_PASSWORD`,'''''''')
					) INTO @bb FROM `gt_covmo`.`rnc_information` WHERE `RNC`=',PU_ID,' AND `TECHNOLOGY`=''LTE''
					;');	
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
	 	INSERT INTO gt_gw_main.SP_LOG VALUES(GT_DB,'SP_Generate_Global_Statistic_Sub_TmpTbl_IMSI',CONCAT('spider_bg_direct_sql tmp_subscriber_lte_',PU_ID,'_',SUB_WORKER_ID,' cost:',TIMESTAMPDIFF(SECOND,STEP_START_TIME,SYSDATE()),' sec.'), NOW());
		SET STEP_START_TIME := SYSDATE();
		IF @bb=1 THEN 
			SET @SqlCmd=CONCAT('INSERT INTO gt_global_statistic.tmp_subscriber_lte_',@EX_DATE,'
						SELECT * FROM gt_global_statistic.tmp_subscriber_lte_',PU_ID,'_',SUB_WORKER_ID,';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
			INSERT INTO gt_gw_main.SP_LOG VALUES(GT_DB,'SP_Generate_Global_Statistic_Sub_TmpTbl_SUBSCRIBER',CONCAT('tmp_subscriber_lte_',PU_ID,'_',SUB_WORKER_ID,' INSERT tmp_cell_tile_lte cost:',TIMESTAMPDIFF(SECOND,STEP_START_TIME,SYSDATE()),' sec.'), NOW());
			SET STEP_START_TIME := SYSDATE();		
		
			SET @SqlCmd=CONCAT('UPDATE gt_global_statistic.tmp_table_call_cnt_',WORKER_ID,' 
						SET `IsSuccess`=1
						WHERE `DATA_DATE`=''',DATA_DATE,''' AND `DATA_HOUR`=',DATA_HOUR,'
							AND `PU_ID`=',PU_ID,' AND `TECH_MASK`=',TECH_MASK,'
						;');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
			INSERT INTO gt_gw_main.SP_LOG VALUES(GT_DB,'SP_Generate_Global_Statistic_Sub_TmpTbl_SUBSCRIBER',CONCAT('UPDATE tmp_table_call_cnt_',PU_ID,'_',SUB_WORKER_ID,' INSERT tmp_cell_tile_lte cost:',TIMESTAMPDIFF(SECOND,STEP_START_TIME,SYSDATE()),' sec.'), NOW());
		ELSE
			INSERT INTO `gt_gw_main`.`tbl_rpt_error` (`PID`,`CreateTime`,`error_str`) VALUES (WORKER_ID,NOW(),'Spider SP EXECUTE Failed - SP_Generate_Global_Statistic_Sub_TmpTbl_SUBSCRIBER');
			SIGNAL KPI_ERROR
				SET MESSAGE_TEXT = 'Spider SP EXECUTE Failed - SP_Generate_Global_Statistic_Sub_TmpTbl_SUBSCRIBER';
	 		INSERT INTO gt_gw_main.SP_LOG VALUES(GT_DB,'SP_Generate_Global_Statistic_Sub_TmpTbl_SUBSCRIBER',CONCAT('INSERT ERROR,tmp_subscriber_lte_',PU_ID,'_',SUB_WORKER_ID,' cost:',TIMESTAMPDIFF(SECOND,STEP_START_TIME,SYSDATE()),' sec.'), NOW());
			SET STEP_START_TIME := SYSDATE();
		END IF;	
		SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS gt_global_statistic.tmp_subscriber_lte_',PU_ID,'_',SUB_WORKER_ID,';');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
					
	 	INSERT INTO gt_gw_main.SP_LOG VALUES(GT_DB,'SP_Generate_Global_Statistic_Sub_TmpTbl_SUBSCRIBER',CONCAT('DROP tmp_imsi_lte_,',PU_ID,'_',SUB_WORKER_ID,' cost:',TIMESTAMPDIFF(SECOND,STEP_START_TIME,SYSDATE()),' sec.'), NOW());
		SET STEP_START_TIME := SYSDATE();	
	
	END IF;	
	INSERT INTO gt_gw_main.SP_LOG VALUES(GT_DB,'SP_Generate_Global_Statistic_Sub_TmpTbl_SUBSCRIBER',CONCAT(DATA_DATE,',',DATA_HOUR,',',PU_ID,',',TECH_MASK,',',PU_ID,'_',SUB_WORKER_ID,' Done: ',TIMESTAMPDIFF(SECOND,START_TIME,SYSDATE()),' seconds.'), NOW());
END$$
DELIMITER ;
