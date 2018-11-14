DELIMITER $$
USE `gt_global_statistic`$$
DROP PROCEDURE IF EXISTS `SP_CreateDB_LTE`$$
CREATE DEFINER=`covmo`@`%` PROCEDURE `SP_Generate_Global_Statistic_Sub_TmpTbl_failcause`(IN DATA_DATE DATE,IN DATA_HOUR TINYINT(2),IN PU_ID MEDIUMINT(9),IN TECH_MASK TINYINT(2), IN GT_DB VARCHAR(100),IN WORKER_ID VARCHAR(10))
BEGIN
	DECLARE START_TIME DATETIME DEFAULT SYSDATE();
	DECLARE STEP_START_TIME DATETIME DEFAULT SYSDATE();
	DECLARE SP_Process VARCHAR(100) DEFAULT NULL;
	DECLARE SUB_WORKER_ID VARCHAR(10) DEFAULT CONNECTION_ID();
	DECLARE EX_DATE VARCHAR(100) DEFAULT NULL;
	DECLARE KPI_ERROR CONDITION FOR SQLSTATE '99999' ;
	DECLARE EXIT HANDLER FOR 1146
		
	INSERT INTO gt_gw_main.SP_LOG VALUES(GT_DB,'SP_Generate_Global_Statistic_Sub_TmpTbl_failcause',CONCAT(DATA_DATE,',',DATA_HOUR,',',PU_ID,',',TECH_MASK,',',PU_ID,'_',SUB_WORKER_ID,' Start'), NOW());
	SET STEP_START_TIME := SYSDATE();
	
	SET EX_DATE=CONCAT(DATE_FORMAT(DATA_DATE,'%Y%m%d_'),DATA_HOUR,'_',WORKER_ID);
  	
	IF TECH_MASK=2 THEN 	
		SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS gt_global_statistic.tmp_failcause_umts_',PU_ID,'_',SUB_WORKER_ID,';');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		
		SET SP_Process = 'SP_Generate_Global_Statistic_failcause_umts';
		SET @SqlCmd=CONCAT('CREATE TEMPORARY TABLE gt_global_statistic.tmp_failcause_umts_',PU_ID,'_',SUB_WORKER_ID,' (
					  `DATA_DATE` date NOT NULL,
					  `TILE_ID` bigint(20) NOT NULL,
					  `RNC_ID` mediumint(9) NOT NULL,
					  `SITE_ID` varchar(20) NOT NULL,
					  `CELL_ID` mediumint(9) NOT NULL,
					  `IMSI` varchar(20) NOT NULL,
					  `IMEI` varchar(20) NOT NULL,
					  `EVENT_ID` smallint(6) NOT NULL,
					  `FAILURE_EVENT_ID` mediumint(9) NOT NULL,
					  `FAILURE_EVENT_CAUSE` mediumint(9) NOT NULL,
					  `FAILURE_CNT` mediumint(9) NOT NULL
					) ENGINE=MYISAM DEFAULT CHARSET=latin1;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		
		SET STEP_START_TIME := SYSDATE();
		SET @SqlCmd=CONCAT('SELECT spider_bg_direct_sql(CONCAT(''CALL gt_global_statistic.',SP_Process,'(''''',DATA_DATE,''''',',DATA_HOUR,',',PU_ID,',',TECH_MASK,',''''',GT_DB,''''');'') 
			, ''gt_global_statistic.tmp_failcause_umts_',PU_ID,'_',SUB_WORKER_ID,'''
			, CONCAT(''HOST '''''',REPLACE(gt_strtok(DS_AP_URI,3,'':''),''/'',''''),'''''', PORT '''''',REPLACE(gt_strtok(DS_AP_URI,4,'':''),''/'',''''),'''''',USER '''''',`DS_AP_USER`,'''''', PASSWORD '''''',`DS_AP_PASSWORD`,'''''''')
			) INTO @bb FROM `gt_covmo`.`rnc_information` WHERE `RNC`=',PU_ID,' AND `TECHNOLOGY`=''UMTS''
			;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
	 	INSERT INTO gt_gw_main.SP_LOG VALUES(GT_DB,'SP_Generate_Global_Statistic_Sub_TmpTbl_failcause',CONCAT('SELECT spider_bg_direct_sql,umts:',GT_DB,'_',WORKER_ID), NOW());
		SET STEP_START_TIME := SYSDATE();
		
		IF @bb=1 THEN 			
			SET @SqlCmd=CONCAT('INSERT INTO gt_global_statistic.tmp_failcause_umts_',EX_DATE,'
						SELECT * FROM gt_global_statistic.tmp_failcause_umts_',PU_ID,'_',SUB_WORKER_ID,';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
			INSERT INTO gt_gw_main.SP_LOG VALUES(GT_DB,'SP_Generate_Global_Statistic_Sub_TmpTbl_failcause',CONCAT('INSERT tmp_failcause_umts_',EX_DATE,' cost:',TIMESTAMPDIFF(SECOND,STEP_START_TIME,SYSDATE()),' sec.'), NOW());
			SET STEP_START_TIME := SYSDATE();
	
			SET @SqlCmd=CONCAT('UPDATE gt_global_statistic.tmp_table_call_cnt_',WORKER_ID,' 
						SET `IsSuccess`=1
						WHERE `DATA_DATE`=''',DATA_DATE,''' AND `DATA_HOUR`=',DATA_HOUR,'
							AND `PU_ID`=',PU_ID,' AND `TECH_MASK`=',TECH_MASK,'
						;');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
			INSERT INTO gt_gw_main.SP_LOG VALUES(GT_DB,'SP_Generate_Global_Statistic_Sub_TmpTbl_failcause',CONCAT('UPDATE tmp_table_call_cnt_',PU_ID,'_',SUB_WORKER_ID,' ,umts cost:',TIMESTAMPDIFF(SECOND,STEP_START_TIME,SYSDATE()),' sec.'), NOW());		
		ELSE
			INSERT INTO gt_gw_main.SP_LOG VALUES(GT_DB,'SP_Generate_Global_Statistic_Sub_TmpTbl_failcause',CONCAT('INSERT ERROR,',WORKER_ID), NOW());
			INSERT INTO `gt_gw_main`.`tbl_rpt_error` (`PID`,`CreateTime`,`error_str`) VALUES (WORKER_ID,NOW(),'Spider SP EXECUTE Failed - SP_Generate_Global_Statistic_Sub_TmpTbl_failcause, UMTS');
			SIGNAL KPI_ERROR
				SET MESSAGE_TEXT = 'Spider SP EXECUTE Failed - SP_Generate_Global_Statistic_Sub_TmpTbl_failcause, UMTS';
		END IF;	
			
		SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS gt_global_statistic.tmp_failcause_umts_',PU_ID,'_',SUB_WORKER_ID,';');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
	END IF;	
  	
	IF TECH_MASK=4 THEN 	
		SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS gt_global_statistic.tmp_failcause_lte_',PU_ID,'_',SUB_WORKER_ID,';');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		
		SET SP_Process = 'SP_Generate_Global_Statistic_failcause_lte';
		SET @SqlCmd=CONCAT('CREATE TEMPORARY TABLE gt_global_statistic.tmp_failcause_lte_',PU_ID,'_',SUB_WORKER_ID,' (
					  `DATA_DATE` date NOT NULL,
					  `TILE_ID` bigint(20) NOT NULL,
					  `PU_ID` mediumint(9) NOT NULL,
					  `ENODEB_ID` mediumint(9) NOT NULL,
					  `CELL_ID` mediumint(9) NOT NULL,
					  `IMSI` varchar(20) NOT NULL,
					  `IMEI` varchar(20) NOT NULL,
					  `EVENT_ID` smallint(6) NOT NULL,
					  `FAILURE_EVENT_ID` mediumint(9) NOT NULL,
					  `FAILURE_EVENT_CAUSE` mediumint(9) NOT NULL,
					  `FAILURE_CNT` mediumint(9) NOT NULL
					) ENGINE=MYISAM DEFAULT CHARSET=latin1;');
    		
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		
		SET STEP_START_TIME := SYSDATE();
		SET @SqlCmd=CONCAT('SELECT spider_bg_direct_sql(CONCAT(''CALL gt_global_statistic.',SP_Process,'(''''',DATA_DATE,''''',',DATA_HOUR,',',PU_ID,',',TECH_MASK,',''''',GT_DB,''''');'') 
			, ''gt_global_statistic.tmp_failcause_lte_',PU_ID,'_',SUB_WORKER_ID,'''
			, CONCAT(''HOST '''''',REPLACE(gt_strtok(DS_AP_URI,3,'':''),''/'',''''),'''''', PORT '''''',REPLACE(gt_strtok(DS_AP_URI,4,'':''),''/'',''''),'''''',USER '''''',`DS_AP_USER`,'''''', PASSWORD '''''',`DS_AP_PASSWORD`,'''''''')
			) INTO @bb FROM `gt_covmo`.`rnc_information` WHERE `RNC`=',PU_ID,' AND `TECHNOLOGY`=''LTE''
			;');
    		
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
	 	INSERT INTO gt_gw_main.SP_LOG VALUES(GT_DB,'SP_Generate_Global_Statistic_Sub_TmpTbl_failcause',CONCAT('SELECT spider_bg_direct_sql,umts:',GT_DB,'_',WORKER_ID), NOW());
		SET STEP_START_TIME := SYSDATE();
		
		IF @bb=1 THEN 			
			SET @SqlCmd=CONCAT('INSERT INTO gt_global_statistic.tmp_failcause_lte_',EX_DATE,'
						SELECT * FROM gt_global_statistic.tmp_failcause_lte_',PU_ID,'_',SUB_WORKER_ID,';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
			INSERT INTO gt_gw_main.SP_LOG VALUES(GT_DB,'SP_Generate_Global_Statistic_Sub_TmpTbl_failcause',CONCAT('INSERT tmp_failcause_lte_',EX_DATE,' cost:',TIMESTAMPDIFF(SECOND,STEP_START_TIME,SYSDATE()),' sec.'), NOW());
			SET STEP_START_TIME := SYSDATE();
	
			SET @SqlCmd=CONCAT('UPDATE gt_global_statistic.tmp_table_call_cnt_',WORKER_ID,' 
						SET `IsSuccess`=1
						WHERE `DATA_DATE`=''',DATA_DATE,''' AND `DATA_HOUR`=',DATA_HOUR,'
							AND `PU_ID`=',PU_ID,' AND `TECH_MASK`=',TECH_MASK,'
						;');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
			INSERT INTO gt_gw_main.SP_LOG VALUES(GT_DB,'SP_Generate_Global_Statistic_Sub_TmpTbl_failcause',CONCAT('UPDATE tmp_table_call_cnt_',PU_ID,'_',SUB_WORKER_ID,' ,lte cost:',TIMESTAMPDIFF(SECOND,STEP_START_TIME,SYSDATE()),' sec.'), NOW());		
		ELSE
			INSERT INTO gt_gw_main.SP_LOG VALUES(GT_DB,'SP_Generate_Global_Statistic_Sub_TmpTbl_failcause',CONCAT('INSERT ERROR,',WORKER_ID), NOW());
			INSERT INTO `gt_gw_main`.`tbl_rpt_error` (`PID`,`CreateTime`,`error_str`) VALUES (WORKER_ID,NOW(),'Spider SP EXECUTE Failed - SP_Generate_Global_Statistic_Sub_TmpTbl_failcause, LTE');
			SIGNAL KPI_ERROR
				SET MESSAGE_TEXT = 'Spider SP EXECUTE Failed - SP_Generate_Global_Statistic_Sub_TmpTbl_failcause, LTE';
		END IF;	
			
		SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS gt_global_statistic.tmp_failcause_lte_',PU_ID,'_',SUB_WORKER_ID,';');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
	END IF;	
	INSERT INTO gt_gw_main.SP_LOG VALUES(GT_DB,'SP_Generate_Global_Statistic_Sub_TmpTbl_failcause',CONCAT(DATA_DATE,',',DATA_HOUR,',',PU_ID,',',TECH_MASK,',',PU_ID,'_',SUB_WORKER_ID,' Done: ',TIMESTAMPDIFF(SECOND,START_TIME,SYSDATE()),' seconds.'), NOW());
END$$
DELIMITER ;
