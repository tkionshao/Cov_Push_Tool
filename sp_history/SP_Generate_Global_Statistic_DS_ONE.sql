DELIMITER $$
USE `gt_global_statistic`$$
DROP PROCEDURE IF EXISTS `SP_CreateDB_LTE`$$
CREATE DEFINER=`covmo`@`%` PROCEDURE `SP_Generate_Global_Statistic_DS_ONE`(IN TECH_MASK TINYINT(2),IN exDate VARCHAR(10),IN exHour TINYINT(2),IN FLAG TINYINT(2),IN DS_FLAG TINYINT(2),IN RPT_TYPE TINYINT(2),PENDING_FLAG TINYINT(2))
BEGIN	
	DECLARE WORKER_ID VARCHAR(10) DEFAULT CONNECTION_ID();
	DECLARE START_TIME DATETIME DEFAULT SYSDATE();
	DECLARE STEP_START_TIME DATETIME DEFAULT SYSDATE();
	DECLARE TileResolution VARCHAR(10);
	
		
	DECLARE EXIT HANDLER FOR SQLEXCEPTION
	BEGIN
		INSERT INTO gt_gw_main.SP_LOG VALUES('gt_global_statistic','SP_Generate_Global_Statistic_DS_ONE',CONCAT(WORKER_ID,' SQLEXCEPTION cost:',TIMESTAMPDIFF(SECOND,START_TIME,SYSDATE()),' sec.'), NOW());
		TRUNCATE TABLE `gt_global_statistic`.`table_running_task_ds` ;
	END;
		
	INSERT INTO gt_gw_main.SP_LOG VALUES('gt_global_statistic','SP_Generate_Global_Statistic_DS_ONE','START', START_TIME);
	SET @SqlCmd=CONCAT('SELECT COUNT(*) INTO @CNT FROM `gt_global_statistic`.table_running_task_ds;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	select @CNT;
	IF @CNT>0 THEN 	
	
		SELECT att_value INTO @ZOOM_LEVEL FROM `gt_global_statistic`.`nw_config` WHERE `group_name`='system' AND att_name = 'MapResolution';
		SELECT att_value INTO @TileResolution FROM `gt_global_statistic`.`nw_config` WHERE group_name = 'tile' AND att_name = 'TileResolution';
		SET TileResolution=CONCAT(@TileResolution,',',@ZOOM_LEVEL);
		
		CALL gt_global_statistic.`SP_Generate_Global_Statistic_DS_dCheck_MMR`(TECH_MASK,WORKER_ID,DS_FLAG,RPT_TYPE,TileResolution); 
		
		CALL gt_global_statistic.`SP_Generate_Global_Statistic_DS_Sub_TmpTbl_Crt`(WORKER_ID,RPT_TYPE,TileResolution);
		
		CALL gt_global_statistic.`SP_Generate_Global_Statistic_DS_dsp_MMR`(WORKER_ID,FLAG,exDate,'TILE',DS_FLAG,PENDING_FLAG,TileResolution);
		
		CALL gt_global_statistic.`SP_Generate_Global_Statistic_DS_Aggr`(TECH_MASK,WORKER_ID,exDate,TileResolution);
		
		SET @SqlCmd=CONCAT('INSERT INTO `gt_global_statistic`.`table_call_cnt_history_ds`
				    (`DATA_DATE`,
				     `DATA_HOUR`,
				     `PU_ID`,
				     `TECH_MASK`,
				     `CREATE_TIME`,
				     `FLAG_DS`) 
				     SELECT `DATA_DATE`,
					     `DATA_HOUR`,
					     `PU_ID`,
					     `TECH_MASK`,
					     ''',NOW(),''' AS `CREATE_TIME`,
					     `FLAG_DS`
				     FROM gt_global_statistic.table_running_task_ds;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		INSERT INTO gt_gw_main.SP_LOG VALUES('gt_global_statistic','SP_Generate_Global_Statistic_DS_ONE',CONCAT('INSERT INTO table_call_cnt_history_ds cost:',TIMESTAMPDIFF(SECOND,STEP_START_TIME,SYSDATE()),' sec.'), NOW());
		SET STEP_START_TIME := SYSDATE();
	
		SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS `gt_global_statistic`.tmp_ds_information_',WORKER_ID,';');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
				
		SET @SqlCmd=CONCAT('CREATE TEMPORARY TABLE `gt_global_statistic`.tmp_ds_information_',WORKER_ID,' LIKE `gt_global_statistic`.ds_information;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		
		SET @SqlCmd=CONCAT('INSERT INTO `gt_global_statistic`.tmp_ds_information_',WORKER_ID,' 
					(
					`DATA_DATE`,
					`DATA_HOUR`,
					`GSM_COMPLETED_CNT`,
					`UMTS_COMPLETED_CNT`,
					`LTE_COMPLETED_CNT`
					)
					SELECT	
						`DATA_DATE`,
						`DATA_HOUR`,
						SUM(IF(TECH_MASK=1,1,0)) AS `GSM_COMPLETED_CNT`,
						SUM(IF(TECH_MASK=2,1,0)) AS `UMTS_COMPLETED_CNT`,
						SUM(IF(TECH_MASK=4,1,0)) AS `LTE_COMPLETED_CNT`
					FROM `gt_global_statistic`.`table_running_task_ds`
					;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		INSERT INTO gt_gw_main.SP_LOG VALUES('gt_global_statistic','SP_Generate_Global_Statistic_DS_ONE',CONCAT('INSERT INTO tmp_ds_information cost:',TIMESTAMPDIFF(SECOND,STEP_START_TIME,SYSDATE()),' sec.'), NOW());
		SET STEP_START_TIME := SYSDATE();
		
		SET @SqlCmd=CONCAT('SELECT COUNT(*) INTO @DS_CNT FROM `gt_global_statistic`.tmp_ds_information_',WORKER_ID,' A
					JOIN `gt_global_statistic`.ds_information B
					ON A.`DATA_DATE`=B.`DATA_DATE` AND A.`DATA_HOUR`=B.`DATA_HOUR`;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
			
		IF @DS_CNT>0 THEN 	 
			SET @SqlCmd=CONCAT('INSERT INTO `gt_global_statistic`.ds_information
						(
						`DATA_DATE`,
						`DATA_HOUR`,
						`GSM_COMPLETED_CNT`,
						`UMTS_COMPLETED_CNT`,
						`LTE_COMPLETED_CNT`
						)
						SELECT	
							A.`DATA_DATE`,
							A.`DATA_HOUR`,
							A.`GSM_COMPLETED_CNT`,
							A.`UMTS_COMPLETED_CNT`,
							A.`LTE_COMPLETED_CNT`
						FROM `gt_global_statistic`.tmp_ds_information_',WORKER_ID,' A
						ON DUPLICATE KEY UPDATE 
						`gt_global_statistic`.ds_information.GSM_COMPLETED_CNT=`gt_global_statistic`.ds_information.GSM_COMPLETED_CNT+VALUES(GSM_COMPLETED_CNT),
						`gt_global_statistic`.ds_information.UMTS_COMPLETED_CNT=`gt_global_statistic`.ds_information.UMTS_COMPLETED_CNT+VALUES(UMTS_COMPLETED_CNT),
						`gt_global_statistic`.ds_information.LTE_COMPLETED_CNT=`gt_global_statistic`.ds_information.LTE_COMPLETED_CNT+VALUES(LTE_COMPLETED_CNT)
						;');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
			INSERT INTO gt_gw_main.SP_LOG VALUES('gt_global_statistic','SP_Generate_Global_Statistic_DS_ONE',CONCAT('INSERT UPDATE ds_information cost:',TIMESTAMPDIFF(SECOND,STEP_START_TIME,SYSDATE()),' sec.'), NOW());
			SET STEP_START_TIME := SYSDATE();
		else
			SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS `gt_global_statistic`.tmp_task_plan_',WORKER_ID,';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
			
			SET @SqlCmd=CONCAT('CREATE TEMPORARY TABLE gt_global_statistic.tmp_task_plan_',WORKER_ID,' 
					(				  
						`rncid` mediumint(9) NOT NULL,
						`TECHNOLOGY` varchar(10) NOT NULL,
						PRIMARY KEY (`rncid`,`TECHNOLOGY`)
					) ENGINE=MYISAM DEFAULT CHARSET=latin1;');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
			
			SET @SqlCmd=CONCAT('insert into gt_global_statistic.tmp_task_plan_',WORKER_ID,' 
						(`rncid`,`TECHNOLOGY` )
					SELECT `rncid`,`TECHNOLOGY` FROM `gt_covmo`.`task_plan` WHERE `enabled`=1;');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
			INSERT INTO gt_gw_main.SP_LOG VALUES('gt_global_statistic','SP_Generate_Global_Statistic_DS_ONE',CONCAT('INSERT INTO tmp_task_plan_',WORKER_ID,' cost:',TIMESTAMPDIFF(SECOND,STEP_START_TIME,SYSDATE()),' sec.'), NOW());
			SET STEP_START_TIME := SYSDATE();
		
			SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS `gt_global_statistic`.tmp_rnc_information_',WORKER_ID,';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
					
			SET @SqlCmd=CONCAT('CREATE TEMPORARY TABLE `gt_global_statistic`.tmp_rnc_information_',WORKER_ID,'
						(
						  `GSM_TOTAL_CNT` smallint(6) NOT NULL,
						  `UMTS_TOTAL_CNT` smallint(6) NOT NULL,
						  `LTE_TOTAL_CNT` smallint(6) NOT NULL
						) ENGINE=MyISAM DEFAULT CHARSET=latin1
					;');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
			
			SET @SqlCmd=CONCAT('INSERT INTO `gt_global_statistic`.tmp_rnc_information_',WORKER_ID,'
						(
						`GSM_TOTAL_CNT`,
						`UMTS_TOTAL_CNT`,
						`LTE_TOTAL_CNT`
						)
						SELECT SUM(IF(A.TECHNOLOGY=''GSM'',1,0)) AS `GSM_TOTAL_CNT`,
							SUM(IF(A.TECHNOLOGY=''UMTS'',1,0)) AS `UMTS_TOTAL_CNT`,
							SUM(IF(A.TECHNOLOGY=''LTE'',1,0)) AS `LTE_TOTAL_CNT`
						FROM `gt_covmo`.`rnc_information` A, gt_global_statistic.tmp_task_plan_',WORKER_ID,' B
						WHERE A.`RNC`=B.`rncid` AND A.`TECHNOLOGY`=B.`TECHNOLOGY`
						;');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
			INSERT INTO gt_gw_main.SP_LOG VALUES('gt_global_statistic','SP_Generate_Global_Statistic_DS_ONE',CONCAT('INSERT INTO tmp_rnc_information cost:',TIMESTAMPDIFF(SECOND,STEP_START_TIME,SYSDATE()),' sec.'), NOW());
			SET STEP_START_TIME := SYSDATE();
			SET @SqlCmd=CONCAT('INSERT INTO `gt_global_statistic`.ds_information
						(
						`DATA_DATE`,
						`DATA_HOUR`,
						`GSM_COMPLETED_CNT`,
						`GSM_TOTAL_CNT`,
						`UMTS_COMPLETED_CNT`,
						`UMTS_TOTAL_CNT`,
						`LTE_COMPLETED_CNT`,
						`LTE_TOTAL_CNT`
						)
						SELECT	
							A.`DATA_DATE`,
							A.`DATA_HOUR`,
							A.`GSM_COMPLETED_CNT`,
							B.`GSM_TOTAL_CNT`,
							A.`UMTS_COMPLETED_CNT`,
							B.`UMTS_TOTAL_CNT`,
							A.`LTE_COMPLETED_CNT`,
							B.`LTE_TOTAL_CNT`
						FROM `gt_global_statistic`.tmp_ds_information_',WORKER_ID,' A
						,`gt_global_statistic`.tmp_rnc_information_',WORKER_ID,' B
						;');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
			INSERT INTO gt_gw_main.SP_LOG VALUES('gt_global_statistic','SP_Generate_Global_Statistic_DS_ONE',CONCAT('INSERT INTO ds_information cost:',TIMESTAMPDIFF(SECOND,STEP_START_TIME,SYSDATE()),' sec.'), NOW());
			SET STEP_START_TIME := SYSDATE();
		end if;
	ELSE 
		INSERT INTO gt_gw_main.SP_LOG VALUES('gt_global_statistic','SP_Generate_Global_Statistic_DS_ONE',CONCAT('No New data cost:',TIMESTAMPDIFF(SECOND,STEP_START_TIME,SYSDATE()),' sec.'), NOW());
		SET STEP_START_TIME := SYSDATE();
		SELECT 'No New Data.' AS IsSuccess;
	END IF;	
	
	SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS `gt_global_statistic`.tmp_ds_information_',WORKER_ID,';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
			
	SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS `gt_global_statistic`.tmp_rnc_information_',WORKER_ID,';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
			
	
	SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS `gt_global_statistic`.tmp_task_plan_',WORKER_ID,';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
			 
	SET @SqlCmd=CONCAT('TRUNCATE TABLE gt_global_statistic.table_running_task_ds;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	INSERT INTO gt_gw_main.SP_LOG VALUES('gt_global_statistic','SP_Generate_Global_Statistic_DS_ONE',CONCAT(WORKER_ID,' Done cost:',TIMESTAMPDIFF(SECOND,START_TIME,SYSDATE()),' sec.'), NOW());
END$$
DELIMITER ;
