CREATE DEFINER=`covmo`@`%` PROCEDURE `SP_Generate_Global_Statistic_ONE_TILE`(IN TECH_MASK TINYINT(2),IN exDate VARCHAR(10),IN exHour TINYINT(2),IN FLAG TINYINT(2),IN DS_FLAG TINYINT(2),IN RPT_TYPE TINYINT(2),PENDING_FLAG TINYINT(2))
BEGIN	
	DECLARE WORKER_ID VARCHAR(10) DEFAULT CONNECTION_ID();
	DECLARE START_TIME DATETIME DEFAULT SYSDATE();
	INSERT INTO gt_gw_main.SP_LOG VALUES('gt_global_statistic','SP_Generate_Global_Statistic','START', START_TIME);
	SET @SqlCmd=CONCAT('CREATE TABLE IF NOT EXISTS `gt_global_statistic`.table_running_task
				(
				  `DATA_DATE` DATE NOT NULL,
				  `DATA_HOUR` TINYINT(4) NOT NULL,
				  `PU_ID` MEDIUMINT(9) NOT NULL,
				  `TECH_MASK` TINYINT(4) NOT NULL DEFAULT ''2'',
				  `WORKER_ID` INT(11) DEFAULT NULL
				) ENGINE=MYISAM;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	 
	SET @SqlCmd=CONCAT('DROP TABLE IF EXISTS gt_global_statistic.tmp_table_call_cnt_',WORKER_ID,';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('CREATE TABLE IF NOT EXISTS `gt_global_statistic`.tmp_table_call_cnt_',WORKER_ID,' 
				(
				  `DATA_DATE` DATE NOT NULL,
				  `DATA_HOUR` TINYINT(4) NOT NULL,
				  `PU_ID` MEDIUMINT(9) NOT NULL,
				  `TECH_MASK` TINYINT(4) NOT NULL DEFAULT ''2'',
				  `IsSuccess` TINYINT(4) NULL DEFAULT ''0'',
				  `CREATE_TIME` datetime DEFAULT NULL,
				  `FLAG_IMSI_HR` tinyint(2) DEFAULT NULL,
				  `FLAG_IMSI_DY` tinyint(2) DEFAULT NULL,
				  `FLAG_MAKE_HR` tinyint(2) DEFAULT NULL,
				  `FLAG_MAKE_DY` tinyint(2) DEFAULT NULL,
				  `FLAG_IMSI_WK` tinyint(2) DEFAULT NULL,
				  `FLAG_IMSI_MN` tinyint(2) DEFAULT NULL,
				  `FLAG_MAKE_WK` tinyint(2) DEFAULT NULL,
				  `FLAG_MAKE_MN` tinyint(2) DEFAULT NULL,
				  `FLAG_TILE_19_HR` tinyint(2) DEFAULT NULL,
				  `FLAG_TILE_19_DY` tinyint(2) DEFAULT NULL,
				  `FLAG_TILE_16_HR` tinyint(2) DEFAULT NULL,
				  `FLAG_TILE_16_DY` tinyint(2) DEFAULT NULL,
				  `FLAG_TILE_13_HR` tinyint(2) DEFAULT NULL,
				  `FLAG_TILE_13_DY` tinyint(2) DEFAULT NULL,
				  `FLAG_TILE_19_WK` tinyint(2) DEFAULT NULL,
				  `FLAG_TILE_19_MN` tinyint(2) DEFAULT NULL,
				  `FLAG_TILE_16_WK` tinyint(2) DEFAULT NULL,
				  `FLAG_TILE_16_MN` tinyint(2) DEFAULT NULL,
				  `FLAG_TILE_13_WK` tinyint(2) DEFAULT NULL,
				  `FLAG_TILE_13_MN` tinyint(2) DEFAULT NULL,
				  `FLAG_REG_3_HR` tinyint(2) DEFAULT NULL,
				  `FLAG_REG_3_DY` tinyint(2) DEFAULT NULL,
				  `FLAG_REG_2_HR` tinyint(2) DEFAULT NULL,
				  `FLAG_REG_2_DY` tinyint(2) DEFAULT NULL,
				  `FLAG_REG_1_HR` tinyint(2) DEFAULT NULL,
				  `FLAG_REG_1_DY` tinyint(2) DEFAULT NULL,
				  `FLAG_REG_3_WK` tinyint(2) DEFAULT NULL,
				  `FLAG_REG_3_MN` tinyint(2) DEFAULT NULL,
				  `FLAG_REG_2_WK` tinyint(2) DEFAULT NULL,
				  `FLAG_REG_2_MN` tinyint(2) DEFAULT NULL,
				  `FLAG_REG_1_WK` tinyint(2) DEFAULT NULL,
				  `FLAG_REG_1_MN` tinyint(2) DEFAULT NULL,
				  `FLAG_CELL_PM_HR` tinyint(2) DEFAULT NULL,
				  `FLAG_CELL_PM_DY` tinyint(2) DEFAULT NULL,
				  `FLAG_CELL_PM_WK` tinyint(2) DEFAULT NULL,
				  `FLAG_CELL_PM_MN` tinyint(2) DEFAULT NULL,
				  `FLAG_CELL_AGG_HR` tinyint(2) DEFAULT NULL,
				  `FLAG_CELL_AGG_DY` tinyint(2) DEFAULT NULL,
				  `FLAG_CELL_AGG_WK` tinyint(2) DEFAULT NULL,
				  `FLAG_CELL_AGG_MN` tinyint(2) DEFAULT NULL,
				  PRIMARY KEY (`DATA_DATE`,`DATA_HOUR`,`PU_ID`,`TECH_MASK`)
				) ENGINE=MyISAM DEFAULT CHARSET=latin1;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('INSERT INTO `gt_global_statistic`.tmp_table_call_cnt_',WORKER_ID,' 
				(`DATA_DATE`,`DATA_HOUR`,`PU_ID`,`TECH_MASK`)
				SELECT `DATA_DATE`,
					`DATA_HOUR`,
					`PU_ID`,
					`TECH_MASK`
 				FROM `gt_covmo`.`table_call_cnt_ap` a
				WHERE `DATA_DATE`=''',exDate,''' AND `DATA_HOUR`=',exHour,'
				GROUP BY a.`DATA_DATE`,a.`DATA_HOUR`,a.`PU_ID`,a.`TECH_MASK`;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	if PENDING_FLAG=1 then
		SET @SqlCmd=CONCAT('DELETE a.*
					FROM `gt_global_statistic`.tmp_table_call_cnt_',WORKER_ID,' a
					WHERE EXISTS
					(
					SELECT NULL 
					FROM `gt_global_statistic`.table_running_task b
					WHERE a.`DATA_DATE`=b.`DATA_DATE` AND a.`DATA_HOUR`=b.`DATA_HOUR` AND a.`PU_ID`=b.`PU_ID` AND a.`TECH_MASK`=b.`TECH_MASK`
					);');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;		
	end if;
	
	SET @SqlCmd=CONCAT('SELECT COUNT(*) INTO @CNT FROM `gt_global_statistic`.tmp_table_call_cnt_',WORKER_ID,';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;	
	IF @CNT>0 or FLAG=2 then 	 
		SET @SqlCmd=CONCAT('INSERT IGNORE INTO `gt_global_statistic`.table_running_task
					(`DATA_DATE`,`DATA_HOUR`,`PU_ID`,`TECH_MASK`,`WORKER_ID`)
					SELECT `DATA_DATE`,
						`DATA_HOUR`,
						`PU_ID`,
						`TECH_MASK`,
						',WORKER_ID,' AS `WORKER_ID`
					FROM `gt_global_statistic`.tmp_table_call_cnt_',WORKER_ID,';');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		
		CALL gt_gw_main.`SP_Generate_Global_Statistic_dCheck_MMR`(TECH_MASK,WORKER_ID,DS_FLAG,RPT_TYPE); 
	
		
		IF FLAG IN (0,1) THEN 
	
			CALL gt_gw_main.`SP_Generate_Global_Statistic_Sub_TmpTbl_Crt`(WORKER_ID,RPT_TYPE);
		END IF;
		
		IF (RPT_TYPE & 1)>0 THEN 
			CALL gt_gw_main.`SP_Generate_Global_Statistic_dsp_MMR`(WORKER_ID,FLAG,exDate,'TILE',DS_FLAG,PENDING_FLAG);
		END IF;
		
		IF (RPT_TYPE & 2)>0 THEN 
			CALL gt_gw_main.`SP_Generate_Global_Statistic_dsp_MMR`(WORKER_ID,FLAG,exDate,'IMSI',DS_FLAG,PENDING_FLAG);
		END IF;
		
		IF (RPT_TYPE & 4)>0 THEN 
			CALL gt_gw_main.`SP_Generate_Global_Statistic_dsp_MMR`(WORKER_ID,FLAG,exDate,'CELL',DS_FLAG,PENDING_FLAG);
		END IF;
		
		IF FLAG IN (0,1) AND DS_FLAG=1 THEN 
			CALL gt_gw_main.`SP_Generate_Global_Statistic_DS_Aggr`(TECH_MASK,WORKER_ID,exDate);
		END IF;
		
		CALL gt_gw_main.`SP_Generate_Global_Statistic_log_MMR`(TECH_MASK,WORKER_ID);
	else 
		SELECT 'No New Data.' AS IsSuccess;
	end if;
			 
	SET @SqlCmd=CONCAT('DELETE FROM gt_global_statistic.table_running_task 
				WHERE `WORKER_ID`=',WORKER_ID,';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('DROP TABLE IF EXISTS gt_global_statistic.tmp_table_call_cnt_',WORKER_ID,';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	INSERT INTO gt_gw_main.SP_LOG VALUES('gt_global_statistic','SP_Generate_Global_Statistic',CONCAT(WORKER_ID,' Done cost:',TIMESTAMPDIFF(SECOND,START_TIME,SYSDATE()),' sec.'), NOW());
