DELIMITER $$
USE `gt_global_statistic`$$
DROP PROCEDURE IF EXISTS `SP_CreateDB_LTE`$$
CREATE DEFINER=`covmo`@`%` PROCEDURE `SP_Generate_Global_Statistic_ONE_IMSI`(IN TECH_MASK TINYINT(2),IN exDate VARCHAR(10),IN exHour TINYINT(2),IN FLAG TINYINT(2))
BEGIN	
	DECLARE WORKER_ID VARCHAR(10) DEFAULT CONNECTION_ID();
	DECLARE START_TIME DATETIME DEFAULT SYSDATE();
	INSERT INTO gt_gw_main.SP_LOG VALUES('gt_global_statistic','SP_Generate_Global_Statistic_ONE_IMSI','START', START_TIME);
		
	SET @SqlCmd=CONCAT('DROP TABLE IF EXISTS gt_global_statistic.tmp_table_call_cnt_',WORKER_ID,';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET SESSION max_heap_table_size=4*1024*1024*1024;
	SET @SqlCmd=CONCAT('CREATE TABLE IF NOT EXISTS `gt_global_statistic`.tmp_running_task 
				(
				  `DATA_DATE` date NOT NULL,
				  `DATA_HOUR` tinyint(4) NOT NULL,
				  `PU_ID` mediumint(9) NOT NULL,
				  `TECH_MASK` tinyint(4) NOT NULL DEFAULT ''2'',
				  `WORKER_ID` int(11) DEFAULT NULL
				) ENGINE=MyISAM;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SELECT COUNT(*) INTO @V_RUNNING_HR FROM `gt_global_statistic`.tmp_running_task;
	
	IF @V_RUNNING_HR=0 THEN 
		SET @SqlCmd=CONCAT('CREATE TABLE IF NOT EXISTS `gt_global_statistic`.tmp_table_call_cnt_',WORKER_ID,' 
					(
					  `DATA_DATE` date NOT NULL,
					  `DATA_HOUR` tinyint(4) NOT NULL,
					  `PU_ID` mediumint(9) NOT NULL,
					  `TECH_MASK` tinyint(4) NOT NULL DEFAULT ''2'',
					  `IsSuccess` tinyint(4) NULL DEFAULT ''0''
					) ENGINE=MyISAM;');
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
					WHERE  `PU_ID` IN (100,200)
   					AND `DATA_DATE`=''',exDate,''' AND `DATA_HOUR`=',exHour,'
					GROUP BY a.`DATA_DATE`,a.`DATA_HOUR`,a.`PU_ID`;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		
	ELSE
		SET @SqlCmd=CONCAT('DROP TABLE IF EXISTS gt_global_statistic.tmp_table_call_cnt_sub_',WORKER_ID,';');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		
		SET @SqlCmd=CONCAT('CREATE TEMPORARY TABLE IF NOT EXISTS `gt_global_statistic`.tmp_table_call_cnt_sub_',WORKER_ID,' ENGINE=MYISAM
					SELECT `DATA_DATE`,`DATA_HOUR`,`PU_ID`,`TECH_MASK`,0 AS IsSuccess
					FROM `gt_global_statistic`.table_call_cnt_history WHERE 1<>1;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		
		SET @SqlCmd=CONCAT('INSERT INTO `gt_global_statistic`.tmp_table_call_cnt_sub_',WORKER_ID,' 
					(`DATA_DATE`,`DATA_HOUR`,`PU_ID`,`TECH_MASK`,IsSuccess)
					SELECT `DATA_DATE`,
						`DATA_HOUR`,
						`PU_ID`,
						`TECH_MASK`,
						0 AS IsSuccess
 					FROM `gt_covmo`.`table_call_cnt_ap` a
					WHERE  `PU_ID` IN (100,200)
   					AND `DATA_DATE`=''',exDate,''' AND `DATA_HOUR`=',exHour,'
					GROUP BY a.`DATA_DATE`,a.`DATA_HOUR`,a.`PU_ID`;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		
		SET @SqlCmd=CONCAT('CREATE TABLE `gt_global_statistic`.tmp_table_call_cnt_',WORKER_ID,' ENGINE=MYISAM
					SELECT `DATA_DATE`,
						`DATA_HOUR`,
						`PU_ID`,
						`TECH_MASK`,
						0 AS IsSuccess
					FROM `gt_global_statistic`.tmp_table_call_cnt_sub_',WORKER_ID,'  a
					WHERE NOT EXISTS
					(
						SELECT NULL 
						FROM `gt_global_statistic`.tmp_running_task b
						WHERE a.`DATA_DATE`=b.`DATA_DATE` AND a.`DATA_HOUR`=b.`DATA_HOUR` AND a.`PU_ID`=b.`PU_ID`
					)
					GROUP BY a.`DATA_DATE`,a.`DATA_HOUR`,a.`PU_ID`;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;		
		DEALLOCATE PREPARE Stmt;
	END IF;
	
	SELECT GET_LOCK('GT_UPDATE_LOCK', 60) INTO @aa; 
	SET @SqlCmd=CONCAT('INSERT IGNORE INTO `gt_global_statistic`.tmp_running_task
				(`DATA_DATE`,`DATA_HOUR`,`PU_ID`,`TECH_MASK`)
				SELECT `DATA_DATE`,
					`DATA_HOUR`,
					`PU_ID`,
					`TECH_MASK`
				FROM `gt_global_statistic`.tmp_table_call_cnt_',WORKER_ID,';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	SELECT RELEASE_LOCK('GT_UPDATE_LOCK') INTO @aa;	
	
	CALL gt_gw_main.`SP_Generate_Global_Statistic_dCheck_MMR`(TECH_MASK,WORKER_ID); 
 	CALL gt_gw_main.`SP_Generate_Global_Statistic_dsp_MMR_IMSI`(WORKER_ID,FLAG,exDate);
 	CALL gt_gw_main.`SP_Generate_Global_Statistic_log_MMR`(TECH_MASK,WORKER_ID);
	
	SET @SqlCmd=CONCAT('DROP TABLE IF EXISTS gt_global_statistic.tmp_table_call_cnt_sub_',WORKER_ID,';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	SET @SqlCmd=CONCAT('DROP TABLE IF EXISTS gt_global_statistic.tmp_table_call_cnt_',WORKER_ID,';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	INSERT INTO gt_gw_main.SP_LOG VALUES('gt_global_statistic','SP_Generate_Global_Statistic_ONE_IMSI',CONCAT(WORKER_ID,' Done cost:',TIMESTAMPDIFF(SECOND,START_TIME,SYSDATE()),' sec.'), NOW());
END$$
DELIMITER ;
