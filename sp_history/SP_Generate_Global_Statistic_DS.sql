DELIMITER $$
USE `gt_global_statistic`$$
DROP PROCEDURE IF EXISTS `SP_CreateDB_LTE`$$
CREATE DEFINER=`covmo`@`%` PROCEDURE `SP_Generate_Global_Statistic_DS`()
do_nothing:
BEGIN	
	DECLARE WORKER_ID VARCHAR(10) DEFAULT CONNECTION_ID();
	DECLARE START_TIME DATETIME DEFAULT SYSDATE();
	DECLARE sDate DATE;
	DECLARE eDate DATE;
	DECLARE v_FLAG TINYINT(2);
	DECLARE v_DS_FLAG TINYINT(2);
	DECLARE v_RPT_TYPE TINYINT(2);
	DECLARE v_PENDING_FLAG TINYINT(2);
	
	DECLARE EXIT HANDLER FOR SQLEXCEPTION
	BEGIN
		INSERT INTO gt_gw_main.SP_LOG VALUES('gt_global_statistic','SP_Generate_Global_Statistic_DS',CONCAT(WORKER_ID,' SQLEXCEPTION cost:',TIMESTAMPDIFF(SECOND,START_TIME,SYSDATE()),' sec.'), NOW());
		TRUNCATE TABLE `gt_global_statistic`.`table_running_task_ds` ;
	END;
	
	SET v_FLAG=1;
	SET v_DS_FLAG=1;
	SET v_RPT_TYPE=1;
	SET v_PENDING_FLAG=0;
	INSERT INTO gt_gw_main.SP_LOG VALUES('gt_global_statistic','SP_Generate_Global_Statistic_DS',CONCAT(WORKER_ID,' START'), START_TIME);
	
	SET @SqlCmd=CONCAT('CREATE TABLE IF NOT EXISTS `gt_global_statistic`.`table_running_task_ds` 
				(
				  `DATA_DATE` DATE NOT NULL,
				  `DATA_HOUR` TINYINT(4) NOT NULL,
				  `PU_ID` MEDIUMINT(9) NOT NULL,
				  `TECH_MASK` TINYINT(4) NOT NULL DEFAULT ''2'',
				  `WORKER_ID` INT(11) DEFAULT NULL,
				  `FLAG_DS` TINYINT(2) DEFAULT NULL			  
				) ENGINE=MYISAM;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SELECT COUNT(*) INTO @V_RUNNING_HR FROM `gt_global_statistic`.table_running_task_ds;
	IF @V_RUNNING_HR=0 THEN 
		SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS gt_global_statistic.tmp_table_call_cnt_ap_',WORKER_ID,' ;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		
		SET @SqlCmd=CONCAT('CREATE TEMPORARY TABLE gt_global_statistic.tmp_table_call_cnt_ap_',WORKER_ID,' 
				(				  
				  `DATA_DATE` DATE DEFAULT NULL,
				  `DATA_HOUR` TINYINT(4) DEFAULT NULL,
				  `PU_ID` MEDIUMINT(9) DEFAULT NULL,
				  `TECH_MASK` TINYINT(4) NOT NULL DEFAULT ''2'',
				PRIMARY KEY (`DATA_DATE`,`DATA_HOUR`,`PU_ID`,`TECH_MASK`)
				) ENGINE=MYISAM DEFAULT CHARSET=latin1;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;	
	
		SET @SqlCmd=CONCAT('SELECT REPLACE(gt_strtok(`value`,3,'':''),''/'','''') ,REPLACE(gt_strtok(`value`,4,'':''),''/'','''') INTO @AP_IP,@AP_PORT FROM `gt_gw_main`.`integration_param` WHERE gt_group=''system'' AND gt_name=''apDbUri'';');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt; 
		
		SET @SqlCmd=CONCAT('SELECT `value` INTO @AP_USER FROM `gt_gw_main`.`integration_param` WHERE gt_group=''system'' AND gt_name=''apDbUser'';');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt; 
		
		SET @SqlCmd=CONCAT('SELECT `value` INTO @AP_PSWD FROM `gt_gw_main`.`integration_param` WHERE gt_group=''system'' AND gt_name=''apDbPass'';');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
	
		SET @SqlCmd=CONCAT('SELECT spider_bg_direct_sql(CONCAT('' SELECT `DATA_DATE`,`DATA_HOUR`,`PU_ID`,`TECH_MASK` FROM `gt_covmo`.`table_call_cnt`
										WHERE `DATA_DATE` < DATE(DATE_ADD(NOW(), INTERVAL 1 DAY))
										AND `DATA_DATE` >= DATE(DATE_SUB(NOW(), INTERVAL 3 DAY))
										GROUP BY `DATA_DATE`,`DATA_HOUR`,`PU_ID`,`TECH_MASK`;'') 
					, ''gt_global_statistic.tmp_table_call_cnt_ap_',WORKER_ID,'''
					, CONCAT(''HOST ''''',@AP_IP,''''', PORT ''''',@AP_PORT,''''',USER ''''',@AP_USER,''''', PASSWORD ''''',@AP_PSWD,''''''')
					) INTO @bb
					;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
	
		SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS gt_global_statistic.tmp_table_ds_',WORKER_ID,';');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		
		SET @SqlCmd=CONCAT('CREATE TEMPORARY TABLE `gt_global_statistic`.tmp_table_ds_',WORKER_ID,' 
					(
					  `DATA_DATE` DATE NOT NULL,
					  `DATA_HOUR` TINYINT(4) NOT NULL,
					  `PU_ID` MEDIUMINT(9) NOT NULL,
					  `TECH_MASK` TINYINT(4) NOT NULL DEFAULT ''2''
					) ENGINE=MYISAM;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
			
		SET @SqlCmd=CONCAT('INSERT INTO `gt_global_statistic`.tmp_table_ds_',WORKER_ID,' 
					(`DATA_DATE`,`DATA_HOUR`,`PU_ID`,`TECH_MASK`)
					SELECT `DATA_DATE`,
						`DATA_HOUR`,
						`PU_ID`,
						`TECH_MASK`
 					FROM gt_global_statistic.tmp_table_call_cnt_ap_',WORKER_ID,' a
					WHERE NOT EXISTS
					(
						SELECT NULL 
						FROM `gt_global_statistic`.`table_call_cnt_history_ds` b
						WHERE a.`DATA_DATE`=b.`DATA_DATE` AND a.`DATA_HOUR`=b.`DATA_HOUR` AND a.`PU_ID`=b.`PU_ID` AND a.`TECH_MASK`=b.`TECH_MASK`
					)
					;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
	
		SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS gt_global_statistic.tmp_table_call_cnt_ap_',WORKER_ID,' ;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		
		SET @V_PENDING=0;
		SET @SqlCmd=CONCAT('SELECT COUNT(*) INTO @V_PENDING FROM `gt_global_statistic`.tmp_table_ds_',WORKER_ID,';');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		
		IF @V_PENDING>0 THEN 
			SET @v_DATA_DATE=NULL;
			SET @v_DATA_HOUR=NULL;
			
			SET @SqlCmd=CONCAT('SELECT GROUP_CONCAT(DISTINCT CONCAT(`DATA_DATE`,'','',`DATA_HOUR`) ORDER BY DATA_DATE DESC,`DATA_HOUR` DESC SEPARATOR ''|'' ) INTO @DATE_STR
						FROM `gt_global_statistic`.tmp_table_ds_',WORKER_ID,' 
						;');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
			SET @v_m=1;
			WHILE @v_m <= 2 DO
			BEGIN
				SET @v_DATA_DATE:=gt_covmo_csv_get(gt_strtok(@DATE_STR, @v_m, '|'),1);		
				SET @v_DATA_HOUR:=gt_covmo_csv_get(gt_strtok(@DATE_STR, @v_m, '|'),2);
				
				SET @SqlCmd=CONCAT('INSERT INTO `gt_global_statistic`.table_running_task_ds
							(`DATA_DATE`,`DATA_HOUR`,`PU_ID`,`TECH_MASK`,`WORKER_ID`)
							SELECT `DATA_DATE`,
								`DATA_HOUR`,
								`PU_ID`,
								`TECH_MASK`,
								',WORKER_ID,' AS `WORKER_ID`
							FROM `gt_global_statistic`.tmp_table_ds_',WORKER_ID,' 
							WHERE `DATA_DATE`=''',@v_DATA_DATE,''' AND `DATA_HOUR`=',@v_DATA_HOUR,';');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
 				CALL gt_global_statistic.`SP_Generate_Global_Statistic_DS_ONE`(0,@v_DATA_DATE,@v_DATA_HOUR,v_FLAG,v_DS_FLAG,v_RPT_TYPE,v_PENDING_FLAG);
				SET @v_m=@v_m+1;
			END;
			END WHILE;
			
			SET @SqlCmd=CONCAT('TRUNCATE TABLE gt_global_statistic.table_running_task_ds;');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;		
		END IF;
	ELSE
		INSERT INTO gt_gw_main.SP_LOG VALUES('gt_global_statistic','SP_Generate_Global_Statistic_DS',CONCAT(WORKER_ID,' DO nothing cost:',TIMESTAMPDIFF(SECOND,START_TIME,SYSDATE()),' sec.'), NOW());
		LEAVE do_nothing;
	END IF;
	INSERT INTO gt_gw_main.SP_LOG VALUES('gt_global_statistic','SP_Generate_Global_Statistic_DS',CONCAT(WORKER_ID,',',@v_DATA_DATE,',',@v_DATA_HOUR,' Done cost:',TIMESTAMPDIFF(SECOND,START_TIME,SYSDATE()),' sec.'), NOW());
END$$
DELIMITER ;
