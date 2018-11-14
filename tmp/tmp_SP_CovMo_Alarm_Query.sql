CREATE DEFINER=`covmo`@`%` PROCEDURE `SP_CovMo_Alarm_Query`(IN PU_ID MEDIUMINT(9),IN DATA_DATE DATE,IN DATA_HOUR TINYINT(2),IN NT_SOURCE VARCHAR(50),IN IMSI_GID INT(11),IN CELL_GID INT(11),IN sql_fetch_str LONGTEXT,IN sql_check_str LONGTEXT)
a_label:
BEGIN
	DECLARE START_TIME DATETIME DEFAULT SYSDATE();
	DECLARE STEP_START_TIME DATETIME DEFAULT SYSDATE();
	DECLARE WORKER_ID VARCHAR(10) DEFAULT CONNECTION_ID();
	DECLARE GT_DB VARCHAR(50);
	DECLARE CELL_STR VARCHAR(1000);
	DECLARE EXIT HANDLER FOR SQLEXCEPTION
	BEGIN
		SELECT NULL;
	END;
		
	SET GT_DB=CONCAT('gt_',PU_ID,'_',DATE_FORMAT(DATA_DATE,'%Y%m%d'),'_0000_0000');
	
	
	IF PU_ID = 0 THEN
		SET GT_DB='gt_global_statistic_g1';
	END IF;
	
 	INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'SP_CovMo_Alarm_Query','Filter Check Start',START_TIME);
 	
	SET sql_check_str=REPLACE(sql_check_str,':DATA_DATE:',DATA_DATE);
	SET sql_check_str=REPLACE(sql_check_str,':DATA_HOUR:',DATA_HOUR);
	SET sql_check_str=REPLACE(sql_check_str,':NT_SOURCE:',NT_SOURCE);
	SET sql_check_str=REPLACE(sql_check_str,':GT_DB:',GT_DB);
	SET sql_check_str=REPLACE(sql_check_str,':CELL_TABLE_DATE:',DATE_FORMAT(DATA_DATE,'%Y%m%d'));
	
	SET sql_fetch_str=REPLACE(sql_fetch_str,':DATA_DATE:',DATA_DATE);
	SET sql_fetch_str=REPLACE(sql_fetch_str,':DATA_HOUR:',DATA_HOUR);
	SET sql_fetch_str=REPLACE(sql_fetch_str,':NT_SOURCE:',NT_SOURCE);
	SET sql_fetch_str=REPLACE(sql_fetch_str,':GT_DB:',GT_DB);
	SET sql_fetch_str=REPLACE(sql_fetch_str,':CELL_TABLE_DATE:',DATE_FORMAT(DATA_DATE,'%Y%m%d'));
	
	IF IMSI_GID>0 THEN
		SET @SqlCmd=CONCAT('SELECT GROUP_CONCAT(`IMSI`) into @imsi_group_id FROM `gt_covmo`.`dim_imsi` WHERE `GROUP_ID`=',IMSI_GID,';');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		SET sql_check_str=REPLACE(sql_check_str,':IMSI_LIST:',SUBSTRING(IN_QUOTE(@imsi_group_id),2,LENGTH(IN_QUOTE(@imsi_group_id))-2));
		SET sql_fetch_str=REPLACE(sql_fetch_str,':IMSI_LIST:',SUBSTRING(IN_QUOTE(@imsi_group_id),2,LENGTH(IN_QUOTE(@imsi_group_id))-2));
	END IF; 
	
	IF CELL_GID>0 THEN 
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
		
		SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS gt_gw_main.tmp_cell_group_',WORKER_ID,' ;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		
		SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS tmp_usr_cell_upload_',WORKER_ID,' ;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
	
		SET @SqlCmd=CONCAT('CREATE TEMPORARY TABLE tmp_usr_cell_upload_',WORKER_ID,' 
					(
					  `group_id` int(11) NOT NULL,
					  `cell_id` int(11) NOT NULL,
					  `enodeb_id` int(11) NOT NULL,
					  `pu_id` int(11) DEFAULT NULL,
					  `cell_name` varchar(50) CHARACTER SET utf8 DEFAULT NULL,
					  `enodeb_name` varchar(50) CHARACTER SET utf8 DEFAULT NULL,
					  PRIMARY KEY (`group_id`,`cell_id`,`enodeb_id`)					  
					) ENGINE=FEDERATED DEFAULT CHARSET=latin1 CONNECTION=''mysql://',@AP_USER,':',@AP_PSWD,'@',@AP_IP,':',@AP_PORT,'/gt_covmo/usr_cell_upload''
					;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
	
		SET @SqlCmd=CONCAT('CREATE TEMPORARY TABLE tmp_cell_group_',WORKER_ID,' 
					(
					  `group_id` int(11) NOT NULL,
					  `cell_id` int(11) NOT NULL,
					  `enodeb_id` int(11) NOT NULL,
					  `pu_id` int(11) DEFAULT NULL,
					  KEY (`cell_id`,`enodeb_id`)					  
					) ENGINE=MYISAM;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
	
		SET @SqlCmd=CONCAT('INSERT INTO tmp_cell_group_',WORKER_ID,' 
					(`group_id`,`pu_id`,`cell_id`,`enodeb_id`)
					SELECT `group_id`,`pu_id`,`cell_id`,`enodeb_id` FROM tmp_usr_cell_upload_',WORKER_ID,' WHERE `group_id`=',CELL_GID,';');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		
		SET CELL_STR=CONCAT('(SELECT NULL FROM tmp_cell_group_',WORKER_ID,' B WHERE fact.enodeb_id= b.enodeb_id  AND fact.cell_id = b.cell_id)');
		
		SET sql_fetch_str=CONCAT(REPLACE(sql_fetch_str,':CELL_LIST:',CELL_STR),' ORDER BY NULL;');
		SET sql_check_str=CONCAT(REPLACE(sql_check_str,':CELL_LIST:',CELL_STR),' ORDER BY NULL;');
	END IF;
	
	IF sql_check_str IS  NULL OR sql_check_str = '' THEN	
		SET @SqlCmd=CONCAT(sql_fetch_str,';');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		INSERT INTO gt_gw_main.SP_LOG VALUES(GT_DB,'SP_CovMo_Alarm_Query',CONCAT('Select call cost:',TIMESTAMPDIFF(SECOND,STEP_START_TIME,SYSDATE()),' sec.'), NOW());
	ELSE		
		SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS tmp_check_table_',WORKER_ID,';');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
			
		SET @SqlCmd=CONCAT('CREATE TEMPORARY TABLE tmp_check_table_',WORKER_ID,' AS ',sql_check_str,';');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		
		SET @SqlCmd=CONCAT('SELECT COUNT(*) INTO @CNT FROM tmp_check_table_',WORKER_ID,';');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;	
		
		INSERT INTO gt_gw_main.SP_LOG VALUES(GT_DB,'SP_CovMo_Alarm_Query',CONCAT('Check value cost:',TIMESTAMPDIFF(SECOND,STEP_START_TIME,SYSDATE()),' sec.'), NOW());
		SET STEP_START_TIME := SYSDATE();
		
		IF IFNULL(@CNT,0)=0 THEN 
			SELECT 'No Data available!' AS NoSessionAvailable;
			LEAVE a_label;
		ELSE		
			SET @SqlCmd=CONCAT(sql_fetch_str,';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
		INSERT INTO gt_gw_main.SP_LOG VALUES(GT_DB,'SP_CovMo_Alarm_Query',CONCAT('Select call cost:',TIMESTAMPDIFF(SECOND,STEP_START_TIME,SYSDATE()),' sec.'), NOW());
		END IF;
		
		SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS tmp_check_table_',WORKER_ID,';');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		
		SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS tmp_cell_group_',WORKER_ID,' ;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		
		SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS tmp_usr_cell_upload_',WORKER_ID,' ;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;	
	END IF;
	
	INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'SP_CovMo_Alarm_Query',CONCAT('Done: ',TIMESTAMPDIFF(SECOND,START_TIME,SYSDATE()),' seconds.'), NOW());
