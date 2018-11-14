CREATE DEFINER=`covmo`@`%` PROCEDURE `SP_CovMo_Cross_Query_Parallel_Remote`(IN SQL_STR TEXT,IN POLYGON_STR LONGTEXT,IN IMSI_STR TEXT,IN CELL_GID SMALLINT(6),IN PU_ID INT(11),IN PLOYGON_ID VARCHAR(100),IN LIMT_RAW_COUNT  SMALLINT(6))
BEGIN
	DECLARE WORKER_ID VARCHAR(10) DEFAULT CONNECTION_ID();
	DECLARE FSQL_STR LONGTEXT;
	DECLARE CELL_STR VARCHAR(1000);
	DECLARE i INT DEFAULT 0;
	INSERT INTO gt_gw_main.sp_log VALUES('gt_gw_main','SP_CovMo_Cross_Query_Parallel_Remote',CONCAT(' Start ,',WORKER_ID), NOW());	
	
	SET FSQL_STR=SQL_STR;
	IF LENGTH(POLYGON_STR)>0 THEN		
		SET @v_Max=gt_covmo_csv_count(PLOYGON_ID,',');
		IF @v_Max=1  THEN 
		BEGIN
			IF LOCATE('@polygon_id_',FSQL_STR)>0 THEN 
				SET FSQL_STR=CONCAT(REPLACE(FSQL_STR,CONCAT('@polygon_id_',gt_covmo_csv_get(PLOYGON_ID,1),'@'),POLYGON_STR),' ');
			ELSE 			
				SET FSQL_STR=CONCAT(REPLACE(FSQL_STR,'@polygon_id',POLYGON_STR),' ');
			END IF;	
		END;
		ELSE 
		BEGIN
			SET i=1;
			WHILE i <= @v_Max DO
			BEGIN
				SET FSQL_STR=CONCAT(REPLACE(FSQL_STR,CONCAT('@polygon_id_',gt_covmo_csv_get(PLOYGON_ID,i),'@'),SPLIT_STR(POLYGON_STR,'|',i)),' ');
				SET i = i + 1;
			END;
			END WHILE;
		END;
		END IF;
	END IF;
	IF LENGTH(IMSI_STR)>0 THEN 		
		SET FSQL_STR=CONCAT(REPLACE(FSQL_STR,'@imsi_group_id',SUBSTRING(IN_QUOTE(IMSI_STR),3,LENGTH(IN_QUOTE(IMSI_STR))-4)),' '); 
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
		
		SET @SqlCmd=CONCAT('DROP TABLE IF EXISTS gt_gw_main.tmp_cell_group_',WORKER_ID,' ;');
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
	
		SET @SqlCmd=CONCAT('CREATE TABLE gt_gw_main.tmp_cell_group_',WORKER_ID,' 
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
	
		SET @SqlCmd=CONCAT('INSERT INTO gt_gw_main.tmp_cell_group_',WORKER_ID,' 
					(`group_id`,`pu_id`,`cell_id`,`enodeb_id`)
					SELECT `group_id`,`pu_id`,`cell_id`,`enodeb_id` FROM tmp_usr_cell_upload_',WORKER_ID,' WHERE `group_id`=',CELL_GID,' AND pu_id=',PU_ID,';');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		
		SET CELL_STR=CONCAT('(SELECT NULL FROM gt_gw_main.tmp_cell_group_',WORKER_ID,' B WHERE fact.enodeb_id=b.enodeb_id AND fact.cell_id=b.cell_id)');
		
		IF LOCATE('opt_cf_report_lte',FSQL_STR) > 0
			THEN
			SET CELL_STR=CONCAT('(SELECT NULL FROM gt_gw_main.tmp_cell_group_',WORKER_ID,' B WHERE fact.enodeb_id=b.enodeb_id AND fact.cell_id_a =b.cell_id  OR (fact.enodeb_id_b=b.enodeb_id AND fact.cell_id_b=b.cell_id) )');
		END IF;	
	
		SET FSQL_STR=CONCAT(REPLACE(FSQL_STR,'@CELL_GID',CELL_STR),' ');
		IF LOCATE('table_erab_volte_lte',FSQL_STR) > 0 THEN
			IF LOCATE('ERAB_START_SERVING_ENODEB',FSQL_STR) > 0 THEN 
				SET FSQL_STR = REPLACE(FSQL_STR,'fact.enodeb_id','fact.ERAB_START_SERVING_ENODEB');
				SET FSQL_STR = REPLACE(FSQL_STR,'fact.cell_id','fact.ERAB_START_SERVING_CELL');
			ELSEIF LOCATE('ERAB_END_SERVING_ENODEB',FSQL_STR) > 0 THEN 
				SET FSQL_STR = REPLACE(FSQL_STR,'fact.enodeb_id','ERAB_END_SERVING_ENODEB');
				SET FSQL_STR = REPLACE(FSQL_STR,'fact.cell_id','fact.ERAB_END_SERVING_CELL');
			END IF;
		ELSEIF LOCATE('table_call_failure_lte',FSQL_STR) > 0 THEN
			IF LOCATE('POS_LAST_S_ENODEB',FSQL_STR) > 0 THEN 
				SET FSQL_STR = REPLACE(FSQL_STR,'fact.enodeb_id','fact.POS_LAST_S_ENODEB');
				SET FSQL_STR = REPLACE(FSQL_STR,'fact.cell_id','fact.POS_LAST_S_CELL');
			ELSEIF LOCATE('POS_FIRST_S_ENODEB',FSQL_STR) > 0 THEN 
				SET FSQL_STR = REPLACE(FSQL_STR,'fact.enodeb_id','fact.POS_FIRST_S_ENODEB');
				SET FSQL_STR = REPLACE(FSQL_STR,'fact.cell_id','fact.POS_FIRST_S_CELL');
			ELSEIF LOCATE('POS_FIRST_TILE',FSQL_STR) > 0 THEN 
				SET FSQL_STR = REPLACE(FSQL_STR,'fact.enodeb_id','fact.POS_FIRST_S_ENODEB');
				SET FSQL_STR = REPLACE(FSQL_STR,'fact.cell_id','fact.POS_FIRST_S_CELL');
			ELSEIF LOCATE('POS_LAST_TILE',FSQL_STR) > 0 THEN 
				SET FSQL_STR = REPLACE(FSQL_STR,'fact.enodeb_id','fact.POS_LAST_S_ENODEB');
				SET FSQL_STR = REPLACE(FSQL_STR,'fact.cell_id','fact.POS_LAST_S_CELL');
			END IF;
		ELSEIF LOCATE('table_call_lte',FSQL_STR) > 0 THEN
			IF LOCATE('POS_LAST_S_ENODEB',FSQL_STR) > 0 THEN 
				SET FSQL_STR = REPLACE(FSQL_STR,'fact.enodeb_id','fact.POS_LAST_S_ENODEB');
				SET FSQL_STR = REPLACE(FSQL_STR,'fact.cell_id','fact.POS_LAST_S_CELL');
			ELSEIF LOCATE('POS_FIRST_S_ENODEB',FSQL_STR) > 0 THEN 
				SET FSQL_STR = REPLACE(FSQL_STR,'fact.enodeb_id','fact.POS_FIRST_S_ENODEB');
				SET FSQL_STR = REPLACE(FSQL_STR,'fact.cell_id','fact.POS_FIRST_S_CELL');
			ELSEIF LOCATE('POS_FIRST_TILE',FSQL_STR) > 0 THEN 
				SET FSQL_STR = REPLACE(FSQL_STR,'fact.enodeb_id','fact.POS_FIRST_S_ENODEB');
				SET FSQL_STR = REPLACE(FSQL_STR,'fact.cell_id','fact.POS_FIRST_S_CELL');
			ELSEIF LOCATE('POS_LAST_TILE',FSQL_STR) > 0 THEN 
				SET FSQL_STR = REPLACE(FSQL_STR,'fact.enodeb_id','fact.POS_LAST_S_ENODEB');
				SET FSQL_STR = REPLACE(FSQL_STR,'fact.cell_id','fact.POS_LAST_S_CELL');
			END IF;
		END IF;
	END IF;
	
	SET @SqlCmd=IF(LIMT_RAW_COUNT>0 ,CONCAT(FSQL_STR,' LIMIT ',LIMT_RAW_COUNT),FSQL_STR);
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('DROP TABLE IF EXISTS tmp_cell_group_',WORKER_ID,' ;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS tmp_usr_cell_upload_',WORKER_ID,' ;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	INSERT INTO gt_gw_main.sp_log VALUES('gt_gw_main','SP_CovMo_Cross_Query_Parallel_Remote',CONCAT(' END,',WORKER_ID), NOW());
