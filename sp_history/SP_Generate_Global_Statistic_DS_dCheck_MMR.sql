DELIMITER $$
USE `gt_global_statistic`$$
DROP PROCEDURE IF EXISTS `SP_CreateDB_LTE`$$
CREATE DEFINER=`covmo`@`%` PROCEDURE `SP_Generate_Global_Statistic_DS_dCheck_MMR`(IN TECH_MASK TINYINT(2),IN WORKER_ID VARCHAR(10),IN DS_FLAG TINYINT(2),IN RPT_TYPE TINYINT(2),IN TileResolution VARCHAR(10))
BEGIN	
	DECLARE v_finished_d INTEGER DEFAULT 0;
	DECLARE v_DATA_DATE_d VARCHAR(10) DEFAULT NULL;
	DECLARE v_group_db_name VARCHAR(100) DEFAULT '';
	DECLARE PARTITION_STR VARCHAR(1500) DEFAULT 
		' PARTITION BY LIST(DATA_HOUR)
		(
		 PARTITION h0 VALUES IN (0) ENGINE = MYISAM,
		 PARTITION h1 VALUES IN (1) ENGINE = MYISAM,
		 PARTITION h2 VALUES IN (2) ENGINE = MYISAM,
		 PARTITION h3 VALUES IN (3) ENGINE = MYISAM,
		 PARTITION h4 VALUES IN (4) ENGINE = MYISAM,
		 PARTITION h5 VALUES IN (5) ENGINE = MYISAM,
		 PARTITION h6 VALUES IN (6) ENGINE = MYISAM,
		 PARTITION h7 VALUES IN (7) ENGINE = MYISAM,
		 PARTITION h8 VALUES IN (8) ENGINE = MYISAM,
		 PARTITION h9 VALUES IN (9) ENGINE = MYISAM,
		 PARTITION h10 VALUES IN (10) ENGINE = MYISAM,
		 PARTITION h11 VALUES IN (11) ENGINE = MYISAM,
		 PARTITION h12 VALUES IN (12) ENGINE = MYISAM,
		 PARTITION h13 VALUES IN (13) ENGINE = MYISAM,
		 PARTITION h14 VALUES IN (14) ENGINE = MYISAM,
		 PARTITION h15 VALUES IN (15) ENGINE = MYISAM,
		 PARTITION h16 VALUES IN (16) ENGINE = MYISAM,
		 PARTITION h17 VALUES IN (17) ENGINE = MYISAM,
		 PARTITION h18 VALUES IN (18) ENGINE = MYISAM,
		 PARTITION h19 VALUES IN (19) ENGINE = MYISAM,
		 PARTITION h20 VALUES IN (20) ENGINE = MYISAM,
		 PARTITION h21 VALUES IN (21) ENGINE = MYISAM,
		 PARTITION h22 VALUES IN (22) ENGINE = MYISAM,
		 PARTITION h23 VALUES IN (23) ENGINE = MYISAM		
		 );';
		
	DECLARE TILE_DS_COLUMN_STR VARCHAR(5000) DEFAULT 
		'RSRP_SUM DOUBLE DEFAULT NULL,
		RSRP_CNT INT(11) DEFAULT NULL,
		RSCP_SUM DOUBLE DEFAULT NULL,
		RSCP_CNT INT(11) DEFAULT NULL,
		RXLEV_SUM DOUBLE DEFAULT NULL,
		RXLEV_CNT INT(11) DEFAULT NULL,
		RSRQ_SUM DOUBLE DEFAULT NULL,
		RSRQ_CNT INT(11) DEFAULT NULL,
		ECNO_SUM DOUBLE DEFAULT NULL,
		ECNO_CNT INT(11) DEFAULT NULL,
		RXQUAL_SUM DOUBLE DEFAULT NULL,
		RXQUAL_CNT INT(11) DEFAULT NULL,
		GSM_CS_CALL_CNT INT(11) DEFAULT NULL,
		GSM_PS_CALL_CNT INT(11) DEFAULT NULL,
		GSM_VOICE_DROP_CNT INT(11) DEFAULT NULL,
		GSM_GPRS_DROP_CNT INT(11) DEFAULT NULL,
		GSM_SMS_DROP_CNT INT(11) DEFAULT NULL,
		GSM_CS_BLOCK_CNT INT(11) DEFAULT NULL,
		GSM_PS_BLOCK_CNT INT(11) DEFAULT NULL,
		GSM_CS_CALL_DURATION BIGINT(20) DEFAULT NULL,
		GSM_CALL_SETUP_TIME_SUM BIGINT(20) DEFAULT NULL,
		GSM_CALL_SETUP_TIME_CNT INT(11) DEFAULT NULL,
	
		GSM_CS_CALL_SETUP_TIME_SUM INT(11) DEFAULT NULL,
		GSM_CS_CALL_SETUP_TIME_CNT INT(11) DEFAULT NULL,
		GSM_PS_CALL_SETUP_TIME_SUM INT(11) DEFAULT NULL,
		GSM_PS_CALL_SETUP_TIME_CNT INT(11) DEFAULT NULL,
	
		GSM_CS_SETUP_FAILURE_CNT INT(11) DEFAULT NULL,
		GSM_PS_SETUP_FAILURE_CNT INT(11) DEFAULT NULL,
		UMTS_CS_CALL_CNT INT(11) DEFAULT NULL,
		UMTS_PS_CALL_CNT INT(11) DEFAULT NULL,
		UMTS_VOICE_DROP_CNT INT(11) DEFAULT NULL,
		UMTS_PS_DROP_CNT INT(11) DEFAULT NULL,
		UMTS_CS_CALL_DURATION BIGINT(20) DEFAULT NULL,
		UMTS_CS_BLOCK_CNT INT(11) DEFAULT NULL,
		UMTS_PS_BLOCK_CNT INT(11) DEFAULT NULL,
		UMTS_UL_VOLUME DOUBLE DEFAULT NULL,
		UMTS_DL_VOLUME DOUBLE DEFAULT NULL,
		UMTS_UL_THROUPUT_SUM DOUBLE DEFAULT NULL,
		UMTS_UL_THROUPUT_CNT INT(11) DEFAULT NULL,
		UMTS_DL_THROUPUT_SUM DOUBLE DEFAULT NULL,
		UMTS_DL_THROUPUT_CNT INT(11) DEFAULT NULL,
		UMTS_CALL_SETUP_TIME_SUM BIGINT(20) DEFAULT NULL,
		UMTS_CALL_SETUP_TIME_CNT INT(11) DEFAULT NULL,
	
		UMTS_CS_CALL_SETUP_TIME_SUM INT(11) DEFAULT NULL,
		UMTS_CS_CALL_SETUP_TIME_CNT INT(11) DEFAULT NULL,
		UMTS_PS_CALL_SETUP_TIME_SUM INT(11) DEFAULT NULL,
		UMTS_PS_CALL_SETUP_TIME_CNT INT(11) DEFAULT NULL,
	
		UMTS_CS_SETUP_FAILURE_CNT INT(11) DEFAULT NULL,
		UMTS_PS_SETUP_FAILURE_CNT INT(11) DEFAULT NULL,
	
		UMTS_IRAT_HO_ATMP INT(11) DEFAULT NULL,
		UMTS_IRAT_HO_FAIL INT(11) DEFAULT NULL,
	
		LTE_CALL_CNT INT(11) DEFAULT NULL,
		LTE_DROP_CNT INT(11) DEFAULT NULL,
		LTE_BLOCK_CNT INT(11) DEFAULT NULL,
		LTE_UL_VOLUME DOUBLE DEFAULT NULL,
		LTE_DL_VOLUME DOUBLE DEFAULT NULL,
		LTE_UL_THROUPUT_SUM DOUBLE DEFAULT NULL,
		LTE_UL_THROUPUT_CNT INT(11) DEFAULT NULL,
		LTE_DL_THROUPUT_SUM DOUBLE DEFAULT NULL,
		LTE_DL_THROUPUT_CNT INT(11) DEFAULT NULL,
		LATENCY_SUM DOUBLE DEFAULT NULL,
		LATENCY_CNT INT(11) DEFAULT NULL,
		LTE_CALL_SETUP_TIME_SUM BIGINT(20) DEFAULT NULL,
		LTE_CALL_SETUP_TIME_CNT INT(11) DEFAULT NULL,
		LTE_SETUP_FAILURE_CNT INT(11) DEFAULT NULL,
		SRVCC_ATTEMPT_CNT INT(11) DEFAULT NULL,
		SRVCC_FAILURE_CNT INT(11) DEFAULT NULL,
		S1_HO_ATTEMPT INT(11) DEFAULT NULL,
		S1_HO_FAILURE INT(11) DEFAULT NULL,
		X2_HO_ATTEMPT INT(11) DEFAULT NULL,
		X2_HO_FAILURE INT(11) DEFAULT NULL,
		UMTS_MAX_UL_THROUPUT DOUBLE DEFAULT NULL,
		UMTS_MAX_DL_THROUPUT DOUBLE DEFAULT NULL,
		LTE_MAX_UL_THROUPUT DOUBLE DEFAULT NULL,
		LTE_MAX_DL_THROUPUT DOUBLE DEFAULT NULL,
	
		LTE_IRAT_TO_UMTS_ATMP INT(11) DEFAULT NULL,
		LTE_IRAT_TO_GERAN_ATMP INT(11) DEFAULT NULL,
		LTE_IRAT_TO_CDMA_ATMP INT(11) DEFAULT NULL,
		LTE_IRAT_TO_UMTS_FAIL INT(11) DEFAULT NULL,
		LTE_IRAT_TO_GERAN_FAIL INT(11) DEFAULT NULL,
		LTE_IRAT_TO_CDMA_FAIL INT(11) DEFAULT NULL,
	
		COMPLETED_PU_CNT SMALLINT(11) DEFAULT NULL,
		TOTAL_PU_CNT SMALLINT(11) DEFAULT NULL,';
		  
	SET @global_db='gt_global_statistic';
	
	SET @ZOOM_LEVEL3 = gt_covmo_csv_get(TileResolution,3);
	SET @ZOOM_LEVEL2 = gt_covmo_csv_get(TileResolution,2);
	SET @ZOOM_LEVEL1 = gt_covmo_csv_get(TileResolution,1);
	SET @TILE_ID_LVL1 = CONCAT('TILE_ID_',gt_covmo_csv_get(TileResolution,1));
	SET @TILE_ID_LVL2 = CONCAT('TILE_ID_',gt_covmo_csv_get(TileResolution,2));
		
	SET @SqlCmd=CONCAT('SELECT GROUP_CONCAT(DISTINCT CONCAT(`DATA_DATE`,'','',`TECH_MASK`) SEPARATOR ''|'' ) INTO @PU_STR
				FROM gt_global_statistic.table_running_task_ds;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
		
	SET @SqlCmd=CONCAT('SELECT GROUP_CONCAT(DISTINCT `group_id` ORDER BY `group_id` SEPARATOR ''|'') INTO @REG_GROUP FROM ',@global_db,'.`usr_polygon_reg_3`;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;		
        	
	SET @v_reg_m=1;
	SET @v_reg_Max=gt_covmo_csv_count(@REG_GROUP,'|');
	WHILE @v_reg_m <= @v_reg_Max DO
	BEGIN
		SET v_group_db_name=CONCAT('gt_global_statistic_g',gt_strtok(@REG_GROUP, @v_reg_m, '|'));
		SET @SqlCmd=CONCAT('SELECT COUNT(*) INTO @V_EXIST_DB FROM information_schema.`SCHEMATA`
					WHERE SCHEMA_NAME=''',v_group_db_name,''';');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		
		IF @V_EXIST_DB=0 THEN 			
			SET @SqlCmd=CONCAT('CREATE DATABASE ',v_group_db_name,';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;		
		END IF;
				
		SET @v_i=1;
		SET @v_R_Max=gt_covmo_csv_count(@PU_STR,'|');
		WHILE @v_i <= @v_R_Max DO
		BEGIN		
			SET v_DATA_DATE_d:=gt_covmo_csv_get(gt_strtok(@PU_STR, @v_i, '|'),1);
			SET @DATE_DY=DATE_FORMAT(v_DATA_DATE_d,'%Y%m%d');
			
			SET @SqlCmd=CONCAT('SELECT COUNT(*) INTO @V_EXIST_DS_19_HR FROM information_schema.`TABLES`
						WHERE TABLE_SCHEMA=''',v_group_db_name,''' 
						AND TABLE_NAME=''table_tile_',@ZOOM_LEVEL3,'_ds_hr_',@DATE_DY,''';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
					
			IF @V_EXIST_DS_19_HR=0 THEN 
				SET @SqlCmd=CONCAT('CREATE TABLE ',v_group_db_name,'.table_tile_',@ZOOM_LEVEL3,'_ds_hr_',@DATE_DY,' 
							(
							  `TILE_ID` bigint(20) NOT NULL,
							  `DATA_DATE` datetime NOT NULL,
							  `DATA_HOUR` tinyint(4) NOT NULL,
							  ',TILE_DS_COLUMN_STR,' 
							  `',@TILE_ID_LVL2,'` bigint(20) NOT NULL,
							  `',@TILE_ID_LVL1,'` bigint(20) NOT NULL,
							  `REG_1_ID` bigint(20) DEFAULT NULL,
							  `REG_2_ID` bigint(20) DEFAULT NULL,
							  `REG_3_ID` bigint(20) DEFAULT NULL,
							  PRIMARY KEY (`TILE_ID`,`DATA_DATE`,`DATA_HOUR`)
							) ENGINE=MyISAM DEFAULT CHARSET=latin1 DELAY_KEY_WRITE=1' 
							,PARTITION_STR);
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				INSERT IGNORE INTO `gt_global_statistic`.`table_created_history` VALUES(v_DATA_DATE_d,NULL,4,NOW(),1,CONCAT('table_tile_',@ZOOM_LEVEL3,'_ds_hr_',@DATE_DY));	
				
			END IF;	
			
			SET @SqlCmd=CONCAT('SELECT COUNT(*) INTO @V_EXIST_DS_16_HR FROM information_schema.`TABLES`
						WHERE TABLE_SCHEMA=''',v_group_db_name,''' 
						AND TABLE_NAME=''table_tile_',@ZOOM_LEVEL2,'_ds_hr_',@DATE_DY,''';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
					
			IF @V_EXIST_DS_16_HR=0 THEN 
				SET @SqlCmd=CONCAT('CREATE TABLE ',v_group_db_name,'.table_tile_',@ZOOM_LEVEL2,'_ds_hr_',@DATE_DY,' 
							(
							  `TILE_ID` bigint(20) NOT NULL,
							  `DATA_DATE` datetime NOT NULL,
							  `DATA_HOUR` tinyint(4) NOT NULL,
							  ',TILE_DS_COLUMN_STR,'  
							  `REG_1_ID` bigint(20) DEFAULT NULL,
							  `REG_2_ID` bigint(20) DEFAULT NULL,
							  `REG_3_ID` bigint(20) DEFAULT NULL,
							  PRIMARY KEY (`TILE_ID`,`DATA_DATE`,`DATA_HOUR`)
							) ENGINE=MyISAM DEFAULT CHARSET=latin1 DELAY_KEY_WRITE=1' 
							,PARTITION_STR);
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				INSERT IGNORE INTO `gt_global_statistic`.`table_created_history` VALUES(v_DATA_DATE_d,NULL,4,NOW(),1,CONCAT('table_tile_',@ZOOM_LEVEL2,'_ds_hr_',@DATE_DY));
			END IF;			
			
			SET @SqlCmd=CONCAT('SELECT COUNT(*) INTO @V_EXIST_DS_13_HR FROM information_schema.`TABLES`
						WHERE TABLE_SCHEMA=''',v_group_db_name,''' 
						AND TABLE_NAME=''table_tile_',@ZOOM_LEVEL1,'_ds_hr_',@DATE_DY,''';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
					
			IF @V_EXIST_DS_13_HR=0 THEN 
				SET @SqlCmd=CONCAT('CREATE TABLE ',v_group_db_name,'.table_tile_',@ZOOM_LEVEL1,'_ds_hr_',@DATE_DY,' 
							(
							  `TILE_ID` bigint(20) NOT NULL,
							  `DATA_DATE` datetime NOT NULL,
							  `DATA_HOUR` tinyint(4) NOT NULL,
							  ',TILE_DS_COLUMN_STR,'  
							  `REG_1_ID` bigint(20) DEFAULT NULL,
							  `REG_2_ID` bigint(20) DEFAULT NULL,
							  `REG_3_ID` bigint(20) DEFAULT NULL,
							  PRIMARY KEY (`TILE_ID`,`DATA_DATE`,`DATA_HOUR`)
							) ENGINE=MyISAM DEFAULT CHARSET=latin1 DELAY_KEY_WRITE=1' 
							,PARTITION_STR);
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				INSERT IGNORE INTO `gt_global_statistic`.`table_created_history` VALUES(v_DATA_DATE_d,NULL,4,NOW(),1,CONCAT('table_tile_',@ZOOM_LEVEL1,'_ds_hr_',@DATE_DY));
			END IF;			
			
			SET @SqlCmd=CONCAT('SELECT COUNT(*) INTO @V_EXIST_DS_R3_HR FROM information_schema.`TABLES`
						WHERE TABLE_SCHEMA=''',v_group_db_name,''' 
						AND TABLE_NAME=''table_reg_3_ds_hr_',@DATE_DY,''';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
					
			IF @V_EXIST_DS_R3_HR=0 THEN 
				SET @SqlCmd=CONCAT('CREATE TABLE ',v_group_db_name,'.table_reg_3_ds_hr_',@DATE_DY,' 
							(
							  `REG_ID` bigint(20) NOT NULL,
							  `DATA_DATE` datetime NOT NULL,
							  `DATA_HOUR` tinyint(4) NOT NULL,
							  ',TILE_DS_COLUMN_STR,'  
							  PRIMARY KEY (`REG_ID`,`DATA_DATE`,`DATA_HOUR`)
							) ENGINE=MyISAM DEFAULT CHARSET=latin1 DELAY_KEY_WRITE=1 '
							,PARTITION_STR);
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				INSERT IGNORE INTO `gt_global_statistic`.`table_created_history` VALUES(v_DATA_DATE_d,NULL,4,NOW(),1,CONCAT('table_reg_3_ds_hr_',@DATE_DY));
			END IF;	
			
			SET @SqlCmd=CONCAT('SELECT COUNT(*) INTO @V_EXIST_DS_R2_HR FROM information_schema.`TABLES`
						WHERE TABLE_SCHEMA=''',v_group_db_name,''' 
						AND TABLE_NAME=''table_reg_2_ds_hr_',@DATE_DY,''';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
					
			IF @V_EXIST_DS_R2_HR=0 THEN 
				SET @SqlCmd=CONCAT('CREATE TABLE ',v_group_db_name,'.table_reg_2_ds_hr_',@DATE_DY,' 
							(
							  `REG_ID` bigint(20) NOT NULL,
							  `DATA_DATE` datetime NOT NULL,
							  `DATA_HOUR` tinyint(4) NOT NULL,
							  ',TILE_DS_COLUMN_STR,'  
							  PRIMARY KEY (`REG_ID`,`DATA_DATE`,`DATA_HOUR`)
							) ENGINE=MyISAM DEFAULT CHARSET=latin1 DELAY_KEY_WRITE=1 '
							,PARTITION_STR);
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				INSERT IGNORE INTO `gt_global_statistic`.`table_created_history` VALUES(v_DATA_DATE_d,NULL,4,NOW(),1,CONCAT('table_reg_2_ds_hr_',@DATE_DY));
			END IF;	
			
			SET @SqlCmd=CONCAT('SELECT COUNT(*) INTO @V_EXIST_DS_R1_HR FROM information_schema.`TABLES`
						WHERE TABLE_SCHEMA=''',v_group_db_name,''' 
						AND TABLE_NAME=''table_reg_1_ds_hr_',@DATE_DY,''';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
					
			IF @V_EXIST_DS_R1_HR=0 THEN 
				SET @SqlCmd=CONCAT('CREATE TABLE ',v_group_db_name,'.table_reg_1_ds_hr_',@DATE_DY,' 
							(
							  `REG_ID` bigint(20) NOT NULL,
							  `DATA_DATE` datetime NOT NULL,
							  `DATA_HOUR` tinyint(4) NOT NULL,
							  ',TILE_DS_COLUMN_STR,'  
							  PRIMARY KEY (`REG_ID`,`DATA_DATE`,`DATA_HOUR`)
							) ENGINE=MyISAM DEFAULT CHARSET=latin1 DELAY_KEY_WRITE=1 '
							,PARTITION_STR);
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				INSERT IGNORE INTO `gt_global_statistic`.`table_created_history` VALUES(v_DATA_DATE_d,NULL,4,NOW(),1,CONCAT('table_reg_1_ds_hr_',@DATE_DY));
			END IF;	
			
			SET @SqlCmd=CONCAT('SELECT COUNT(*) INTO @V_EXIST_DS_19_DY FROM information_schema.`TABLES`
						WHERE TABLE_SCHEMA=''',v_group_db_name,''' 
						AND TABLE_NAME=''table_tile_',@ZOOM_LEVEL3,'_ds_dy_',@DATE_DY,''';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
					
			IF @V_EXIST_DS_19_DY=0 THEN 
				SET @SqlCmd=CONCAT('CREATE TABLE ',v_group_db_name,'.table_tile_',@ZOOM_LEVEL3,'_ds_dy_',@DATE_DY,' 
							(
							  `TILE_ID` bigint(20) NOT NULL,
							  `DATA_DATE` datetime NOT NULL,
							  ',TILE_DS_COLUMN_STR,' 
							  `',@TILE_ID_LVL2,'` bigint(20) NOT NULL,
							  `',@TILE_ID_LVL1,'` bigint(20) NOT NULL,
							  `REG_1_ID` bigint(20) DEFAULT NULL,
							  `REG_2_ID` bigint(20) DEFAULT NULL,
							  `REG_3_ID` bigint(20) DEFAULT NULL,
							  PRIMARY KEY (`TILE_ID`,`DATA_DATE`)
							) ENGINE=MyISAM DEFAULT CHARSET=latin1 DELAY_KEY_WRITE=1;');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				INSERT IGNORE INTO `gt_global_statistic`.`table_created_history` VALUES(v_DATA_DATE_d,NULL,4,NOW(),2,CONCAT('table_tile_',@ZOOM_LEVEL3,'_ds_dy_',@DATE_DY));	
			END IF;	
			
			SET @SqlCmd=CONCAT('SELECT COUNT(*) INTO @V_EXIST_DS_16_DY FROM information_schema.`TABLES`
						WHERE TABLE_SCHEMA=''',v_group_db_name,''' 
						AND TABLE_NAME=''table_tile_',@ZOOM_LEVEL2,'_ds_dy_',@DATE_DY,''';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
					
			IF @V_EXIST_DS_16_DY=0 THEN 
				SET @SqlCmd=CONCAT('CREATE TABLE ',v_group_db_name,'.table_tile_',@ZOOM_LEVEL2,'_ds_dy_',@DATE_DY,' 
							(
							  `TILE_ID` bigint(20) NOT NULL,
							  `DATA_DATE` datetime NOT NULL,
							  ',TILE_DS_COLUMN_STR,'  
							  `REG_1_ID` bigint(20) DEFAULT NULL,
							  `REG_2_ID` bigint(20) DEFAULT NULL,
							  `REG_3_ID` bigint(20) DEFAULT NULL,
							  PRIMARY KEY (`TILE_ID`,`DATA_DATE`)
							) ENGINE=MyISAM DEFAULT CHARSET=latin1 DELAY_KEY_WRITE=1;');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				INSERT IGNORE INTO `gt_global_statistic`.`table_created_history` VALUES(v_DATA_DATE_d,NULL,4,NOW(),2,CONCAT('table_tile_',@ZOOM_LEVEL2,'_ds_dy_',@DATE_DY));
			END IF;			
			
			SET @SqlCmd=CONCAT('SELECT COUNT(*) INTO @V_EXIST_DS_13_DY FROM information_schema.`TABLES`
						WHERE TABLE_SCHEMA=''',v_group_db_name,''' 
						AND TABLE_NAME=''table_tile_',@ZOOM_LEVEL1,'_ds_dy_',@DATE_DY,''';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
					
			IF @V_EXIST_DS_13_DY=0 THEN 
				SET @SqlCmd=CONCAT('CREATE TABLE ',v_group_db_name,'.table_tile_',@ZOOM_LEVEL1,'_ds_dy_',@DATE_DY,' 
							(
							  `TILE_ID` bigint(20) NOT NULL,
							  `DATA_DATE` datetime NOT NULL,
							  ',TILE_DS_COLUMN_STR,'  
							  `REG_1_ID` bigint(20) DEFAULT NULL,
							  `REG_2_ID` bigint(20) DEFAULT NULL,
							  `REG_3_ID` bigint(20) DEFAULT NULL,
							  PRIMARY KEY (`TILE_ID`,`DATA_DATE`)
							) ENGINE=MyISAM DEFAULT CHARSET=latin1 DELAY_KEY_WRITE=1;');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				INSERT IGNORE INTO `gt_global_statistic`.`table_created_history` VALUES(v_DATA_DATE_d,NULL,4,NOW(),2,CONCAT('table_tile_',@ZOOM_LEVEL1,'_ds_dy_',@DATE_DY));
			END IF;			
			
			SET @SqlCmd=CONCAT('SELECT COUNT(*) INTO @V_EXIST_DS_R3_DY FROM information_schema.`TABLES`
						WHERE TABLE_SCHEMA=''',v_group_db_name,''' 
						AND TABLE_NAME=''table_reg_3_ds_dy_',@DATE_DY,''';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
					
			IF @V_EXIST_DS_R3_DY=0 THEN 
				SET @SqlCmd=CONCAT('CREATE TABLE ',v_group_db_name,'.table_reg_3_ds_dy_',@DATE_DY,' 
							(
							  `REG_ID` bigint(20) NOT NULL,
							  `DATA_DATE` datetime NOT NULL,
							  ',TILE_DS_COLUMN_STR,'  
							  PRIMARY KEY (`REG_ID`,`DATA_DATE`)
							) ENGINE=MyISAM DEFAULT CHARSET=latin1 DELAY_KEY_WRITE=1;');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				INSERT IGNORE INTO `gt_global_statistic`.`table_created_history` VALUES(v_DATA_DATE_d,NULL,4,NOW(),2,CONCAT('table_reg_3_ds_dy_',@DATE_DY));
			END IF;	
			
			SET @SqlCmd=CONCAT('SELECT COUNT(*) INTO @V_EXIST_DS_R2_DY FROM information_schema.`TABLES`
						WHERE TABLE_SCHEMA=''',v_group_db_name,''' 
						AND TABLE_NAME=''table_reg_2_ds_dy_',@DATE_DY,''';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
					
			IF @V_EXIST_DS_R2_DY=0 THEN 
				SET @SqlCmd=CONCAT('CREATE TABLE ',v_group_db_name,'.table_reg_2_ds_dy_',@DATE_DY,' 
							(
							  `REG_ID` bigint(20) NOT NULL,
							  `DATA_DATE` datetime NOT NULL,
							  ',TILE_DS_COLUMN_STR,'  
							  PRIMARY KEY (`REG_ID`,`DATA_DATE`)
							) ENGINE=MyISAM DEFAULT CHARSET=latin1 DELAY_KEY_WRITE=1;');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				INSERT IGNORE INTO `gt_global_statistic`.`table_created_history` VALUES(v_DATA_DATE_d,NULL,4,NOW(),2,CONCAT('table_reg_2_ds_dy_',@DATE_DY));
			END IF;	
			
			SET @SqlCmd=CONCAT('SELECT COUNT(*) INTO @V_EXIST_DS_R1_DY FROM information_schema.`TABLES`
						WHERE TABLE_SCHEMA=''',v_group_db_name,''' 
						AND TABLE_NAME=''table_reg_1_ds_dy_',@DATE_DY,''';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
					
			IF @V_EXIST_DS_R1_DY=0 THEN 
				SET @SqlCmd=CONCAT('CREATE TABLE ',v_group_db_name,'.table_reg_1_ds_dy_',@DATE_DY,' 
							(
							  `REG_ID` bigint(20) NOT NULL,
							  `DATA_DATE` datetime NOT NULL,
							  ',TILE_DS_COLUMN_STR,'  
							  PRIMARY KEY (`REG_ID`,`DATA_DATE`)
							) ENGINE=MyISAM DEFAULT CHARSET=latin1 DELAY_KEY_WRITE=1;');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				INSERT IGNORE INTO `gt_global_statistic`.`table_created_history` VALUES(v_DATA_DATE_d,NULL,4,NOW(),2,CONCAT('table_reg_1_ds_dy_',@DATE_DY));
			END IF;			
			SET @v_i=@v_i+1;
		END;
		END WHILE;
		SET @v_reg_m=@v_reg_m+1;
	END;
	END WHILE;
END$$
DELIMITER ;
