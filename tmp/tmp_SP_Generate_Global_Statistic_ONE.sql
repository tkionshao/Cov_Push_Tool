CREATE DEFINER=`covmo`@`%` PROCEDURE `SP_Generate_Global_Statistic_ONE`(IN TECH_MASK TINYINT(2),IN exDate VARCHAR(10),IN exHour TINYINT(2),IN FLAG TINYINT(2),IN DS_FLAG TINYINT(2),IN RPT_TYPE TINYINT(2),IN PENDING_FLAG TINYINT(2),IN IMSI_CELL TINYINT(2),IN TRENDING_FLAG TINYINT(2))
BEGIN	
	DECLARE WORKER_ID VARCHAR(10) DEFAULT CONNECTION_ID();
	DECLARE START_TIME DATETIME DEFAULT SYSDATE();
	DECLARE TileResolution VARCHAR(10);
	DECLARE v_DATA_DATE_d VARCHAR(10) DEFAULT NULL;
	DECLARE v_TECH_MASK_d VARCHAR(10) DEFAULT NULL;
	
	SET GLOBAL keycache1.key_buffer_size=2*1024*1024;
	CACHE INDEX gt_global_statistic.table_region_tile_g IN keycache1;
	LOAD INDEX INTO CACHE gt_global_statistic.table_region_tile_g;
	INSERT INTO gt_gw_main.SP_LOG VALUES('gt_global_statistic','SP_Generate_Global_Statistic_ONE','START', START_TIME);
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
	
	SET @SqlCmd=CONCAT('CREATE TABLE IF NOT EXISTS `gt_global_statistic`.tmp_table_call_cnt_',WORKER_ID,' LIKE `gt_global_statistic`.`table_call_cnt_history`;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('ALTER TABLE `gt_global_statistic`.tmp_table_call_cnt_',WORKER_ID,' ADD `IsSuccess` TINYINT(4) NULL DEFAULT ''0'';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS gt_global_statistic.tmp_table_call_cnt_ap_',WORKER_ID,' ;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('CREATE TEMPORARY TABLE gt_global_statistic.tmp_table_call_cnt_ap_',WORKER_ID,' 
			(				  
			  `DATA_DATE` date DEFAULT NULL,
			  `DATA_HOUR` tinyint(4) DEFAULT NULL,
			  `PU_ID` mediumint(9) DEFAULT NULL,
			  `TECH_MASK` tinyint(4) NOT NULL DEFAULT ''2'',
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
	
	IF FLAG IN (0,1) THEN 
		SET @SqlCmd=CONCAT('SELECT spider_bg_direct_sql(CONCAT('' SELECT `DATA_DATE`,`DATA_HOUR`,`PU_ID`,`TECH_MASK` FROM `gt_covmo`.`table_call_cnt`
									WHERE `DATA_DATE`=''''',exDate,''''' AND `DATA_HOUR`=',exHour,CASE TECH_MASK WHEN 0 THEN '' ELSE CONCAT(' AND TECH_MASK=',TECH_MASK) END , '
									GROUP BY `DATA_DATE`,`DATA_HOUR`,`PU_ID`,`TECH_MASK`;'') 
				, ''gt_global_statistic.tmp_table_call_cnt_ap_',WORKER_ID,'''
				, CONCAT(''HOST ''''',@AP_IP,''''', PORT ''''',@AP_PORT,''''',USER ''''',@AP_USER,''''', PASSWORD ''''',@AP_PSWD,''''''')
				) INTO @bb
				;');
	ELSE 
		SET @SqlCmd=CONCAT('INSERT INTO gt_global_statistic.tmp_table_call_cnt_ap_',WORKER_ID,'
					SELECT `DATA_DATE`,NULL AS `DATA_HOUR`,NULL AS `PU_ID`,`TECH_MASK` FROM `gt_global_statistic`.`table_call_cnt_history`
					WHERE `DATA_DATE`=''',exDate,''' ',CASE TECH_MASK WHEN 0 THEN '' ELSE CONCAT(' AND TECH_MASK=',TECH_MASK) END , '
					GROUP BY `DATA_DATE`,`TECH_MASK`;');
	END IF;
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('INSERT INTO `gt_global_statistic`.tmp_table_call_cnt_',WORKER_ID,' 
				(`DATA_DATE`,`DATA_HOUR`,`PU_ID`,`TECH_MASK`)
				SELECT `DATA_DATE`,
					`DATA_HOUR`,
					`PU_ID`,
					`TECH_MASK`
 				FROM gt_global_statistic.tmp_table_call_cnt_ap_',WORKER_ID,' a
				WHERE NOT EXISTS
				(
					SELECT NULL 
					FROM `gt_global_statistic`.`table_call_cnt_history` b
					WHERE a.`DATA_DATE`=b.`DATA_DATE` AND a.`DATA_HOUR`=b.`DATA_HOUR` AND a.`PU_ID`=b.`PU_ID` AND a.`TECH_MASK`=b.`TECH_MASK`
				)
				;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	IF PENDING_FLAG=1 THEN
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
	END IF;
	
	SET @SqlCmd=CONCAT('SELECT COUNT(*) INTO @CNT FROM `gt_global_statistic`.tmp_table_call_cnt_',WORKER_ID,';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	IF (@CNT>0) OR (FLAG=2 AND TRENDING_FLAG <>1) THEN 
	
		SELECT att_value INTO @ZOOM_LEVEL FROM `gt_global_statistic`.`nw_config` WHERE `group_name`='system' AND att_name = 'MapResolution';
		SELECT att_value INTO @TileResolution FROM `gt_global_statistic`.`nw_config` WHERE group_name = 'tile' AND att_name = 'TileResolution';
		
		SET TileResolution=CONCAT(@TileResolution,',',@ZOOM_LEVEL);
		
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
		CALL gt_global_statistic.`SP_Generate_Global_Statistic_dCheck_MMR`(TECH_MASK,WORKER_ID,DS_FLAG,RPT_TYPE,IMSI_CELL,TileResolution); 
	
		IF FLAG IN (0,1) THEN 	
			CALL gt_global_statistic.`SP_Generate_Global_Statistic_Sub_TmpTbl_Crt`(WORKER_ID,RPT_TYPE,IMSI_CELL,TileResolution);
		END IF;
		
		IF (RPT_TYPE & 1)>0 THEN 
			CALL gt_global_statistic.`SP_Generate_Global_Statistic_dsp_MMR`(WORKER_ID,FLAG,exDate,'TILE',DS_FLAG,PENDING_FLAG,IMSI_CELL,TileResolution);
		END IF;
		
		IF (RPT_TYPE & 2)>0 THEN
			SET @SqlCmd=CONCAT('SELECT COUNT(*) INTO @V_EXIST_TMP_DIM_HANDSET FROM information_schema.`TABLES`
						WHERE TABLE_SCHEMA=''gt_aggregate_db'' AND TABLE_NAME=''tmp_dim_handset'';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
			
			IF @V_EXIST_TMP_DIM_HANDSET=0 THEN
				SET @SqlCmd=CONCAT('CREATE TABLE gt_aggregate_db.tmp_dim_handset AS 
							SELECT make_id,model_id,CONCAT(manufacturer,''-'',model) AS MAKE_MODEL FROM gt_covmo.dim_handset 
							GROUP BY make_id,model_id;');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
							
				SET @SqlCmd=CONCAT('CREATE INDEX IX_model_id ON gt_aggregate_db.tmp_dim_handset (`make_id`,`model_id`,MAKE_MODEL);');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
			END IF;
			CALL gt_global_statistic.`SP_Generate_Global_Statistic_dsp_MMR`(WORKER_ID,FLAG,exDate,'IMSI',DS_FLAG,PENDING_FLAG,IMSI_CELL,TileResolution);
		END IF;
		
		IF (RPT_TYPE & 4)>0 THEN 
			CALL gt_global_statistic.`SP_Generate_Global_Statistic_dsp_MMR`(WORKER_ID,FLAG,exDate,'CELL',DS_FLAG,PENDING_FLAG,IMSI_CELL,TileResolution);
		END IF;
		
		IF (RPT_TYPE & 8)>0 THEN 
			CALL gt_global_statistic.`SP_Generate_Global_Statistic_dsp_MMR`(WORKER_ID,FLAG,exDate,'ROAMER',DS_FLAG,PENDING_FLAG,IMSI_CELL,TileResolution);
		END IF;
		
		IF (RPT_TYPE & 16)>0 THEN 
			CALL gt_global_statistic.`SP_Generate_Global_Statistic_dsp_MMR`(WORKER_ID,FLAG,exDate,'FAILCAUSE',DS_FLAG,PENDING_FLAG,IMSI_CELL,TileResolution);
		END IF;
	
		IF (RPT_TYPE & 32)>0 THEN 
			CALL gt_global_statistic.`SP_Generate_Global_Statistic_dsp_MMR`(WORKER_ID,FLAG,exDate,'SUBSCRIBER',DS_FLAG,PENDING_FLAG,IMSI_CELL,TileResolution);
		END IF;
	
		SET GLOBAL keycache1.key_buffer_size=0;
		
		CALL gt_global_statistic.`SP_Generate_Global_Statistic_log_MMR`(TECH_MASK,WORKER_ID);
		IF FLAG IN (0,1) THEN 
			SET @SqlCmd=CONCAT('INSERT INTO `gt_global_statistic`.`table_call_cnt_history`
					    (`DATA_DATE`,
					     `DATA_HOUR`,
					     `PU_ID`,
					     `TECH_MASK`,
					     `CREATE_TIME`,
					     `FLAG_IMSI_HR`,
					     `FLAG_IMSI_DY`,
					     `FLAG_MAKE_HR`,
					     `FLAG_MAKE_DY`,
					     `FLAG_IMSI_WK`,
					     `FLAG_IMSI_MN`,
					     `FLAG_MAKE_WK`,
					     `FLAG_MAKE_MN`,
					     `FLAG_TILE_19_HR`,
					     `FLAG_TILE_19_DY`,
					     `FLAG_TILE_16_HR`,
					     `FLAG_TILE_16_DY`,
					     `FLAG_TILE_13_HR`,
					     `FLAG_TILE_13_DY`,
					     `FLAG_TILE_19_WK`,
					     `FLAG_TILE_19_MN`,
					     `FLAG_TILE_16_WK`,
					     `FLAG_TILE_16_MN`,
					     `FLAG_TILE_13_WK`,
					     `FLAG_TILE_13_MN`,
					     `FLAG_REG_3_HR`,
					     `FLAG_REG_3_DY`,
					     `FLAG_REG_2_HR`,
					     `FLAG_REG_2_DY`,
					     `FLAG_REG_1_HR`,
					     `FLAG_REG_1_DY`,
					     `FLAG_REG_3_WK`,
					     `FLAG_REG_3_MN`,
					     `FLAG_REG_2_WK`,
					     `FLAG_REG_2_MN`,
					     `FLAG_REG_1_WK`,
					     `FLAG_REG_1_MN`,
					     `FLAG_CELL_PM_HR`,
					     `FLAG_CELL_PM_DY`,
					     `FLAG_CELL_PM_WK`,
					     `FLAG_CELL_PM_MN`,
					     `FLAG_CELL_AGG_HR`,
					     `FLAG_CELL_AGG_DY`,
					     `FLAG_CELL_AGG_WK`,
					     `FLAG_CELL_AGG_MN`,
					  `FLAG_ROMER_TILE_19_HR`,
					  `FLAG_ROMER_TILE_19_DY`,
					  `FLAG_ROMER_TILE_16_HR`,
					  `FLAG_ROMER_TILE_16_DY`,
					  `FLAG_ROMER_TILE_13_HR`,
					  `FLAG_ROMER_TILE_13_DY`,
					  `FLAG_ROMER_TILE_19_WK`,
					  `FLAG_ROMER_TILE_19_MN`,
					  `FLAG_ROMER_TILE_16_WK`,
					  `FLAG_ROMER_TILE_16_MN`,
					  `FLAG_ROMER_TILE_13_WK`,
					  `FLAG_ROMER_TILE_13_MN`,
					  `FLAG_ROMER_REG_3_HR`,
					  `FLAG_ROMER_REG_3_DY`,
					  `FLAG_ROMER_REG_2_HR`,
					  `FLAG_ROMER_REG_2_DY`,
					  `FLAG_ROMER_REG_1_HR`,
					  `FLAG_ROMER_REG_1_DY`,
					  `FLAG_ROMER_REG_3_WK`,
					  `FLAG_ROMER_REG_3_MN`,
					  `FLAG_ROMER_REG_2_WK`,
					  `FLAG_ROMER_REG_2_MN`,
					  `FLAG_ROMER_REG_1_WK`,
					  `FLAG_ROMER_REG_1_MN`,
					  `FLAG_FAILCAUSE_HR`,
					  `FLAG_SUBSCRIBER_DY`) 
					     SELECT `DATA_DATE`,
						     `DATA_HOUR`,
						     `PU_ID`,
						     `TECH_MASK`,
						     ''',NOW(),''' AS `CREATE_TIME`,
						     `FLAG_IMSI_HR`,
						     `FLAG_IMSI_DY`,
						     `FLAG_MAKE_HR`,
						     `FLAG_MAKE_DY`,
						     `FLAG_IMSI_WK`,
						     `FLAG_IMSI_MN`,
						     `FLAG_MAKE_WK`,
						     `FLAG_MAKE_MN`,
						     `FLAG_TILE_19_HR`,
						     `FLAG_TILE_19_DY`,
						     `FLAG_TILE_16_HR`,
						     `FLAG_TILE_16_DY`,
						     `FLAG_TILE_13_HR`,
						     `FLAG_TILE_13_DY`,
						     `FLAG_TILE_19_WK`,
						     `FLAG_TILE_19_MN`,
						     `FLAG_TILE_16_WK`,
						     `FLAG_TILE_16_MN`,
						     `FLAG_TILE_13_WK`,
						     `FLAG_TILE_13_MN`,
						     `FLAG_REG_3_HR`,
						     `FLAG_REG_3_DY`,
						     `FLAG_REG_2_HR`,
						     `FLAG_REG_2_DY`,
						     `FLAG_REG_1_HR`,
						     `FLAG_REG_1_DY`,
						     `FLAG_REG_3_WK`,
						     `FLAG_REG_3_MN`,
						     `FLAG_REG_2_WK`,
						     `FLAG_REG_2_MN`,
						     `FLAG_REG_1_WK`,
						     `FLAG_REG_1_MN`,
						     `FLAG_CELL_PM_HR`,
						     `FLAG_CELL_PM_DY`,
						     `FLAG_CELL_PM_WK`,
						     `FLAG_CELL_PM_MN`,
						     `FLAG_CELL_AGG_HR`,
						     `FLAG_CELL_AGG_DY`,
						     `FLAG_CELL_AGG_WK`,
						     `FLAG_CELL_AGG_MN`,
						  `FLAG_ROMER_TILE_19_HR`,
						  `FLAG_ROMER_TILE_19_DY`,
						  `FLAG_ROMER_TILE_16_HR`,
						  `FLAG_ROMER_TILE_16_DY`,
						  `FLAG_ROMER_TILE_13_HR`,
						  `FLAG_ROMER_TILE_13_DY`,
						  `FLAG_ROMER_TILE_19_WK`,
						  `FLAG_ROMER_TILE_19_MN`,
						  `FLAG_ROMER_TILE_16_WK`,
						  `FLAG_ROMER_TILE_16_MN`,
						  `FLAG_ROMER_TILE_13_WK`,
						  `FLAG_ROMER_TILE_13_MN`,
						  `FLAG_ROMER_REG_3_HR`,
						  `FLAG_ROMER_REG_3_DY`,
						  `FLAG_ROMER_REG_2_HR`,
						  `FLAG_ROMER_REG_2_DY`,
						  `FLAG_ROMER_REG_1_HR`,
						  `FLAG_ROMER_REG_1_DY`,
						  `FLAG_ROMER_REG_3_WK`,
						  `FLAG_ROMER_REG_3_MN`,
						  `FLAG_ROMER_REG_2_WK`,
						  `FLAG_ROMER_REG_2_MN`,
						  `FLAG_ROMER_REG_1_WK`,
						  `FLAG_ROMER_REG_1_MN`,
						  `FLAG_FAILCAUSE_HR`,
						  `FLAG_SUBSCRIBER_DY`
					     FROM gt_global_statistic.tmp_table_call_cnt_',WORKER_ID,'
					     WHERE IsSuccess=1 ;');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
		END IF;
	ELSE
		SELECT 'No New Data.' AS IsSuccess;
	END IF;	
	
	IF TRENDING_FLAG =1 AND FLAG IN (2,3,4) THEN 
	INSERT INTO gt_gw_main.SP_LOG VALUES('gt_global_statistic','SP_Generate_Global_Statistic_ONE','START INSERT TRENDING DATA', START_TIME);
	
		SET @global_db='gt_global_statistic';
		SET @SqlCmd=CONCAT('SELECT GROUP_CONCAT(DISTINCT CONCAT(`DATA_DATE`,'','',`TECH_MASK`) SEPARATOR ''|'' ) INTO @PU_STR
					FROM ',@global_db,'.tmp_table_call_cnt_ap_',WORKER_ID,';');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;	
		SET @v_m=1;
		SET @v_R_Max=gt_covmo_csv_count(@PU_STR,'|');
	
		WHILE @v_m <= @v_R_Max DO
		BEGIN 
	
		SET v_DATA_DATE_d:=gt_covmo_csv_get(gt_strtok(@PU_STR, @v_m, '|'),1);
		SET v_TECH_MASK_d:=gt_covmo_csv_get(gt_strtok(@PU_STR, @v_m, '|'),2);
			IF (RPT_TYPE & 1)>0 THEN 
				IF v_TECH_MASK_d IN (0,2) THEN 
					CALL gt_global_statistic.SP_Generate_Global_Statistic_TREND('2',FLAG,v_DATA_DATE_d,'TILE');
					INSERT INTO gt_gw_main.SP_LOG VALUES('gt_global_statistic','SP_Generate_Global_Statistic_ONE',CONCAT('INSERT TRENDING DATA TILE UMTS',(CASE WHEN FLAG=2 THEN ' DAILY' WHEN FLAG=3 THEN ' WEEKLY' WHEN FLAG=4 THEN ' MONTHLY' ELSE '' END)), START_TIME);
				END IF;
				IF v_TECH_MASK_d IN (0,4) THEN 
					CALL gt_global_statistic.SP_Generate_Global_Statistic_TREND('4',FLAG,v_DATA_DATE_d,'TILE');
					INSERT INTO gt_gw_main.SP_LOG VALUES('gt_global_statistic','SP_Generate_Global_Statistic_ONE',CONCAT('INSERT TRENDING DATA TILE LTE',(CASE WHEN FLAG=2 THEN ' DAILY' WHEN FLAG=3 THEN ' WEEKLY' WHEN FLAG=4 THEN ' MONTHLY' ELSE '' END)), START_TIME);
				END IF;
				IF v_TECH_MASK_d IN (0,1) THEN 
					CALL gt_global_statistic.SP_Generate_Global_Statistic_TREND('1',FLAG,v_DATA_DATE_d,'TILE');
					INSERT INTO gt_gw_main.SP_LOG VALUES('gt_global_statistic','SP_Generate_Global_Statistic_ONE',CONCAT('INSERT TRENDING DATA TILE GSM',(CASE WHEN FLAG=2 THEN ' DAILY' WHEN FLAG=3 THEN ' WEEKLY' WHEN FLAG=4 THEN ' MONTHLY' ELSE '' END)), START_TIME);
				END IF;
			END IF;
				
			IF (RPT_TYPE & 4)>0 THEN 
				IF v_TECH_MASK_d IN (0,2) THEN 
					CALL gt_global_statistic.SP_Generate_Global_Statistic_TREND('2',FLAG,v_DATA_DATE_d,'CELL');
					INSERT INTO gt_gw_main.SP_LOG VALUES('gt_global_statistic','SP_Generate_Global_Statistic_ONE',CONCAT('INSERT TRENDING DATA CELL UMTS',(CASE WHEN FLAG=2 THEN ' DAILY' WHEN FLAG=3 THEN ' WEEKLY' WHEN FLAG=4 THEN ' MONTHLY' ELSE '' END)), START_TIME);
				END IF;
				IF v_TECH_MASK_d IN (0,4) THEN 
					CALL gt_global_statistic.SP_Generate_Global_Statistic_TREND('4',FLAG,v_DATA_DATE_d,'CELL');
					INSERT INTO gt_gw_main.SP_LOG VALUES('gt_global_statistic','SP_Generate_Global_Statistic_ONE',CONCAT('INSERT TRENDING DATA CELL LTE',(CASE WHEN FLAG=2 THEN ' DAILY' WHEN FLAG=3 THEN ' WEEKLY' WHEN FLAG=4 THEN ' MONTHLY' ELSE '' END)), START_TIME);
				END IF;
				IF v_TECH_MASK_d IN (0,1) THEN 
					CALL gt_global_statistic.SP_Generate_Global_Statistic_TREND('1',FLAG,v_DATA_DATE_d,'CELL');
					INSERT INTO gt_gw_main.SP_LOG VALUES('gt_global_statistic','SP_Generate_Global_Statistic_ONE',CONCAT('INSERT TRENDING DATA CELL GSM',(CASE WHEN FLAG=2 THEN ' DAILY' WHEN FLAG=3 THEN ' WEEKLY' WHEN FLAG=4 THEN ' MONTHLY' ELSE '' END)), START_TIME);
				END IF;
			END IF;
	
		SET @v_m=@v_m+1;
	
	
		END;
		END WHILE;
		
		IF  TRENDING_FLAG =1 AND FLAG = 2 THEN
	
		SELECT 'INSERT Daily data' AS IsSuccess;
	
		ELSEIF  TRENDING_FLAG =1 AND FLAG = 3 THEN
	
		SELECT 'INSERT Weekly data' AS IsSuccess;
	
		ELSEIF TRENDING_FLAG =1 AND  FLAG = 4 THEN
	
		SELECT 'INSERT Monthly data' AS IsSuccess;
	END IF;	
	
	END IF;
		 
	SET @SqlCmd=CONCAT('DELETE FROM gt_global_statistic.table_running_task 
				WHERE `WORKER_ID`=',WORKER_ID,';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS gt_global_statistic.tmp_table_call_cnt_ap_',WORKER_ID,' ;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('DROP TABLE IF EXISTS gt_global_statistic.tmp_table_call_cnt_',WORKER_ID,';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('FLUSH TABLES;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	INSERT INTO gt_gw_main.SP_LOG VALUES('gt_global_statistic','SP_Generate_Global_Statistic_ONE',CONCAT(WORKER_ID,' Done cost:',TIMESTAMPDIFF(SECOND,START_TIME,SYSDATE()),' sec.'), NOW());
