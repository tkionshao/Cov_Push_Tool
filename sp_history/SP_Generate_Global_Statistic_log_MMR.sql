DELIMITER $$
USE `gt_global_statistic`$$
DROP PROCEDURE IF EXISTS `SP_CreateDB_LTE`$$
CREATE DEFINER=`covmo`@`%` PROCEDURE `SP_Generate_Global_Statistic_log_MMR`(IN TECH_MASK TINYINT(2),IN MAIN_WORKER_ID VARCHAR(10))
BEGIN	
	DECLARE START_TIME DATETIME DEFAULT SYSDATE();
	DECLARE v_DATA_DATE_d VARCHAR(10) DEFAULT NULL;
	DECLARE v_group_db_name VARCHAR(100) DEFAULT '';
	
	SET @global_db='gt_global_statistic';
	set @v_reg_m=1;	
	SET @SqlCmd=CONCAT('SELECT DISTINCT `group_id` INTO @REG_GROUP FROM ',@global_db,'.`usr_polygon_reg_3` ORDER BY `group_id` LIMIT 1;');
  	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET v_group_db_name=CONCAT('gt_global_statistic_g',@v_reg_m);
	
	SET @SqlCmd=CONCAT('SELECT GROUP_CONCAT(DISTINCT `DATA_DATE` SEPARATOR '',|'' ) INTO @DIS_DATE
				FROM ',@global_db,'.tmp_table_call_cnt_',MAIN_WORKER_ID,' a;');
	
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
        	
	SET @v_i=1;
	SET @v_R_Max=gt_covmo_csv_count(@DIS_DATE,'|');
	WHILE @v_i <= @v_R_Max DO
	BEGIN
		SET v_DATA_DATE_d:=gt_covmo_csv_get(gt_strtok(@DIS_DATE, @v_i, '|'),1);
		SELECT `DAY_OF_WEEK`(v_DATA_DATE_d) INTO @DAY_OF_WEEK;
		SET @FIRST_DAY=gt_strtok(@DAY_OF_WEEK, 1, '|');
		SET @END_DAY=gt_strtok(@DAY_OF_WEEK, 2, '|');
		SET @DATE_WK=CONCAT(DATE_FORMAT(@FIRST_DAY,'%Y%m%d'),'_',DATE_FORMAT(@END_DAY,'%Y%m%d'));
		SET @DATE_MN=DATE_FORMAT(v_DATA_DATE_d,'%Y%m');
		SET @DATE_DY=DATE_FORMAT(v_DATA_DATE_d,'%Y%m%d');
		SET @FIRST_DAY_MN=DATE_FORMAT(CONCAT(@DATE_MN,'01'),'%Y-%m-%d');
		IF TECH_MASK IN (0,2) THEN 
			SET @SqlCmd=CONCAT('SELECT COUNT(*) INTO @EXISTS_UMTS_DY FROM ',@global_db,'.`nation_wide_session_information`
						WHERE `DATA_DATE`=''',v_DATA_DATE_d,''' AND `TECH_MASK`=2 AND `SESSION_TYPE`=''DAY'';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
			
			IF @EXISTS_UMTS_DY=0 THEN 
				
				SET @SqlCmd=CONCAT('SELECT TABLE_ROWS INTO @V_ROW_CNT_UMTS_DY1 FROM information_schema.`TABLES`
					WHERE TABLE_SCHEMA=''',v_group_db_name,''' 
					AND TABLE_NAME=''table_cell_tile_19_umts_hr_',@DATE_DY,''';');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				
				SET @SqlCmd=CONCAT('SELECT TABLE_ROWS INTO @V_ROW_CNT_UMTS_DY2 FROM information_schema.`TABLES`
					WHERE TABLE_SCHEMA=''',v_group_db_name,'''
					AND TABLE_NAME=''table_cell_tile_19_umts_dy_',@DATE_DY,''';');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;	
			
				SET @SqlCmd=CONCAT('SELECT TABLE_ROWS INTO @V_ROW_CNT_UMTS_DY3 FROM information_schema.`TABLES`
					WHERE TABLE_SCHEMA=''',v_group_db_name,''' 
					AND TABLE_NAME=''table_cell_tile_16_umts_hr_',@DATE_DY,''';');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				
				SET @SqlCmd=CONCAT('SELECT TABLE_ROWS INTO @V_ROW_CNT_UMTS_DY4 FROM information_schema.`TABLES`
					WHERE TABLE_SCHEMA=''',v_group_db_name,''' 
					AND TABLE_NAME=''table_cell_tile_16_umts_dy_',@DATE_DY,''';');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				
				SET @SqlCmd=CONCAT('SELECT TABLE_ROWS INTO @V_ROW_CNT_UMTS_DY5 FROM information_schema.`TABLES`
					WHERE TABLE_SCHEMA=''',v_group_db_name,''' 
					AND TABLE_NAME=''table_cell_tile_13_umts_hr_',@DATE_DY,''';');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				
				SET @SqlCmd=CONCAT('SELECT TABLE_ROWS INTO @V_ROW_CNT_UMTS_DY6 FROM information_schema.`TABLES`
					WHERE TABLE_SCHEMA=''',v_group_db_name,''' 
					AND TABLE_NAME=''table_cell_tile_13_umts_dy_',@DATE_DY,''';');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				
				SET @SqlCmd=CONCAT('SELECT TABLE_ROWS INTO @V_ROW_CNT_UMTS_DY7 FROM information_schema.`TABLES`
					WHERE TABLE_SCHEMA=''',v_group_db_name,''' 
					AND TABLE_NAME=''table_handset_umts_hr_',@DATE_DY,''';');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				
				SET @SqlCmd=CONCAT('SELECT TABLE_ROWS INTO @V_ROW_CNT_UMTS_DY8 FROM information_schema.`TABLES`
					WHERE TABLE_SCHEMA=''',v_group_db_name,''' 
					AND TABLE_NAME=''table_handset_umts_dy_',@DATE_DY,''';');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				
				SET @SqlCmd=CONCAT('SELECT TABLE_ROWS INTO @V_ROW_CNT_UMTS_DY9 FROM information_schema.`TABLES`
					WHERE TABLE_SCHEMA=''',v_group_db_name,''' 
					AND TABLE_NAME=''table_imsi_umts_hr_',@DATE_DY,''';');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				
				SET @SqlCmd=CONCAT('SELECT TABLE_ROWS INTO @V_ROW_CNT_UMTS_DY10 FROM information_schema.`TABLES`
					WHERE TABLE_SCHEMA=''',v_group_db_name,''' 
					AND TABLE_NAME=''table_imsi_umts_dy_',@DATE_DY,''';');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				
				SET @SqlCmd=CONCAT('SELECT TABLE_ROWS INTO @V_ROW_CNT_UMTS_DY11 FROM information_schema.`TABLES`
					WHERE TABLE_SCHEMA=''',v_group_db_name,''' 
					AND TABLE_NAME=''table_tile_19_umts_hr_',@DATE_DY,''';');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				
				SET @SqlCmd=CONCAT('SELECT TABLE_ROWS INTO @V_ROW_CNT_UMTS_DY12 FROM information_schema.`TABLES`
					WHERE TABLE_SCHEMA=''',v_group_db_name,''' 
					AND TABLE_NAME=''table_tile_19_umts_dy_',@DATE_DY,''';');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;	
		
				SET @SqlCmd=CONCAT('SELECT TABLE_ROWS INTO @V_ROW_CNT_UMTS_DY13 FROM information_schema.`TABLES`
					WHERE TABLE_SCHEMA=''',v_group_db_name,''' 
					AND TABLE_NAME=''table_tile_16_umts_hr_',@DATE_DY,''';');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				
				SET @SqlCmd=CONCAT('SELECT TABLE_ROWS INTO @V_ROW_CNT_UMTS_DY14 FROM information_schema.`TABLES`
					WHERE TABLE_SCHEMA=''',v_group_db_name,''' 
					AND TABLE_NAME=''table_tile_16_umts_dy_',@DATE_DY,''';');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
					
				SET @SqlCmd=CONCAT('SELECT TABLE_ROWS INTO @V_ROW_CNT_UMTS_DY15 FROM information_schema.`TABLES`
					WHERE TABLE_SCHEMA=''',v_group_db_name,''' 
					AND TABLE_NAME=''table_tile_13_umts_hr_',@DATE_DY,''';');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				
				SET @SqlCmd=CONCAT('SELECT TABLE_ROWS INTO @V_ROW_CNT_UMTS_DY16 FROM information_schema.`TABLES`
					WHERE TABLE_SCHEMA=''',v_group_db_name,''' 
					AND TABLE_NAME=''table_tile_13_umts_dy_',@DATE_DY,''';');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
							
				SET @V_ROW_CNT_UMTS_DY=IFNULL(@V_ROW_CNT_UMTS_DY1,0)+IFNULL(@V_ROW_CNT_UMTS_DY2,0)+IFNULL(@V_ROW_CNT_UMTS_DY3,0)+IFNULL(@V_ROW_CNT_UMTS_DY4,0)
						+IFNULL(@V_ROW_CNT_UMTS_DY5,0)+IFNULL(@V_ROW_CNT_UMTS_DY6,0)+IFNULL(@V_ROW_CNT_UMTS_DY7,0)+IFNULL(@V_ROW_CNT_UMTS_DY8,0)
						+IFNULL(@V_ROW_CNT_UMTS_DY9,0)+IFNULL(@V_ROW_CNT_UMTS_DY10,0)+IFNULL(@V_ROW_CNT_UMTS_DY11,0)+IFNULL(@V_ROW_CNT_UMTS_DY12,0)
						+IFNULL(@V_ROW_CNT_UMTS_DY13,0)+IFNULL(@V_ROW_CNT_UMTS_DY14,0)+IFNULL(@V_ROW_CNT_UMTS_DY15,0)+IFNULL(@V_ROW_CNT_UMTS_DY16,0);
						
				IF @V_ROW_CNT_UMTS_DY>0 THEN
					SET @SqlCmd=CONCAT('INSERT IGNORE INTO ',@global_db,'.`nation_wide_session_information`
							    (`DATA_DATE`,
							     `END_DATA_DATE`,
							     `TECH_MASK`,
							     `SESSION_TYPE`,
							     `SESSION_URI`)
							VALUES (''',v_DATA_DATE_d,''',
								''',v_DATA_DATE_d,''',
								2,
								''DAY'',
								NULL);');
					
					PREPARE Stmt FROM @SqlCmd;
					EXECUTE Stmt;
					DEALLOCATE PREPARE Stmt;		
				END IF;			
			END IF;
		
			SET @SqlCmd=CONCAT('SELECT COUNT(*) INTO @EXISTS_UMTS_WK FROM ',@global_db,'.`nation_wide_session_information`
						WHERE `DATA_DATE`=''',@FIRST_DAY,''' AND `TECH_MASK`=2 AND `SESSION_TYPE`=''WEEK'';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
			
			IF @EXISTS_UMTS_WK=0 THEN 
				SET @SqlCmd=CONCAT('SELECT TABLE_ROWS INTO @V_ROW_CNT_UMTS_WK1 FROM information_schema.`TABLES`
					WHERE TABLE_SCHEMA=''',v_group_db_name,''' 
					AND TABLE_NAME=''table_cell_tile_19_umts_wk_',@DATE_WK,''';');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				
				SET @SqlCmd=CONCAT('SELECT TABLE_ROWS INTO @V_ROW_CNT_UMTS_WK2 FROM information_schema.`TABLES`
					WHERE TABLE_SCHEMA=''',v_group_db_name,''' 
					AND TABLE_NAME=''table_cell_tile_16_umts_wk_',@DATE_WK,''';');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				
				SET @SqlCmd=CONCAT('SELECT TABLE_ROWS INTO @V_ROW_CNT_UMTS_WK3 FROM information_schema.`TABLES`
					WHERE TABLE_SCHEMA=''',v_group_db_name,''' 
					AND TABLE_NAME=''table_cell_tile_13_umts_wk_',@DATE_WK,''';');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				
				SET @SqlCmd=CONCAT('SELECT TABLE_ROWS INTO @V_ROW_CNT_UMTS_WK4 FROM information_schema.`TABLES`
					WHERE TABLE_SCHEMA=''',v_group_db_name,''' 
					AND TABLE_NAME=''table_handset_umts_wk_',@DATE_WK,''';');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				
				SET @SqlCmd=CONCAT('SELECT TABLE_ROWS INTO @V_ROW_CNT_UMTS_WK5 FROM information_schema.`TABLES`
					WHERE TABLE_SCHEMA=''',v_group_db_name,''' 
					AND TABLE_NAME=''table_imsi_umts_wk_',@DATE_WK,''';');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				
				SET @SqlCmd=CONCAT('SELECT TABLE_ROWS INTO @V_ROW_CNT_UMTS_WK6 FROM information_schema.`TABLES`
					WHERE TABLE_SCHEMA=''',v_group_db_name,''' 
					AND TABLE_NAME=''table_tile_19_umts_wk_',@DATE_WK,''';');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				
				SET @SqlCmd=CONCAT('SELECT TABLE_ROWS INTO @V_ROW_CNT_UMTS_WK7 FROM information_schema.`TABLES`
					WHERE TABLE_SCHEMA=''',v_group_db_name,''' 
					AND TABLE_NAME=''table_tile_16_umts_wk_',@DATE_WK,''';');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				
				SET @SqlCmd=CONCAT('SELECT TABLE_ROWS INTO @V_ROW_CNT_UMTS_WK8 FROM information_schema.`TABLES`
					WHERE TABLE_SCHEMA=''',v_group_db_name,''' 
					AND TABLE_NAME=''table_tile_13_umts_wk_',@DATE_WK,''';');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
							
				SET @V_ROW_CNT_UMTS_WK=IFNULL(@V_ROW_CNT_UMTS_WK1,0)+IFNULL(@V_ROW_CNT_UMTS_WK2,0)+IFNULL(@V_ROW_CNT_UMTS_WK3,0)+IFNULL(@V_ROW_CNT_UMTS_WK4,0)
						+IFNULL(@V_ROW_CNT_UMTS_WK5,0)+IFNULL(@V_ROW_CNT_UMTS_WK6,0)+IFNULL(@V_ROW_CNT_UMTS_WK7,0)+IFNULL(@V_ROW_CNT_UMTS_WK8,0);
						
				IF @V_ROW_CNT_UMTS_WK>0 THEN
					SET @SqlCmd=CONCAT('INSERT IGNORE INTO ',@global_db,'.`nation_wide_session_information`
							    (`DATA_DATE`,
							     `END_DATA_DATE`,
							     `TECH_MASK`,
							     `SESSION_TYPE`,
							     `SESSION_URI`)
							VALUES (''',@FIRST_DAY,''',
								''',@END_DAY,''',
								2,
								''WEEK'',
								NULL);');
					PREPARE Stmt FROM @SqlCmd;
					EXECUTE Stmt;
					DEALLOCATE PREPARE Stmt;
				END IF;	
			END IF;		
			
			SET @SqlCmd=CONCAT('SELECT COUNT(*) INTO @EXISTS_UMTS_MN FROM ',@global_db,'.`nation_wide_session_information`
						WHERE `DATA_DATE`=''',@FIRST_DAY_MN,''' AND `TECH_MASK`=2 AND `SESSION_TYPE`=''MONTH'';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
			
			IF @EXISTS_UMTS_MN=0 THEN 
				
				SET @SqlCmd=CONCAT('SELECT TABLE_ROWS INTO @V_ROW_CNT_UMTS_MN1 FROM information_schema.`TABLES`
					WHERE TABLE_SCHEMA=''',v_group_db_name,''' 
					AND TABLE_NAME=''table_cell_tile_19_umts_mn_',@DATE_MN,''';');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;	
				
				SET @SqlCmd=CONCAT('SELECT TABLE_ROWS INTO @V_ROW_CNT_UMTS_MN2 FROM information_schema.`TABLES`
					WHERE TABLE_SCHEMA=''',v_group_db_name,''' 
					AND TABLE_NAME=''table_cell_tile_16_umts_mn_',@DATE_MN,''';');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				
				SET @SqlCmd=CONCAT('SELECT TABLE_ROWS INTO @V_ROW_CNT_UMTS_MN3 FROM information_schema.`TABLES`
					WHERE TABLE_SCHEMA=''',v_group_db_name,''' 
					AND TABLE_NAME=''table_cell_tile_13_umts_mn_',@DATE_MN,''';');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				
				SET @SqlCmd=CONCAT('SELECT TABLE_ROWS INTO @V_ROW_CNT_UMTS_MN4 FROM information_schema.`TABLES`
					WHERE TABLE_SCHEMA=''',v_group_db_name,''' 
					AND TABLE_NAME=''table_handset_umts_mn_',@DATE_MN,''';');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				
				SET @SqlCmd=CONCAT('SELECT TABLE_ROWS INTO @V_ROW_CNT_UMTS_MN5 FROM information_schema.`TABLES`
					WHERE TABLE_SCHEMA=''',v_group_db_name,''' 
					AND TABLE_NAME=''table_imsi_umts_mn_',@DATE_MN,''';');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				
				SET @SqlCmd=CONCAT('SELECT TABLE_ROWS INTO @V_ROW_CNT_UMTS_MN6 FROM information_schema.`TABLES`
					WHERE TABLE_SCHEMA=''',v_group_db_name,''' 
					AND TABLE_NAME=''table_tile_19_umts_mn_',@DATE_MN,''';');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				
				SET @SqlCmd=CONCAT('SELECT TABLE_ROWS INTO @V_ROW_CNT_UMTS_MN7 FROM information_schema.`TABLES`
					WHERE TABLE_SCHEMA=''',v_group_db_name,''' 
					AND TABLE_NAME=''table_tile_16_umts_mn_',@DATE_MN,''';');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				
				SET @SqlCmd=CONCAT('SELECT TABLE_ROWS INTO @V_ROW_CNT_UMTS_MN8 FROM information_schema.`TABLES`
					WHERE TABLE_SCHEMA=''',v_group_db_name,''' 
					AND TABLE_NAME=''table_tile_13_umts_mn_',@DATE_MN,''';');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
							
				SET @V_ROW_CNT_UMTS_MN=IFNULL(@V_ROW_CNT_UMTS_MN1,0)+IFNULL(@V_ROW_CNT_UMTS_MN2,0)+IFNULL(@V_ROW_CNT_UMTS_MN3,0)+IFNULL(@V_ROW_CNT_UMTS_MN4,0)
						+IFNULL(@V_ROW_CNT_UMTS_MN5,0)+IFNULL(@V_ROW_CNT_UMTS_MN6,0)+IFNULL(@V_ROW_CNT_UMTS_MN7,0)+IFNULL(@V_ROW_CNT_UMTS_MN8,0);
						
				IF @V_ROW_CNT_UMTS_MN>0 THEN
					SET @SqlCmd=CONCAT('INSERT IGNORE INTO ',@global_db,'.`nation_wide_session_information`
						    (`DATA_DATE`,
						     `END_DATA_DATE`,
						     `TECH_MASK`,
						     `SESSION_TYPE`,
						     `SESSION_URI`)
					VALUES (''',DATE_SUB(v_DATA_DATE_d,INTERVAL DAY(v_DATA_DATE_d)-1 DAY),''',
						''',LAST_DAY(v_DATA_DATE_d),''',
						2,
						''MONTH'',
						NULL);');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;		
				END IF;	
			END IF;
		END IF;
		IF TECH_MASK IN (0,1) THEN 
			SET @SqlCmd=CONCAT('SELECT COUNT(*) INTO @EXISTS_gsm_DY FROM ',@global_db,'.`nation_wide_session_information`
						WHERE `DATA_DATE`=''',v_DATA_DATE_d,''' AND `TECH_MASK`=1 AND `SESSION_TYPE`=''DAY'';');
			
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
			IF @EXISTS_gsm_DY=0 THEN 
				
				SET @SqlCmd=CONCAT('SELECT TABLE_ROWS INTO @V_ROW_CNT_GSM_DY1 FROM information_schema.`TABLES`
					WHERE TABLE_SCHEMA=''',v_group_db_name,''' 
					AND TABLE_NAME=''table_cell_tile_19_gsm_hr_',@DATE_DY,''';');
 				 
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				
				SET @SqlCmd=CONCAT('SELECT TABLE_ROWS INTO @V_ROW_CNT_GSM_DY2 FROM information_schema.`TABLES`
					WHERE TABLE_SCHEMA=''',v_group_db_name,''' 
					AND TABLE_NAME=''table_cell_tile_19_gsm_dy_',@DATE_DY,''';');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
		
				SET @SqlCmd=CONCAT('SELECT TABLE_ROWS INTO @V_ROW_CNT_GSM_DY3 FROM information_schema.`TABLES`
					WHERE TABLE_SCHEMA=''',v_group_db_name,''' 
					AND TABLE_NAME=''table_cell_tile_16_gsm_hr_',@DATE_DY,''';');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				
				SET @SqlCmd=CONCAT('SELECT TABLE_ROWS INTO @V_ROW_CNT_GSM_DY4 FROM information_schema.`TABLES`
					WHERE TABLE_SCHEMA=''',v_group_db_name,''' 
					AND TABLE_NAME=''table_cell_tile_16_gsm_dy_',@DATE_DY,''';');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				
				SET @SqlCmd=CONCAT('SELECT TABLE_ROWS INTO @V_ROW_CNT_GSM_DY5 FROM information_schema.`TABLES`
					WHERE TABLE_SCHEMA=''',v_group_db_name,''' 
					AND TABLE_NAME=''table_cell_tile_13_gsm_hr_',@DATE_DY,''';');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				
				SET @SqlCmd=CONCAT('SELECT TABLE_ROWS INTO @V_ROW_CNT_GSM_DY6 FROM information_schema.`TABLES`
					WHERE TABLE_SCHEMA=''',v_group_db_name,''' 
					AND TABLE_NAME=''table_cell_tile_13_gsm_dy_',@DATE_DY,''';');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				
				SET @SqlCmd=CONCAT('SELECT TABLE_ROWS INTO @V_ROW_CNT_GSM_DY7 FROM information_schema.`TABLES`
					WHERE TABLE_SCHEMA=''',v_group_db_name,''' 
					AND TABLE_NAME=''table_handset_gsm_hr_',@DATE_DY,''';');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				
				SET @SqlCmd=CONCAT('SELECT TABLE_ROWS INTO @V_ROW_CNT_GSM_DY8 FROM information_schema.`TABLES`
					WHERE TABLE_SCHEMA=''',v_group_db_name,''' 
					AND TABLE_NAME=''table_handset_gsm_dy_',@DATE_DY,''';');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				
				SET @SqlCmd=CONCAT('SELECT TABLE_ROWS INTO @V_ROW_CNT_GSM_DY9 FROM information_schema.`TABLES`
					WHERE TABLE_SCHEMA=''',v_group_db_name,''' 
					AND TABLE_NAME=''table_imsi_gsm_hr_',@DATE_DY,''';');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				
				SET @SqlCmd=CONCAT('SELECT TABLE_ROWS INTO @V_ROW_CNT_GSM_DY10 FROM information_schema.`TABLES`
					WHERE TABLE_SCHEMA=''',v_group_db_name,''' 
					AND TABLE_NAME=''table_imsi_gsm_dy_',@DATE_DY,''';');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				
				SET @SqlCmd=CONCAT('SELECT TABLE_ROWS INTO @V_ROW_CNT_GSM_DY11 FROM information_schema.`TABLES`
					WHERE TABLE_SCHEMA=''',v_group_db_name,''' 
					AND TABLE_NAME=''table_tile_19_gsm_hr_',@DATE_DY,''';');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				
				SET @SqlCmd=CONCAT('SELECT TABLE_ROWS INTO @V_ROW_CNT_GSM_DY12 FROM information_schema.`TABLES`
					WHERE TABLE_SCHEMA=''',v_group_db_name,''' 
					AND TABLE_NAME=''table_tile_19_gsm_dy_',@DATE_DY,''';');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;	
		
				SET @SqlCmd=CONCAT('SELECT TABLE_ROWS INTO @V_ROW_CNT_GSM_DY13 FROM information_schema.`TABLES`
					WHERE TABLE_SCHEMA=''',v_group_db_name,''' 
					AND TABLE_NAME=''table_tile_16_gsm_hr_',@DATE_DY,''';');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				
				SET @SqlCmd=CONCAT('SELECT TABLE_ROWS INTO @V_ROW_CNT_GSM_DY14 FROM information_schema.`TABLES`
					WHERE TABLE_SCHEMA=''',v_group_db_name,''' 
					AND TABLE_NAME=''table_tile_16_gsm_dy_',@DATE_DY,''';');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				
				SET @SqlCmd=CONCAT('SELECT TABLE_ROWS INTO @V_ROW_CNT_GSM_DY15 FROM information_schema.`TABLES`
					WHERE TABLE_SCHEMA=''',v_group_db_name,''' 
					AND TABLE_NAME=''table_tile_13_gsm_hr_',@DATE_DY,''';');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				
				SET @SqlCmd=CONCAT('SELECT TABLE_ROWS INTO @V_ROW_CNT_GSM_DY16 FROM information_schema.`TABLES`
					WHERE TABLE_SCHEMA=''',v_group_db_name,''' 
					AND TABLE_NAME=''table_tile_13_gsm_dy_',@DATE_DY,''';');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
							
				SET @V_ROW_CNT_GSM_DY=IFNULL(@V_ROW_CNT_GSM_DY1,0)+IFNULL(@V_ROW_CNT_GSM_DY2,0)+IFNULL(@V_ROW_CNT_GSM_DY3,0)+IFNULL(@V_ROW_CNT_GSM_DY4,0)
						+IFNULL(@V_ROW_CNT_GSM_DY5,0)+IFNULL(@V_ROW_CNT_GSM_DY6,0)+IFNULL(@V_ROW_CNT_GSM_DY7,0)+IFNULL(@V_ROW_CNT_GSM_DY8,0)
						+IFNULL(@V_ROW_CNT_GSM_DY9,0)+IFNULL(@V_ROW_CNT_GSM_DY10,0)+IFNULL(@V_ROW_CNT_GSM_DY11,0)+IFNULL(@V_ROW_CNT_GSM_DY12,0)
						+IFNULL(@V_ROW_CNT_GSM_DY13,0)+IFNULL(@V_ROW_CNT_GSM_DY14,0)+IFNULL(@V_ROW_CNT_GSM_DY15,0)+IFNULL(@V_ROW_CNT_GSM_DY16,0);
						
				IF @V_ROW_CNT_GSM_DY>0 THEN
					SET @SqlCmd=CONCAT('INSERT IGNORE INTO ',@global_db,'.`nation_wide_session_information`
							    (`DATA_DATE`,
							     `END_DATA_DATE`,
							     `TECH_MASK`,
							     `SESSION_TYPE`,
							     `SESSION_URI`)
							VALUES (''',v_DATA_DATE_d,''',
								''',v_DATA_DATE_d,''',
								1,
								''DAY'',
								NULL);');
					
					PREPARE Stmt FROM @SqlCmd;
					EXECUTE Stmt;
					DEALLOCATE PREPARE Stmt;		
				END IF;			
			END IF;
		
			SET @SqlCmd=CONCAT('SELECT COUNT(*) INTO @EXISTS_gsm_WK FROM ',@global_db,'.`nation_wide_session_information`
						WHERE `DATA_DATE`=''',@FIRST_DAY,''' AND `TECH_MASK`=1 AND `SESSION_TYPE`=''WEEK'';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
			
			IF @EXISTS_gsm_WK=0 THEN 
				SET @SqlCmd=CONCAT('SELECT TABLE_ROWS INTO @V_ROW_CNT_GSM_WK1 FROM information_schema.`TABLES`
					WHERE TABLE_SCHEMA=''',v_group_db_name,''' 
					AND TABLE_NAME=''table_cell_tile_19_gsm_wk_',@DATE_WK,''';');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;	
				
				SET @SqlCmd=CONCAT('SELECT TABLE_ROWS INTO @V_ROW_CNT_GSM_WK2 FROM information_schema.`TABLES`
					WHERE TABLE_SCHEMA=''',v_group_db_name,''' 
					AND TABLE_NAME=''table_cell_tile_16_gsm_wk_',@DATE_WK,''';');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				
				SET @SqlCmd=CONCAT('SELECT TABLE_ROWS INTO @V_ROW_CNT_GSM_WK3 FROM information_schema.`TABLES`
					WHERE TABLE_SCHEMA=''',v_group_db_name,''' 
					AND TABLE_NAME=''table_cell_tile_13_gsm_wk_',@DATE_WK,''';');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				
				SET @SqlCmd=CONCAT('SELECT TABLE_ROWS INTO @V_ROW_CNT_GSM_WK4 FROM information_schema.`TABLES`
					WHERE TABLE_SCHEMA=''',v_group_db_name,''' 
					AND TABLE_NAME=''table_handset_gsm_wk_',@DATE_WK,''';');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				
				SET @SqlCmd=CONCAT('SELECT TABLE_ROWS INTO @V_ROW_CNT_GSM_WK5 FROM information_schema.`TABLES`
					WHERE TABLE_SCHEMA=''',v_group_db_name,''' 
					AND TABLE_NAME=''table_imsi_gsm_wk_',@DATE_WK,''';');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				
				SET @SqlCmd=CONCAT('SELECT TABLE_ROWS INTO @V_ROW_CNT_GSM_WK6 FROM information_schema.`TABLES`
					WHERE TABLE_SCHEMA=''',v_group_db_name,''' 
					AND TABLE_NAME=''table_tile_19_gsm_wk_',@DATE_WK,''';');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				
				SET @SqlCmd=CONCAT('SELECT TABLE_ROWS INTO @V_ROW_CNT_GSM_WK7 FROM information_schema.`TABLES`
					WHERE TABLE_SCHEMA=''',v_group_db_name,''' 
					AND TABLE_NAME=''table_tile_16_gsm_wk_',@DATE_WK,''';');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				
				SET @SqlCmd=CONCAT('SELECT TABLE_ROWS INTO @V_ROW_CNT_GSM_WK8 FROM information_schema.`TABLES`
					WHERE TABLE_SCHEMA=''',v_group_db_name,''' 
					AND TABLE_NAME=''table_tile_13_gsm_wk_',@DATE_WK,''';');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
							
				SET @V_ROW_CNT_GSM_WK=IFNULL(@V_ROW_CNT_GSM_WK1,0)+IFNULL(@V_ROW_CNT_GSM_WK2,0)+IFNULL(@V_ROW_CNT_GSM_WK3,0)+IFNULL(@V_ROW_CNT_GSM_WK4,0)
						+IFNULL(@V_ROW_CNT_GSM_WK5,0)+IFNULL(@V_ROW_CNT_GSM_WK6,0)+IFNULL(@V_ROW_CNT_GSM_WK7,0)+IFNULL(@V_ROW_CNT_GSM_WK8,0);
						
				IF @V_ROW_CNT_GSM_WK>0 THEN
					SET @SqlCmd=CONCAT('INSERT IGNORE INTO ',@global_db,'.`nation_wide_session_information`
							    (`DATA_DATE`,
							     `END_DATA_DATE`,
							     `TECH_MASK`,
							     `SESSION_TYPE`,
							     `SESSION_URI`)
							VALUES (''',@FIRST_DAY,''',
								''',@END_DAY,''',
								1,
								''WEEK'',
								NULL);');
					PREPARE Stmt FROM @SqlCmd;
					EXECUTE Stmt;
					DEALLOCATE PREPARE Stmt;
				END IF;	
			END IF;		
			
			SET @SqlCmd=CONCAT('SELECT COUNT(*) INTO @EXISTS_gsm_MN FROM ',@global_db,'.`nation_wide_session_information`
						WHERE `DATA_DATE`=''',@FIRST_DAY_MN,''' AND `TECH_MASK`=1 AND `SESSION_TYPE`=''MONTH'';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
			
			IF @EXISTS_gsm_MN=0 THEN 
				
				SET @SqlCmd=CONCAT('SELECT TABLE_ROWS INTO @V_ROW_CNT_GSM_MN1 FROM information_schema.`TABLES`
					WHERE TABLE_SCHEMA=''',v_group_db_name,''' 
					AND TABLE_NAME=''table_cell_tile_19_gsm_mn_',@DATE_MN,''';');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;	
				
				SET @SqlCmd=CONCAT('SELECT TABLE_ROWS INTO @V_ROW_CNT_GSM_MN2 FROM information_schema.`TABLES`
					WHERE TABLE_SCHEMA=''',v_group_db_name,''' 
					AND TABLE_NAME=''table_cell_tile_16_gsm_mn_',@DATE_MN,''';');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				
				SET @SqlCmd=CONCAT('SELECT TABLE_ROWS INTO @V_ROW_CNT_GSM_MN3 FROM information_schema.`TABLES`
					WHERE TABLE_SCHEMA=''',v_group_db_name,''' 
					AND TABLE_NAME=''table_cell_tile_13_gsm_mn_',@DATE_MN,''';');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				
				SET @SqlCmd=CONCAT('SELECT TABLE_ROWS INTO @V_ROW_CNT_GSM_MN4 FROM information_schema.`TABLES`
					WHERE TABLE_SCHEMA=''',v_group_db_name,''' 
					AND TABLE_NAME=''table_handset_gsm_mn_',@DATE_MN,''';');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				
				SET @SqlCmd=CONCAT('SELECT TABLE_ROWS INTO @V_ROW_CNT_GSM_MN5 FROM information_schema.`TABLES`
					WHERE TABLE_SCHEMA=''',v_group_db_name,''' 
					AND TABLE_NAME=''table_imsi_gsm_mn_',@DATE_MN,''';');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				
				SET @SqlCmd=CONCAT('SELECT TABLE_ROWS INTO @V_ROW_CNT_GSM_MN6 FROM information_schema.`TABLES`
					WHERE TABLE_SCHEMA=''',v_group_db_name,''' 
					AND TABLE_NAME=''table_tile_19_gsm_mn_',@DATE_MN,''';');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				
				SET @SqlCmd=CONCAT('SELECT TABLE_ROWS INTO @V_ROW_CNT_GSM_MN7 FROM information_schema.`TABLES`
					WHERE TABLE_SCHEMA=''',v_group_db_name,''' 
					AND TABLE_NAME=''table_tile_16_gsm_mn_',@DATE_MN,''';');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				
				SET @SqlCmd=CONCAT('SELECT TABLE_ROWS INTO @V_ROW_CNT_GSM_MN8 FROM information_schema.`TABLES`
					WHERE TABLE_SCHEMA=''',v_group_db_name,''' 
					AND TABLE_NAME=''table_tile_13_gsm_mn_',@DATE_MN,''';');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
							
				SET @V_ROW_CNT_GSM_MN=IFNULL(@V_ROW_CNT_GSM_MN1,0)+IFNULL(@V_ROW_CNT_GSM_MN2,0)+IFNULL(@V_ROW_CNT_GSM_MN3,0)+IFNULL(@V_ROW_CNT_GSM_MN4,0)
						+IFNULL(@V_ROW_CNT_GSM_MN5,0)+IFNULL(@V_ROW_CNT_GSM_MN6,0)+IFNULL(@V_ROW_CNT_GSM_MN7,0)+IFNULL(@V_ROW_CNT_GSM_MN8,0);
						
				IF @V_ROW_CNT_GSM_MN>0 THEN
					SET @SqlCmd=CONCAT('INSERT IGNORE INTO ',@global_db,'.`nation_wide_session_information`
						    (`DATA_DATE`,
						     `END_DATA_DATE`,
						     `TECH_MASK`,
						     `SESSION_TYPE`,
						     `SESSION_URI`)
					VALUES (''',DATE_SUB(v_DATA_DATE_d,INTERVAL DAY(v_DATA_DATE_d)-1 DAY),''',
						''',LAST_DAY(v_DATA_DATE_d),''',
						1,
						''MONTH'',
						NULL);');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;		
				END IF;	
			END IF;
		END IF;
		IF TECH_MASK IN (0,4) THEN  
			SET @SqlCmd=CONCAT('SELECT COUNT(*) INTO @EXISTS_LTE_DY FROM ',@global_db,'.`nation_wide_session_information`
						WHERE `DATA_DATE`=''',v_DATA_DATE_d,''' AND `TECH_MASK`=4 AND `SESSION_TYPE`=''DAY'';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
			
			IF @EXISTS_LTE_DY=0 THEN 
				SET @SqlCmd=CONCAT('SELECT TABLE_ROWS INTO @V_ROW_CNT_LTE_DY21 FROM information_schema.`TABLES`
					WHERE TABLE_SCHEMA=''',v_group_db_name,''' 
					AND TABLE_NAME=''table_cell_tile_19_lte_hr_',@DATE_DY,''';');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				
				SET @SqlCmd=CONCAT('SELECT TABLE_ROWS INTO @V_ROW_CNT_LTE_DY22 FROM information_schema.`TABLES`
					WHERE TABLE_SCHEMA=''',v_group_db_name,''' 
					AND TABLE_NAME=''table_cell_tile_19_lte_dy_',@DATE_DY,''';');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				
				SET @SqlCmd=CONCAT('SELECT TABLE_ROWS INTO @V_ROW_CNT_LTE_DY23 FROM information_schema.`TABLES`
					WHERE TABLE_SCHEMA=''',v_group_db_name,''' 
					AND TABLE_NAME=''table_cell_tile_16_lte_hr_',@DATE_DY,''';');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				
				SET @SqlCmd=CONCAT('SELECT TABLE_ROWS INTO @V_ROW_CNT_LTE_DY24 FROM information_schema.`TABLES`
					WHERE TABLE_SCHEMA=''',v_group_db_name,''' 
					AND TABLE_NAME=''table_cell_tile_16_lte_dy_',@DATE_DY,''';');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				
				SET @SqlCmd=CONCAT('SELECT TABLE_ROWS INTO @V_ROW_CNT_LTE_DY25 FROM information_schema.`TABLES`
					WHERE TABLE_SCHEMA=''',v_group_db_name,''' 
					AND TABLE_NAME=''table_cell_tile_13_lte_hr_',@DATE_DY,''';');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				
				SET @SqlCmd=CONCAT('SELECT TABLE_ROWS INTO @V_ROW_CNT_LTE_DY26 FROM information_schema.`TABLES`
					WHERE TABLE_SCHEMA=''',v_group_db_name,''' 
					AND TABLE_NAME=''table_cell_tile_13_lte_dy_',@DATE_DY,''';');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				
				SET @SqlCmd=CONCAT('SELECT TABLE_ROWS INTO @V_ROW_CNT_LTE_DY27 FROM information_schema.`TABLES`
					WHERE TABLE_SCHEMA=''',v_group_db_name,''' 
					AND TABLE_NAME=''table_tile_19_lte_hr_',@DATE_DY,''';');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				
				SET @SqlCmd=CONCAT('SELECT TABLE_ROWS INTO @V_ROW_CNT_LTE_DY28 FROM information_schema.`TABLES`
					WHERE TABLE_SCHEMA=''',v_group_db_name,''' 
					AND TABLE_NAME=''table_tile_19_lte_dy_',@DATE_DY,''';');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				
				SET @SqlCmd=CONCAT('SELECT TABLE_ROWS INTO @V_ROW_CNT_LTE_DY29 FROM information_schema.`TABLES`
					WHERE TABLE_SCHEMA=''',v_group_db_name,''' 
					AND TABLE_NAME=''table_tile_16_lte_hr_',@DATE_DY,''';');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				
				SET @SqlCmd=CONCAT('SELECT TABLE_ROWS INTO @V_ROW_CNT_LTE_DY30 FROM information_schema.`TABLES`
					WHERE TABLE_SCHEMA=''',v_group_db_name,''' 
					AND TABLE_NAME=''table_tile_16_lte_dy_',@DATE_DY,''';');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				
				SET @SqlCmd=CONCAT('SELECT TABLE_ROWS INTO @V_ROW_CNT_LTE_DY31 FROM information_schema.`TABLES`
					WHERE TABLE_SCHEMA=''',v_group_db_name,''' 
					AND TABLE_NAME=''table_tile_13_lte_hr_',@DATE_DY,''';');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				
				SET @SqlCmd=CONCAT('SELECT TABLE_ROWS INTO @V_ROW_CNT_LTE_DY32 FROM information_schema.`TABLES`
					WHERE TABLE_SCHEMA=''',v_group_db_name,''' 
					AND TABLE_NAME=''table_tile_13_lte_dy_',@DATE_DY,''';');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				
				SET @SqlCmd=CONCAT('SELECT TABLE_ROWS INTO @V_ROW_CNT_LTE_DY33 FROM information_schema.`TABLES`
					WHERE TABLE_SCHEMA=''',v_group_db_name,''' 
					AND TABLE_NAME=''table_imsi_lte_hr_',@DATE_DY,''';');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				
				SET @SqlCmd=CONCAT('SELECT TABLE_ROWS INTO @V_ROW_CNT_LTE_DY34 FROM information_schema.`TABLES`
					WHERE TABLE_SCHEMA=''',v_group_db_name,''' 
					AND TABLE_NAME=''table_imsi_lte_dy_',@DATE_DY,''';');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				
				SET @SqlCmd=CONCAT('SELECT TABLE_ROWS INTO @V_ROW_CNT_LTE_DY35 FROM information_schema.`TABLES`
					WHERE TABLE_SCHEMA=''',v_group_db_name,''' 
					AND TABLE_NAME=''table_handset_lte_hr_',@DATE_DY,''';');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				
				SET @SqlCmd=CONCAT('SELECT TABLE_ROWS INTO @V_ROW_CNT_LTE_DY36 FROM information_schema.`TABLES`
					WHERE TABLE_SCHEMA=''',v_group_db_name,''' 
					AND TABLE_NAME=''table_handset_lte_dy_',@DATE_DY,''';');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
		
				SET @SqlCmd=CONCAT('SELECT TABLE_ROWS INTO @V_ROW_CNT_LTE_DY37 FROM information_schema.`TABLES`
					WHERE TABLE_SCHEMA=''',v_group_db_name,''' 
					AND TABLE_NAME=''table_cell_lte_hr_',@DATE_DY,''';');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
		
				SET @SqlCmd=CONCAT('SELECT TABLE_ROWS INTO @V_ROW_CNT_LTE_DY38 FROM information_schema.`TABLES`
					WHERE TABLE_SCHEMA=''',v_group_db_name,''' 
					AND TABLE_NAME=''table_cell_lte_dy_',@DATE_DY,''';');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				
				SET @V_ROW_CNT_LTE_DY=IFNULL(@V_ROW_CNT_LTE_DY21,0)+IFNULL(@V_ROW_CNT_LTE_DY22,0)+IFNULL(@V_ROW_CNT_LTE_DY23,0)+IFNULL(@V_ROW_CNT_LTE_DY24,0)
						+IFNULL(@V_ROW_CNT_LTE_DY25,0)+IFNULL(@V_ROW_CNT_LTE_DY26,0)+IFNULL(@V_ROW_CNT_LTE_DY27,0)+IFNULL(@V_ROW_CNT_LTE_DY28,0)
						+IFNULL(@V_ROW_CNT_LTE_DY29,0)+IFNULL(@V_ROW_CNT_LTE_DY30,0)+IFNULL(@V_ROW_CNT_LTE_DY31,0)+IFNULL(@V_ROW_CNT_LTE_DY32,0)
						+IFNULL(@V_ROW_CNT_LTE_DY33,0)+IFNULL(@V_ROW_CNT_LTE_DY34,0)+IFNULL(@V_ROW_CNT_LTE_DY35,0)+IFNULL(@V_ROW_CNT_LTE_DY36,0)
						+IFNULL(@V_ROW_CNT_LTE_DY37,0)+IFNULL(@V_ROW_CNT_LTE_DY38,0);
				IF @V_ROW_CNT_LTE_DY>0 THEN
					SET @SqlCmd=CONCAT('INSERT IGNORE INTO ',@global_db,'.`nation_wide_session_information`
						    (`DATA_DATE`,
						     `END_DATA_DATE`,
						     `TECH_MASK`,
						     `SESSION_TYPE`,
						     `SESSION_URI`)
					VALUES (''',v_DATA_DATE_d,''',
						''',v_DATA_DATE_d,''',
						4,
						''DAY'',
						NULL);');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				END IF;
			END IF;
			SET @SqlCmd=CONCAT('SELECT COUNT(*) INTO @EXISTS_LTE_WK FROM ',@global_db,'.`nation_wide_session_information`
						WHERE `DATA_DATE`=''',@FIRST_DAY,''' AND `TECH_MASK`=4 AND `SESSION_TYPE`=''WEEK'';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
			
			IF @EXISTS_LTE_WK=0 THEN 
				SET @SqlCmd=CONCAT('SELECT TABLE_ROWS INTO @V_ROW_CNT_LTE_WK21 FROM information_schema.`TABLES`
					WHERE TABLE_SCHEMA=''',v_group_db_name,''' 
					AND TABLE_NAME=''table_cell_tile_19_lte_wk_',@DATE_WK,''';');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				
				SET @SqlCmd=CONCAT('SELECT TABLE_ROWS INTO @V_ROW_CNT_LTE_WK22 FROM information_schema.`TABLES`
					WHERE TABLE_SCHEMA=''',v_group_db_name,''' 
					AND TABLE_NAME=''table_cell_tile_16_lte_wk_',@DATE_WK,''';');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				
				SET @SqlCmd=CONCAT('SELECT TABLE_ROWS INTO @V_ROW_CNT_LTE_WK23 FROM information_schema.`TABLES`
					WHERE TABLE_SCHEMA=''',v_group_db_name,''' 
					AND TABLE_NAME=''table_cell_tile_13_lte_wk_',@DATE_WK,''';');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				
				SET @SqlCmd=CONCAT('SELECT TABLE_ROWS INTO @V_ROW_CNT_LTE_WK24 FROM information_schema.`TABLES`
					WHERE TABLE_SCHEMA=''',v_group_db_name,''' 
					AND TABLE_NAME=''table_tile_19_lte_wk_',@DATE_WK,''';');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				
				SET @SqlCmd=CONCAT('SELECT TABLE_ROWS INTO @V_ROW_CNT_LTE_WK25 FROM information_schema.`TABLES`
					WHERE TABLE_SCHEMA=''',v_group_db_name,''' 
					AND TABLE_NAME=''table_tile_16_lte_wk_',@DATE_WK,''';');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				
				SET @SqlCmd=CONCAT('SELECT TABLE_ROWS INTO @V_ROW_CNT_LTE_WK26 FROM information_schema.`TABLES`
					WHERE TABLE_SCHEMA=''',v_group_db_name,''' 
					AND TABLE_NAME=''table_tile_13_lte_wk_',@DATE_WK,''';');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				
				SET @SqlCmd=CONCAT('SELECT TABLE_ROWS INTO @V_ROW_CNT_LTE_WK27 FROM information_schema.`TABLES`
					WHERE TABLE_SCHEMA=''',v_group_db_name,''' 
					AND TABLE_NAME=''table_imsi_lte_wk_',@DATE_WK,''';');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				
				SET @SqlCmd=CONCAT('SELECT TABLE_ROWS INTO @V_ROW_CNT_LTE_WK28 FROM information_schema.`TABLES`
					WHERE TABLE_SCHEMA=''',v_group_db_name,''' 
					AND TABLE_NAME=''table_handset_lte_wk_',@DATE_WK,''';');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
		
				SET @SqlCmd=CONCAT('SELECT TABLE_ROWS INTO @V_ROW_CNT_LTE_WK29 FROM information_schema.`TABLES`
					WHERE TABLE_SCHEMA=''',v_group_db_name,''' 
					AND TABLE_NAME=''table_cell_lte_wk_',@DATE_WK,''';');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				
				SET @V_ROW_CNT_LTE_WK=IFNULL(@V_ROW_CNT_LTE_WK21,0)+IFNULL(@V_ROW_CNT_LTE_WK22,0)+IFNULL(@V_ROW_CNT_LTE_WK23,0)+IFNULL(@V_ROW_CNT_LTE_WK24,0)
						+IFNULL(@V_ROW_CNT_LTE_WK25,0)+IFNULL(@V_ROW_CNT_LTE_WK26,0)+IFNULL(@V_ROW_CNT_LTE_WK27,0)+IFNULL(@V_ROW_CNT_LTE_WK28,0)+IFNULL(@V_ROW_CNT_LTE_WK29,0);
				IF @V_ROW_CNT_LTE_WK>0 THEN
					SET @SqlCmd=CONCAT('INSERT IGNORE INTO ',@global_db,'.`nation_wide_session_information`
						    (`DATA_DATE`,
						     `END_DATA_DATE`,
						     `TECH_MASK`,
						     `SESSION_TYPE`,
						     `SESSION_URI`)
					VALUES (''',@FIRST_DAY,''',
						''',@END_DAY,''',
						4,
						''WEEK'',
						NULL);');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;		
				END IF;
			END IF;
			
			SET @SqlCmd=CONCAT('SELECT COUNT(*) INTO @EXISTS_LTE_MN FROM ',@global_db,'.`nation_wide_session_information`
						WHERE `DATA_DATE`=''',@FIRST_DAY_MN,''' AND `TECH_MASK`=4 AND `SESSION_TYPE`=''MONTH'';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
			
			IF @EXISTS_LTE_MN=0 THEN 				
				SET @SqlCmd=CONCAT('SELECT TABLE_ROWS INTO @V_ROW_CNT_LTE_MN21 FROM information_schema.`TABLES`
					WHERE TABLE_SCHEMA=''',v_group_db_name,''' 
					AND TABLE_NAME=''table_cell_tile_19_lte_mn_',@DATE_MN,''';');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				
				SET @SqlCmd=CONCAT('SELECT TABLE_ROWS INTO @V_ROW_CNT_LTE_MN22 FROM information_schema.`TABLES`
					WHERE TABLE_SCHEMA=''',v_group_db_name,''' 
					AND TABLE_NAME=''table_cell_tile_16_lte_mn_',@DATE_MN,''';');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				
				SET @SqlCmd=CONCAT('SELECT TABLE_ROWS INTO @V_ROW_CNT_LTE_MN23 FROM information_schema.`TABLES`
					WHERE TABLE_SCHEMA=''',v_group_db_name,''' 
					AND TABLE_NAME=''table_cell_tile_13_lte_mn_',@DATE_MN,''';');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				
				SET @SqlCmd=CONCAT('SELECT TABLE_ROWS INTO @V_ROW_CNT_LTE_MN24 FROM information_schema.`TABLES`
					WHERE TABLE_SCHEMA=''',v_group_db_name,''' 
					AND TABLE_NAME=''table_tile_19_lte_mn_',@DATE_MN,''';');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				
				SET @SqlCmd=CONCAT('SELECT TABLE_ROWS INTO @V_ROW_CNT_LTE_MN25 FROM information_schema.`TABLES`
					WHERE TABLE_SCHEMA=''',v_group_db_name,''' 
					AND TABLE_NAME=''table_tile_16_lte_mn_',@DATE_MN,''';');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				
				SET @SqlCmd=CONCAT('SELECT TABLE_ROWS INTO @V_ROW_CNT_LTE_MN26 FROM information_schema.`TABLES`
					WHERE TABLE_SCHEMA=''',v_group_db_name,''' 
					AND TABLE_NAME=''table_tile_13_lte_mn_',@DATE_MN,''';');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				
				SET @SqlCmd=CONCAT('SELECT TABLE_ROWS INTO @V_ROW_CNT_LTE_MN27 FROM information_schema.`TABLES`
					WHERE TABLE_SCHEMA=''',v_group_db_name,''' 
					AND TABLE_NAME=''table_imsi_lte_mn_',@DATE_MN,''';');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				
				SET @SqlCmd=CONCAT('SELECT TABLE_ROWS INTO @V_ROW_CNT_LTE_MN28 FROM information_schema.`TABLES`
					WHERE TABLE_SCHEMA=''',v_group_db_name,''' 
					AND TABLE_NAME=''table_handset_lte_mn_',@DATE_MN,''';');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
		
				SET @SqlCmd=CONCAT('SELECT TABLE_ROWS INTO @V_ROW_CNT_LTE_MN29 FROM information_schema.`TABLES`
					WHERE TABLE_SCHEMA=''',v_group_db_name,''' 
					AND TABLE_NAME=''table_cell_lte_mn_',@DATE_MN,''';');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				
				SET @V_ROW_CNT_LTE_MN=IFNULL(@V_ROW_CNT_LTE_MN21,0)+IFNULL(@V_ROW_CNT_LTE_MN22,0)+IFNULL(@V_ROW_CNT_LTE_MN23,0)+IFNULL(@V_ROW_CNT_LTE_MN24,0)
						+IFNULL(@V_ROW_CNT_LTE_MN25,0)+IFNULL(@V_ROW_CNT_LTE_MN26,0)+IFNULL(@V_ROW_CNT_LTE_MN27,0)+IFNULL(@V_ROW_CNT_LTE_MN28,0)
						+IFNULL(@V_ROW_CNT_LTE_MN29,0);
				IF @V_ROW_CNT_LTE_MN>0 THEN
					SET @SqlCmd=CONCAT('INSERT IGNORE INTO ',@global_db,'.`nation_wide_session_information`
						    (`DATA_DATE`,
						     `END_DATA_DATE`,
						     `TECH_MASK`,
						     `SESSION_TYPE`,
						     `SESSION_URI`)
					VALUES (''',DATE_SUB(v_DATA_DATE_d,INTERVAL DAY(v_DATA_DATE_d)-1 DAY),''',
						''',LAST_DAY(v_DATA_DATE_d),''',
						4,
						''MONTH'',
						NULL);');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;		
				END IF;
			END IF;
		END IF;
		SET @v_i=@v_i+1;
	END;
	END WHILE;
	INSERT INTO gt_gw_main.SP_LOG VALUES('gt_global_statistic','SP_Generate_Global_Statistic_log_MMR',CONCAT(MAIN_WORKER_ID,' Done cost:',TIMESTAMPDIFF(SECOND,START_TIME,SYSDATE()),' sec.'), NOW());
END$$
DELIMITER ;
