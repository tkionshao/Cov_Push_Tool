DELIMITER $$
USE `gt_global_statistic`$$
DROP PROCEDURE IF EXISTS `SP_CreateDB_LTE`$$
CREATE DEFINER=`covmo`@`%` PROCEDURE `SP_Generate_Global_Statistic_Purge_Data`(IN TECH_MASK TINYINT(2),IN exDate VARCHAR(10),IN KIND TINYINT(2))
BEGIN
	DECLARE START_TIME DATETIME DEFAULT SYSDATE();
	DECLARE STEP_START_TIME DATETIME DEFAULT SYSDATE();
	DECLARE FIRSTDAY_OF_WEEK TINYINT(4) DEFAULT 6;
	DECLARE v_DATA_DATE VARCHAR(10) DEFAULT NULL;
	DECLARE v_group_db_name VARCHAR(100) DEFAULT '';
			
	SET v_DATA_DATE:=DATE_FORMAT(exDate,'%Y%m%d');
	SELECT `DAY_OF_WEEK`(v_DATA_DATE) INTO @DAY_OF_WEEK;
	SET @FIRST_DAY=gt_strtok(@DAY_OF_WEEK, 1, '|');
	SET @END_DAY=gt_strtok(@DAY_OF_WEEK, 2, '|');
	SET @DATE_WK=CONCAT(DATE_FORMAT(@FIRST_DAY,'%Y%m%d'),'_',DATE_FORMAT(@END_DAY,'%Y%m%d'));
	SET @DATE_MN=DATE_FORMAT(v_DATA_DATE,'%Y%m');
	SET @DATE_DY=DATE_FORMAT(v_DATA_DATE,'%Y%m%d');
	SET SESSION max_heap_table_size=8*1024*1024*1024;
	SET SESSION tmp_table_size=8*1024*1024*1024;	
	SET SESSION read_buffer_size=2*1024*1024*1024;
	SET SESSION group_concat_max_len=102400; 
	SET @global_db='gt_global_statistic';
	INSERT INTO gt_gw_main.SP_LOG VALUES('gt_global_statistic','SP_Generate_Global_Statistic_Purge_Data','START', START_TIME);
	SET STEP_START_TIME := SYSDATE();
	SET @SqlCmd=CONCAT('SELECT GROUP_CONCAT(DISTINCT `group_id` ORDER BY `group_id` SEPARATOR ''|'') INTO @REG_GROUP FROM ',@global_db,'.`usr_polygon_reg_3`;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;		
        	
	SET @v_reg_m=1;
	SET @v_reg_Max=gt_covmo_csv_count(@REG_GROUP,'|');
	WHILE @v_reg_m <= @v_reg_Max DO
	BEGIN
		SET v_group_db_name=CONCAT('gt_global_statistic_g',gt_strtok(@REG_GROUP, @v_reg_m, '|'));
		IF TECH_MASK IN (0,1) THEN  
		BEGIN
			IF KIND=1 THEN 
			BEGIN
				SET @SqlCmd=CONCAT('SELECT GROUP_CONCAT(tbl_name SEPARATOR ''|'' ) INTO @PRG_STR 
							FROM ',@global_db,'.`table_created_history`
							WHERE `START_DATE`<''',exDate,'''
							AND `TECH_MASK`=1
							AND `KIND`=',KIND,'
							;');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				
				SET @v_i=1;
				SET @v_R_Max=gt_covmo_csv_count(@PRG_STR,'|');
				WHILE @v_i <= @v_R_Max DO
				BEGIN
					SET @table_name:=gt_strtok(@PRG_STR, @v_i, '|');
					SET @SqlCmd=CONCAT('DROP TABLE IF EXISTS ',v_group_db_name,'.',@table_name,';');
					PREPARE Stmt FROM @SqlCmd;
					EXECUTE Stmt;
					DEALLOCATE PREPARE Stmt;
					SET @v_i=@v_i+1; 
				END;
				END WHILE;
				IF @v_R_Max>0 then 
					SET @SqlCmd=CONCAT('DELETE FROM ',@global_db,'.`table_created_history`
								WHERE `START_DATE`<''',exDate,'''
								AND `TECH_MASK`=1
								AND `KIND`=',KIND,'
								;');
					PREPARE Stmt FROM @SqlCmd;
					EXECUTE Stmt;
					DEALLOCATE PREPARE Stmt;
					
					SET @SqlCmd=CONCAT('DELETE FROM ',@global_db,'.`nation_wide_session_information`
								WHERE `DATA_DATE`<''',exDate,'''
								AND `TECH_MASK`=1
								AND `SESSION_TYPE`=''DAY''
								;');
					PREPARE Stmt FROM @SqlCmd;
					EXECUTE Stmt;
					DEALLOCATE PREPARE Stmt;
				end if;
			END;
			ELSEIF KIND=2 THEN 
			BEGIN
				SET @SqlCmd=CONCAT('SELECT GROUP_CONCAT(tbl_name SEPARATOR ''|'' ) INTO @PRG_STR 
							FROM ',@global_db,'.`table_created_history` 
							WHERE `START_DATE`<''',exDate,'''
							AND `TECH_MASK`=1
							AND `KIND`=',KIND,'
							;');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				
				SET @v_i=1;
				SET @v_R_Max=gt_covmo_csv_count(@PRG_STR,'|');
				WHILE @v_i <= @v_R_Max DO
				BEGIN
					SET @table_name:=gt_strtok(@PRG_STR, @v_i, '|');
					SET @SqlCmd=CONCAT('DROP TABLE IF EXISTS ',v_group_db_name,'.',@table_name,';');
					PREPARE Stmt FROM @SqlCmd;
					EXECUTE Stmt;
					DEALLOCATE PREPARE Stmt;
					SET @v_i=@v_i+1; 
				END;
				END WHILE;
				IF @v_R_Max>0 THEN 
					SET @SqlCmd=CONCAT('DELETE FROM ',@global_db,'.`table_created_history`
								WHERE `START_DATE`<''',exDate,'''
								AND `TECH_MASK`=1
								AND `KIND`=',KIND,'
								;');
					PREPARE Stmt FROM @SqlCmd;
					EXECUTE Stmt;
					DEALLOCATE PREPARE Stmt;
					
					SET @SqlCmd=CONCAT('DELETE FROM ',@global_db,'.`nation_wide_session_information`
								WHERE `DATA_DATE`<''',exDate,'''
								AND `TECH_MASK`=1
								AND `SESSION_TYPE`=''DAY''
								;');
					PREPARE Stmt FROM @SqlCmd;
					EXECUTE Stmt;
					DEALLOCATE PREPARE Stmt;
				END IF;
			END;
			ELSEIF KIND=3 THEN 
			BEGIN
				SET @SqlCmd=CONCAT('SELECT GROUP_CONCAT(tbl_name SEPARATOR ''|'' ) INTO @PRG_STR 
							FROM ',@global_db,'.`table_created_history`
							WHERE `START_DATE`<=''',DATE_SUB(exDate,INTERVAL 1 WEEK),'''
							AND `TECH_MASK`=1
							AND `KIND`=',KIND,'
							;');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				
				SET @v_i=1;
				SET @v_R_Max=gt_covmo_csv_count(@PRG_STR,'|');
				WHILE @v_i <= @v_R_Max DO
				BEGIN
					SET @table_name:=gt_strtok(@PRG_STR, @v_i, '|');
					SET @SqlCmd=CONCAT('DROP TABLE IF EXISTS ',v_group_db_name,'.',@table_name,';');
					PREPARE Stmt FROM @SqlCmd;
					EXECUTE Stmt;
					DEALLOCATE PREPARE Stmt;
					SET @v_i=@v_i+1; 
				END;
				END WHILE;
				IF @v_R_Max>0 THEN 
					SET @SqlCmd=CONCAT('DELETE FROM ',@global_db,'.`table_created_history`
								WHERE `START_DATE`<=''',DATE_SUB(exDate,INTERVAL 1 WEEK),'''
								AND `TECH_MASK`=1
								AND `KIND`=',KIND,'
								;');
					PREPARE Stmt FROM @SqlCmd;
					EXECUTE Stmt;
					DEALLOCATE PREPARE Stmt;
					SET @SqlCmd=CONCAT('DELETE FROM ',@global_db,'.`nation_wide_session_information`
								WHERE `DATA_DATE`<=''',DATE_SUB(exDate,INTERVAL 1 WEEK),'''
								AND `TECH_MASK`=1
								AND `SESSION_TYPE`=''WEEK''
								;');
					PREPARE Stmt FROM @SqlCmd;
					EXECUTE Stmt;
					DEALLOCATE PREPARE Stmt;
				END IF;
			END;
			ELSEIF KIND=4 THEN 
			BEGIN
				SET @SqlCmd=CONCAT('SELECT GROUP_CONCAT(tbl_name SEPARATOR ''|'' ) INTO @PRG_STR 
							FROM ',@global_db,'.`table_created_history` 
							WHERE `START_DATE`<=''',DATE_SUB(exDate,INTERVAL 1 MONTH),'''
							AND `TECH_MASK`=1
							AND `KIND`=',KIND,'
							;');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				
				SET @v_i=1;
				SET @v_R_Max=gt_covmo_csv_count(@PRG_STR,'|');
				WHILE @v_i <= @v_R_Max DO
				BEGIN
					SET @table_name:=gt_strtok(@PRG_STR, @v_i, '|');
					SET @SqlCmd=CONCAT('DROP TABLE IF EXISTS ',v_group_db_name,'.',@table_name,';');	
					PREPARE Stmt FROM @SqlCmd;
					EXECUTE Stmt;
					DEALLOCATE PREPARE Stmt;
					SET @v_i=@v_i+1; 
				END;
				END WHILE;
				IF @v_R_Max>0 THEN 
					SET @SqlCmd=CONCAT('DELETE FROM ',@global_db,'.`table_created_history`
								WHERE `START_DATE`<=''',DATE_SUB(exDate,INTERVAL 1 MONTH),'''
								AND `TECH_MASK`=1
								AND `KIND`=',KIND,'
								;');
					PREPARE Stmt FROM @SqlCmd;
					EXECUTE Stmt;
					DEALLOCATE PREPARE Stmt;
					SET @SqlCmd=CONCAT('DELETE FROM ',@global_db,'.`nation_wide_session_information`
								WHERE `DATA_DATE`<=''',DATE_SUB(exDate,INTERVAL 1 MONTH),'''
								AND `TECH_MASK`=1
								AND `SESSION_TYPE`=''MONTH''
								;');
					PREPARE Stmt FROM @SqlCmd;
					EXECUTE Stmt;
					DEALLOCATE PREPARE Stmt;
				END IF;
			END;
			
	
			ELSEIF KIND=5 THEN 
			BEGIN
				SET @SqlCmd=CONCAT('SELECT GROUP_CONCAT(tbl_name SEPARATOR ''|'' ) INTO @PRG_STR 
							FROM ',@global_db,'.`table_created_history` 
							WHERE `START_DATE`<''',exDate,'''
							AND `TECH_MASK`=1
							AND `KIND`=',KIND,'
							;');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				
				SET @v_i=1;
				SET @v_R_Max=gt_covmo_csv_count(@PRG_STR,'|');
				WHILE @v_i <= @v_R_Max DO
				BEGIN
					SET @table_name:=gt_strtok(@PRG_STR, @v_i, '|');
					SET @SqlCmd=CONCAT('DROP TABLE IF EXISTS ',v_group_db_name,'.',@table_name,';');
					PREPARE Stmt FROM @SqlCmd;
					EXECUTE Stmt;
					DEALLOCATE PREPARE Stmt;
					SET @v_i=@v_i+1; 
				END;
				END WHILE;
				IF @v_R_Max>0 THEN 
					SET @SqlCmd=CONCAT('DELETE FROM ',@global_db,'.`table_created_history`
								WHERE `START_DATE`<''',exDate,'''
								AND `TECH_MASK`=1
								AND `KIND`=',KIND,'
								;');
					PREPARE Stmt FROM @SqlCmd;
					EXECUTE Stmt;
					DEALLOCATE PREPARE Stmt;
					
					SET @SqlCmd=CONCAT('DELETE FROM ',@global_db,'.`nation_wide_session_information`
								WHERE `DATA_DATE`<''',exDate,'''
								AND `TECH_MASK`=1
								AND `SESSION_TYPE`=''DAY''
								;');
					PREPARE Stmt FROM @SqlCmd;
					EXECUTE Stmt;
					DEALLOCATE PREPARE Stmt;
				END IF;
			END;
	
			ELSEIF KIND=6 THEN 
			BEGIN
				SET @SqlCmd=CONCAT('SELECT GROUP_CONCAT(tbl_name SEPARATOR ''|'' ) INTO @PRG_STR 
							FROM ',@global_db,'.`table_created_history` 
							WHERE `START_DATE`<''',exDate,'''
							AND `TECH_MASK`=1
							AND `KIND`=',KIND,'
							;');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				
				SET @v_i=1;
				SET @v_R_Max=gt_covmo_csv_count(@PRG_STR,'|');
				WHILE @v_i <= @v_R_Max DO
				BEGIN
					SET @table_name:=gt_strtok(@PRG_STR, @v_i, '|');
					SET @SqlCmd=CONCAT('DROP TABLE IF EXISTS ',v_group_db_name,'.',@table_name,';');
					PREPARE Stmt FROM @SqlCmd;
					EXECUTE Stmt;
					DEALLOCATE PREPARE Stmt;
					SET @v_i=@v_i+1; 
				END;
				END WHILE;
				IF @v_R_Max>0 THEN 
					SET @SqlCmd=CONCAT('DELETE FROM ',@global_db,'.`table_created_history`
								WHERE `START_DATE`<''',exDate,'''
								AND `TECH_MASK`=1
								AND `KIND`=',KIND,'
								;');
					PREPARE Stmt FROM @SqlCmd;
					EXECUTE Stmt;
					DEALLOCATE PREPARE Stmt;
					
					SET @SqlCmd=CONCAT('DELETE FROM ',@global_db,'.`nation_wide_session_information`
								WHERE `DATA_DATE`<''',exDate,'''
								AND `TECH_MASK`=1
								AND `SESSION_TYPE`=''DAY''
								;');
					PREPARE Stmt FROM @SqlCmd;
					EXECUTE Stmt;
					DEALLOCATE PREPARE Stmt;
				END IF;
			END;
			END IF;
		END;
		ELSEIF TECH_MASK IN (0,2) THEN  
		BEGIN
			IF KIND=1 THEN 
			BEGIN
				SET @SqlCmd=CONCAT('SELECT GROUP_CONCAT(tbl_name SEPARATOR ''|'' ) INTO @PRG_STR 
							FROM ',@global_db,'.`table_created_history`
							WHERE `START_DATE`<''',exDate,'''
							AND `TECH_MASK`=2
							AND `KIND`=',KIND,'
							;');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				
				SET @v_i=1;
				SET @v_R_Max=gt_covmo_csv_count(@PRG_STR,'|');
				WHILE @v_i <= @v_R_Max DO
				BEGIN
					SET @table_name:=gt_strtok(@PRG_STR, @v_i, '|');
					SET @SqlCmd=CONCAT('DROP TABLE IF EXISTS ',v_group_db_name,'.',@table_name,';');
	 				PREPARE Stmt FROM @SqlCmd;
					EXECUTE Stmt;
					DEALLOCATE PREPARE Stmt;
				SET @v_i=@v_i+1; 
				END;
				END WHILE;
				IF @v_R_Max>0 THEN 
					SET @SqlCmd=CONCAT('DELETE FROM ',@global_db,'.`table_created_history`
								WHERE `START_DATE`<''',exDate,'''
								AND `TECH_MASK`=2
								AND `KIND`=',KIND,'
								;');
					PREPARE Stmt FROM @SqlCmd;
					EXECUTE Stmt;
					DEALLOCATE PREPARE Stmt;
					
					SET @SqlCmd=CONCAT('DELETE FROM ',@global_db,'.`nation_wide_session_information`
								WHERE `DATA_DATE`<''',exDate,'''
								AND `TECH_MASK`=2
								AND `SESSION_TYPE`=''DAY''
								;');
					PREPARE Stmt FROM @SqlCmd;
					EXECUTE Stmt;
					DEALLOCATE PREPARE Stmt;
				END IF;
			END;
			ELSEIF KIND=2 THEN 
			BEGIN
				SET @SqlCmd=CONCAT('SELECT GROUP_CONCAT(tbl_name SEPARATOR ''|'' ) INTO @PRG_STR 
							FROM ',@global_db,'.`table_created_history`
							WHERE `START_DATE`<''',exDate,'''
							AND `TECH_MASK`=2
							AND `KIND`=',KIND,'
							;');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				
				SET @v_i=1;
				SET @v_R_Max=gt_covmo_csv_count(@PRG_STR,'|');
				WHILE @v_i <= @v_R_Max DO
				BEGIN
					SET @table_name:=gt_strtok(@PRG_STR, @v_i, '|');
					SET @SqlCmd=CONCAT('DROP TABLE IF EXISTS ',v_group_db_name,'.',@table_name,';');
					PREPARE Stmt FROM @SqlCmd;
					EXECUTE Stmt;
					DEALLOCATE PREPARE Stmt;
					SET @v_i=@v_i+1; 
				END;
				END WHILE;
				IF @v_R_Max>0 THEN 
					SET @SqlCmd=CONCAT('DELETE FROM ',@global_db,'.`table_created_history`
								WHERE `START_DATE`<''',exDate,'''
								AND `TECH_MASK`=2
								AND `KIND`=',KIND,'
								;');
					PREPARE Stmt FROM @SqlCmd;
					EXECUTE Stmt;
					DEALLOCATE PREPARE Stmt;
					SET @SqlCmd=CONCAT('DELETE FROM ',@global_db,'.`nation_wide_session_information`
								WHERE `DATA_DATE`<''',exDate,'''
								AND `TECH_MASK`=2
								AND `SESSION_TYPE`=''DAY''
								;');
					PREPARE Stmt FROM @SqlCmd;
					EXECUTE Stmt;
					DEALLOCATE PREPARE Stmt;
				END IF;
			END;
			ELSEIF KIND=3 THEN 
			BEGIN
				SET @SqlCmd=CONCAT('SELECT GROUP_CONCAT(tbl_name SEPARATOR ''|'' ) INTO @PRG_STR 
							FROM ',@global_db,'.`table_created_history` 
							WHERE `START_DATE`<=''',DATE_SUB(exDate,INTERVAL 1 WEEK),'''
							AND `TECH_MASK`=2
							AND `KIND`=',KIND,'
							;');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				
				SET @v_i=1;
				SET @v_R_Max=gt_covmo_csv_count(@PRG_STR,'|');
				WHILE @v_i <= @v_R_Max DO
				BEGIN
					SET @table_name:=gt_strtok(@PRG_STR, @v_i, '|');
					SET @SqlCmd=CONCAT('DROP TABLE IF EXISTS ',v_group_db_name,'.',@table_name,';');
					PREPARE Stmt FROM @SqlCmd;
					EXECUTE Stmt;
					DEALLOCATE PREPARE Stmt;
					SET @v_i=@v_i+1; 
				END;
				END WHILE;
				IF @v_R_Max>0 THEN 
					SET @SqlCmd=CONCAT('DELETE FROM ',@global_db,'.`table_created_history`
								WHERE `START_DATE`<=''',DATE_SUB(exDate,INTERVAL 1 WEEK),'''
								AND `TECH_MASK`=2
								AND `KIND`=',KIND,'
								;');
					PREPARE Stmt FROM @SqlCmd;
					EXECUTE Stmt;
					DEALLOCATE PREPARE Stmt;
					SET @SqlCmd=CONCAT('DELETE FROM ',@global_db,'.`nation_wide_session_information`
								WHERE `DATA_DATE`<=''',DATE_SUB(exDate,INTERVAL 1 WEEK),'''
								AND `TECH_MASK`=2
								AND `SESSION_TYPE`=''WEEK''
								;');
					PREPARE Stmt FROM @SqlCmd;
					EXECUTE Stmt;
					DEALLOCATE PREPARE Stmt;
				END IF;
			END;
			ELSEIF KIND=4 THEN 
			BEGIN
				SET @SqlCmd=CONCAT('SELECT GROUP_CONCAT(tbl_name SEPARATOR ''|'' ) INTO @PRG_STR 
							FROM ',@global_db,'.`table_created_history` 
							WHERE `START_DATE`<=''',DATE_SUB(exDate,INTERVAL 1 MONTH),'''
							AND `TECH_MASK`=2
							AND `KIND`=',KIND,'
							;');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				
				SET @v_i=1;
				SET @v_R_Max=gt_covmo_csv_count(@PRG_STR,'|');
				WHILE @v_i <= @v_R_Max DO
				BEGIN
					SET @table_name:=gt_strtok(@PRG_STR, @v_i, '|');
					SET @SqlCmd=CONCAT('DROP TABLE IF EXISTS ',v_group_db_name,'.',@table_name,';');
					PREPARE Stmt FROM @SqlCmd;
					EXECUTE Stmt;
					DEALLOCATE PREPARE Stmt;
					SET @v_i=@v_i+1; 
				END;
				END WHILE;
				IF @v_R_Max>0 THEN 
					SET @SqlCmd=CONCAT('DELETE FROM ',@global_db,'.`table_created_history`
								WHERE `START_DATE`<=''',DATE_SUB(exDate,INTERVAL 1 MONTH),'''
								AND `TECH_MASK`=2
								AND `KIND`=',KIND,'
								;');
					PREPARE Stmt FROM @SqlCmd;
					EXECUTE Stmt;
					DEALLOCATE PREPARE Stmt;
					SET @SqlCmd=CONCAT('DELETE FROM ',@global_db,'.`nation_wide_session_information`
								WHERE `DATA_DATE`<=''',DATE_SUB(exDate,INTERVAL 1 MONTH),'''
								AND `TECH_MASK`=2
								AND `SESSION_TYPE`=''MONTH''
								;');
					PREPARE Stmt FROM @SqlCmd;
					EXECUTE Stmt;
					DEALLOCATE PREPARE Stmt;
				END IF;
			END;
			
			ELSEIF KIND=5 THEN 
			BEGIN
				SET @SqlCmd=CONCAT('SELECT GROUP_CONCAT(tbl_name SEPARATOR ''|'' ) INTO @PRG_STR 
							FROM ',@global_db,'.`table_created_history` 
							WHERE `START_DATE`<''',exDate,'''
							AND `TECH_MASK`=2
							AND `KIND`=',KIND,'
							;');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				
				SET @v_i=1;
				SET @v_R_Max=gt_covmo_csv_count(@PRG_STR,'|');
				WHILE @v_i <= @v_R_Max DO
				BEGIN
					SET @table_name:=gt_strtok(@PRG_STR, @v_i, '|');
					SET @SqlCmd=CONCAT('DROP TABLE IF EXISTS ',v_group_db_name,'.',@table_name,';');
					PREPARE Stmt FROM @SqlCmd;
					EXECUTE Stmt;
					DEALLOCATE PREPARE Stmt;
					SET @v_i=@v_i+1; 
				END;
				END WHILE;
				IF @v_R_Max>0 THEN 
					SET @SqlCmd=CONCAT('DELETE FROM ',@global_db,'.`table_created_history`
								WHERE `START_DATE`<''',exDate,'''
								AND `TECH_MASK`=2
								AND `KIND`=',KIND,'
								;');
					PREPARE Stmt FROM @SqlCmd;
					EXECUTE Stmt;
					DEALLOCATE PREPARE Stmt;
				END IF;
			END;
	
			ELSEIF KIND=6 THEN 
			BEGIN
				SET @SqlCmd=CONCAT('SELECT GROUP_CONCAT(tbl_name SEPARATOR ''|'' ) INTO @PRG_STR 
							FROM ',@global_db,'.`table_created_history` 
							WHERE `START_DATE`<''',exDate,'''
							AND `TECH_MASK`=2
							AND `KIND`=',KIND,'
							;');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				
				SET @v_i=1;
				SET @v_R_Max=gt_covmo_csv_count(@PRG_STR,'|');
				WHILE @v_i <= @v_R_Max DO
				BEGIN
					SET @table_name:=gt_strtok(@PRG_STR, @v_i, '|');
					SET @SqlCmd=CONCAT('DROP TABLE IF EXISTS ',v_group_db_name,'.',@table_name,';');
					PREPARE Stmt FROM @SqlCmd;
					EXECUTE Stmt;
					DEALLOCATE PREPARE Stmt;
					SET @v_i=@v_i+1; 
				END;
				END WHILE;
				IF @v_R_Max>0 THEN 
					SET @SqlCmd=CONCAT('DELETE FROM ',@global_db,'.`table_created_history`
								WHERE `START_DATE`<''',exDate,'''
								AND `TECH_MASK`=2
								AND `KIND`=',KIND,'
								;');
					PREPARE Stmt FROM @SqlCmd;
					EXECUTE Stmt;
					DEALLOCATE PREPARE Stmt;
					
				END IF;
			END;
			END IF;
		END;
		ELSEIF TECH_MASK IN (0,4) THEN  
		BEGIN
			IF KIND=1 THEN 
			BEGIN
				SET @SqlCmd=CONCAT('SELECT GROUP_CONCAT(tbl_name SEPARATOR ''|'' ) INTO @PRG_STR 
							FROM ',@global_db,'.`table_created_history`
							WHERE `START_DATE`<''',exDate,'''
							AND `TECH_MASK`=4
							AND `KIND`=',KIND,'
							;');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;	
				SET @v_i=1;
				SET @v_R_Max=gt_covmo_csv_count(@PRG_STR,'|');
				WHILE @v_i <= @v_R_Max DO
				BEGIN
					SET @table_name:=gt_strtok(@PRG_STR, @v_i, '|');
					SET @SqlCmd=CONCAT('DROP TABLE IF EXISTS ',v_group_db_name,'.',@table_name,';');
					PREPARE Stmt FROM @SqlCmd;
					EXECUTE Stmt;
					DEALLOCATE PREPARE Stmt;
					SET @v_i=@v_i+1; 
				END;
				END WHILE;
				IF @v_R_Max>0 THEN 
					SET @SqlCmd=CONCAT('DELETE FROM ',@global_db,'.`table_created_history`
								WHERE `START_DATE`<''',exDate,'''
								AND `TECH_MASK`=4
								AND `KIND`=',KIND,'
								;');
					PREPARE Stmt FROM @SqlCmd;
					EXECUTE Stmt;
					DEALLOCATE PREPARE Stmt;
					SET @SqlCmd=CONCAT('DELETE FROM ',@global_db,'.`nation_wide_session_information`
								WHERE `DATA_DATE`<''',exDate,'''
								AND `TECH_MASK`=4
								AND `SESSION_TYPE`=''DAY''
								;');
					PREPARE Stmt FROM @SqlCmd;
					EXECUTE Stmt;
					DEALLOCATE PREPARE Stmt;
				END IF;
			END;
			ELSEIF KIND=2 THEN 
			BEGIN
				SET @SqlCmd=CONCAT('SELECT GROUP_CONCAT(tbl_name SEPARATOR ''|'' ) INTO @PRG_STR 
							FROM ',@global_db,'.`table_created_history` 
							WHERE `START_DATE`<''',exDate,'''
							AND `TECH_MASK`=4
							AND `KIND`=',KIND,'
							;');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				
				SET @v_i=1;
				SET @v_R_Max=gt_covmo_csv_count(@PRG_STR,'|');
				WHILE @v_i <= @v_R_Max DO
				BEGIN
					SET @table_name:=gt_strtok(@PRG_STR, @v_i, '|');
					SET @SqlCmd=CONCAT('DROP TABLE IF EXISTS ',v_group_db_name,'.',@table_name,';');
					PREPARE Stmt FROM @SqlCmd;
					EXECUTE Stmt;
					DEALLOCATE PREPARE Stmt;
					SET @v_i=@v_i+1; 
				END;
				END WHILE;
				IF @v_R_Max>0 THEN 
					SET @SqlCmd=CONCAT('DELETE FROM ',@global_db,'.`table_created_history`
								WHERE `START_DATE`<''',exDate,'''
								AND `TECH_MASK`=4
								AND `KIND`=',KIND,'
								;');
					PREPARE Stmt FROM @SqlCmd;
					EXECUTE Stmt;
					DEALLOCATE PREPARE Stmt;
					SET @SqlCmd=CONCAT('DELETE FROM ',@global_db,'.`nation_wide_session_information`
								WHERE `DATA_DATE`<''',exDate,'''
								AND `TECH_MASK`=4
								AND `SESSION_TYPE`=''DAY''
								;');
					PREPARE Stmt FROM @SqlCmd;
					EXECUTE Stmt;
					DEALLOCATE PREPARE Stmt;
				END IF;
			END;
			ELSEIF KIND=3 THEN 
			BEGIN
				SET @SqlCmd=CONCAT('SELECT GROUP_CONCAT(tbl_name SEPARATOR ''|'' ) INTO @PRG_STR 
							FROM ',@global_db,'.`table_created_history` 
							WHERE `START_DATE`<=''',DATE_SUB(exDate,INTERVAL 1 WEEK),'''
							AND `TECH_MASK`=4
							AND `KIND`=',KIND,'
							;');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				
				SET @v_i=1;
				SET @v_R_Max=gt_covmo_csv_count(@PRG_STR,'|');
				WHILE @v_i <= @v_R_Max DO
				BEGIN
					SET @table_name:=gt_strtok(@PRG_STR, @v_i, '|');
					SET @SqlCmd=CONCAT('DROP TABLE IF EXISTS ',v_group_db_name,'.',@table_name,';');
					PREPARE Stmt FROM @SqlCmd;
					EXECUTE Stmt;
					DEALLOCATE PREPARE Stmt;
					SET @v_i=@v_i+1; 
				END;
				END WHILE;
				IF @v_R_Max>0 THEN 
					SET @SqlCmd=CONCAT('DELETE FROM ',@global_db,'.`table_created_history`
								WHERE `START_DATE`<=''',DATE_SUB(exDate,INTERVAL 1 WEEK),'''
								AND `TECH_MASK`=4
								AND `KIND`=',KIND,'
								;');
					PREPARE Stmt FROM @SqlCmd;
					EXECUTE Stmt;
					DEALLOCATE PREPARE Stmt;
					SET @SqlCmd=CONCAT('DELETE FROM ',@global_db,'.`nation_wide_session_information`
								WHERE `DATA_DATE`<=''',DATE_SUB(exDate,INTERVAL 1 WEEK),'''
								AND `TECH_MASK`=4
								AND `SESSION_TYPE`=''WEEK''
								;');
					PREPARE Stmt FROM @SqlCmd;
					EXECUTE Stmt;
					DEALLOCATE PREPARE Stmt;
				END IF;
			END;
			ELSEIF KIND=4 THEN 
			BEGIN
				SET @SqlCmd=CONCAT('SELECT GROUP_CONCAT(tbl_name SEPARATOR ''|'' ) INTO @PRG_STR 
							FROM ',@global_db,'.`table_created_history` 
							WHERE `START_DATE`<=''',DATE_SUB(exDate,INTERVAL 1 MONTH),'''
							AND `TECH_MASK`=4
							AND `KIND`=',KIND,'
							;');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				
				SET @v_i=1;
				SET @v_R_Max=gt_covmo_csv_count(@PRG_STR,'|');
				WHILE @v_i <= @v_R_Max DO
				BEGIN
					SET @table_name:=gt_strtok(@PRG_STR, @v_i, '|');
					SET @SqlCmd=CONCAT('DROP TABLE IF EXISTS ',v_group_db_name,'.',@table_name,';');
					PREPARE Stmt FROM @SqlCmd;
					EXECUTE Stmt;
					DEALLOCATE PREPARE Stmt;
					SET @v_i=@v_i+1; 
				END;
				END WHILE;
				IF @v_R_Max>0 THEN 
					SET @SqlCmd=CONCAT('DELETE FROM ',@global_db,'.`table_created_history`
								WHERE `START_DATE`<=''',DATE_SUB(exDate,INTERVAL 1 MONTH),'''
								AND `TECH_MASK`=4
								AND `KIND`=',KIND,'
								;');
					PREPARE Stmt FROM @SqlCmd;
					EXECUTE Stmt;
					DEALLOCATE PREPARE Stmt;
					
					SET @SqlCmd=CONCAT('DELETE FROM ',@global_db,'.`nation_wide_session_information`
								WHERE `DATA_DATE`<=''',DATE_SUB(exDate,INTERVAL 1 MONTH),'''
								AND `TECH_MASK`=4
								AND `SESSION_TYPE`=''MONTH''
								;');
					PREPARE Stmt FROM @SqlCmd;
					EXECUTE Stmt;
					DEALLOCATE PREPARE Stmt;
				END IF;
			END;
			
			ELSEIF KIND=5 THEN 
			BEGIN
				SET @SqlCmd=CONCAT('SELECT GROUP_CONCAT(tbl_name SEPARATOR ''|'' ) INTO @PRG_STR 
							FROM ',@global_db,'.`table_created_history` 
							WHERE `START_DATE`<''',exDate,'''
							AND `TECH_MASK`=4
							AND `KIND`=',KIND,'
							;');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				
				SET @v_i=1;
				SET @v_R_Max=gt_covmo_csv_count(@PRG_STR,'|');
				WHILE @v_i <= @v_R_Max DO
				BEGIN
					SET @table_name:=gt_strtok(@PRG_STR, @v_i, '|');
					SET @SqlCmd=CONCAT('DROP TABLE IF EXISTS ',v_group_db_name,'.',@table_name,';');
					PREPARE Stmt FROM @SqlCmd;
					EXECUTE Stmt;
					DEALLOCATE PREPARE Stmt;
					SET @v_i=@v_i+1; 
				END;
				END WHILE;
				IF @v_R_Max>0 THEN 
					SET @SqlCmd=CONCAT('DELETE FROM ',@global_db,'.`table_created_history`
								WHERE `START_DATE`<''',exDate,'''
								AND `TECH_MASK`=4
								AND `KIND`=',KIND,'
								;');
					PREPARE Stmt FROM @SqlCmd;
					EXECUTE Stmt;
					DEALLOCATE PREPARE Stmt;
					
				END IF;
			END;
	
			ELSEIF KIND=6 THEN 
			BEGIN
				SET @SqlCmd=CONCAT('SELECT GROUP_CONCAT(tbl_name SEPARATOR ''|'' ) INTO @PRG_STR 
							FROM ',@global_db,'.`table_created_history` 
							WHERE `START_DATE`<''',exDate,'''
							AND `TECH_MASK`=4
							AND `KIND`=',KIND,'
							;');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				
				SET @v_i=1;
				SET @v_R_Max=gt_covmo_csv_count(@PRG_STR,'|');
				WHILE @v_i <= @v_R_Max DO
				BEGIN
					SET @table_name:=gt_strtok(@PRG_STR, @v_i, '|');
					SET @SqlCmd=CONCAT('DROP TABLE IF EXISTS ',v_group_db_name,'.',@table_name,';');
					PREPARE Stmt FROM @SqlCmd;
					EXECUTE Stmt;
					DEALLOCATE PREPARE Stmt;
					SET @v_i=@v_i+1; 
				END;
				END WHILE;
				IF @v_R_Max>0 THEN 
					SET @SqlCmd=CONCAT('DELETE FROM ',@global_db,'.`table_created_history`
								WHERE `START_DATE`<''',exDate,'''
								AND `TECH_MASK`=4
								AND `KIND`=',KIND,'
								;');
					PREPARE Stmt FROM @SqlCmd;
					EXECUTE Stmt;
					DEALLOCATE PREPARE Stmt;
					
				END IF;
			END;
			END IF;
		END;
		END IF; 	
		SET v_group_db_name='';
		SET @v_reg_m=@v_reg_m+1;
	END;
	END WHILE;
	INSERT INTO gt_gw_main.SP_LOG VALUES('gt_global_statistic','SP_Generate_Global_Statistic_Purge_Data',CONCAT(v_DATA_DATE,',',TECH_MASK,',',KIND,' Done: ',TIMESTAMPDIFF(SECOND,START_TIME,SYSDATE()),' seconds.'), NOW());
END$$
DELIMITER ;
