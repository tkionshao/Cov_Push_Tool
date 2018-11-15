DELIMITER $$
USE `gt_gw_main`$$
DROP PROCEDURE IF EXISTS `SP_CreateDB_LTE`$$
CREATE DEFINER=`covmo`@`%` PROCEDURE `SP_Purge_Hourly_Data_GSM`(IN GT_DB VARCHAR(100),IN KIND VARCHAR(50),IN GT_COVMO VARCHAR(100))
BEGIN
	DECLARE START_TIME DATETIME DEFAULT SYSDATE();
	DECLARE DATA_DATE VARCHAR(20) DEFAULT DATE(SUBSTRING(RIGHT(GT_DB,18),1,8));
	DECLARE PU_ID VARCHAR(100) DEFAULT gt_strtok(GT_DB,2,'_');
	SET @@session.group_concat_max_len = @@global.max_allowed_packet;
	
	INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'SP_Purge_Hourly_Data_GSM','delete hourly tables', NOW());
	
	IF KIND = 'AP' THEN 
		SELECT COUNT(*) INTO @CNT FROM `information_schema`.`SCHEMATA` WHERE `SCHEMA_NAME` = GT_DB;
		IF @CNT =0 THEN
			SET @SqlCmd=CONCAT('DELETE FROM ',GT_COVMO,'.table_call_cnt WHERE DATA_DATE = ''',DATA_DATE,''' AND PU_ID= ''',PU_ID,''';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
			
			SET @SqlCmd=CONCAT('DELETE FROM gt_gw_main.table_call_cnt WHERE DATA_DATE = ''',DATA_DATE,''' AND PU_ID= ''',PU_ID,''';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
		
			INSERT INTO gt_gw_main.sp_log VALUES('gt_gw_main','SP_Purge_Hourly_Data_GSM',CONCAT('DELETE ',KIND,' table_call_cnt WHERE DATA_DATE = ''',DATA_DATE,''' AND PU_ID= ''',PU_ID,''''), NOW());	
		ELSE
			SET @SqlCmd=CONCAT('SELECT GROUP_CONCAT(table_name SEPARATOR ''|'') into @delete_str FROM information_schema.TABLES 
			WHERE table_schema = ''',GT_DB,''' AND 
			(table_name like ''table_tile_dominant_cell_gsm%'' OR table_name like ''%table_tile_start_gsm%'' 
			OR table_name like ''%rpt_cell_relation_gsm%'' OR table_name like ''%table_call_gsm_dump%'' 
			OR table_name like ''%table_call_nopos_gsm%'' OR table_name like ''%table_position_gsm%''			
			OR table_name like ''%table_tile_end_gsm%'' OR table_name like ''%table_tile_ue_gsm%''
			OR table_name like ''%table_tile_fp_gsm%''  OR table_name like ''%table_call_gsm%'' 
			OR table_name like ''%table_call_imsi_gsm%'' OR table_name like ''%table_imsi%'' 
			OR table_name like ''%table_roamer_call_gsm%'' OR table_name like ''%session_information%''
			OR table_name like ''%table_call_cnt%'')  
			AND table_name NOT like ''%dy%'' AND `ENGINE`<>''MRG_MYISAM''
			;');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
			
			IF @delete_str IS NOT NULL THEN
				SET @v_i=1;
				SET @v_R_Max=gt_covmo_csv_count(@delete_str,'|');
				WHILE @v_i <= @v_R_Max DO
				BEGIN
					SET @v_i_minus=@v_i-1;
					SET @table_name=SUBSTRING(SUBSTRING_INDEX(@delete_str,'|',@v_i),LENGTH(SUBSTRING_INDEX(@delete_str,'|',@v_i_minus))+ 1);
					SET @table_name=REPLACE(@table_name,'|','');
					SET @SqlCmd=CONCAT('TRUNCATE  ',GT_DB,'.',@table_name,';');
					PREPARE Stmt FROM @SqlCmd;
					EXECUTE Stmt;
					DEALLOCATE PREPARE Stmt;
					SET @v_i=@v_i+1; 
				END;
				END WHILE;
				INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'SP_Purge_Hourly_Data_GSM',CONCAT('DELETE ',KIND,' hourly table WHERE DATA_DATE = ''',DATA_DATE,''' AND PU_ID= ''',PU_ID,''''), NOW());
		
				SET @SqlCmd=CONCAT('DELETE FROM ',GT_COVMO,'.table_call_cnt WHERE DATA_DATE = ''',DATA_DATE,''' AND PU_ID= ''',PU_ID,''';');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				
				SET @SqlCmd=CONCAT('DELETE FROM gt_gw_main.table_call_cnt WHERE DATA_DATE = ''',DATA_DATE,''' AND PU_ID= ''',PU_ID,''';');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
			ELSE
				SELECT 'no data to delete!' AS Message;
			END IF;
		END IF;
	ELSEIF KIND = 'DAILY' THEN
		SELECT COUNT(*) INTO @CNT FROM `information_schema`.`SCHEMATA` WHERE `SCHEMA_NAME` = GT_DB;
		IF @CNT =0 THEN
			SET @SqlCmd=CONCAT('DELETE FROM ',GT_COVMO,'.table_call_cnt WHERE DATA_DATE = ''',DATA_DATE,''' AND PU_ID= ''',PU_ID,''';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT('DELETE FROM gt_gw_main.table_call_cnt WHERE DATA_DATE = ''',DATA_DATE,''' AND PU_ID= ''',PU_ID,''';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
			
			INSERT INTO gt_gw_main.sp_log VALUES('gt_gw_main','SP_Purge_Hourly_Data_GSM',CONCAT('DELETE ',KIND,' table_call_cnt WHERE DATA_DATE = ''',DATA_DATE,''' AND PU_ID= ''',PU_ID,''''), NOW());
		ELSE
			SET @SqlCmd=CONCAT('SELECT GROUP_CONCAT(table_name SEPARATOR ''|'') into @delete_str FROM information_schema.TABLES 
			WHERE table_schema = ''',GT_DB,''' AND 
			(table_name like ''table_tile_dominant_cell_gsm%'' OR table_name like ''%table_tile_start_gsm%'' 
			OR table_name like ''%rpt_cell_relation_gsm%'' OR table_name like ''%table_call_gsm_dump%'' 
			OR table_name like ''%table_call_nopos_gsm%'' OR table_name like ''%table_position_gsm%''			
			OR table_name like ''%table_tile_end_gsm%'' OR table_name like ''%table_tile_ue_gsm%''
			OR table_name like ''%table_tile_fp_gsm%''  OR table_name like ''%table_call_gsm%'' 
			OR table_name like ''%table_call_imsi_gsm%'' OR table_name like ''%table_imsi%'' 
			OR table_name like ''%table_roamer_call_gsm%'' OR table_name like ''%session_information%''
			OR table_name like ''%table_call_cnt%'')  
			AND table_name NOT like ''%dy%'' AND `ENGINE`<>''MRG_MYISAM'' 
			;');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
				
			IF @delete_str IS NOT NULL THEN
				SET @v_i=1;
				SET @v_R_Max=gt_covmo_csv_count(@delete_str,'|');
				WHILE @v_i <= @v_R_Max DO
				BEGIN
					SET @v_i_minus=@v_i-1;
					SET @table_name=SUBSTRING(SUBSTRING_INDEX(@delete_str,'|',@v_i),LENGTH(SUBSTRING_INDEX(@delete_str,'|',@v_i_minus))+ 1);
					SET @table_name=REPLACE(@table_name,'|','');
					SET @SqlCmd=CONCAT('TRUNCATE  ',GT_DB,'.',@table_name,';');
					PREPARE Stmt FROM @SqlCmd;
					EXECUTE Stmt;
					DEALLOCATE PREPARE Stmt;
					SET @v_i=@v_i+1; 
				END;
				END WHILE;
				
				INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'SP_Purge_Hourly_Data_GSM',CONCAT('DELETE ',KIND,' hourly table WHERE DATA_DATE = ''',DATA_DATE,''' AND PU_ID= ''',PU_ID,''''), NOW());
				SET @SqlCmd=CONCAT('DELETE FROM ',GT_COVMO,'.table_call_cnt WHERE DATA_DATE = ''',DATA_DATE,''' AND PU_ID= ''',PU_ID,''';');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				
				SET @SqlCmd=CONCAT('DELETE FROM gt_gw_main.table_call_cnt WHERE DATA_DATE = ''',DATA_DATE,''' AND PU_ID= ''',PU_ID,''';');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				
				INSERT INTO gt_gw_main.sp_log VALUES('gt_gw_main','SP_Purge_Hourly_Data_GSM',CONCAT('DELETE ',KIND,' table_call_cnt WHERE DATA_DATE = ''',DATA_DATE,''' AND PU_ID= ''',PU_ID,''''), NOW());
			ELSE
				SELECT 'no data to delete!' AS Message;
			END IF;
		END IF;
	END IF;
	
	INSERT INTO gt_gw_main.SP_LOG VALUES(GT_DB,'SP_Purge_Hourly_Data_GSM',CONCAT('Done: ',TIMESTAMPDIFF(SECOND,START_TIME,SYSDATE()),' seconds.'), NOW());
END$$
DELIMITER ;
