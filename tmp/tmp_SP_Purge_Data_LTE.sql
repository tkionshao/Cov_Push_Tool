CREATE DEFINER=`covmo`@`%` PROCEDURE `SP_Purge_Data_LTE`(IN GT_DB VARCHAR(100),IN KIND VARCHAR(20))
BEGIN
	DECLARE START_TIME DATETIME DEFAULT SYSDATE();
	DECLARE FILEDATE VARCHAR(18) DEFAULT RIGHT(GT_DB,18);
	DECLARE FILE_ENDTIME VARCHAR(20) DEFAULT CONCAT(LEFT(FILEDATE,4),'-',SUBSTRING(FILEDATE,5,2),'-',SUBSTRING(FILEDATE,7,2),' ', SUBSTRING(FILEDATE,15,2),':',SUBSTRING(FILEDATE,17,2),':00');
	SET FILE_ENDTIME=REPLACE(FILE_ENDTIME,'00:00:00','23:59:59') ;	
	SET SESSION group_concat_max_len = 10000000;
	INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'SP_Purge_Data_LTE','Truncate hourly tables', NOW());
	
	
	IF KIND = 'AP' THEN 
	
	
			SET @SqlCmd=CONCAT('SELECT GROUP_CONCAT(TABLE_NAME SEPARATOR ''|'' ) INTO @PRG_STR 
						FROM information_schema.TABLES
						WHERE TABLE_SCHEMA = ',GT_DB,' AND TABLE_NAME IN(antenna_info,nt_antenna_current_lte)
						;');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
			
			SET @v_i=1;
			SET @v_R_Max=gt_covmo_csv_count(@PRG_STR,'|');
			WHILE @v_i <= @v_R_Max DO
			BEGIN
				SET @table_name:=gt_strtok(@PRG_STR, @v_i, '|');
				SET @SqlCmd=CONCAT('DROP TABLE IF EXISTS ',GT_DB,'.',@table_name,';');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				SET @v_i=@v_i+1; 
			END;
			END WHILE;
			IF @v_R_Max>0 THEN 
				SET @SqlCmd=CONCAT('DELETE FROM ',GT_DB,'.`nt_cell_current_lte`;');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
			END IF;
	
	
	ELSEIF KIND = 'GW' THEN
	
	SET @SqlCmd=CONCAT(' TRUNCATE TABLE ',GT_DB,'.rpt_cell_lte;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt; 
	
	SET @SqlCmd=CONCAT('DELETE FROM gt_gw_main.table_call_cnt WHERE DATA_DATE = ''',FILEDATE,''';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	
	
	
	END IF;
	
	INSERT INTO gt_gw_main.SP_LOG VALUES(GT_DB,'SP_Purge_Data_LTE',CONCAT('Done: ',TIMESTAMPDIFF(SECOND,START_TIME,SYSDATE()),' seconds.'), NOW());
	
