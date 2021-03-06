DELIMITER $$
USE `gt_gw_main`$$
DROP PROCEDURE IF EXISTS `SP_CreateDB_LTE`$$
CREATE DEFINER=`covmo`@`%` PROCEDURE `SP_Purge_invalid_Data_LTE`(IN GT_DB VARCHAR(100),IN KIND VARCHAR(50),IN GT_COVMO VARCHAR(100))
BEGIN
	DECLARE START_TIME DATETIME DEFAULT SYSDATE();
	DECLARE DATA_DATE VARCHAR(20) DEFAULT DATE(SUBSTRING(RIGHT(GT_DB,18),1,8)) ;	
	SET SESSION group_concat_max_len = 100000000;
	INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'SP_Purge_Hourly_Data_LTE','delete hourly tables', NOW());
	
	
			SET @SqlCmd=CONCAT('SELECT GROUP_CONCAT(table_name SEPARATOR ''|'') into @delete_str FROM information_schema.TABLES 
			WHERE table_schema = ''',GT_DB,''' AND 
			(table_name like ''rpt_tile_%_dy_%'' OR table_name like ''rpt_cell_tile_%_dy%'')   
			AND table_name not in (''rpt_tile_start'', ''rpt_tile_end'', ''rpt_tile_position'' ,''rpt_cell_tile_start'', ''rpt_cell_tile_end'', ''rpt_cell_tile_position'',''rpt_tile_start_def'', ''rpt_tile_end_def'', ''rpt_tile_position_def'' ,''rpt_cell_tile_start_def'', ''rpt_cell_tile_end_def'', ''rpt_cell_tile_position_def'' )
			;');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
			
				
				IF @delete_str IS NOT NULL
				
				THEN
			
	
				SET @v_i=1;
				SET @v_R_Max=gt_covmo_csv_count(@delete_str,'|');
				WHILE @v_i <= @v_R_Max DO
				BEGIN
					SET @v_i_minus=@v_i-1;
					SET @table_name=SUBSTRING(SUBSTRING_INDEX(@delete_str,'|',@v_i),LENGTH(SUBSTRING_INDEX(@delete_str,'|',@v_i_minus))+ 1);
					SET @table_name=REPLACE(@table_name,'|','');
	
					SET @SqlCmd=CONCAT('delete   from  ',GT_DB,'.',@table_name,'
					where  sub_region_id = -1;');
				
					PREPARE Stmt FROM @SqlCmd;
					EXECUTE Stmt;
					DEALLOCATE PREPARE Stmt; 
	
				SET @v_i=@v_i+1; 
				
				END;
				END WHILE;
	
				ELSE
				SELECT 'no data to delete!' AS Message;
				END IF;
	
	
	
	INSERT INTO gt_gw_main.SP_LOG VALUES(GT_DB,'SP_Purge_Hourly_Data_LTE',CONCAT('Done: ',TIMESTAMPDIFF(SECOND,START_TIME,SYSDATE()),' seconds.'), NOW());
	
END$$
DELIMITER ;
