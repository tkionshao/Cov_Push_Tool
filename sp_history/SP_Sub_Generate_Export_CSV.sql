DELIMITER $$
USE `gt_gw_main`$$
DROP PROCEDURE IF EXISTS `SP_CreateDB_LTE`$$
CREATE DEFINER=`covmo`@`%` PROCEDURE `SP_Sub_Generate_Export_CSV`(IN DB_NAME VARCHAR(100),IN TBL_NAME VARCHAR(100),IN PATH VARCHAR(100),IN CSV_NAME VARCHAR(100))
BEGIN
	DECLARE START_TIME DATETIME DEFAULT SYSDATE();
	DECLARE GT_DB VARCHAR(100) DEFAULT CONCAT(DB_NAME,'.',TBL_NAME);
        SET SESSION max_heap_table_size = 1024*1024*1024*4; 
        SET SESSION tmp_table_size = 1024*1024*1024*6; 
        SET SESSION join_buffer_size = 1024*1024*1024*1; 
        SET SESSION sort_buffer_size = 1024*1024*1024*1; 
        SET SESSION read_buffer_size = 1024*1024*1024*1; 
	
	SET @SqlCmd=CONCAT('
			SELECT CONCAT('''''''',REPLACE(GROUP_CONCAT(column_name),',''','',''',''''',''''''),'''''''') INTO @col_str
			FROM information_schema.COLUMNS
			WHERE table_schema = ''',DB_NAME,''' AND table_name = ''',TBL_NAME,'''
			ORDER BY ordinal_position;
			    ;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'SP_Sub_Generate_Export_CSV','Insert data to table_tile_fp_15-Start ', NOW());
	
	SET @SqlCmd=CONCAT('SELECT ',@col_str,'
			    UNION 
			    SELECT *
			    FROM ',DB_NAME,'.',TBL_NAME,'
			    INTO OUTFILE ''',PATH,'/',CSV_NAME,'''
			    FIELDS TERMINATED BY '',''
			    LINES TERMINATED BY ''\\n''
			    ;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	INSERT INTO gt_gw_main.SP_LOG VALUES(GT_DB,'SP_Sub_Generate_Export_CSV',CONCAT('Done: ',TIMESTAMPDIFF(SECOND,START_TIME,SYSDATE()),' seconds.'), NOW());	
	
        SET SESSION max_heap_table_size = 1024*1024*128; 
        SET SESSION tmp_table_size = 1024*1024*128; 
        SET SESSION join_buffer_size = 1024*1024*128; 
        SET SESSION sort_buffer_size = 1024*1024*128; 
        SET SESSION read_buffer_size = 1024*1024*128; 
	
END$$
DELIMITER ;
