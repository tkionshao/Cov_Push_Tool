DELIMITER $$
USE `gt_gw_main`$$
DROP PROCEDURE IF EXISTS `SP_CreateDB_LTE`$$
CREATE DEFINER=`covmo`@`%` PROCEDURE `SP_Rename_Column`(IN table_name VARCHAR(100),IN col_name_org VARCHAR(100),IN col_name_new VARCHAR(100),IN col_type VARCHAR(50),IN GT_DB VARCHAR(100))
BEGIN
	SET SESSION group_concat_max_len = 100000000;
	
	SET @SqlCmd=CONCAT('SELECT GROUP_CONCAT(distinct table_name SEPARATOR ''|'') into @rename_table FROM information_schema.COLUMNS 
			WHERE table_schema = ''',GT_DB,''' AND 
			table_name like ''',table_name,'%''
			;');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
	SET @v_i=1;
	SET @v_R_Max=gt_covmo_csv_count(@rename_table,'|');
	
	WHILE @v_i <= @v_R_Max DO
		BEGIN
		SET @v_i_minus=@v_i-1;
		SET @table_name=SUBSTRING(SUBSTRING_INDEX(@rename_table,'|',@v_i),LENGTH(SUBSTRING_INDEX(@rename_table,'|',@v_i_minus))+ 1);
		SET @table_name=REPLACE(@table_name,'|','');
		SET @SqlCmd=CONCAT('SELECT COUNT(DISTINCT table_name)  into  @cnt_table_name FROM information_schema.COLUMNS B
				WHERE table_schema = ''',GT_DB,''' AND table_name  =  ''',@table_name,'''
				AND  EXISTS
				(SELECT  DISTINCT table_name  FROM information_schema.COLUMNS  A
						WHERE table_schema = ''',GT_DB,''' AND table_name  =  ''',@table_name,'''
				AND column_name = ''',col_name_org,'''  AND A.table_name = B.table_name
				)
				;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
	
				IF @cnt_table_name >0 
				THEN
				SET @SqlCmd = CONCAT('
				CREATE TABLE IF NOT EXISTS  ',GT_DB,'.`rename_log` (
				  `TABLE_NAME` VARCHAR(50) NOT NULL,
				  `COLUMN_NAME` VARCHAR(50) DEFAULT NULL,
				  `START_TIME` DATETIME DEFAULT NULL
				) ENGINE=MYISAM DEFAULT CHARSET=utf8;');
				PREPARE stmt FROM @SqlCmd;
				EXECUTE stmt;
				DEALLOCATE PREPARE stmt;
	
				SET @SqlCmd = CONCAT('ALTER TABLE ',GT_DB,'.',@table_name,' CHANGE COLUMN ',col_name_org,' ',col_name_new,' ',col_type,';');
				PREPARE stmt FROM @SqlCmd;
				EXECUTE stmt;
				DEALLOCATE PREPARE stmt;
				
				SET @SqlCmd=CONCAT('INSERT INTO ',GT_DB,'.rename_log 
				SELECT ''',@table_name,''',
					''',col_name_new,''',
					NOW();');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt; 
				
				
	
				END IF;
				
				SET @v_i=@v_i+1; 
				END;
				END WHILE;
	
	
END$$
DELIMITER ;
