CREATE DEFINER=`covmo`@`%` PROCEDURE `SP_Alter_Column`(IN table_name VARCHAR(100),IN col_name VARCHAR(100),IN col_type VARCHAR(50),IN GT_DB VARCHAR(100))
BEGIN
	SET SESSION group_concat_max_len = 100000000;
	
	SET @SqlCmd=CONCAT('SELECT GROUP_CONCAT(distinct table_name SEPARATOR ''|'') into @alter_table FROM information_schema.COLUMNS 
			WHERE table_schema = ''',GT_DB,''' AND 
			table_name like ''',table_name,'%''
			;');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
	SET @v_i=1;
	SET @v_R_Max=gt_covmo_csv_count(@alter_table,'|');
	
	WHILE @v_i <= @v_R_Max DO
		BEGIN
		SET @v_i_minus=@v_i-1;
		SET @table_name=SUBSTRING(SUBSTRING_INDEX(@alter_table,'|',@v_i),LENGTH(SUBSTRING_INDEX(@alter_table,'|',@v_i_minus))+ 1);
		SET @table_name=REPLACE(@table_name,'|','');
		SET @SqlCmd=CONCAT('SELECT COUNT(DISTINCT table_name)  into  @cnt_table_name FROM information_schema.COLUMNS B
				WHERE table_schema = ''',GT_DB,''' AND table_name  =  ''',@table_name,'''
				AND NOT EXISTS
				(SELECT  DISTINCT table_name  FROM information_schema.COLUMNS  A
						WHERE table_schema = ''',GT_DB,''' AND table_name  =  ''',@table_name,'''
				AND column_name = ''',col_name,'''  AND A.table_name = B.table_name
				)
				;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
	
				IF @cnt_table_name >0 
				THEN
				SET @SqlCmd = CONCAT('
				CREATE TABLE IF NOT EXISTS  ',GT_DB,'.`alter_log` (
				  `TABLE_NAME` VARCHAR(50) NOT NULL,
				  `COLUMN_NAME` VARCHAR(50) DEFAULT NULL,
				  `START_TIME` DATETIME DEFAULT NULL
				) ENGINE=MYISAM DEFAULT CHARSET=utf8;');
				PREPARE stmt FROM @SqlCmd;
				EXECUTE stmt;
				DEALLOCATE PREPARE stmt;
	
				SET @SqlCmd = CONCAT('ALTER TABLE ',GT_DB,'.',@table_name,' ADD ',col_name,' ',col_type,';');
				PREPARE stmt FROM @SqlCmd;
				EXECUTE stmt;
				DEALLOCATE PREPARE stmt;
				
	
				IF col_name IN ('POS_FIRST_LAT')
				then
	
				SET @SqlCmd = CONCAT('UPDATE ',GT_DB,'.',@table_name,' SET 
				POS_FIRST_LAT=
				gt_covmo_proj_geohash_to_lat(POS_FIRST_LOC);');
				PREPARE stmt FROM @SqlCmd;
				EXECUTE stmt;
				DEALLOCATE PREPARE stmt;
	
				end if;
	
	
				IF col_name IN ('POS_FIRST_LON')
				THEN
	
				SET @SqlCmd = CONCAT('UPDATE ',GT_DB,'.',@table_name,' SET 
				POS_FIRST_LON=
				gt_covmo_proj_geohash_to_lng(POS_FIRST_LOC);');
				PREPARE stmt FROM @SqlCmd;
				EXECUTE stmt;
				DEALLOCATE PREPARE stmt;
	
				END IF;
	
				SET @SqlCmd=CONCAT('INSERT INTO ',GT_DB,'.alter_log 
				SELECT ''',@table_name,''',
					''',col_name,''',
					NOW();');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt; 
				
				
	
				END IF;
				
				SET @v_i=@v_i+1; 
				END;
				END WHILE;
	
	
