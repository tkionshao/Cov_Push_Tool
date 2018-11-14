CREATE DEFINER=`covmo`@`%` PROCEDURE `SP_Alter_DB`(IN table_name VARCHAR(200),IN col_name VARCHAR(50),IN col_type VARCHAR(20),IN intervaldate TINYINT(4),IN data_date VARCHAR(50),IN alter_type VARCHAR(50),IN col_name_org VARCHAR(50))
BEGIN
	SET SESSION group_concat_max_len = 500000000;
	
	
	SET @SqlCmd=CONCAT('SELECT GROUP_CONCAT(distinct TABLE_SCHEMA SEPARATOR ''|'') into @alter_db FROM information_schema.tables 
			WHERE 1
			AND table_schema LIKE ''gt_%_%_0000_0000'' 
			AND str_to_date(gt_strtok(table_schema,3,''_''),''%Y%m%d'')   between  date_sub(''',data_date,''',INTERVAL ',intervaldate,' day)    and    ''',data_date,'''
			-- AND  table_schema LIKE ''gt_%_%_0000_0000'' 
			;');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
		
	SET @v_i2=1;
	SET @v_R_Max1=gt_covmo_csv_count(@alter_db,'|');
	
	WHILE @v_i2 <= @v_R_Max1 DO
		BEGIN
		SET @v_i2_minus=@v_i2-1;
		SET @db_name=SUBSTRING(SUBSTRING_INDEX(@alter_db,'|',@v_i2),LENGTH(SUBSTRING_INDEX(@alter_db,'|',@v_i2_minus))+ 1);
		SET @db_name=REPLACE(@db_name,'|','');
				select @db_name;
				if alter_type = 'ALTER'
				then
				CALL gt_gw_main.SP_Alter_Column(table_name,col_name,col_type,@db_name);
				ELSEif alter_type = 'RENAME'
				THEN 
				CALL gt_gw_main.`SP_Rename_Column`(table_name,col_name_org,col_name,col_type,@db_name);
				end if;
				SET @v_i2=@v_i2+1; 
		END;
	END WHILE;
	
	
