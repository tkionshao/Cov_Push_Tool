CREATE DEFINER=`covmo`@`%` PROCEDURE `SP_Generate_Global_Statistic_db_cp_test`(IN NW_IP VARCHAR(17),IN NW_PORT SMALLINT(6),IN NW_USER VARCHAR(32),IN NW_PASSWORD VARCHAR(32),IN GROUP_DB_NAME VARCHAR(100),IN GT_AGGREGATE_DB VARCHAR(100),IN SOURCE_TABLE_NAME VARCHAR(100),IN WORKER_ID VARCHAR(10))
BEGIN
	DECLARE v_i SMALLINT DEFAULT 1;	
	SET @flush_table='';
	SET @table_name='';
	SET @v_Max=gt_covmo_csv_count(SOURCE_TABLE_NAME,',');
	
	WHILE v_i <= @v_Max DO
	BEGIN
		SET @table_name=gt_covmo_csv_get(SOURCE_TABLE_NAME,v_i);
		SET @lock_str=CONCAT('mysql -h',NW_IP,' -P',NW_PORT,' -u',NW_USER,' -p',NW_PASSWORD,' -e''LOCK TABLES ',GROUP_DB_NAME,'.',@table_name,' WRITE;''');
		SELECT sys_exec(@lock_str) INTO @aa;
		
		SET @SqlCmd=CONCAT('FLUSH TABLES ',GROUP_DB_NAME,'.',@table_name,';');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		
		SET @SqlCmd=CONCAT('DROP TABLE IF EXISTS ',GT_AGGREGATE_DB,'.',@table_name,';');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		
		SELECT @@datadir INTO @SPIDER_DATA_DIR;
		SET @cp_str=CONCAT('cp -fp ',@SPIDER_DATA_DIR,GROUP_DB_NAME,'/',@table_name,'* /dev/shm/',GT_AGGREGATE_DB,'/');
		SELECT sys_exec(@cp_str) INTO @aa;
		IF v_i=1 THEN 
			SET @flush_table=CONCAT(GT_AGGREGATE_DB,'.',@table_name);
		ELSE 
			SET @flush_table=CONCAT(@flush_table,',',GT_AGGREGATE_DB,'.',@table_name);
		END IF;
		
		SET v_i = v_i + 1;
	END;
	END WHILE;
	
	SET @unlock_str=CONCAT('mysql -h',NW_IP,' -P',NW_PORT,' -u',NW_USER,' -p',NW_PASSWORD,' -e''UNLOCK TABLES;''');
	SELECT sys_exec(@unlock_str) INTO @aa;
	SET @SqlCmd=CONCAT('FLUSH TABLES ',@flush_table,';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
		
			
