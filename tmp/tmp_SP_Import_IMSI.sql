CREATE DEFINER=`covmo`@`%` PROCEDURE `SP_Import_IMSI`()
do_nothing:
BEGIN	
	DECLARE WORKER_ID VARCHAR(10) DEFAULT CONNECTION_ID();
	DECLARE START_TIME DATETIME DEFAULT SYSDATE();
	DECLARE STEP_START_TIME DATETIME DEFAULT SYSDATE();
	DECLARE EXECUTE_TIME DATETIME DEFAULT SYSDATE();
	DECLARE exDate DATE;
	DECLARE GT_DB VARCHAR(20) DEFAULT 'gt_global_imsi';
	
	DECLARE EXIT HANDLER FOR SQLEXCEPTION
	BEGIN
		SET @SqlCmd=CONCAT('TRUNCATE TABLE ',GT_DB,'.table_running_task;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		INSERT INTO gt_gw_main.SP_LOG VALUES(GT_DB,'SP_Import_IMSI',CONCAT(WORKER_ID,' SQLEXCEPTION cost:',TIMESTAMPDIFF(SECOND,START_TIME,SYSDATE()),' sec.'), NOW());
	END;
	SET SESSION group_concat_max_len=1024000; 
	INSERT INTO gt_gw_main.SP_LOG VALUES(GT_DB,'SP_Import_IMSI',CONCAT(WORKER_ID,' START'), START_TIME);
	SET @SqlCmd=CONCAT('INSERT INTO `gt_global_imsi`.table_running_task 
				(`EXECUTE_TIME`,`WORKER_ID`)
				VALUES(''',EXECUTE_TIME,''',',WORKER_ID,');');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS ',GT_DB,'.tmp_diff_table_date;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('CREATE TEMPORARY TABLE ',GT_DB,'.tmp_diff_table_date AS 
				SELECT TABLE_NAME AS DIFF_TABLE_NAME,gt_strtok(TABLE_NAME,5,''_'') AS DIFF_DATE FROM information_schema.`TABLES`
				WHERE TABLE_SCHEMA=''',GT_DB,''' AND TABLE_NAME LIKE ''table_imsi_diff_%'' AND TABLE_ROWS IS NOT NULL
			;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('SELECT COUNT(*) INTO @DIFF_CNT FROM ',GT_DB,'.tmp_diff_table_date;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	IF @DIFF_CNT>0 THEN 
		SET @SqlCmd=CONCAT('SELECT `IN_QUOTE`(GROUP_CONCAT(RIGHT(PARTITION_NAME,8) SEPARATOR '','' )) INTO @H_DATE
					FROM information_schema.`PARTITIONS`
					WHERE TABLE_SCHEMA=''',GT_DB,''' AND TABLE_NAME=''table_imsi_pu'';');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		
		SET @SqlCmd=CONCAT('SELECT GROUP_CONCAT(DISTINCT DIFF_DATE SEPARATOR ''|'') INTO @DATE_STR
					FROM ',GT_DB,'.tmp_diff_table_date 
					WHERE DIFF_DATE NOT IN ',@H_DATE,'
					ORDER BY DIFF_DATE;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		IF @DATE_STR <> '' THEN 
			SET @v_i=1;
			SET @v_R_Max=gt_covmo_csv_count(@DATE_STR,'|');
			WHILE @v_i <= @v_R_Max DO
			BEGIN
				SET @DATA_DATE:=CONCAT(SUBSTRING(gt_strtok(@DATE_STR, @v_i, '|'),1,4),'-',SUBSTRING(gt_strtok(@DATE_STR, @v_i, '|'),5,2),'-',SUBSTRING(gt_strtok(@DATE_STR, @v_i, '|'),7,2));
				SET @DATA_DATE:=DATE_FORMAT(ADDDATE(@DATA_DATE,1),GET_FORMAT(DATE,'ISO'));
				SET @SqlCmd=CONCAT('ALTER TABLE ',GT_DB,'.table_imsi_pu ADD PARTITION (PARTITION p',gt_strtok(@DATE_STR, @v_i, '|'),' VALUES LESS THAN(UNIX_TIMESTAMP(''',@DATA_DATE,''')));');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt; 	
			
				SET @v_i=@v_i+1;
			END;
			END WHILE;
		END IF;
		
		SET @SqlCmd=CONCAT('SELECT GROUP_CONCAT(REPLACE(DIFF_TABLE_NAME,''table_imsi_diff_'','''') SEPARATOR ''|'' ) INTO @PU_STR
					FROM ',GT_DB,'.tmp_diff_table_date;
					;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		
		SET @j_i=1;
		SET @j_R_Max=gt_covmo_csv_count(@PU_STR,'|');
		WHILE @j_i <= @j_R_Max DO
		BEGIN
			SET @SqlCmd=CONCAT('INSERT IGNORE INTO `gt_global_imsi`.`table_imsi_pu`
						(`IMSI`,`DATA_DATE`,`PU_ID`,`DATA_DATE_TS`,`TECH_MASK`)
						SELECT `IMSI`,`DATA_DATE`,`PU_ID`,`DATA_DATE_TS`,`TECH_MASK`
						FROM ',GT_DB,'.table_imsi_diff_',SPLIT_STR(@PU_STR,'|',@j_i),';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt; 
			INSERT INTO gt_gw_main.SP_LOG VALUES(GT_DB,'SP_Import_IMSI',CONCAT('INSERT table ',GT_DB,'.table_imsi_diff_',SPLIT_STR(@PU_STR,'|',@j_i),' cost:',TIMESTAMPDIFF(SECOND,STEP_START_TIME,SYSDATE()),' sec.'), NOW());
			
			SET @SqlCmd=CONCAT('DROP TABLE IF EXISTS ',GT_DB,'.table_imsi_diff_',SPLIT_STR(@PU_STR,'|',@j_i),';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt; 
			INSERT INTO gt_gw_main.SP_LOG VALUES(GT_DB,'SP_Import_IMSI',CONCAT('DROP table ',GT_DB,'.table_imsi_diff_',SPLIT_STR(@PU_STR,'|',@j_i),' cost:',TIMESTAMPDIFF(SECOND,STEP_START_TIME,SYSDATE()),' sec.'), NOW());
			SET STEP_START_TIME := SYSDATE();
			SET @j_i=@j_i+1;
		END;
		END WHILE;
		
		SET @SqlCmd=CONCAT('TRUNCATE TABLE ',GT_DB,'.table_running_task;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;INSERT INTO gt_gw_main.SP_LOG VALUES('gt_global_imsi','SP_Import_IMSI',CONCAT(WORKER_ID,' Done cost:',TIMESTAMPDIFF(SECOND,START_TIME,SYSDATE()),' sec.'), NOW());
	ELSE 
		SET @SqlCmd=CONCAT('TRUNCATE TABLE ',GT_DB,'.table_running_task;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		INSERT INTO gt_gw_main.SP_LOG VALUES('gt_global_imsi','SP_Import_IMSI',CONCAT(WORKER_ID,' Done cost:',TIMESTAMPDIFF(SECOND,START_TIME,SYSDATE()),' sec.'), NOW());
		LEAVE do_nothing;
	END IF;
	
