CREATE DEFINER=`covmo`@`%` PROCEDURE `SP_Sub_Generate_Pre_NBO_Daily`(IN GT_DB VARCHAR(100),IN TABLE_NAME VARCHAR(100),IN TECHNOLOGY VARCHAR(10)
				,IN KEY_STR_COL VARCHAR(1000),IN SUM_STR_COL VARCHAR(1000),IN OTHER_STR_COL VARCHAR(3000))
BEGIN	
	DECLARE START_TIME DATETIME DEFAULT SYSDATE();
	DECLARE COLUMN_STR VARCHAR(1500) DEFAULT 
		'POS_FIRST_RSRP,
		POS_FIRST_RSRP_CNT,
		...sample';
		
	DECLARE COLUMN_IFNULL_STR VARCHAR(5000) DEFAULT 
		'IFNULL(POS_FIRST_RSRP,0) AS POS_FIRST_RSRP,
		IFNULL(POS_FIRST_RSRP_CNT,0) AS POS_FIRST_RSRP_CNT,
		...sample';
		
	DECLARE COLUMN_SUM_STR VARCHAR(5000) DEFAULT 
		'IFNULL(SUM(POS_FIRST_RSRP),0) AS POS_FIRST_RSRP,
		IFNULL(SUM(POS_FIRST_RSRP_CNT),0) AS POS_FIRST_RSRP_CNT,
		...sample';
		
	DECLARE COLUMN_UPD_STR VARCHAR(10000) DEFAULT 
		'RPT_TABLE_NAME.POS_FIRST_RSRP=RPT_TABLE_NAME.POS_FIRST_RSRP+VALUES(POS_FIRST_RSRP),
		RPT_TABLE_NAME.POS_FIRST_RSRP_CNT=RPT_TABLE_NAME.POS_FIRST_RSRP_CNT+VALUES(POS_FIRST_RSRP_CNT),
		...sample';
		
	DECLARE OTHER_STR_COL_IN VARCHAR(10000);
		
	INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'SP_Sub_Generate_Pre_NBO_Daily','Start', START_TIME);
	INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'SP_Sub_Generate_Pre_NBO_Daily','prepare config', NOW());
	
	SET @col_cnt=gt_covmo_csv_count(SUM_STR_COL,'|');
	SET @c_i=1;
	WHILE @c_i <= @col_cnt DO
	BEGIN
		SET @cur_col_name = gt_strtok(SUM_STR_COL, @c_i, '|');
		IF @c_i=1 THEN
			SET COLUMN_STR = @cur_col_name;
			SET COLUMN_IFNULL_STR = CONCAT('IFNULL(',@cur_col_name,',0) AS ',@cur_col_name,'');
			SET COLUMN_SUM_STR = CONCAT('IFNULL(SUM(',@cur_col_name,'),0) AS ',@cur_col_name,'');
			SET COLUMN_UPD_STR = CONCAT('RPT_TABLE_NAME.',@cur_col_name,'=RPT_TABLE_NAME.',@cur_col_name,'+VALUES(',@cur_col_name,')');
		ELSE
			SET COLUMN_STR = CONCAT(COLUMN_STR,',',@cur_col_name);
			SET COLUMN_IFNULL_STR = CONCAT(COLUMN_IFNULL_STR,',','IFNULL(',@cur_col_name,',0) AS ',@cur_col_name,'');
			SET COLUMN_SUM_STR = CONCAT(COLUMN_SUM_STR,',','IFNULL(SUM(',@cur_col_name,'),0) AS ',@cur_col_name,'');
			SET COLUMN_UPD_STR = CONCAT(COLUMN_UPD_STR,',','RPT_TABLE_NAME.',@cur_col_name,'=RPT_TABLE_NAME.',@cur_col_name,'+VALUES(',@cur_col_name,')');
		END IF;
	
		SET @c_i = @c_i + 1;
	END;
	END WHILE;
	SET KEY_STR_COL = REPLACE(KEY_STR_COL,'|',',');
	SET OTHER_STR_COL = REPLACE(OTHER_STR_COL,'|',',');
	SET @FILE_STARTTIME=CONCAT(SUBSTRING(gt_strtok(GT_DB,3,'_'),1,4),'-',SUBSTRING(gt_strtok(GT_DB,3,'_'),5,2),'-',SUBSTRING(gt_strtok(GT_DB,3,'_'),7,2),' ',SUBSTRING(gt_strtok(GT_DB,4,'_'),1,2),':',SUBSTRING(gt_strtok(GT_DB,4,'_'),3,2),':00');
	SET @RNC= gt_strtok(GT_DB,2,'_');
	SET @SqlCmd=CONCAT('SELECT GROUP_CONCAT(session_DB SEPARATOR ''|'') into @session_db_str FROM gt_gw_main.session_information
		WHERE file_starttime <= ''',@FILE_STARTTIME,'''  AND file_starttime > DATE_SUB(''',@FILE_STARTTIME,''',INTERVAL 7 DAY )
		AND RNC = ''',@RNC,''' AND SESSION_TYPE = ''DAY'' AND TECHNOLOGY = ''',TECHNOLOGY,''';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'SP_Sub_Generate_Pre_NBO_Daily','d1', NOW());
	
	SET @rpt_target_table=CONCAT(GT_DB,'.',TABLE_NAME,'_d1');
	SET @rpt_source_table=CONCAT(GT_DB,'.',TABLE_NAME,'');
	
	SET @SqlCmd=CONCAT('TRUNCATE TABLE ',@rpt_target_table,';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	SET @SqlCmd=CONCAT('INSERT INTO ',@rpt_target_table,' 
					(
					',KEY_STR_COL,
					',',COLUMN_STR,
					',',OTHER_STR_COL,')
				SELECT	
					',KEY_STR_COL,
					',',COLUMN_SUM_STR,
					',',OTHER_STR_COL,'
				FROM ',@rpt_source_table,
				' GROUP BY ',KEY_STR_COL,' 
				ORDER BY NULL
				;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @v_i=1;
	SET @v_R_Max=gt_covmo_csv_count(@session_db_str,'|');
	SET @rpt_target_table=CONCAT(GT_DB,'.',TABLE_NAME,'_d7');
	SET @DATA_DATE=CONCAT('''',SUBSTRING(gt_strtok(GT_DB,3,'_'),1,4),'-',SUBSTRING(gt_strtok(GT_DB,3,'_'),5,2),'-',SUBSTRING(gt_strtok(GT_DB,3,'_'),7,2),' 00:00:00'' AS DATA_DATE');
	SET OTHER_STR_COL_IN = REPLACE(OTHER_STR_COL,'DATA_DATE',@DATA_DATE);
	
	SET @SqlCmd=CONCAT('TRUNCATE TABLE ',@rpt_target_table,';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	WHILE @v_i <= @v_R_Max DO
	BEGIN
		SET @session_DB=gt_strtok(@session_db_str, @v_i, '|');
		SET @rpt_source_table=CONCAT(@session_DB,'.',TABLE_NAME,'_d1');
		
		SET @SqlCmd=CONCAT('SELECT COUNT(*) into @db_exists FROM information_schema.SCHEMATA WHERE SCHEMA_NAME = ''',@session_DB,''';');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
	
		SET @SqlCmd=CONCAT('SELECT COUNT(*) into @db_exists2 FROM information_schema.TABLES WHERE TABLE_SCHEMA = ''',@session_DB,''' AND TABLE_NAME = ''',TABLE_NAME,'_d1'';');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;	
		SET @SqlCmd=CONCAT('SELECT COUNT(*) into @col_cnt1 FROM `information_schema`.`COLUMNS` WHERE table_schema = ''',GT_DB,''' AND table_name = ''',TABLE_NAME,'_d7'';');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
	
		SET @SqlCmd=CONCAT('SELECT COUNT(*) into @col_cnt2 FROM `information_schema`.`COLUMNS` WHERE table_schema = ''',@session_DB,''' AND table_name = ''',TABLE_NAME,'_d1'';');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
	
		SET @SqlCmd=CONCAT('SELECT GROUP_CONCAT(DISTINCT CONCAT(COLUMN_NAME) SEPARATOR ''|'' )  into @col_name_1 FROM `information_schema`.`COLUMNS`  WHERE table_schema = ''',GT_DB,''' AND table_name = ''',TABLE_NAME,'_d7'';');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
	
		SET @SqlCmd=CONCAT('SELECT GROUP_CONCAT(DISTINCT CONCAT(COLUMN_NAME) SEPARATOR ''|'' )  into @col_name_2 FROM `information_schema`.`COLUMNS` WHERE table_schema = ''',@session_DB,''' AND table_name = ''',TABLE_NAME,'_d1'';');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
	
		IF  (@db_exists * @db_exists2 > 0) AND (@col_cnt1 - @col_cnt2 = 0) AND (@col_name_1 =  @col_name_2) THEN
			SET @SqlCmd=CONCAT('INSERT INTO ',@rpt_target_table,' 
							(
							',KEY_STR_COL,
							',',COLUMN_STR,
							',',OTHER_STR_COL,')
						SELECT	
							',KEY_STR_COL,
							',',COLUMN_SUM_STR,
							',',OTHER_STR_COL_IN,'
						FROM ',@rpt_source_table,
						' GROUP BY ',KEY_STR_COL,' 
						ORDER BY NULL
						ON DUPLICATE KEY UPDATE
						',@rpt_target_table,'.HO_ALL_CNT=',@rpt_target_table,'.HO_ALL_CNT+VALUES(HO_ALL_CNT),
						',@rpt_target_table,'.HO_SUCC_CNT=',@rpt_target_table,'.HO_SUCC_CNT+VALUES(HO_SUCC_CNT),
						',@rpt_target_table,'.HO_FAIL_CNT=',@rpt_target_table,'.HO_FAIL_CNT+VALUES(HO_FAIL_CNT),
						',@rpt_target_table,'.HO_DROP_CNT=',@rpt_target_table,'.HO_DROP_CNT+VALUES(HO_DROP_CNT),
						',@rpt_target_table,'.MEAS_RXLEV=',@rpt_target_table,'.MEAS_RXLEV+VALUES(MEAS_RXLEV),
						',@rpt_target_table,'.MEAS_CNT=',@rpt_target_table,'.MEAS_CNT+VALUES(MEAS_CNT),
						',@rpt_target_table,'.SERV_RXLEV=',@rpt_target_table,'.SERV_RXLEV+VALUES(SERV_RXLEV),
						',@rpt_target_table,'.SERV_RXLEV_CNT=',@rpt_target_table,'.SERV_RXLEV_CNT+VALUES(SERV_RXLEV_CNT),
						',@rpt_target_table,'.SERV_RXQUAL=',@rpt_target_table,'.SERV_RXQUAL+VALUES(SERV_RXQUAL),
						',@rpt_target_table,'.SERV_RXQUAL_CNT=',@rpt_target_table,'.SERV_RXQUAL_CNT+VALUES(SERV_RXQUAL_CNT)
						;');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
			INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'SP_Sub_Generate_Pre_NBO_Daily',CONCAT('d2-',@session_DB), NOW());
		END IF;
	
		SET @v_i=@v_i+1; 
				
	END;
	END WHILE;
	
	INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'SP_Sub_Generate_Pre_NBO_Daily',CONCAT('Done: ',TIMESTAMPDIFF(SECOND,START_TIME,SYSDATE()),' seconds.'), NOW());		
