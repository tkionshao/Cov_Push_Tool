CREATE DEFINER=`covmo`@`%` PROCEDURE `SP_Sub_Generate_Pre_NBO_LTE_Daily`(IN gt_db VARCHAR(100),IN GT_COVMO VARCHAR(100),IN table_name VARCHAR(100))
BEGIN	
	DECLARE START_TIME DATETIME DEFAULT SYSDATE();
	DECLARE SH_EH VARCHAR(9) DEFAULT RIGHT(GT_DB,9);
	DECLARE NBO_INTER_INTRA_KEY_STR VARCHAR(1000) DEFAULT 
		'
		`DATA_DATE`,
		`SUB_REGION_ID`,
		`EUTRABAND`,
		`EARFCN`,
		`ENODEB_ID`,
		`CELL_ID`,
		`CELL_NAME`,
		`NBR_ENODEB_ID`,
		`NBR_CELL_ID`,
		`NBR_CELL_NAME`,
		`NBR_TYPE`';
	
	DECLARE NBO_INTER_INTRA_COL_STR VARCHAR(1500) DEFAULT 
		'
		DISTANCE_METER,
		HO_COUNT,
		PINGPONG_HO_CNT,
		HO_FAIL_CNT,
		HO_RE_EST_CNT,
		TA_HO_SUM,
		TA_HO_CNT,
		MEAS_COUNT,
		RSRP_SUM,
		RSRQ_SUM,
		RSRP_CNT,
		RSRQ_CNT,
		BEST_MEAS_CNT,
		SERVING_RSRP_SUM,
		SERVING_RSRQ_SUM,
		SERVING_RSRP_CNT,
		SERVING_RSRQ_CNT,
		HO_INTERRUP_TIME,
		HO_INTERRUP_CNT';
	
	
	DECLARE NBO_INTER_INTRA_SUM_STR VARCHAR(2500) DEFAULT 
		'
		SUM(IFNULL(DISTANCE_METER,0)) AS DISTANCE_METER,
		SUM(IFNULL(HO_COUNT,0)) AS HO_COUNT,
		SUM(IFNULL(PINGPONG_HO_CNT,0)) AS PINGPONG_HO_CNT,
		SUM(IFNULL(HO_FAIL_CNT,0)) AS HO_FAIL_CNT,
		SUM(IFNULL(HO_RE_EST_CNT,0)) AS HO_RE_EST_CNT,
		SUM(IFNULL(TA_HO_SUM,0)) AS TA_HO_SUM,
		SUM(IFNULL(TA_HO_CNT,0)) AS TA_HO_CNT,
		SUM(IFNULL(MEAS_COUNT,0)) AS MEAS_COUNT,
		SUM(IFNULL(RSRP_SUM,0)) AS RSRP_SUM,
		SUM(IFNULL(RSRQ_SUM,0)) AS RSRQ_SUM,
		SUM(IFNULL(RSRP_CNT,0)) AS RSRP_CNT,
		SUM(IFNULL(RSRQ_CNT,0)) AS RSRQ_CNT,
		SUM(IFNULL(BEST_MEAS_CNT,0)) AS BEST_MEAS_CNT,
		SUM(IFNULL(SERVING_RSRP_SUM,0)) AS SERVING_RSRP_SUM,
		SUM(IFNULL(SERVING_RSRQ_SUM,0)) AS SERVING_RSRQ_SUM,
		SUM(IFNULL(SERVING_RSRP_CNT,0)) AS SERVING_RSRP_CNT,
		SUM(IFNULL(SERVING_RSRQ_CNT,0)) AS SERVING_RSRQ_CNT,
		SUM(IFNULL(HO_INTERRUP_TIME,0)) AS HO_INTERRUP_TIME,
		SUM(IFNULL(HO_INTERRUP_CNT,0)) AS HO_INTERRUP_CNT';
	
	
	SELECT REPLACE(GT_DB,SH_EH,'0000_0000') INTO GT_DB;
	SET @FILE_STARTTIME=CONCAT(SUBSTRING(gt_strtok(GT_DB,3,'_'),1,4),'-',SUBSTRING(gt_strtok(GT_DB,3,'_'),5,2),'-',SUBSTRING(gt_strtok(GT_DB,3,'_'),7,2),' ',SUBSTRING(gt_strtok(GT_DB,4,'_'),1,2),':',SUBSTRING(gt_strtok(GT_DB,4,'_'),3,2),':00');
	SET @rnc= gt_strtok(GT_DB,2,'_');		 
	INSERT INTO gt_gw_main.SP_LOG VALUES(GT_DB,' SP_Sub_Generate_Pre_NBO_LTE_Daily','Start', START_TIME);	
	
	IF table_name = 'opt_nbr_inter_intra_lte'
			THEN
			
			SET @SqlCmd=CONCAT('TRUNCATE TABLE ',GT_DB,'.',table_name,'_d1;');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt; 
	
			SET @SqlCmd=CONCAT('INSERT INTO ',GT_DB,'.',table_name,'_d1(',NBO_INTER_INTRA_KEY_STR,',',NBO_INTER_INTRA_COL_STR,')
					SELECT 
					',NBO_INTER_INTRA_KEY_STR,',
					',NBO_INTER_INTRA_SUM_STR,'
					FROM ',GT_DB,'.opt_nbr_inter_intra_lte_dy
					GROUP BY ',NBO_INTER_INTRA_KEY_STR,';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt; 
			
			
			SET @SqlCmd=CONCAT('DROP TABLE IF EXISTS  ',GT_DB,'.`tmp_',table_name,'_d1`;'); 
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
			
			SET @SqlCmd=CONCAT('CREATE TABLE ',GT_DB,'.`tmp_',table_name,'_d1` LIKE ',GT_DB,'.`',table_name,'_d1`;'); 
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
			
			
			SET @SqlCmd=CONCAT('SELECT GROUP_CONCAT(session_DB SEPARATOR ''|'') into @session_db_str FROM gt_gw_main.session_information A  RIGHT JOIN information_schema.TABLES B
			ON A.session_db = B.table_schema
			WHERE A.file_starttime <= ''',@FILE_STARTTIME,'''  AND A.file_starttime >=  DATE_SUB(''',@FILE_STARTTIME,''',INTERVAL 3 DAY )
			AND A.rnc = ''',@rnc,''' AND B.table_name = ''',table_name,''';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
			
			
			SET @v_i=1;
			SET @v_R_Max=gt_covmo_csv_count(@session_db_str,'|');
			WHILE @v_i <= @v_R_Max DO
			BEGIN
			SET @v_i_minus=@v_i-1;
			SET @session_DB=SUBSTRING(SUBSTRING_INDEX(@session_db_str,'|',@v_i),LENGTH(SUBSTRING_INDEX(@session_db_str,'|',@v_i_minus))+ 1);
			SET @session_DB=REPLACE(@session_DB,'|','');
			
			
			SET @SqlCmd=CONCAT('SELECT COUNT(*)  into @db_exists FROM information_schema.SCHEMATA WHERE SCHEMA_NAME = ''',@session_DB,''';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
			
				IF  @db_exists = 0
				
					THEN
						SELECT 1;
					ELSE
					
					
					SET @SqlCmd=CONCAT('CREATE TABLE 
					if  not exists ',@session_DB,'.',table_name,'_d1
					SELECT 
						',NBO_INTER_INTRA_KEY_STR,',
						',NBO_INTER_INTRA_COL_STR,'
						FROM ',@session_DB,'.opt_nbr_inter_intra_lte_dy
						GROUP BY ',NBO_INTER_INTRA_KEY_STR,';'); 
					
					PREPARE Stmt FROM @SqlCmd;
					EXECUTE Stmt;
					DEALLOCATE PREPARE Stmt;
					
							
					SET @SqlCmd=CONCAT('insert into   ',GT_DB,'.`tmp_',table_name,'_d1`
					(',NBO_INTER_INTRA_KEY_STR,',',NBO_INTER_INTRA_COL_STR,')
					SELECT 
						',NBO_INTER_INTRA_KEY_STR,',
						',NBO_INTER_INTRA_COL_STR,'
					from  ',@session_DB,'.',table_name,'_d1
					;');
					PREPARE Stmt FROM @SqlCmd;
					EXECUTE Stmt;
					DEALLOCATE PREPARE Stmt;
			
				END IF;
			
				SET @v_i=@v_i+1; 
						
				
				END;
				END WHILE;
			SET @SqlCmd=CONCAT('TRUNCATE TABLE ',GT_DB,'.`',table_name,'_d7`;');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
			
			SET @SqlCmd=CONCAT('insert into   ',GT_DB,'.`',table_name,'_d7`
				(',NBO_INTER_INTRA_KEY_STR,',',NBO_INTER_INTRA_COL_STR,')
				SELECT
				',NBO_INTER_INTRA_KEY_STR,',
				',NBO_INTER_INTRA_SUM_STR,'
				from  ',GT_DB,'.`tmp_',table_name,'_d1`
				GROUP BY ',NBO_INTER_INTRA_KEY_STR,';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
			
			SET @SqlCmd=CONCAT('DROP TABLE IF EXISTS  ',GT_DB,'.`tmp_',table_name,'_d1`;'); 
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
	END IF;
	
	INSERT INTO gt_gw_main.SP_LOG VALUES(GT_DB,' SP_Sub_Generate_Pre_NBO_LTE_Daily',CONCAT('Done: ',TIMESTAMPDIFF(SECOND,START_TIME,SYSDATE()),' seconds.'), NOW());		
