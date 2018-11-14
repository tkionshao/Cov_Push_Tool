CREATE DEFINER=`covmo`@`%` PROCEDURE `SP_Sub_Generate_Pre_Crossfeeder_LTE_Daily`(IN gt_db VARCHAR(100),IN GT_COVMO VARCHAR(100))
BEGIN	
	DECLARE START_TIME DATETIME DEFAULT SYSDATE();
	DECLARE SH_EH VARCHAR(9) DEFAULT RIGHT(GT_DB,9);
	SELECT REPLACE(GT_DB,SH_EH,'0000_0000') INTO GT_DB;	
	SET @FILE_STARTTIME=CONCAT(SUBSTRING(gt_strtok(GT_DB,3,'_'),1,4),'-',SUBSTRING(gt_strtok(GT_DB,3,'_'),5,2),'-',SUBSTRING(gt_strtok(GT_DB,3,'_'),7,2),' ',SUBSTRING(gt_strtok(GT_DB,4,'_'),1,2),':',SUBSTRING(gt_strtok(GT_DB,4,'_'),3,2),':00');
	SET @rnc= gt_strtok(GT_DB,2,'_');
	 
	INSERT INTO gt_gw_main.SP_LOG VALUES(GT_DB,' SP_Sub_Generate_Pre_Crossfeeder_LTE_Daily','Start', START_TIME);	
	
	
	SET @SqlCmd=CONCAT('INSERT INTO ',GT_DB,'.opt_cf_pre_agg_report_lte_d1
				(`ENODEB_ID`,`CELL_ID`,`POS_MR_ANGLE`,`MR_COUNT`,`AZIMUTH`,
				`CAL_AZIMUTH`,`CAL_POS_MR_ANGLE`)
			SELECT 
				  ENODEB_ID,
				  CELL_ID,
				  NULL AS POS_MR_ANGLE,
				  SUM(MR_COUNT) AS MR_COUNT,
				  AZIMUTH,
				  CAL_AZIMUTH,
				  SUM(CAL_POS_MR_ANGLE * MR_COUNT) / SUM(MR_COUNT) CAL_POS_MR_ANGLE 
			FROM ',GT_DB,'.opt_cf_pre_agg_report_lte
			GROUP BY ENODEB_ID,CELL_ID;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt; 
	
	
	 
	
	SET @SqlCmd=CONCAT('DROP TABLE IF EXISTS  ',GT_DB,'.`tmp_opt_cf_pre_agg_report_lte_d7`;'); 
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('CREATE TABLE ',GT_DB,'.`tmp_opt_cf_pre_agg_report_lte_d7` (
					  
					  `ENODEB_ID` MEDIUMINT(9) NOT NULL,
					  `CELL_ID` SMALLINT(6) UNSIGNED NOT NULL,
					  `POS_MR_ANGLE` DOUBLE NOT NULL,
					  `MR_COUNT` BIGINT(20) NOT NULL,
					  `AZIMUTH` DOUBLE NOT NULL,
					  `CAL_AZIMUTH` DOUBLE NOT NULL,
					  `CAL_POS_MR_ANGLE` DOUBLE NOT NULL,
					  KEY `cf_pre_report_table_idx1` (`ENODEB_ID`,`CELL_ID`)
				) ENGINE=MyISAM DEFAULT CHARSET=latin1 DELAY_KEY_WRITE=1;'); 
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	
	SET @SqlCmd=CONCAT('SELECT GROUP_CONCAT(session_DB SEPARATOR ''|'') into @session_db_str FROM gt_gw_main.session_information
	WHERE file_starttime <= ''',@FILE_STARTTIME,'''  AND file_starttime >=  DATE_SUB(''',@FILE_STARTTIME,''',INTERVAL 7 DAY )
	AND rnc = ''',@rnc,'''  AND SESSION_TYPE = ''DAY'';');
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
		if  not exists ',@session_DB,'.opt_cf_pre_agg_report_lte_d1
					SELECT *
						FROM ',@session_DB,'.opt_cf_pre_agg_report_lte
						GROUP BY ENODEB_ID,CELL_ID;'); 
					
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
	
		
		SET @SqlCmd=CONCAT('insert into   ',GT_DB,'.`tmp_opt_cf_pre_agg_report_lte_d7`
		(`ENODEB_ID`,`CELL_ID`,`POS_MR_ANGLE`,`MR_COUNT`,`AZIMUTH`,`CAL_AZIMUTH`,`CAL_POS_MR_ANGLE`)
		SELECT `ENODEB_ID`,`CELL_ID`,`POS_MR_ANGLE`,`MR_COUNT`,`AZIMUTH`,`CAL_AZIMUTH`,`CAL_POS_MR_ANGLE`
		from  ',@session_DB,'.opt_cf_pre_agg_report_lte_d1
		;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
	
 		END IF;
	
	        SET @v_i=@v_i+1; 
				
		
		END;
		END WHILE;
	
	
	
		SET @SqlCmd=CONCAT('insert into   ',GT_DB,'.`opt_cf_pre_agg_report_lte_d7`
		(`ENODEB_ID`,`CELL_ID`,`POS_MR_ANGLE`,`MR_COUNT`,`AZIMUTH`,`CAL_AZIMUTH`,`CAL_POS_MR_ANGLE`)
		SELECT            ENODEB_ID,
				  CELL_ID,
				  NULL AS POS_MR_ANGLE,
				  SUM(MR_COUNT) AS MR_COUNT,
				  AZIMUTH,
				  CAL_AZIMUTH,
				  SUM(CAL_POS_MR_ANGLE * MR_COUNT) / SUM(MR_COUNT) CAL_POS_MR_ANGLE 
		from  ',GT_DB,'.`tmp_opt_cf_pre_agg_report_lte_d7`
		GROUP BY ENODEB_ID,CELL_ID;
		;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('DROP TABLE IF EXISTS  ',GT_DB,'.`tmp_opt_cf_pre_agg_report_lte_d7`;'); 
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	
	
	INSERT INTO gt_gw_main.SP_LOG VALUES(GT_DB,' SP_Sub_Generate_Pre_Crossfeeder_LTE_Daily',CONCAT('Done: ',TIMESTAMPDIFF(SECOND,START_TIME,SYSDATE()),' seconds.'), NOW());		
