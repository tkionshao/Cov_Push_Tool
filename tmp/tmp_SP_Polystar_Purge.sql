CREATE DEFINER=`covmo`@`%` PROCEDURE `SP_Polystar_Purge`(IN TECH_MASK TINYINT(2),IN gt_polystar_db VARCHAR (20),TRUNCATE_DATE VARCHAR (20),DROP_DATE VARCHAR (20),IN TIME_TYPE TINYINT(4))
BEGIN
	DECLARE START_TIME DATETIME DEFAULT SYSDATE();
	DECLARE RNC_ID INT;
	DECLARE DATA_DATE VARCHAR(8);
	DECLARE DATA_QRT VARCHAR(4);
	DECLARE DATA_HOUR VARCHAR(4);
	DECLARE DAILY_DB VARCHAR(25);
	SELECT `DAY_OF_WEEK`(DROP_DATE) INTO @DAY_OF_WEEK;
	SET @FIRST_DAY=gt_strtok(@DAY_OF_WEEK, 1, '|');
	SET @END_DAY=gt_strtok(@DAY_OF_WEEK, 2, '|');
	SET @DATE_WK=CONCAT(DATE_FORMAT(@FIRST_DAY,'%Y%m%d'),'_',DATE_FORMAT(@END_DAY,'%Y%m%d'));
	
	INSERT INTO gt_gw_main.sp_log VALUES('gt_gw_main','SP_Polystar_Purge','START',NOW());	
	IF TECH_MASK IN (0,4) AND TIME_TYPE=1 THEN
	
			SET @SqlCmd=CONCAT(' TRUNCATE TABLE ',gt_polystar_db,'.TABLE_POS_POLYSTAR_LTE_',TRUNCATE_DATE,';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT(' DROP TABLE ',gt_polystar_db,'.TABLE_POS_POLYSTAR_LTE_',DROP_DATE,';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
			
	END IF;
	
	IF TECH_MASK IN (0,2) AND TIME_TYPE=1 THEN
	
			SET @SqlCmd=CONCAT(' TRUNCATE TABLE ',gt_polystar_db,'.TABLE_POS_POLYSTAR_umts_',TRUNCATE_DATE,';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT(' DROP TABLE ',gt_polystar_db,'.TABLE_POS_POLYSTAR_umts_',DROP_DATE,';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
			
	END IF;
	
	IF TECH_MASK IN (0,4) AND TIME_TYPE=4 THEN
	
			SET @SqlCmd=CONCAT(' DELETE FROM ',gt_polystar_db,'.`polystar_session_information`
						WHERE START_DATE=',DROP_DATE,'
						AND TECH_MASK=',TECH_MASK,'
						AND SESSION_TYPE=''WEEK''
						;');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT(' DROP TABLE IF EXISTS ',gt_polystar_db,'.rpt_family2_cell_def_lte_wk_',@DATE_WK,';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
			
			SET @SqlCmd=CONCAT(' DROP TABLE IF EXISTS ',gt_polystar_db,'.rpt_family2_cell_lte_wk_',@DATE_WK,';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
			
			SET @SqlCmd=CONCAT(' DROP TABLE IF EXISTS ',gt_polystar_db,'.rpt_family2_cell_tile_def_lte_wk_',@DATE_WK,';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT(' DROP TABLE IF EXISTS ',gt_polystar_db,'.rpt_family2_cell_tile_lte_wk_',@DATE_WK,';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT(' DROP TABLE IF EXISTS ',gt_polystar_db,'.rpt_family2_tile_def_lte_wk_',@DATE_WK,';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT(' DROP TABLE IF EXISTS ',gt_polystar_db,'.rpt_family2_tile_lte_wk_',@DATE_WK,';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT(' DROP TABLE IF EXISTS ',gt_polystar_db,'.rpt_family2_tile_max_lte_wk_',@DATE_WK,';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT(' DROP TABLE IF EXISTS ',gt_polystar_db,'.rpt_family2_tile_median_lte_wk_',@DATE_WK,';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
		
			SET @SqlCmd=CONCAT(' DROP TABLE IF EXISTS ',gt_polystar_db,'.rpt_family2_tile_min_lte_wk_',@DATE_WK,';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT(' DROP TABLE IF EXISTS ',gt_polystar_db,'.rpt_family2_reg_2_lte_wk_',@DATE_WK,';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT(' DROP TABLE IF EXISTS ',gt_polystar_db,'.rpt_family2_reg_3_lte_wk_',@DATE_WK,';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT(' DROP TABLE IF EXISTS ',gt_polystar_db,'.rpt_type2_cell_tile_def_lte_wk_',@DATE_WK,';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT(' DROP TABLE IF EXISTS ',gt_polystar_db,'.rpt_type2_cell_tile_lte_wk_',@DATE_WK,';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT(' DROP TABLE IF EXISTS ',gt_polystar_db,'.rpt_type2_cell_def_lte_wk_',@DATE_WK,';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT(' DROP TABLE IF EXISTS ',gt_polystar_db,'.rpt_type2_cell_lte_wk_',@DATE_WK,';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT(' DROP TABLE IF EXISTS ',gt_polystar_db,'.rpt_type2_tile_def_lte_wk_',@DATE_WK,';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT(' DROP TABLE IF EXISTS ',gt_polystar_db,'.rpt_type2_tile_lte_wk_',@DATE_WK,';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT(' DROP TABLE IF EXISTS ',gt_polystar_db,'.rpt_type2_tile_max_lte_wk_',@DATE_WK,';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT(' DROP TABLE IF EXISTS ',gt_polystar_db,'.rpt_type2_tile_median_lte_wk_',@DATE_WK,';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
		
			SET @SqlCmd=CONCAT(' DROP TABLE IF EXISTS ',gt_polystar_db,'.rpt_type2_tile_min_lte_wk_',@DATE_WK,';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT(' DROP TABLE IF EXISTS ',gt_polystar_db,'.rpt_type2_reg_2_lte_wk_',@DATE_WK,';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT(' DROP TABLE IF EXISTS ',gt_polystar_db,'.rpt_type2_reg_3_lte_wk_',@DATE_WK,';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT(' DROP TABLE IF EXISTS ',gt_polystar_db,'.rpt_useragent_cell_def_lte_wk_',@DATE_WK,';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			
			SET @SqlCmd=CONCAT(' DROP TABLE IF EXISTS ',gt_polystar_db,'.rpt_useragent_cell_lte_wk_',@DATE_WK,';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
			
			SET @SqlCmd=CONCAT(' DROP TABLE IF EXISTS ',gt_polystar_db,'.rpt_useragent_cell_tile_def_lte_wk_',@DATE_WK,';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT(' DROP TABLE IF EXISTS ',gt_polystar_db,'.rpt_useragent_cell_tile_lte_wk_',@DATE_WK,';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT(' DROP TABLE IF EXISTS ',gt_polystar_db,'.rpt_useragent_tile_def_lte_wk_',@DATE_WK,';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT(' DROP TABLE IF EXISTS ',gt_polystar_db,'.rpt_useragent_tile_lte_wk_',@DATE_WK,';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT(' DROP TABLE IF EXISTS ',gt_polystar_db,'.rpt_host_cell_def_lte_wk_',@DATE_WK,';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT(' DROP TABLE IF EXISTS ',gt_polystar_db,'.rpt_useragent_tile_max_lte_wk_',@DATE_WK,';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT(' DROP TABLE IF EXISTS ',gt_polystar_db,'.rpt_useragent_tile_median_lte_wk_',@DATE_WK,';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
		
			SET @SqlCmd=CONCAT(' DROP TABLE IF EXISTS ',gt_polystar_db,'.rpt_useragent_tile_min_lte_wk_',@DATE_WK,';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT(' DROP TABLE IF EXISTS ',gt_polystar_db,'.rpt_useragent_reg_2_lte_wk_',@DATE_WK,';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT(' DROP TABLE IF EXISTS ',gt_polystar_db,'.rpt_useragent_reg_3_lte_wk_',@DATE_WK,';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			
			SET @SqlCmd=CONCAT(' DROP TABLE IF EXISTS ',gt_polystar_db,'.rpt_host_cell_lte_wk_',@DATE_WK,';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
			
			SET @SqlCmd=CONCAT(' DROP TABLE IF EXISTS ',gt_polystar_db,'.rpt_host_cell_tile_def_lte_wk_',@DATE_WK,';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT(' DROP TABLE IF EXISTS ',gt_polystar_db,'.rpt_host_cell_tile_lte_wk_',@DATE_WK,';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT(' DROP TABLE IF EXISTS ',gt_polystar_db,'.rpt_host_tile_def_lte_wk_',@DATE_WK,';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT(' DROP TABLE IF EXISTS ',gt_polystar_db,'.rpt_host_tile_lte_wk_',@DATE_WK,';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;	
	
			SET @SqlCmd=CONCAT(' DROP TABLE IF EXISTS ',gt_polystar_db,'.rpt_host_tile_max_lte_wk_',@DATE_WK,';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT(' DROP TABLE IF EXISTS ',gt_polystar_db,'.rpt_host_tile_median_lte_wk_',@DATE_WK,';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
		
			SET @SqlCmd=CONCAT(' DROP TABLE IF EXISTS ',gt_polystar_db,'.rpt_host_tile_min_lte_wk_',@DATE_WK,';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT(' DROP TABLE IF EXISTS ',gt_polystar_db,'.rpt_host_reg_2_lte_wk_',@DATE_WK,';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT(' DROP TABLE IF EXISTS ',gt_polystar_db,'.rpt_host_reg_3_lte_wk_',@DATE_WK,';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT(' DROP TABLE IF EXISTS ',gt_polystar_db,'.rpt_xdr_appFamily_lte_wk_',@DATE_WK,';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT(' DROP TABLE IF EXISTS ',gt_polystar_db,'.rpt_xdr_apptype_lte_lte_wk_',@DATE_WK,';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT(' DROP TABLE IF EXISTS ',gt_polystar_db,'.rpt_xdr_apn_lte_wk_',@DATE_WK,';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			
			
	END IF;
	
	IF TECH_MASK IN (0,2) AND TIME_TYPE=4 THEN
	
			SET @SqlCmd=CONCAT(' DELETE FROM ',gt_polystar_db,'.`polystar_session_information`
						WHERE START_DATE=',DROP_DATE,'
						AND TECH_MASK=',TECH_MASK,'
						AND SESSION_TYPE=''WEEK''
						;');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT(' DROP TABLE IF EXISTS ',gt_polystar_db,'.rpt_family2_cell_def_umts_wk_',@DATE_WK,';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
			
			SET @SqlCmd=CONCAT(' DROP TABLE IF EXISTS ',gt_polystar_db,'.rpt_family2_cell_umts_wk_',@DATE_WK,';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
			
			SET @SqlCmd=CONCAT(' DROP TABLE IF EXISTS ',gt_polystar_db,'.rpt_family2_cell_tile_def_umts_wk_',@DATE_WK,';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT(' DROP TABLE IF EXISTS ',gt_polystar_db,'.rpt_family2_cell_tile_umts_wk_',@DATE_WK,';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
			
			SET @SqlCmd=CONCAT(' DROP TABLE IF EXISTS ',gt_polystar_db,'.rpt_family2_tile_def_umts_wk_',@DATE_WK,';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT(' DROP TABLE IF EXISTS ',gt_polystar_db,'.rpt_family2_tile_umts_wk_',@DATE_WK,';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT(' DROP TABLE IF EXISTS ',gt_polystar_db,'.rpt_family2_tile_max_umts_wk_',@DATE_WK,';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT(' DROP TABLE IF EXISTS ',gt_polystar_db,'.rpt_family2_tile_median_umts_wk_',@DATE_WK,';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
		
			SET @SqlCmd=CONCAT(' DROP TABLE IF EXISTS ',gt_polystar_db,'.rpt_family2_tile_min_umts_wk_',@DATE_WK,';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT(' DROP TABLE IF EXISTS ',gt_polystar_db,'.rpt_family2_reg_2_umts_wk_',@DATE_WK,';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT(' DROP TABLE IF EXISTS ',gt_polystar_db,'.rpt_family2_reg_3_umts_wk_',@DATE_WK,';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT(' DROP TABLE IF EXISTS ',gt_polystar_db,'.rpt_type2_cell_tile_def_umts_wk_',@DATE_WK,';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
	
			SET @SqlCmd=CONCAT(' DROP TABLE IF EXISTS ',gt_polystar_db,'.rpt_type2_cell_tile_umts_wk_',@DATE_WK,';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT(' DROP TABLE IF EXISTS ',gt_polystar_db,'.rpt_type2_cell_def_umts_wk_',@DATE_WK,';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT(' DROP TABLE IF EXISTS ',gt_polystar_db,'.rpt_type2_cell_umts_wk_',@DATE_WK,';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
			SET @SqlCmd=CONCAT(' DROP TABLE IF EXISTS ',gt_polystar_db,'.rpt_type2_tile_def_umts_wk_',@DATE_WK,';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT(' DROP TABLE IF EXISTS ',gt_polystar_db,'.rpt_type2_tile_umts_wk_',@DATE_WK,';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT(' DROP TABLE IF EXISTS ',gt_polystar_db,'.rpt_type2_tile_max_umts_wk_',@DATE_WK,';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT(' DROP TABLE IF EXISTS ',gt_polystar_db,'.rpt_type2_tile_median_umts_wk_',@DATE_WK,';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
		
			SET @SqlCmd=CONCAT(' DROP TABLE IF EXISTS ',gt_polystar_db,'.rpt_type2_tile_min_umts_wk_',@DATE_WK,';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT(' DROP TABLE IF EXISTS ',gt_polystar_db,'.rpt_type2_reg_2_umts_wk_',@DATE_WK,';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT(' DROP TABLE IF EXISTS ',gt_polystar_db,'.rpt_type2_reg_3_umts_wk_',@DATE_WK,';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT(' DROP TABLE IF EXISTS ',gt_polystar_db,'.rpt_useragent_cell_def_umts_wk_',@DATE_WK,';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
			
			SET @SqlCmd=CONCAT(' DROP TABLE IF EXISTS ',gt_polystar_db,'.rpt_useragent_cell_umts_wk_',@DATE_WK,';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
			
			SET @SqlCmd=CONCAT(' DROP TABLE IF EXISTS ',gt_polystar_db,'.rpt_useragent_cell_tile_def_umts_wk_',@DATE_WK,';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT(' DROP TABLE IF EXISTS ',gt_polystar_db,'.rpt_useragent_cell_tile_umts_wk_',@DATE_WK,';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT(' DROP TABLE IF EXISTS ',gt_polystar_db,'.rpt_useragent_tile_def_umts_wk_',@DATE_WK,';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT(' DROP TABLE IF EXISTS ',gt_polystar_db,'.rpt_useragent_tile_umts_wk_',@DATE_WK,';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT(' DROP TABLE IF EXISTS ',gt_polystar_db,'.rpt_useragent_tile_max_umts_wk_',@DATE_WK,';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT(' DROP TABLE IF EXISTS ',gt_polystar_db,'.rpt_useragent_tile_median_umts_wk_',@DATE_WK,';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
		
			SET @SqlCmd=CONCAT(' DROP TABLE IF EXISTS ',gt_polystar_db,'.rpt_useragent_tile_min_umts_wk_',@DATE_WK,';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT(' DROP TABLE IF EXISTS ',gt_polystar_db,'.rpt_useragent_reg_2_umts_wk_',@DATE_WK,';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT(' DROP TABLE IF EXISTS ',gt_polystar_db,'.rpt_useragent_reg_3_umts_wk_',@DATE_WK,';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT(' DROP TABLE IF EXISTS ',gt_polystar_db,'.rpt_host_cell_def_umts_wk_',@DATE_WK,';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			
			SET @SqlCmd=CONCAT(' DROP TABLE IF EXISTS ',gt_polystar_db,'.rpt_host_cell_umts_wk_',@DATE_WK,';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
			
			SET @SqlCmd=CONCAT(' DROP TABLE IF EXISTS ',gt_polystar_db,'.rpt_host_cell_tile_def_umts_wk_',@DATE_WK,';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT(' DROP TABLE IF EXISTS ',gt_polystar_db,'.rpt_host_cell_tile_umts_wk_',@DATE_WK,';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT(' DROP TABLE IF EXISTS ',gt_polystar_db,'.rpt_host_tile_def_umts_wk_',@DATE_WK,';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT(' DROP TABLE IF EXISTS ',gt_polystar_db,'.rpt_host_tile_umts_wk_',@DATE_WK,';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT(' DROP TABLE IF EXISTS ',gt_polystar_db,'.rpt_host_tile_max_umts_wk_',@DATE_WK,';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT(' DROP TABLE IF EXISTS ',gt_polystar_db,'.rpt_host_tile_median_umts_wk_',@DATE_WK,';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
		
			SET @SqlCmd=CONCAT(' DROP TABLE IF EXISTS ',gt_polystar_db,'.rpt_host_tile_min_umts_wk_',@DATE_WK,';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT(' DROP TABLE IF EXISTS ',gt_polystar_db,'.rpt_host_reg_2_umts_wk_',@DATE_WK,';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT(' DROP TABLE IF EXISTS ',gt_polystar_db,'.rpt_host_reg_3_umts_wk_',@DATE_WK,';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT(' DROP TABLE IF EXISTS ',gt_polystar_db,'.rpt_xdr_appFamily_umts_wk_',@DATE_WK,';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT(' DROP TABLE IF EXISTS ',gt_polystar_db,'.rpt_xdr_apptype_lte_umts_wk_',@DATE_WK,';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT(' DROP TABLE IF EXISTS ',gt_polystar_db,'.rpt_xdr_apn_umts_wk_',@DATE_WK,';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	END IF;
	
	INSERT INTO gt_gw_main.SP_LOG VALUES('gt_gw_main','SP_Polystar_Purge',CONCAT('Done: ',TIMESTAMPDIFF(SECOND,START_TIME,SYSDATE()),' seconds.'), NOW());
