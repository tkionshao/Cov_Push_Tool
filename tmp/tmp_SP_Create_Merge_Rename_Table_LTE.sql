CREATE DEFINER=`covmo`@`%` PROCEDURE `SP_Create_Merge_Rename_Table_LTE`(IN GT_DB VARCHAR(100),IN FLAG CHAR(2),IN EVENT_NUM TINYINT(2),IN POS_NUM TINYINT(2))
BEGIN
	DECLARE START_TIME DATETIME DEFAULT SYSDATE();
	DECLARE UNION_STR_CALL TEXT ;
	DECLARE UNION_STR_POS_HO TEXT ;
	DECLARE UNION_STR_POS_SRV MEDIUMTEXT;
	DECLARE QRT VARCHAR(50) DEFAULT gt_strtok(GT_DB,4,'_');
	DECLARE tbl_num SMALLINT(6) DEFAULT 0;
	DECLARE tile_count SMALLINT(6) DEFAULT 0;
	DECLARE imsig_count SMALLINT(6) DEFAULT 0;
	DECLARE cr_count SMALLINT(6) DEFAULT 0;
	DECLARE qry_tbl_name VARCHAR(50);
	DECLARE qry_tbl_name_pos_ho VARCHAR(50);
	DECLARE qry_tbl_name_pos_srv VARCHAR(50);
	DECLARE qry_tbl_name_erab VARCHAR(50);
	DECLARE v_i SMALLINT(6);
	DECLARE v_j SMALLINT(6);
	DECLARE SPECIAL_IMSI VARCHAR(50);
	DECLARE CURRENT_NT_DB VARCHAR(50) DEFAULT CONCAT('gt_nt_',gt_strtok(GT_DB,3,'_'));
	DECLARE TAC_REPORT_FLAG VARCHAR(10);
	DECLARE CQI_FLAG VARCHAR(10);
	DECLARE PMC_FLAG VARCHAR(10);
	DECLARE MDT_FLAG VARCHAR(10);
	DECLARE PM_COUNTER_FLAG VARCHAR(10);
	DECLARE RPT_HIGH_FLAG VARCHAR(10);
	DECLARE RPT_HIGH_MED_FLAG VARCHAR(10);
	DECLARE NBIoT_FLAG VARCHAR(10);
	DECLARE IMSI_TABLE_FLAG VARCHAR(10);

	SELECT LOWER(`value`) INTO TAC_REPORT_FLAG  FROM gt_gw_main.integration_param WHERE gt_group = 'sp' AND gt_name = '01.tac.report' ;
	SELECT LOWER(`value`) INTO CQI_FLAG FROM gt_gw_main.integration_param WHERE gt_group = 'sp' AND gt_name = 'cqi' ;
	SELECT LOWER(`value`) INTO PMC_FLAG FROM gt_gw_main.integration_param WHERE gt_group = 'sp' AND gt_name = 'pmc' ;
	SELECT LOWER(`value`) INTO MDT_FLAG FROM gt_gw_main.integration_param WHERE gt_group = 'sp' AND gt_name = 'mdt' ;
	SELECT LOWER(`value`) INTO PM_COUNTER_FLAG FROM gt_gw_main.integration_param WHERE gt_group = 'sp' AND gt_name = 'pm_counter';
	SELECT LOWER(`value`) INTO RPT_HIGH_FLAG FROM gt_gw_main.integration_param WHERE gt_group = 'sp' AND gt_name = 'rpt_high' ; 
	SELECT LOWER(`value`) INTO RPT_HIGH_MED_FLAG FROM gt_gw_main.integration_param WHERE gt_group = 'sp' AND gt_name = 'rpt_high_med' ; 
	SELECT LOWER(`value`) INTO NBIoT_FLAG FROM gt_gw_main.integration_param WHERE gt_group = 'sp' AND gt_name = 'NB-IoT';
	SELECT LOWER(`value`) INTO IMSI_TABLE_FLAG FROM gt_gw_main.integration_param WHERE gt_group = 'sp' AND gt_name = 'imsi_table' ; 

	SET @tbl_num2 = NULL;
	SET @TileResolution = NULL;
	SET @special_imsig_TileResolution = NULL;
	SET @special_imsig_SpecifiedIMSIRangeValue = NULL;
	SET @zoomlevel = NULL;
	SET @cr = NULL;
	

	
	SET @SqlCmd=CONCAT('SELECT att_value INTO @TileResolution FROM `',CURRENT_NT_DB,'`.`sys_config` WHERE group_name = ''system'' AND att_name = ''TileResolution'';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	SET tile_count = gt_covmo_csv_count(@TileResolution,',');
	


	
	
	SET @SqlCmd=CONCAT('SELECT LOWER(att_value) INTO @SPECIAL_IMSI_CR FROM ',CURRENT_NT_DB,'.sys_config WHERE group_name = ''System'' AND tech_mask = 7 AND att_name = ''DataProcessForImsiRange'';');
	PREPARE Stmt FROM @SqlCmd;
	SELECT @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;	
	SET @SqlCmd=CONCAT('SELECT att_value INTO @CR_THRESHOLD FROM ',CURRENT_NT_DB,'.sys_config WHERE group_name = ''System'' AND tech_mask = 7 AND att_name = ''SpecifiedIMSIRangeValue'';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	IF @SPECIAL_IMSI_CR = 'true' THEN
		SET @SqlCmd=CONCAT('SELECT GROUP_CONCAT(DISTINCT (ABS(`GROUP_ID`))) INTO @special_imsig_SpecifiedIMSIRangeValue FROM ',CURRENT_NT_DB,'.`dim_imsi_group` WHERE GROUP_ID < ',@CR_THRESHOLD,';');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
	END IF;
	SET cr_count = gt_covmo_csv_count(@special_imsig_SpecifiedIMSIRangeValue,',');

	
	
	IF IMSI_TABLE_FLAG = 'true' THEN 
		SELECT LOWER(`value`) INTO SPECIAL_IMSI  FROM gt_gw_main.integration_param WHERE gt_group = 'sp' AND gt_name = 'special_imsi' ;
	
		SET @SqlCmd=CONCAT('SELECT GROUP_CONCAT(DISTINCT (ABS(`GROUP_ID`))) INTO @special_imsig_TileResolution FROM ',CURRENT_NT_DB,'.`dim_imsi_group` WHERE GROUP_ID < 0 AND GROUP_ID >= -10000;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		SET imsig_count = gt_covmo_csv_count(@special_imsig_TileResolution,',');
		
		INSERT INTO gt_gw_main.SP_LOG VALUES(GT_DB,'SP_Create_Merge_Rename_Table_LTE','Start', START_TIME);
	END IF;
	
	IF FLAG='WK' THEN		
		SET v_j=1;
		WHILE v_j <= tile_count DO
		BEGIN	
			SET @zoomlevel = gt_covmo_csv_get(@TileResolution,v_j);
			
			CALL gt_gw_main.SP_Create_Merge_Rename_ZoomLevel(GT_DB,@zoomlevel,'rpt_cell_tile_end_wk');
			CALL gt_gw_main.SP_Create_Merge_Rename_ZoomLevel(GT_DB,@zoomlevel,'rpt_cell_tile_end_wk_def');
			CALL gt_gw_main.SP_Create_Merge_Rename_ZoomLevel(GT_DB,@zoomlevel,'rpt_cell_tile_position_wk');
			CALL gt_gw_main.SP_Create_Merge_Rename_ZoomLevel(GT_DB,@zoomlevel,'rpt_cell_tile_position_wk_def');
			CALL gt_gw_main.SP_Create_Merge_Rename_ZoomLevel(GT_DB,@zoomlevel,'rpt_cell_tile_start_wk');
			CALL gt_gw_main.SP_Create_Merge_Rename_ZoomLevel(GT_DB,@zoomlevel,'rpt_cell_tile_start_wk_def');
			CALL gt_gw_main.SP_Create_Merge_Rename_ZoomLevel(GT_DB,@zoomlevel,'rpt_tile_end_wk');
			CALL gt_gw_main.SP_Create_Merge_Rename_ZoomLevel(GT_DB,@zoomlevel,'rpt_tile_end_wk_def');
			CALL gt_gw_main.SP_Create_Merge_Rename_ZoomLevel(GT_DB,@zoomlevel,'rpt_tile_position_wk');
			CALL gt_gw_main.SP_Create_Merge_Rename_ZoomLevel(GT_DB,@zoomlevel,'rpt_tile_position_wk_def');
			CALL gt_gw_main.SP_Create_Merge_Rename_ZoomLevel(GT_DB,@zoomlevel,'rpt_tile_start_wk');
			CALL gt_gw_main.SP_Create_Merge_Rename_ZoomLevel(GT_DB,@zoomlevel,'rpt_tile_start_wk_def');
	
			IF RPT_HIGH_FLAG = 'true' THEN 
				CALL gt_gw_main.SP_Create_Merge_Rename_ZoomLevel(GT_DB,@zoomlevel,'rpt_cell_tile_position_high_wk');
				CALL gt_gw_main.SP_Create_Merge_Rename_ZoomLevel(GT_DB,@zoomlevel,'rpt_cell_tile_position_high_wk_def');
				CALL gt_gw_main.SP_Create_Merge_Rename_ZoomLevel(GT_DB,@zoomlevel,'rpt_cell_tile_start_high_wk');
				CALL gt_gw_main.SP_Create_Merge_Rename_ZoomLevel(GT_DB,@zoomlevel,'rpt_cell_tile_start_high_wk_def');
				CALL gt_gw_main.SP_Create_Merge_Rename_ZoomLevel(GT_DB,@zoomlevel,'rpt_cell_tile_end_high_wk');
				CALL gt_gw_main.SP_Create_Merge_Rename_ZoomLevel(GT_DB,@zoomlevel,'rpt_cell_tile_end_high_wk_def');
				CALL gt_gw_main.SP_Create_Merge_Rename_ZoomLevel(GT_DB,@zoomlevel,'rpt_tile_position_high_wk');
				CALL gt_gw_main.SP_Create_Merge_Rename_ZoomLevel(GT_DB,@zoomlevel,'rpt_tile_position_high_wk_def');
				CALL gt_gw_main.SP_Create_Merge_Rename_ZoomLevel(GT_DB,@zoomlevel,'rpt_tile_start_high_wk');
				CALL gt_gw_main.SP_Create_Merge_Rename_ZoomLevel(GT_DB,@zoomlevel,'rpt_tile_start_high_wk_def');
				CALL gt_gw_main.SP_Create_Merge_Rename_ZoomLevel(GT_DB,@zoomlevel,'rpt_tile_end_high_wk');
				CALL gt_gw_main.SP_Create_Merge_Rename_ZoomLevel(GT_DB,@zoomlevel,'rpt_tile_end_high_wk_def');
			END IF;
	
			IF RPT_HIGH_MED_FLAG = 'true' THEN 
				CALL gt_gw_main.SP_Create_Merge_Rename_ZoomLevel(GT_DB,@zoomlevel,'rpt_cell_tile_position_high_med_wk');
				CALL gt_gw_main.SP_Create_Merge_Rename_ZoomLevel(GT_DB,@zoomlevel,'rpt_cell_tile_position_high_med_wk_def');
				CALL gt_gw_main.SP_Create_Merge_Rename_ZoomLevel(GT_DB,@zoomlevel,'rpt_cell_tile_start_high_med_wk');
				CALL gt_gw_main.SP_Create_Merge_Rename_ZoomLevel(GT_DB,@zoomlevel,'rpt_cell_tile_start_high_med_wk_def');
				CALL gt_gw_main.SP_Create_Merge_Rename_ZoomLevel(GT_DB,@zoomlevel,'rpt_cell_tile_end_high_med_wk');
				CALL gt_gw_main.SP_Create_Merge_Rename_ZoomLevel(GT_DB,@zoomlevel,'rpt_cell_tile_end_high_med_wk_def');
				CALL gt_gw_main.SP_Create_Merge_Rename_ZoomLevel(GT_DB,@zoomlevel,'rpt_tile_position_high_med_wk');
				CALL gt_gw_main.SP_Create_Merge_Rename_ZoomLevel(GT_DB,@zoomlevel,'rpt_tile_position_high_med_wk_def');
				CALL gt_gw_main.SP_Create_Merge_Rename_ZoomLevel(GT_DB,@zoomlevel,'rpt_tile_start_high_med_wk');
				CALL gt_gw_main.SP_Create_Merge_Rename_ZoomLevel(GT_DB,@zoomlevel,'rpt_tile_start_high_med_wk_def');
				CALL gt_gw_main.SP_Create_Merge_Rename_ZoomLevel(GT_DB,@zoomlevel,'rpt_tile_end_high_med_wk');
				CALL gt_gw_main.SP_Create_Merge_Rename_ZoomLevel(GT_DB,@zoomlevel,'rpt_tile_end_high_med_wk_def');
			END IF;
	
			
			SET v_j=v_j+1;
		END;
		END WHILE;
	END IF ;
	
	IF FLAG='DY' THEN
		IF SPECIAL_IMSI = 'true' THEN
SELECT 'merge_rename s';			
			SET v_j=1;
			WHILE v_j <= imsig_count DO
			BEGIN	
				SET @group_id = gt_covmo_csv_get(@special_imsig_TileResolution,v_j);
				SET @group_bottom_str = CONCAT('_imsig',@group_id);

				CALL gt_gw_main.SP_Create_Merge_Rename_24QI(GT_DB,CONCAT('table_call_lte',@group_bottom_str),@group_bottom_str,'table_call_lte');
				CALL gt_gw_main.SP_Create_Merge_Rename_24QI(GT_DB,CONCAT('table_position_convert_ho_lte',@group_bottom_str),@group_bottom_str,'table_position_convert_ho_lte');
				CALL gt_gw_main.SP_Create_Merge_Rename_24QI(GT_DB,CONCAT('table_position_convert_serving_lte',@group_bottom_str),@group_bottom_str,'table_position_convert_serving_lte');
				CALL gt_gw_main.SP_Create_Merge_Rename_24QI(GT_DB,CONCAT('table_erab_lte',@group_bottom_str),@group_bottom_str,'table_erab_lte');

				SET v_j=v_j+1;
			END;
			END WHILE;
SELECT 'merge_rename e';

		END IF;	
		SET v_j=1;
		
		WHILE v_j <= tile_count DO
		BEGIN	
			SET @zoomlevel = gt_covmo_csv_get(@TileResolution,v_j);
			CALL gt_gw_main.SP_Create_Merge_Rename_ZoomLevel(GT_DB,@zoomlevel,'rpt_cell_tile_end');
			CALL gt_gw_main.SP_Create_Merge_Rename_ZoomLevel(GT_DB,@zoomlevel,'rpt_cell_tile_end_def');
			CALL gt_gw_main.SP_Create_Merge_Rename_ZoomLevel(GT_DB,@zoomlevel,'rpt_cell_tile_end_dy');
			CALL gt_gw_main.SP_Create_Merge_Rename_ZoomLevel(GT_DB,@zoomlevel,'rpt_cell_tile_end_dy_def');
			CALL gt_gw_main.SP_Create_Merge_Rename_ZoomLevel(GT_DB,@zoomlevel,'rpt_cell_tile_position');
			CALL gt_gw_main.SP_Create_Merge_Rename_ZoomLevel(GT_DB,@zoomlevel,'rpt_cell_tile_position_def');
			CALL gt_gw_main.SP_Create_Merge_Rename_ZoomLevel(GT_DB,@zoomlevel,'rpt_cell_tile_position_dy');
			CALL gt_gw_main.SP_Create_Merge_Rename_ZoomLevel(GT_DB,@zoomlevel,'rpt_cell_tile_position_dy_def');
			CALL gt_gw_main.SP_Create_Merge_Rename_ZoomLevel(GT_DB,@zoomlevel,'rpt_cell_tile_start');
			CALL gt_gw_main.SP_Create_Merge_Rename_ZoomLevel(GT_DB,@zoomlevel,'rpt_cell_tile_start_def');
			CALL gt_gw_main.SP_Create_Merge_Rename_ZoomLevel(GT_DB,@zoomlevel,'rpt_cell_tile_start_dy');
			CALL gt_gw_main.SP_Create_Merge_Rename_ZoomLevel(GT_DB,@zoomlevel,'rpt_cell_tile_start_dy_def');
			CALL gt_gw_main.SP_Create_Merge_Rename_ZoomLevel(GT_DB,@zoomlevel,'rpt_tile_end');
			CALL gt_gw_main.SP_Create_Merge_Rename_ZoomLevel(GT_DB,@zoomlevel,'rpt_tile_end_def');
			CALL gt_gw_main.SP_Create_Merge_Rename_ZoomLevel(GT_DB,@zoomlevel,'rpt_tile_end_dy');
			CALL gt_gw_main.SP_Create_Merge_Rename_ZoomLevel(GT_DB,@zoomlevel,'rpt_tile_end_dy_def');
			CALL gt_gw_main.SP_Create_Merge_Rename_ZoomLevel(GT_DB,@zoomlevel,'rpt_tile_position');
			CALL gt_gw_main.SP_Create_Merge_Rename_ZoomLevel(GT_DB,@zoomlevel,'rpt_tile_position_def');
			CALL gt_gw_main.SP_Create_Merge_Rename_ZoomLevel(GT_DB,@zoomlevel,'rpt_tile_position_dy');
			CALL gt_gw_main.SP_Create_Merge_Rename_ZoomLevel(GT_DB,@zoomlevel,'rpt_tile_position_dy_def');
			CALL gt_gw_main.SP_Create_Merge_Rename_ZoomLevel(GT_DB,@zoomlevel,'rpt_tile_start');
			CALL gt_gw_main.SP_Create_Merge_Rename_ZoomLevel(GT_DB,@zoomlevel,'rpt_tile_start_def');
			CALL gt_gw_main.SP_Create_Merge_Rename_ZoomLevel(GT_DB,@zoomlevel,'rpt_tile_start_dy');
			CALL gt_gw_main.SP_Create_Merge_Rename_ZoomLevel(GT_DB,@zoomlevel,'rpt_tile_start_dy_def');
	
			
	
			CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,CONCAT('rpt_cell_tile',@zoomlevel,'_end'));
			CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,CONCAT('rpt_cell_tile',@zoomlevel,'_end_def'));
			CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,CONCAT('rpt_cell_tile',@zoomlevel,'_position'));
			CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,CONCAT('rpt_cell_tile',@zoomlevel,'_position_def'));
			CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,CONCAT('rpt_cell_tile',@zoomlevel,'_start'));
			CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,CONCAT('rpt_cell_tile',@zoomlevel,'_start_def'));
			CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,CONCAT('rpt_tile',@zoomlevel,'_end'));
			CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,CONCAT('rpt_tile',@zoomlevel,'_end_def'));
			CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,CONCAT('rpt_tile',@zoomlevel,'_position'));
			CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,CONCAT('rpt_tile',@zoomlevel,'_position_def'));
			CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,CONCAT('rpt_tile',@zoomlevel,'_start'));
			CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,CONCAT('rpt_tile',@zoomlevel,'_start_def'));
	
			
			
			IF CQI_FLAG = 'true' THEN
				CALL gt_gw_main.SP_Create_Merge_Rename_ZoomLevel(GT_DB,@zoomlevel,'rpt_cell_tile_cqi');
				CALL gt_gw_main.SP_Create_Merge_Rename_ZoomLevel(GT_DB,@zoomlevel,'rpt_cell_tile_cqi_def');
				CALL gt_gw_main.SP_Create_Merge_Rename_ZoomLevel(GT_DB,@zoomlevel,'rpt_cell_tile_cqi_dy');
				CALL gt_gw_main.SP_Create_Merge_Rename_ZoomLevel(GT_DB,@zoomlevel,'rpt_cell_tile_cqi_dy_def');
				CALL gt_gw_main.SP_Create_Merge_Rename_ZoomLevel(GT_DB,@zoomlevel,'rpt_tile_cqi');
				CALL gt_gw_main.SP_Create_Merge_Rename_ZoomLevel(GT_DB,@zoomlevel,'rpt_tile_cqi_def');
				CALL gt_gw_main.SP_Create_Merge_Rename_ZoomLevel(GT_DB,@zoomlevel,'rpt_tile_cqi_dy');
				CALL gt_gw_main.SP_Create_Merge_Rename_ZoomLevel(GT_DB,@zoomlevel,'rpt_tile_cqi_dy_def');
				CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,CONCAT('rpt_cell_tile',@zoomlevel,'_cqi'));
				CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,CONCAT('rpt_cell_tile',@zoomlevel,'_cqi_def'));
				CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,CONCAT('rpt_tile',@zoomlevel,'_cqi'));
				CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,CONCAT('rpt_tile',@zoomlevel,'_cqi_def'));
			END IF;
			
			
			IF RPT_HIGH_FLAG = 'true' THEN 
				CALL gt_gw_main.SP_Create_Merge_Rename_ZoomLevel(GT_DB,@zoomlevel,'rpt_cell_tile_position_high');
				CALL gt_gw_main.SP_Create_Merge_Rename_ZoomLevel(GT_DB,@zoomlevel,'rpt_cell_tile_position_high_def');
				CALL gt_gw_main.SP_Create_Merge_Rename_ZoomLevel(GT_DB,@zoomlevel,'rpt_cell_tile_position_high_dy');
				CALL gt_gw_main.SP_Create_Merge_Rename_ZoomLevel(GT_DB,@zoomlevel,'rpt_cell_tile_position_high_dy_def');
				CALL gt_gw_main.SP_Create_Merge_Rename_ZoomLevel(GT_DB,@zoomlevel,'rpt_cell_tile_start_high');
				CALL gt_gw_main.SP_Create_Merge_Rename_ZoomLevel(GT_DB,@zoomlevel,'rpt_cell_tile_start_high_def');
				CALL gt_gw_main.SP_Create_Merge_Rename_ZoomLevel(GT_DB,@zoomlevel,'rpt_cell_tile_start_high_dy');
				CALL gt_gw_main.SP_Create_Merge_Rename_ZoomLevel(GT_DB,@zoomlevel,'rpt_cell_tile_start_high_dy_def');
	
				CALL gt_gw_main.SP_Create_Merge_Rename_ZoomLevel(GT_DB,@zoomlevel,'rpt_cell_tile_end_high');
				CALL gt_gw_main.SP_Create_Merge_Rename_ZoomLevel(GT_DB,@zoomlevel,'rpt_cell_tile_end_high_def');
				CALL gt_gw_main.SP_Create_Merge_Rename_ZoomLevel(GT_DB,@zoomlevel,'rpt_cell_tile_end_high_dy');
				CALL gt_gw_main.SP_Create_Merge_Rename_ZoomLevel(GT_DB,@zoomlevel,'rpt_cell_tile_end_high_dy_def');
	
				CALL gt_gw_main.SP_Create_Merge_Rename_ZoomLevel(GT_DB,@zoomlevel,'rpt_tile_position_high');
				CALL gt_gw_main.SP_Create_Merge_Rename_ZoomLevel(GT_DB,@zoomlevel,'rpt_tile_position_high_def');
				CALL gt_gw_main.SP_Create_Merge_Rename_ZoomLevel(GT_DB,@zoomlevel,'rpt_tile_position_high_dy');
				CALL gt_gw_main.SP_Create_Merge_Rename_ZoomLevel(GT_DB,@zoomlevel,'rpt_tile_position_high_dy_def');
				CALL gt_gw_main.SP_Create_Merge_Rename_ZoomLevel(GT_DB,@zoomlevel,'rpt_tile_start_high');
				CALL gt_gw_main.SP_Create_Merge_Rename_ZoomLevel(GT_DB,@zoomlevel,'rpt_tile_start_high_def');
				CALL gt_gw_main.SP_Create_Merge_Rename_ZoomLevel(GT_DB,@zoomlevel,'rpt_tile_start_high_dy');
				CALL gt_gw_main.SP_Create_Merge_Rename_ZoomLevel(GT_DB,@zoomlevel,'rpt_tile_start_high_dy_def');
				CALL gt_gw_main.SP_Create_Merge_Rename_ZoomLevel(GT_DB,@zoomlevel,'rpt_tile_end_high');
				CALL gt_gw_main.SP_Create_Merge_Rename_ZoomLevel(GT_DB,@zoomlevel,'rpt_tile_end_high_def');
				CALL gt_gw_main.SP_Create_Merge_Rename_ZoomLevel(GT_DB,@zoomlevel,'rpt_tile_end_high_dy');
				CALL gt_gw_main.SP_Create_Merge_Rename_ZoomLevel(GT_DB,@zoomlevel,'rpt_tile_end_high_dy_def');
				CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,CONCAT('rpt_cell_tile',@zoomlevel,'_position_high'));
				CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,CONCAT('rpt_cell_tile',@zoomlevel,'_position_high_def'));
				CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,CONCAT('rpt_cell_tile',@zoomlevel,'_start_high'));
				CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,CONCAT('rpt_cell_tile',@zoomlevel,'_start_high_def'));
				CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,CONCAT('rpt_cell_tile',@zoomlevel,'_end_high'));
				CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,CONCAT('rpt_cell_tile',@zoomlevel,'_end_high_def'));
				CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,CONCAT('rpt_tile',@zoomlevel,'_position_high'));
				CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,CONCAT('rpt_tile',@zoomlevel,'_position_high_def'));
				CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,CONCAT('rpt_tile',@zoomlevel,'_start_high'));
				CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,CONCAT('rpt_tile',@zoomlevel,'_start_high_def'));
				CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,CONCAT('rpt_tile',@zoomlevel,'_end_high'));
				CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,CONCAT('rpt_tile',@zoomlevel,'_end_high_def'));
			END IF;
	
			IF RPT_HIGH_MED_FLAG = 'true' THEN 
				CALL gt_gw_main.SP_Create_Merge_Rename_ZoomLevel(GT_DB,@zoomlevel,'rpt_cell_tile_position_high_med');
				CALL gt_gw_main.SP_Create_Merge_Rename_ZoomLevel(GT_DB,@zoomlevel,'rpt_cell_tile_position_high_med_def');
				CALL gt_gw_main.SP_Create_Merge_Rename_ZoomLevel(GT_DB,@zoomlevel,'rpt_cell_tile_position_high_med_dy');
				CALL gt_gw_main.SP_Create_Merge_Rename_ZoomLevel(GT_DB,@zoomlevel,'rpt_cell_tile_position_high_med_dy_def');
	
				CALL gt_gw_main.SP_Create_Merge_Rename_ZoomLevel(GT_DB,@zoomlevel,'rpt_cell_tile_start_high_med');
				CALL gt_gw_main.SP_Create_Merge_Rename_ZoomLevel(GT_DB,@zoomlevel,'rpt_cell_tile_start_high_med_def');
				CALL gt_gw_main.SP_Create_Merge_Rename_ZoomLevel(GT_DB,@zoomlevel,'rpt_cell_tile_start_high_med_dy');
				CALL gt_gw_main.SP_Create_Merge_Rename_ZoomLevel(GT_DB,@zoomlevel,'rpt_cell_tile_start_high_med_dy_def');
	
				CALL gt_gw_main.SP_Create_Merge_Rename_ZoomLevel(GT_DB,@zoomlevel,'rpt_cell_tile_end_high_med');
				CALL gt_gw_main.SP_Create_Merge_Rename_ZoomLevel(GT_DB,@zoomlevel,'rpt_cell_tile_end_high_med_def');
				CALL gt_gw_main.SP_Create_Merge_Rename_ZoomLevel(GT_DB,@zoomlevel,'rpt_cell_tile_end_high_med_dy');
				CALL gt_gw_main.SP_Create_Merge_Rename_ZoomLevel(GT_DB,@zoomlevel,'rpt_cell_tile_end_high_med_dy_def');
	
				CALL gt_gw_main.SP_Create_Merge_Rename_ZoomLevel(GT_DB,@zoomlevel,'rpt_tile_position_high_med');
				CALL gt_gw_main.SP_Create_Merge_Rename_ZoomLevel(GT_DB,@zoomlevel,'rpt_tile_position_high_med_def');
				CALL gt_gw_main.SP_Create_Merge_Rename_ZoomLevel(GT_DB,@zoomlevel,'rpt_tile_position_high_med_dy');
				CALL gt_gw_main.SP_Create_Merge_Rename_ZoomLevel(GT_DB,@zoomlevel,'rpt_tile_position_high_med_dy_def');
	
				CALL gt_gw_main.SP_Create_Merge_Rename_ZoomLevel(GT_DB,@zoomlevel,'rpt_tile_start_high_med');
				CALL gt_gw_main.SP_Create_Merge_Rename_ZoomLevel(GT_DB,@zoomlevel,'rpt_tile_start_high_med_def');
				CALL gt_gw_main.SP_Create_Merge_Rename_ZoomLevel(GT_DB,@zoomlevel,'rpt_tile_start_high_med_dy');
				CALL gt_gw_main.SP_Create_Merge_Rename_ZoomLevel(GT_DB,@zoomlevel,'rpt_tile_start_high_med_dy_def');
	
				CALL gt_gw_main.SP_Create_Merge_Rename_ZoomLevel(GT_DB,@zoomlevel,'rpt_tile_end_high_med');
				CALL gt_gw_main.SP_Create_Merge_Rename_ZoomLevel(GT_DB,@zoomlevel,'rpt_tile_end_high_med_def');
				CALL gt_gw_main.SP_Create_Merge_Rename_ZoomLevel(GT_DB,@zoomlevel,'rpt_tile_end_high_med_dy');
				CALL gt_gw_main.SP_Create_Merge_Rename_ZoomLevel(GT_DB,@zoomlevel,'rpt_tile_end_high_med_dy_def');
		
				CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,CONCAT('rpt_cell_tile',@zoomlevel,'_position_high_med'));
				CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,CONCAT('rpt_cell_tile',@zoomlevel,'_position_high_med_def'));
	
				CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,CONCAT('rpt_cell_tile',@zoomlevel,'_start_high_med'));
				CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,CONCAT('rpt_cell_tile',@zoomlevel,'_start_high_med_def'));
	
				CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,CONCAT('rpt_cell_tile',@zoomlevel,'_end_high_med'));
				CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,CONCAT('rpt_cell_tile',@zoomlevel,'_end_high_med_def'));
	
				CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,CONCAT('rpt_tile',@zoomlevel,'_position_high_med'));
				CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,CONCAT('rpt_tile',@zoomlevel,'_position_high_med_def'));
	
				CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,CONCAT('rpt_tile',@zoomlevel,'_start_high_med'));
				CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,CONCAT('rpt_tile',@zoomlevel,'_start_high_med_def'));
	
				CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,CONCAT('rpt_tile',@zoomlevel,'_end_high_med'));
				CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,CONCAT('rpt_tile',@zoomlevel,'_end_high_med_def'));
			END IF;
			
			
			SET v_i=1;
			WHILE v_i <= cr_count DO
			BEGIN
				SET @cr = CONCAT('_imsig',gt_covmo_csv_get(@special_imsig_SpecifiedIMSIRangeValue,v_i));
				
				CALL gt_gw_main.SP_Create_Merge_Rename_CR(GT_DB,@zoomlevel,'rpt_cell_tile_end_dy',@cr);
				CALL gt_gw_main.SP_Create_Merge_Rename_CR(GT_DB,@zoomlevel,'rpt_cell_tile_end_dy_def',@cr);
				CALL gt_gw_main.SP_Create_Merge_Rename_CR(GT_DB,@zoomlevel,'rpt_cell_tile_position_dy',@cr);
				CALL gt_gw_main.SP_Create_Merge_Rename_CR(GT_DB,@zoomlevel,'rpt_cell_tile_position_dy_def',@cr);
				CALL gt_gw_main.SP_Create_Merge_Rename_CR(GT_DB,@zoomlevel,'rpt_cell_tile_start_dy',@cr);
				CALL gt_gw_main.SP_Create_Merge_Rename_CR(GT_DB,@zoomlevel,'rpt_cell_tile_start_dy_def',@cr);
				CALL gt_gw_main.SP_Create_Merge_Rename_CR(GT_DB,@zoomlevel,'rpt_tile_end_dy',@cr);
				CALL gt_gw_main.SP_Create_Merge_Rename_CR(GT_DB,@zoomlevel,'rpt_tile_end_dy_def',@cr);
				CALL gt_gw_main.SP_Create_Merge_Rename_CR(GT_DB,@zoomlevel,'rpt_tile_position_dy',@cr);
				CALL gt_gw_main.SP_Create_Merge_Rename_CR(GT_DB,@zoomlevel,'rpt_tile_position_dy_def',@cr);
				CALL gt_gw_main.SP_Create_Merge_Rename_CR(GT_DB,@zoomlevel,'rpt_tile_start_dy',@cr);
				CALL gt_gw_main.SP_Create_Merge_Rename_CR(GT_DB,@zoomlevel,'rpt_tile_start_dy_def',@cr);
				
				
	
				
				CALL gt_gw_main.SP_Create_Merge_Rename_24HI(GT_DB,CONCAT('rpt_cell_tile',@zoomlevel,'_end',@cr),@cr,'rpt_cell_tile_end');
				CALL gt_gw_main.SP_Create_Merge_Rename_24HI(GT_DB,CONCAT('rpt_cell_tile',@zoomlevel,'_end_def',@cr),@cr,'rpt_cell_tile_end_def');
				CALL gt_gw_main.SP_Create_Merge_Rename_24HI(GT_DB,CONCAT('rpt_cell_tile',@zoomlevel,'_position',@cr),@cr,'rpt_cell_tile_position');
				CALL gt_gw_main.SP_Create_Merge_Rename_24HI(GT_DB,CONCAT('rpt_cell_tile',@zoomlevel,'_position_def',@cr),@cr,'rpt_cell_tile_position_def');
				CALL gt_gw_main.SP_Create_Merge_Rename_24HI(GT_DB,CONCAT('rpt_cell_tile',@zoomlevel,'_start',@cr),@cr,'rpt_cell_tile_start');
				CALL gt_gw_main.SP_Create_Merge_Rename_24HI(GT_DB,CONCAT('rpt_cell_tile',@zoomlevel,'_start_def',@cr),@cr,'rpt_cell_tile_start_def');
				CALL gt_gw_main.SP_Create_Merge_Rename_24HI(GT_DB,CONCAT('rpt_tile',@zoomlevel,'_end',@cr),@cr,'rpt_cell_tile_end');
				CALL gt_gw_main.SP_Create_Merge_Rename_24HI(GT_DB,CONCAT('rpt_tile',@zoomlevel,'_end_def',@cr),@cr,'rpt_cell_tile_end_def');
				CALL gt_gw_main.SP_Create_Merge_Rename_24HI(GT_DB,CONCAT('rpt_tile',@zoomlevel,'_position',@cr),@cr,'rpt_cell_tile_position');
				CALL gt_gw_main.SP_Create_Merge_Rename_24HI(GT_DB,CONCAT('rpt_tile',@zoomlevel,'_position_def',@cr),@cr,'rpt_cell_tile_position_def');
				CALL gt_gw_main.SP_Create_Merge_Rename_24HI(GT_DB,CONCAT('rpt_tile',@zoomlevel,'_start',@cr),@cr,'rpt_cell_tile_start');
				CALL gt_gw_main.SP_Create_Merge_Rename_24HI(GT_DB,CONCAT('rpt_tile',@zoomlevel,'_start_def',@cr),@cr,'rpt_cell_tile_start_def');
				
				
	
				IF CQI_FLAG = 'true' THEN
					CALL gt_gw_main.SP_Create_Merge_Rename_CR(GT_DB,@zoomlevel,'rpt_cell_tile_cqi_dy',@cr);
					CALL gt_gw_main.SP_Create_Merge_Rename_CR(GT_DB,@zoomlevel,'rpt_cell_tile_cqi_dy_def',@cr);
					CALL gt_gw_main.SP_Create_Merge_Rename_CR(GT_DB,@zoomlevel,'rpt_tile_cqi_dy',@cr);
					CALL gt_gw_main.SP_Create_Merge_Rename_CR(GT_DB,@zoomlevel,'rpt_tile_cqi_dy_def',@cr);
					CALL gt_gw_main.SP_Create_Merge_Rename_24HI(GT_DB,CONCAT('rpt_cell_tile',@zoomlevel,'_cqi',@cr),@cr,'rpt_cell_tile_cqi');
					CALL gt_gw_main.SP_Create_Merge_Rename_24HI(GT_DB,CONCAT('rpt_cell_tile',@zoomlevel,'_cqi_def',@cr),@cr,'rpt_cell_tile_cqi_def');
					CALL gt_gw_main.SP_Create_Merge_Rename_24HI(GT_DB,CONCAT('rpt_tile',@zoomlevel,'_cqi',@cr),@cr,'rpt_cell_tile_cqi');
					CALL gt_gw_main.SP_Create_Merge_Rename_24HI(GT_DB,CONCAT('rpt_tile',@zoomlevel,'_cqi_def',@cr),@cr,'rpt_cell_tile_cqi_def');
				END IF;
				
				SET v_i=v_i+1;
			END;
			END WHILE;
	
			SET v_j=v_j+1;
		END;
		END WHILE;
		
		SET v_i=1;
		WHILE v_i <= cr_count DO
		BEGIN
			SET @cr = CONCAT('_imsig',gt_covmo_csv_get(@special_imsig_SpecifiedIMSIRangeValue,v_i));
			
			CALL gt_gw_main.SP_Create_Merge_Rename_CR(GT_DB,'','rpt_cell_end_dy',@cr);
			CALL gt_gw_main.SP_Create_Merge_Rename_CR(GT_DB,'','rpt_cell_end_dy_def',@cr);
			CALL gt_gw_main.SP_Create_Merge_Rename_CR(GT_DB,'','rpt_cell_imsi_end_dy',@cr);
			CALL gt_gw_main.SP_Create_Merge_Rename_CR(GT_DB,'','rpt_cell_imsi_end_dy_def',@cr);
			CALL gt_gw_main.SP_Create_Merge_Rename_CR(GT_DB,'','rpt_cell_imsi_ho_dy',@cr);
			CALL gt_gw_main.SP_Create_Merge_Rename_CR(GT_DB,'','rpt_cell_imsi_ho_dy_def',@cr);
			CALL gt_gw_main.SP_Create_Merge_Rename_CR(GT_DB,'','rpt_cell_imsi_srv_dy',@cr);
			CALL gt_gw_main.SP_Create_Merge_Rename_CR(GT_DB,'','rpt_cell_imsi_srv_dy_def',@cr);
			CALL gt_gw_main.SP_Create_Merge_Rename_CR(GT_DB,'','rpt_cell_imsi_start_dy',@cr);
			CALL gt_gw_main.SP_Create_Merge_Rename_CR(GT_DB,'','rpt_cell_imsi_start_dy_def',@cr);
			CALL gt_gw_main.SP_Create_Merge_Rename_CR(GT_DB,'','rpt_cell_position_dy',@cr);
			CALL gt_gw_main.SP_Create_Merge_Rename_CR(GT_DB,'','rpt_cell_position_dy_def',@cr);
			CALL gt_gw_main.SP_Create_Merge_Rename_CR(GT_DB,'','rpt_cell_start_dy',@cr);
			CALL gt_gw_main.SP_Create_Merge_Rename_CR(GT_DB,'','rpt_cell_start_dy_def',@cr);
			CALL gt_gw_main.SP_Create_Merge_Rename_CR(GT_DB,'','rpt_cell_ta_dy',@cr);
			CALL gt_gw_main.SP_Create_Merge_Rename_CR(GT_DB,'','rpt_pre_agg_dominate_cell_dy',@cr);
			CALL gt_gw_main.SP_Create_Merge_Rename_CR(GT_DB,'','rpt_volte_cell_end_dy',@cr);
			CALL gt_gw_main.SP_Create_Merge_Rename_CR(GT_DB,'','rpt_volte_cell_end_dy_def',@cr);
			CALL gt_gw_main.SP_Create_Merge_Rename_CR(GT_DB,'','rpt_volte_cell_start_dy',@cr);
			CALL gt_gw_main.SP_Create_Merge_Rename_CR(GT_DB,'','rpt_volte_cell_start_dy_def',@cr);
			CALL gt_gw_main.SP_Create_Merge_Rename_CR(GT_DB,'','rpt_volte_cell_tile_end_dy',@cr);
			CALL gt_gw_main.SP_Create_Merge_Rename_CR(GT_DB,'','rpt_volte_cell_tile_end_dy_def',@cr);
			CALL gt_gw_main.SP_Create_Merge_Rename_CR(GT_DB,'','rpt_volte_cell_tile_start_dy',@cr);
			CALL gt_gw_main.SP_Create_Merge_Rename_CR(GT_DB,'','rpt_volte_cell_tile_start_dy_def',@cr);
			CALL gt_gw_main.SP_Create_Merge_Rename_CR(GT_DB,'','rpt_volte_tile_end_dy',@cr);
			CALL gt_gw_main.SP_Create_Merge_Rename_CR(GT_DB,'','rpt_volte_tile_end_dy_def',@cr);
			CALL gt_gw_main.SP_Create_Merge_Rename_CR(GT_DB,'','rpt_volte_tile_start_dy',@cr);
			CALL gt_gw_main.SP_Create_Merge_Rename_CR(GT_DB,'','rpt_volte_tile_start_dy_def',@cr);
	
			CALL gt_gw_main.SP_Create_Merge_Rename_CR(GT_DB,'','rpt_cell_erab_start_dy',@cr);
			CALL gt_gw_main.SP_Create_Merge_Rename_CR(GT_DB,'','rpt_cell_erab_start_dy_def',@cr);
			CALL gt_gw_main.SP_Create_Merge_Rename_CR(GT_DB,'','rpt_cell_erab_end_dy',@cr);
			CALL gt_gw_main.SP_Create_Merge_Rename_CR(GT_DB,'','rpt_cell_erab_end_dy_def',@cr);
	
			
			CALL gt_gw_main.SP_Create_Merge_Rename_24HI(GT_DB,CONCAT('rpt_cell_dominatecallcell',@cr),@cr,'rpt_cell_dominatecallcell');
			CALL gt_gw_main.SP_Create_Merge_Rename_24HI(GT_DB,CONCAT('rpt_cell_dominatersrpcell',@cr),@cr,'rpt_cell_dominatersrpcell');
			CALL gt_gw_main.SP_Create_Merge_Rename_24HI(GT_DB,CONCAT('rpt_cell_end',@cr),@cr,'rpt_cell_end');
			CALL gt_gw_main.SP_Create_Merge_Rename_24HI(GT_DB,CONCAT('rpt_cell_end_def',@cr),@cr,'rpt_cell_end_def');
			CALL gt_gw_main.SP_Create_Merge_Rename_24HI(GT_DB,CONCAT('rpt_cell_imsi_end',@cr),@cr,'rpt_cell_imsi_end');
			CALL gt_gw_main.SP_Create_Merge_Rename_24HI(GT_DB,CONCAT('rpt_cell_imsi_end_def',@cr),@cr,'rpt_cell_imsi_end_def');
			CALL gt_gw_main.SP_Create_Merge_Rename_24HI(GT_DB,CONCAT('rpt_cell_imsi_ho',@cr),@cr,'rpt_cell_imsi_ho');
			CALL gt_gw_main.SP_Create_Merge_Rename_24HI(GT_DB,CONCAT('rpt_cell_imsi_ho_def',@cr),@cr,'rpt_cell_imsi_ho_def');
			CALL gt_gw_main.SP_Create_Merge_Rename_24HI(GT_DB,CONCAT('rpt_cell_imsi_srv',@cr),@cr,'rpt_cell_imsi_srv');
			CALL gt_gw_main.SP_Create_Merge_Rename_24HI(GT_DB,CONCAT('rpt_cell_imsi_srv_def',@cr),@cr,'rpt_cell_imsi_srv_def');
			CALL gt_gw_main.SP_Create_Merge_Rename_24HI(GT_DB,CONCAT('rpt_cell_imsi_start',@cr),@cr,'rpt_cell_imsi_start');
			CALL gt_gw_main.SP_Create_Merge_Rename_24HI(GT_DB,CONCAT('rpt_cell_imsi_start_def',@cr),@cr,'rpt_cell_imsi_start_def');
			CALL gt_gw_main.SP_Create_Merge_Rename_24HI(GT_DB,CONCAT('rpt_cell_position',@cr),@cr,'rpt_cell_position');
			CALL gt_gw_main.SP_Create_Merge_Rename_24HI(GT_DB,CONCAT('rpt_cell_position_def',@cr),@cr,'rpt_cell_position_def');
			CALL gt_gw_main.SP_Create_Merge_Rename_24HI(GT_DB,CONCAT('rpt_cell_start',@cr),@cr,'rpt_cell_start');
			CALL gt_gw_main.SP_Create_Merge_Rename_24HI(GT_DB,CONCAT('rpt_cell_start_def',@cr),@cr,'rpt_cell_start_def');
			CALL gt_gw_main.SP_Create_Merge_Rename_24HI(GT_DB,CONCAT('rpt_cell_ta',@cr),@cr,'rpt_cell_ta');
			CALL gt_gw_main.SP_Create_Merge_Rename_24HI(GT_DB,CONCAT('rpt_cell_type_dominatecallcell',@cr),@cr,'rpt_cell_type_dominatecallcell');
			CALL gt_gw_main.SP_Create_Merge_Rename_24HI(GT_DB,CONCAT('rpt_cell_type_dominatersrpcell',@cr),@cr,'rpt_cell_type_dominatersrpcell');
			CALL gt_gw_main.SP_Create_Merge_Rename_24HI(GT_DB,CONCAT('rpt_pre_agg_dominate_cell',@cr),@cr,'rpt_pre_agg_dominate_cell');
			CALL gt_gw_main.SP_Create_Merge_Rename_24HI(GT_DB,CONCAT('rpt_tile_dominatecallcell',@cr),@cr,'rpt_tile_dominatecallcell');
			CALL gt_gw_main.SP_Create_Merge_Rename_24HI(GT_DB,CONCAT('rpt_tile_dominatecallcell_def',@cr),@cr,'rpt_tile_dominatecallcell_def');
			CALL gt_gw_main.SP_Create_Merge_Rename_24HI(GT_DB,CONCAT('rpt_tile_dominatersrpcell',@cr),@cr,'rpt_tile_dominatersrpcell');
			CALL gt_gw_main.SP_Create_Merge_Rename_24HI(GT_DB,CONCAT('rpt_tile_dominatersrpcell_def',@cr),@cr,'rpt_tile_dominatersrpcell_def');
			CALL gt_gw_main.SP_Create_Merge_Rename_24HI(GT_DB,CONCAT('rpt_tile_type_dominatecallcell',@cr),@cr,'rpt_tile_type_dominatecallcell');
			CALL gt_gw_main.SP_Create_Merge_Rename_24HI(GT_DB,CONCAT('rpt_tile_type_dominatersrpcell',@cr),@cr,'rpt_tile_type_dominatersrpcell');
			CALL gt_gw_main.SP_Create_Merge_Rename_24HI(GT_DB,CONCAT('rpt_volte_cell_end',@cr),@cr,'rpt_volte_cell_end');
			CALL gt_gw_main.SP_Create_Merge_Rename_24HI(GT_DB,CONCAT('rpt_volte_cell_end_def',@cr),@cr,'rpt_volte_cell_end_def');
			CALL gt_gw_main.SP_Create_Merge_Rename_24HI(GT_DB,CONCAT('rpt_volte_cell_start',@cr),@cr,'rpt_volte_cell_start');
			CALL gt_gw_main.SP_Create_Merge_Rename_24HI(GT_DB,CONCAT('rpt_volte_cell_start_def',@cr),@cr,'rpt_volte_cell_start_def');
			CALL gt_gw_main.SP_Create_Merge_Rename_24HI(GT_DB,CONCAT('rpt_volte_cell_tile_end',@cr),@cr,'rpt_volte_cell_tile_end');
			CALL gt_gw_main.SP_Create_Merge_Rename_24HI(GT_DB,CONCAT('rpt_volte_cell_tile_end_def',@cr),@cr,'rpt_volte_cell_tile_end_def');
			CALL gt_gw_main.SP_Create_Merge_Rename_24HI(GT_DB,CONCAT('rpt_volte_cell_tile_start',@cr),@cr,'rpt_volte_cell_tile_start');
			CALL gt_gw_main.SP_Create_Merge_Rename_24HI(GT_DB,CONCAT('rpt_volte_cell_tile_start_def',@cr),@cr,'rpt_volte_cell_tile_start_def');
			CALL gt_gw_main.SP_Create_Merge_Rename_24HI(GT_DB,CONCAT('rpt_volte_tile_end',@cr),@cr,'rpt_volte_tile_end');
			CALL gt_gw_main.SP_Create_Merge_Rename_24HI(GT_DB,CONCAT('rpt_volte_tile_end_def',@cr),@cr,'rpt_volte_tile_end_def');
			CALL gt_gw_main.SP_Create_Merge_Rename_24HI(GT_DB,CONCAT('rpt_volte_tile_start',@cr),@cr,'rpt_volte_tile_start');
			CALL gt_gw_main.SP_Create_Merge_Rename_24HI(GT_DB,CONCAT('rpt_volte_tile_start_def',@cr),@cr,'rpt_volte_tile_start_def');
	
			CALL gt_gw_main.SP_Create_Merge_Rename_24HI(GT_DB,CONCAT('rpt_cell_erab_start',@cr),@cr,'rpt_cell_erab_start');
			CALL gt_gw_main.SP_Create_Merge_Rename_24HI(GT_DB,CONCAT('rpt_cell_erab_start_def',@cr),@cr,'rpt_cell_erab_start_def');
			CALL gt_gw_main.SP_Create_Merge_Rename_24HI(GT_DB,CONCAT('rpt_cell_erab_end',@cr),@cr,'rpt_cell_erab_start');
			CALL gt_gw_main.SP_Create_Merge_Rename_24HI(GT_DB,CONCAT('rpt_cell_erab_end_def',@cr),@cr,'rpt_cell_erab_start_def');
			
			IF CQI_FLAG = 'true' THEN
				CALL gt_gw_main.SP_Create_Merge_Rename_CR(GT_DB,'','rpt_cell_cqi_dy',@cr);
				CALL gt_gw_main.SP_Create_Merge_Rename_CR(GT_DB,'','rpt_cell_cqi_dy_def',@cr);
				CALL gt_gw_main.SP_Create_Merge_Rename_24HI(GT_DB,CONCAT('rpt_cell_cqi',@cr),@cr,'rpt_cell_cqi');
				CALL gt_gw_main.SP_Create_Merge_Rename_24HI(GT_DB,CONCAT('rpt_cell_cqi_def',@cr),@cr,'rpt_cell_cqi_def');
			END IF;
			
			SET v_i=v_i+1;
		END;
		END WHILE;
	
		IF NBIoT_FLAG = 'true' THEN
			CALL gt_gw_main.SP_Create_Merge_Rename_24Q(GT_DB,'table_nbiot_call_lte');
			CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,'rpt_cell_nbiot');
			CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,'rpt_cell_nbiot_def');
			CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,'rpt_tile_nbiot');
			CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,'rpt_tile_nbiot_def');
			CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,'rpt_cell_tile_nbiot');
			CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,'rpt_cell_tile_nbiot_def');
		END IF;
	
		CALL gt_gw_main.SP_Create_Merge_Rename_24Q(GT_DB,'table_call_lte');
		CALL gt_gw_main.SP_Create_Merge_Rename_24Q(GT_DB,'table_call_imsi_lte');
		CALL gt_gw_main.SP_Create_Merge_Rename_24Q(GT_DB,'table_roamer_call_lte');
		CALL gt_gw_main.SP_Create_Merge_Rename_24Q(GT_DB,'table_call_failure_lte');
		CALL gt_gw_main.SP_Create_Merge_Rename_24Q(GT_DB,'table_position_convert_ho_lte');
		CALL gt_gw_main.SP_Create_Merge_Rename_24Q(GT_DB,'table_position_convert_serving_lte');
		CALL gt_gw_main.SP_Create_Merge_Rename_24Q(GT_DB,'table_position_convert_ho_failure_lte');
		CALL gt_gw_main.SP_Create_Merge_Rename_24Q(GT_DB,'table_position_convert_serving_failure_lte');
		CALL gt_gw_main.SP_Create_Merge_Rename_24Q(GT_DB,'table_erab_volte_lte');
		CALL gt_gw_main.SP_Create_Merge_Rename_24Q(GT_DB,'table_cell_lte');
		CALL gt_gw_main.SP_Create_Merge_Rename_24Q(GT_DB,'table_protocol_failure_event_lte');
		CALL gt_gw_main.SP_Create_Merge_Rename_24Q(GT_DB,'table_erab_lte');
		CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,'rpt_cell_tile_position');
		CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,'rpt_cell_tile_position_def');
		CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,'rpt_tile_position');
		CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,'rpt_tile_position_def');
		
		
		IF IMSI_TABLE_FLAG = 'true' THEN 
		CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,'rpt_cell_imsi_end');
		CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,'rpt_cell_imsi_end_def');
		CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,'rpt_cell_imsi_ho');
		CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,'rpt_cell_imsi_ho_def');
		CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,'rpt_cell_imsi_srv');
		CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,'rpt_cell_imsi_srv_def');
		CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,'rpt_cell_imsi_start');
		CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,'rpt_cell_imsi_start_def');
		END IF;
		
		
		
		CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,'rpt_cell_lte');
		CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,'rpt_cell_position');
		CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,'rpt_cell_position_def');
		CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,'rpt_cell_start');
		CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,'rpt_cell_start_def');
		CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,'rpt_cell_end');
		CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,'rpt_cell_end_def');
		CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,'rpt_tile_start');
		CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,'rpt_tile_start_def');
		CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,'rpt_tile_end');
		CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,'rpt_tile_end_def');
		CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,'rpt_cell_tile_start');
		CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,'rpt_cell_tile_start_def');
		CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,'rpt_cell_tile_end');
		CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,'rpt_cell_tile_end_def');
		CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,'opt_nbr_inter_intra_lte');	
		CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,'opt_nbr_irat3G_lte');
		CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,'opt_cf_pre_agg_report_lte');
		CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,'rpt_pre_agg_dominate_cell');
		CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,'rpt_volte_cell_end');
		CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,'rpt_volte_cell_end_def');
		CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,'rpt_volte_cell_start');
		CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,'rpt_volte_cell_start_def');
		CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,'rpt_volte_cell_tile_end');
		CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,'rpt_volte_cell_tile_end_def');
		CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,'rpt_volte_cell_tile_start');
		CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,'rpt_volte_cell_tile_start_def');
		CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,'rpt_volte_tile_end');
		CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,'rpt_volte_tile_end_def');
		CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,'rpt_volte_tile_start');
		CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,'rpt_volte_tile_start_def');
		CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,'rpt_volte_handset');
	
		CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,'rpt_tile_type_dominatecallcell');
		CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,'rpt_tile_dominatecallcell');
		CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,'rpt_tile_dominatecallcell_def');
		CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,'rpt_tile_type_dominatersrpcell');
		CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,'rpt_tile_dominatersrpcell');
		CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,'rpt_tile_dominatersrpcell_def');
	
		CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,'rpt_cell_type_dominatecallcell');
		CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,'rpt_cell_dominatecallcell');
		CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,'rpt_cell_type_dominatersrpcell');
		CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,'rpt_cell_dominatersrpcell');
	
		CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,'rpt_cell_ta');		
		CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,'table_imsi_aggregated_hr');
	
		CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,'opt_aco_traffic_lte');
	
		CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,'rpt_cell_erab_start');
		CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,'rpt_cell_erab_start_def');
		CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,'rpt_cell_tile_erab_start');
		CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,'rpt_cell_tile_erab_start_def');
		CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,'rpt_tile_erab_start');
		CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,'rpt_tile_erab_start_def');
	
		CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,'rpt_cell_erab_end');
		CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,'rpt_cell_erab_end_def');
		CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,'rpt_cell_tile_erab_end');
		CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,'rpt_cell_tile_erab_end_def');
		CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,'rpt_tile_erab_end');
		CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,'rpt_tile_erab_end_def');
		
		IF TAC_REPORT_FLAG = 'true' THEN 
			CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,'rpt_tac_start');
			CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,'rpt_tac_start_def');
			CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,'rpt_tac_end');
			CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,'rpt_tac_end_def');
		END IF;
		
		IF CQI_FLAG = 'true' THEN
			CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,'rpt_cell_cqi');
			CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,'rpt_cell_cqi_def');
			CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,'rpt_tile_cqi');
			CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,'rpt_tile_cqi_def');
			CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,'rpt_cell_tile_cqi');
			CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,'rpt_cell_tile_cqi_def');
		END IF;
	
		IF PMC_FLAG = 'true' THEN
			CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,'rpt_cell_pmc_lte');
		END IF;
		
		IF MDT_FLAG = 'true' THEN
			CALL gt_gw_main.SP_Create_Merge_Rename_24Q(GT_DB,'table_position_convert_mdt_lte');
			CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,'rpt_cell_tile_mdt');
			CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,'rpt_cell_mdt');
			CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,'rpt_tile_mdt');
		END IF;
		
		IF PM_COUNTER_FLAG = 'true' THEN
			CALL gt_gw_main.SP_Create_Merge_Rename_24Q(GT_DB,'table_pm_ericsson_lte');
			CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,'table_pm_ericsson_lte_aggr');
			CALL gt_gw_main.SP_Create_Merge_Rename_24Q(GT_DB,'table_pm_pdf_ericsson_lte');
			CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,'table_pm_pdf_ericsson_lte_aggr');
		END IF;
	
		IF RPT_HIGH_FLAG = 'true' THEN 
			CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,'rpt_cell_tile_position_high');
			CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,'rpt_cell_tile_position_high_def');
			CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,'rpt_tile_position_high');
			CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,'rpt_tile_position_high_def');
	
			CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,'rpt_cell_position_high');
			CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,'rpt_cell_position_high_def');
	
			CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,'rpt_cell_start_high');
			CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,'rpt_cell_start_high_def');
			CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,'rpt_tile_start_high');
			CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,'rpt_tile_start_high_def');
	
			CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,'rpt_cell_tile_start_high');
			CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,'rpt_cell_tile_start_high_def');
	
			CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,'rpt_cell_end_high');
			CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,'rpt_cell_end_high_def');
			CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,'rpt_tile_end_high');
			CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,'rpt_tile_end_high_def');
	
			CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,'rpt_cell_tile_end_high');
			CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,'rpt_cell_tile_end_high_def');
		END IF;
		
		IF RPT_HIGH_MED_FLAG = 'true' THEN 
			CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,'rpt_cell_tile_position_high_med');
			CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,'rpt_cell_tile_position_high_med_def');
			CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,'rpt_tile_position_high_med');
			CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,'rpt_tile_position_high_med_def');
	
			CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,'rpt_cell_position_high_med');
			CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,'rpt_cell_position_high_med_def');
	
			CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,'rpt_cell_start_high_med');
			CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,'rpt_cell_start_high_med_def');
			CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,'rpt_tile_start_high_med');
			CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,'rpt_tile_start_high_med_def');
	
			CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,'rpt_cell_tile_start_high_med');
			CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,'rpt_cell_tile_start_high_med_def');
	
			CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,'rpt_cell_end_high_med');
			CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,'rpt_cell_end_high_med_def');
			CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,'rpt_tile_end_high_med');
			CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,'rpt_tile_end_high_med_def');
	
			CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,'rpt_cell_tile_end_high_med');
			CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,'rpt_cell_tile_end_high_med_def');
		END IF;
	ELSE 
		SET @SqlCmd=CONCAT('SELECT COUNT(DISTINCT table_name) INTO @tbl_num2 FROM information_schema.TABLES 
					WHERE table_schema = ''',GT_DB,''' AND table_name LIKE ''table_call_lte_update_%'';');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
	
		SET tbl_num = @tbl_num2;
	
		SET qry_tbl_name='';
		SET qry_tbl_name_pos_ho='';
		SET qry_tbl_name_pos_srv='';
		SET v_j=1;
		
		WHILE v_j <= tbl_num DO
		BEGIN	
			SET qry_tbl_name=CONCAT('table_call_lte_',QRT,'_',v_j);
			
			SET @SqlCmd=CONCAT('RENAME TABLE ',GT_DB,'.table_call_lte_update_',v_j,' TO ',GT_DB,'.',qry_tbl_name,';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
						
			IF UNION_STR_CALL IS NULL THEN
				SET UNION_STR_CALL = CONCAT(GT_DB,'.',qry_tbl_name,'');
			ELSE
				SET UNION_STR_CALL = CONCAT(UNION_STR_CALL,',',GT_DB,'.',qry_tbl_name,'');
			END IF;	
	
			SET qry_tbl_name_erab=CONCAT('table_erab_volte_lte_',QRT,'_',v_j);
			
			SET @SqlCmd=CONCAT('RENAME TABLE ',GT_DB,'.table_erab_volte_lte_',v_j,' TO ',GT_DB,'.',qry_tbl_name_erab,';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
			
			IF UNION_STR_CALL IS NULL THEN
				SET UNION_STR_CALL = CONCAT(GT_DB,'.',qry_tbl_name_erab,'');
			ELSE
				SET UNION_STR_CALL = CONCAT(UNION_STR_CALL,',',GT_DB,'.',qry_tbl_name_erab,'');
			END IF;	
			
			SET qry_tbl_name_pos_ho=CONCAT('table_position_convert_ho_lte_',QRT,'_',v_j);
	
			SET @SqlCmd=CONCAT('RENAME TABLE ',GT_DB,'.table_position_convert_ho_lte_',v_j,' TO ',GT_DB,'.',qry_tbl_name_pos_ho,';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
						
			IF UNION_STR_POS_HO IS NULL THEN
				SET UNION_STR_POS_HO = CONCAT(GT_DB,'.',qry_tbl_name_pos_ho,'');
			ELSE
				SET UNION_STR_POS_HO = CONCAT(UNION_STR_POS_HO,',',GT_DB,'.',qry_tbl_name_pos_ho,'');
			END IF;	
			
			SET qry_tbl_name_pos_srv=CONCAT('table_position_convert_serving_lte_',QRT,'_',v_j);
			SET @SqlCmd=CONCAT('RENAME TABLE ',GT_DB,'.table_position_convert_serving_lte_',v_j,' TO ',GT_DB,'.',qry_tbl_name_pos_srv,';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
						
			IF UNION_STR_POS_SRV IS NULL THEN
				SET UNION_STR_POS_SRV = CONCAT(GT_DB,'.',qry_tbl_name_pos_srv,'');
			ELSE
				SET UNION_STR_POS_SRV = CONCAT(UNION_STR_POS_SRV,',',GT_DB,'.',qry_tbl_name_pos_srv,'');
			END IF;	
					
			SET v_j=v_j+1;
		END;
		END WHILE;
	END IF;
	INSERT INTO gt_gw_main.SP_LOG VALUES(GT_DB,'SP_Create_Merge_Rename_Table_LTE',CONCAT('Done: ',TIMESTAMPDIFF(SECOND,START_TIME,SYSDATE()),' seconds.'), NOW());
