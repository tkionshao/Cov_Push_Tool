CREATE DEFINER=`covmo`@`%` PROCEDURE `SP_Create_Merge_Rename_Table`(IN GT_DB VARCHAR(100),IN FLAG CHAR(2),IN EVENT_NUM TINYINT(2),IN POS_NUM TINYINT(2))
BEGIN
	DECLARE START_TIME DATETIME DEFAULT SYSDATE();
	DECLARE SPECIAL_IMSI VARCHAR(50);
	DECLARE imsig_count SMALLINT(6) DEFAULT 0;
	DECLARE v_i SMALLINT(6);
	DECLARE v_j SMALLINT(6);
	DECLARE cr_count SMALLINT(6) DEFAULT 0;
	DECLARE CURRENT_NT_DB VARCHAR(50) DEFAULT CONCAT('gt_nt_',gt_strtok(GT_DB,3,'_'));
	DECLARE TAC_REPORT_FLAG VARCHAR(10);
	DECLARE MDT_FLAG VARCHAR(10);
	
	SELECT LOWER(`value`) INTO TAC_REPORT_FLAG  FROM gt_gw_main.integration_param WHERE gt_group = 'sp' AND gt_name = '01.tac.report' ;
	SELECT LOWER(`value`) INTO MDT_FLAG FROM gt_gw_main.integration_param WHERE gt_group = 'sp' AND gt_name = 'mdt' ;
	
	
	SET @SqlCmd=CONCAT('SELECT LOWER(att_value) INTO @SPECIAL_IMSI_CR FROM ',CURRENT_NT_DB,'.sys_config WHERE group_name = ''System'' AND tech_mask = 7 AND att_name = ''DataProcessForImsiRange'';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('SELECT att_value INTO @CR_THRESHOLD FROM ',CURRENT_NT_DB,'.sys_config WHERE group_name = ''System'' AND tech_mask = 7 AND att_name = ''SpecifiedIMSIRangeValue'';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	IF @SPECIAL_IMSI_CR = 'true' THEN
		SET @SqlCmd=CONCAT('SELECT GROUP_CONCAT(DISTINCT (ABS(`GROUP_ID`))) INTO @special_imsig_SpecifiedIMSIRangeValue FROM `gt_covmo`.`dim_imsi_group` WHERE GROUP_ID < ',@CR_THRESHOLD,';');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
	END IF;
	
	SET cr_count = gt_covmo_csv_count(@special_imsig_SpecifiedIMSIRangeValue,',');
	
	
	SELECT LOWER(`value`) INTO SPECIAL_IMSI  FROM gt_gw_main.integration_param WHERE gt_group = 'sp' AND gt_name = 'special_imsi' ;
	SELECT GROUP_CONCAT(DISTINCT (ABS(`GROUP_ID`))) INTO @imsig_value FROM `gt_covmo`.`dim_imsi_group` WHERE GROUP_ID < 0 AND GROUP_ID >= -10000;
	INSERT INTO gt_gw_main.SP_LOG VALUES(GT_DB,'SP_Create_Merge_Rename_Table','Start', START_TIME);
	
	IF FLAG='DY' THEN 
	
		IF SPECIAL_IMSI = 'true' THEN
			SET imsig_count = gt_covmo_csv_count(@imsig_value,',');
			SET v_j=1;
			WHILE v_j <= imsig_count DO
			BEGIN	
				SET @group_id = gt_covmo_csv_get(@imsig_value,v_j);
				SET @group_bottom_str = CONCAT('_imsig',@group_id);
				
				CALL gt_gw_main.SP_Create_Merge_Rename_24QI(GT_DB,CONCAT('table_call',@group_bottom_str),@group_bottom_str,'table_call_imsig');
				CALL gt_gw_main.SP_Create_Merge_Rename_24QI(GT_DB,CONCAT('table_position',@group_bottom_str),@group_bottom_str,'table_position_imsig');
	
				SET v_j=v_j+1;
			END;
			END WHILE;
		END IF;	
	
		
		
		IF @SPECIAL_IMSI_CR = 'true' AND cr_count > 0 THEN
			SET v_i=1;
			WHILE v_i <= cr_count DO
			BEGIN
		
				SET @cr = CONCAT('_imsig',gt_covmo_csv_get(@special_imsig_SpecifiedIMSIRangeValue,v_i));
				
				CALL gt_gw_main.SP_Create_Merge_Rename_CR(GT_DB,'','table_tile_1st_server_dy',@cr);
				CALL gt_gw_main.SP_Create_Merge_Rename_CR(GT_DB,'','table_tile_1st_server_dy_def',@cr);
				CALL gt_gw_main.SP_Create_Merge_Rename_CR(GT_DB,'','table_tile_1st_server_dy_c',@cr);
				CALL gt_gw_main.SP_Create_Merge_Rename_CR(GT_DB,'','table_tile_1st_server_dy_c_def',@cr);
				CALL gt_gw_main.SP_Create_Merge_Rename_CR(GT_DB,'','table_tile_1st_server_dy_t',@cr);
				CALL gt_gw_main.SP_Create_Merge_Rename_CR(GT_DB,'','table_tile_1st_server_dy_t_def',@cr);
				CALL gt_gw_main.SP_Create_Merge_Rename_CR(GT_DB,'','table_tile_end_dy',@cr);
				CALL gt_gw_main.SP_Create_Merge_Rename_CR(GT_DB,'','table_tile_end_dy_def',@cr);
				CALL gt_gw_main.SP_Create_Merge_Rename_CR(GT_DB,'','table_tile_end_dy_c',@cr);
				CALL gt_gw_main.SP_Create_Merge_Rename_CR(GT_DB,'','table_tile_end_dy_c_def',@cr);
				CALL gt_gw_main.SP_Create_Merge_Rename_CR(GT_DB,'','table_tile_end_dy_t',@cr);
				CALL gt_gw_main.SP_Create_Merge_Rename_CR(GT_DB,'','table_tile_end_dy_t_def',@cr);
				CALL gt_gw_main.SP_Create_Merge_Rename_CR(GT_DB,'','table_tile_fp_dy',@cr);
				CALL gt_gw_main.SP_Create_Merge_Rename_CR(GT_DB,'','table_tile_fp_dy_def',@cr);
				CALL gt_gw_main.SP_Create_Merge_Rename_CR(GT_DB,'','table_tile_fp_dy_c',@cr);
				CALL gt_gw_main.SP_Create_Merge_Rename_CR(GT_DB,'','table_tile_fp_dy_c_def',@cr);
				CALL gt_gw_main.SP_Create_Merge_Rename_CR(GT_DB,'','table_tile_fp_dy_t',@cr);
				CALL gt_gw_main.SP_Create_Merge_Rename_CR(GT_DB,'','table_tile_fp_dy_t_def',@cr);
				CALL gt_gw_main.SP_Create_Merge_Rename_CR(GT_DB,'','table_tile_imsi_start_dy_c',@cr);
				CALL gt_gw_main.SP_Create_Merge_Rename_CR(GT_DB,'','table_tile_imsi_start_dy_c_def',@cr);
				CALL gt_gw_main.SP_Create_Merge_Rename_CR(GT_DB,'','table_tile_start_dy',@cr);
				CALL gt_gw_main.SP_Create_Merge_Rename_CR(GT_DB,'','table_tile_start_dy_def',@cr);
				CALL gt_gw_main.SP_Create_Merge_Rename_CR(GT_DB,'','table_tile_start_dy_c',@cr);
				CALL gt_gw_main.SP_Create_Merge_Rename_CR(GT_DB,'','table_tile_start_dy_c_def',@cr);
				CALL gt_gw_main.SP_Create_Merge_Rename_CR(GT_DB,'','table_tile_start_dy_t',@cr);
				CALL gt_gw_main.SP_Create_Merge_Rename_CR(GT_DB,'','table_tile_start_dy_t_def',@cr);
				CALL gt_gw_main.SP_Create_Merge_Rename_CR(GT_DB,'','table_tile_ue_dy',@cr);
				CALL gt_gw_main.SP_Create_Merge_Rename_CR(GT_DB,'','table_tile_ue_dy_def',@cr);
				CALL gt_gw_main.SP_Create_Merge_Rename_CR(GT_DB,'','table_tile_ue_dy_c',@cr);
				CALL gt_gw_main.SP_Create_Merge_Rename_CR(GT_DB,'','table_tile_ue_dy_c_def',@cr);
				CALL gt_gw_main.SP_Create_Merge_Rename_CR(GT_DB,'','table_tile_ue_dy_t',@cr);
				CALL gt_gw_main.SP_Create_Merge_Rename_CR(GT_DB,'','table_tile_ue_dy_t_def',@cr);
				
				CALL gt_gw_main.SP_Create_Merge_Rename_24HI(GT_DB,CONCAT('table_tile_1st_server',@cr),@cr,'table_tile_1st_server');
				CALL gt_gw_main.SP_Create_Merge_Rename_24HI(GT_DB,CONCAT('table_tile_1st_server_def',@cr),@cr,'table_tile_1st_server_def');
				CALL gt_gw_main.SP_Create_Merge_Rename_24HI(GT_DB,CONCAT('table_tile_1st_server_c',@cr),@cr,'table_tile_1st_server_c');
				CALL gt_gw_main.SP_Create_Merge_Rename_24HI(GT_DB,CONCAT('table_tile_1st_server_c_def',@cr),@cr,'table_tile_1st_server_c_def');
				CALL gt_gw_main.SP_Create_Merge_Rename_24HI(GT_DB,CONCAT('table_tile_1st_server_t',@cr),@cr,'table_tile_1st_server_t');
				CALL gt_gw_main.SP_Create_Merge_Rename_24HI(GT_DB,CONCAT('table_tile_1st_server_t_def',@cr),@cr,'table_tile_1st_server_t_def');
				CALL gt_gw_main.SP_Create_Merge_Rename_24HI(GT_DB,CONCAT('table_tile_end',@cr),@cr,'table_tile_end');
				CALL gt_gw_main.SP_Create_Merge_Rename_24HI(GT_DB,CONCAT('table_tile_end_def',@cr),@cr,'table_tile_end_def');
				CALL gt_gw_main.SP_Create_Merge_Rename_24HI(GT_DB,CONCAT('table_tile_end_c',@cr),@cr,'table_tile_end_c');
				CALL gt_gw_main.SP_Create_Merge_Rename_24HI(GT_DB,CONCAT('table_tile_end_c_def',@cr),@cr,'table_tile_end_c_def');
				CALL gt_gw_main.SP_Create_Merge_Rename_24HI(GT_DB,CONCAT('table_tile_end_t',@cr),@cr,'table_tile_end_t');
				CALL gt_gw_main.SP_Create_Merge_Rename_24HI(GT_DB,CONCAT('table_tile_end_t_def',@cr),@cr,'table_tile_end_t_def');
				CALL gt_gw_main.SP_Create_Merge_Rename_24HI(GT_DB,CONCAT('table_tile_failure_cause',@cr),@cr,'table_tile_failure_cause');
				CALL gt_gw_main.SP_Create_Merge_Rename_24HI(GT_DB,CONCAT('table_tile_fp',@cr),@cr,'table_tile_fp');
				CALL gt_gw_main.SP_Create_Merge_Rename_24HI(GT_DB,CONCAT('table_tile_fp_def',@cr),@cr,'table_tile_fp_def');
				CALL gt_gw_main.SP_Create_Merge_Rename_24HI(GT_DB,CONCAT('table_tile_fp_c',@cr),@cr,'table_tile_fp_c');
				CALL gt_gw_main.SP_Create_Merge_Rename_24HI(GT_DB,CONCAT('table_tile_fp_c_def',@cr),@cr,'table_tile_fp_c_def');
				CALL gt_gw_main.SP_Create_Merge_Rename_24HI(GT_DB,CONCAT('table_tile_fp_t',@cr),@cr,'table_tile_fp_t');
				CALL gt_gw_main.SP_Create_Merge_Rename_24HI(GT_DB,CONCAT('table_tile_fp_t_def',@cr),@cr,'table_tile_fp_t_def');
				CALL gt_gw_main.SP_Create_Merge_Rename_24HI(GT_DB,CONCAT('table_tile_imsi_start_c',@cr),@cr,'table_tile_imsi_start_c');
				CALL gt_gw_main.SP_Create_Merge_Rename_24HI(GT_DB,CONCAT('table_tile_imsi_start_c_def',@cr),@cr,'table_tile_imsi_start_c_def');
				CALL gt_gw_main.SP_Create_Merge_Rename_24HI(GT_DB,CONCAT('table_tile_start',@cr),@cr,'table_tile_start');
				CALL gt_gw_main.SP_Create_Merge_Rename_24HI(GT_DB,CONCAT('table_tile_start_def',@cr),@cr,'table_tile_start_def');
				CALL gt_gw_main.SP_Create_Merge_Rename_24HI(GT_DB,CONCAT('table_tile_start_c',@cr),@cr,'table_tile_start_c');
				CALL gt_gw_main.SP_Create_Merge_Rename_24HI(GT_DB,CONCAT('table_tile_start_c_def',@cr),@cr,'table_tile_start_c_def');
				CALL gt_gw_main.SP_Create_Merge_Rename_24HI(GT_DB,CONCAT('table_tile_start_t',@cr),@cr,'table_tile_start_t');
				CALL gt_gw_main.SP_Create_Merge_Rename_24HI(GT_DB,CONCAT('table_tile_start_t_def',@cr),@cr,'table_tile_start_t_def');
				CALL gt_gw_main.SP_Create_Merge_Rename_24HI(GT_DB,CONCAT('table_tile_ue',@cr),@cr,'table_tile_ue');
				CALL gt_gw_main.SP_Create_Merge_Rename_24HI(GT_DB,CONCAT('table_tile_ue_def',@cr),@cr,'table_tile_ue_def');
				CALL gt_gw_main.SP_Create_Merge_Rename_24HI(GT_DB,CONCAT('table_tile_ue_c',@cr),@cr,'table_tile_ue_c');
				CALL gt_gw_main.SP_Create_Merge_Rename_24HI(GT_DB,CONCAT('table_tile_ue_c_def',@cr),@cr,'table_tile_ue_c_def');
				CALL gt_gw_main.SP_Create_Merge_Rename_24HI(GT_DB,CONCAT('table_tile_ue_t',@cr),@cr,'table_tile_ue_t');
				CALL gt_gw_main.SP_Create_Merge_Rename_24HI(GT_DB,CONCAT('table_tile_ue_t_def',@cr),@cr,'table_tile_ue_t_def');
				SET v_i=v_i+1;
			END;
			END WHILE;
		END IF;
		CALL gt_gw_main.SP_Create_Merge_Rename_24Q_and_HR(GT_DB,'table_call');
		CALL gt_gw_main.SP_Create_Merge_Rename_24Q(GT_DB,'table_roamer_call');
		CALL gt_gw_main.SP_Create_Merge_Rename_24Q(GT_DB,'table_position');
		CALL gt_gw_main.SP_Create_Merge_Rename_24Q(GT_DB,'table_lu_reject');
		CALL gt_gw_main.SP_Create_Merge_Rename_24Q(GT_DB,'table_protocol_failure_event');
		CALL gt_gw_main.SP_Create_Merge_Rename_24Q(GT_DB,'table_dump_call');
		CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,'opt_nbr_relation');
		CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,'opt_nbr_relation_irat2g');
		CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,'table_tile_1st_server');
		CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,'table_tile_1st_server_c');
		CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,'table_tile_1st_server_c_def');
		CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,'table_tile_1st_server_def');
		CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,'table_tile_1st_server_t');
		CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,'table_tile_1st_server_t_def');
		CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,'table_tile_end');
		CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,'table_tile_end_c');
		CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,'table_tile_end_c_def');
		CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,'table_tile_end_def');
		CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,'table_tile_end_t');
		CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,'table_tile_end_t_def');
		CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,'table_tile_fp');
		CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,'table_tile_fp_c');
		CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,'table_tile_fp_c_def');
		CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,'table_tile_fp_def');
		CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,'table_tile_fp_t');
		CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,'table_tile_fp_t_def');
		CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,'table_tile_start');
		CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,'table_tile_start_c');
		CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,'table_tile_start_c_def');
		CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,'table_tile_start_def');
		CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,'table_tile_start_t');
		CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,'table_tile_start_t_def');
		CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,'table_tile_ue');
		CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,'table_tile_ue_c');
		CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,'table_tile_ue_c_def');
		CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,'table_tile_ue_def');
		CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,'table_tile_ue_t');
		CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,'table_tile_ue_t_def');
	
		CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,'table_tile_imsi_start_c');
		CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,'table_tile_imsi_start_c_def');
	
		CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,'opt_aco_traffic');
		CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,'table_tile_failure_cause');
		CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,'table_imsi_aggregated_hr');
	
		CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,'table_tile_cell_imsi');
		CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,'table_tile_cell_imsi_def');
	
		IF TAC_REPORT_FLAG = 'true' THEN 
			CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,'table_tile_start_lac');
			CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,'table_tile_start_lac_def');
			CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,'table_tile_end_lac');
			CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,'table_tile_end_lac_def');
		END IF;
	
		IF MDT_FLAG = 'true' THEN
			CALL gt_gw_main.SP_Create_Merge_Rename_24Q(GT_DB,'table_mdt_position');
			CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,'table_tile_mdt');
			CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,'table_tile_mdt_c');
			CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,'table_tile_mdt_t');
		END IF;
	END IF;
	
	INSERT INTO gt_gw_main.SP_LOG VALUES(GT_DB,'SP_Create_Merge_Rename_Table',CONCAT('Done: ',TIMESTAMPDIFF(SECOND,START_TIME,SYSDATE()),' seconds.'), NOW());
	
