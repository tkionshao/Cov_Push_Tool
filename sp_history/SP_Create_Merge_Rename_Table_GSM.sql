DELIMITER $$
USE `gt_gw_main`$$
DROP PROCEDURE IF EXISTS `SP_CreateDB_LTE`$$
CREATE DEFINER=`covmo`@`%` PROCEDURE `SP_Create_Merge_Rename_Table_GSM`(IN GT_DB VARCHAR(100),IN FLAG CHAR(2),IN EVENT_NUM TINYINT(2),IN POS_NUM TINYINT(2))
BEGIN
	DECLARE START_TIME DATETIME DEFAULT SYSDATE();
	DECLARE CUSTOMER_USER_FLAG VARCHAR(50);
	DECLARE IMSIG_COUNT SMALLINT(6) DEFAULT 0;
	DECLARE v_j SMALLINT(6);
	DECLARE v_i SMALLINT(6);
	DECLARE cr_count SMALLINT(6) DEFAULT 0;
	DECLARE CURRENT_NT_DB VARCHAR(50) DEFAULT CONCAT('gt_nt_',gt_strtok(GT_DB,3,'_'));
	
	#CR variables for SingTel
	SET @SqlCmd=CONCAT('SELECT LOWER(att_value) INTO @SPECIAL_IMSI_CR FROM ',CURRENT_NT_DB,'.sys_config WHERE group_name = ''System'' AND tech_mask = 7 AND att_name = ''DataProcessForImsiRange'';');
	PREPARE Stmt FROM @SqlCmd;
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
	
	SELECT LOWER(`value`) INTO CUSTOMER_USER_FLAG  FROM gt_gw_main.integration_param WHERE gt_group = 'sp' AND gt_name = 'special_imsi' ;
	SELECT GROUP_CONCAT(DISTINCT (ABS(`GROUP_ID`))) INTO @imsig_value FROM `gt_covmo`.`dim_imsi_group` WHERE GROUP_ID < 0;
	INSERT INTO gt_gw_main.SP_LOG VALUES(GT_DB,'SP_Create_Merge_Rename_Table','Start', START_TIME);
	
	IF FLAG='DY' THEN 	
		IF CUSTOMER_USER_FLAG = 'true' THEN
			SET IMSIG_COUNT = gt_covmo_csv_count(@imsig_value,',');
			SET v_j=1;
			WHILE v_j <= IMSIG_COUNT DO
			BEGIN	
				SET @group_id = gt_covmo_csv_get(@imsig_value,v_j);
				SET @group_bottom_str = CONCAT('_imsig',@group_id);
				
				CALL gt_gw_main.SP_Create_Merge_Rename_24QI(GT_DB,CONCAT('table_call_gsm',@group_bottom_str),@group_bottom_str,'table_call_gsm_imsig');
				CALL gt_gw_main.SP_Create_Merge_Rename_24QI(GT_DB,CONCAT('table_position_gsm',@group_bottom_str),@group_bottom_str,'table_position_gsm_imsig');
	
				SET v_j=v_j+1;
			END;
			END WHILE;
		END IF;		
		#this loop for CR
		#Please do not delete below comment code by chao
		IF @SPECIAL_IMSI_CR = 'true' AND cr_count > 0 THEN
			SET v_i=1;
			WHILE v_i <= cr_count DO
			BEGIN		
				SET @cr = CONCAT('_imsig',gt_covmo_csv_get(@special_imsig_SpecifiedIMSIRangeValue,v_i));
				#dy
				CALL gt_gw_main.SP_Create_Merge_Rename_CR(GT_DB,'table_tile_fp_gsm_dy',@cr);
				CALL gt_gw_main.SP_Create_Merge_Rename_CR(GT_DB,'table_tile_fp_gsm_dy_t',@cr);
				CALL gt_gw_main.SP_Create_Merge_Rename_CR(GT_DB,'table_tile_fp_gsm_dy_c',@cr);
				CALL gt_gw_main.SP_Create_Merge_Rename_CR(GT_DB,'table_tile_fp_gsm_dy_def',@cr);
				CALL gt_gw_main.SP_Create_Merge_Rename_CR(GT_DB,'table_tile_fp_gsm_dy_t_def',@cr);
				CALL gt_gw_main.SP_Create_Merge_Rename_CR(GT_DB,'table_tile_fp_gsm_dy_c_def',@cr);
				CALL gt_gw_main.SP_Create_Merge_Rename_CR(GT_DB,'table_tile_start_gsm_dy',@cr);
				CALL gt_gw_main.SP_Create_Merge_Rename_CR(GT_DB,'table_tile_start_gsm_dy_t',@cr);
				CALL gt_gw_main.SP_Create_Merge_Rename_CR(GT_DB,'table_tile_start_gsm_dy_c',@cr);
				CALL gt_gw_main.SP_Create_Merge_Rename_CR(GT_DB,'table_tile_start_gsm_dy_def',@cr);
				CALL gt_gw_main.SP_Create_Merge_Rename_CR(GT_DB,'table_tile_start_gsm_dy_t_def',@cr);
				CALL gt_gw_main.SP_Create_Merge_Rename_CR(GT_DB,'table_tile_start_gsm_dy_c_def',@cr);
				CALL gt_gw_main.SP_Create_Merge_Rename_CR(GT_DB,'table_tile_end_gsm_dy',@cr);
				CALL gt_gw_main.SP_Create_Merge_Rename_CR(GT_DB,'table_tile_end_gsm_dy_t',@cr);
				CALL gt_gw_main.SP_Create_Merge_Rename_CR(GT_DB,'table_tile_end_gsm_dy_c',@cr);
				CALL gt_gw_main.SP_Create_Merge_Rename_CR(GT_DB,'table_tile_end_gsm_dy_def',@cr);
				CALL gt_gw_main.SP_Create_Merge_Rename_CR(GT_DB,'table_tile_end_gsm_dy_t_def',@cr);
				CALL gt_gw_main.SP_Create_Merge_Rename_CR(GT_DB,'table_tile_end_gsm_dy_c_def',@cr);
				#not dy
				CALL gt_gw_main.SP_Create_Merge_Rename_24HI(GT_DB,CONCAT('table_tile_fp_gsm',@cr),@cr,'table_tile_fp_gsm');
				CALL gt_gw_main.SP_Create_Merge_Rename_24HI(GT_DB,CONCAT('table_tile_fp_gsm_t',@cr),@cr,'table_tile_fp_gsm_t');
				CALL gt_gw_main.SP_Create_Merge_Rename_24HI(GT_DB,CONCAT('table_tile_fp_gsm_c',@cr),@cr,'table_tile_fp_gsm_c');
				CALL gt_gw_main.SP_Create_Merge_Rename_24HI(GT_DB,CONCAT('table_tile_fp_gsm_def',@cr),@cr,'table_tile_fp_gsm_def');
				CALL gt_gw_main.SP_Create_Merge_Rename_24HI(GT_DB,CONCAT('table_tile_fp_gsm_t_def',@cr),@cr,'table_tile_fp_gsm_t_def');
				CALL gt_gw_main.SP_Create_Merge_Rename_24HI(GT_DB,CONCAT('table_tile_fp_gsm_c_def',@cr),@cr,'table_tile_fp_gsm_c_def');
				CALL gt_gw_main.SP_Create_Merge_Rename_24HI(GT_DB,CONCAT('table_tile_start_gsm',@cr),@cr,'table_tile_start_gsm');
				CALL gt_gw_main.SP_Create_Merge_Rename_24HI(GT_DB,CONCAT('table_tile_start_gsm_t',@cr),@cr,'table_tile_start_gsm_t');
				CALL gt_gw_main.SP_Create_Merge_Rename_24HI(GT_DB,CONCAT('table_tile_start_gsm_c',@cr),@cr,'table_tile_start_gsm_c');
				CALL gt_gw_main.SP_Create_Merge_Rename_24HI(GT_DB,CONCAT('table_tile_start_gsm_def',@cr),@cr,'table_tile_start_gsm_def');
				CALL gt_gw_main.SP_Create_Merge_Rename_24HI(GT_DB,CONCAT('table_tile_start_gsm_t_def',@cr),@cr,'table_tile_start_gsm_t_def');
				CALL gt_gw_main.SP_Create_Merge_Rename_24HI(GT_DB,CONCAT('table_tile_start_gsm_c_def',@cr),@cr,'table_tile_start_gsm_c_def');
				CALL gt_gw_main.SP_Create_Merge_Rename_24HI(GT_DB,CONCAT('table_tile_end_gsm',@cr),@cr,'table_tile_end_gsm');
				CALL gt_gw_main.SP_Create_Merge_Rename_24HI(GT_DB,CONCAT('table_tile_end_gsm_t',@cr),@cr,'table_tile_end_gsm_t');
				CALL gt_gw_main.SP_Create_Merge_Rename_24HI(GT_DB,CONCAT('table_tile_end_gsm_c',@cr),@cr,'table_tile_end_gsm_c');
				CALL gt_gw_main.SP_Create_Merge_Rename_24HI(GT_DB,CONCAT('table_tile_end_gsm_def',@cr),@cr,'table_tile_end_gsm_def');
				CALL gt_gw_main.SP_Create_Merge_Rename_24HI(GT_DB,CONCAT('table_tile_end_gsm_t_def',@cr),@cr,'table_tile_end_gsm_t_def');
				CALL gt_gw_main.SP_Create_Merge_Rename_24HI(GT_DB,CONCAT('table_tile_end_gsm_c_def',@cr),@cr,'table_tile_end_gsm_c_def');
				SET v_i=v_i+1;
			END;
			END WHILE;
		END IF;			
		
		CALL gt_gw_main.SP_Create_Merge_Rename_24Q(GT_DB,'table_call_gsm');
		CALL gt_gw_main.SP_Create_Merge_Rename_24Q(GT_DB,'table_position_gsm');
 		CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,'opt_nbr_relation_gsm');
		CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,'table_tile_fp_gsm');
		CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,'table_tile_fp_gsm_t');
		CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,'table_tile_fp_gsm_c');
		CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,'table_tile_fp_gsm_def');
		CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,'table_tile_fp_gsm_t_def');
		CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,'table_tile_fp_gsm_c_def');
		CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,'table_tile_start_gsm');
		CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,'table_tile_start_gsm_t');
		CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,'table_tile_start_gsm_c');
		CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,'table_tile_start_gsm_def');
		CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,'table_tile_start_gsm_t_def');
		CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,'table_tile_start_gsm_c_def');
		CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,'table_tile_end_gsm');
		CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,'table_tile_end_gsm_t');
		CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,'table_tile_end_gsm_c');
		CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,'table_tile_end_gsm_def');
		CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,'table_tile_end_gsm_t_def');
		CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,'table_tile_end_gsm_c_def');
		CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,'table_tile_dominant_cell_gsm');
		CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,'table_tile_imsi_gsm');
		CALL gt_gw_main.SP_Create_Merge_Rename_24H(GT_DB,'table_tile_handset_gsm');	
		CALL gt_gw_main.SP_Create_Merge_Rename_24Q(GT_DB,'table_roamer_call_gsm');
	END IF;	
	INSERT INTO gt_gw_main.SP_LOG VALUES(GT_DB,'SP_Create_Merge_Rename_Table',CONCAT('Done: ',TIMESTAMPDIFF(SECOND,START_TIME,SYSDATE()),' seconds.'), NOW());
	
END$$
DELIMITER ;
