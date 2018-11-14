DELIMITER $$
USE `gt_gw_main`$$
DROP PROCEDURE IF EXISTS `SP_CreateDB_LTE`$$
CREATE DEFINER=`covmo`@`%` PROCEDURE `SP_Sub_Generate_TAC_Start`(IN GT_DB VARCHAR(100))
BEGIN
	DECLARE RNC_ID INT;
	DECLARE O_GT_DB VARCHAR(100) DEFAULT GT_DB;
	DECLARE START_TIME DATETIME DEFAULT SYSDATE();
	DECLARE PARTITION_ID INT DEFAULT SUBSTRING(RIGHT(GT_DB,18),10,2) ;
	DECLARE SH_EH VARCHAR(9) DEFAULT RIGHT(GT_DB,9);
	DECLARE STARTHOUR VARCHAR(2) DEFAULT SUBSTRING(RIGHT(GT_DB,18),10,2);
	DECLARE ENDHOUR VARCHAR(2) DEFAULT IF(SUBSTRING(RIGHT(GT_DB,18),15,2)='00','24',SUBSTRING(RIGHT(GT_DB,18),15,2));
	DECLARE WORKER_ID VARCHAR(10) DEFAULT CONNECTION_ID();
	DECLARE CURRENT_NT_DB VARCHAR(50) DEFAULT CONCAT('gt_nt_',gt_strtok(GT_DB,3,'_'));
	
	DECLARE SUM_STR_COL VARCHAR(5000);
	DECLARE OTHER_STR_COL VARCHAR(2500);
	DECLARE A_KEY VARCHAR(3000);
	DECLARE A_DEF_KEY VARCHAR(3000);
	DECLARE A_DY_KEY VARCHAR(3000);
	DECLARE A_DY_DEF_KEY VARCHAR(3000);
	DECLARE A_KEY_AVOID_Ambiguous VARCHAR(3000);
	DECLARE A_DEF_KEY_AVOID_Ambiguous VARCHAR(3000);
	DECLARE A_DY_KEY_AVOID_Ambiguous VARCHAR(3000);
	DECLARE A_DY_DEF_KEY_AVOID_Ambiguous VARCHAR(3000);
		
	DECLARE COLUMN_STR VARCHAR(2000) DEFAULT 
		'POS_FIRST_RSRP,
		POS_FIRST_RSRP_CNT,
...sample';
		
	DECLARE COLUMN_IFNULL_STR VARCHAR(7000) DEFAULT 
		'IFNULL(POS_FIRST_RSRP,0) AS POS_FIRST_RSRP,
		IFNULL(POS_FIRST_RSRP_CNT,0) AS POS_FIRST_RSRP_CNT,
...sample';
		
	DECLARE COLUMN_SUM_STR VARCHAR(7000) DEFAULT 
		'IFNULL(SUM(POS_FIRST_RSRP),0) AS POS_FIRST_RSRP,
		IFNULL(SUM(POS_FIRST_RSRP_CNT),0) AS POS_FIRST_RSRP_CNT,
...sample';
		
	DECLARE COLUMN_UPD_STR VARCHAR(10000) DEFAULT 
		'RPT_TABLE_NAME.POS_FIRST_RSRP=RPT_TABLE_NAME.POS_FIRST_RSRP+VALUES(POS_FIRST_RSRP),
		RPT_TABLE_NAME.POS_FIRST_RSRP_CNT=RPT_TABLE_NAME.POS_FIRST_RSRP_CNT+VALUES(POS_FIRST_RSRP_CNT),
...sample';
	INSERT INTO gt_gw_main.sp_log VALUES(O_GT_DB,'SP_Sub_Generate_TAC_Start','Start', NOW());
	
	set SUM_STR_COL = 'CALL_CNT|BEST_RSCP_1|BEST_RSCP_1_MED|BEST_ECN0_1|BEST_ECN0_1_MED|SUB_DENSITY|ERLANG|DL_DATA_THRU|UL_DATA_THRU|DL_TRAFFIC|UL_TRAFFIC|DL_THRU_HIGH_CALL_CNT|DL_THRU_HIGH|UL_THRU_HIGH_CALL_CNT|UL_THRU_HIGH|U_UL_DATA_THRU|max.U_MAX_UL_DATA_THRU|U_DL_DATA_THRU|max.U_MAX_DL_DATA_THRU|OVERSHOOTING_CALL_COUNT|CALL_CNT_ACT|BEST_RSCP_1_ACT|BEST_RSCP_1_ACT_MED|BEST_ECN0_1_ACT|BEST_ECN0_1_ACT_MED|RAB_NO_RESOURCE_AVAILABLE|RAB_RELEASE_DUE_TO_UTRAN_GENERATED_REASON|RAB_OTHER|RRC_CONGESTION|RRC_UNSPECIFIED|BEST_RSCP_1_CNT|BEST_ECN0_1_CNT|U_UL_DATA_THRU_CNT|U_DL_DATA_THRU_CNT|BEST_RSCP_1_ACT_CNT|BEST_ECN0_1_ACT_CNT|GROUP_CANCAT_IMSI|HOUR_CNT|NAS_SM_SUM|NAS_GPRS_MM_ATTACH_SUM|CALL_TYPE_10|CALL_TYPE_11|CALL_TYPE_12|CALL_TYPE_13|CALL_TYPE_14|CALL_TYPE_16|CALL_TYPE_18|AUTH_REQUEST|AUTH_RESPONSE|AUTH_FAILURE|AUTH_REJECT|LU_REQUEST|LU_ACCEPT|LU_REJECT|AUTH_CIPH_REQUEST|AUTH_CIPH_RESPONSE|AUTH_CIPH_FAILURE|AUTH_CIPH_REJECT|RAU_REQUEST|RAU_ACCEPT|RAU_REJECT|PDP_ACTIVATION_REQUEST_CNT|PDP_ACTIVATION_ACCEPT_CNT|PDP_ACTIVATION_REJECT_CNT|RRC_2000000|RRC_2001000|RAB_1006000|RAB_1014000|RAB_1015000|RAB_1019000|RAB_1033000|RAB_1034000|RAB_1039000|RAB_1040000|RAB_1042000|RAB_1046000|RAB_5113000|RAB_5114000|RRC_RAB_OTHERS|CSFB_SETUP_TIME_SUM|CSFB_SETUP_TIME_CNT|max.CSFB_SETUP_TIME_MAX|CSFB_CALL_CNT|RAB_FAILURE_IN_THE_RADIO_INTERFACE_PROCEDURE|RAB_REQUESTED_MAXIMUM_BIT_RATE_FOR_DL_UL_NOT_AVAILABLE|RAB_NO_RADIO_RESOURCES_AVAILABLE_IN_TARGET_CELL|RAB_LU_TRANSPORT_CONNECTION_FAILED_TO_ESTABLISH|RAB_UNSPECIFIED_FAILURE|SMS_TYPE_01|SMS_TYPE_10|SMS_TYPE_11|SMS_TYPE_00|SETUP_TIME_SUM|SETUP_TIME_CNT|max.SETUP_TIME_MAX|ROAMING_CALL_COUNT|ROAMING_IN_ATTEMPT|ROAMING_IN_FAILURE|MO_SETUP_TIME_SUM|MO_SETUP_CNT|max.MO_SETUP_TIME_MAX|MT_SETUP_TIME_SUM|MT_SETUP_CNT|max.MT_SETUP_TIME_MAX';
	SET A_KEY = 'DATA_DATE,DATA_HOUR,CALL_TYPE,CALL_STATUS,MOVING,INDOOR,FREQUENCY,UARFCN,RNC_ID,LAC';
	SET A_DEF_KEY = 'DATA_DATE,DATA_HOUR,RNC_ID,LAC';
	SET A_DY_KEY = 'DATA_DATE,CALL_TYPE,CALL_STATUS,MOVING,INDOOR,FREQUENCY,UARFCN,RNC_ID,LAC';
	SET A_DY_DEF_KEY = 'DATA_DATE,RNC_ID,LAC';
	SET A_KEY_AVOID_Ambiguous = 'DATA_DATE,DATA_HOUR,CALL_TYPE,CALL_STATUS,MOVING,A.INDOOR,A.FREQUENCY,UARFCN,A.RNC_ID,LAC';
	SET A_DEF_KEY_AVOID_Ambiguous = 'DATA_DATE,DATA_HOUR,A.RNC_ID,LAC';
	SET A_DY_KEY_AVOID_Ambiguous = 'DATA_DATE,CALL_TYPE,CALL_STATUS,MOVING,A.INDOOR,A.FREQUENCY,UARFCN,A.RNC_ID,LAC';
	SET A_DY_DEF_KEY_AVOID_Ambiguous = 'DATA_DATE,A.RNC_ID,LAC';
		
	SET @col_cnt=gt_covmo_csv_count(SUM_STR_COL,'|');
	SET @c_i=1;
	WHILE @c_i <= @col_cnt DO
	BEGIN
		SET @cur_col_name = gt_strtok(SUM_STR_COL, @c_i, '|');
		IF @c_i=1 THEN
			SET COLUMN_STR = @cur_col_name;
			SET COLUMN_IFNULL_STR = CONCAT('IFNULL(',@cur_col_name,',0) AS ',@cur_col_name,'');
			IF LEFT(@cur_col_name,4) != 'max.' THEN
				SET COLUMN_SUM_STR = CONCAT('IFNULL(SUM(',@cur_col_name,'),0) AS ',@cur_col_name,'');
				SET COLUMN_UPD_STR = CONCAT('RPT_TABLE_NAME.',@cur_col_name,'=RPT_TABLE_NAME.',@cur_col_name,'+VALUES(',@cur_col_name,')');
			else
				SET @cur_col_name2 = right(@cur_col_name,length(@cur_col_name)-4);
				SET COLUMN_SUM_STR = CONCAT('IFNULL(MAX(',@cur_col_name2,'),0) AS ',@cur_col_name2,'');
				SET COLUMN_UPD_STR = CONCAT('RPT_TABLE_NAME.',@cur_col_name2,'=IF(RPT_TABLE_NAME.',@cur_col_name2,' > VALUES(',@cur_col_name2,'),RPT_TABLE_NAME.',@cur_col_name2,',VALUES(',@cur_col_name2,'))');
			end if;
		ELSE
			SET COLUMN_STR = CONCAT(COLUMN_STR,',',@cur_col_name);
			SET COLUMN_IFNULL_STR = CONCAT(COLUMN_IFNULL_STR,',','IFNULL(',@cur_col_name,',0) AS ',@cur_col_name,'');
			IF upper(LEFT(@cur_col_name,4)) != 'max.' THEN
				SET COLUMN_SUM_STR = CONCAT(COLUMN_SUM_STR,',','IFNULL(SUM(',@cur_col_name,'),0) AS ',@cur_col_name,'');
				SET COLUMN_UPD_STR = CONCAT(COLUMN_UPD_STR,'\n,','RPT_TABLE_NAME.',@cur_col_name,'=RPT_TABLE_NAME.',@cur_col_name,'+VALUES(',@cur_col_name,')');
			else
				SET @cur_col_name2 = RIGHT(@cur_col_name,LENGTH(@cur_col_name)-4);
				SET COLUMN_SUM_STR = CONCAT(COLUMN_SUM_STR,',','IFNULL(MAX(',@cur_col_name2,'),0) AS ',@cur_col_name2,'');
				SET COLUMN_UPD_STR = CONCAT(COLUMN_UPD_STR,'\n,','RPT_TABLE_NAME.',@cur_col_name2,'=IF(RPT_TABLE_NAME.',@cur_col_name2,' > VALUES(',@cur_col_name2,'),RPT_TABLE_NAME.',@cur_col_name2,',VALUES(',@cur_col_name2,'))');
			end if;
		END IF;
	
		SET @c_i = @c_i + 1;
	END;
	END WHILE;
	
	SELECT gt_strtok(GT_DB,2,'_') INTO RNC_ID;
	SELECT REPLACE(GT_DB,SH_EH,'0000_0000') INTO GT_DB;
	set COLUMN_STR = replace(COLUMN_STR,'max.','');
	INSERT INTO gt_gw_main.sp_log VALUES(O_GT_DB,'SP_Sub_Generate_TAC_Start','table_tile_start_lac', NOW());
	
	SET @rpt_target_table=CONCAT(GT_DB,'.table_tile_start_lac_',STARTHOUR);
	SET @rpt_source_table=concat(GT_DB,'.table_tile_start_c_',STARTHOUR);
	SET @SqlCmd=CONCAT('TRUNCATE TABLE ',@rpt_target_table,';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	SET @SqlCmd=CONCAT('INSERT INTO ',@rpt_target_table,' 
					(
					',A_KEY,
					',',COLUMN_STR,')
				SELECT	
					',A_KEY_AVOID_Ambiguous,
					',',COLUMN_SUM_STR,'
				FROM ',@rpt_source_table,' A, ',CURRENT_NT_DB,'.nt_current B
				WHERE A.RNC_ID = B.RNC_ID AND A.CELL_ID = B.CELL_ID
				GROUP BY ',A_KEY,' 
				ORDER BY NULL
				;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	INSERT INTO gt_gw_main.sp_log VALUES(O_GT_DB,'SP_Sub_Generate_TAC_Start','table_tile_start_lac', NOW());
	SET @rpt_target_table=CONCAT(GT_DB,'.table_tile_start_lac_def_',STARTHOUR);
	SET @rpt_source_table=CONCAT(GT_DB,'.table_tile_start_c_',STARTHOUR);
	SET @SqlCmd=CONCAT('TRUNCATE TABLE ',@rpt_target_table,';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	SET @SqlCmd=CONCAT('INSERT INTO ',@rpt_target_table,' 
					(
					',A_DEF_KEY,
					',',COLUMN_STR,')
				SELECT	
					',A_DEF_KEY_AVOID_Ambiguous,
					',',COLUMN_SUM_STR,'
				FROM ',@rpt_source_table,' A, ',CURRENT_NT_DB,'.nt_current B
				WHERE A.RNC_ID = B.RNC_ID AND A.CELL_ID = B.CELL_ID
				GROUP BY ',A_DEF_KEY,' 
				ORDER BY NULL
				;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	INSERT INTO gt_gw_main.sp_log VALUES(O_GT_DB,'SP_Sub_Generate_TAC_Start','table_tile_start_lac_dy', NOW());
	SET @rpt_target_table=CONCAT(GT_DB,'.table_tile_start_lac_dy');
	SET @rpt_source_table=CONCAT(GT_DB,'.table_tile_start_c_',STARTHOUR);
	SET @SqlCmd=CONCAT('INSERT INTO ',@rpt_target_table,' 
					(
					',A_DY_KEY,
					',',COLUMN_STR,')
				SELECT	
					',A_DY_KEY_AVOID_Ambiguous,
					',',COLUMN_SUM_STR,'
				FROM ',@rpt_source_table,' A, ',CURRENT_NT_DB,'.nt_current B
				WHERE A.RNC_ID = B.RNC_ID AND A.CELL_ID = B.CELL_ID
				GROUP BY ',A_DY_KEY,' 
				ORDER BY NULL
				ON DUPLICATE KEY UPDATE 
				',REPLACE(COLUMN_UPD_STR,'RPT_TABLE_NAME',@rpt_target_table),'
				;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;	
	
	INSERT INTO gt_gw_main.sp_log VALUES(O_GT_DB,'SP_Sub_Generate_TAC_Start','table_tile_start_lac_dy', NOW());
	SET @rpt_target_table=CONCAT(GT_DB,'.table_tile_start_lac_dy_def');
	SET @rpt_source_table=CONCAT(GT_DB,'.table_tile_start_c_',STARTHOUR);
	SET @SqlCmd=CONCAT('INSERT INTO ',@rpt_target_table,' 
					(
					',A_DY_DEF_KEY,
					',',COLUMN_STR,')
				SELECT	
					',A_DY_DEF_KEY_AVOID_Ambiguous,
					',',COLUMN_SUM_STR,'
				FROM ',@rpt_source_table,' A, ',CURRENT_NT_DB,'.nt_current B
				WHERE A.RNC_ID = B.RNC_ID AND A.CELL_ID = B.CELL_ID
				GROUP BY ',A_DY_DEF_KEY,' 
				ORDER BY NULL
				ON DUPLICATE KEY UPDATE 
				',REPLACE(COLUMN_UPD_STR,'RPT_TABLE_NAME',@rpt_target_table),'
				;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	INSERT INTO gt_gw_main.sp_log VALUES(O_GT_DB,'SP_Sub_Generate_TAC_Start',CONCAT('Done: ',TIMESTAMPDIFF(SECOND,START_TIME,SYSDATE()),' seconds.'), NOW());
END$$
DELIMITER ;
