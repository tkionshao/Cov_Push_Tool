DELIMITER $$
USE `gt_gw_main`$$
DROP PROCEDURE IF EXISTS `SP_CreateDB_LTE`$$
CREATE DEFINER=`covmo`@`%` PROCEDURE `SP_Sub_Generate_TAC_End_LTE`(IN GT_DB VARCHAR(100))
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
	
	DECLARE SUM_STR_COL VARCHAR(6000);
	DECLARE OTHER_STR_COL VARCHAR(2500);
	DECLARE A_KEY VARCHAR(3000);
	DECLARE A_DEF_KEY VARCHAR(3000);
	DECLARE A_DY_KEY VARCHAR(3000);
	DECLARE A_DY_DEF_KEY VARCHAR(3000);
	
	DECLARE COLUMN_STR VARCHAR(2500) DEFAULT 
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
	INSERT INTO gt_gw_main.sp_log VALUES(O_GT_DB,'SP_Sub_Generate_TAC_End_LTE','Start', NOW());
	
	SET SUM_STR_COL = 'POS_LAST_RSRP|POS_LAST_RSRP_CNT|POS_LAST_RSRQ|POS_LAST_RSRQ_CNT|MEASURE_CNT|DURATION|IRAT_TO_GERAN_SUCCESS_1_RSRP|IRAT_TO_GERAN_SUCCESS_1_RSRP_CNT|IRAT_TO_GERAN_SUCCESS_1_RSRQ|IRAT_TO_GERAN_SUCCESS_1_RSRQ_CNT|IRAT_TO_GERAN_SUCCESS_1_RSSI|IRAT_TO_GERAN_SUCCESS_1_RSSI_CNT|IRAT_TO_UMTS_SUCCESS_1_ECN0|IRAT_TO_UMTS_SUCCESS_1_ECN0_CNT|IRAT_TO_UMTS_SUCCESS_1_RSCP|IRAT_TO_UMTS_SUCCESS_1_RSCP_CNT|IRAT_TO_UMTS_SUCCESS_1_RSRP|IRAT_TO_UMTS_SUCCESS_1_RSRP_CNT|IRAT_TO_UMTS_SUCCESS_1_RSRQ|IRAT_TO_UMTS_SUCCESS_1_RSRQ_CNT|RRC_REQUEST_TYPE_1|RRC_REQUEST_TYPE_1_CNT|RRC_REQUEST_TYPE_2|RRC_REQUEST_TYPE_2_CNT|RRC_REQUEST_TYPE_3|RRC_REQUEST_TYPE_3_CNT|RRC_REQUEST_TYPE_4|RRC_REQUEST_TYPE_4_CNT|RRC_REQUEST_TYPE_5|RRC_REQUEST_TYPE_5_CNT|RRC_REQUEST_TYPE_6|RRC_REQUEST_TYPE_6_CNT|RRC_REQUEST_TYPE_7|RRC_REQUEST_TYPE_7_CNT|RRC_REQUEST_TYPE_8|RRC_REQUEST_TYPE_8_CNT|RRC_REQUEST_TYPE_NOT_1_8|RRC_REQUEST_TYPE_NOT_1_8_CNT|ERAB_SETUP_FAILURE|CALL_STATUS_2|CALL_STATUS_3|END_NON_BLOCK_CALL_CNT|DURATION_M30|DURATION_L10|DURATION_L30|DURATION_L1|RRC_CONNECTION_REJECT|IRAT_TO_GERAN_ATTEMPT|IRAT_TO_GERAN_SUCCESS|IRAT_TO_UMTS_ATTEMPT|IRAT_TO_UMTS_SUCCESS|SERVING_CNT|POS_LAST_S_RSRP|POS_LAST_S_RSRP_CNT|POS_LAST_S_RSRQ|POS_LAST_S_RSRQ_CNT|IMSI_CNT|SRVCC_ATTEMPT|SRVCC_SUCCESS|SRVCC_FAILURE|RRC_1|RRC_4|RRC_18|S1_1031|S1_1033|S1_1037|S1_1046|S1_1056|S1_1060|S1_1070|S1_1073|S1_2065|X2_2055|X2_2056|X2_2062|CA_SERVING_CNT|CA_CALL_STATUS_2|CA_END_NON_BLOCK_CALL_CNT|CSFB_SETUP_TIME_SUM|CSFB_SETUP_TIME_CNT|max.CSFB_SETUP_TIME_MAX|CA_POS_LAST_S_RSRP|CA_POS_LAST_S_RSRP_CNT|CA_POS_LAST_S_RSRQ|CA_POS_LAST_S_RSRQ_CNT|RRC_SETUP_CNT';
	SET A_KEY = 'DATA_DATE,DATA_HOUR,CALL_TYPE,CALL_STATUS,MOVING,INDOOR,EUTRABAND,EARFCN,SUB_REGION_ID,TAC';
	SET A_DEF_KEY = 'DATA_DATE,DATA_HOUR,SUB_REGION_ID,TAC';
	SET A_DY_KEY = 'DATA_DATE,CALL_TYPE,CALL_STATUS,MOVING,INDOOR,EUTRABAND,EARFCN,SUB_REGION_ID,TAC';
	SET A_DY_DEF_KEY = 'DATA_DATE,SUB_REGION_ID,TAC';
	
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
			ELSE
				SET @cur_col_name2 = RIGHT(@cur_col_name,LENGTH(@cur_col_name)-4);
				SET COLUMN_SUM_STR = CONCAT('IFNULL(MAX(',@cur_col_name2,'),0) AS ',@cur_col_name2,'');
				SET COLUMN_UPD_STR = CONCAT('RPT_TABLE_NAME.',@cur_col_name2,'=IF(RPT_TABLE_NAME.',@cur_col_name2,' > VALUES(',@cur_col_name2,'),RPT_TABLE_NAME.',@cur_col_name2,',VALUES(',@cur_col_name2,'))');
			END IF;
		ELSE
			SET COLUMN_STR = CONCAT(COLUMN_STR,',',@cur_col_name);
			SET COLUMN_IFNULL_STR = CONCAT(COLUMN_IFNULL_STR,',','IFNULL(',@cur_col_name,',0) AS ',@cur_col_name,'');
			IF UPPER(LEFT(@cur_col_name,4)) != 'max.' THEN
				SET COLUMN_SUM_STR = CONCAT(COLUMN_SUM_STR,',','IFNULL(SUM(',@cur_col_name,'),0) AS ',@cur_col_name,'');
				SET COLUMN_UPD_STR = CONCAT(COLUMN_UPD_STR,'\n,','RPT_TABLE_NAME.',@cur_col_name,'=RPT_TABLE_NAME.',@cur_col_name,'+VALUES(',@cur_col_name,')');
			ELSE
				SET @cur_col_name2 = RIGHT(@cur_col_name,LENGTH(@cur_col_name)-4);
				SET COLUMN_SUM_STR = CONCAT(COLUMN_SUM_STR,',','IFNULL(MAX(',@cur_col_name2,'),0) AS ',@cur_col_name2,'');
				SET COLUMN_UPD_STR = CONCAT(COLUMN_UPD_STR,'\n,','RPT_TABLE_NAME.',@cur_col_name2,'=IF(RPT_TABLE_NAME.',@cur_col_name2,' > VALUES(',@cur_col_name2,'),RPT_TABLE_NAME.',@cur_col_name2,',VALUES(',@cur_col_name2,'))');
			END IF;
		END IF;
	
		SET @c_i = @c_i + 1;
	END;
	END WHILE;
	
	SELECT gt_strtok(GT_DB,2,'_') INTO RNC_ID;
	SELECT REPLACE(GT_DB,SH_EH,'0000_0000') INTO GT_DB;
	SET COLUMN_STR = REPLACE(COLUMN_STR,'max.','');
	INSERT INTO gt_gw_main.sp_log VALUES(O_GT_DB,'SP_Sub_Generate_TAC_End_LTE','rpt_tac_end', NOW());
	
	SET @rpt_target_table=CONCAT(GT_DB,'.rpt_tac_end_',STARTHOUR);
	SET @rpt_source_table=CONCAT(GT_DB,'.rpt_cell_end_',STARTHOUR);
	SET @SqlCmd=CONCAT('TRUNCATE TABLE ',@rpt_target_table,';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	SET @SqlCmd=CONCAT('INSERT INTO ',@rpt_target_table,' 
					(
					',A_KEY,
					',',COLUMN_STR,')
				SELECT	
					',A_KEY,
					',',COLUMN_SUM_STR,'
				FROM ',@rpt_source_table,' A, ',CURRENT_NT_DB,'.nt_tac_cell_current_lte B
				WHERE A.ENODEB_ID = B.ENODEB_ID AND A.CELL_ID = B.CELL_ID
				GROUP BY ',A_KEY,' 
				ORDER BY NULL
				;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	INSERT INTO gt_gw_main.sp_log VALUES(O_GT_DB,'SP_Sub_Generate_TAC_End_LTE','rpt_tac_end_def', NOW());
	SET @rpt_target_table=CONCAT(GT_DB,'.rpt_tac_end_def_',STARTHOUR);
	SET @rpt_source_table=CONCAT(GT_DB,'.rpt_cell_end_',STARTHOUR);
	SET @SqlCmd=CONCAT('TRUNCATE TABLE ',@rpt_target_table,';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	SET @SqlCmd=CONCAT('INSERT INTO ',@rpt_target_table,' 
					(
					',A_DEF_KEY,
					',',COLUMN_STR,')
				SELECT	
					',A_DEF_KEY,
					',',COLUMN_SUM_STR,'
				FROM ',@rpt_source_table,' A, ',CURRENT_NT_DB,'.nt_tac_cell_current_lte B
				WHERE A.ENODEB_ID = B.ENODEB_ID AND A.CELL_ID = B.CELL_ID
				GROUP BY ',A_DEF_KEY,' 
				ORDER BY NULL
				;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	INSERT INTO gt_gw_main.sp_log VALUES(O_GT_DB,'SP_Sub_Generate_TAC_End_LTE','rpt_tac_end_dy', NOW());
	SET @rpt_target_table=CONCAT(GT_DB,'.rpt_tac_end_dy');
	SET @rpt_source_table=CONCAT(GT_DB,'.rpt_cell_end_',STARTHOUR);
	SET @SqlCmd=CONCAT('INSERT INTO ',@rpt_target_table,' 
					(
					',A_DY_KEY,
					',',COLUMN_STR,')
				SELECT	
					',A_DY_KEY,
					',',COLUMN_SUM_STR,'
				FROM ',@rpt_source_table,' A, ',CURRENT_NT_DB,'.nt_tac_cell_current_lte B
				WHERE A.ENODEB_ID = B.ENODEB_ID AND A.CELL_ID = B.CELL_ID
				GROUP BY ',A_DY_KEY,' 
				ORDER BY NULL
				ON DUPLICATE KEY UPDATE 
				',REPLACE(COLUMN_UPD_STR,'RPT_TABLE_NAME',@rpt_target_table),'
				;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;	
	
	INSERT INTO gt_gw_main.sp_log VALUES(O_GT_DB,'SP_Sub_Generate_TAC_End_LTE','rpt_tac_end_dy_def', NOW());
	SET @rpt_target_table=CONCAT(GT_DB,'.rpt_tac_end_dy_def');
	SET @rpt_source_table=CONCAT(GT_DB,'.rpt_cell_end_',STARTHOUR);
	SET @SqlCmd=CONCAT('INSERT INTO ',@rpt_target_table,' 
					(
					',A_DY_DEF_KEY,
					',',COLUMN_STR,')
				SELECT	
					',A_DY_DEF_KEY,
					',',COLUMN_SUM_STR,'
				FROM ',@rpt_source_table,' A, ',CURRENT_NT_DB,'.nt_tac_cell_current_lte B
				WHERE A.ENODEB_ID = B.ENODEB_ID AND A.CELL_ID = B.CELL_ID
				GROUP BY ',A_DY_DEF_KEY,' 
				ORDER BY NULL
				ON DUPLICATE KEY UPDATE 
				',REPLACE(COLUMN_UPD_STR,'RPT_TABLE_NAME',@rpt_target_table),'
				;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	INSERT INTO gt_gw_main.sp_log VALUES(O_GT_DB,'SP_Sub_Generate_TAC_End_LTE',CONCAT('Done: ',TIMESTAMPDIFF(SECOND,START_TIME,SYSDATE()),' seconds.'), NOW());
END$$
DELIMITER ;
