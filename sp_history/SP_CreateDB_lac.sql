DELIMITER $$
USE `gt_gw_main`$$
DROP PROCEDURE IF EXISTS `SP_CreateDB_LTE`$$
CREATE DEFINER=`covmo`@`%` PROCEDURE `SP_CreateDB_lac`(IN GT_DB VARCHAR(50))
BEGIN
		DECLARE UNION_STR TEXT ;
		DECLARE UNION_STR_2 TEXT ;
		DECLARE UNION_STR_3 TEXT ;
		DECLARE UNION_STR_4 TEXT ;
		DECLARE MAX_HR SMALLINT(6);
		DECLARE STR_HR SMALLINT(6);
			
		DECLARE TBL_NAME_table_tile_end_lac VARCHAR(100);
		DECLARE TBL_NAME_table_tile_end_lac_def VARCHAR(100);
		DECLARE TBL_NAME_table_tile_start_lac VARCHAR(100);
		DECLARE TBL_NAME_table_tile_start_lac_def VARCHAR(100);
	
		DECLARE qry_tbl_name_hr VARCHAR(50);
		DECLARE qry_tbl_name_hr_2 VARCHAR(50);
		DECLARE qry_tbl_name_hr_3 VARCHAR(50);
		DECLARE qry_tbl_name_hr_4 VARCHAR(50);
		DECLARE v_k SMALLINT(6);
		DECLARE v_k_Diff SMALLINT(6);
		
		DECLARE table_tile_end_lac_STR VARCHAR(2000) DEFAULT 
		'
				  `RNC_ID` MEDIUMINT(9) NOT NULL COMMENT ''KEY'',
				  `LAC` MEDIUMINT(9) NOT NULL COMMENT ''KEY'',
				  `CALL_CNT` MEDIUMINT(9) DEFAULT ''0'',
				  `CAUSE_14_CNT` SMALLINT(6) DEFAULT ''0'',
				  `CAUSE_15_CNT` SMALLINT(6) DEFAULT ''0'',
				  `CAUSE_46_CNT` SMALLINT(6) DEFAULT ''0'',
				  `CAUSE_115_CNT` SMALLINT(6) DEFAULT ''0'',
				  `CAUSE_OTHERS_CNT` SMALLINT(6) DEFAULT ''0'',
				  `CAUSE_53_CNT` SMALLINT(6) DEFAULT ''0'',
				  `CAUSE_65_CNT` SMALLINT(6) DEFAULT ''0'',
				  `CAUSE_114_CNT` SMALLINT(6) DEFAULT ''0'',
				  `CAUSE_263_CNT` SMALLINT(6) DEFAULT ''0'',
				  `CAUSE_CAPACITY` SMALLINT(6) DEFAULT ''0'',
				  `BEST_RSCP_1` DOUBLE DEFAULT NULL,
				  `BEST_RSCP_1_MED` DOUBLE DEFAULT NULL,
				  `BEST_ECN0_1` DOUBLE DEFAULT NULL,
				  `BEST_ECN0_1_MED` DOUBLE DEFAULT NULL,
				  `IRAT_HHO_ATTEMPT` MEDIUMINT(9) DEFAULT ''0'',
				  `IRAT_HHO_SUCCESS` MEDIUMINT(9) DEFAULT ''0'',
				  `SYNCFAILURE_CNT` MEDIUMINT(9) DEFAULT ''0'',
				  `POS_LAST_RSCP_CNT` DOUBLE DEFAULT NULL,
				  `POS_LAST_ECN0_CNT` DOUBLE DEFAULT NULL,
				  `NAS_CAUSE_SM_SUM` DOUBLE DEFAULT NULL,
				  `NAS_GPRS_MM_DETACH_SUM` DOUBLE DEFAULT NULL,
				  `PDP_DEACTIVATION_REQUEST_CNT` INT(11) DEFAULT NULL,
				  `PDP_DEACTIVATION_ACCEPT_CNT` INT(11) DEFAULT NULL,
				  `IRAT_HHO_FAILURE` INT(11) DEFAULT NULL
		';
	
		DECLARE table_tile_start_lac_STR VARCHAR(5500) DEFAULT 
		'
				  `RNC_ID` MEDIUMINT(9) NOT NULL COMMENT ''KEY'',
				  `LAC` MEDIUMINT(9) NOT NULL COMMENT ''KEY'',
				  `CALL_CNT` MEDIUMINT(9) DEFAULT 0,
				  `BEST_RSCP_1` DOUBLE DEFAULT NULL,
				  `BEST_RSCP_1_MED` DOUBLE DEFAULT NULL,
				  `BEST_ECN0_1` DOUBLE DEFAULT NULL,
				  `BEST_ECN0_1_MED` DOUBLE DEFAULT NULL,
				  `SUB_DENSITY` MEDIUMINT(9) DEFAULT NULL,
				  `ERLANG` DOUBLE DEFAULT NULL,
				  `DL_DATA_THRU` DOUBLE DEFAULT NULL,
				  `UL_DATA_THRU` DOUBLE DEFAULT NULL,
				  `DL_TRAFFIC` DOUBLE DEFAULT NULL,
				  `UL_TRAFFIC` DOUBLE DEFAULT NULL,
				  `DL_THRU_HIGH_CALL_CNT` INT(11) DEFAULT 0,
				  `DL_THRU_HIGH` DOUBLE DEFAULT NULL,
				  `UL_THRU_HIGH_CALL_CNT` INT(11) DEFAULT 0,
				  `UL_THRU_HIGH` DOUBLE DEFAULT NULL,
				  `U_UL_DATA_THRU` DOUBLE DEFAULT NULL,
				  `U_MAX_UL_DATA_THRU` DOUBLE DEFAULT NULL COMMENT ''MAX'',
				  `U_DL_DATA_THRU` DOUBLE DEFAULT NULL,
				  `U_MAX_DL_DATA_THRU` DOUBLE DEFAULT NULL COMMENT ''MAX'',
				  `OVERSHOOTING_CALL_COUNT` MEDIUMINT(9) DEFAULT 0,
				  `CALL_CNT_ACT` MEDIUMINT(9) DEFAULT 0,
				  `BEST_RSCP_1_ACT` DOUBLE DEFAULT NULL,
				  `BEST_RSCP_1_ACT_MED` DOUBLE DEFAULT NULL,
				  `BEST_ECN0_1_ACT` DOUBLE DEFAULT NULL,
				  `BEST_ECN0_1_ACT_MED` DOUBLE DEFAULT NULL,
				  `RAB_NO_RESOURCE_AVAILABLE` MEDIUMINT(9) DEFAULT NULL,
				  `RAB_RELEASE_DUE_TO_UTRAN_GENERATED_REASON` MEDIUMINT(9) DEFAULT NULL,
				  `RAB_OTHER` MEDIUMINT(9) DEFAULT NULL,
				  `RRC_CONGESTION` MEDIUMINT(9) DEFAULT NULL,
				  `RRC_UNSPECIFIED` MEDIUMINT(9) DEFAULT NULL,
				  `BEST_RSCP_1_CNT` DOUBLE DEFAULT NULL,
				  `BEST_ECN0_1_CNT` DOUBLE DEFAULT NULL,
				  `U_UL_DATA_THRU_CNT` DOUBLE DEFAULT NULL,
				  `U_DL_DATA_THRU_CNT` DOUBLE DEFAULT NULL,
				  `BEST_RSCP_1_ACT_CNT` DOUBLE DEFAULT NULL,
				  `BEST_ECN0_1_ACT_CNT` DOUBLE DEFAULT NULL,
				  `GROUP_CANCAT_IMSI` MEDIUMTEXT,
				  `HOUR_CNT` INT(10) UNSIGNED DEFAULT NULL,
				  `NAS_SM_SUM` DOUBLE DEFAULT NULL,
				  `NAS_GPRS_MM_ATTACH_SUM` DOUBLE DEFAULT NULL,
				  `CALL_TYPE_10` MEDIUMINT(9) DEFAULT 0,
				  `CALL_TYPE_11` MEDIUMINT(9) DEFAULT 0,
				  `CALL_TYPE_12` MEDIUMINT(9) DEFAULT 0,
				  `CALL_TYPE_13` MEDIUMINT(9) DEFAULT 0,
				  `CALL_TYPE_14` MEDIUMINT(9) DEFAULT 0,
				  `CALL_TYPE_16` MEDIUMINT(9) DEFAULT 0,
				  `CALL_TYPE_18` MEDIUMINT(9) DEFAULT 0,
				  `AUTH_REQUEST` INT(11) DEFAULT NULL,
				  `AUTH_RESPONSE` INT(11) DEFAULT NULL,
				  `AUTH_FAILURE` INT(11) DEFAULT NULL,
				  `AUTH_REJECT` INT(11) DEFAULT NULL,
				  `LU_REQUEST` INT(11) DEFAULT NULL,
				  `LU_ACCEPT` INT(11) DEFAULT NULL,
				  `LU_REJECT` INT(11) DEFAULT NULL,
				  `AUTH_CIPH_REQUEST` INT(11) DEFAULT NULL,
				  `AUTH_CIPH_RESPONSE` INT(11) DEFAULT NULL,
				  `AUTH_CIPH_FAILURE` INT(11) DEFAULT NULL,
				  `AUTH_CIPH_REJECT` INT(11) DEFAULT NULL,
				  `RAU_REQUEST` INT(11) DEFAULT NULL,
				  `RAU_ACCEPT` INT(11) DEFAULT NULL,
				  `RAU_REJECT` INT(11) DEFAULT NULL,
				  `PDP_ACTIVATION_REQUEST_CNT` INT(11) DEFAULT NULL,
				  `PDP_ACTIVATION_ACCEPT_CNT` INT(11) DEFAULT NULL,
				  `PDP_ACTIVATION_REJECT_CNT` INT(11) DEFAULT NULL,
				  `RRC_2000000` INT(11) DEFAULT NULL,
				  `RRC_2001000` INT(11) DEFAULT NULL,
				  `RAB_1006000` INT(11) DEFAULT NULL,
				  `RAB_1014000` INT(11) DEFAULT NULL,
				  `RAB_1015000` INT(11) DEFAULT NULL,
				  `RAB_1019000` INT(11) DEFAULT NULL,
				  `RAB_1033000` INT(11) DEFAULT NULL,
				  `RAB_1034000` INT(11) DEFAULT NULL,
				  `RAB_1039000` INT(11) DEFAULT NULL,
				  `RAB_1040000` INT(11) DEFAULT NULL,
				  `RAB_1042000` INT(11) DEFAULT NULL,
				  `RAB_1046000` INT(11) DEFAULT NULL,
				  `RAB_5113000` INT(11) DEFAULT NULL,
				  `RAB_5114000` INT(11) DEFAULT NULL,
				  `RRC_RAB_OTHERS` INT(11) DEFAULT NULL,
				  `CSFB_SETUP_TIME_SUM` INT(11) UNSIGNED DEFAULT 0,
				  `CSFB_SETUP_TIME_CNT` INT(11) UNSIGNED DEFAULT 0,
				  `CSFB_SETUP_TIME_MAX` MEDIUMINT(9) DEFAULT 0 COMMENT ''MAX'',
				  `CSFB_CALL_CNT` INT(11) UNSIGNED DEFAULT 0,
				  `RAB_FAILURE_IN_THE_RADIO_INTERFACE_PROCEDURE` INT(11) DEFAULT 0,
				  `RAB_REQUESTED_MAXIMUM_BIT_RATE_FOR_DL_UL_NOT_AVAILABLE` INT(11) DEFAULT 0,
				  `RAB_NO_RADIO_RESOURCES_AVAILABLE_IN_TARGET_CELL` INT(11) DEFAULT 0,
				  `RAB_LU_TRANSPORT_CONNECTION_FAILED_TO_ESTABLISH` INT(11) DEFAULT 0,
				  `RAB_UNSPECIFIED_FAILURE` INT(11) DEFAULT 0,
				  `SMS_TYPE_01` INT(11) DEFAULT 0,
				  `SMS_TYPE_10` INT(11) DEFAULT 0,
				  `SMS_TYPE_11` INT(11) DEFAULT 0,
				  `SMS_TYPE_00` INT(11) DEFAULT 0,
				  `SETUP_TIME_SUM` INT(11) UNSIGNED DEFAULT 0,
				  `SETUP_TIME_CNT` INT(11) UNSIGNED DEFAULT 0,
				  `SETUP_TIME_MAX` INT(11) DEFAULT 0 COMMENT ''MAX'',
				  `ROAMING_CALL_COUNT` INT(11) DEFAULT 0,
				  `ROAMING_IN_ATTEMPT` INT(11) DEFAULT 0,
				  `ROAMING_IN_FAILURE` INT(11) DEFAULT 0,
				  `MO_SETUP_TIME_SUM` INT(11) DEFAULT 0,
				  `MO_SETUP_CNT` INT(11) DEFAULT 0,
				  `MO_SETUP_TIME_MAX` INT(11) DEFAULT 0 COMMENT ''MAX'',
				  `MT_SETUP_TIME_SUM` INT(11) DEFAULT 0,
				  `MT_SETUP_CNT` INT(11) DEFAULT 0,
				  `MT_SETUP_TIME_MAX` INT(11) DEFAULT 0 COMMENT ''MAX'',
				  `RAB_RESPONSE_TIME_SUM` DOUBLE DEFAULT 0,
				  `RAB_RESPONSE_TIME_CNT` INT(11) DEFAULT 0,
				  `RAB_RESPONSE_TIME_MAX` DOUBLE DEFAULT 0
		';
	
		SET MAX_HR=24;
		SET STR_HR=0;
		SET qry_tbl_name_hr='';
		SET qry_tbl_name_hr_2='';
		SET qry_tbl_name_hr_3='';
		SET v_k=STR_HR;
		SET v_k_Diff=1;
	
		SET TBL_NAME_table_tile_end_lac='table_tile_end_lac';
		SET TBL_NAME_table_tile_end_lac_def='table_tile_end_lac_def';
		SET TBL_NAME_table_tile_start_lac='table_tile_start_lac';
		SET TBL_NAME_table_tile_start_lac_def='table_tile_start_lac_def';
	
	
		
		
	
		SET @SqlCmd =CONCAT('DROP TABLE IF EXISTS ',GT_DB,'.opt_inter_irat_pri;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		SET @SqlCmd=CONCAT('CREATE TABLE ',GT_DB,'.`opt_inter_irat_pri` 
				(
				  `RNC_ID` MEDIUMINT(9) DEFAULT NULL,
				  `CELL_ID` MEDIUMINT(9) DEFAULT NULL,
				  `NBR_BSC_ID` INT(11) DEFAULT NULL,
				  `NBR_CELL_ID` MEDIUMINT(9) DEFAULT NULL,
				  `NBR_TYPE` SMALLINT(6) DEFAULT ''0'',
				  `PRIORITY` TINYINT(4) DEFAULT NULL,
				  `IRAT_EVT_CNT` DECIMAL(14,4) DEFAULT ''0.0000'',
				  `AVG_RSSI` DECIMAL(14,4) DEFAULT NULL,
				  `IRAT_CALL_SUCCESS` DECIMAL(14,4) DEFAULT ''0.0000'',
				  `DISTANCE_METER` MEDIUMINT(9) DEFAULT NULL,
				  `NBR_AZIMUTH_ANGLE` INT(11) DEFAULT NULL,
				  `PRI_IRAT_HO` MEDIUMINT(9) DEFAULT NULL,
				  `PRI_IRAT_EVENT` MEDIUMINT(9) DEFAULT NULL,
				  `PRI_PRIORITY` MEDIUMINT(9) DEFAULT NULL,
				  `PRI_DISTANCE` MEDIUMINT(9) DEFAULT NULL,
				  `PRI_RSSI` MEDIUMINT(9) DEFAULT NULL,
				  `PRI_ANG` MEDIUMINT(9) UNSIGNED DEFAULT NULL,
				  `PRI_WEIGHTED` MEDIUMINT(9) UNSIGNED DEFAULT NULL,
				  KEY `IX_RNC_CELL` (`CELL_ID`,`RNC_ID`,`NBR_CELL_ID`,`NBR_BSC_ID`)
				) ENGINE=MYISAM DEFAULT CHARSET=latin1'
				);	
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt; 
	
	-- qry_tbl_name_hr
	SET @SqlCmd =CONCAT('DROP TABLE IF EXISTS ',GT_DB,'.table_tile_end_lac;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	SET @SqlCmd=CONCAT('CREATE TABLE ',GT_DB,'.`table_tile_end_lac` 
				(
				  `DATA_DATE` DATE NOT NULL COMMENT ''KEY'',
				  `DATA_HOUR` TINYINT(4) NOT NULL COMMENT ''KEY'',
				  `CALL_TYPE` TINYINT(4) NOT NULL COMMENT ''KEY'',
				  `CALL_STATUS` TINYINT(4) NOT NULL COMMENT ''KEY'',
				  `MOVING` TINYINT(4) NOT NULL COMMENT ''KEY'',
				  `INDOOR` TINYINT(4) NOT NULL COMMENT ''KEY'',
				  `FREQUENCY` SMALLINT(6) NOT NULL COMMENT ''KEY'',
				  `UARFCN` MEDIUMINT(9) NOT NULL COMMENT ''KEY'',
				  ',table_tile_end_lac_STR,',
				  PRIMARY KEY (`DATA_DATE`,`DATA_HOUR`,`CALL_TYPE`,`CALL_STATUS`,`MOVING`,`INDOOR`,`FREQUENCY`,`UARFCN`,`RNC_ID`,`LAC`),
				  KEY `IX_CELL_ID` (`LAC`)
				) ENGINE=MYISAM DEFAULT CHARSET=latin1 DELAY_KEY_WRITE=1'
				);
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt; 
	
	--  qry_tbl_name_hr_2
	SET @SqlCmd =CONCAT('DROP TABLE IF EXISTS ',GT_DB,'.table_tile_end_lac_def;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	SET @SqlCmd=CONCAT('CREATE TABLE ',GT_DB,'.`table_tile_end_lac_def` 
				(
				  `DATA_DATE` DATE NOT NULL COMMENT ''KEY'',
				  `DATA_HOUR` TINYINT(4) NOT NULL COMMENT ''KEY'',
				   ',table_tile_end_lac_STR,',
				  PRIMARY KEY (`DATA_DATE`,`DATA_HOUR`,`RNC_ID`,`LAC`),
				  KEY `IX_CELL_ID` (`LAC`)
				  
				) ENGINE=MYISAM DEFAULT CHARSET=latin1 DELAY_KEY_WRITE=1'
				);	
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt; 
	
	SET @SqlCmd =CONCAT('DROP TABLE IF EXISTS ',GT_DB,'.table_tile_end_lac_dy;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	SET @SqlCmd=CONCAT('CREATE TABLE ',GT_DB,'.`table_tile_end_lac_dy` 
				(
				  `DATA_DATE` DATE NOT NULL COMMENT ''KEY'',
				  `CALL_TYPE` TINYINT(4) NOT NULL COMMENT ''KEY'',
				  `CALL_STATUS` TINYINT(4) NOT NULL COMMENT ''KEY'',
				  `MOVING` TINYINT(4) NOT NULL COMMENT ''KEY'',
				  `INDOOR` TINYINT(4) NOT NULL COMMENT ''KEY'',
				  `FREQUENCY` SMALLINT(6) NOT NULL COMMENT ''KEY'',
				  `UARFCN` MEDIUMINT(9) NOT NULL COMMENT ''KEY'',
				  ',table_tile_end_lac_STR,',
				  PRIMARY KEY (`DATA_DATE`,`CALL_TYPE`,`CALL_STATUS`,`MOVING`,`INDOOR`,`FREQUENCY`,`UARFCN`,`RNC_ID`,`LAC`),
				  KEY `IX_CELL_ID` (`LAC`)
				  
				) ENGINE=MYISAM DEFAULT CHARSET=latin1 DELAY_KEY_WRITE=1'
				);	
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt; 
	
	
	SET @SqlCmd =CONCAT('DROP TABLE IF EXISTS ',GT_DB,'.table_tile_end_lac_dy_def;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;	
	SET @SqlCmd=CONCAT('CREATE TABLE ',GT_DB,'.`table_tile_end_lac_dy_def` 
				(
				  `DATA_DATE` DATE NOT NULL COMMENT ''KEY'',
				   ',table_tile_end_lac_STR,',
				  PRIMARY KEY (`DATA_DATE`,`RNC_ID`,`LAC`),
				  KEY `IX_CELL_ID` (`LAC`)
				  
				) ENGINE=MYISAM DEFAULT CHARSET=latin1 DELAY_KEY_WRITE=1'
				);	
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt; 
	
	--  qry_tbl_name_hr_3
	SET @SqlCmd =CONCAT('DROP TABLE IF EXISTS ',GT_DB,'.table_tile_start_lac;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;	
	SET @SqlCmd=CONCAT('CREATE TABLE ',GT_DB,'.`table_tile_start_lac` 
				(
				  `DATA_DATE` DATE NOT NULL COMMENT ''KEY'',
				  `DATA_HOUR` TINYINT(4) NOT NULL COMMENT ''KEY'',
				  `CALL_TYPE` TINYINT(4) NOT NULL COMMENT ''KEY'',
				  `CALL_STATUS` TINYINT(4) NOT NULL COMMENT ''KEY'',
				  `MOVING` TINYINT(4) NOT NULL COMMENT ''KEY'',
				  `INDOOR` TINYINT(4) NOT NULL COMMENT ''KEY'',
				  `FREQUENCY` SMALLINT(6) NOT NULL COMMENT ''KEY'',
				  `UARFCN` MEDIUMINT(9) NOT NULL COMMENT ''KEY'',
				  ',table_tile_start_lac_STR,',
				  PRIMARY KEY (`DATA_DATE`,`DATA_HOUR`,`CALL_TYPE`,`CALL_STATUS`,`MOVING`,`INDOOR`,`FREQUENCY`,`UARFCN`,`RNC_ID`,`LAC`),
				  KEY `IX_CELL_ID` (`LAC`)	
				) ENGINE=MYISAM DEFAULT CHARSET=latin1 DELAY_KEY_WRITE=1'
				);	
	
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt; 
	
	--  qry_tbl_name_hr_4
	SET @SqlCmd =CONCAT('DROP TABLE IF EXISTS ',GT_DB,'.table_tile_start_lac_def;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;	
	SET @SqlCmd=CONCAT('CREATE TABLE ',GT_DB,'.`table_tile_start_lac_def` 
				(
				  `DATA_DATE` DATE NOT NULL COMMENT ''KEY'',
				  `DATA_HOUR` TINYINT(4) NOT NULL COMMENT ''KEY'',
				  ',table_tile_start_lac_STR,',
				  PRIMARY KEY (`DATA_DATE`,`DATA_HOUR`,`RNC_ID`,`LAC`),
				  KEY `IX_CELL_ID` (`LAC`)				  
				) ENGINE=MYISAM DEFAULT CHARSET=latin1 DELAY_KEY_WRITE=1'
				);
	
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt; 
	
	
	SET @SqlCmd =CONCAT('DROP TABLE IF EXISTS ',GT_DB,'.table_tile_start_lac_dy;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	SET @SqlCmd=CONCAT('CREATE TABLE ',GT_DB,'.`table_tile_start_lac_dy` 
				(
				  `DATA_DATE` DATE NOT NULL COMMENT ''KEY'',
				  `CALL_TYPE` TINYINT(4) NOT NULL COMMENT ''KEY'',
				  `CALL_STATUS` TINYINT(4) NOT NULL COMMENT ''KEY'',
				  `MOVING` TINYINT(4) NOT NULL COMMENT ''KEY'',
				  `INDOOR` TINYINT(4) NOT NULL COMMENT ''KEY'',
				  `FREQUENCY` SMALLINT(6) NOT NULL COMMENT ''KEY'',
				  `UARFCN` MEDIUMINT(9) NOT NULL COMMENT ''KEY'',
				  ',table_tile_start_lac_STR,',
				  PRIMARY KEY (`DATA_DATE`,`CALL_TYPE`,`CALL_STATUS`,`MOVING`,`INDOOR`,`FREQUENCY`,`UARFCN`,`RNC_ID`,`LAC`),
				  KEY `IX_CELL_ID` (`LAC`)
				  
				) ENGINE=MYISAM DEFAULT CHARSET=latin1 DELAY_KEY_WRITE=1'
				);	
	
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt; 
	
	SET @SqlCmd =CONCAT('DROP TABLE IF EXISTS ',GT_DB,'.table_tile_start_lac_dy_def;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	SET @SqlCmd=CONCAT('CREATE TABLE ',GT_DB,'.`table_tile_start_lac_dy_def` 
				(
				  `DATA_DATE` DATE NOT NULL COMMENT ''KEY'',
				  ',table_tile_start_lac_STR,',
				  PRIMARY KEY (`DATA_DATE`,`RNC_ID`,`LAC`),
				  KEY `IX_CELL_ID` (`LAC`)				  
				) ENGINE=MYISAM DEFAULT CHARSET=latin1 DELAY_KEY_WRITE=1'
				);	
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt; 
	
	SET @SqlCmd =CONCAT('DROP TABLE IF EXISTS ',GT_DB,'.tmp_opt_aco_traffic_unbalance;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	SET @SqlCmd=CONCAT('CREATE TABLE ',GT_DB,'.`tmp_opt_aco_traffic_unbalance` 
				(
				  `RNC_ID` MEDIUMINT(9) DEFAULT NULL,
				  `CELL_ID` MEDIUMINT(9) DEFAULT NULL,
				  `DATA_DATE` DATETIME DEFAULT NULL,
				  `DATA_HOUR` TINYINT(4) DEFAULT NULL,
				  `PS_CALL_COUNT` MEDIUMINT(9) DEFAULT ''0'',
				  `CS_CALL_COUNT` MEDIUMINT(9) DEFAULT ''0'',
				  `AVG_RTWP` DOUBLE DEFAULT NULL,
				  `MAX_RTWP` DOUBLE DEFAULT NULL,
				  `MIN_THROUGHPUT` DOUBLE DEFAULT NULL,
				  `MAX_THROUGHPUT` DOUBLE DEFAULT NULL,
				  `AVG_THROUGHPUT` DOUBLE DEFAULT NULL,
				  `TRAFFIC_VOLUME_DIST` DOUBLE DEFAULT NULL,
				  `TOTAL_ERLANG_DIST` DOUBLE DEFAULT NULL,
				  `BLOCK_RATE` DOUBLE DEFAULT NULL,
				  `BLOCK_CALL_CNT` MEDIUMINT(9) DEFAULT ''0'',
				  `BLOCK_WEIGHT` DOUBLE DEFAULT NULL,
				  `AVG_TRA_HIGH_THAN_NEB` DOUBLE DEFAULT NULL,
				  `NBR_CNT` MEDIUMINT(9) DEFAULT NULL,
				  `CALL_CONCURRENT` MEDIUMINT(9) DEFAULT NULL,
				  `FACTOR` DOUBLE DEFAULT NULL,
				  `AVG_TRA_DIFF_RATE` DOUBLE DEFAULT NULL,
				  `MAX_TRA_DIFF_RATE` DOUBLE DEFAULT NULL,
				  KEY `opt_aco_traffic_unbalance_idx1` (`CELL_ID`,`DATA_HOUR`)
				 ) ENGINE=MYISAM DEFAULT CHARSET=latin1'
				);	
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt; 
	
	
	WHILE v_k < MAX_HR DO
	BEGIN
		SET qry_tbl_name_hr=CONCAT(TBL_NAME_table_tile_end_lac,'_',LPAD(v_k,2,0));
		SET qry_tbl_name_hr_2=CONCAT(TBL_NAME_table_tile_end_lac_def,'_',LPAD(v_k,2,0));
		SET qry_tbl_name_hr_3=CONCAT(TBL_NAME_table_tile_start_lac,'_',LPAD(v_k,2,0));
		SET qry_tbl_name_hr_4=CONCAT(TBL_NAME_table_tile_start_lac_def,'_',LPAD(v_k,2,0));
		
		SET @SqlCmd=CONCAT('DROP TABLE IF EXISTS ',GT_DB,'.',qry_tbl_name_hr,';');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		SET @SqlCmd=CONCAT('CREATE TABLE ',GT_DB,'.',qry_tbl_name_hr,' LIKE ',GT_DB,'.',TBL_NAME_table_tile_end_lac,';');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		IF UNION_STR IS NULL THEN
			SET UNION_STR = CONCAT(GT_DB,'.',qry_tbl_name_hr,'');
		ELSE
			SET UNION_STR = CONCAT(UNION_STR,',',GT_DB,'.',qry_tbl_name_hr,'');
		END IF;	
		
		SET @SqlCmd=CONCAT('DROP TABLE IF EXISTS ',GT_DB,'.',qry_tbl_name_hr_2,';');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		SET @SqlCmd=CONCAT('CREATE TABLE ',GT_DB,'.',qry_tbl_name_hr_2,' LIKE ',GT_DB,'.',TBL_NAME_table_tile_end_lac_def,';');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		IF UNION_STR_2 IS NULL THEN
			SET UNION_STR_2 = CONCAT(GT_DB,'.',qry_tbl_name_hr_2,'');
		ELSE
			SET UNION_STR_2 = CONCAT(UNION_STR_2,',',GT_DB,'.',qry_tbl_name_hr_2,'');
		END IF;	
		SET @SqlCmd=CONCAT('DROP TABLE IF EXISTS ',GT_DB,'.',qry_tbl_name_hr_3,';');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;	
		SET @SqlCmd=CONCAT('CREATE TABLE ',GT_DB,'.',qry_tbl_name_hr_3,' LIKE ',GT_DB,'.',TBL_NAME_table_tile_start_lac,';');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		IF UNION_STR_3 IS NULL THEN
			SET UNION_STR_3 = CONCAT(GT_DB,'.',qry_tbl_name_hr_3,'');
		ELSE
			SET UNION_STR_3 = CONCAT(UNION_STR_3,',',GT_DB,'.',qry_tbl_name_hr_3,'');
		END IF;	
	
		SET @SqlCmd=CONCAT('DROP TABLE IF EXISTS ',GT_DB,'.',qry_tbl_name_hr_4,';');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		SET @SqlCmd=CONCAT('CREATE TABLE ',GT_DB,'.',qry_tbl_name_hr_4,' LIKE ',GT_DB,'.',TBL_NAME_table_tile_start_lac_def,';');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		IF UNION_STR_4 IS NULL THEN
			SET UNION_STR_4 = CONCAT(GT_DB,'.',qry_tbl_name_hr_4,'');
		ELSE
			SET UNION_STR_4 = CONCAT(UNION_STR_4,',',GT_DB,'.',qry_tbl_name_hr_4,'');
		END IF;	
		SET v_k=v_k+v_k_Diff;
	END;
	END WHILE;
	
	SET @SqlCmd=CONCAT('ALTER TABLE ',GT_DB,'.',TBL_NAME_table_tile_end_lac,' ENGINE = MRG_MYISAM UNION=(',UNION_STR,');');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	SET @SqlCmd=CONCAT('ALTER TABLE ',GT_DB,'.',TBL_NAME_table_tile_end_lac_def,' ENGINE = MRG_MYISAM UNION=(',UNION_STR_2,');');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	SET @SqlCmd=CONCAT('ALTER TABLE ',GT_DB,'.',TBL_NAME_table_tile_start_lac,' ENGINE = MRG_MYISAM UNION=(',UNION_STR_3,');');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	SET @SqlCmd=CONCAT('ALTER TABLE ',GT_DB,'.',TBL_NAME_table_tile_start_lac_def,' ENGINE = MRG_MYISAM UNION=(',UNION_STR_4,');');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
END$$
DELIMITER ;
