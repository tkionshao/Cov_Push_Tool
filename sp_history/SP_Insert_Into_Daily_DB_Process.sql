DELIMITER $$
USE `gt_gw_main`$$
DROP PROCEDURE IF EXISTS `SP_CreateDB_LTE`$$
CREATE DEFINER=`covmo`@`%` PROCEDURE `SP_Insert_Into_Daily_DB_Process`(IN FROM_GT_DB VARCHAR(100),IN TO_GT_DB VARCHAR(100),IN VENDER_ID INT,IN GW_IP VARCHAR(50),IN RUN_STATUS VARCHAR(10))
BEGIN
	DECLARE START_TIME DATETIME DEFAULT SYSDATE();
	DECLARE FILEDATE VARCHAR(100) DEFAULT  RIGHT(FROM_GT_DB,18);
	DECLARE RNC_ID INT; 
	DECLARE FILE_STARTTIME VARCHAR(18) DEFAULT CONCAT(LEFT(FILEDATE,4),'-',SUBSTRING(FILEDATE,5,2),'-',SUBSTRING(FILEDATE,7,2),' ', SUBSTRING(FILEDATE,10,2),':',SUBSTRING(FILEDATE,12,2));
	DECLARE FILE_ENDTIME VARCHAR(20) DEFAULT CONCAT(LEFT(FILEDATE,4),'-',SUBSTRING(FILEDATE,5,2),'-',SUBSTRING(FILEDATE,7,2),' ', SUBSTRING(FILEDATE,15,2),':',SUBSTRING(FILEDATE,17,2),':00');	
	DECLARE DB_FLAG INT DEFAULT 0;
	DECLARE PARTITION_ID INT DEFAULT SUBSTRING(RIGHT(FROM_GT_DB,18),10,2) ;
	DECLARE GT_DB_START_HOUR INT DEFAULT SUBSTRING(RIGHT(FROM_GT_DB,18),10,4);
	DECLARE ORG_NT_DATE VARCHAR(20) DEFAULT SUBSTRING(RIGHT(TO_GT_DB,18),1,8);
	DECLARE NT_DATE VARCHAR(20) DEFAULT CONCAT(SUBSTRING(ORG_NT_DATE,1,4),'-',SUBSTRING(ORG_NT_DATE,5,2),'-',SUBSTRING(ORG_NT_DATE,7,2));
	DECLARE GT_DB_START_MIN VARCHAR(10) DEFAULT SUBSTRING(RIGHT(FROM_GT_DB,18),12,2);
	
	SET FILE_ENDTIME=IF(RIGHT(FILE_ENDTIME,8)='00:00:00',DATE_ADD(FILE_ENDTIME, INTERVAL 1 DAY),FILE_ENDTIME) ; 	
	SELECT gt_strtok(TO_GT_DB,2,'_') INTO RNC_ID;
	
	SELECT COUNT(*) INTO DB_FLAG FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = TO_GT_DB;
	
	
	IF DB_FLAG = 0 THEN
		CALL SP_CreateDB(RNC_ID,RIGHT(TO_GT_DB,18),'DAILY');
		SELECT 1 AS Message;
	ELSE
		SELECT 0 AS Message;
	END IF;
	
	SET GW_IP = CONCAT('http://',GW_IP,':8989');
	UPDATE `gt_gw_main`.`session_information` SET `DATA_VENDOR` = VENDER_ID,`GW_IP` = GW_IP WHERE `SESSION_DB` = TO_GT_DB;	
	
	SET @SqlCmd=CONCAT('SELECT COUNT(*) INTO @CHECK_CNT FROM ',FROM_GT_DB,'.table_call_update WHERE DATA_DATE = ''',NT_DATE,''';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	IF @CHECK_CNT > 0 THEN
		INSERT INTO gt_gw_main.sp_log VALUES(FROM_GT_DB,'SP_Insert_Into_Daily_DB_Process','QUARTER DATA INTO daily START', NOW());
	
		
		IF RUN_STATUS = 'rerun' THEN
			SET @data_quarter = 
				CASE WHEN GT_DB_START_MIN = 00 THEN 0
				WHEN GT_DB_START_MIN = 15 THEN 1
				WHEN GT_DB_START_MIN = 30 THEN 2
				WHEN GT_DB_START_MIN = 45 THEN 3 END;	
			SET @SqlCmd=CONCAT('SELECT COUNT(*) INTO @CHK1 FROM ',TO_GT_DB,'.`table_call` WHERE DATA_HOUR = ',PARTITION_ID,' AND DATA_QUARTER = ''',@data_quarter,''';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
			SELECT GT_DB_START_MIN;
			IF @CHK1 > 0 THEN
	
				INSERT INTO gt_gw_main.sp_log VALUES(FROM_GT_DB,'SP_Insert_Into_Daily_DB_Process','DELETE INTO table_call_update', NOW());
				SET @SqlCmd=CONCAT('DELETE FROM ',TO_GT_DB,'.`table_call` WHERE DATA_HOUR = ',PARTITION_ID,' AND DATA_QUARTER = ''',@data_quarter,''';');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
			END IF;
		END IF;
		
		INSERT INTO gt_gw_main.sp_log VALUES(FROM_GT_DB,'SP_Insert_Into_Daily_DB_Process','INSERT table_call', NOW());
		
		SET @SqlCmd=CONCAT('INSERT INTO   ',TO_GT_DB,'.table_call
			(CALL_ID,START_TIME,START_TIME_MS,END_TIME,END_TIME_MS,RRC_CONNECT_DURATION,FIRST_EVENT_ID,LAST_EVENT_ID,IMSI,IMEI,
			S_RNTI,LAC,LAC_NEW,RAC,RAC_NEW,START_CELL_ID,START_RNC_ID,END_CELL_ID,END_RNC_ID,ECNO_RACH,RRC_REQUEST_TYPE,CALL_TYPE,
			INITIAL_RAB_CELL_ID,INITIAL_RAB_RNC_ID,IU_RELEASE_CAUSE,IU_RELEASE_REQUEST_CAUSE,SHO,SOHO,IRAT_HHO_ATTEMPT,IRAT_HHO_SUCCESS,
			IRAT_HHO_S_SAI,IRAT_HHO_T_C_ID,IRAT_HHO_T_BCCH_ARFCN,IRAT_HHO_T_NCC_BCC,SRNC_RELOC_ATTEMPT,SRNC_RELOC_SUCCESS,SRNC_RELOC_S_RNC_ID,
			SRNC_RELOC_T_RNC_ID,IRAT_HHO_SRNC_RELOC_CAUSE,IFHO,RRC_CONN_REJ_CAUSE,RAB_ASSIGN_FAIL_CAUSE,UL_THROUGHPUT_AVG,UL_THROUGHPUT_MAX,
			UL_TRAFFIC_VOLUME,DL_THROUGHPUT_AVG,DL_THROUGHPUT_MAX,DL_TRAFFIC_VOLUME,CALL_STATUS,VENDOR_DROP_CAUSE,MOVING,INDOOR,RRC_FAILURE_CNT,
			RRC_FAILURE_EVENT_LAST,RRC_FAILURE_CAUSE_LAST,NBAP_FAILURE_CNT,NBAP_FAILURE_EVENT_LAST,NBAP_FAILURE_CAUSE_LAST,RNSAP_FAILURE_CNT,
			RNSAP_FAILURE_EVNET_LAST,RNSAP_FAILURE_CAUSE_LAST,RANAP_FAILURE_CNT,RANAP_FAILURE_EVENT_LAST,RANAP_FAILURE_CAUSE_LAST,CS_CALL_DURA,
			CS_RAB_CNT,PS_RAB_CNT,CS_TRAF_CLASS,PS_TRAF_CLASS,PS_DL_TRAN_CH,PS_UL_TRAN_CH,UE_RF_BAND_CAP,UE_HSDSCH_CATEGORY,UE_EDCH_CATEGORY,
			LU_FAILURE,RRC_RELEASE_CAUSE,NAS_CAUSE_CC,NAS_CAUSE_SM,NAS_SM,ACCESS_POINT_NAME,MCC,MNC,MULTI_RAB,NAS_GPRS_MM_ATTACH,NAS_GPRS_MM_DETACH,
			IP_ADDRESS,UL_THROUGHPUT_EVENT_CNT,DL_THROUGHPUT_EVENT_CNT,SHO_FAILURE,SOHO_FAILURE,IFHO_FAILURE,MULTI_RAB_DROP_INDICATOR,USER_EXPERIENCE_INDICATOR,
			DATA_DATE,DATA_HOUR,POS_FIRST_RNC,
			POS_FIRST_CELL_INDOOR,POS_FIRST_CLUSTER,POS_FIRST_SITE,POS_FIRST_FREQUENCY,POS_FIRST_UARFCN,POS_FIRST_CELL,POS_FIRST_RSCP,
			POS_FIRST_ECN0,POS_FIRST_LOC,POS_LAST_RNC,POS_LAST_CELL_INDOOR,POS_LAST_CLUSTER,POS_LAST_SITE,POS_LAST_FREQUENCY,POS_LAST_UARFCN,
			POS_LAST_CELL,POS_LAST_RSCP,POS_LAST_ECN0,POS_LAST_LOC,POS_IFHO_RNC,POS_IFHO_CELL_INDOOR,POS_IFHO_CLUSTER,POS_IFHO_SITE,
			POS_IFHO_FREQUENCY,POS_IFHO_UARFCN,POS_IFHO_CELL,POS_IFHO_LOC,POS_UE_RNC,POS_UE_CELL_INDOOR,POS_UE_CLUSTER,POS_UE_SITE,
			POS_UE_FREQUENCY,POS_UE_UARFCN,POS_UE_CELL,POS_UE_LOC,POS_AS_LOC,POS_AS1_RNC,POS_AS1_CELL_INDOOR,POS_AS1_CLUSTER,POS_AS1_SITE,
			POS_AS1_CELL,POS_AS1_FREQUENCY,POS_AS1_UARFCN,POS_AS1_RSCP,POS_AS1_ECN0,IU_RELEASE_CAUSE_STR,IMEI_NEW,APN,SIMULATED,POS_UE_TXP_RNC,
			POS_UE_TXP_FREQUENCY,POS_UE_TXP_UARFCN,POS_UE_TXP_LOC,POS_UE_TXP_QUALITY_VALUE,POS_UL_SIR_RNC,POS_UL_SIR_FREQUENCY,POS_UL_SIR_UARFCN,
			POS_UL_SIR_LOC,POS_UL_SIR_QUALITY_VALUE,POS_UL_SIR_ERR_RNC,POS_UL_SIR_ERR_FREQUENCY,POS_UL_SIR_ERR_UARFCN,POS_UL_SIR_ERR_LOC,
			POS_UL_SIR_ERR_QUALITY_VALUE,POS_IRAT_FAIL_LOC,POS_IFHO_FAIL_LOC,DATA_QUARTER,MSISDN,RAB_RRC_CAUSE_STR,MAKE_ID,MODEL_ID,PING_PONG_HO_CNT,
			MOVING_SPEED,MOVING_TYPE)
		SELECT 
			CALL_ID,START_TIME,START_TIME_MS,END_TIME,END_TIME_MS,RRC_CONNECT_DURATION,FIRST_EVENT_ID,LAST_EVENT_ID,IMSI,IMEI,
			S_RNTI,LAC,LAC_NEW,RAC,RAC_NEW,START_CELL_ID,START_RNC_ID,END_CELL_ID,END_RNC_ID,ECNO_RACH,RRC_REQUEST_TYPE,CALL_TYPE,
			INITIAL_RAB_CELL_ID,INITIAL_RAB_RNC_ID,IU_RELEASE_CAUSE,IU_RELEASE_REQUEST_CAUSE,SHO,SOHO,IRAT_HHO_ATTEMPT,IRAT_HHO_SUCCESS,
			IRAT_HHO_S_SAI,IRAT_HHO_T_C_ID,IRAT_HHO_T_BCCH_ARFCN,IRAT_HHO_T_NCC_BCC,SRNC_RELOC_ATTEMPT,SRNC_RELOC_SUCCESS,SRNC_RELOC_S_RNC_ID,
			SRNC_RELOC_T_RNC_ID,IRAT_HHO_SRNC_RELOC_CAUSE,IFHO,RRC_CONN_REJ_CAUSE,RAB_ASSIGN_FAIL_CAUSE,UL_THROUGHPUT_AVG,UL_THROUGHPUT_MAX,
			UL_TRAFFIC_VOLUME,DL_THROUGHPUT_AVG,DL_THROUGHPUT_MAX,DL_TRAFFIC_VOLUME,CALL_STATUS,VENDOR_DROP_CAUSE,MOVING,INDOOR,RRC_FAILURE_CNT,
			RRC_FAILURE_EVENT_LAST,RRC_FAILURE_CAUSE_LAST,NBAP_FAILURE_CNT,NBAP_FAILURE_EVENT_LAST,NBAP_FAILURE_CAUSE_LAST,RNSAP_FAILURE_CNT,
			RNSAP_FAILURE_EVNET_LAST,RNSAP_FAILURE_CAUSE_LAST,RANAP_FAILURE_CNT,RANAP_FAILURE_EVENT_LAST,RANAP_FAILURE_CAUSE_LAST,CS_CALL_DURA,
			CS_RAB_CNT,PS_RAB_CNT,CS_TRAF_CLASS,PS_TRAF_CLASS,PS_DL_TRAN_CH,PS_UL_TRAN_CH,UE_RF_BAND_CAP,UE_HSDSCH_CATEGORY,UE_EDCH_CATEGORY,
			LU_FAILURE,RRC_RELEASE_CAUSE,NAS_CAUSE_CC,NAS_CAUSE_SM,NAS_SM,ACCESS_POINT_NAME,MCC,MNC,MULTI_RAB,NAS_GPRS_MM_ATTACH,NAS_GPRS_MM_DETACH,
			IP_ADDRESS,UL_THROUGHPUT_EVENT_CNT,DL_THROUGHPUT_EVENT_CNT,SHO_FAILURE,SOHO_FAILURE,IFHO_FAILURE,MULTI_RAB_DROP_INDICATOR,USER_EXPERIENCE_INDICATOR,
			DATA_DATE,DATA_HOUR,POS_FIRST_RNC,
			POS_FIRST_CELL_INDOOR,POS_FIRST_CLUSTER,POS_FIRST_SITE,POS_FIRST_FREQUENCY,POS_FIRST_UARFCN,POS_FIRST_CELL,POS_FIRST_RSCP,
			POS_FIRST_ECN0,POS_FIRST_LOC,POS_LAST_RNC,POS_LAST_CELL_INDOOR,POS_LAST_CLUSTER,POS_LAST_SITE,POS_LAST_FREQUENCY,POS_LAST_UARFCN,
			POS_LAST_CELL,POS_LAST_RSCP,POS_LAST_ECN0,POS_LAST_LOC,POS_IFHO_RNC,POS_IFHO_CELL_INDOOR,POS_IFHO_CLUSTER,POS_IFHO_SITE,
			POS_IFHO_FREQUENCY,POS_IFHO_UARFCN,POS_IFHO_CELL,POS_IFHO_LOC,POS_UE_RNC,POS_UE_CELL_INDOOR,POS_UE_CLUSTER,POS_UE_SITE,
			POS_UE_FREQUENCY,POS_UE_UARFCN,POS_UE_CELL,POS_UE_LOC,POS_AS_LOC,POS_AS1_RNC,POS_AS1_CELL_INDOOR,POS_AS1_CLUSTER,POS_AS1_SITE,
			POS_AS1_CELL,POS_AS1_FREQUENCY,POS_AS1_UARFCN,POS_AS1_RSCP,POS_AS1_ECN0,IU_RELEASE_CAUSE_STR,IMEI_NEW,APN,SIMULATED,POS_UE_TXP_RNC,
			POS_UE_TXP_FREQUENCY,POS_UE_TXP_UARFCN,POS_UE_TXP_LOC,POS_UE_TXP_QUALITY_VALUE,POS_UL_SIR_RNC,POS_UL_SIR_FREQUENCY,POS_UL_SIR_UARFCN,
			POS_UL_SIR_LOC,POS_UL_SIR_QUALITY_VALUE,POS_UL_SIR_ERR_RNC,POS_UL_SIR_ERR_FREQUENCY,POS_UL_SIR_ERR_UARFCN,POS_UL_SIR_ERR_LOC,
			POS_UL_SIR_ERR_QUALITY_VALUE,POS_IRAT_FAIL_LOC,POS_IFHO_FAIL_LOC,DATA_QUARTER,MSISDN,RAB_RRC_CAUSE_STR,MAKE_ID,MODEL_ID,PING_PONG_HO_CNT,
			MOVING_SPEED,MOVING_TYPE
		FROM ',FROM_GT_DB,'.table_call_update WHERE DATA_DATE = ''',NT_DATE,''' AND DATA_HOUR IN (0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23);');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt; 
	
		
		IF RUN_STATUS = 'rerun' THEN
			SET @SqlCmd=CONCAT('SELECT COUNT(*) INTO @CHK2 FROM ',TO_GT_DB,'.`table_position` WHERE BATCH = ',GT_DB_START_HOUR,';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt; 
			
			IF @CHK2 > 0 THEN
				INSERT INTO gt_gw_main.sp_log VALUES(FROM_GT_DB,'SP_Insert_Into_Daily_DB_Process','DELETE table_position', NOW());
			
				SET @SqlCmd=CONCAT('DELETE FROM ',TO_GT_DB,'.`table_position` WHERE BATCH = ',GT_DB_START_HOUR,';');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt; 
			END IF;
		END IF;	
		
		INSERT INTO gt_gw_main.sp_log VALUES(FROM_GT_DB,'SP_Insert_Into_Daily_DB_Process','INSERT table_position', NOW());
		
		SET @SqlCmd=CONCAT('INSERT INTO   ',TO_GT_DB,'.table_position SELECT * FROM ',FROM_GT_DB,'.table_position WHERE DATA_DATE = ''',NT_DATE,''' AND DATA_HOUR IN (0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23);');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
	
		
		IF RUN_STATUS = 'rerun' THEN
			SET @SqlCmd=CONCAT('SELECT COUNT(*) INTO @CHK3 FROM ',TO_GT_DB,'.`table_nbap_cmr` WHERE DATA_HOUR = ',PARTITION_ID,' AND DATE_TIME BETWEEN ''',FILE_STARTTIME,''' AND ''',FILE_ENDTIME,''';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt; 
	
			IF @CHK3 > 0 THEN
				INSERT INTO gt_gw_main.sp_log VALUES(FROM_GT_DB,'SP_Insert_Into_Daily_DB_Process','DELETE table_nbap_cmr', NOW());
			
				SET @SqlCmd=CONCAT('DELETE FROM ',TO_GT_DB,'.`table_nbap_cmr` WHERE DATA_HOUR = ',PARTITION_ID,' AND DATE_TIME BETWEEN ''',FILE_STARTTIME,''' AND ''',FILE_ENDTIME,''';');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt; 
			END IF;
		END IF;	
		
		INSERT INTO gt_gw_main.sp_log VALUES(FROM_GT_DB,'SP_Insert_Into_Daily_DB_Process','INSERT table_nbap_cmr', NOW());
		
		SET @SqlCmd=CONCAT('INSERT INTO   ',TO_GT_DB,'.table_nbap_cmr SELECT * FROM ',FROM_GT_DB,'.table_nbap_cmr WHERE DATA_DATE = ''',NT_DATE,''' AND DATA_HOUR IN (0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23);');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
	
		
		IF RUN_STATUS = 'rerun' THEN
			SET @SqlCmd=CONCAT('SELECT COUNT(*) INTO @CHK4 FROM ',TO_GT_DB,'.`table_cell_usage` WHERE DATA_HOUR = ',PARTITION_ID,' AND DATE_TIME BETWEEN ''',FILE_STARTTIME,''' AND ''',FILE_ENDTIME,''';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt; 
	
			IF @CHK4 > 0 THEN
				INSERT INTO gt_gw_main.sp_log VALUES(FROM_GT_DB,'SP_Insert_Into_Daily_DB_Process','DELETE table_cell_usage', NOW());
			
				SET @SqlCmd=CONCAT('DELETE FROM ',TO_GT_DB,'.`table_cell_usage` WHERE DATA_HOUR = ',PARTITION_ID,' AND DATE_TIME BETWEEN ''',FILE_STARTTIME,''' AND ''',FILE_ENDTIME,''';');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt; 
			END IF;
		END IF;	
		
		INSERT INTO gt_gw_main.sp_log VALUES(FROM_GT_DB,'SP_Insert_Into_Daily_DB_Process','INSERT table_cell_usage', NOW());	
		
		SET @SqlCmd=CONCAT('INSERT INTO   ',TO_GT_DB,'.table_cell_usage SELECT * FROM ',FROM_GT_DB,'.table_cell_usage WHERE DATA_DATE = ''',NT_DATE,''' AND DATA_HOUR IN (0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23);');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
	
		
		IF RUN_STATUS = 'rerun' THEN
			SET @SqlCmd=CONCAT('SELECT COUNT(*) INTO @CHK5 FROM ',TO_GT_DB,'.`table_call_dump` WHERE DATA_HOUR = ',PARTITION_ID,' AND START_TIME BETWEEN ''',FILE_STARTTIME,''' AND ''',FILE_ENDTIME,''';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt; 
	
			IF @CHK5 > 0 THEN
				INSERT INTO gt_gw_main.sp_log VALUES(FROM_GT_DB,'SP_Insert_Into_Daily_DB_Process','DELETE table_call_dump', NOW());
			
				SET @SqlCmd=CONCAT('DELETE FROM ',TO_GT_DB,'.`table_call_dump` WHERE DATA_HOUR = ',PARTITION_ID,' AND START_TIME BETWEEN ''',FILE_STARTTIME,''' AND ''',FILE_ENDTIME,''';');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt; 
			END IF;
		END IF;	
		
		INSERT INTO gt_gw_main.sp_log VALUES(FROM_GT_DB,'SP_Insert_Into_Daily_DB_Process','INSERT table_call_dump', NOW());	
		
		SET @SqlCmd=CONCAT('INSERT INTO   ',TO_GT_DB,'.table_call_dump SELECT * FROM ',FROM_GT_DB,'.table_call_dump WHERE DATA_DATE = ''',NT_DATE,''' AND DATA_HOUR IN (0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23);');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
	
		INSERT INTO gt_gw_main.sp_log VALUES(FROM_GT_DB,'SP_Insert_Into_Daily_DB_Process','INSERT table_aco_update', NOW());	
		SET @SqlCmd=CONCAT('INSERT INTO   ',TO_GT_DB,'.table_aco_update SELECT * FROM ',FROM_GT_DB,'.table_aco_update;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
	
		INSERT INTO gt_gw_main.sp_log VALUES(FROM_GT_DB,'SP_Insert_Into_Daily_DB_Process','INSERT table_tile_erlang_fp_bs_update', NOW());	
		SET @SqlCmd=CONCAT('INSERT INTO   ',TO_GT_DB,'.table_tile_erlang_fp_bs_update SELECT * FROM ',FROM_GT_DB,'.table_tile_erlang_fp_bs_update;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
	
		INSERT INTO gt_gw_main.sp_log VALUES(FROM_GT_DB,'SP_Insert_Into_Daily_DB_Process','INSERT table_tile_fp_update', NOW());	
		SET @SqlCmd=CONCAT('INSERT INTO   ',TO_GT_DB,'.table_tile_fp_update SELECT * FROM ',FROM_GT_DB,'.table_tile_fp_update;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		INSERT INTO gt_gw_main.sp_log VALUES(FROM_GT_DB,'SP_Insert_Into_Daily_DB_Process','INSERT table_call_nopos', NOW());	
		SET @SqlCmd=CONCAT('INSERT INTO   ',TO_GT_DB,'.table_call_nopos SELECT * FROM ',FROM_GT_DB,'.table_call_nopos;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
	
	ELSE
		INSERT INTO gt_gw_main.sp_log VALUES(FROM_GT_DB,'SP_Insert_Into_Daily_DB_Process',CONCAT('NO TABLE_CALL DATA FROM ',FROM_GT_DB,' TO ',TO_GT_DB), NOW());
	END IF;
	INSERT INTO gt_gw_main.SP_LOG VALUES(FROM_GT_DB,'SP_Insert_Into_Daily_DB_Process',CONCAT('Done: ',TIMESTAMPDIFF(SECOND,START_TIME,SYSDATE()),' seconds.'), NOW());
	
END$$
DELIMITER ;
