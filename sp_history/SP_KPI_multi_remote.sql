DELIMITER $$
USE `gt_gw_main`$$
DROP PROCEDURE IF EXISTS `SP_CreateDB_LTE`$$
CREATE DEFINER=`covmo`@`%` PROCEDURE `SP_KPI_multi_remote`(IN GT_DB VARCHAR(100),IN KPI_ID INT(11),IN START_HOUR TINYINT(2),IN END_HOUR TINYINT(2),IN SOURCE_TYPE TINYINT(2),IN SERVICE TINYINT(2)
							,IN DATA_QUARTER VARCHAR(10),IN CELL_ID VARCHAR(100),IN TILE_ID VARCHAR(100)
							,IN IMSI VARCHAR(4096),IN CLUSTER_ID VARCHAR(50),IN CALL_TYPE VARCHAR(50),IN CALL_STATUS VARCHAR(10)
							,IN INDOOR VARCHAR(5),IN MOVING VARCHAR(5)							
							,IN CELL_INDOOR VARCHAR(10),IN FREQUENCY VARCHAR(100) ,IN UARFCN VARCHAR(300),IN CELL_LON VARCHAR(50),IN CELL_LAT VARCHAR(50)
							,IN MSISDN VARCHAR(20),IN IMEI_NEW VARCHAR(20),IN APN VARCHAR(100)
							,IN FILTER VARCHAR(100),IN PID INT(11),IN POS_KIND VARCHAR(10),IN SITE_ID VARCHAR(100)
							,IN MAKE_ID VARCHAR(1024),IN MODEL_ID VARCHAR(1024),IN POLYGON_STR VARCHAR(250),IN WITHDUMP TINYINT(2),IN GT_COVMO VARCHAR(20),TECH_NAME VARCHAR(10),IN IMSI_STR MEDIUMTEXT, IN SPECIAL_IMSI VARCHAR(20)
							,IN SUB_REGION_ID VARCHAR(100),IN ENODEB_ID VARCHAR(100),IN CELL_GID INT(11),IN IMSI_GID SMALLINT(6))
BEGIN	
	DECLARE WORKER_ID VARCHAR(10) DEFAULT CONNECTION_ID();
	DECLARE PU_ID INT;
	DECLARE DATA_DATE DATE;
	DECLARE DS_DATE DATETIME;
	DECLARE NT_DB VARCHAR(100);
	DECLARE FILTER_STR MEDIUMTEXT;
	DECLARE POS_KIND_LOC VARCHAR(10) DEFAULT '';
	DECLARE DY_FLAG TINYINT DEFAULT '0';
	
	DECLARE STR_SEL_IMSI_LTE MEDIUMTEXT DEFAULT '';
	DECLARE STR_SEL_IMSI_GRP_LTE MEDIUMTEXT DEFAULT '';
	DECLARE STR_SEL_IMSI_POS_LTE MEDIUMTEXT DEFAULT '';
	
	DECLARE STR_IMSI_LTE VARCHAR(15000) DEFAULT '';
	DECLARE STR_IMSI_VOLTE_LTE VARCHAR(15000) DEFAULT '';
	DECLARE STR_IMSI_GRP_LTE VARCHAR(15000) DEFAULT '';
	DECLARE STR_IMSI_GRP_VOLTE_LTE VARCHAR(15000) DEFAULT '';
	DECLARE STR_IMSI_POS_LTE VARCHAR(15000) DEFAULT '';
	
	DECLARE TABLE_CALL_IMSI_GSM VARCHAR(50) DEFAULT '';
	DECLARE TABLE_CALL_IMSI_UMTS VARCHAR(50) DEFAULT '';
	DECLARE TABLE_CALL_IMSI_LTE VARCHAR(50) DEFAULT '';
	
	DECLARE STR_SEL_IMSI_UMTS MEDIUMTEXT DEFAULT '';
	DECLARE STR_SEL_IMSI_GRP_UMTS MEDIUMTEXT DEFAULT '';
	DECLARE STR_SEL_IMSI_POS_UMTS MEDIUMTEXT DEFAULT '';
	
	DECLARE STR_IMSI_UMTS VARCHAR(15000) DEFAULT '';
	DECLARE STR_IMSI_GRP_UMTS VARCHAR(15000) DEFAULT '';
	DECLARE STR_IMSI_POS_UMTS VARCHAR(15000) DEFAULT '';
	DECLARE STR_IMSI_WITHDUMP_UMTS VARCHAR(15000) DEFAULT '';
	DECLARE STR_IMSI_GRP_WITHDUMP_UMTS VARCHAR(15000) DEFAULT '';
	
	DECLARE TABLE_ERAB_VOLTE_IMSI_LTE VARCHAR(50) DEFAULT '';
	
	DECLARE STR_SEL_IMSI_AGG_RPT MEDIUMTEXT DEFAULT '';
	DECLARE STR_IMSI_AGG_RPT MEDIUMTEXT DEFAULT '';
	DECLARE PM_COUNTER_FLAG VARCHAR(10);
	DECLARE STR_SEL_IMSI_MR_LTE MEDIUMTEXT DEFAULT '';
	DECLARE STR_SEL_IMSI_MR_GRP_LTE MEDIUMTEXT DEFAULT '';
	DECLARE STR_IMSI_MR_LTE VARCHAR(15000) DEFAULT '';
	DECLARE STR_IMSI_MR_GRP_LTE VARCHAR(15000) DEFAULT '';
	DECLARE TABLE_POS_IMSI_LTE VARCHAR(50) DEFAULT '';

	DECLARE STR_SEL_VIP_GRP_UMTS MEDIUMTEXT DEFAULT '';
	DECLARE STR_SEL_VIP_GRP_LTE MEDIUMTEXT DEFAULT '';
	DECLARE STR_VIP_GRP_UMTS VARCHAR(15000) DEFAULT '';
	DECLARE STR_VIP_GRP_LTE VARCHAR(15000) DEFAULT '';
		
	SELECT LOWER(`value`) INTO PM_COUNTER_FLAG FROM gt_gw_main.integration_param WHERE gt_group = 'sp' AND gt_name = 'pm_counter';
	
	IF  SPECIAL_IMSI ='1' THEN 		
		IF TECH_NAME='GSM' THEN
			SET TABLE_CALL_IMSI_GSM=CONCAT('table_call_gsm_imsig',ABS(IMSI_GID));
		ELSEIF TECH_NAME='UMTS' THEN
			SET TABLE_CALL_IMSI_UMTS=CONCAT('table_call');
		ELSEIF TECH_NAME='LTE' THEN
			SET TABLE_CALL_IMSI_LTE=CONCAT('table_call_lte');	
			SET TABLE_POS_IMSI_LTE=CONCAT('table_position_convert_serving_lte');	
		END IF;	
	ELSE 
		IF TECH_NAME='GSM' THEN
			SET TABLE_CALL_IMSI_GSM='table_call_gsm';
		ELSEIF TECH_NAME='UMTS' THEN
			SET TABLE_CALL_IMSI_UMTS='table_call';
		ELSEIF TECH_NAME='LTE' THEN
			SET TABLE_CALL_IMSI_LTE='table_call_lte';		
			SET TABLE_POS_IMSI_LTE='table_position_convert_serving_lte';
		END IF;		
	END IF;	
	
	SELECT gt_strtok(GT_DB,2,'_') INTO PU_ID;
	SELECT DATE(gt_strtok(GT_DB,3,'_')) INTO DATA_DATE;		
	SELECT gt_strtok(GT_DB,3,'_') INTO DS_DATE;		
	SELECT CONCAT('gt_nt_',gt_strtok(GT_DB,3,'_')) INTO NT_DB;
	SET STR_IMSI_LTE=CONCAT('A.`CALL_ID` AS `CALL_ID`,
				 CONCAT(A.START_TIME,''.'',LPAD(A.START_TIME_MS,3,0)) AS `START_TIME`,
				 CONCAT(A.END_TIME,''.'',LPAD(A.END_TIME_MS,3,0)) AS `END_TIME`,
				  ROUND((A.`DURATION`/1000),2) AS `DURATION`,
				  NULL AS `CS_TRAFFIC_TIME`,
				  A.`CALL_SETUP_TIME` AS `CALL_SETUP_TIME`,
				  A.`IMSI` AS `IMSI`,
				  A.`IMEI` AS `IMEI`,
				  A.`IMSI` AS `MSISDN`,
				  A.`MAKE_ID`,
				  A.`MODEL_ID`,
				  4 AS `TECH_MASK`,
				  (CASE WHEN A.CALL_TYPE=21 THEN ''LTE-TRAFFIC'' WHEN A.CALL_TYPE=22 THEN ''LTE-SIGNALING'' WHEN A.CALL_TYPE=23 THEN ''LTE-VoLTE'' WHEN A.CALL_TYPE=24 THEN ''LTE-SMS'' WHEN A.CALL_TYPE=29 THEN ''LTE-Unspecified'' ELSE ''Unkonwn'' END) AS `CALL_TYPE`,
				  A.`APN`, 
				(CASE WHEN A.CALL_STATUS=1 THEN ''Normal'' WHEN A.CALL_STATUS=2 THEN ''Drop'' WHEN A.CALL_STATUS=3 THEN ''Block'' WHEN A.CALL_STATUS=5 THEN ''CS-Fallback'' WHEN A.CALL_STATUS=6 THEN ''SetupFailure'' ELSE ''Unspecified'' END) AS `CALL_STATUS`,
				 (CASE  
					WHEN A.UE_CONTEXT_RELEASE_CAUSE=1 THEN ''RadioNetwork_unspecified''
					WHEN A.UE_CONTEXT_RELEASE_CAUSE=2 THEN ''RadioNetwork_tx2relocoverall-expiry''
					WHEN A.UE_CONTEXT_RELEASE_CAUSE=3 THEN ''RadioNetwork_successful-handover''
					WHEN A.UE_CONTEXT_RELEASE_CAUSE=4 THEN ''RadioNetwork_release-due-TO-eutran-generated-reason''
					WHEN A.UE_CONTEXT_RELEASE_CAUSE=5 THEN ''RadioNetwork_handover-cancelled''
					WHEN A.UE_CONTEXT_RELEASE_CAUSE=6 THEN ''RadioNetwork_partial-handover''
					WHEN A.UE_CONTEXT_RELEASE_CAUSE=7 THEN ''RadioNetwork_ho-failure-IN-target-EPC-eNB-OR-target-system''
					WHEN A.UE_CONTEXT_RELEASE_CAUSE=8 THEN ''RadioNetwork_ho-target-NOT-allowed''
					WHEN A.UE_CONTEXT_RELEASE_CAUSE=9 THEN ''RadioNetwork_tS1relocoverall-expiry''
					WHEN A.UE_CONTEXT_RELEASE_CAUSE=10 THEN ''RadioNetwork_tS1relocprep-expiry''
					WHEN A.UE_CONTEXT_RELEASE_CAUSE=11 THEN ''RadioNetwork_cell-NOT-available''
					WHEN A.UE_CONTEXT_RELEASE_CAUSE=12 THEN ''RadioNetwork_unknown-targetID''
					WHEN A.UE_CONTEXT_RELEASE_CAUSE=13 THEN ''RadioNetwork_no-radio-resources-available-IN-target-cell''
					WHEN A.UE_CONTEXT_RELEASE_CAUSE=14 THEN ''RadioNetwork_unknown-mme-ue-s1ap-id''
					WHEN A.UE_CONTEXT_RELEASE_CAUSE=15 THEN ''RadioNetwork_unknown-enb-ue-s1ap-id''
					WHEN A.UE_CONTEXT_RELEASE_CAUSE=16 THEN ''RadioNetwork_unknown-pair-ue-s1ap-id''
					WHEN A.UE_CONTEXT_RELEASE_CAUSE=17 THEN ''RadioNetwork_handover-desirable-FOR-radio-reason''
					WHEN A.UE_CONTEXT_RELEASE_CAUSE=18 THEN ''RadioNetwork_time-critical-handover''
					WHEN A.UE_CONTEXT_RELEASE_CAUSE=19 THEN ''RadioNetwork_resource-optimisation-handover''
					WHEN A.UE_CONTEXT_RELEASE_CAUSE=20 THEN ''RadioNetwork_reduce-LOAD-IN-serving-cell''
					WHEN A.UE_CONTEXT_RELEASE_CAUSE=21 THEN ''RadioNetwork_user-inactivity''
					WHEN A.UE_CONTEXT_RELEASE_CAUSE=22 THEN ''RadioNetwork_radio-CONNECTION-WITH-ue-lost''
					WHEN A.UE_CONTEXT_RELEASE_CAUSE=23 THEN ''RadioNetwork_load-balancing-tau-required''
					WHEN A.UE_CONTEXT_RELEASE_CAUSE=24 THEN ''RadioNetwork_cs-fallback-triggered''
					WHEN A.UE_CONTEXT_RELEASE_CAUSE=25 THEN ''RadioNetwork_ue-NOT-available-FOR-ps-service''
					WHEN A.UE_CONTEXT_RELEASE_CAUSE=26 THEN ''RadioNetwork_radio-resources-NOT-available''
					WHEN A.UE_CONTEXT_RELEASE_CAUSE=27 THEN ''RadioNetwork_failure-IN-radio-interface-PROCEDURE''
					WHEN A.UE_CONTEXT_RELEASE_CAUSE=28 THEN ''RadioNetwork_invalid-qos-combination''
					WHEN A.UE_CONTEXT_RELEASE_CAUSE=29 THEN ''RadioNetwork_interrat-redirection''
					WHEN A.UE_CONTEXT_RELEASE_CAUSE=30 THEN ''RadioNetwork_interaction-WITH-other-PROCEDURE''
					WHEN A.UE_CONTEXT_RELEASE_CAUSE=31 THEN ''RadioNetwork_unknown-E-RAB-ID''
					WHEN A.UE_CONTEXT_RELEASE_CAUSE=32 THEN ''RadioNetwork_multiple-E-RAB-ID-instances''
					WHEN A.UE_CONTEXT_RELEASE_CAUSE=33 THEN ''RadioNetwork_encryption-AND-OR-integrity-protection-algorithms-NOT-supported''
					WHEN A.UE_CONTEXT_RELEASE_CAUSE=34 THEN ''RadioNetwork_s1-intra-system-handover-triggered''
					WHEN A.UE_CONTEXT_RELEASE_CAUSE=35 THEN ''RadioNetwork_s1-inter-system-handover-triggered''
					WHEN A.UE_CONTEXT_RELEASE_CAUSE=36 THEN ''RadioNetwork_x2-handover-triggered''
					WHEN A.UE_CONTEXT_RELEASE_CAUSE=37 THEN ''RadioNetwork_redirection-towards-1xRTT''
					WHEN A.UE_CONTEXT_RELEASE_CAUSE=38 THEN ''RadioNetwork_not-supported-QCI-VALUE''
					WHEN A.UE_CONTEXT_RELEASE_CAUSE=39 THEN ''RadioNetwork_invalid-CSG-Id''
					WHEN A.UE_CONTEXT_RELEASE_CAUSE=40 THEN ''RadioNetwork_Ellipsis''
					WHEN A.UE_CONTEXT_RELEASE_CAUSE=41 THEN ''Transport_transport-resource-unavailable''
					WHEN A.UE_CONTEXT_RELEASE_CAUSE=42 THEN ''Transport_unspecified''
					WHEN A.UE_CONTEXT_RELEASE_CAUSE=43 THEN ''Transport_Others''
					WHEN A.UE_CONTEXT_RELEASE_CAUSE=50 THEN ''Nas_normal-RELEASE''
					WHEN A.UE_CONTEXT_RELEASE_CAUSE=51 THEN ''Nas_authentication-failure''
					WHEN A.UE_CONTEXT_RELEASE_CAUSE=52 THEN ''Nas_detach''
					WHEN A.UE_CONTEXT_RELEASE_CAUSE=53 THEN ''Nas_unspecified''
					WHEN A.UE_CONTEXT_RELEASE_CAUSE=54 THEN ''Nas_csg-subscription-expiry''
					WHEN A.UE_CONTEXT_RELEASE_CAUSE=60 THEN ''Misc_control-processing-overload''
					WHEN A.UE_CONTEXT_RELEASE_CAUSE=61 THEN ''Misc_not-enough-USER-plane-processing-resources''
					WHEN A.UE_CONTEXT_RELEASE_CAUSE=62 THEN ''Misc_hardware-failure''
					WHEN A.UE_CONTEXT_RELEASE_CAUSE=63 THEN ''Misc_om-intervention''
					WHEN A.UE_CONTEXT_RELEASE_CAUSE=64 THEN ''Misc_unspecified''
					WHEN A.UE_CONTEXT_RELEASE_CAUSE=65 THEN ''Misc_unknown-PLMN''
					WHEN A.UE_CONTEXT_RELEASE_CAUSE=70 THEN ''Protocol_transfer-syntax-error''
					WHEN A.UE_CONTEXT_RELEASE_CAUSE=71 THEN ''Protocol_abstract-syntax-error-reject''
					WHEN A.UE_CONTEXT_RELEASE_CAUSE=72 THEN ''Protocol_abstract-syntax-error-IGNORE-AND-notify''
					WHEN A.UE_CONTEXT_RELEASE_CAUSE=73 THEN ''Protocol_message-NOT-compatible-WITH-receiver-state''
					WHEN A.UE_CONTEXT_RELEASE_CAUSE=74 THEN ''Protocol_semantic-error''
					WHEN A.UE_CONTEXT_RELEASE_CAUSE=75 THEN ''Protocol_abstract-syntax-error-falsely-constructed-message''
					WHEN A.UE_CONTEXT_RELEASE_CAUSE=76 THEN ''Protocol_unspecified''
					WHEN A.UE_CONTEXT_RELEASE_CAUSE=80 THEN ''CellAccessMode_hybrid''
					ELSE ''N/A'' END
					) AS `RELEASE_CAUSE`,
				CONCAT(A.`MCC`,''-'',A.`MNC`,''-'',A.`POS_FIRST_S_ENODEB`,''-'',A.`POS_FIRST_S_CELL`) AS START_CELL,
				A.POS_FIRST_LOC AS POS_FIRST_LOC,
				A.`POS_FIRST_S_RSRP` AS START_RXLEV_RSCP_RSRP_dBn,
				A.`POS_FIRST_S_RSRQ` AS START_RXQUAL_ECN0_RSRQ_dB,
				CONCAT(A.`MCC`,''-'',A.`MNC`,''-'',A.`POS_LAST_S_ENODEB`,''-'',A.`POS_LAST_S_CELL`) AS END_CELL,
				A.POS_LAST_LOC AS POS_LAST_LOC,
				A.`POS_LAST_S_RSRP` AS END_RXLEV_RSCP_RSRP_dBn,
				A.`POS_LAST_S_RSRQ` AS END_RXQUAL_ECN0_RSRQ_dB,
				A.`DL_VOLUME` AS `DL_TRAFFIC_VOLUME_MB`, 
				A.`DL_THROUPUT` AS `DL_THROUGHPUT_Kbps`,
				A.`DL_THROUPUT_MAX` AS `DL_THROUGHPUT_MAX_Kbps`,
				A.`UL_VOLUME` AS `UL_TRAFFIC_VOLUME_MB`, 
				A.`UL_THROUPUT` AS `UL_THROUGHPUT_Kbps`,
				A.`UL_THROUPUT_MAX` AS `UL_THROUGHPUT_MAX_Kbps`,
				(IFNULL(A.`X2_HO_INTRA_ATTEMPT`,0) + IFNULL(A.`S1_HO_INTRA_ATTEMPT`,0)) AS `INTRA_FREQ_HO_ATTEMPT`,
				(IFNULL(A.`X2_HO_INTRA_FAILURE`,0) + IFNULL(A.`S1_HO_INTRA_FAILURE`,0)) AS `INTRA_FREQ_HO_FAILURE`,
				(IFNULL(A.`X2_HO_INTER_ATTEMPT`,0) + IFNULL(A.`S1_HO_INTER_ATTEMPT`,0)) AS `INTER_FREQ_HO_ATTEMPT`,
				(IFNULL(A.`X2_HO_INTER_FAILURE`,0) + IFNULL(A.`S1_HO_INTER_FAILURE`,0)) AS `INTER_FREQ_HO_FAILURE`,
				(IFNULL(A.`IRAT_TO_UMTS_ATTEMPT`,0) + IFNULL(A.`IRAT_TO_GERAN_ATTEMPT`,0)) AS `IRAT_HO_ATTEMPT`,
				(IFNULL(A.`IRAT_TO_UMTS_FAILURE`,0) + IFNULL(A.`IRAT_TO_GERAN_FAILURE`,0)) AS `IRAT_HO_FAILURE`,
				(CASE WHEN A.`INDOOR`=1 THEN ''Indoor'' WHEN A.`INDOOR`=0 AND A.`MOVING`=0 THEN ''Stationary'' 
				WHEN A.`INDOOR`=0 AND A.`MOVING`=1 THEN ''Moving'' WHEN A.`INDOOR`=0 AND A.`MOVING` IS NULL THEN ''Outdoor'' ELSE ''N/A'' END) AS `INDOOR`,
				A.`MOVING` AS MOVING,
				(CASE WHEN A.`MOVING_TYPE`=1 THEN ''Walking'' WHEN A.`MOVING_TYPE`=2 THEN ''In Vehicle'' ELSE ''N/A'' END)AS `MOVING_TYPE`,
				NULL AS B_PARTY_NUMBER ,
				A.DATA_DATE AS DATA_DATE,
				A.`DATA_HOUR` AS DATA_HOUR,
				NULL AS `BATCH`,
				NULL AS `CELL_UPDATE_CAUSE`,		
				NULL AS RAB_SEQ_ID,
				DATA_DATE AS ds_date,
				CONCAT(A.POS_FIRST_S_CELL,''@'',A.`POS_FIRST_S_ENODEB`) AS POS_FIRST_CELL,
				CONCAT(A.POS_LAST_S_CELL,''@'',A.`POS_LAST_S_ENODEB`) AS POS_LAST_CELL,
				CONCAT(A.START_CELL_ID,''@'',A.`START_ENODEB_ID`) AS START_CELL_ID,
				CONCAT(A.END_CELL_ID,''@'',A.`END_ENODEB_ID`) AS END_CELL_ID,
				NULL AS `MANUFACTURER`,
				NULL AS `MODEL`,
				',PU_ID,' AS PU');
				
	SET STR_IMSI_VOLTE_LTE=CONCAT('A.`CALL_ID` AS `CALL_ID`,
				IF(B.ERAB_STATUS IS NULL,A.`START_TIME`,B.`ERAB_START_TIME`) AS `START_TIME`,
				IF(B.ERAB_STATUS IS NULL,A.`END_TIME`,B.`ERAB_END_TIME`) AS `END_TIME`,
				ROUND((IF(B.ERAB_STATUS IS NULL,A.`DURATION`,B.`DURATION`)/1000),2) AS `DURATION`,
				 NULL AS `CS_TRAFFIC_TIME`,
				A.`CALL_SETUP_TIME` AS `CALL_SETUP_TIME`,
				A.`IMSI` AS `IMSI`,
				A.`IMEI` AS `IMEI`,
				A.`IMSI` AS `MSISDN`,
				A.`MAKE_ID`,
				A.`MODEL_ID`,
				4 AS `TECH_MASK`,
				(CASE A.CALL_TYPE WHEN 21 THEN ''LTE-TRAFFIC'' WHEN 22 THEN ''LTE-SIGNALING'' WHEN 23 THEN ''LTE-VoLTE'' WHEN 24 THEN ''LTE-SMS'' WHEN 29 THEN ''LTE-Unspecified'' ELSE ''Unkonwn'' END) AS `CALL_TYPE`,
				A.`APN`, 
				IF(B.ERAB_STATUS IS NULL,
				(CASE A.CALL_STATUS WHEN 1 THEN ''Normal'' WHEN 2 THEN ''Drop'' WHEN 3 THEN ''Block'' WHEN 5 THEN ''CS-Fallback'' WHEN 6 THEN ''SetupFailure'' ELSE ''Unspecified'' END),
				(CASE B.ERAB_STATUS WHEN 1 THEN ''Normal'' WHEN 2 THEN ''Drop'' WHEN 3 THEN ''Block'' WHEN 5 THEN ''CS-Fallback'' WHEN 6 THEN ''SetupFailure'' ELSE ''Unspecified'' END)) AS `CALL_STATUS`,
				(CASE  A.UE_CONTEXT_RELEASE_CAUSE
				WHEN 1 THEN ''RadioNetwork_unspecified''
				WHEN 2 THEN ''RadioNetwork_tx2relocoverall-expiry''
				WHEN 3 THEN ''RadioNetwork_successful-handover''
				WHEN 4 THEN ''RadioNetwork_release-due-TO-eutran-generated-reason''
				WHEN 5 THEN ''RadioNetwork_handover-cancelled''
				WHEN 6 THEN ''RadioNetwork_partial-handover''
				WHEN 7 THEN ''RadioNetwork_ho-failure-IN-target-EPC-eNB-OR-target-system''
				WHEN 8 THEN ''RadioNetwork_ho-target-NOT-allowed''
				WHEN 9 THEN ''RadioNetwork_tS1relocoverall-expiry''
				WHEN 10 THEN ''RadioNetwork_tS1relocprep-expiry''
				WHEN 11 THEN ''RadioNetwork_cell-NOT-available''
				WHEN 12 THEN ''RadioNetwork_unknown-targetID''
				WHEN 13 THEN ''RadioNetwork_no-radio-resources-available-IN-target-cell''
				WHEN 14 THEN ''RadioNetwork_unknown-mme-ue-s1ap-id''
				WHEN 15 THEN ''RadioNetwork_unknown-enb-ue-s1ap-id''
				WHEN 16 THEN ''RadioNetwork_unknown-pair-ue-s1ap-id''
				WHEN 17 THEN ''RadioNetwork_handover-desirable-FOR-radio-reason''
				WHEN 18 THEN ''RadioNetwork_time-critical-handover''
				WHEN 19 THEN ''RadioNetwork_resource-optimisation-handover''
				WHEN 20 THEN ''RadioNetwork_reduce-LOAD-IN-serving-cell''
				WHEN 21 THEN ''RadioNetwork_user-inactivity''
				WHEN 22 THEN ''RadioNetwork_radio-CONNECTION-WITH-ue-lost''
				WHEN 23 THEN ''RadioNetwork_load-balancing-tau-required''
				WHEN 24 THEN ''RadioNetwork_cs-fallback-triggered''
				WHEN 25 THEN ''RadioNetwork_ue-NOT-available-FOR-ps-service''
				WHEN 26 THEN ''RadioNetwork_radio-resources-NOT-available''
				WHEN 27 THEN ''RadioNetwork_failure-IN-radio-interface-PROCEDURE''
				WHEN 28 THEN ''RadioNetwork_invalid-qos-combination''
				WHEN 29 THEN ''RadioNetwork_interrat-redirection''
				WHEN 30 THEN ''RadioNetwork_interaction-WITH-other-PROCEDURE''
				WHEN 31 THEN ''RadioNetwork_unknown-E-RAB-ID''
				WHEN 32 THEN ''RadioNetwork_multiple-E-RAB-ID-instances''
				WHEN 33 THEN ''RadioNetwork_encryption-AND-OR-integrity-protection-algorithms-NOT-supported''
				WHEN 34 THEN ''RadioNetwork_s1-intra-system-handover-triggered''
				WHEN 35 THEN ''RadioNetwork_s1-inter-system-handover-triggered''
				WHEN 36 THEN ''RadioNetwork_x2-handover-triggered''
				WHEN 37 THEN ''RadioNetwork_redirection-towards-1xRTT''
				WHEN 38 THEN ''RadioNetwork_not-supported-QCI-VALUE''
				WHEN 39 THEN ''RadioNetwork_invalid-CSG-Id''
				WHEN 40 THEN ''RadioNetwork_Ellipsis''
				WHEN 41 THEN ''Transport_transport-resource-unavailable''
				WHEN 42 THEN ''Transport_unspecified''
				WHEN 43 THEN ''Transport_Others''
				WHEN 50 THEN ''Nas_normal-RELEASE''
				WHEN 51 THEN ''Nas_authentication-failure''
				WHEN 52 THEN ''Nas_detach''
				WHEN 53 THEN ''Nas_unspecified''
				WHEN 54 THEN ''Nas_csg-subscription-expiry''
				WHEN 60 THEN ''Misc_control-processing-overload''
				WHEN 61 THEN ''Misc_not-enough-USER-plane-processing-resources''
				WHEN 62 THEN ''Misc_hardware-failure''
				WHEN 63 THEN ''Misc_om-intervention''
				WHEN 64 THEN ''Misc_unspecified''
				WHEN 65 THEN ''Misc_unknown-PLMN''
				WHEN 70 THEN ''Protocol_transfer-syntax-error''
				WHEN 71 THEN ''Protocol_abstract-syntax-error-reject''
				WHEN 72 THEN ''Protocol_abstract-syntax-error-IGNORE-AND-notify''
				WHEN 73 THEN ''Protocol_message-NOT-compatible-WITH-receiver-state''
				WHEN 74 THEN ''Protocol_semantic-error''
				WHEN 75 THEN ''Protocol_abstract-syntax-error-falsely-constructed-message''
				WHEN 76 THEN ''Protocol_unspecified''
				WHEN 80 THEN ''CellAccessMode_hybrid''
				ELSE ''N/A'' END
				) AS `RELEASE_CAUSE`,
				IF(B.ERAB_STATUS IS NULL,CONCAT(A.`MCC`,''-'',A.`MNC`,''-'',A.`POS_FIRST_S_ENODEB`,''-'',A.`POS_FIRST_S_CELL`),CONCAT(A.`MCC`,''-'',A.`MNC`,''-'',B.`ERAB_START_SERVING_ENODEB`,''-'',B.`ERAB_START_SERVING_CELL`)) AS START_CELL,
				IF(B.ERAB_STATUS IS NULL,A.POS_FIRST_LOC,B.`ERAB_START_LOC`) AS POS_FIRST_LOC,
				IF(B.ERAB_STATUS IS NULL,A.`POS_FIRST_S_RSRP`,B.`ERAB_START_SERVING_RSRP`) AS START_RXLEV_RSCP_RSRP_dBn,
				IF(B.ERAB_STATUS IS NULL,A.`POS_FIRST_S_RSRQ`,B.`ERAB_START_SERVING_RSRQ`) AS START_RXQUAL_ECN0_RSRQ_dB,
				IF(B.ERAB_STATUS IS NULL,CONCAT(A.`MCC`,''-'',A.`MNC`,''-'',A.`POS_LAST_S_ENODEB`,''-'',A.`POS_LAST_S_CELL`),CONCAT(A.`MCC`,''-'',A.`MNC`,''-'',B.`ERAB_END_SERVING_ENODEB`,''-'',B.`ERAB_END_SERVING_CELL`)) AS END_CELL,
				IF(B.ERAB_STATUS IS NULL,A.POS_LAST_LOC,B.`ERAB_END_LOC`) AS POS_LAST_LOC,
				IF(B.ERAB_STATUS IS NULL,A.`POS_LAST_S_RSRP`,B.`ERAB_END_SERVING_RSRP`) AS END_RXLEV_RSCP_RSRP_dBn,
				IF(B.ERAB_STATUS IS NULL,A.`POS_LAST_S_RSRQ`,B.`ERAB_END_SERVING_RSRQ`) AS END_RXQUAL_ECN0_RSRQ_dB,
				A.`DL_VOLUME` AS `DL_TRAFFIC_VOLUME_MB`, 
				A.`DL_THROUPUT` AS `DL_THROUGHPUT_Kbps`,
				A.`DL_THROUPUT_MAX` AS `DL_THROUGHPUT_MAX_Kbps`,
				A.`UL_VOLUME` AS `UL_TRAFFIC_VOLUME_MB`, 
				A.`UL_THROUPUT` AS `UL_THROUGHPUT_Kbps`,
				A.`UL_THROUPUT_MAX` AS `UL_THROUGHPUT_MAX_Kbps`,
				(IFNULL(A.`X2_HO_INTRA_ATTEMPT`,0) + IFNULL(A.`S1_HO_INTRA_ATTEMPT`,0)) AS `INTRA_FREQ_HO_ATTEMPT`,
				(IFNULL(A.`X2_HO_INTRA_FAILURE`,0) + IFNULL(A.`S1_HO_INTRA_FAILURE`,0)) AS `INTRA_FREQ_HO_FAILURE`,
				(IFNULL(A.`X2_HO_INTER_ATTEMPT`,0) + IFNULL(A.`S1_HO_INTER_ATTEMPT`,0)) AS `INTER_FREQ_HO_ATTEMPT`,
				(IFNULL(A.`X2_HO_INTER_FAILURE`,0) + IFNULL(A.`S1_HO_INTER_FAILURE`,0)) AS `INTER_FREQ_HO_FAILURE`,
				(IFNULL(A.`IRAT_TO_UMTS_ATTEMPT`,0) + IFNULL(A.`IRAT_TO_GERAN_ATTEMPT`,0)) AS `IRAT_HO_ATTEMPT`,
				(IFNULL(A.`IRAT_TO_UMTS_FAILURE`,0) + IFNULL(A.`IRAT_TO_GERAN_FAILURE`,0)) AS `IRAT_HO_FAILURE`,
				(CASE WHEN A.`INDOOR`=1 THEN ''Indoor'' WHEN A.`INDOOR`=0 AND A.`MOVING`=0 THEN ''Stationary'' 
				WHEN A.`INDOOR`=0 AND A.`MOVING`=1 THEN ''Moving'' WHEN A.`INDOOR`=0 AND A.`MOVING` IS NULL THEN ''Outdoor'' ELSE ''N/A'' END) AS `INDOOR`,
				A.`MOVING` AS MOVING,
				(CASE A.`MOVING_TYPE` WHEN 1 THEN ''Walking'' WHEN 2 THEN ''In Vehicle'' ELSE ''N/A'' END)AS `MOVING_TYPE`,
				NULL AS B_PARTY_NUMBER ,
				A.DATA_DATE AS DATA_DATE,
				A.DATA_HOUR AS DATA_HOUR,
				NULL AS `BATCH`,
				NULL AS `CELL_UPDATE_CAUSE`,        
				NULL AS RAB_SEQ_ID,
				A.DATA_DATE AS ds_date,
				IF(B.ERAB_STATUS IS NULL,CONCAT(A.POS_FIRST_S_CELL,''@'',A.`POS_FIRST_S_ENODEB`),CONCAT(B.`ERAB_START_SERVING_CELL`,''@'',B.`ERAB_START_SERVING_ENODEB`)) AS POS_FIRST_CELL,
				IF(B.ERAB_STATUS IS NULL,CONCAT(A.POS_LAST_S_CELL,''@'',A.`POS_LAST_S_ENODEB`),CONCAT(B.`ERAB_END_SERVING_CELL`,''@'',B.`ERAB_END_SERVING_ENODEB`)) AS POS_LAST_CELL,
				CONCAT(A.START_CELL_ID,''@'',A.`START_ENODEB_ID`) AS START_CELL_ID,
				CONCAT(A.END_CELL_ID,''@'',A.`END_ENODEB_ID`) AS END_CELL_ID,
				NULL AS `MANUFACTURER`,
				NULL AS `MODEL`,
				',PU_ID,' AS PU');
				
	SET STR_IMSI_GRP_VOLTE_LTE=CONCAT('A.IMSI AS `IMSI`
				,A.DATA_DATE AS `DATA_DATE`				
				,(CONCAT(IFNULL(A.MAKE_ID,'' ''),''|'',IFNULL(A.MODEL_ID,'' ''),''|'',IFNULL(A.IMEI,'' ''))) AS `HANDSET`
				,A.IMSI AS `MSISDN`
				,SUM(1) AS `Total_Call_Count`
				,SUM(IF(B.ERAB_STATUS IS NULL,IF(A.CALL_STATUS=2,1,0),IF(B.ERAB_STATUS=2,1,0))) AS `Drop_Call_Count`
				,SUM(IF(B.ERAB_STATUS IS NULL,IF(A.CALL_STATUS=3,1,0),IF(B.ERAB_STATUS=3,1,0))) AS `Block_Call_Count`
				,SUM(A.DL_VOLUME) AS `Total_DL_Data_Volume` 
				,SUM(A.UL_VOLUME) AS `Total_UL_Data_Volume`					
				,MAX(A.DL_THROUPUT_MAX) AS `MAX_DL_THROUGHPUT`				
				,MAX(A.UL_THROUPUT_MAX) AS `MAX_UL_THROUGHPUT`
				,NULL AS `UMTS_Call_Drop_Rate_1`
				,NULL AS `UMTS_Call_Drop_Rate_2`
				,NULL AS `UMTS_CSSR_1`
				,NULL AS `UMTS_CSSR_2`
				,NULL AS `UMTS_Blocked_Call_Rate_1`
				,NULL AS `UMTS_Blocked_Call_Rate_2`
				,NULL AS `UMTS_Soft_Handover_SR_1`
				,NULL AS `UMTS_Soft_Handover_SR_2`
				,NULL AS `UMTS_Softer_handover_SR_1`
				,NULL AS `UMTS_Softer_handover_SR_2`
				,NULL AS `UMTS_Inter_Freq_Handover_SR_1`
				,NULL AS `UMTS_Inter_Freq_Handover_SR_2`
				,NULL AS `UMTS_IRAT_handover_SR_1`
				,NULL AS `UMTS_IRAT_handover_SR_2`
				,IF(B.ERAB_STATUS IS NULL,IF(A.CALL_STATUS=2,1,0),IF(B.ERAB_STATUS=2,1,0)) AS `LTE_Call_Drop_Rate_1`
				,IF(B.ERAB_STATUS IS NULL,IF(A.CALL_STATUS IN (1,2,4),1,0),IF(B.ERAB_STATUS IN (1,2,4),1,0)) AS `LTE_Call_Drop_Rate_2`
				,IF(B.ERAB_STATUS IS NULL,IF(A.CALL_STATUS IN (3,6),1,0),IF(B.ERAB_STATUS IN (3,6),1,0)) AS `LTE_CSSR_1`
				,SUM(1) AS `LTE_CSSR_2`
				,IF(B.ERAB_STATUS IS NULL,IF(A.CALL_STATUS=3,1,0),IF(B.ERAB_STATUS=3,1,0)) AS `LTE_Blocked_Call_Rate_1`
				,SUM(1) AS `LTE_Blocked_Call_Rate_2`	
				,SUM(IFNULL(A.X2_HO_INTER_FAILURE,0))+(IFNULL(A.S1_HO_INTER_FAILURE,0)) AS `LTE_Inter_Feq_Handover_SR_1`
				,SUM(IFNULL(A.X2_HO_INTER_ATTEMPT,0))+(IFNULL(A.S1_HO_INTER_ATTEMPT,0)) AS `LTE_Inter_Feq_Handover_SR_2`
				,SUM(IFNULL(A.X2_HO_INTRA_FAILURE,0))+(IFNULL(A.S1_HO_INTRA_FAILURE,0)) AS `LTE_Intra_Feq_Handover_SR_1`
				,SUM(IFNULL(A.X2_HO_INTRA_ATTEMPT,0))+(IFNULL(A.S1_HO_INTRA_ATTEMPT,0)) AS `LTE_Intra_Feq_Handover_SR_2`
				,SUM(IFNULL(A.IRAT_TO_UMTS_FAILURE,0)) AS `LTE_IRAT_Handover_SR_3G_1`
				,SUM(IFNULL(A.IRAT_TO_UMTS_ATTEMPT,0)) AS `LTE_IRAT_Handover_SR_3G_2`
				,SUM(IFNULL(A.IRAT_TO_GERAN_FAILURE,0)) AS `LTE_IRAT_Handover_SR_2G_1`
				,SUM(IFNULL(A.IRAT_TO_GERAN_ATTEMPT,0)) AS `LTE_IRAT_Handover_SR_2G_2`
				,NULL AS `GSM_Call_Drop_Rate_1`
				,NULL AS `GSM_Call_Drop_Rate_2`
				,NULL AS `GSM_CSSR_1`
				,NULL AS `GSM_CSSR_2`
				,NULL AS `GSM_Blocked_Call_Rate_1`
				,NULL AS `GSM_Blocked_Call_Rate_2`
				,NULL AS `GSM_Inter_Freq_Handover_SR_1`
				,NULL AS `GSM_Inter_Freq_Handover_SR_2`
				,',PU_ID,' AS PU
				,A.DATA_DATE AS ds_date
				,4 AS `TECH_MASK`
				,MAX(IF(B.ERAB_STATUS IS NULL,A.POS_FIRST_LOC,B.`ERAB_START_LOC`)) AS POS_FIRST_LOC
				,MAX(IF(B.ERAB_STATUS IS NULL,A.POS_LAST_LOC,B.`ERAB_END_LOC`)) AS POS_LAST_LOC
				,NULL AS POS_FIRST_RSCP_SUM
				,NULL AS POS_FIRST_RSCP_CNT
				,NULL AS POS_FIRST_ECN0_SUM
				,NULL AS POS_FIRST_ECN0_CNT
				,NULL AS POS_LAST_RSCP_SUM
				,NULL AS POS_LAST_RSCP_CNT
				,NULL AS POS_LAST_ECN0_SUM
				,NULL AS POS_LAST_ECN0_CNT
				,IF(B.ERAB_STATUS IS NULL,A.POS_FIRST_S_RSRP,B.`ERAB_START_SERVING_RSRP`) AS POS_FIRST_S_RSRP_SUM
				,IF(B.ERAB_STATUS IS NULL AND A.POS_FIRST_S_RSRP IS NULL,0,1) AS POS_FIRST_S_RSRP_CNT
				,IF(B.ERAB_STATUS IS NULL,A.POS_FIRST_S_RSRQ,B.`ERAB_START_SERVING_RSRQ`) AS POS_FIRST_S_RSRQ_SUM
				,IF(B.ERAB_STATUS IS NULL AND A.POS_FIRST_S_RSRQ IS NULL,0,1) AS POS_FIRST_S_RSRQ_CNT
				,IF(B.ERAB_STATUS IS NULL,A.POS_LAST_S_RSRP,B.`ERAB_END_SERVING_RSRP`) AS POS_LAST_S_RSRP_SUM
				,IF(B.ERAB_STATUS IS NULL AND A.POS_LAST_S_RSRP IS NULL,0,1) AS POS_LAST_S_RSRP_CNT
				,IF(B.ERAB_STATUS IS NULL,A.POS_LAST_S_RSRQ,B.`ERAB_END_SERVING_RSRQ`) AS POS_LAST_S_RSRQ_SUM
				,IF(B.ERAB_STATUS IS NULL AND A.POS_LAST_S_RSRQ IS NULL,0,1) AS POS_LAST_S_RSRQ_CNT');
				
	SET STR_IMSI_GRP_LTE=CONCAT('A.IMSI AS `IMSI`
				,A.DATA_DATE AS `DATA_DATE`
				,(CONCAT(IFNULL(A.MAKE_ID,'' ''),''|'',IFNULL(A.MODEL_ID,'' ''),''|'',IFNULL(A.IMEI,'' ''))) AS `HANDSET`
				,A.IMSI AS `MSISDN`
				,SUM(1) AS `Total_Call_Count`
				,SUM(IF(A.CALL_STATUS=2,1,0)) AS `Drop_Call_Count`
				,SUM(IF(A.CALL_STATUS=3,1,0)) AS `Block_Call_Count`
				,SUM(A.DL_VOLUME) AS `Total_DL_Data_Volume`
				,SUM(A.UL_VOLUME) AS `Total_UL_Data_Volume`
				,SUM(A.DL_THROUPUT_MAX) AS `MAX_DL_THROUGHPUT`
				,SUM(A.UL_THROUPUT_MAX) AS `MAX_UL_THROUGHPUT`
				,NULL AS `UMTS_Call_Drop_Rate_1`
				,NULL AS `UMTS_Call_Drop_Rate_2`
				,NULL AS `UMTS_CSSR_1`
				,NULL AS `UMTS_CSSR_2`
				,NULL AS `UMTS_Blocked_Call_Rate_1`
				,NULL AS `UMTS_Blocked_Call_Rate_2`
				,NULL AS `UMTS_Soft_Handover_SR_1`
				,NULL AS `UMTS_Soft_Handover_SR_2`
				,NULL AS `UMTS_Softer_handover_SR_1`
				,NULL AS `UMTS_Softer_handover_SR_2`
				,NULL AS `UMTS_Inter_Freq_Handover_SR_1`
				,NULL AS `UMTS_Inter_Freq_Handover_SR_2`
				,NULL AS `UMTS_IRAT_handover_SR_1`
				,NULL AS `UMTS_IRAT_handover_SR_2`
				,SUM(IF(A.call_status=2,1,0)) AS `LTE_Call_Drop_Rate_1`
				,SUM(IF(A.call_status IN (1,2,4),1,0)) AS `LTE_Call_Drop_Rate_2`
				,SUM(IF(A.call_status IN (3,6),1,0)) AS `LTE_CSSR_1`
				,SUM(1) AS `LTE_CSSR_2`
				,SUM(IF(A.call_status=3,1,0)) AS `LTE_Blocked_Call_Rate_1` 
				,SUM(1) AS `LTE_Blocked_Call_Rate_2`
				,SUM((IFNULL(X2_HO_INTER_FAILURE,0))+(IFNULL(A.S1_HO_INTER_FAILURE,0))) AS `LTE_Inter_Feq_Handover_SR_1`
				,SUM((IFNULL(X2_HO_INTER_ATTEMPT,0))+(IFNULL(A.S1_HO_INTER_ATTEMPT,0))) AS `LTE_Inter_Feq_Handover_SR_2`
				,SUM((IFNULL(X2_HO_INTRA_FAILURE,0))+(IFNULL(A.S1_HO_INTRA_FAILURE,0))) AS `LTE_Intra_Feq_Handover_SR_1`
				,SUM((IFNULL(X2_HO_INTRA_ATTEMPT,0))+(IFNULL(A.S1_HO_INTRA_ATTEMPT,0))) AS `LTE_Intra_Feq_Handover_SR_2`
				,SUM(IFNULL(IRAT_TO_UMTS_FAILURE,0)) AS `LTE_IRAT_Handover_SR_3G_1`
				,SUM(IFNULL(IRAT_TO_UMTS_ATTEMPT,0)) AS `LTE_IRAT_Handover_SR_3G_2`
				,SUM(IFNULL(IRAT_TO_GERAN_FAILURE,0)) AS `LTE_IRAT_Handover_SR_2G_1`
				,SUM(IFNULL(IRAT_TO_GERAN_ATTEMPT,0)) AS `LTE_IRAT_Handover_SR_2G_2`
				,NULL AS `GSM_Call_Drop_Rate_1`
				,NULL AS `GSM_Call_Drop_Rate_2`
				,NULL AS `GSM_CSSR_1`
				,NULL AS `GSM_CSSR_2`
				,NULL AS `GSM_Blocked_Call_Rate_1`
				,NULL AS `GSM_Blocked_Call_Rate_2`
				,NULL AS `GSM_Inter_Freq_Handover_SR_1`
				,NULL AS `GSM_Inter_Freq_Handover_SR_2`
				,',PU_ID,' AS PU
				,DATA_DATE AS ds_date
				,4 AS `TECH_MASK`
				,MAX(A.POS_FIRST_LOC) AS POS_FIRST_LOC
				,MAX(A.POS_LAST_LOC) AS POS_LAST_LOC
				,NULL AS POS_FIRST_RSCP_SUM
				,NULL AS POS_FIRST_RSCP_CNT
				,NULL AS POS_FIRST_ECN0_SUM
				,NULL AS POS_FIRST_ECN0_CNT
				,NULL AS POS_LAST_RSCP_SUM
				,NULL AS POS_LAST_RSCP_CNT
				,NULL AS POS_LAST_ECN0_SUM
				,NULL AS POS_LAST_ECN0_CNT
				,SUM(POS_FIRST_S_RSRP) AS POS_FIRST_S_RSRP_SUM
				,SUM(IF(POS_FIRST_S_RSRP IS NULL,0,1)) AS POS_FIRST_S_RSRP_CNT
				,SUM(POS_FIRST_S_RSRQ) AS POS_FIRST_S_RSRQ_SUM
				,SUM(IF(POS_FIRST_S_RSRQ IS NULL,0,1)) AS POS_FIRST_S_RSRQ_CNT
				,SUM(POS_LAST_S_RSRP) AS POS_LAST_S_RSRP_SUM
				,SUM(IF(POS_LAST_S_RSRP IS NULL,0,1)) AS POS_LAST_S_RSRP_CNT
				,SUM(POS_LAST_S_RSRQ) AS POS_LAST_S_RSRQ_SUM
				,SUM(IF(POS_LAST_S_RSRQ IS NULL,0,1)) AS POS_LAST_S_RSRQ_CNT ');	
				
	SET STR_IMSI_AGG_RPT=CONCAT('A.IMSI AS `IMSI`
				,A.DATA_DATE AS `DATA_DATE`
				,(CONCAT(IFNULL(A.MAKE_ID,'' ''),''|'',IFNULL(A.MODEL_ID,'' ''),''|'',IFNULL(A.IMEI,'' ''))) AS `HANDSET`
				,A.IMSI AS `MSISDN`
				,A.Total_Call_Count AS `Total_Call_Count`
				,A.Drop_Call_Count AS `Drop_Call_Count`
				,A.Block_Call_Count AS `Block_Call_Count`
				,A.Total_DL_Data_Volume AS `Total_DL_Data_Volume`
				,A.Total_UL_Data_Volume AS `Total_UL_Data_Volume`
				,A.MAX_DL_THROUGHPUT AS `MAX_DL_THROUGHPUT`
				,A.MAX_UL_THROUGHPUT AS `MAX_UL_THROUGHPUT`
				,A.UMTS_Call_Drop_Rate_1 AS `UMTS_Call_Drop_Rate_1`
				,A.UMTS_Call_Drop_Rate_2 AS `UMTS_Call_Drop_Rate_2`
				,A.UMTS_CSSR_1 AS `UMTS_CSSR_1`
				,A.UMTS_CSSR_2 AS `UMTS_CSSR_2`
				,A.UMTS_Blocked_Call_Rate_1 AS `UMTS_Blocked_Call_Rate_1`
				,A.UMTS_Blocked_Call_Rate_2 AS `UMTS_Blocked_Call_Rate_2`
				,A.UMTS_Soft_Handover_SR_1 AS `UMTS_Soft_Handover_SR_1`
				,A.UMTS_Soft_Handover_SR_2 AS `UMTS_Soft_Handover_SR_2`
				,A.UMTS_Softer_handover_SR_1 AS `UMTS_Softer_handover_SR_1`
				,A.UMTS_Softer_handover_SR_2 AS `UMTS_Softer_handover_SR_2`
				,A.UMTS_Inter_Freq_Handover_SR_1 AS `UMTS_Inter_Freq_Handover_SR_1`
				,A.UMTS_Inter_Freq_Handover_SR_2 AS `UMTS_Inter_Freq_Handover_SR_2`
				,A.UMTS_IRAT_handover_SR_1 AS `UMTS_IRAT_handover_SR_1`
				,A.UMTS_IRAT_handover_SR_2 AS `UMTS_IRAT_handover_SR_2`
				,A.LTE_Call_Drop_Rate_1 AS `LTE_Call_Drop_Rate_1`
				,A.LTE_Call_Drop_Rate_2 AS `LTE_Call_Drop_Rate_2`
				,A.LTE_CSSR_1 AS `LTE_CSSR_1`
				,A.LTE_CSSR_2 AS `LTE_CSSR_2`
				,A.LTE_Blocked_Call_Rate_1 AS `LTE_Blocked_Call_Rate_1` 
				,A.LTE_Blocked_Call_Rate_2 AS `LTE_Blocked_Call_Rate_2`
				,A.LTE_Inter_Feq_Handover_SR_1 AS `LTE_Inter_Feq_Handover_SR_1`
				,A.LTE_Inter_Feq_Handover_SR_2 AS `LTE_Inter_Feq_Handover_SR_2`
				,A.LTE_Intra_Feq_Handover_SR_1 AS `LTE_Intra_Feq_Handover_SR_1`
				,A.LTE_Intra_Feq_Handover_SR_2 AS `LTE_Intra_Feq_Handover_SR_2`
				,A.LTE_IRAT_Handover_SR_3G_1 AS `LTE_IRAT_Handover_SR_3G_1`
				,A.LTE_IRAT_Handover_SR_3G_2 AS `LTE_IRAT_Handover_SR_3G_2`
				,A.LTE_IRAT_Handover_SR_2G_1 AS `LTE_IRAT_Handover_SR_2G_1`
				,A.LTE_IRAT_Handover_SR_2G_2 AS `LTE_IRAT_Handover_SR_2G_2`
				,NULL AS `GSM_Call_Drop_Rate_1`
				,NULL AS `GSM_Call_Drop_Rate_2`
				,NULL AS `GSM_CSSR_1`
				,NULL AS `GSM_CSSR_2`
				,NULL AS `GSM_Blocked_Call_Rate_1`
				,NULL AS `GSM_Blocked_Call_Rate_2`
				,NULL AS `GSM_Inter_Freq_Handover_SR_1`
				,NULL AS `GSM_Inter_Freq_Handover_SR_2`
				,A.PU AS PU
				,DATA_DATE AS ds_date
				,TECH_MASK AS `TECH_MASK`
				,CONCAT(A.POS_FIRST_LONGITUDE,''|'',A.POS_FIRST_LATITUDE) AS `FIRST_LAT_LON`
				,CONCAT(A.POS_LAST_LONGITUDE,''|'',A.POS_LAST_LATITUDE) AS `LAST_LAT_LON`
				,A.POS_FIRST_RSCP_SUM AS POS_FIRST_RSCP_SUM
				,A.POS_FIRST_RSCP_CNT AS POS_FIRST_RSCP_CNT
				,A.POS_FIRST_ECN0_SUM AS POS_FIRST_ECN0_SUM
				,A.POS_FIRST_ECN0_CNT AS POS_FIRST_ECN0_CNT
				,A.POS_LAST_RSCP_SUM AS POS_LAST_RSCP_SUM
				,A.POS_LAST_RSCP_CNT AS POS_LAST_RSCP_CNT
				,A.POS_LAST_ECN0_SUM AS POS_LAST_ECN0_SUM
				,A.POS_LAST_ECN0_CNT AS POS_LAST_ECN0_CNT
				,A.POS_FIRST_S_RSRP_SUM AS POS_FIRST_S_RSRP_SUM
				,A.POS_FIRST_S_RSRP_CNT AS POS_FIRST_S_RSRP_CNT
				,A.POS_FIRST_S_RSRQ_SUM AS POS_FIRST_S_RSRQ_SUM
				,A.POS_FIRST_S_RSRQ_CNT AS POS_FIRST_S_RSRQ_CNT
				,A.POS_LAST_S_RSRP_SUM AS POS_LAST_S_RSRP_SUM
				,A.POS_LAST_S_RSRP_CNT AS POS_LAST_S_RSRP_CNT
				,A.POS_LAST_S_RSRQ_SUM AS POS_LAST_S_RSRQ_SUM
				,A.POS_LAST_S_RSRQ_CNT AS POS_LAST_S_RSRQ_CNT');	
	SET STR_IMSI_POS_LTE=CONCAT(' A.CALL_ID AS `CALL_ID`
					,NULL AS `RAB_SEQ_ID`
					,A.MOVING_TYPE AS `MOVING_TYPE`
					,',PU_ID,' AS `PU`
					,4 AS `TECH_MASK`
					,A.DATA_DATE AS ds_date
					,A.POS_FIRST_LOC AS `POS_FIRST_LOC`	
					,A.POS_LAST_LOC AS `POS_LAST_LOC`');
					
	SET STR_IMSI_UMTS=CONCAT('A.CALL_ID
					,CONCAT(A.START_TIME,''.'',LPAD(A.START_TIME_MS,3,0)) AS `START_TIME`
					,CONCAT(A.END_TIME,''.'',LPAD(A.END_TIME_MS,3,0)) AS `END_TIME`
					,ROUND((A.RRC_CONNECT_DURATION/1000),2) AS `DURATION`
					,ROUND((A.CS_CALL_DURA/1000),2) AS `CS_TRAFFIC_TIME`
					,A.CALL_SETUP_TIME AS `CALL_SETUP_TIME`
					,A.IMSI AS `IMSI`
					,A.IMEI_NEW AS `IMEI`
					,A.IMSI AS `MSISDN`
					,A.`MAKE_ID`
					,A.`MODEL_ID`
					,2 AS `TECH_MASK`
					,(CASE WHEN A.call_type IN (10,11) THEN ''Voice'' WHEN call_type=12 THEN ''PS 99'' 
						WHEN A.call_type=13 THEN ''PS HSPA'' WHEN call_type=14 THEN ''Multi-RAB''  
						WHEN A.call_type=15 THEN ''Signal'' WHEN call_type=16 THEN ''SMS'' 
						WHEN A.call_type=18 THEN ''PS Others'' ELSE ''Unkonwn'' END) AS call_type
					,A.`ACCESS_POINT_NAME` AS APN
					,(CASE WHEN call_status=1 THEN ''Normal'' 
						WHEN call_status=2 THEN ''Drop''
						WHEN call_status=3 THEN ''Block'' 
						WHEN call_status=6 THEN ''SetupFailure''
						ELSE ''Unspecified'' END) AS call_status
					,IFNULL(IU_RELEASE_CAUSE_STR, ''N/A'') AS RELEASE_CAUSE
					,CONCAT(A.`MCC`,''-'',A.`MNC`,''-'',A.`START_RNC_ID`,''-'',A.`START_CELL_ID`) AS START_CELL
					,A.`POS_FIRST_LOC`
					,A.`POS_FIRST_RSCP` AS START_RXLEV_RSCP_RSRP_dBn
					,A.`POS_FIRST_ECN0` AS START_RXQUAL_ECN0_RSRQ_dB
					,CONCAT(A.`MCC`,''-'',A.`MNC`,''-'',A.`POS_LAST_RNC`,''-'',A.`POS_LAST_CELL`) AS END_CELL
					,A.`POS_LAST_LOC`
					,A.`POS_LAST_RSCP` AS END_RXLEV_RSCP_RSRP_dBn
					,A.`POS_LAST_ECN0` AS END_RXQUAL_ECN0_RSRQ_dB
					,A.`DL_TRAFFIC_VOLUME` AS `DL_TRAFFIC_VOLUME_MB`
					,A.`DL_THROUGHPUT_AVG` AS `DL_THROUGHPUT_Kbps`
					,A.`DL_THROUGHPUT_MAX` AS `DL_THROUGHPUT_MAX_Kbps`
					,A.`UL_TRAFFIC_VOLUME` AS `UL_TRAFFIC_VOLUME_MB`
					,A.`UL_THROUGHPUT_AVG` AS `UL_THROUGHPUT_Kbps`
					,A.`UL_THROUGHPUT_MAX` AS `UL_THROUGHPUT_MAX_Kbps`
					,(IFNULL(A.`SHO`,0) + IFNULL(A.`SOHO`,0)) AS `INTRA_FREQ_HO_ATTEMPT`
					,(IFNULL(A.`SHO_FAILURE`,0) + IFNULL(A.`SOHO_FAILURE`,0)) AS `INTRA_FREQ_HO_FAILURE`
					,A.`IFHO` AS `INTER_FREQ_HO_ATTEMPT`
					,A.`IFHO_FAILURE` AS `INTER_FREQ_HO_FAILURE`
					,A.`IRAT_HHO_ATTEMPT` AS `IRAT_HO_ATTEMPT`
					,(IFNULL(A.`IRAT_HHO_ATTEMPT`,0)-IFNULL(A.`IRAT_HHO_SUCCESS`,0)) AS `IRAT_HO_FAILURE`
					,(CASE WHEN A.`INDOOR`=1 THEN ''Indoor'' WHEN A.`INDOOR`=0 AND A.`MOVING`=0 THEN ''Stationary'' 
					WHEN A.`INDOOR`=0 AND A.`MOVING`=1 THEN ''Moving'' WHEN A.`INDOOR`=0 AND A.`MOVING` IS NULL THEN ''Outdoor'' ELSE ''N/A'' END) AS `INDOOR`
					,A.`MOVING` AS `MOVING`
					,(CASE WHEN A.`MOVING_TYPE`=1 THEN ''Walking'' WHEN A.`MOVING_TYPE`=2 THEN ''In Vehicle'' ELSE ''N/A'' END)AS `MOVING_TYPE`
					,B_PARTY_NUMBER AS `B_PARTY_NUMBER`
					,DATA_DATE AS DATA_DATE
					,A.DATA_HOUR AS `DATA_HOUR`
					,RIGHT(CONCAT(''00'',A.BATCH),4) AS `BATCH`
					,LEFT((CONCAT(CASE WHEN CELL_UPDATE_CAUSE & 1>0 THEN ''cell reselection,'' ELSE '''' END,
						CASE WHEN CELL_UPDATE_CAUSE & 2>0 THEN ''periodic cell,'' ELSE '''' END,
						CASE WHEN CELL_UPDATE_CAUSE & 4>0 THEN ''uplink DATA transmission,'' ELSE '''' END,
						CASE WHEN CELL_UPDATE_CAUSE & 8>0 THEN ''paging response,'' ELSE '''' END,
						CASE WHEN CELL_UPDATE_CAUSE & 16>0 THEN ''re-entered service AREA,'' ELSE '''' END,
						CASE WHEN CELL_UPDATE_CAUSE & 32>0 THEN ''radio link failure,'' ELSE '''' END,
						CASE WHEN CELL_UPDATE_CAUSE & 64>0 THEN ''unrecoverable RLC error,'' ELSE '''' END,
						CASE WHEN CELL_UPDATE_CAUSE & 128>0 THEN ''MBMS reception,'' ELSE '''' END,
						CASE WHEN CELL_UPDATE_CAUSE & 256>0 THEN ''MBMS ptp RB request,'' ELSE '''' END))
						,LENGTH(CONCAT(CASE WHEN CELL_UPDATE_CAUSE & 1>0 THEN ''cell reselection,'' ELSE '''' END,
						CASE WHEN CELL_UPDATE_CAUSE & 2>0 THEN ''periodic cell,'' ELSE '''' END,
						CASE WHEN CELL_UPDATE_CAUSE & 4>0 THEN ''uplink DATA transmission,'' ELSE '''' END,
						CASE WHEN CELL_UPDATE_CAUSE & 8>0 THEN ''paging response,'' ELSE '''' END,
						CASE WHEN CELL_UPDATE_CAUSE & 16>0 THEN ''re-entered service AREA,'' ELSE '''' END,
						CASE WHEN CELL_UPDATE_CAUSE & 32>0 THEN ''radio link failure,'' ELSE '''' END,
						CASE WHEN CELL_UPDATE_CAUSE & 64>0 THEN ''unrecoverable RLC error,'' ELSE '''' END,
						CASE WHEN CELL_UPDATE_CAUSE & 128>0 THEN ''MBMS reception,'' ELSE '''' END,
						CASE WHEN CELL_UPDATE_CAUSE & 256>0 THEN ''MBMS ptp RB request,'' ELSE '''' END))-1)  AS  `CELL_UPDATE_CAUSE`
					,RAB_SEQ_ID AS `RAB_SEQ_ID`
					,DATA_DATE AS ds_date
					,POS_FIRST_CELL AS POS_FIRST_CELL
					,POS_LAST_CELL AS POS_LAST_CELL
					,START_CELL_ID
					,END_CELL_ID
					,NULL AS `MANUFACTURER`,NULL AS `MODEL`
					,',PU_ID,' AS pu');
					
	SET STR_IMSI_GRP_UMTS=CONCAT('A.IMSI AS `IMSI`
					,A.DATA_DATE AS `DATA_DATE`
					,MAX(CONCAT(IFNULL(A.MAKE_ID,'' ''),''|'',IFNULL(A.MODEL_ID,'' ''),''|'',IFNULL(A.IMEI_NEW,'' ''))) AS `HANDSET`
					,A.IMSI AS `MSISDN`
					,COUNT(A.CALL_ID) AS `Total_Call_Count`
					,SUM(IF(A.CALL_STATUS=2,1,0)) AS `Drop_Call_Count`
					,SUM(IF(A.CALL_STATUS=3,1,0)) AS `Block_Call_Count`
					,SUM(A.DL_TRAFFIC_VOLUME) AS `Total_DL_Data_Volume`
					,SUM(A.UL_TRAFFIC_VOLUME) AS `Total_UL_Data_Volume`
					,MAX(A.DL_THROUGHPUT_MAX) AS `MAX_DL_THROUGHPUT`
					,MAX(A.UL_THROUGHPUT_MAX) AS `MAX_UL_THROUGHPUT`
					,IFNULL(SUM(IF(A.call_status=2,1,0)),0) AS `UMTS_Call_Drop_Rate_1`
					,IFNULL(SUM(IF(A.call_status IN (1,2,4),1,0)),0) AS `UMTS_Call_Drop_Rate_2`
					,IFNULL(SUM(IF(A.call_status IN (3,6),1,0)),0) AS `UMTS_CSSR_1`
					,COUNT(*) AS `UMTS_CSSR_2`
					,IFNULL(SUM(IF(A.call_status=3,1,0)),0) AS `UMTS_Blocked_Call_Rate_1`
					,COUNT(*) AS `UMTS_Blocked_Call_Rate_2`
					,SUM(IFNULL(SHO,0)-IFNULL(SHO_FAILURE,0)) AS `UMTS_Soft_Handover_SR_1`
					,IFNULL(SUM(SHO),0) AS `UMTS_Soft_Handover_SR_2`
					,SUM(IFNULL(SOHO,0)-IFNULL(SOHO_FAILURE,0)) AS `UMTS_Softer_handover_SR_1`
					,IFNULL(SUM(SOHO),0) AS `UMTS_Softer_handover_SR_2`
					,SUM(IFNULL(IFHO,0)-IFNULL(IFHO_FAILURE,0))AS `UMTS_Inter_Freq_Handover_SR_1`
					,IFNULL(SUM(IFHO),0) AS `UMTS_Inter_Freq_Handover_SR_2`
					,IFNULL(SUM(IRAT_HHO_SUCCESS),0) AS `UMTS_IRAT_handover_SR_1`
					,IFNULL(SUM(IRAT_HHO_ATTEMPT),0) AS `UMTS_IRAT_handover_SR_2`
					,NULL AS `LTE_Call_Drop_Rate_1`
					,NULL AS `LTE_Call_Drop_Rate_2`
					,NULL AS `LTE_CSSR_1`
					,NULL AS `LTE_CSSR_2`
					,NULL AS `LTE_Blocked_Call_Rate_1` 
					,NULL AS `LTE_Blocked_Call_Rate_2`
					,NULL AS `LTE_Inter_Feq_Handover_SR_1`
					,NULL AS `LTE_Inter_Feq_Handover_SR_2`
					,NULL AS `LTE_Intra_Feq_Handover_SR_1`
					,NULL AS `LTE_Intra_Feq_Handover_SR_2`
					,NULL AS `LTE_IRAT_Handover_SR_3G_1`
					,NULL AS `LTE_IRAT_Handover_SR_3G_2`
					,NULL AS `LTE_IRAT_Handover_SR_2G_1`
					,NULL AS `LTE_IRAT_Handover_SR_2G_2`
					,NULL AS `GSM_Call_Drop_Rate_1`
					,NULL AS `GSM_Call_Drop_Rate_2`
					,NULL AS `GSM_CSSR_1`
					,NULL AS `GSM_CSSR_2`
					,NULL AS `GSM_Blocked_Call_Rate_1`
					,NULL AS `GSM_Blocked_Call_Rate_2`
					,NULL AS `GSM_Inter_Freq_Handover_SR_1`
					,NULL AS `GSM_Inter_Freq_Handover_SR_2`
					,',PU_ID,' AS PU
					,DATA_DATE AS ds_date
					,2 AS `TECH_MASK`
					,MAX(A.POS_FIRST_LOC) AS POS_FIRST_LOC
					,MAX(A.POS_LAST_LOC) AS POS_LAST_LOC
					,SUM(A.POS_FIRST_RSCP) AS POS_FIRST_RSCP_SUM
					,COUNT(A.POS_FIRST_RSCP) AS POS_FIRST_RSCP_CNT
					,SUM(A.POS_FIRST_ECN0) AS POS_FIRST_ECN0_SUM
					,COUNT(A.POS_FIRST_ECN0) AS POS_FIRST_ECN0_CNT
					,SUM(A.POS_LAST_RSCP) AS POS_LAST_RSCP_SUM
					,COUNT(A.POS_LAST_RSCP) AS POS_LAST_RSCP_CNT
					,SUM(A.POS_LAST_ECN0) AS POS_LAST_ECN0_SUM
					,COUNT(A.POS_LAST_ECN0) AS POS_LAST_ECN0_CNT
					,NULL AS POS_FIRST_S_RSRP_SUM
					,NULL AS POS_FIRST_S_RSRP_CNT
					,NULL AS POS_FIRST_S_RSRQ_SUM
					,NULL AS POS_FIRST_S_RSRQ_CNT
					,NULL AS POS_LAST_S_RSRP_SUM
					,NULL AS POS_LAST_S_RSRP_CNT
					,NULL AS POS_LAST_S_RSRQ_SUM
					,NULL AS POS_LAST_S_RSRQ_CNT');	
									
	SET STR_IMSI_GRP_WITHDUMP_UMTS=CONCAT('A.IMSI AS `IMSI`
						,A.DATA_DATE AS `DATA_DATE`
						,NULL AS `HANDSET`
						,A.IMSI AS `MSISDN`
						,COUNT(A.CALL_ID) AS `Total_Call_Count`
						,SUM(IF(A.CALL_STATUS=2,1,0)) AS DROP_CNT
						,SUM(IF(A.CALL_STATUS=3,1,0)) AS BLOCK_CNT
						,SUM(A.DL_TRAFFIC_VOLUME) AS `Total_DL_Data_Volume`
						,SUM(A.UL_TRAFFIC_VOLUME) AS `Total_UL_Data_Volume`
						,MAX(A.DL_THROUGHPUT_MAX) AS `MAX_DL_THROUGHPUT`
						,MAX(A.UL_THROUGHPUT_MAX) AS `MAX_UL_THROUGHPUT`
						,IFNULL(SUM(IF(A.call_status=2,1,0)),0) AS `UMTS_Call_Drop_Rate_1`
						,IFNULL(SUM(IF(A.call_status IN (1,2,4),1,0)),0) AS `UMTS_Call_Drop_Rate_2`
						,IFNULL(SUM(IF(A.call_status IN (3,6),1,0)),0) AS `UMTS_CSSR_1`
						,COUNT(*) AS `UMTS_CSSR_2`
						,IFNULL(SUM(IF(A.call_status=3,1,0)),0) AS `UMTS_Blocked_Call_Rate_1`
						,COUNT(*) AS `UMTS_Blocked_Call_Rate_2`
						,SUM(IFNULL(SHO,0)-IFNULL(SHO_FAILURE,0)) AS `UMTS_Soft_Handover_SR_1`
						,IFNULL(SUM(SHO),0) AS `UMTS_Soft_Handover_SR_2`
						,SUM(IFNULL(SOHO,0)-IFNULL(SOHO_FAILURE,0)) AS `UMTS_Softer_handover_SR_1`
						,IFNULL(SUM(SOHO),0) AS `UMTS_Softer_handover_SR_2`
						,SUM(IFNULL(IFHO,0)-IFNULL(IFHO_FAILURE,0))AS `UMTS_Inter_Freq_Handover_SR_1`
						,IFNULL(SUM(IFHO),0) AS `UMTS_Inter_Freq_Handover_SR_2`
						,IFNULL(SUM(IRAT_HHO_SUCCESS),0) AS `UMTS_IRAT_handover_SR_1`
						,IFNULL(SUM(IRAT_HHO_ATTEMPT),0) AS `UMTS_IRAT_handover_SR_2`
						,NULL AS `LTE_Call_Drop_Rate_1`
						,NULL AS `LTE_Call_Drop_Rate_2`
						,NULL AS `LTE_CSSR_1`
						,NULL AS `LTE_CSSR_2`
						,NULL AS `LTE_Blocked_Call_Rate_1` 
						,NULL AS `LTE_Blocked_Call_Rate_2`
						,NULL AS `LTE_Inter_Feq_Handover_SR_1`
						,NULL AS `LTE_Inter_Feq_Handover_SR_2`
						,NULL AS `LTE_Intra_Feq_Handover_SR_1`
						,NULL AS `LTE_Intra_Feq_Handover_SR_2`
						,NULL AS `LTE_IRAT_Handover_SR_3G_1`
						,NULL AS `LTE_IRAT_Handover_SR_3G_2`
						,NULL AS `LTE_IRAT_Handover_SR_2G_1`
						,NULL AS `LTE_IRAT_Handover_SR_2G_2`
						,NULL AS `GSM_Call_Drop_Rate_1`
						,NULL AS `GSM_Call_Drop_Rate_2`
						,NULL AS `GSM_CSSR_1`
						,NULL AS `GSM_CSSR_2`
						,NULL AS `GSM_Blocked_Call_Rate_1`
						,NULL AS `GSM_Blocked_Call_Rate_2`
						,NULL AS `GSM_Inter_Freq_Handover_SR_1`
						,NULL AS `GSM_Inter_Freq_Handover_SR_2`
						,',PU_ID,' AS PU
						,DATA_DATE AS ds_date
						,2 AS `TECH_MASK`
						,NULL AS POS_FIRST_LOC
						,NULL AS POS_LAST_LOC
						,NULL AS POS_FIRST_RSCP_SUM
						,NULL AS POS_FIRST_RSCP_CNT
						,NULL AS POS_FIRST_ECN0_SUM
						,NULL AS POS_FIRST_ECN0_CNT
						,NULL AS POS_LAST_RSCP_SUM
						,NULL AS POS_LAST_RSCP_CNT
						,NULL AS POS_LAST_ECN0_SUM
						,NULL AS POS_LAST_ECN0_CNT
						,NULL AS POS_FIRST_S_RSRP_SUM
						,NULL AS POS_FIRST_S_RSRP_CNT
						,NULL AS POS_FIRST_S_RSRQ_SUM
						,NULL AS POS_FIRST_S_RSRQ_CNT
						,NULL AS POS_LAST_S_RSRP_SUM
						,NULL AS POS_LAST_S_RSRP_CNT
						,NULL AS POS_LAST_S_RSRQ_SUM
						,NULL AS POS_LAST_S_RSRQ_CNT');	
						
	SET STR_IMSI_WITHDUMP_UMTS=CONCAT('A.CALL_ID
						,A.START_TIME AS `START_TIME`
						,A.END_TIME AS `END_TIME`
						,ROUND((A.RRC_CONNECT_DURATION/1000),2) AS `DURATION`
						,NULL AS `CS_TRAFFIC_TIME`
						,A.CALL_SETUP_TIME AS `CALL_SETUP_TIME`
						,A.IMSI AS `IMSI`
						,A.IMEI_NEW AS `IMEI`
						,A.IMSI AS `MSISDN`
						,NULL AS `MAKE_ID`
						,NULL AS `MODEL_ID`
						,2 AS `TECH_MASK`
						,(CASE WHEN A.call_type IN (10,11) THEN ''Voice'' WHEN call_type=12 THEN ''PS 99'' 
							WHEN A.call_type=13 THEN ''PS HSPA'' WHEN call_type=14 THEN ''Multi-RAB''  
							WHEN A.call_type=15 THEN ''Signal'' WHEN call_type=16 THEN ''SMS'' 
							WHEN A.call_type=18 THEN ''PS Others'' ELSE ''Unkonwn'' END) AS call_type
						,A.`ACCESS_POINT_NAME` AS APN
						,(CASE WHEN call_status=1 THEN ''Normal'' 
							WHEN call_status=2 THEN ''Drop''
							WHEN call_status=3 THEN ''Block'' 
							WHEN call_status=6 THEN ''SetupFailure''
							ELSE ''Unspecified'' END) AS call_status
						,IFNULL(IU_RELEASE_CAUSE_STR, ''N/A'') AS RELEASE_CAUSE
						,CONCAT(A.`MCC`,''-'',A.`MNC`,''-'',A.`START_RNC_ID`,''-'',A.`START_CELL_ID`) AS START_CELL
						,NULL AS `POS_FIRST_LOC`
						,NULL AS START_RXLEV_RSCP_RSRP_dBn
						,NULL AS START_RXQUAL_ECN0_RSRQ_dB
						,CONCAT(A.`MCC`,''-'',A.`MNC`,''-'',A.`END_RNC_ID`,''-'',A.`END_CELL_ID`) AS END_CELL
						,NULL AS `POS_LAST_LOC`
						,NULL AS  END_RXLEV_RSCP_RSRP_dBn
						,NULL AS  END_RXQUAL_ECN0_RSRQ_dB
						,A.`DL_TRAFFIC_VOLUME` AS `DL_TRAFFIC_VOLUME_MB`
						,A.`DL_THROUGHPUT_AVG` AS `DL_THROUGHPUT_Kbps`
						,A.`DL_THROUGHPUT_MAX` AS `DL_THROUGHPUT_MAX_Kbps`
						,A.`UL_TRAFFIC_VOLUME` AS `UL_TRAFFIC_VOLUME_MB`
						,A.`UL_THROUGHPUT_AVG` AS `UL_THROUGHPUT_Kbps`
						,A.`UL_THROUGHPUT_MAX` AS `UL_THROUGHPUT_MAX_Kbps`
						,(IFNULL(A.`SHO`,0) + IFNULL(A.`SOHO`,0)) AS `INTRA_FREQ_HO_ATTEMPT`
						,(IFNULL(A.`SHO_FAILURE`,0) + IFNULL(A.`SOHO_FAILURE`,0)) AS `INTRA_FREQ_HO_FAILURE`
						,A.`IFHO` AS `INTER_FREQ_HO_ATTEMPT`
						,A.`IFHO_FAILURE` AS `INTER_FREQ_HO_FAILURE`
						,A.`IRAT_HHO_ATTEMPT` AS `IRAT_HO_ATTEMPT`
						,(IFNULL(A.`IRAT_HHO_ATTEMPT`,0)-IFNULL(A.`IRAT_HHO_SUCCESS`,0)) AS `IRAT_HO_FAILURE`
						,(CASE WHEN A.`INDOOR`=1 THEN ''Indoor'' WHEN A.`INDOOR`=0 AND A.`MOVING`=0 THEN ''Stationary'' 
						WHEN A.`INDOOR`=0 AND A.`MOVING`=1 THEN ''Moving'' WHEN A.`INDOOR`=0 AND A.`MOVING` IS NULL THEN ''Outdoor'' ELSE ''N/A'' END) AS `INDOOR`
						,A.`MOVING` AS `MOVING`
						,''N/A'' AS `MOVING_TYPE`
						,B_PARTY_NUMBER AS `B_PARTY_NUMBER`
						,DATA_DATE AS DATA_DATE
						,A.DATA_HOUR AS `DATA_HOUR`
						,NULL AS `BATCH`
						,LEFT((CONCAT(CASE WHEN CELL_UPDATE_CAUSE & 1>0 THEN ''cell reselection,'' ELSE '''' END,
						CASE WHEN CELL_UPDATE_CAUSE & 2>0 THEN ''periodic cell,'' ELSE '''' END,
						CASE WHEN CELL_UPDATE_CAUSE & 4>0 THEN ''uplink DATA transmission,'' ELSE '''' END,
						CASE WHEN CELL_UPDATE_CAUSE & 8>0 THEN ''paging response,'' ELSE '''' END,
						CASE WHEN CELL_UPDATE_CAUSE & 16>0 THEN ''re-entered service AREA,'' ELSE '''' END,
						CASE WHEN CELL_UPDATE_CAUSE & 32>0 THEN ''radio link failure,'' ELSE '''' END,
						CASE WHEN CELL_UPDATE_CAUSE & 64>0 THEN ''unrecoverable RLC error,'' ELSE '''' END,
						CASE WHEN CELL_UPDATE_CAUSE & 128>0 THEN ''MBMS reception,'' ELSE '''' END,
						CASE WHEN CELL_UPDATE_CAUSE & 256>0 THEN ''MBMS ptp RB request,'' ELSE '''' END))
						,LENGTH(CONCAT(CASE WHEN CELL_UPDATE_CAUSE & 1>0 THEN ''cell reselection,'' ELSE '''' END,
						CASE WHEN CELL_UPDATE_CAUSE & 2>0 THEN ''periodic cell,'' ELSE '''' END,
						CASE WHEN CELL_UPDATE_CAUSE & 4>0 THEN ''uplink DATA transmission,'' ELSE '''' END,
						CASE WHEN CELL_UPDATE_CAUSE & 8>0 THEN ''paging response,'' ELSE '''' END,
						CASE WHEN CELL_UPDATE_CAUSE & 16>0 THEN ''re-entered service AREA,'' ELSE '''' END,
						CASE WHEN CELL_UPDATE_CAUSE & 32>0 THEN ''radio link failure,'' ELSE '''' END,
						CASE WHEN CELL_UPDATE_CAUSE & 64>0 THEN ''unrecoverable RLC error,'' ELSE '''' END,
						CASE WHEN CELL_UPDATE_CAUSE & 128>0 THEN ''MBMS reception,'' ELSE '''' END,
						CASE WHEN CELL_UPDATE_CAUSE & 256>0 THEN ''MBMS ptp RB request,'' ELSE '''' END))-1)  AS  `CELL_UPDATE_CAUSE`
						,RAB_SEQ_ID AS `RAB_SEQ_ID`
						,DATA_DATE AS ds_date
						,NULL AS POS_FIRST_CELL
						,NULL AS POS_LAST_CELL
						,START_CELL_ID
						,END_CELL_ID
						,NULL,NUll
						,',PU_ID,' AS pu');	
						
	SET STR_IMSI_POS_UMTS=CONCAT('A.CALL_ID AS `CALL_ID`
					,A.RAB_SEQ_ID AS `RAB_SEQ_ID`					
					,A.MOVING_TYPE AS `MOVING_TYPE` 
					,',PU_ID,' AS `PU`
					,2 AS `TECH_MASK`
					,A.DATA_DATE AS ds_date		
					,POS_FIRST_LOC AS `POS_FIRST_LOC`
					,POS_LAST_LOC AS `POS_LAST_LOC`');
					
SET STR_IMSI_MR_LTE=CONCAT('CALL_ID,
								SEQ_ID,
								DATE_TIME,
								DATE_TIME_MS,
								EVENT_ID,
								SUB_REGION_ID,
								EUTRABAND,
								EARFCN,
								ENODEB_ID,
								CELL_ID,
								CELL_NAME,
								RSRP,
								RSRQ,
								TA,
								LOC_ID,
								TILE_ID,
								UL_VOLUME,
								DL_VOLUME,
								UL_THROUPUT,
								DL_THROUPUT,
								DATA_DATE,
								DATA_HOUR,
								CALL_STATUS,
								CALL_TYPE,
								MOVING,
								INDOOR,
								IMEI,
								IMSI,
								MAKE_ID,
								MODEL_ID,
								CQI,
								SINR_PUSCH,
								SINR_PUCCH,
								DL_THROUGHPUT_EVENT_CNT,
								UL_THROUGHPUT_EVENT_CNT,
								MOVING_SPEED,
								MOVING_DIRECTION,
								MR_MERGE_CNT,
								SINR_DL,
								CQI_SAMPLE,
								SINR_PUSCH_SAMPLE,
								SINR_PUCCH_SAMPLE,
								UL_THROUGHPUT_MAX,
								DL_THROUGHPUT_MAX,
								IMSI_GROUP_ID,
								LON,
								LAT,
								ALTITUDE,
								M_TMSI,
								DEVIATION_LEVEL,
								BATCH,
								',PU_ID,' AS PU,
								METHOD,
								QCI_ID,
								DL_TRAFFIC_DUR,
								UL_TRAFFIC_DUR');					
					
	SET STR_IMSI_MR_GRP_LTE=CONCAT('A.CALL_ID,
								A.IMSI,
								(CONCAT(IFNULL(A.MAKE_ID,'' ''),''|'',IFNULL(A.MODEL_ID,'' ''),''|'',IFNULL(A.IMEI,'' ''))) AS HANDSET,
								MAX(IF(A.EVENT_ID=0, A.DATE_TIME, NULL)) AS START_TIME,
								MAX(IF(A.EVENT_ID=1, A.DATE_TIME, NULL)) AS END_TIME,
								A.CALL_TYPE,
								A.CALL_STATUS,
								SUM(IF(A.EVENT_ID=200 AND A.RSRP IS NOT NULL,A.RSRP,NULL)) AS RSRP_SUM,
								SUM(IF(A.EVENT_ID=200 AND A.RSRP IS NOT NULL,1,NULL)) AS RSRP_CNT,
								SUM(IF(A.EVENT_ID=200 AND A.RSRQ IS NOT NULL,A.RSRQ,NULL)) AS RSRQ_SUM,
								SUM(IF(A.EVENT_ID=200 AND A.RSRQ IS NOT NULL,1,NULL)) AS RSRQ_CNT,
								SUM(A.CQI) AS CQI,
								SUM(A.CQI_SAMPLE) AS CQI_SAMPLE,
								SUM(A.SINR_PUSCH) AS SINR_PUSCH,
								SUM(A.SINR_PUSCH_SAMPLE) AS SINR_PUSCH_SAMPLE,
								SUM(A.SINR_PUCCH) AS SINR_PUCCH,
								SUM(A.SINR_PUCCH_SAMPLE) AS SINR_PUCCH_SAMPLE,
								SUM(A.UL_VOLUME) AS UL_VOLUME,
								SUM(A.UL_TRAFFIC_DUR) AS UL_TRAFFIC_DUR,
								SUM(A.DL_VOLUME) AS DL_VOLUME,
								SUM(A.DL_TRAFFIC_DUR) AS DL_TRAFFIC_DUR,
								',PU_ID,' AS PU,
								IF(A.EVENT_ID=0, A.LON, NULL) AS  LONGITUDE,
								IF(A.EVENT_ID=0, A.LAT, NULL) AS LATITUDE,
								A.DATA_DATE');	

	SET STR_VIP_GRP_UMTS=CONCAT('
					A.IMSI AS `IMSI`,
					A.DATA_DATE AS `DATA_DATE`,
					A.IMEI_NEW AS `IMEI`,
					1 AS TOTAL_CNT,
					IF(A.CALL_STATUS=2, 1, 0) AS DROP_CNT,
					IF(A.CALL_STATUS IN (1,2,4,5), 1, 0) AS NB_TOTAL_CNT,
					IF(A.CALL_STATUS=3, 1, 0) AS BLOCK_CNT,
					IF(A.CALL_STATUS IN (3,6), 1, 0) AS CSF_CNT,
					IF(A.CALL_STATUS NOT IN (3,6), 1, 0) AS CSS_CNT,
					A.IRAT_HHO_ATTEMPT AS UMTS_IRAT_HHO_ATTEMPT,
					A.IRAT_HHO_FAIL AS UMTS_IRAT_HHO_FAIL,
					A.IRAT_HHO_SUCCESS AS UMTS_IRAT_HHO_SUCCESS,
					IF(A.CALL_STATUS=3, 1, 0) AS UMTS_BLOCK_CNT,
					1 AS UMTS_TOTAL_CNT,
					IF(A.CALL_STATUS NOT IN (3,6), 1, 0) AS UMTS_CSS_CNT,
					IF(A.CALL_STATUS IN (3,6), 1, 0) AS UMTS_CSF_CNT,
					IF(A.CALL_STATUS=2, 1, 0) AS UMTS_DROP_CNT,
					IF(A.CALL_STATUS IN (1,2,4,5), 1, 0) AS UMTS_NB_TOTAL_CNT,
					NULL AS LTE_IRAT_TO_UMTS_ATTEMPT,
					NULL AS LTE_IRAT_TO_UMTS_FAILURE,
					NULL AS LTE_BLOCK_CNT,
					NULL AS LTE_TOTAL_CNT,
					NULL AS LTE_CSS_CNT,
					NULL AS LTE_CSF_CNT,
					NULL AS LTE_DROP_CNT,
					NULL AS LTE_NB_TOTAL_CNT,
					',PU_ID,' AS PU,
					DATA_DATE AS ds_date,
					2 AS `TECH_MASK`,
					A.POS_FIRST_LON,
					A.POS_FIRST_LAT,
					A.POS_LAST_LON,
					A.POS_LAST_LAT');						

	SET STR_VIP_GRP_LTE=CONCAT('
					A.IMSI AS `IMSI`,
					A.DATA_DATE AS `DATA_DATE`,
					A.IMEI AS `IMEI`,
					1 AS TOTAL_CNT,
					IF(A.CALL_STATUS=2, 1, 0) AS DROP_CNT,
					IF(A.CALL_STATUS IN (1,2,4,5), 1, 0) AS NB_TOTAL_CNT,
					IF(A.CALL_STATUS=3, 1, 0) AS BLOCK_CNT,
					IF(A.CALL_STATUS IN (3,6), 1, 0) AS CSF_CNT,
					IF(A.CALL_STATUS NOT IN (3,6), 1, 0) AS CSS_CNT,
					NULL AS UMTS_IRAT_HHO_ATTEMPT,
					NULL AS UMTS_IRAT_HHO_FAIL,
					NULL AS UMTS_IRAT_HHO_SUCCESS,
					NULL AS UMTS_BLOCK_CNT,
					NULL AS UMTS_TOTAL_CNT,
					NULL AS UMTS_CSS_CNT,
					NULL AS UMTS_CSF_CNT,
					NULL AS UMTS_DROP_CNT,
					NULL AS UMTS_NB_TOTAL_CNT,
					A.IRAT_TO_UMTS_ATTEMPT AS LTE_IRAT_TO_UMTS_ATTEMPT,
					A.IRAT_TO_UMTS_FAILURE AS LTE_IRAT_TO_UMTS_FAILURE,
					IF(A.CALL_STATUS=3, 1, 0) AS LTE_BLOCK_CNT,
					1 AS LTE_TOTAL_CNT,
					IF(A.CALL_STATUS NOT IN (3,6), 1, 0) AS LTE_CSS_CNT,
					IF(A.CALL_STATUS IN (3,6), 1, 0) AS LTE_CSF_CNT,
					IF(A.CALL_STATUS=2, 1, 0) AS LTE_DROP_CNT,
					IF(A.CALL_STATUS IN (1,2,4,5), 1, 0) AS LTE_NB_TOTAL_CNT,
					',PU_ID,' AS PU,
					DATA_DATE AS ds_date,
					4 AS `TECH_MASK`,
					A.POS_FIRST_LON,
					A.POS_FIRST_LAT,
					A.POS_LAST_LON,
					A.POS_LAST_LAT');		
	
	IF START_HOUR=0 AND END_HOUR=23 THEN 
		SET DY_FLAG=1;
		IF (KPI_ID=110001 OR KPI_ID=110005 OR KPI_ID=110014 OR KPI_ID=110020 OR KPI_ID=110021 OR KPI_ID=110025 OR KPI_ID=110026 OR KPI_ID=110027) AND TECH_NAME IN('LTE','UMTS') THEN
			SET FILTER_STR=CONCAT(IN_STR('DATA_QUARTER',DATA_QUARTER)
					,CASE WHEN POS_KIND='' THEN IN_STR('A.CELL_ID',CELL_ID) ELSE IN_STR(CONCAT('POS_',POS_KIND,'_CELL'),CELL_ID) END
					,CASE WHEN POS_KIND='' THEN IN_STR('TILE_ID',TILE_ID) ELSE IN_STR(CONCAT('gt_covmo_proj_geohash_to_hex_geohash(POS_',POS_KIND_LOC,'_LOC)'),TILE_ID) END
					,CASE WHEN (IMSI<>'' AND IMSI_STR<> '') THEN IN_STR('A.IMSI',IN_QUOTE(CONCAT(IMSI,',',IMSI_STR))) 
					      WHEN (IMSI<>'') THEN IN_STR('A.IMSI',IN_QUOTE(IMSI))  
					      WHEN (IMSI_STR<>'') THEN IN_STR('A.IMSI',IN_QUOTE(IMSI_STR)) 
						ELSE ''
					 END
					,IN_STR('CLUSTER_ID',CLUSTER_ID)
					,IN_STR('A.CALL_TYPE',CALL_TYPE)
					,IN_STR('A.CALL_STATUS',IN_QUOTE(CALL_STATUS))
					,IN_STR('A.INDOOR',INDOOR),IN_STR('A.MOVING',MOVING),IN_STR('A.CELL_INDOOR',CELL_INDOOR)
					,IN_STR('A.FREQUENCY',FREQUENCY),IN_STR('A.UARFCN',UARFCN),IN_STR('CELL_LON',CELL_LON),IN_STR('CELL_LAT',CELL_LAT)					
					,CASE WHEN TECH_NAME='UMTS' THEN IN_STR('IMEI_NEW',IN_QUOTE(IMEI_NEW)) ELSE IN_STR('IMEI',IN_QUOTE(IMEI_NEW)) END	
					,IN_STR('APN',APN)		
					,IN_STR('SITE_ID',IN_QUOTE(SITE_ID)),IN_STR('MAKE_ID',MAKE_ID),IN_STR('MODEL_ID',MODEL_ID),IN_STR('POLYGON_STR',POLYGON_STR)
					,CASE WHEN FILTER='' THEN '' ELSE CONCAT(' AND ',FILTER) END);
		ELSEIF KPI_ID=110019 THEN 
					SET FILTER_STR=CONCAT(' 1 '
					,CASE WHEN ENODEB_ID <> '' THEN  IN_STR('ENODEB_ID',IN_QUOTE(ENODEB_ID)) ELSE '' END
					,CASE WHEN CELL_ID <> '' THEN  IN_STR('CELL_ID',IN_QUOTE(CELL_ID)) ELSE '' END
					,CASE WHEN SUB_REGION_ID <> '' THEN  IN_STR('SUB_REGION_ID',IN_QUOTE(SUB_REGION_ID)) ELSE ''  END);
		ELSE
			SET FILTER_STR=CONCAT(' 1 ',IN_STR('DATA_QUARTER',DATA_QUARTER)
					,CASE WHEN POS_KIND='' THEN IN_STR('A.CELL_ID',CELL_ID) ELSE IN_STR(CONCAT('POS_',POS_KIND,'_CELL'),CELL_ID) END
					,CASE WHEN POS_KIND='' THEN IN_STR('TILE_ID',TILE_ID) ELSE IN_STR(CONCAT('gt_covmo_proj_geohash_to_hex_geohash(POS_',POS_KIND_LOC,'_LOC)'),TILE_ID) END
					,CASE WHEN (IMSI<>'' AND IMSI_STR<> '') THEN IN_STR('IMSI',IN_QUOTE(CONCAT(IMSI,',',IMSI_STR))) 
					      WHEN (IMSI<>'') THEN IN_STR('IMSI',IN_QUOTE(IMSI))  
					      WHEN (IMSI_STR<>'') THEN IN_STR('IMSI',IN_QUOTE(IMSI_STR)) 
					      ELSE ''
					 END
					,IN_STR('CLUSTER_ID',CLUSTER_ID),IN_STR('CALL_TYPE',IN_QUOTE(CALL_TYPE)),IN_STR('CALL_STATUS',IN_QUOTE(CALL_STATUS))
					,IN_STR('A.INDOOR',INDOOR),IN_STR('A.MOVING',MOVING),IN_STR('CELL_INDOOR',CELL_INDOOR)
					,CASE WHEN KPI_ID IN (110002,110003) THEN IN_STR('A.FREQUENCY',FREQUENCY) WHEN KPI_ID IN (110015,110016) THEN IN_STR('A.EUTRABAND',FREQUENCY) WHEN KPI_ID IN (110017,110018) THEN IN_STR('A.FREQUENCY',FREQUENCY) ELSE IN_STR('A.FREQUENCY',FREQUENCY) END
					,CASE WHEN KPI_ID IN (110002,110003) THEN IN_STR('A.EARFCN',UARFCN) WHEN KPI_ID IN (110015,110016) THEN IN_STR('A.UARFCN',UARFCN) WHEN KPI_ID IN (110017,110018) THEN IN_STR('A.ARFCN',UARFCN) ELSE IN_STR('A.UARFCN',UARFCN) END
					,IN_STR('CELL_LON',CELL_LON),IN_STR('CELL_LAT',CELL_LAT)
					,CASE WHEN TECH_NAME='UMTS' THEN IN_STR('IMEI_NEW',IN_QUOTE(IMEI_NEW)) ELSE IN_STR('IMEI',IN_QUOTE(IMEI_NEW)) END
					,IN_STR('APN',APN)
					,IN_STR('SITE_ID',IN_QUOTE(SITE_ID)),IN_STR('MAKE_ID',MAKE_ID),IN_STR('MODEL_ID',MODEL_ID),IN_STR('POLYGON_STR',POLYGON_STR)
					,CASE WHEN ENODEB_ID <> '' THEN  IN_STR('ENODEB_ID',IN_QUOTE(ENODEB_ID)) ELSE '' END
					,CASE WHEN FILTER='' THEN '' ELSE CONCAT(' AND ',FILTER) END);		
		 END IF;
	ELSE 
		IF (KPI_ID=110001 OR KPI_ID=110005 OR KPI_ID=110014 OR KPI_ID=110020 OR KPI_ID=110021 OR KPI_ID=110025 OR KPI_ID=110026 OR KPI_ID=110027) AND TECH_NAME IN ('LTE','UMTS') THEN
			SET FILTER_STR=CONCAT(IN_STR('DATA_QUARTER',DATA_QUARTER)
					,CASE WHEN POS_KIND='' THEN IN_STR('CELL_ID',CELL_ID) ELSE IN_STR(CONCAT('POS_',POS_KIND,'_CELL'),CELL_ID) END
					,CASE WHEN POS_KIND='' THEN IN_STR('TILE_ID',TILE_ID) ELSE IN_STR(CONCAT('gt_covmo_proj_geohash_to_hex_geohash(POS_',POS_KIND_LOC,'_LOC)'),TILE_ID) END
					,CASE WHEN (IMSI<>'' AND IMSI_STR<> '') THEN IN_STR('A.IMSI',IN_QUOTE(CONCAT(IMSI,',',IMSI_STR))) 
					      WHEN (IMSI<>'') THEN IN_STR('A.IMSI',IN_QUOTE(IMSI))  
					      WHEN (IMSI_STR<>'') THEN IN_STR('A.IMSI',IN_QUOTE(IMSI_STR)) 
					      ELSE ''
					 END
					,IN_STR('CLUSTER_ID',CLUSTER_ID)
					,IN_STR('A.CALL_TYPE',CALL_TYPE)
					,IN_STR('A.CALL_STATUS',IN_QUOTE(CALL_STATUS))
					,IN_STR('A.INDOOR',INDOOR),IN_STR('A.MOVING',MOVING),IN_STR('A.CELL_INDOOR',CELL_INDOOR)
					,IN_STR('FREQUENCY',FREQUENCY),IN_STR('UARFCN',UARFCN),IN_STR('CELL_LON',CELL_LON),IN_STR('CELL_LAT',CELL_LAT)
					,CASE WHEN TECH_NAME='UMTS' THEN IN_STR('IMEI_NEW',IN_QUOTE(IMEI_NEW)) ELSE IN_STR('IMEI',IN_QUOTE(IMEI_NEW)) END
					,IN_STR('APN',APN)
					,IN_STR('SITE_ID',IN_QUOTE(SITE_ID)),IN_STR('MAKE_ID',MAKE_ID),IN_STR('MODEL_ID',MODEL_ID),IN_STR('POLYGON_STR',POLYGON_STR)
					,CASE WHEN FILTER='' THEN '' ELSE CONCAT(' AND ',FILTER) END);
		ELSEIF KPI_ID=110019 THEN 
					SET FILTER_STR=CONCAT(' 1 '
					,CASE WHEN ENODEB_ID <> '' THEN  IN_STR('ENODEB_ID',IN_QUOTE(ENODEB_ID)) ELSE '' END
					,CASE WHEN CELL_ID <> '' THEN  IN_STR('CELL_ID',IN_QUOTE(CELL_ID)) ELSE '' END
					,CASE WHEN SUB_REGION_ID <> '' THEN  IN_STR('SUB_REGION_ID',IN_QUOTE(SUB_REGION_ID)) ELSE ''  END);
		ELSE 
			SET FILTER_STR=CONCAT(CONCAT('A.DATA_HOUR >=',START_HOUR,' AND A.DATA_HOUR<=',END_HOUR),IN_STR('DATA_QUARTER',DATA_QUARTER)
					,CASE WHEN POS_KIND='' THEN IN_STR('CELL_ID',CELL_ID) ELSE IN_STR(CONCAT('POS_',POS_KIND,'_CELL'),CELL_ID) END
					,CASE WHEN POS_KIND='' THEN IN_STR('TILE_ID',TILE_ID) ELSE IN_STR(CONCAT('gt_covmo_proj_geohash_to_hex_geohash(POS_',POS_KIND_LOC,'_LOC)'),TILE_ID) END
					,CASE WHEN (IMSI<>'' AND IMSI_STR<> '') THEN IN_STR('IMSI',IN_QUOTE(CONCAT(IMSI,',',IMSI_STR))) 
					      WHEN (IMSI<>'') THEN IN_STR('IMSI',IN_QUOTE(IMSI))  
					      WHEN (IMSI_STR<>'') THEN IN_STR('IMSI',IN_QUOTE(IMSI_STR)) 
					      ELSE ''
					 END
					,IN_STR('CLUSTER_ID',CLUSTER_ID),IN_STR('CALL_TYPE',IN_QUOTE(CALL_TYPE)),IN_STR('CALL_STATUS',IN_QUOTE(CALL_STATUS))
					,IN_STR('INDOOR',INDOOR),IN_STR('MOVING',MOVING),IN_STR('CELL_INDOOR',CELL_INDOOR)
					,CASE WHEN KPI_ID IN (110002,110003) THEN IN_STR('A.FREQUENCY',FREQUENCY) WHEN KPI_ID IN (110015,110016) THEN IN_STR('A.EUTRABAND',FREQUENCY) WHEN KPI_ID IN (110017,110018) THEN IN_STR('A.FREQUENCY',FREQUENCY) ELSE IN_STR('A.FREQUENCY',FREQUENCY) END
					,CASE WHEN KPI_ID IN (110002,110003) THEN IN_STR('A.EARFCN',UARFCN) WHEN KPI_ID IN (110015,110016) THEN IN_STR('A.UARFCN',UARFCN) WHEN KPI_ID IN (110017,110018) THEN IN_STR('A.ARFCN',UARFCN) ELSE IN_STR('A.UARFCN',UARFCN) END
					,IN_STR('CELL_LON',CELL_LON),IN_STR('CELL_LAT',CELL_LAT)
					,CASE WHEN TECH_NAME='UMTS' THEN IN_STR('IMEI_NEW',IN_QUOTE(IMEI_NEW)) ELSE IN_STR('IMEI',IN_QUOTE(IMEI_NEW)) END
					,IN_STR('APN',APN)
					,IN_STR('SITE_ID',IN_QUOTE(SITE_ID)),IN_STR('MAKE_ID',MAKE_ID),IN_STR('MODEL_ID',MODEL_ID),IN_STR('POLYGON_STR',POLYGON_STR)
					,CASE WHEN ENODEB_ID <> '' THEN  IN_STR('ENODEB_ID',IN_QUOTE(ENODEB_ID)) ELSE '' END
					,CASE WHEN FILTER='' THEN '' ELSE CONCAT(' AND ',FILTER) END);
		END IF;
	END IF;	
	
	SET @TECHNOLOGY=TECH_NAME;
	IF TECH_NAME<>'LTE' THEN 
		SET FILTER_STR=CONCAT(FILTER_STR,IN_STR('MSISDN',IN_QUOTE(MSISDN)));
	END IF; 	
	INSERT INTO gt_gw_main.SP_LOG VALUES(GT_DB,'SP_KPI_multi_remote','Start', NOW());
	IF @TECHNOLOGY='UMTS' THEN 
	BEGIN
		IF  KPI_ID=110001 AND SPECIAL_IMSI<>1  THEN 
			SET @v_i=START_HOUR;
			SET @v_i_Max=END_HOUR;
			SET @v_j=0;
			WHILE @v_i <= @v_i_Max DO
			BEGIN	
				WHILE @v_j<60  DO
				BEGIN		
					IF (@v_i=@v_i_Max AND @v_j=45) THEN 
						SET @UNION=' '; 
					ELSE
						SET @UNION=' UNION ALL '; 
					END IF;	
					SET STR_SEL_IMSI_UMTS=CONCAT(STR_SEL_IMSI_UMTS,'SELECT ',STR_IMSI_UMTS,' FROM ',GT_DB,'.',TABLE_CALL_IMSI_UMTS,'',CONCAT('_',LPAD(@v_i,2,'0'),LPAD(@v_j,2,'0')),' A ',CASE WHEN (IMSI<>'' OR IMSI_STR<> '') THEN ' FORCE INDEX (IMSI)' ELSE '' END,' WHERE DATA_HOUR=', @v_i,FILTER_STR,CASE WHEN WITHDUMP=0 THEN ' AND A.`POS_LAST_TILE`>0 ' ELSE ' ' END,@UNION); 
					SET @v_j=@v_j+15;
				END;
				END WHILE;					
				SET @v_j=0;
				SET @v_i=@v_i+1;
			END;
			END WHILE;	
			IF WITHDUMP=1 AND SPECIAL_IMSI<>1 THEN 
				SET STR_SEL_IMSI_UMTS=CONCAT(STR_SEL_IMSI_UMTS,' UNION ALL SELECT ',STR_IMSI_WITHDUMP_UMTS,' FROM ',GT_DB,'.table_dump_call A ',CASE WHEN (IMSI<>'' OR IMSI_STR<> '') THEN ' FORCE INDEX (IMSI)' ELSE '' END,' WHERE DATA_HOUR>=',START_HOUR,' AND DATA_HOUR<=',END_HOUR,FILTER_STR);
			END IF;
			SET @SqlCmd=STR_SEL_IMSI_UMTS;		
		ELSEIF KPI_ID=110001 AND SPECIAL_IMSI=1  THEN 
			SET @v_i=START_HOUR;
			SET @v_i_Max=END_HOUR;
			SET @v_j=0;
			WHILE @v_i <= @v_i_Max DO
			BEGIN	
				WHILE @v_j<60  DO
				BEGIN		
					IF (@v_i=@v_i_Max AND @v_j=45) THEN 
						SET @UNION=' '; 
					ELSE
						SET @UNION=' UNION ALL '; 
					END IF;	
					SET STR_SEL_IMSI_UMTS=CONCAT(STR_SEL_IMSI_UMTS,'SELECT ',STR_IMSI_UMTS,' FROM ',GT_DB,'.',TABLE_CALL_IMSI_UMTS,'',CONCAT('_',LPAD(@v_i,2,'0'),LPAD(@v_j,2,'0'),'_imsig',ABS(IMSI_GID)),' A ',CASE WHEN (IMSI<>'' OR IMSI_STR<> '') THEN ' FORCE INDEX (IMSI)' ELSE '' END,' WHERE DATA_HOUR=', @v_i,FILTER_STR,@UNION); 
					SET @v_j=@v_j+15;
				END;
				END WHILE;					
				SET @v_j=0;
				SET @v_i=@v_i+1;
			END;
			END WHILE;
			SET @SqlCmd=STR_SEL_IMSI_UMTS;
		END IF;	
		IF KPI_ID=110005 AND SPECIAL_IMSI<>1 THEN 
			SET @v_i=START_HOUR;
			SET @v_i_Max=END_HOUR;
			SET @v_j=0;
			WHILE @v_i <= @v_i_Max DO
			BEGIN	
				WHILE @v_j<60  DO
				BEGIN		
					IF (@v_i=@v_i_Max AND @v_j=45) THEN 
						SET @UNION=' '; 
					ELSE
						SET @UNION=' UNION ALL '; 
					END IF;	
					SET STR_SEL_IMSI_GRP_UMTS=CONCAT(STR_SEL_IMSI_GRP_UMTS,'SELECT ',STR_IMSI_GRP_UMTS,' FROM ',GT_DB,'.',TABLE_CALL_IMSI_UMTS,'',CONCAT('_',LPAD(@v_i,2,'0'),LPAD(@v_j,2,'0')),' A ',CASE WHEN (IMSI<>'' OR IMSI_STR<> '') THEN ' FORCE INDEX (IMSI)' ELSE '' END,' WHERE DATA_HOUR=', @v_i,FILTER_STR,CASE WHEN WITHDUMP=0 THEN ' AND A.`POS_LAST_TILE`>0 ' ELSE ' ' END,' GROUP BY IMSI ',@UNION); 
					SET @v_j=@v_j+15;
				END;
				END WHILE;
				SET @v_j=0;
				SET @v_i=@v_i+1;
			END;
			END WHILE;	
			IF WITHDUMP=1 AND SPECIAL_IMSI<>1 THEN 
				SET STR_SEL_IMSI_GRP_UMTS=CONCAT(STR_SEL_IMSI_GRP_UMTS,' UNION ALL SELECT ',STR_IMSI_GRP_WITHDUMP_UMTS,' FROM ',GT_DB,'.table_dump_call A ',CASE WHEN (IMSI<>'' OR IMSI_STR<> '') THEN ' FORCE INDEX (IMSI)' ELSE '' END,' WHERE DATA_HOUR>=',START_HOUR,' AND DATA_HOUR<=',END_HOUR,FILTER_STR,' GROUP BY IMSI ');
			END IF;
			SET @SqlCmd=CONCAT('
				SELECT  IMSI AS `IMSI`
					,DATA_DATE AS `DATA_DATE`
					,MAX(B.`HANDSET`) AS `HANDSET`
					,B.IMSI AS `MSISDN`
					,SUM(B.`Total_Call_Count`) AS `Total_Call_Count`
					,SUM(B.`Drop_Call_Count`) AS `Drop_Call_Count`
					,SUM(B.`Block_Call_Count`) AS `Block_Call_Count`
					,SUM(B.`Total_DL_Data_Volume`) AS `Total_DL_Data_Volume` 
					,SUM(B.`Total_UL_Data_Volume`) AS `Total_UL_Data_Volume`					
					,MAX(B.`MAX_DL_THROUGHPUT`) AS `MAX_DL_THROUGHPUT`				
					,MAX(B.`MAX_UL_THROUGHPUT`) AS `MAX_UL_THROUGHPUT`	
					,SUM(B.`UMTS_Call_Drop_Rate_1`) AS `UMTS_Call_Drop_Rate_1`
					,SUM(B.`UMTS_Call_Drop_Rate_2`) AS `UMTS_Call_Drop_Rate_2`
					,SUM(B.`UMTS_CSSR_1`) AS `UMTS_CSSR_1`
					,SUM(B.`UMTS_CSSR_2`) AS `UMTS_CSSR_2`
					,SUM(B.`UMTS_Blocked_Call_Rate_1`) AS `UMTS_Blocked_Call_Rate_1`
					,SUM(B.`UMTS_Blocked_Call_Rate_2`) AS `UMTS_Blocked_Call_Rate_2`
					,SUM(B.`UMTS_Soft_Handover_SR_1`) AS `UMTS_Soft_Handover_SR_1`
					,SUM(B.`UMTS_Soft_Handover_SR_2`) AS `UMTS_Soft_Handover_SR_2`
					,SUM(B.`UMTS_Softer_handover_SR_1`) AS `UMTS_Softer_handover_SR_1`
					,SUM(B.`UMTS_Softer_handover_SR_2`) AS `UMTS_Softer_handover_SR_2`
					,SUM(B.`UMTS_Inter_Freq_Handover_SR_1`)AS `UMTS_Inter_Freq_Handover_SR_1`
					,SUM(B.`UMTS_Inter_Freq_Handover_SR_2`) AS `UMTS_Inter_Freq_Handover_SR_2`
					,SUM(B.`UMTS_IRAT_handover_SR_1`) AS `UMTS_IRAT_handover_SR_1`
					,SUM(B.`UMTS_IRAT_handover_SR_2`) AS `UMTS_IRAT_handover_SR_2`
					,NULL AS `LTE_Call_Drop_Rate_1`
					,NULL AS `LTE_Call_Drop_Rate_2`
					,NULL AS `LTE_CSSR_1`
					,NULL AS `LTE_CSSR_2`
					,NULL AS `LTE_Blocked_Call_Rate_1` 
					,NULL AS `LTE_Blocked_Call_Rate_2`
					,NULL AS `LTE_Inter_Feq_Handover_SR_1`
					,NULL AS `LTE_Inter_Feq_Handover_SR_2`
					,NULL AS `LTE_Intra_Feq_Handover_SR_1`
					,NULL AS `LTE_Intra_Feq_Handover_SR_2`
					,NULL AS `LTE_IRAT_Handover_SR_3G_1`
					,NULL AS `LTE_IRAT_Handover_SR_3G_2`
					,NULL AS `LTE_IRAT_Handover_SR_2G_1`
					,NULL AS `LTE_IRAT_Handover_SR_2G_2`
					,NULL AS `GSM_Call_Drop_Rate_1`
					,NULL AS `GSM_Call_Drop_Rate_2`
					,NULL AS `GSM_CSSR_1`
					,NULL AS `GSM_CSSR_2`
					,NULL AS `GSM_Blocked_Call_Rate_1`
					,NULL AS `GSM_Blocked_Call_Rate_2`
					,NULL AS `GSM_Inter_Freq_Handover_SR_1`
					,NULL AS `GSM_Inter_Freq_Handover_SR_2`
					,',PU_ID,' AS PU
					,DATA_DATE AS ds_date
					,2 AS `TECH_MASK`
					,POS_FIRST_LOC AS POS_FIRST_LOC
					,POS_LAST_LOC AS POS_LAST_LOC
					,POS_FIRST_RSCP_SUM AS POS_FIRST_RSCP_SUM
					,POS_FIRST_RSCP_CNT AS POS_FIRST_RSCP_CNT
					,POS_FIRST_ECN0_SUM AS POS_FIRST_ECN0_SUM
					,POS_FIRST_ECN0_CNT AS POS_FIRST_ECN0_CNT
					,POS_LAST_RSCP_SUM AS POS_LAST_RSCP_SUM
					,POS_LAST_RSCP_CNT AS POS_LAST_RSCP_CNT
					,POS_LAST_ECN0_SUM AS POS_LAST_ECN0_SUM
					,POS_LAST_ECN0_CNT AS POS_LAST_ECN0_CNT
					,NULL AS POS_FIRST_S_RSRP_SUM
					,NULL AS POS_FIRST_S_RSRP_CNT
					,NULL AS POS_FIRST_S_RSRQ_SUM
					,NULL AS POS_FIRST_S_RSRQ_CNT
					,NULL AS POS_LAST_S_RSRP_SUM
					,NULL AS POS_LAST_S_RSRP_CNT
					,NULL AS POS_LAST_S_RSRQ_SUM
					,NULL AS POS_LAST_S_RSRQ_CNT
				FROM (',STR_SEL_IMSI_GRP_UMTS,') B
				WHERE IMSI IS NOT NULL
				GROUP BY IMSI;'); 
		ELSEIF KPI_ID=110005 AND SPECIAL_IMSI=1  THEN 
			SET @v_i=START_HOUR;
			SET @v_i_Max=END_HOUR;
			SET @v_j=0;
			WHILE @v_i <= @v_i_Max DO
			BEGIN	
				WHILE @v_j<60  DO
				BEGIN		
					IF (@v_i=@v_i_Max AND @v_j=45) THEN 
						SET @UNION=' '; 
					ELSE
						SET @UNION=' UNION ALL '; 
					END IF;	
					SET STR_SEL_IMSI_GRP_UMTS=CONCAT(STR_SEL_IMSI_GRP_UMTS,'SELECT ',STR_IMSI_GRP_UMTS,' FROM ',GT_DB,'.',TABLE_CALL_IMSI_UMTS,'',CONCAT('_',LPAD(@v_i,2,'0'),LPAD(@v_j,2,'0'),'_imsig',ABS(IMSI_GID)), ' A ',CASE WHEN (IMSI<>'' OR IMSI_STR<> '') THEN ' FORCE INDEX (IMSI)' ELSE '' END,' WHERE DATA_HOUR=', @v_i,FILTER_STR,' GROUP BY IMSI ',@UNION); 
					SET @v_j=@v_j+15;
				END;
				END WHILE;
				SET @v_j=0;
				SET @v_i=@v_i+1;
			END;
			END WHILE;
			SET @SqlCmd=CONCAT('
				SELECT  IMSI AS `IMSI`
					,DATA_DATE AS `DATA_DATE`
					,MAX(B.`HANDSET`) AS `HANDSET`
					,B.IMSI AS `MSISDN`
					,SUM(B.`Total_Call_Count`) AS `Total_Call_Count`
					,SUM(B.`Drop_Call_Count`) AS `Drop_Call_Count`
					,SUM(B.`Block_Call_Count`) AS `Block_Call_Count`
					,SUM(B.`Total_DL_Data_Volume`) AS `Total_DL_Data_Volume` 
					,SUM(B.`Total_UL_Data_Volume`) AS `Total_UL_Data_Volume`					
					,MAX(B.`MAX_DL_THROUGHPUT`) AS `MAX_DL_THROUGHPUT`				
					,MAX(B.`MAX_UL_THROUGHPUT`) AS `MAX_UL_THROUGHPUT`	
					,SUM(B.`UMTS_Call_Drop_Rate_1`) AS `UMTS_Call_Drop_Rate_1`
					,SUM(B.`UMTS_Call_Drop_Rate_2`) AS `UMTS_Call_Drop_Rate_2`
					,SUM(B.`UMTS_CSSR_1`) AS `UMTS_CSSR_1`
					,SUM(B.`UMTS_CSSR_2`) AS `UMTS_CSSR_2`
					,SUM(B.`UMTS_Blocked_Call_Rate_1`) AS `UMTS_Blocked_Call_Rate_1`
					,SUM(B.`UMTS_Blocked_Call_Rate_2`) AS `UMTS_Blocked_Call_Rate_2`
					,SUM(B.`UMTS_Soft_Handover_SR_1`) AS `UMTS_Soft_Handover_SR_1`
					,SUM(B.`UMTS_Soft_Handover_SR_2`) AS `UMTS_Soft_Handover_SR_2`
					,SUM(B.`UMTS_Softer_handover_SR_1`) AS `UMTS_Softer_handover_SR_1`
					,SUM(B.`UMTS_Softer_handover_SR_2`) AS `UMTS_Softer_handover_SR_2`
					,SUM(B.`UMTS_Inter_Freq_Handover_SR_1`)AS `UMTS_Inter_Freq_Handover_SR_1`
					,SUM(B.`UMTS_Inter_Freq_Handover_SR_2`) AS `UMTS_Inter_Freq_Handover_SR_2`
					,SUM(B.`UMTS_IRAT_handover_SR_1`) AS `UMTS_IRAT_handover_SR_1`
					,SUM(B.`UMTS_IRAT_handover_SR_2`) AS `UMTS_IRAT_handover_SR_2`
					,NULL AS `LTE_Call_Drop_Rate_1`
					,NULL AS `LTE_Call_Drop_Rate_2`
					,NULL AS `LTE_CSSR_1`
					,NULL AS `LTE_CSSR_2`
					,NULL AS `LTE_Blocked_Call_Rate_1` 
					,NULL AS `LTE_Blocked_Call_Rate_2`
					,NULL AS `LTE_Inter_Feq_Handover_SR_1`
					,NULL AS `LTE_Inter_Feq_Handover_SR_2`
					,NULL AS `LTE_Intra_Feq_Handover_SR_1`
					,NULL AS `LTE_Intra_Feq_Handover_SR_2`
					,NULL AS `LTE_IRAT_Handover_SR_3G_1`
					,NULL AS `LTE_IRAT_Handover_SR_3G_2`
					,NULL AS `LTE_IRAT_Handover_SR_2G_1`
					,NULL AS `LTE_IRAT_Handover_SR_2G_2`
					,NULL AS `GSM_Call_Drop_Rate_1`
					,NULL AS `GSM_Call_Drop_Rate_2`
					,NULL AS `GSM_CSSR_1`
					,NULL AS `GSM_CSSR_2`
					,NULL AS `GSM_Blocked_Call_Rate_1`
					,NULL AS `GSM_Blocked_Call_Rate_2`
					,NULL AS `GSM_Inter_Freq_Handover_SR_1`
					,NULL AS `GSM_Inter_Freq_Handover_SR_2`
					,',PU_ID,' AS PU
					,DATA_DATE AS ds_date
					,2 AS `TECH_MASK`
					,POS_FIRST_LOC AS POS_FIRST_LOC
					,POS_LAST_LOC AS POS_LAST_LOC
					,POS_FIRST_RSCP_SUM AS POS_FIRST_RSCP_SUM
					,POS_FIRST_RSCP_CNT AS POS_FIRST_RSCP_CNT
					,POS_FIRST_ECN0_SUM AS POS_FIRST_ECN0_SUM
					,POS_FIRST_ECN0_CNT AS POS_FIRST_ECN0_CNT
					,POS_LAST_RSCP_SUM AS POS_LAST_RSCP_SUM
					,POS_LAST_RSCP_CNT AS POS_LAST_RSCP_CNT
					,POS_LAST_ECN0_SUM AS POS_LAST_ECN0_SUM
					,POS_LAST_ECN0_CNT AS POS_LAST_ECN0_CNT
					,NULL AS POS_FIRST_S_RSRP_SUM
					,NULL AS POS_FIRST_S_RSRP_CNT
					,NULL AS POS_FIRST_S_RSRQ_SUM
					,NULL AS POS_FIRST_S_RSRQ_CNT
					,NULL AS POS_LAST_S_RSRP_SUM
					,NULL AS POS_LAST_S_RSRP_CNT
					,NULL AS POS_LAST_S_RSRQ_SUM
					,NULL AS POS_LAST_S_RSRQ_CNT
				FROM (',STR_SEL_IMSI_GRP_UMTS,') B
				WHERE IMSI IS NOT NULL
				GROUP BY IMSI;'); 
		END IF;
		IF KPI_ID=110020 AND SPECIAL_IMSI<>1 THEN 
			SET @v_i=START_HOUR;
			SET @v_i_Max=END_HOUR;
			SET @v_j=0;
			WHILE @v_i <= @v_i_Max DO
			BEGIN	
				WHILE @v_j<60  DO
				BEGIN		
					IF (@v_i=@v_i_Max AND @v_j=45) THEN 
						SET @UNION=' '; 
					ELSE
						SET @UNION=' UNION ALL '; 
					END IF;	
					SET STR_SEL_IMSI_GRP_UMTS=CONCAT(STR_SEL_IMSI_GRP_UMTS,'SELECT ',STR_IMSI_GRP_UMTS,' FROM ',GT_DB,'.',TABLE_CALL_IMSI_UMTS,'',CONCAT('_',LPAD(@v_i,2,'0'),LPAD(@v_j,2,'0')),' A ',CASE WHEN (IMSI<>'' OR IMSI_STR<> '') THEN ' FORCE INDEX (IMSI)' ELSE '' END,' WHERE DATA_HOUR=', @v_i,FILTER_STR,CASE WHEN WITHDUMP=0 THEN ' AND A.`POS_LAST_TILE`>0 ' ELSE ' ' END,' GROUP BY IMSI ',@UNION); 
					SET @v_j=@v_j+15;
				END;
				END WHILE;
				SET @v_j=0;
				SET @v_i=@v_i+1;
			END;
			END WHILE;	
			IF WITHDUMP=1 AND SPECIAL_IMSI<>1 THEN 
				SET STR_SEL_IMSI_GRP_UMTS=CONCAT(STR_SEL_IMSI_GRP_UMTS,' UNION ALL SELECT ',STR_IMSI_GRP_WITHDUMP_UMTS,' FROM ',GT_DB,'.table_dump_call A ',CASE WHEN (IMSI<>'' OR IMSI_STR<> '') THEN ' FORCE INDEX (IMSI)' ELSE '' END,' WHERE DATA_HOUR>=',START_HOUR,' AND DATA_HOUR<=',END_HOUR,FILTER_STR,' GROUP BY IMSI ');
			END IF;
			SET @SqlCmd=CONCAT('
				SELECT  IMSI AS `IMSI`
					,DATA_DATE AS `DATA_DATE`
					,MAX(B.`HANDSET`) AS `HANDSET`
					,B.IMSI AS `MSISDN`
					,SUM(B.`Total_Call_Count`) AS `Total_Call_Count`
					,SUM(B.`Drop_Call_Count`) AS `Drop_Call_Count`
					,SUM(B.`Block_Call_Count`) AS `Block_Call_Count`
					,SUM(B.`Total_DL_Data_Volume`) AS `Total_DL_Data_Volume` 
					,SUM(B.`Total_UL_Data_Volume`) AS `Total_UL_Data_Volume`					
					,MAX(B.`MAX_DL_THROUGHPUT`) AS `MAX_DL_THROUGHPUT`				
					,MAX(B.`MAX_UL_THROUGHPUT`) AS `MAX_UL_THROUGHPUT`	
					,SUM(B.`UMTS_Call_Drop_Rate_1`) AS `UMTS_Call_Drop_Rate_1`
					,SUM(B.`UMTS_Call_Drop_Rate_2`) AS `UMTS_Call_Drop_Rate_2`
					,SUM(B.`UMTS_CSSR_1`) AS `UMTS_CSSR_1`
					,SUM(B.`UMTS_CSSR_2`) AS `UMTS_CSSR_2`
					,SUM(B.`UMTS_Blocked_Call_Rate_1`) AS `UMTS_Blocked_Call_Rate_1`
					,SUM(B.`UMTS_Blocked_Call_Rate_2`) AS `UMTS_Blocked_Call_Rate_2`
					,SUM(B.`UMTS_Soft_Handover_SR_1`) AS `UMTS_Soft_Handover_SR_1`
					,SUM(B.`UMTS_Soft_Handover_SR_2`) AS `UMTS_Soft_Handover_SR_2`
					,SUM(B.`UMTS_Softer_handover_SR_1`) AS `UMTS_Softer_handover_SR_1`
					,SUM(B.`UMTS_Softer_handover_SR_2`) AS `UMTS_Softer_handover_SR_2`
					,SUM(B.`UMTS_Inter_Freq_Handover_SR_1`)AS `UMTS_Inter_Freq_Handover_SR_1`
					,SUM(B.`UMTS_Inter_Freq_Handover_SR_2`) AS `UMTS_Inter_Freq_Handover_SR_2`
					,SUM(B.`UMTS_IRAT_handover_SR_1`) AS `UMTS_IRAT_handover_SR_1`
					,SUM(B.`UMTS_IRAT_handover_SR_2`) AS `UMTS_IRAT_handover_SR_2`
					,NULL AS `LTE_Call_Drop_Rate_1`
					,NULL AS `LTE_Call_Drop_Rate_2`
					,NULL AS `LTE_CSSR_1`
					,NULL AS `LTE_CSSR_2`
					,NULL AS `LTE_Blocked_Call_Rate_1` 
					,NULL AS `LTE_Blocked_Call_Rate_2`
					,NULL AS `LTE_Inter_Feq_Handover_SR_1`
					,NULL AS `LTE_Inter_Feq_Handover_SR_2`
					,NULL AS `LTE_Intra_Feq_Handover_SR_1`
					,NULL AS `LTE_Intra_Feq_Handover_SR_2`
					,NULL AS `LTE_IRAT_Handover_SR_3G_1`
					,NULL AS `LTE_IRAT_Handover_SR_3G_2`
					,NULL AS `LTE_IRAT_Handover_SR_2G_1`
					,NULL AS `LTE_IRAT_Handover_SR_2G_2`
					,NULL AS `GSM_Call_Drop_Rate_1`
					,NULL AS `GSM_Call_Drop_Rate_2`
					,NULL AS `GSM_CSSR_1`
					,NULL AS `GSM_CSSR_2`
					,NULL AS `GSM_Blocked_Call_Rate_1`
					,NULL AS `GSM_Blocked_Call_Rate_2`
					,NULL AS `GSM_Inter_Freq_Handover_SR_1`
					,NULL AS `GSM_Inter_Freq_Handover_SR_2`
					,',PU_ID,' AS PU
					,DATA_DATE AS ds_date
					,2 AS `TECH_MASK`
					,POS_FIRST_LOC AS POS_FIRST_LOC
					,POS_LAST_LOC AS POS_LAST_LOC
				FROM (',STR_SEL_IMSI_GRP_UMTS,') B
				WHERE IMSI IS NOT NULL
				GROUP BY IMSI;'); 
		ELSEIF KPI_ID=110020 AND SPECIAL_IMSI=1  THEN 			
			SET @v_i=START_HOUR;
			SET @v_i_Max=END_HOUR;
			SET @v_j=0;						
			WHILE @v_i <= @v_i_Max DO
			BEGIN	
				WHILE @v_j<60  DO
				BEGIN		
					IF (@v_i=@v_i_Max AND @v_j=45) THEN 
						SET @UNION=' '; 
					ELSE
						SET @UNION=' UNION ALL '; 
					END IF;	
					SET STR_SEL_IMSI_GRP_UMTS=CONCAT(STR_SEL_IMSI_GRP_UMTS,'SELECT ',STR_IMSI_GRP_UMTS,' FROM ',GT_DB,'.',TABLE_CALL_IMSI_UMTS,'',CONCAT('_',LPAD(@v_i,2,'0'),LPAD(@v_j,2,'0'),'_imsig',ABS(IMSI_GID)), ' A ',CASE WHEN (IMSI<>'' OR IMSI_STR<> '') THEN ' FORCE INDEX (IMSI)' ELSE '' END,' WHERE DATA_HOUR=', @v_i,FILTER_STR,' GROUP BY IMSI ',@UNION); 
					SET @v_j=@v_j+15;
				END;
				END WHILE;					
				SET @v_j=0;
				SET @v_i=@v_i+1;
			END;
			END WHILE;	
			SET @SqlCmd=CONCAT('
				SELECT  IMSI AS `IMSI`
					,DATA_DATE AS `DATA_DATE`
					,MAX(B.`HANDSET`) AS `HANDSET`
					,B.IMSI AS `MSISDN`
					,SUM(B.`Total_Call_Count`) AS `Total_Call_Count`
					,SUM(B.`Drop_Call_Count`) AS `Drop_Call_Count`
					,SUM(B.`Block_Call_Count`) AS `Block_Call_Count`
					,SUM(B.`Total_DL_Data_Volume`) AS `Total_DL_Data_Volume` 
					,SUM(B.`Total_UL_Data_Volume`) AS `Total_UL_Data_Volume`					
					,MAX(B.`MAX_DL_THROUGHPUT`) AS `MAX_DL_THROUGHPUT`				
					,MAX(B.`MAX_UL_THROUGHPUT`) AS `MAX_UL_THROUGHPUT`	
					,SUM(B.`UMTS_Call_Drop_Rate_1`) AS `UMTS_Call_Drop_Rate_1`
					,SUM(B.`UMTS_Call_Drop_Rate_2`) AS `UMTS_Call_Drop_Rate_2`
					,SUM(B.`UMTS_CSSR_1`) AS `UMTS_CSSR_1`
					,SUM(B.`UMTS_CSSR_2`) AS `UMTS_CSSR_2`
					,SUM(B.`UMTS_Blocked_Call_Rate_1`) AS `UMTS_Blocked_Call_Rate_1`
					,SUM(B.`UMTS_Blocked_Call_Rate_2`) AS `UMTS_Blocked_Call_Rate_2`
					,SUM(B.`UMTS_Soft_Handover_SR_1`) AS `UMTS_Soft_Handover_SR_1`
					,SUM(B.`UMTS_Soft_Handover_SR_2`) AS `UMTS_Soft_Handover_SR_2`
					,SUM(B.`UMTS_Softer_handover_SR_1`) AS `UMTS_Softer_handover_SR_1`
					,SUM(B.`UMTS_Softer_handover_SR_2`) AS `UMTS_Softer_handover_SR_2`
					,SUM(B.`UMTS_Inter_Freq_Handover_SR_1`)AS `UMTS_Inter_Freq_Handover_SR_1`
					,SUM(B.`UMTS_Inter_Freq_Handover_SR_2`) AS `UMTS_Inter_Freq_Handover_SR_2`
					,SUM(B.`UMTS_IRAT_handover_SR_1`) AS `UMTS_IRAT_handover_SR_1`
					,SUM(B.`UMTS_IRAT_handover_SR_2`) AS `UMTS_IRAT_handover_SR_2`
					,NULL AS `LTE_Call_Drop_Rate_1`
					,NULL AS `LTE_Call_Drop_Rate_2`
					,NULL AS `LTE_CSSR_1`
					,NULL AS `LTE_CSSR_2`
					,NULL AS `LTE_Blocked_Call_Rate_1` 
					,NULL AS `LTE_Blocked_Call_Rate_2`
					,NULL AS `LTE_Inter_Feq_Handover_SR_1`
					,NULL AS `LTE_Inter_Feq_Handover_SR_2`
					,NULL AS `LTE_Intra_Feq_Handover_SR_1`
					,NULL AS `LTE_Intra_Feq_Handover_SR_2`
					,NULL AS `LTE_IRAT_Handover_SR_3G_1`
					,NULL AS `LTE_IRAT_Handover_SR_3G_2`
					,NULL AS `LTE_IRAT_Handover_SR_2G_1`
					,NULL AS `LTE_IRAT_Handover_SR_2G_2`
					,NULL AS `GSM_Call_Drop_Rate_1`
					,NULL AS `GSM_Call_Drop_Rate_2`
					,NULL AS `GSM_CSSR_1`
					,NULL AS `GSM_CSSR_2`
					,NULL AS `GSM_Blocked_Call_Rate_1`
					,NULL AS `GSM_Blocked_Call_Rate_2`
					,NULL AS `GSM_Inter_Freq_Handover_SR_1`
					,NULL AS `GSM_Inter_Freq_Handover_SR_2`
					,',PU_ID,' AS PU
					,DATA_DATE AS ds_date
					,2 AS `TECH_MASK`
					,POS_FIRST_LOC AS POS_FIRST_LOC
					,POS_LAST_LOC AS POS_LAST_LOC
				FROM (',STR_SEL_IMSI_GRP_UMTS,') B
				WHERE IMSI IS NOT NULL
				GROUP BY IMSI;'); 
		END IF;	
		IF KPI_ID=110014 AND SPECIAL_IMSI<>1 THEN 			
			SET @v_i=START_HOUR;
			SET @v_i_Max=END_HOUR;
			SET @v_j=0;						
			WHILE @v_i <= @v_i_Max DO
			BEGIN	
				WHILE @v_j<60  DO
				BEGIN		
					IF (@v_i=@v_i_Max AND @v_j=45) THEN 
						SET @UNION=' '; 
					ELSE
						SET @UNION=' UNION ALL '; 
					END IF;	
					SET STR_SEL_IMSI_POS_UMTS=CONCAT(STR_SEL_IMSI_POS_UMTS,'SELECT ',STR_IMSI_POS_UMTS,' FROM ',GT_DB,'.table_call',CONCAT('_',LPAD(@v_i,2,'0'),LPAD(@v_j,2,'0')),' A WHERE A.DATA_HOUR=', @v_i,FILTER_STR,@UNION); 				
					SET @v_j=@v_j+15;
				END;
				END WHILE;				
				SET @v_j=0;
				SET @v_i=@v_i+1;
			END;
			END WHILE;	
			SET @SqlCmd=STR_SEL_IMSI_POS_UMTS;		
		END IF;	
		IF KPI_ID=110014 AND SPECIAL_IMSI=1 THEN 			
			SET @v_i=START_HOUR;
			SET @v_i_Max=END_HOUR;
			SET @v_j=0;						
			WHILE @v_i <= @v_i_Max DO
			BEGIN	
				WHILE @v_j<60  DO
				BEGIN		
					IF (@v_i=@v_i_Max AND @v_j=45) THEN 
						SET @UNION=' '; 
					ELSE
						SET @UNION=' UNION ALL '; 
					END IF;	
					SET STR_SEL_IMSI_POS_UMTS=CONCAT(STR_SEL_IMSI_POS_UMTS,'SELECT ',STR_IMSI_POS_UMTS,' FROM ',GT_DB,'.table_call',CONCAT('_',LPAD(@v_i,2,'0'),LPAD(@v_j,2,'0')),' A WHERE A.DATA_HOUR=', @v_i,FILTER_STR,@UNION); 				
					SET @v_j=@v_j+15;
				END;
				END WHILE;				
				SET @v_j=0;
				SET @v_i=@v_i+1;
			END;
			END WHILE;	
			SET @SqlCmd=STR_SEL_IMSI_POS_UMTS;		
		END IF;
		IF KPI_ID=110002 THEN 
			SET @SqlCmd=CONCAT('
						SELECT 
						 nt_tile.rnc_id AS RNC_ID,
						 nt_tile.cell_id AS CELL_ID,
						 nt_tile.CELL_NAME AS CELL_NAME,
						 nt_tile.ACTIVE_STATUS AS ACTIVE_STATUS,
						 IFNULL(nt_tile.CS_CALL_DURA,0) AS CS_CALL_DURA,
						 IFNULL(nt_tile.DL_DATA_THRU,0) AS DL_DATA_THRU,
						 IFNULL(nt_tile.UL_DATA_THRU,0) AS UL_DATA_THRU,
						 IFNULL(nt_tile.CALL_CNT,0) AS CALL_CNT,
						 nt_tile.Administrative_state,
						 nt_tile.OPERSTATE_ENABLE,	
						',CASE WHEN PM_COUNTER_FLAG = 'true' THEN
							 ' pm.Downtimeman,
							   pm.Downtimeauto, '
						ELSE
							' 0 AS Downtimeman,
							  0 AS Downtimeauto, '
						 END ,'
						 nt_tile.ds_date,
						 ',PU_ID,' AS pu,
						 2 AS TECH_MASK 
						FROM
						(	SELECT 
							  nt.rnc_id AS RNC_ID,
							  nt.cell_id AS CELL_ID,
							  nt.CELL_NAME AS CELL_NAME,
							  nt.ACTIVE_STATE AS ACTIVE_STATUS,
							  IFNULL(dat.cs,0) AS CS_CALL_DURA,
							  IFNULL(dat.dl,0) AS DL_DATA_THRU,
							  IFNULL(dat.ul,0) AS UL_DATA_THRU,
							  IFNULL(dat.cc,0) AS CALL_CNT,
							(CASE WHEN nt.CM_ADMIN_STATE=1 THEN ''Locked''
								WHEN nt.CM_ADMIN_STATE=0 THEN ''Unlocked''
								ELSE '''' END) AS Administrative_state,
							(CASE WHEN nt.CM_OPERATION_STATE=1 THEN ''Enable''
								WHEN nt.CM_OPERATION_STATE=0 THEN ''Disable''
								ELSE '''' END) AS OPERSTATE_ENABLE,
							ds_date,
							 ',PU_ID,' AS pu,
							2 AS TECH_MASK 
							FROM ',NT_DB,'.nt2_cell_umts nt						
							LEFT JOIN
							(
								SELECT cell_id,rnc_id,CELL_NAME,
								DATA_DATE AS ds_date,
									IFNULL(SUM(IF(CALL_TYPE IN (10,11),erlang,0)),0) AS cs,
									IFNULL(SUM(IF(CALL_TYPE IN (12,13,14,18),DL_TRAFFIC,0)),0) AS dl,
									IFNULL(SUM(IF(CALL_TYPE IN (12,13,14,18),UL_TRAFFIC,0)),0) AS ul,
									IFNULL(SUM(CALL_CNT),0) AS cc
								FROM ',GT_DB,'.',CASE WHEN DY_FLAG=1 THEN 'table_tile_start_dy_c A' ELSE 'table_tile_start_c A' END,' 
								WHERE ',CASE WHEN FILTER_STR='' OR FILTER_STR IS NULL THEN '' ELSE CONCAT(FILTER_STR,' AND ') END,' A.rnc_id=',PU_ID,' 
								GROUP BY a.cell_id,a.rnc_id
								HAVING cs=0 AND (dl=0 OR ul=0)
							) dat 
							ON dat.cell_id = nt.cell_id AND dat.rnc_id = nt.rnc_id 
							WHERE nt.rnc_id=',PU_ID,' 
							AND (cs=0)
						) nt_tile	'
						,CASE WHEN PM_COUNTER_FLAG = 'true' THEN 
							CONCAT(' LEFT JOIN 
							(
								SELECT cell_id,rnc_id,pmCellDowntimeMan AS Downtimeman,pmCellDowntimeAuto AS Downtimeauto
								FROM ',GT_DB,'.table_pm_ericsson_umts_aggr A 
								WHERE A.rnc_id=',PU_ID,' 
								GROUP BY a.cell_id,a.rnc_id 						
							) pm
							ON pm.cell_id = nt_tile.cell_id AND pm.rnc_id = nt_tile.rnc_id ' )
						ELSE '' END ,'
						WHERE nt_tile.rnc_id=',PU_ID,' ;');
		END IF;
		IF KPI_ID=110003 THEN 
			SET @SqlCmd=CONCAT('
						SELECT 
						 nt_tile.rnc_id AS RNC_ID,
						 nt_tile.cell_id AS CELL_ID,
						 nt_tile.CELL_NAME AS CELL_NAME,
						 nt_tile.ACTIVE_STATUS AS ACTIVE_STATUS,
						 IFNULL(nt_tile.CS_CALL_DURA,0) AS CS_CALL_DURA,
						 IFNULL(nt_tile.DL_DATA_THRU,0) AS DL_DATA_THRU,
						 IFNULL(nt_tile.UL_DATA_THRU,0) AS UL_DATA_THRU,
						 IFNULL(nt_tile.CALL_CNT,0) AS CALL_CNT,
						 nt_tile.Administrative_state,
						 nt_tile.OPERSTATE_ENABLE,	
						',CASE WHEN PM_COUNTER_FLAG = 'true' THEN
							 ' pm.Downtimeman,
							   pm.Downtimeauto, '
						ELSE
							' 0 AS Downtimeman,
							  0 AS Downtimeauto, '
						 END ,'
						 nt_tile.ds_date,
						 ',PU_ID,' AS pu,
						 2 AS TECH_MASK 
						FROM 		
					(	SELECT 
						  nt.rnc_id AS RNC_ID,
						  nt.cell_id AS CELL_ID,
						  nt.CELL_NAME AS CELL_NAME,
						  nt.ACTIVE_STATE AS ACTIVE_STATUS,
						  IFNULL(dat.cs,0) AS CS_CALL_DURA,
						  IFNULL(dat.dl,0) AS DL_DATA_THRU,
						  IFNULL(dat.ul,0) AS UL_DATA_THRU,
						  IFNULL(dat.cc,0) AS CALL_CNT,						
						(CASE WHEN nt.CM_ADMIN_STATE=1 THEN ''Locked''
						WHEN nt.CM_ADMIN_STATE=0 THEN ''Unlocked''
						ELSE '''' END) AS Administrative_state,
						(CASE WHEN nt.CM_OPERATION_STATE=1 THEN ''Enable''
						WHEN nt.CM_OPERATION_STATE=0 THEN ''Disable''
						ELSE '''' END) AS OPERSTATE_ENABLE,													
						ds_date,
						 ',PU_ID,' AS pu,
						2 AS TECH_MASK 
						FROM ',NT_DB,'.nt2_cell_umts nt						
						LEFT JOIN
						(
							SELECT cell_id,rnc_id,CELL_NAME,
							DATA_DATE AS ds_date,
								IFNULL(SUM(IF(CALL_TYPE IN (10,11),erlang,0)),0) AS cs,
								IFNULL(SUM(IF(CALL_TYPE IN (12,13,14,18),DL_TRAFFIC,0)),0) AS dl,
								IFNULL(SUM(IF(CALL_TYPE IN (12,13,14,18),UL_TRAFFIC,0)),0) AS ul,
								IFNULL(SUM(CALL_CNT),0) AS cc
							FROM ',GT_DB,'.',CASE WHEN DY_FLAG=1 THEN 'table_tile_start_dy_c A' ELSE 'table_tile_start_c A' END,' 
							WHERE ',CASE WHEN FILTER_STR='' OR FILTER_STR IS NULL THEN '' ELSE CONCAT(FILTER_STR,' AND ') END,' A.rnc_id=',PU_ID,' 
							GROUP BY a.cell_id,a.rnc_id
 							HAVING cs=0 AND (dl=0 OR ul=0)
						) dat 
						ON dat.cell_id = nt.cell_id AND dat.rnc_id = nt.rnc_id 
						WHERE nt.rnc_id=',PU_ID,' 
  						AND (cs=0)
--  						AND (cs=0 or cs IS NULL) AND (dl=0 OR dl IS NULL OR ul=0 or ul IS NULL) 
						) nt_tile	
						',CASE WHEN PM_COUNTER_FLAG = 'true' THEN 
						CONCAT(' LEFT JOIN 
							(
								SELECT cell_id,rnc_id,pmCellDowntimeMan AS Downtimeman,pmCellDowntimeAuto AS Downtimeauto
								FROM ',GT_DB,'.table_pm_ericsson_umts_aggr A 
								WHERE A.rnc_id=',PU_ID,' 
								GROUP BY a.cell_id,a.rnc_id 						
							) pm
							ON pm.cell_id = nt_tile.cell_id AND pm.rnc_id = nt_tile.rnc_id ') 
						ELSE '' END ,'
						WHERE nt_tile.rnc_id=',PU_ID,' ;');
		END IF;
		IF KPI_ID=110021 AND SPECIAL_IMSI<>1 THEN 	
			IF (DATA_DATE <> DATE(NOW())) THEN
				IF (START_HOUR=0 AND END_HOUR=23) THEN 
					SET DY_FLAG=1;	
				ELSE 
					SET DY_FLAG='';
				END IF;	
			ELSE 
				SET DY_FLAG='';
			END IF;	
			
			IF DY_FLAG=1 THEN 
				SET STR_SEL_IMSI_AGG_RPT=CONCAT(STR_SEL_IMSI_AGG_RPT,'SELECT ',STR_IMSI_AGG_RPT,' FROM ',GT_DB,'.table_imsi_aggregated_dy A WHERE 1 ',FILTER_STR,''); 				
			ELSE 			
				SET @v_i=START_HOUR;
				SET @v_i_Max=END_HOUR;
				SET @v_j=0;							
				WHILE @v_i <= @v_i_Max DO
				BEGIN						
					IF (@v_i=@v_i_Max ) THEN 
						SET @UNION=' '; 
					ELSE
						SET @UNION=' UNION ALL '; 
					END IF;	
					SET STR_SEL_IMSI_AGG_RPT=CONCAT(STR_SEL_IMSI_AGG_RPT,'SELECT ',STR_IMSI_AGG_RPT,' FROM ',GT_DB,'.table_imsi_aggregated_hr',CONCAT('_',LPAD(@v_i,2,'0')),' A  WHERE DATA_HOUR=', @v_i,FILTER_STR,'',@UNION); 
					SET @v_i=@v_i+1;
				END;
				END WHILE;					
			END IF;	
			SET @SqlCmd=STR_SEL_IMSI_AGG_RPT;
			SET @SqlCmd=CONCAT('
				SELECT  IMSI AS `IMSI`
					,DATA_DATE AS `DATA_DATE`
					,MAX(B.`HANDSET`) AS `HANDSET`
					,B.IMSI AS `MSISDN`
					,SUM(B.`Total_Call_Count`) AS `Total_Call_Count`
					,SUM(B.`Drop_Call_Count`) AS `Drop_Call_Count`
					,SUM(B.`Block_Call_Count`) AS `Block_Call_Count`
					,SUM(B.`Total_DL_Data_Volume`) AS `Total_DL_Data_Volume` 
					,SUM(B.`Total_UL_Data_Volume`) AS `Total_UL_Data_Volume`					
					,MAX(B.`MAX_DL_THROUGHPUT`) AS `MAX_DL_THROUGHPUT`				
					,MAX(B.`MAX_UL_THROUGHPUT`) AS `MAX_UL_THROUGHPUT`	
					,SUM(B.`UMTS_Call_Drop_Rate_1`) AS `UMTS_Call_Drop_Rate_1`
					,SUM(B.`UMTS_Call_Drop_Rate_2`) AS `UMTS_Call_Drop_Rate_2`
					,SUM(B.`UMTS_CSSR_1`) AS `UMTS_CSSR_1`
					,SUM(B.`UMTS_CSSR_2`) AS `UMTS_CSSR_2`
					,SUM(B.`UMTS_Blocked_Call_Rate_1`) AS `UMTS_Blocked_Call_Rate_1`
					,SUM(B.`UMTS_Blocked_Call_Rate_2`) AS `UMTS_Blocked_Call_Rate_2`
					,SUM(B.`UMTS_Soft_Handover_SR_1`) AS `UMTS_Soft_Handover_SR_1`
					,SUM(B.`UMTS_Soft_Handover_SR_2`) AS `UMTS_Soft_Handover_SR_2`
					,SUM(B.`UMTS_Softer_handover_SR_1`) AS `UMTS_Softer_handover_SR_1`
					,SUM(B.`UMTS_Softer_handover_SR_2`) AS `UMTS_Softer_handover_SR_2`
					,SUM(B.`UMTS_Inter_Freq_Handover_SR_1`)AS `UMTS_Inter_Freq_Handover_SR_1`
					,SUM(B.`UMTS_Inter_Freq_Handover_SR_2`) AS `UMTS_Inter_Freq_Handover_SR_2`
					,SUM(B.`UMTS_IRAT_handover_SR_1`) AS `UMTS_IRAT_handover_SR_1`
					,SUM(B.`UMTS_IRAT_handover_SR_2`) AS `UMTS_IRAT_handover_SR_2`
					,SUM(B.`LTE_Call_Drop_Rate_1`) AS `LTE_Call_Drop_Rate_1`
					,SUM(B.`LTE_Call_Drop_Rate_2`) AS `LTE_Call_Drop_Rate_2`
					,SUM(B.`LTE_CSSR_1`) AS `LTE_CSSR_1`
					,SUM(B.`LTE_CSSR_2`)  AS `LTE_CSSR_2`
					,SUM(B.`LTE_Blocked_Call_Rate_1`) AS `LTE_Blocked_Call_Rate_1` 
					,SUM(B.`LTE_Blocked_Call_Rate_2`) AS `LTE_Blocked_Call_Rate_2`
					,SUM(B.`LTE_Inter_Feq_Handover_SR_1`) AS `LTE_Inter_Feq_Handover_SR_1`
					,SUM(B.`LTE_Inter_Feq_Handover_SR_2`) AS `LTE_Inter_Feq_Handover_SR_2`
					,SUM(B.`LTE_Intra_Feq_Handover_SR_1`) AS `LTE_Intra_Feq_Handover_SR_1`
					,SUM(B.`LTE_Intra_Feq_Handover_SR_2`) AS `LTE_Intra_Feq_Handover_SR_2`
					,SUM(B.`LTE_IRAT_Handover_SR_3G_1`) AS `LTE_IRAT_Handover_SR_3G_1`
					,SUM(B.`LTE_IRAT_Handover_SR_3G_2`) AS `LTE_IRAT_Handover_SR_3G_2`
					,SUM(B.`LTE_IRAT_Handover_SR_2G_1`) AS `LTE_IRAT_Handover_SR_2G_1`
					,SUM(B.`LTE_IRAT_Handover_SR_2G_2`) AS `LTE_IRAT_Handover_SR_2G_2`
					,SUM(B.`GSM_Call_Drop_Rate_1`) AS `GSM_Call_Drop_Rate_1`
					,SUM(B.`GSM_Call_Drop_Rate_2`) AS `GSM_Call_Drop_Rate_2`
					,SUM(B.`GSM_CSSR_1`) AS `GSM_CSSR_1`
					,SUM(B.`GSM_CSSR_2`) AS `GSM_CSSR_2`
					,SUM(B.`GSM_Blocked_Call_Rate_1`) AS `GSM_Blocked_Call_Rate_1`
					,SUM(B.`GSM_Blocked_Call_Rate_2`) AS `GSM_Blocked_Call_Rate_2`
					,SUM(B.`GSM_Inter_Freq_Handover_SR_1`) AS `GSM_Inter_Freq_Handover_SR_1`
					,SUM(B.`GSM_Inter_Freq_Handover_SR_2`) AS `GSM_Inter_Freq_Handover_SR_2`
					,PU AS PU
					,DATA_DATE AS ds_date
					,MAX(TECH_MASK) AS `TECH_MASK`
					,MAX(FIRST_LAT_LON) AS `FIRST_LAT_LON`
					,MAX(LAST_LAT_LON) AS `LAST_LAT_LON`
					,POS_FIRST_RSCP_SUM AS POS_FIRST_RSCP_SUM
					,POS_FIRST_RSCP_CNT AS POS_FIRST_RSCP_CNT
					,POS_FIRST_ECN0_SUM AS POS_FIRST_ECN0_SUM
					,POS_FIRST_ECN0_CNT AS POS_FIRST_ECN0_CNT
					,POS_LAST_RSCP_SUM AS POS_LAST_RSCP_SUM
					,POS_LAST_RSCP_CNT AS POS_LAST_RSCP_CNT
					,POS_LAST_ECN0_SUM AS POS_LAST_ECN0_SUM
					,POS_LAST_ECN0_CNT AS POS_LAST_ECN0_CNT
					,POS_FIRST_S_RSRP_SUM AS POS_FIRST_S_RSRP_SUM
					,POS_FIRST_S_RSRP_CNT AS POS_FIRST_S_RSRP_CNT
					,POS_FIRST_S_RSRQ_SUM AS POS_FIRST_S_RSRQ_SUM
					,POS_FIRST_S_RSRQ_CNT AS POS_FIRST_S_RSRQ_CNT
					,POS_LAST_S_RSRP_SUM AS POS_LAST_S_RSRP_SUM
					,POS_LAST_S_RSRP_CNT AS POS_LAST_S_RSRP_CNT
					,POS_LAST_S_RSRQ_SUM AS POS_LAST_S_RSRQ_SUM
					,POS_LAST_S_RSRQ_CNT AS POS_LAST_S_RSRQ_CNT
				FROM (',STR_SEL_IMSI_AGG_RPT,') B
				WHERE IMSI IS NOT NULL
				GROUP BY IMSI;'); 	
		ELSEIF KPI_ID=110021 AND SPECIAL_IMSI=1  THEN 			
			SET @v_i=START_HOUR;
			SET @v_i_Max=END_HOUR;
			SET @v_j=0;						
			WHILE @v_i <= @v_i_Max DO
			BEGIN	
				WHILE @v_j<60  DO
				BEGIN		
					IF (@v_i=@v_i_Max AND @v_j=45) THEN 
						SET @UNION=' '; 
					ELSE
						SET @UNION=' UNION ALL '; 
					END IF;	
					SET STR_SEL_IMSI_GRP_UMTS=CONCAT(STR_SEL_IMSI_GRP_UMTS,'SELECT ',STR_IMSI_GRP_UMTS,' FROM ',GT_DB,'.',TABLE_CALL_IMSI_UMTS,'',CONCAT('_',LPAD(@v_i,2,'0'),LPAD(@v_j,2,'0'),'_imsig',ABS(IMSI_GID)), ' A ',CASE WHEN (IMSI<>'' OR IMSI_STR<> '') THEN ' FORCE INDEX (IMSI)' ELSE '' END,' WHERE DATA_HOUR=', @v_i,FILTER_STR,' GROUP BY IMSI ',@UNION); 
					SET @v_j=@v_j+15;
				END;
				END WHILE;					
				SET @v_j=0;
				SET @v_i=@v_i+1;
			END;
			END WHILE;
			SET @SqlCmd=CONCAT('
				SELECT  IMSI AS `IMSI`
					,DATA_DATE AS `DATA_DATE`
					,MAX(B.`HANDSET`) AS `HANDSET`
					,B.IMSI AS `MSISDN`
					,SUM(B.`Total_Call_Count`) AS `Total_Call_Count`
					,SUM(B.`Drop_Call_Count`) AS `Drop_Call_Count`
					,SUM(B.`Block_Call_Count`) AS `Block_Call_Count`
					,SUM(B.`Total_DL_Data_Volume`) AS `Total_DL_Data_Volume` 
					,SUM(B.`Total_UL_Data_Volume`) AS `Total_UL_Data_Volume`					
					,MAX(B.`MAX_DL_THROUGHPUT`) AS `MAX_DL_THROUGHPUT`				
					,MAX(B.`MAX_UL_THROUGHPUT`) AS `MAX_UL_THROUGHPUT`	
					,SUM(B.`UMTS_Call_Drop_Rate_1`) AS `UMTS_Call_Drop_Rate_1`
					,SUM(B.`UMTS_Call_Drop_Rate_2`) AS `UMTS_Call_Drop_Rate_2`
					,SUM(B.`UMTS_CSSR_1`) AS `UMTS_CSSR_1`
					,SUM(B.`UMTS_CSSR_2`) AS `UMTS_CSSR_2`
					,SUM(B.`UMTS_Blocked_Call_Rate_1`) AS `UMTS_Blocked_Call_Rate_1`
					,SUM(B.`UMTS_Blocked_Call_Rate_2`) AS `UMTS_Blocked_Call_Rate_2`
					,SUM(B.`UMTS_Soft_Handover_SR_1`) AS `UMTS_Soft_Handover_SR_1`
					,SUM(B.`UMTS_Soft_Handover_SR_2`) AS `UMTS_Soft_Handover_SR_2`
					,SUM(B.`UMTS_Softer_handover_SR_1`) AS `UMTS_Softer_handover_SR_1`
					,SUM(B.`UMTS_Softer_handover_SR_2`) AS `UMTS_Softer_handover_SR_2`
					,SUM(B.`UMTS_Inter_Freq_Handover_SR_1`)AS `UMTS_Inter_Freq_Handover_SR_1`
					,SUM(B.`UMTS_Inter_Freq_Handover_SR_2`) AS `UMTS_Inter_Freq_Handover_SR_2`
					,SUM(B.`UMTS_IRAT_handover_SR_1`) AS `UMTS_IRAT_handover_SR_1`
					,SUM(B.`UMTS_IRAT_handover_SR_2`) AS `UMTS_IRAT_handover_SR_2`
					,NULL AS `LTE_Call_Drop_Rate_1`
					,NULL AS `LTE_Call_Drop_Rate_2`
					,NULL AS `LTE_CSSR_1`
					,NULL AS `LTE_CSSR_2`
					,NULL AS `LTE_Blocked_Call_Rate_1` 
					,NULL AS `LTE_Blocked_Call_Rate_2`
					,NULL AS `LTE_Inter_Feq_Handover_SR_1`
					,NULL AS `LTE_Inter_Feq_Handover_SR_2`
					,NULL AS `LTE_Intra_Feq_Handover_SR_1`
					,NULL AS `LTE_Intra_Feq_Handover_SR_2`
					,NULL AS `LTE_IRAT_Handover_SR_3G_1`
					,NULL AS `LTE_IRAT_Handover_SR_3G_2`
					,NULL AS `LTE_IRAT_Handover_SR_2G_1`
					,NULL AS `LTE_IRAT_Handover_SR_2G_2`
					,NULL AS `GSM_Call_Drop_Rate_1`
					,NULL AS `GSM_Call_Drop_Rate_2`
					,NULL AS `GSM_CSSR_1`
					,NULL AS `GSM_CSSR_2`
					,NULL AS `GSM_Blocked_Call_Rate_1`
					,NULL AS `GSM_Blocked_Call_Rate_2`
					,NULL AS `GSM_Inter_Freq_Handover_SR_1`
					,NULL AS `GSM_Inter_Freq_Handover_SR_2`
					,',PU_ID,' AS PU
					,DATA_DATE AS ds_date
					,MAX(TECH_MASK) AS `TECH_MASK`
					,CONCAT(gt_covmo_proj_geohash_to_lng(POS_FIRST_LOC),''|'',gt_covmo_proj_geohash_to_lat(POS_FIRST_LOC)) AS `FIRST_LAT_LON`
					,CONCAT(gt_covmo_proj_geohash_to_lng(POS_LAST_LOC),''|'',gt_covmo_proj_geohash_to_lat(POS_LAST_LOC)) AS `FIRST_LAT_LON`
				--	POS_FIRST_LOC AS POS_FIRST_LOC
				--	,POS_LAST_LOC AS POS_LAST_LOC
					,POS_FIRST_RSCP_SUM AS POS_FIRST_RSCP_SUM
					,POS_FIRST_RSCP_CNT AS POS_FIRST_RSCP_CNT
					,POS_FIRST_ECN0_SUM AS POS_FIRST_ECN0_SUM
					,POS_FIRST_ECN0_CNT AS POS_FIRST_ECN0_CNT
					,POS_LAST_RSCP_SUM AS POS_LAST_RSCP_SUM
					,POS_LAST_RSCP_CNT AS POS_LAST_RSCP_CNT
					,POS_LAST_ECN0_SUM AS POS_LAST_ECN0_SUM
					,POS_LAST_ECN0_CNT AS POS_LAST_ECN0_CNT
					,NULL AS POS_FIRST_S_RSRP_SUM
					,NULL AS POS_FIRST_S_RSRP_CNT
					,NULL AS POS_FIRST_S_RSRQ_SUM
					,NULL AS POS_FIRST_S_RSRQ_CNT
					,NULL AS POS_LAST_S_RSRP_SUM
					,NULL AS POS_LAST_S_RSRP_CNT
					,NULL AS POS_LAST_S_RSRQ_SUM
					,NULL AS POS_LAST_S_RSRQ_CNT
				FROM (',STR_SEL_IMSI_GRP_UMTS,') B
				WHERE IMSI IS NOT NULL
				GROUP BY IMSI;'); 
		END IF;	
		IF KPI_ID=110022 THEN 
			SET @SqlCmd=CONCAT('SELECT RNC_ID,CELL_ID FROM ',GT_DB,'.table_tile_start_dy_c_def GROUP BY RNC_ID,CELL_ID;');
		END IF;
		IF KPI_ID=110027 THEN
			IF SPECIAL_IMSI<>1 THEN 
				SET @v_i=START_HOUR;
				SET @v_i_Max=END_HOUR;
				SET @v_j=0;
				WHILE @v_i <= @v_i_Max DO
				BEGIN	
					WHILE @v_j<60  DO
					BEGIN		
						IF (@v_i=@v_i_Max AND @v_j=45) THEN 
							SET @UNION=' '; 
						ELSE
							SET @UNION=' UNION ALL '; 
						END IF;	
						SET STR_SEL_VIP_GRP_UMTS=CONCAT(STR_SEL_VIP_GRP_UMTS,'SELECT ',STR_VIP_GRP_UMTS,' FROM ',GT_DB,'.',TABLE_CALL_IMSI_UMTS,'',CONCAT('_',LPAD(@v_i,2,'0'),LPAD(@v_j,2,'0')),' A ',CASE WHEN (IMSI<>'' OR IMSI_STR<> '') THEN ' FORCE INDEX (IMSI)' ELSE '' END,' WHERE DATA_HOUR=', @v_i,FILTER_STR,' AND A.`POS_LAST_TILE`>0 ' ,@UNION); 
						SET @v_j=@v_j+15;
					END;
					END WHILE;
					SET @v_j=0;
					SET @v_i=@v_i+1;
				END;
				END WHILE;	
			ELSEIF SPECIAL_IMSI=1  THEN 
				SET @v_i=START_HOUR;
				SET @v_i_Max=END_HOUR;
				SET @v_j=0;
				WHILE @v_i <= @v_i_Max DO
				BEGIN	
					WHILE @v_j<60  DO
					BEGIN		
						IF (@v_i=@v_i_Max AND @v_j=45) THEN 
							SET @UNION=' '; 
						ELSE
							SET @UNION=' UNION ALL '; 
						END IF;	
						SET STR_SEL_VIP_GRP_UMTS=CONCAT(STR_SEL_VIP_GRP_UMTS,'SELECT ',STR_VIP_GRP_UMTS,' FROM ',GT_DB,'.',TABLE_CALL_IMSI_UMTS,'',CONCAT('_',LPAD(@v_i,2,'0'),LPAD(@v_j,2,'0'),'_imsig',ABS(IMSI_GID)), ' A ',CASE WHEN (IMSI<>'' OR IMSI_STR<> '') THEN ' FORCE INDEX (IMSI)' ELSE '' END,' WHERE DATA_HOUR=', @v_i,FILTER_STR,@UNION); 
						SET @v_j=@v_j+15;
					END;
					END WHILE;
					SET @v_j=0;
					SET @v_i=@v_i+1;
				END;
				END WHILE;
			END IF;
			SET @SqlCmd=CONCAT('
				SELECT  B.IMSI AS `IMSI`,
					B.DATA_DATE AS `DATA_DATE`,
					B.IMEI AS `IMEI`,
					SUM(B.TOTAL_CNT) AS TOTAL_CNT,
					SUM(B.DROP_CNT) AS DROP_CNT,
					SUM(B.NB_TOTAL_CNT) AS NB_TOTAL_CNT,
					SUM(B.BLOCK_CNT) AS BLOCK_CNT,
					SUM(B.CSF_CNT) AS CSF_CNT,
					SUM(B.CSS_CNT) AS CSS_CNT,
					SUM(B.UMTS_IRAT_HHO_ATTEMPT) AS UMTS_IRAT_HHO_ATTEMPT,
					SUM(B.UMTS_IRAT_HHO_FAIL) AS UMTS_IRAT_HHO_FAIL,
					SUM(B.UMTS_IRAT_HHO_SUCCESS) AS UMTS_IRAT_HHO_SUCCESS,
					SUM(B.UMTS_BLOCK_CNT) AS UMTS_BLOCK_CNT,
					SUM(B.UMTS_TOTAL_CNT) AS UMTS_TOTAL_CNT,
					SUM(B.UMTS_CSS_CNT) AS UMTS_CSS_CNT,
					SUM(B.UMTS_CSF_CNT) AS UMTS_CSF_CNT,
					SUM(B.UMTS_DROP_CNT) AS UMTS_DROP_CNT,
					SUM(B.UMTS_NB_TOTAL_CNT) AS UMTS_NB_TOTAL_CNT,
					SUM(B.LTE_IRAT_TO_UMTS_ATTEMPT) AS LTE_IRAT_TO_UMTS_ATTEMPT,
					SUM(B.LTE_IRAT_TO_UMTS_FAILURE) AS LTE_IRAT_TO_UMTS_FAILURE,
					SUM(B.LTE_BLOCK_CNT) AS LTE_BLOCK_CNT,
					SUM(B.LTE_TOTAL_CNT) AS LTE_TOTAL_CNT,
					SUM(B.LTE_CSS_CNT) AS LTE_CSS_CNT,
					SUM(B.LTE_CSF_CNT) AS LTE_CSF_CNT,
					SUM(B.LTE_DROP_CNT) AS LTE_DROP_CNT,
					SUM(B.LTE_NB_TOTAL_CNT) AS LTE_NB_TOTAL_CNT,
					B.PU AS PU,
					B.DATA_DATE AS ds_date,
					B.TECH_MASK AS `TECH_MASK`,
					MAX(CONCAT(IFNULL(B.POS_FIRST_LON,'' ''),''|'',IFNULL(B.POS_FIRST_LAT,'' ''))) AS FIRST_LON_LAT,
					MAX(CONCAT(IFNULL(B.POS_LAST_LON,'' ''),''|'',IFNULL(B.POS_LAST_LAT,'' ''))) AS LAST_LON_LAT
				FROM (',STR_SEL_VIP_GRP_UMTS,') B
				GROUP BY IMSI;'); 
		END IF;
	END;	
	ELSEIF @TECHNOLOGY='LTE' THEN 
	BEGIN
		IF  KPI_ID=110001 AND SPECIAL_IMSI<>1  THEN 			
			SET @v_i=START_HOUR;
			SET @v_i_Max=END_HOUR;
			SET @v_j=0;						
			WHILE @v_i <= @v_i_Max DO
			BEGIN	
				WHILE @v_j<60  DO
				BEGIN		
					IF (@v_i=@v_i_Max AND @v_j=45) THEN 
						SET @UNION=' '; 
					ELSE
						SET @UNION=' UNION ALL '; 
					END IF;		
					SET STR_SEL_IMSI_LTE=CONCAT(STR_SEL_IMSI_LTE,'SELECT ',STR_IMSI_LTE,' FROM ',GT_DB,'.',TABLE_CALL_IMSI_LTE,'',CONCAT('_',LPAD(@v_i,2,'0'),LPAD(@v_j,2,'0')),' A ',CASE WHEN (IMSI<>'' OR IMSI_STR<> '') THEN ' FORCE INDEX (IMSI)' ELSE '' END,' WHERE DATA_HOUR=', @v_i,FILTER_STR,CASE WHEN WITHDUMP=0 THEN ' AND A.`POS_LAST_TILE`>0 ' ELSE '' END,@UNION); 
					SET @v_j=@v_j+15;
				END;
				END WHILE;					
				SET @v_j=0;
				SET @v_i=@v_i+1;
			END;
			END WHILE;	
			SET @SqlCmd=STR_SEL_IMSI_LTE;
		ELSEIF KPI_ID=110001 AND SPECIAL_IMSI=1  THEN 	
			SET @v_i=START_HOUR;
			SET @v_i_Max=END_HOUR;
			SET @v_j=0;						
			WHILE @v_i <= @v_i_Max DO
			BEGIN	
				WHILE @v_j<60  DO
				BEGIN		
					IF (@v_i=@v_i_Max AND @v_j=45) THEN 
						SET @UNION=' '; 
					ELSE
						SET @UNION=' UNION ALL '; 
					END IF;
					SET STR_SEL_IMSI_LTE=CONCAT(STR_SEL_IMSI_LTE,'SELECT ',STR_IMSI_LTE,' FROM ',GT_DB,'.',TABLE_CALL_IMSI_LTE,'',CONCAT('_',LPAD(@v_i,2,'0'),LPAD(@v_j,2,'0'),'_imsig',ABS(IMSI_GID)),' A ',CASE WHEN (IMSI<>'' OR IMSI_STR<> '') THEN ' FORCE INDEX (IMSI)' ELSE '' END,' WHERE DATA_HOUR=', @v_i,FILTER_STR,@UNION); 
					SET @v_j=@v_j+15;
				END;
				END WHILE;					
				SET @v_j=0;
				SET @v_i=@v_i+1;
			END;
			END WHILE;
			SET @SqlCmd=STR_SEL_IMSI_LTE;
		END IF;	
		
		IF KPI_ID=110005 AND SPECIAL_IMSI<>1 THEN 
			SET @v_i=START_HOUR;
			SET @v_i_Max=END_HOUR;
			SET @v_j=0;
			WHILE @v_i <= @v_i_Max DO
			BEGIN	
				WHILE @v_j<60  DO
				BEGIN		
					IF (@v_i=@v_i_Max AND @v_j=45) THEN 
						SET @UNION=' '; 
					ELSE
						SET @UNION=' UNION ALL '; 
					END IF;
					SET STR_SEL_IMSI_GRP_LTE=CONCAT(STR_SEL_IMSI_GRP_LTE,'SELECT ',STR_IMSI_GRP_LTE,' FROM ',GT_DB,'.',TABLE_CALL_IMSI_LTE,'',CONCAT('_',LPAD(@v_i,2,'0'),LPAD(@v_j,2,'0')),' A ',CASE WHEN (IMSI<>'' OR IMSI_STR<> '') THEN ' FORCE INDEX (IMSI)' ELSE '' END,' WHERE DATA_HOUR=', @v_i,FILTER_STR,CASE WHEN WITHDUMP=0 THEN ' AND A.`POS_LAST_TILE`>0 ' ELSE ' ' END,' GROUP BY IMSI ',@UNION); 
					SET @v_j=@v_j+15;
				END;
				END WHILE;					
				SET @v_j=0;
				SET @v_i=@v_i+1;
			END;
			END WHILE;
			SET @SqlCmd=STR_SEL_IMSI_GRP_LTE;
			SET @SqlCmd=CONCAT('
				SELECT  IMSI AS `IMSI`
					,DATA_DATE AS `DATA_DATE`
					,MAX(B.HANDSET) AS `HANDSET`
					,IMSI AS `MSISDN`
					,SUM(B.Total_Call_Count) AS `Total_Call_Count`
					,SUM(B.Drop_Call_Count) AS `Drop_Call_Count`
					,SUM(B.Block_Call_Count) AS `Block_Call_Count`
					,SUM(B.Total_DL_Data_Volume) AS `Total_DL_Data_Volume`
					,SUM(B.Total_UL_Data_Volume) AS `Total_UL_Data_Volume`
					,MAX(B.MAX_DL_THROUGHPUT) AS `MAX_DL_THROUGHPUT`
					,MAX(B.MAX_UL_THROUGHPUT) AS `MAX_UL_THROUGHPUT`
					,NULL AS `UMTS_Call_Drop_Rate_1`
					,NULL AS `UMTS_Call_Drop_Rate_2`
					,NULL AS `UMTS_CSSR_1`
					,NULL AS `UMTS_CSSR_2`
					,NULL AS `UMTS_Blocked_Call_Rate_1`
					,NULL AS `UMTS_Blocked_Call_Rate_2`
					,NULL AS `UMTS_Soft_Handover_SR_1`
					,NULL AS `UMTS_Soft_Handover_SR_2`
					,NULL AS `UMTS_Softer_handover_SR_1`
					,NULL AS `UMTS_Softer_handover_SR_2`
					,NULL AS `UMTS_Inter_Freq_Handover_SR_1`
					,NULL AS `UMTS_Inter_Freq_Handover_SR_2`
					,NULL AS `UMTS_IRAT_handover_SR_1`
					,NULL AS `UMTS_IRAT_handover_SR_2`
					,SUM(B.LTE_Call_Drop_Rate_1) AS `LTE_Call_Drop_Rate_1`
					,SUM(B.LTE_Call_Drop_Rate_2) AS `LTE_Call_Drop_Rate_2`
					,SUM(B.LTE_CSSR_1) AS `LTE_CSSR_1`
					,SUM(B.LTE_CSSR_2) AS `LTE_CSSR_2`
					,SUM(B.LTE_Blocked_Call_Rate_1) AS `LTE_Blocked_Call_Rate_1` 
					,SUM(B.LTE_Blocked_Call_Rate_2) AS `LTE_Blocked_Call_Rate_2`
					,SUM(B.LTE_Inter_Feq_Handover_SR_1) AS `LTE_Inter_Feq_Handover_SR_1`
					,SUM(B.LTE_Inter_Feq_Handover_SR_2) AS `LTE_Inter_Feq_Handover_SR_2`
					,SUM(B.LTE_Intra_Feq_Handover_SR_1) AS `LTE_Intra_Feq_Handover_SR_1`
					,SUM(B.LTE_Intra_Feq_Handover_SR_2) AS `LTE_Intra_Feq_Handover_SR_2`
					,SUM(B.LTE_IRAT_Handover_SR_3G_1) AS `LTE_IRAT_Handover_SR_3G_1`
					,SUM(B.LTE_IRAT_Handover_SR_3G_2) AS `LTE_IRAT_Handover_SR_3G_2`
					,SUM(B.LTE_IRAT_Handover_SR_2G_1) AS `LTE_IRAT_Handover_SR_2G_1`
					,SUM(B.LTE_IRAT_Handover_SR_2G_2) AS `LTE_IRAT_Handover_SR_2G_2`
					,NULL AS `GSM_Call_Drop_Rate_1`
					,NULL AS `GSM_Call_Drop_Rate_2`
					,NULL AS `GSM_CSSR_1`
					,NULL AS `GSM_CSSR_2`
					,NULL AS `GSM_Blocked_Call_Rate_1`
					,NULL AS `GSM_Blocked_Call_Rate_2`
					,NULL AS `GSM_Inter_Freq_Handover_SR_1`
					,NULL AS `GSM_Inter_Freq_Handover_SR_2`
					,PU AS PU
					,ds_date
					,`TECH_MASK`
					,POS_FIRST_LOC AS POS_FIRST_LOC
					,POS_LAST_LOC AS POS_LAST_LOC
					,NULL AS POS_FIRST_RSCP_SUM
					,NULL AS POS_FIRST_RSCP_CNT
					,NULL AS POS_FIRST_ECN0_SUM
					,NULL AS POS_FIRST_ECN0_CNT
					,NULL AS POS_LAST_RSCP_SUM
					,NULL AS POS_LAST_RSCP_CNT
					,NULL AS POS_LAST_ECN0_SUM
					,NULL AS POS_LAST_ECN0_CNT
					,SUM(POS_FIRST_S_RSRP_SUM) AS POS_FIRST_S_RSRP_SUM
					,SUM(POS_FIRST_S_RSRP_CNT) AS POS_FIRST_S_RSRP_CNT
					,SUM(POS_FIRST_S_RSRQ_SUM) AS POS_FIRST_S_RSRQ_SUM
					,SUM(POS_FIRST_S_RSRQ_CNT) AS POS_FIRST_S_RSRQ_CNT
					,SUM(POS_LAST_S_RSRP_SUM) AS POS_LAST_S_RSRP_SUM
					,SUM(POS_LAST_S_RSRP_CNT) AS POS_LAST_S_RSRP_CNT
					,SUM(POS_LAST_S_RSRQ_SUM) AS POS_LAST_S_RSRQ_SUM
					,SUM(POS_LAST_S_RSRQ_CNT) AS POS_LAST_S_RSRQ_CNT
				FROM (',STR_SEL_IMSI_GRP_LTE,') B
				WHERE IMSI IS NOT NULL
				GROUP BY IMSI;'); 
		ELSEIF KPI_ID=110005 AND SPECIAL_IMSI=1  THEN 
			SET @v_i=START_HOUR;
			SET @v_i_Max=END_HOUR;
			SET @v_j=0;
			WHILE @v_i <= @v_i_Max DO
			BEGIN	
				WHILE @v_j<60  DO
				BEGIN		
					IF (@v_i=@v_i_Max AND @v_j=45) THEN 
						SET @UNION=' '; 
					ELSE
						SET @UNION=' UNION ALL '; 
					END IF;
					SET STR_SEL_IMSI_GRP_LTE=CONCAT(STR_SEL_IMSI_GRP_LTE,'SELECT ',STR_IMSI_GRP_LTE,' FROM ',GT_DB,'.',TABLE_CALL_IMSI_LTE,'',CONCAT('_',LPAD(@v_i,2,'0'),LPAD(@v_j,2,'0'),'_imsig',ABS(IMSI_GID)), ' A ',CASE WHEN (IMSI<>'' OR IMSI_STR<> '') THEN ' FORCE INDEX (IMSI)' ELSE '' END,' WHERE DATA_HOUR=', @v_i,FILTER_STR,' GROUP BY IMSI ',@UNION); 
					SET @v_j=@v_j+15;				
				END;
				END WHILE;					
				SET @v_j=0;
				SET @v_i=@v_i+1;
			END;
			END WHILE;
			SET @SqlCmd=CONCAT('
				SELECT  IMSI AS `IMSI`
					,DATA_DATE AS `DATA_DATE`
					,MAX(B.HANDSET) AS `HANDSET`
					,IMSI AS `MSISDN`
					,SUM(B.Total_Call_Count) AS `Total_Call_Count`
					,SUM(B.Drop_Call_Count) AS `Drop_Call_Count`
					,SUM(B.Block_Call_Count) AS `Block_Call_Count`
					,SUM(B.Total_DL_Data_Volume) AS `Total_DL_Data_Volume`
					,SUM(B.Total_UL_Data_Volume) AS `Total_UL_Data_Volume`
					,MAX(B.MAX_DL_THROUGHPUT) AS `MAX_DL_THROUGHPUT`
					,MAX(B.MAX_UL_THROUGHPUT) AS `MAX_UL_THROUGHPUT`
					,NULL AS `UMTS_Call_Drop_Rate_1`
					,NULL AS `UMTS_Call_Drop_Rate_2`
					,NULL AS `UMTS_CSSR_1`
					,NULL AS `UMTS_CSSR_2`
					,NULL AS `UMTS_Blocked_Call_Rate_1`
					,NULL AS `UMTS_Blocked_Call_Rate_2`
					,NULL AS `UMTS_Soft_Handover_SR_1`
					,NULL AS `UMTS_Soft_Handover_SR_2`
					,NULL AS `UMTS_Softer_handover_SR_1`
					,NULL AS `UMTS_Softer_handover_SR_2`
					,NULL AS `UMTS_Inter_Freq_Handover_SR_1`
					,NULL AS `UMTS_Inter_Freq_Handover_SR_2`
					,NULL AS `UMTS_IRAT_handover_SR_1`
					,NULL AS `UMTS_IRAT_handover_SR_2`
					,SUM(B.LTE_Call_Drop_Rate_1) AS `LTE_Call_Drop_Rate_1`
					,SUM(B.LTE_Call_Drop_Rate_2) AS `LTE_Call_Drop_Rate_2`
					,SUM(B.LTE_CSSR_1) AS `LTE_CSSR_1`
					,SUM(B.LTE_CSSR_2) AS `LTE_CSSR_2`
					,SUM(B.LTE_Blocked_Call_Rate_1) AS `LTE_Blocked_Call_Rate_1` 
					,SUM(B.LTE_Blocked_Call_Rate_2) AS `LTE_Blocked_Call_Rate_2`
					,SUM(B.LTE_Inter_Feq_Handover_SR_1) AS `LTE_Inter_Feq_Handover_SR_1`
					,SUM(B.LTE_Inter_Feq_Handover_SR_2) AS `LTE_Inter_Feq_Handover_SR_2`
					,SUM(B.LTE_Intra_Feq_Handover_SR_1) AS `LTE_Intra_Feq_Handover_SR_1`
					,SUM(B.LTE_Intra_Feq_Handover_SR_2) AS `LTE_Intra_Feq_Handover_SR_2`
					,SUM(B.LTE_IRAT_Handover_SR_3G_1) AS `LTE_IRAT_Handover_SR_3G_1`
					,SUM(B.LTE_IRAT_Handover_SR_3G_2) AS `LTE_IRAT_Handover_SR_3G_2`
					,SUM(B.LTE_IRAT_Handover_SR_2G_1) AS `LTE_IRAT_Handover_SR_2G_1`
					,SUM(B.LTE_IRAT_Handover_SR_2G_2) AS `LTE_IRAT_Handover_SR_2G_2`
					,NULL AS `GSM_Call_Drop_Rate_1`
					,NULL AS `GSM_Call_Drop_Rate_2`
					,NULL AS `GSM_CSSR_1`
					,NULL AS `GSM_CSSR_2`
					,NULL AS `GSM_Blocked_Call_Rate_1`
					,NULL AS `GSM_Blocked_Call_Rate_2`
					,NULL AS `GSM_Inter_Freq_Handover_SR_1`
					,NULL AS `GSM_Inter_Freq_Handover_SR_2`
					,PU AS PU
					,ds_date
					,`TECH_MASK`
					,POS_FIRST_LOC AS POS_FIRST_LOC
					,POS_LAST_LOC AS POS_LAST_LOC
					,NULL AS POS_FIRST_RSCP_SUM
					,NULL AS POS_FIRST_RSCP_CNT
					,NULL AS POS_FIRST_ECN0_SUM
					,NULL AS POS_FIRST_ECN0_CNT
					,NULL AS POS_LAST_RSCP_SUM
					,NULL AS POS_LAST_RSCP_CNT
					,NULL AS POS_LAST_ECN0_SUM
					,NULL AS POS_LAST_ECN0_CNT
					,POS_FIRST_S_RSRP_SUM AS POS_FIRST_S_RSRP_SUM
					,POS_FIRST_S_RSRP_CNT AS POS_FIRST_S_RSRP_CNT
					,POS_FIRST_S_RSRQ_SUM AS POS_FIRST_S_RSRQ_SUM
					,POS_FIRST_S_RSRQ_CNT AS POS_FIRST_S_RSRQ_CNT
					,POS_LAST_S_RSRP_SUM AS POS_LAST_S_RSRP_SUM
					,POS_LAST_S_RSRP_CNT AS POS_LAST_S_RSRP_CNT
					,POS_LAST_S_RSRQ_SUM AS POS_LAST_S_RSRQ_SUM
					,POS_LAST_S_RSRQ_CNT AS POS_LAST_S_RSRQ_CNT
					,NULL AS POS_FIRST_RSCP_SUM
					,NULL AS POS_FIRST_RSCP_CNT
					,NULL AS POS_FIRST_ECN0_SUM
					,NULL AS POS_FIRST_ECN0_CNT
					,NULL AS POS_LAST_RSCP_SUM
					,NULL AS POS_LAST_RSCP_CNT
					,NULL AS POS_LAST_ECN0_SUM
					,NULL AS POS_LAST_ECN0_CNT
					,SUM(POS_FIRST_S_RSRP_SUM) AS POS_FIRST_S_RSRP_SUM
					,SUM(POS_FIRST_S_RSRP_CNT) AS POS_FIRST_S_RSRP_CNT
					,SUM(POS_FIRST_S_RSRQ_SUM) AS POS_FIRST_S_RSRQ_SUM
					,SUM(POS_FIRST_S_RSRQ_CNT) AS POS_FIRST_S_RSRQ_CNT
					,SUM(POS_LAST_S_RSRP_SUM) AS POS_LAST_S_RSRP_SUM
					,SUM(POS_LAST_S_RSRP_CNT) AS POS_LAST_S_RSRP_CNT
					,SUM(POS_LAST_S_RSRQ_SUM) AS POS_LAST_S_RSRQ_SUM
					,SUM(POS_LAST_S_RSRQ_CNT) AS POS_LAST_S_RSRQ_CNT
				FROM (',STR_SEL_IMSI_GRP_LTE,') B
				WHERE IMSI IS NOT NULL
				GROUP BY IMSI;'); 
		END IF;
		
		IF KPI_ID=110020 AND SPECIAL_IMSI<>1 THEN 
			SET @v_i=START_HOUR;
			SET @v_i_Max=END_HOUR;
			SET @v_j=0;
			WHILE @v_i <= @v_i_Max DO
			BEGIN	
				WHILE @v_j<60  DO
				BEGIN		
					IF (@v_i=@v_i_Max AND @v_j=45) THEN 
						SET @UNION=' '; 
					ELSE
						SET @UNION=' UNION ALL '; 
					END IF;
					SET STR_SEL_IMSI_GRP_LTE=CONCAT(STR_SEL_IMSI_GRP_LTE,'SELECT ',STR_IMSI_GRP_LTE,' FROM ',GT_DB,'.',TABLE_CALL_IMSI_LTE,'',CONCAT('_',LPAD(@v_i,2,'0'),LPAD(@v_j,2,'0')),' A ',CASE WHEN (IMSI<>'' OR IMSI_STR<> '') THEN ' FORCE INDEX (IMSI)' ELSE '' END,' WHERE DATA_HOUR=', @v_i,FILTER_STR,CASE WHEN WITHDUMP=0 THEN ' AND A.`POS_LAST_TILE`>0 ' ELSE ' ' END,' GROUP BY IMSI ',@UNION); 
					SET @v_j=@v_j+15;
				END;
				END WHILE;
				SET @v_j=0;
				SET @v_i=@v_i+1;
			END;
			END WHILE;
			SET @SqlCmd=STR_SEL_IMSI_GRP_LTE;
			SET @SqlCmd=CONCAT('
				SELECT  IMSI AS `IMSI`
					,DATA_DATE AS `DATA_DATE`
					,MAX(B.HANDSET) AS `HANDSET`
					,IMSI AS `MSISDN`
					,SUM(B.Total_Call_Count) AS `Total_Call_Count`
					,SUM(B.Drop_Call_Count) AS `Drop_Call_Count`
					,SUM(B.Block_Call_Count) AS `Block_Call_Count`
					,SUM(B.Total_DL_Data_Volume) AS `Total_DL_Data_Volume` 
					,SUM(B.Total_UL_Data_Volume) AS `Total_UL_Data_Volume`					
					,MAX(B.MAX_DL_THROUGHPUT) AS `MAX_DL_THROUGHPUT`				
					,MAX(B.MAX_UL_THROUGHPUT) AS `MAX_UL_THROUGHPUT`
					,NULL AS `UMTS_Call_Drop_Rate_1`
					,NULL AS `UMTS_Call_Drop_Rate_2`
					,NULL AS `UMTS_CSSR_1`
					,NULL AS `UMTS_CSSR_2`
					,NULL AS `UMTS_Blocked_Call_Rate_1`
					,NULL AS `UMTS_Blocked_Call_Rate_2`
					,NULL AS `UMTS_Soft_Handover_SR_1`
					,NULL AS `UMTS_Soft_Handover_SR_2`
					,NULL AS `UMTS_Softer_handover_SR_1`
					,NULL AS `UMTS_Softer_handover_SR_2`
					,NULL AS `UMTS_Inter_Freq_Handover_SR_1`
					,NULL AS `UMTS_Inter_Freq_Handover_SR_2`
					,NULL AS `UMTS_IRAT_handover_SR_1`
					,NULL AS `UMTS_IRAT_handover_SR_2`
					,SUM(B.LTE_Call_Drop_Rate_1) AS `LTE_Call_Drop_Rate_1`
					,SUM(B.LTE_Call_Drop_Rate_2) AS `LTE_Call_Drop_Rate_2`
					,SUM(B.LTE_CSSR_1) AS `LTE_CSSR_1`
					,SUM(B.LTE_CSSR_2) AS `LTE_CSSR_2`
					,SUM(B.LTE_Blocked_Call_Rate_1) AS `LTE_Blocked_Call_Rate_1` 
					,SUM(B.LTE_Blocked_Call_Rate_2) AS `LTE_Blocked_Call_Rate_2`
					,SUM(B.LTE_Inter_Feq_Handover_SR_1) AS `LTE_Inter_Feq_Handover_SR_1`
					,SUM(B.LTE_Inter_Feq_Handover_SR_2) AS `LTE_Inter_Feq_Handover_SR_2`
					,SUM(B.LTE_Intra_Feq_Handover_SR_1) AS `LTE_Intra_Feq_Handover_SR_1`
					,SUM(B.LTE_Intra_Feq_Handover_SR_2) AS `LTE_Intra_Feq_Handover_SR_2`
					,SUM(B.LTE_IRAT_Handover_SR_3G_1) AS `LTE_IRAT_Handover_SR_3G_1`
					,SUM(B.LTE_IRAT_Handover_SR_3G_2) AS `LTE_IRAT_Handover_SR_3G_2`
					,SUM(B.LTE_IRAT_Handover_SR_2G_1) AS `LTE_IRAT_Handover_SR_2G_1`
					,SUM(B.LTE_IRAT_Handover_SR_2G_2) AS `LTE_IRAT_Handover_SR_2G_2`
					,NULL AS `GSM_Call_Drop_Rate_1`
					,NULL AS `GSM_Call_Drop_Rate_2`
					,NULL AS `GSM_CSSR_1`
					,NULL AS `GSM_CSSR_2`
					,NULL AS `GSM_Blocked_Call_Rate_1`
					,NULL AS `GSM_Blocked_Call_Rate_2`
					,NULL AS `GSM_Inter_Freq_Handover_SR_1`
					,NULL AS `GSM_Inter_Freq_Handover_SR_2`
					,PU AS PU
					,ds_date
					,`TECH_MASK`
					,POS_FIRST_LOC AS POS_FIRST_LOC
					,POS_LAST_LOC AS POS_LAST_LOC	
				FROM (',STR_SEL_IMSI_GRP_LTE,') B
				WHERE IMSI IS NOT NULL
				GROUP BY IMSI,DATA_DATE;');
		ELSEIF KPI_ID=110020 AND SPECIAL_IMSI=1  THEN
			SET @v_i=START_HOUR;
			SET @v_i_Max=END_HOUR;
			SET @v_j=0;
			WHILE @v_i <= @v_i_Max DO
			BEGIN	
				WHILE @v_j<60  DO
				BEGIN		
					IF (@v_i=@v_i_Max AND @v_j=45) THEN 
						SET @UNION=' '; 
					ELSE
						SET @UNION=' UNION ALL '; 
					END IF;
					SET STR_SEL_IMSI_GRP_LTE=CONCAT(STR_SEL_IMSI_GRP_LTE,'SELECT ',STR_IMSI_GRP_LTE,' FROM ',GT_DB,'.',TABLE_CALL_IMSI_LTE,'',CONCAT('_',LPAD(@v_i,2,'0'),LPAD(@v_j,2,'0'),'_imsig',ABS(IMSI_GID)), ' A ',CASE WHEN (IMSI<>'' OR IMSI_STR<> '') THEN ' FORCE INDEX (IMSI)' ELSE '' END,' WHERE DATA_HOUR=', @v_i,FILTER_STR,' GROUP BY IMSI ',@UNION); 
					SET @v_j=@v_j+15;
				END;
				END WHILE;
				SET @v_j=0;
				SET @v_i=@v_i+1;
			END;
			END WHILE;
			SET @SqlCmd=CONCAT('
				SELECT  IMSI AS `IMSI`
					,DATA_DATE AS `DATA_DATE`
					,MAX(B.HANDSET) AS `HANDSET`
					,IMSI AS `MSISDN`
					,SUM(B.Total_Call_Count) AS `Total_Call_Count`
					,SUM(B.Drop_Call_Count) AS `Drop_Call_Count`
					,SUM(B.Block_Call_Count) AS `Block_Call_Count`
					,SUM(B.Total_DL_Data_Volume) AS `Total_DL_Data_Volume` 
					,SUM(B.Total_UL_Data_Volume) AS `Total_UL_Data_Volume`					
					,MAX(B.MAX_DL_THROUGHPUT) AS `MAX_DL_THROUGHPUT`				
					,MAX(B.MAX_UL_THROUGHPUT) AS `MAX_UL_THROUGHPUT`
					,NULL AS `UMTS_Call_Drop_Rate_1`
					,NULL AS `UMTS_Call_Drop_Rate_2`
					,NULL AS `UMTS_CSSR_1`
					,NULL AS `UMTS_CSSR_2`
					,NULL AS `UMTS_Blocked_Call_Rate_1`
					,NULL AS `UMTS_Blocked_Call_Rate_2`
					,NULL AS `UMTS_Soft_Handover_SR_1`
					,NULL AS `UMTS_Soft_Handover_SR_2`
					,NULL AS `UMTS_Softer_handover_SR_1`
					,NULL AS `UMTS_Softer_handover_SR_2`
					,NULL AS `UMTS_Inter_Freq_Handover_SR_1`
					,NULL AS `UMTS_Inter_Freq_Handover_SR_2`
					,NULL AS `UMTS_IRAT_handover_SR_1`
					,NULL AS `UMTS_IRAT_handover_SR_2`
					,SUM(B.LTE_Call_Drop_Rate_1) AS `LTE_Call_Drop_Rate_1`
					,SUM(B.LTE_Call_Drop_Rate_2) AS `LTE_Call_Drop_Rate_2`
					,SUM(B.LTE_CSSR_1) AS `LTE_CSSR_1`
					,SUM(B.LTE_CSSR_2) AS `LTE_CSSR_2`
					,SUM(B.LTE_Blocked_Call_Rate_1) AS `LTE_Blocked_Call_Rate_1` 
					,SUM(B.LTE_Blocked_Call_Rate_2) AS `LTE_Blocked_Call_Rate_2`
					,SUM(B.LTE_Inter_Feq_Handover_SR_1) AS `LTE_Inter_Feq_Handover_SR_1`
					,SUM(B.LTE_Inter_Feq_Handover_SR_2) AS `LTE_Inter_Feq_Handover_SR_2`
					,SUM(B.LTE_Intra_Feq_Handover_SR_1) AS `LTE_Intra_Feq_Handover_SR_1`
					,SUM(B.LTE_Intra_Feq_Handover_SR_2) AS `LTE_Intra_Feq_Handover_SR_2`
					,SUM(B.LTE_IRAT_Handover_SR_3G_1) AS `LTE_IRAT_Handover_SR_3G_1`
					,SUM(B.LTE_IRAT_Handover_SR_3G_2) AS `LTE_IRAT_Handover_SR_3G_2`
					,SUM(B.LTE_IRAT_Handover_SR_2G_1) AS `LTE_IRAT_Handover_SR_2G_1`
					,SUM(B.LTE_IRAT_Handover_SR_2G_2) AS `LTE_IRAT_Handover_SR_2G_2`
					,NULL AS `GSM_Call_Drop_Rate_1`
					,NULL AS `GSM_Call_Drop_Rate_2`
					,NULL AS `GSM_CSSR_1`
					,NULL AS `GSM_CSSR_2`
					,NULL AS `GSM_Blocked_Call_Rate_1`
					,NULL AS `GSM_Blocked_Call_Rate_2`
					,NULL AS `GSM_Inter_Freq_Handover_SR_1`
					,NULL AS `GSM_Inter_Freq_Handover_SR_2`
					,PU AS PU
					,ds_date
					,`TECH_MASK`
					,POS_FIRST_LOC AS POS_FIRST_LOC
					,POS_LAST_LOC AS POS_LAST_LOC	
				FROM (',STR_SEL_IMSI_GRP_LTE,') B
				WHERE IMSI IS NOT NULL
				GROUP BY IMSI,DATA_DATE;');
		END IF;
		
		IF KPI_ID=110014 AND SPECIAL_IMSI<>1 THEN 
			SET @v_i=START_HOUR;
			SET @v_i_Max=END_HOUR;
			SET @v_j=0;
			WHILE @v_i <= @v_i_Max DO
			BEGIN	
				WHILE @v_j<60  DO
				BEGIN		
					IF (@v_i=@v_i_Max AND @v_j=45) THEN 
						SET @UNION=' '; 
					ELSE
						SET @UNION=' UNION ALL '; 
					END IF;	
					SET STR_SEL_IMSI_POS_LTE=CONCAT(STR_SEL_IMSI_POS_LTE,'SELECT ',STR_IMSI_POS_LTE,' FROM ',GT_DB,'.table_call_lte',CONCAT('_',LPAD(@v_i,2,'0'),LPAD(@v_j,2,'0')),' A WHERE A.DATA_HOUR=', @v_i,FILTER_STR,@UNION); 				
					SET @v_j=@v_j+15;
				END;
				END WHILE;				
				SET @v_j=0;
				SET @v_i=@v_i+1;
			END;
			END WHILE;	
			SET @SqlCmd=STR_SEL_IMSI_POS_LTE;		
		END IF;	
		IF KPI_ID=110014 AND SPECIAL_IMSI=1 THEN 			
			SET @v_i=START_HOUR;
			SET @v_i_Max=END_HOUR;
			SET @v_j=0;						
			WHILE @v_i <= @v_i_Max DO
			BEGIN	
				WHILE @v_j<60  DO
				BEGIN		
					IF (@v_i=@v_i_Max AND @v_j=45) THEN 
						SET @UNION=' '; 
					ELSE
						SET @UNION=' UNION ALL '; 
					END IF;	
					SET STR_SEL_IMSI_POS_LTE=CONCAT(STR_SEL_IMSI_POS_LTE,'SELECT ',STR_IMSI_POS_LTE,' FROM ',GT_DB,'.table_call_lte',CONCAT('_',LPAD(@v_i,2,'0'),LPAD(@v_j,2,'0'),'_imsig',ABS(IMSI_GID)), ' A WHERE A.DATA_HOUR=', @v_i,FILTER_STR,@UNION); 				
					SET @v_j=@v_j+15;
				END;
				END WHILE;				
				SET @v_j=0;
				SET @v_i=@v_i+1;
			END;
			END WHILE;	
			SET @SqlCmd=STR_SEL_IMSI_POS_LTE;		
		END IF;	
			
		IF KPI_ID=110015 THEN 
			SET @SqlCmd=CONCAT('
						SELECT 						
						  nt_tile.cell_id AS `CELL_ID`,
						  nt_tile.cell_name AS `CELL_NAME`,
						  nt_tile.ENODEB_ID AS `ENODEB_ID`,
						  nt_tile.ds_date AS `DATE`,
						  IFNULL(nt_tile.DL_DATA_THRU,0) AS `DL_DATA_THRU`,
						  IFNULL(nt_tile.UL_DATA_THRU,0) AS `UL_DATA_THRU`,
						  nt_tile.call_count AS `Call_Count`,
						  nt_tile.ACTIVE_STATE AS `ACTIVE_STATUS`,
						  nt_tile.Administrative_state,
						  nt_tile.OPERSTATE_ENABLE,
						',CASE WHEN PM_COUNTER_FLAG = 'true' THEN 
							 ' IFNULL(pm.Downtimeman,0),
							  IFNULL(pm.Downtimeauto,0), '
						 ELSE 
							' 0 AS Downtimeman,
							  0 AS Downtimeauto, '
						 END, '
						  ds_date,
						  ',PU_ID,' AS pu,
						  4 AS TECH_MASK 
						 FROM
						(SELECT 
							nt.PU_ID AS PU_ID,
							nt.ENODEB_ID AS ENODEB_ID,
							nt.CELL_ID AS CELL_ID,
							nt.CELL_NAME AS CELL_NAME,
							IFNULL(dat.dl,0) AS `DL_DATA_THRU`,
							IFNULL(dat.ul,0) AS `UL_DATA_THRU`,
							dat.call_count as `Call_Count`,
							nt.ACTIVE_STATE AS ACTIVE_STATE,	
							(CASE WHEN nt.CM_ADMIN_STATE=1 THEN ''Locked''
								WHEN nt.CM_ADMIN_STATE=0 THEN ''Unlocked''
								ELSE '''' END) AS Administrative_state,
							(CASE WHEN nt.CM_OPERATION_STATE=1 THEN ''Enable''
								WHEN nt.CM_OPERATION_STATE=0 THEN ''Disable''
								ELSE '''' END) AS OPERSTATE_ENABLE,													
							ds_date,
							',PU_ID,' AS pu,
							4 AS TECH_MASK 
							FROM ',NT_DB,'.nt2_cell_lte nt 
							LEFT JOIN
							(SELECT cell_id,ENODEB_ID,
								DATA_DATE AS ds_date,
									IFNULL(SUM(DL_VOLUME_SUM)/1024,0) AS dl,
									IFNULL(SUM(UL_VOLUME_SUM)/1024,0) AS ul,
									IFNULL(SUM(INITIAL_CALL_CNT),0) AS Call_Count
								FROM ',GT_DB,'.',CASE WHEN DY_FLAG=1 THEN ' rpt_cell_position_dy_def A' ELSE 'rpt_cell_position_def A' END,' 
								WHERE ',CASE WHEN FILTER_STR='' OR FILTER_STR IS NULL THEN '' ELSE CONCAT(FILTER_STR,'') END,' 
								GROUP BY a.cell_id,a.ENODEB_id
								HAVING dl=0 OR ul=0
							) dat 	
							ON dat.cell_id = nt.cell_id AND dat.ENODEB_id = nt.ENODEB_id 
							WHERE nt.PU_ID=',PU_ID,' 
							AND dl=0 or ul=0
						) nt_tile 
						',CASE WHEN PM_COUNTER_FLAG = 'true' THEN 
						CONCAT(' LEFT JOIN 	
							(
								SELECT ENODEB_id,
									cell_id,
									pmCellDowntimeMan AS `Downtimeman`,
									pmCellDowntimeAuto AS `Downtimeauto`
								FROM ',GT_DB,'.table_pm_ericsson_lte_aggr A 
-- 								WHERE A.PU_ID=',PU_ID,' 
								GROUP BY a.cell_id,a.ENODEB_id 						
							) pm
							ON pm.cell_id = nt_tile.cell_id AND pm.ENODEB_id = nt_tile.ENODEB_id ')
						 ELSE '' END ,'
						WHERE nt_tile.PU_ID=',PU_ID,' ;');
		END IF;
		
		IF KPI_ID=110016 THEN 
			SET @SqlCmd=CONCAT('
						SELECT 								
						  nt_tile.cell_id AS `CELL_ID`,
						  nt_tile.cell_name AS `CELL_NAME`,
						  nt_tile.ENODEB_ID AS `ENODEB_ID`,
						  nt_tile.ds_date AS `DATE`,
						  IFNULL(nt_tile.DL_DATA_THRU,0) AS `DL_DATA_THRU`,
						  IFNULL(nt_tile.UL_DATA_THRU,0) AS `UL_DATA_THRU`,
						  nt_tile.call_count AS `Call_Count`,
						  nt_tile.ACTIVE_STATE AS `ACTIVE_STATUS`,
						  nt_tile.Administrative_state,
						  nt_tile.OPERSTATE_ENABLE,
						',CASE WHEN PM_COUNTER_FLAG = 'true' THEN 
							 ' IFNULL(pm.Downtimeman,0),
							  IFNULL(pm.Downtimeauto,0), '
						 ELSE 
							' 0 AS Downtimeman,
							  0 AS Downtimeauto, '
						 END, '
						  ds_date,
						  ',PU_ID,' AS pu,
						  4 AS TECH_MASK 
						 FROM
				(	SELECT 
						  nt.PU_ID AS PU_ID,
						  nt.ENODEB_ID AS ENODEB_ID,
						  nt.CELL_ID AS CELL_ID,
						  nt.CELL_NAME AS CELL_NAME,
						  IFNULL(dat.dl,0) AS `DL_DATA_THRU`,
						  IFNULL(dat.ul,0) AS `UL_DATA_THRU`,
						  dat.call_count as `Call_Count`,
						nt.ACTIVE_STATE AS ACTIVE_STATE,	
						(CASE WHEN nt.CM_ADMIN_STATE=1 THEN ''Locked''
						WHEN nt.CM_ADMIN_STATE=0 THEN ''Unlocked''
						ELSE '''' END) AS Administrative_state,
						(CASE WHEN nt.CM_OPERATION_STATE=1 THEN ''Enable''
						WHEN nt.CM_OPERATION_STATE=0 THEN ''Disable''
						ELSE '''' END) AS OPERSTATE_ENABLE,													
						ds_date,
						 ',PU_ID,' AS pu,
						4 AS TECH_MASK 
						FROM ',NT_DB,'.nt2_cell_lte nt						
						LEFT JOIN	
						(SELECT cell_id,ENODEB_ID,
							DATA_DATE AS ds_date,
								IFNULL(SUM(DL_VOLUME_SUM)/1024,0) AS dl,
								IFNULL(SUM(UL_VOLUME_SUM)/1024,0) AS ul,
								IFNULL(SUM(INITIAL_CALL_CNT),0) AS Call_Count
							FROM ',GT_DB,'.',CASE WHEN DY_FLAG=1 THEN ' rpt_cell_position_dy_def A' ELSE 'rpt_cell_position_def A' END,' 
							WHERE ',CASE WHEN FILTER_STR='' OR FILTER_STR IS NULL THEN '' ELSE CONCAT(FILTER_STR,'') END,' 
							GROUP BY a.cell_id,a.ENODEB_id
 							HAVING dl=0 OR ul=0
						) dat 	
						ON dat.cell_id = nt.cell_id AND dat.ENODEB_id = nt.ENODEB_id 
						WHERE nt.PU_ID=',PU_ID,' 
  						AND dl=0 or ul=0) nt_tile 
						',CASE WHEN PM_COUNTER_FLAG = 'true' THEN 
						CONCAT('LEFT JOIN 						
							(
								SELECT ENODEB_id,
									cell_id,
									pmCellDowntimeMan AS `Downtimeman`,
									pmCellDowntimeAuto AS `Downtimeauto`
								FROM ',GT_DB,'.table_pm_ericsson_lte_aggr A 
-- 								WHERE A.PU_ID=',PU_ID,' 
								GROUP BY a.cell_id,a.ENODEB_id 						
							) pm
							ON pm.cell_id = nt_tile.cell_id AND pm.ENODEB_id = nt_tile.ENODEB_id ')
						 ELSE '' END ,'
						WHERE nt_tile.PU_ID=',PU_ID,' ;');
		END IF;	
		
		IF KPI_ID=110019 THEN 
			IF CELL_GID > 0  THEN 
				SET @SqlCmd=CONCAT('SELECT REPLACE(gt_strtok(`value`,3,'':''),''/'','''') ,REPLACE(gt_strtok(`value`,4,'':''),''/'','''') INTO @AP_IP,@AP_PORT FROM `gt_gw_main`.`integration_param` WHERE gt_group=''system'' AND gt_name=''apDbUri'';');			
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt; 
				
				SET @SqlCmd=CONCAT('SELECT `value` INTO @AP_USER FROM `gt_gw_main`.`integration_param` WHERE gt_group=''system'' AND gt_name=''apDbUser'';');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt; 
				
				SET @SqlCmd=CONCAT('SELECT `value` INTO @AP_PSWD FROM `gt_gw_main`.`integration_param` WHERE gt_group=''system'' AND gt_name=''apDbPass'';');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt; 
				
				SET @SqlCmd=CONCAT('DROP TABLE IF EXISTS gt_gw_main.tmp_cell_group_',WORKER_ID,' ;');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				
				SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS tmp_usr_cell_upload_',WORKER_ID,' ;');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
			
				SET @SqlCmd=CONCAT('CREATE TEMPORARY TABLE tmp_usr_cell_upload_',WORKER_ID,' 
							(
							  `group_id` int(11) NOT NULL,
							  `cell_id` int(11) NOT NULL,
							  `enodeb_id` int(11) NOT NULL,
							  `pu_id` int(11) DEFAULT NULL,
							  `cell_name` varchar(50) CHARACTER SET utf8 DEFAULT NULL,
							  `enodeb_name` varchar(50) CHARACTER SET utf8 DEFAULT NULL,
							  PRIMARY KEY (`group_id`,`cell_id`,`enodeb_id`)					  
							) ENGINE=FEDERATED DEFAULT CHARSET=latin1 CONNECTION=''mysql://',@AP_USER,':',@AP_PSWD,'@',@AP_IP,':',@AP_PORT,'/gt_covmo/usr_cell_upload''
							;');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
			
				SET @SqlCmd=CONCAT('CREATE TABLE gt_gw_main.tmp_cell_group_',WORKER_ID,' 
							(
							  `group_id` int(11) NOT NULL,
							  `cell_id` int(11) NOT NULL,
							  `enodeb_id` int(11) NOT NULL,
							  `pu_id` int(11) DEFAULT NULL,
							  KEY (`cell_id`,`enodeb_id`)					  
							) ENGINE=MYISAM;');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
			
				SET @SqlCmd=CONCAT('INSERT INTO gt_gw_main.tmp_cell_group_',WORKER_ID,' 
							(`group_id`,`pu_id`,`cell_id`,`enodeb_id`)
							SELECT `group_id`,`pu_id`,`cell_id`,`enodeb_id` FROM tmp_usr_cell_upload_',WORKER_ID,'
							WHERE `group_id`=',CELL_GID,' AND pu_id=',PU_ID,';');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
		
				SET @SqlCmd=CONCAT('SELECT 
					TILE_ID,
					NULL AS PCI,
					CELL_ID,
					ENODEB_ID,
					CELL_NAME 
					FROM ',GT_DB,'.',CASE WHEN  DY_FLAG=1 THEN 'rpt_tile_dominatecallcell_dy_def a'   ELSE 'rpt_tile_dominatecallcell_def a' END,'
					WHERE  ',FILTER_STR,'
					AND  EXISTS  (SELECT b.enodeb_id,b.cell_id FROM gt_gw_main.tmp_cell_group_',WORKER_ID,'  b  
					WHERE 1 AND
					a.enodeb_id= b.enodeb_id  AND a.cell_id = b.cell_id  ) GROUP BY tile_id   ORDER BY NULL
					;');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				
				SET @SqlCmd=CONCAT('DROP TABLE IF EXISTS gt_gw_main.tmp_cell_group_',WORKER_ID,' ;');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;	
			ELSE
				SET @SqlCmd=CONCAT('SELECT 
					TILE_ID,
					NULL AS PCI,
					CELL_ID,
					ENODEB_ID,
					CELL_NAME 
					FROM ',GT_DB,'.',CASE WHEN  DY_FLAG=1 THEN 'rpt_tile_dominatecallcell_dy_def'   ELSE 'rpt_tile_dominatecallcell_def' END,'
					WHERE  ',FILTER_STR,';');
			END IF;
		END IF;
		
		IF KPI_ID=110021 AND SPECIAL_IMSI<>1 THEN 	
			IF (DATA_DATE <> DATE(NOW())) THEN
				IF (START_HOUR=0 AND END_HOUR=23) THEN 
					SET DY_FLAG=1;	
				ELSE 
					SET DY_FLAG='';
				END IF;	
			ELSE 
				SET DY_FLAG='';
			END IF;	
			IF DY_FLAG=1 THEN 
				SET STR_SEL_IMSI_AGG_RPT=CONCAT(STR_SEL_IMSI_AGG_RPT,'SELECT ',STR_IMSI_AGG_RPT,' FROM ',GT_DB,'.table_imsi_aggregated_dy A WHERE 1 ',FILTER_STR,''); 				
			ELSE 			
				SET @v_i=START_HOUR;
				SET @v_i_Max=END_HOUR;
				SET @v_j=0;							
				WHILE @v_i <= @v_i_Max DO
				BEGIN 
					IF (@v_i=@v_i_Max ) THEN 
						SET @UNION=' '; 
					ELSE
						SET @UNION=' UNION ALL '; 
					END IF;	
					SET STR_SEL_IMSI_AGG_RPT=CONCAT(STR_SEL_IMSI_AGG_RPT,'SELECT ',STR_IMSI_AGG_RPT,' FROM ',GT_DB,'.table_imsi_aggregated_hr',CONCAT('_',LPAD(@v_i,2,'0')),' A  WHERE DATA_HOUR=', @v_i,FILTER_STR,'',@UNION); 
					SET @v_i=@v_i+1;
				END;
				END WHILE;				
			END IF;	
			SET @SqlCmd=STR_SEL_IMSI_AGG_RPT;
			SET @SqlCmd=CONCAT('
				SELECT  IMSI AS `IMSI`
					,DATA_DATE AS `DATA_DATE`
					,MAX(B.`HANDSET`) AS `HANDSET`
					,B.IMSI AS `MSISDN`
					,SUM(B.`Total_Call_Count`) AS `Total_Call_Count`
					,SUM(B.`Drop_Call_Count`) AS `Drop_Call_Count`
					,SUM(B.`Block_Call_Count`) AS `Block_Call_Count`
					,SUM(B.`Total_DL_Data_Volume`) AS `Total_DL_Data_Volume` 
					,SUM(B.`Total_UL_Data_Volume`) AS `Total_UL_Data_Volume`					
					,MAX(B.`MAX_DL_THROUGHPUT`) AS `MAX_DL_THROUGHPUT`				
					,MAX(B.`MAX_UL_THROUGHPUT`) AS `MAX_UL_THROUGHPUT`	
					,SUM(B.`UMTS_Call_Drop_Rate_1`) AS `UMTS_Call_Drop_Rate_1`
					,SUM(B.`UMTS_Call_Drop_Rate_2`) AS `UMTS_Call_Drop_Rate_2`
					,SUM(B.`UMTS_CSSR_1`) AS `UMTS_CSSR_1`
					,SUM(B.`UMTS_CSSR_2`) AS `UMTS_CSSR_2`
					,SUM(B.`UMTS_Blocked_Call_Rate_1`) AS `UMTS_Blocked_Call_Rate_1`
					,SUM(B.`UMTS_Blocked_Call_Rate_2`) AS `UMTS_Blocked_Call_Rate_2`
					,SUM(B.`UMTS_Soft_Handover_SR_1`) AS `UMTS_Soft_Handover_SR_1`
					,SUM(B.`UMTS_Soft_Handover_SR_2`) AS `UMTS_Soft_Handover_SR_2`
					,SUM(B.`UMTS_Softer_handover_SR_1`) AS `UMTS_Softer_handover_SR_1`
					,SUM(B.`UMTS_Softer_handover_SR_2`) AS `UMTS_Softer_handover_SR_2`
					,SUM(B.`UMTS_Inter_Freq_Handover_SR_1`)AS `UMTS_Inter_Freq_Handover_SR_1`
					,SUM(B.`UMTS_Inter_Freq_Handover_SR_2`) AS `UMTS_Inter_Freq_Handover_SR_2`
					,SUM(B.`UMTS_IRAT_handover_SR_1`) AS `UMTS_IRAT_handover_SR_1`
					,SUM(B.`UMTS_IRAT_handover_SR_2`) AS `UMTS_IRAT_handover_SR_2`
					,SUM(B.`LTE_Call_Drop_Rate_1`) AS `LTE_Call_Drop_Rate_1`
					,SUM(B.`LTE_Call_Drop_Rate_2`) AS `LTE_Call_Drop_Rate_2`
					,SUM(B.`LTE_CSSR_1`) AS `LTE_CSSR_1`
					,SUM(B.`LTE_CSSR_2`)  AS `LTE_CSSR_2`
					,SUM(B.`LTE_Blocked_Call_Rate_1`) AS `LTE_Blocked_Call_Rate_1` 
					,SUM(B.`LTE_Blocked_Call_Rate_2`) AS `LTE_Blocked_Call_Rate_2`
					,SUM(B.`LTE_Inter_Feq_Handover_SR_1`) AS `LTE_Inter_Feq_Handover_SR_1`
					,SUM(B.`LTE_Inter_Feq_Handover_SR_2`) AS `LTE_Inter_Feq_Handover_SR_2`
					,SUM(B.`LTE_Intra_Feq_Handover_SR_1`) AS `LTE_Intra_Feq_Handover_SR_1`
					,SUM(B.`LTE_Intra_Feq_Handover_SR_2`) AS `LTE_Intra_Feq_Handover_SR_2`
					,SUM(B.`LTE_IRAT_Handover_SR_3G_1`) AS `LTE_IRAT_Handover_SR_3G_1`
					,SUM(B.`LTE_IRAT_Handover_SR_3G_2`) AS `LTE_IRAT_Handover_SR_3G_2`
					,SUM(B.`LTE_IRAT_Handover_SR_2G_1`) AS `LTE_IRAT_Handover_SR_2G_1`
					,SUM(B.`LTE_IRAT_Handover_SR_2G_2`) AS `LTE_IRAT_Handover_SR_2G_2`
					,SUM(B.`GSM_Call_Drop_Rate_1`) AS `GSM_Call_Drop_Rate_1`
					,SUM(B.`GSM_Call_Drop_Rate_2`) AS `GSM_Call_Drop_Rate_2`
					,SUM(B.`GSM_CSSR_1`) AS `GSM_CSSR_1`
					,SUM(B.`GSM_CSSR_2`) AS `GSM_CSSR_2`
					,SUM(B.`GSM_Blocked_Call_Rate_1`) AS `GSM_Blocked_Call_Rate_1`
					,SUM(B.`GSM_Blocked_Call_Rate_2`) AS `GSM_Blocked_Call_Rate_2`
					,SUM(B.`GSM_Inter_Freq_Handover_SR_1`) AS `GSM_Inter_Freq_Handover_SR_1`
					,SUM(B.`GSM_Inter_Freq_Handover_SR_2`) AS `GSM_Inter_Freq_Handover_SR_2`
					,PU AS PU
					,DATA_DATE AS ds_date
					,MAX(TECH_MASK) AS `TECH_MASK`
					,MAX(FIRST_LAT_LON) AS `FIRST_LAT_LON`
					,MAX(LAST_LAT_LON) AS `LAST_LAT_LON`
					,POS_FIRST_RSCP_SUM AS POS_FIRST_RSCP_SUM
					,POS_FIRST_RSCP_CNT AS POS_FIRST_RSCP_CNT
					,POS_FIRST_ECN0_SUM AS POS_FIRST_ECN0_SUM
					,POS_FIRST_ECN0_CNT AS POS_FIRST_ECN0_CNT
					,POS_LAST_RSCP_SUM AS POS_LAST_RSCP_SUM
					,POS_LAST_RSCP_CNT AS POS_LAST_RSCP_CNT
					,POS_LAST_ECN0_SUM AS POS_LAST_ECN0_SUM
					,POS_LAST_ECN0_CNT AS POS_LAST_ECN0_CNT
					,POS_FIRST_S_RSRP_SUM AS POS_FIRST_S_RSRP_SUM
					,POS_FIRST_S_RSRP_CNT AS POS_FIRST_S_RSRP_CNT
					,POS_FIRST_S_RSRQ_SUM AS POS_FIRST_S_RSRQ_SUM
					,POS_FIRST_S_RSRQ_CNT AS POS_FIRST_S_RSRQ_CNT
					,POS_LAST_S_RSRP_SUM AS POS_LAST_S_RSRP_SUM
					,POS_LAST_S_RSRP_CNT AS POS_LAST_S_RSRP_CNT
					,POS_LAST_S_RSRQ_SUM AS POS_LAST_S_RSRQ_SUM
					,POS_LAST_S_RSRQ_CNT AS POS_LAST_S_RSRQ_CNT
				FROM (',STR_SEL_IMSI_AGG_RPT,') B
				WHERE IMSI IS NOT NULL
				GROUP BY IMSI;'); 
		ELSEIF KPI_ID=110021 AND SPECIAL_IMSI=1  THEN 			
			SET @v_i=START_HOUR;
			SET @v_i_Max=END_HOUR;
			SET @v_j=0;						
			WHILE @v_i <= @v_i_Max DO
			BEGIN	
				WHILE @v_j<60  DO
				BEGIN		
					IF (@v_i=@v_i_Max AND @v_j=45) THEN 
						SET @UNION=' '; 
					ELSE
						SET @UNION=' UNION ALL '; 
					END IF;
					SET STR_SEL_IMSI_GRP_LTE=CONCAT(STR_SEL_IMSI_GRP_LTE,'SELECT ',STR_IMSI_GRP_LTE,' FROM ',GT_DB,'.',TABLE_CALL_IMSI_LTE,'',CONCAT('_',LPAD(@v_i,2,'0'),LPAD(@v_j,2,'0'),'_imsig',ABS(IMSI_GID)), ' A ',CASE WHEN (IMSI<>'' OR IMSI_STR<> '') THEN ' FORCE INDEX (IMSI)' ELSE '' END,' WHERE DATA_HOUR=', @v_i,FILTER_STR,' GROUP BY IMSI ',@UNION); 
					SET @v_j=@v_j+15;				
				END;
				END WHILE;					
				SET @v_j=0;
				SET @v_i=@v_i+1;
			END;
			END WHILE;
			SET @SqlCmd=CONCAT('
				SELECT  IMSI AS `IMSI`
					,DATA_DATE AS `DATA_DATE`
					,MAX(B.HANDSET) AS `HANDSET`
					,IMSI AS `MSISDN`
					,SUM(B.Total_Call_Count) AS `Total_Call_Count`
					,SUM(B.Drop_Call_Count) AS `Drop_Call_Count`
					,SUM(B.Block_Call_Count) AS `Block_Call_Count`
					,SUM(B.Total_DL_Data_Volume) AS `Total_DL_Data_Volume`
					,SUM(B.Total_UL_Data_Volume) AS `Total_UL_Data_Volume`
					,MAX(B.MAX_DL_THROUGHPUT) AS `MAX_DL_THROUGHPUT`
					,MAX(B.MAX_UL_THROUGHPUT) AS `MAX_UL_THROUGHPUT`
					,NULL AS `UMTS_Call_Drop_Rate_1`
					,NULL AS `UMTS_Call_Drop_Rate_2`
					,NULL AS `UMTS_CSSR_1`
					,NULL AS `UMTS_CSSR_2`
					,NULL AS `UMTS_Blocked_Call_Rate_1`
					,NULL AS `UMTS_Blocked_Call_Rate_2`
					,NULL AS `UMTS_Soft_Handover_SR_1`
					,NULL AS `UMTS_Soft_Handover_SR_2`
					,NULL AS `UMTS_Softer_handover_SR_1`
					,NULL AS `UMTS_Softer_handover_SR_2`
					,NULL AS `UMTS_Inter_Freq_Handover_SR_1`
					,NULL AS `UMTS_Inter_Freq_Handover_SR_2`
					,NULL AS `UMTS_IRAT_handover_SR_1`
					,NULL AS `UMTS_IRAT_handover_SR_2`
					,SUM(B.LTE_Call_Drop_Rate_1) AS `LTE_Call_Drop_Rate_1`
					,SUM(B.LTE_Call_Drop_Rate_2) AS `LTE_Call_Drop_Rate_2`
					,SUM(B.LTE_CSSR_1) AS `LTE_CSSR_1`
					,SUM(B.LTE_CSSR_2) AS `LTE_CSSR_2`
					,SUM(B.LTE_Blocked_Call_Rate_1) AS `LTE_Blocked_Call_Rate_1` 
					,SUM(B.LTE_Blocked_Call_Rate_2) AS `LTE_Blocked_Call_Rate_2`
					,SUM(B.LTE_Inter_Feq_Handover_SR_1) AS `LTE_Inter_Feq_Handover_SR_1`
					,SUM(B.LTE_Inter_Feq_Handover_SR_2) AS `LTE_Inter_Feq_Handover_SR_2`
					,SUM(B.LTE_Intra_Feq_Handover_SR_1) AS `LTE_Intra_Feq_Handover_SR_1`
					,SUM(B.LTE_Intra_Feq_Handover_SR_2) AS `LTE_Intra_Feq_Handover_SR_2`
					,SUM(B.LTE_IRAT_Handover_SR_3G_1) AS `LTE_IRAT_Handover_SR_3G_1`
					,SUM(B.LTE_IRAT_Handover_SR_3G_2) AS `LTE_IRAT_Handover_SR_3G_2`
					,SUM(B.LTE_IRAT_Handover_SR_2G_1) AS `LTE_IRAT_Handover_SR_2G_1`
					,SUM(B.LTE_IRAT_Handover_SR_2G_2) AS `LTE_IRAT_Handover_SR_2G_2`
					,NULL AS `GSM_Call_Drop_Rate_1`
					,NULL AS `GSM_Call_Drop_Rate_2`
					,NULL AS `GSM_CSSR_1`
					,NULL AS `GSM_CSSR_2`
					,NULL AS `GSM_Blocked_Call_Rate_1`
					,NULL AS `GSM_Blocked_Call_Rate_2`
					,NULL AS `GSM_Inter_Freq_Handover_SR_1`
					,NULL AS `GSM_Inter_Freq_Handover_SR_2`
					,PU AS PU
					,ds_date
					,MAX(TECH_MASK) AS `TECH_MASK`
					,CONCAT(gt_covmo_proj_geohash_to_lng(POS_FIRST_LOC),''|'',gt_covmo_proj_geohash_to_lat(POS_FIRST_LOC)) AS `FIRST_LAT_LON`
					,CONCAT(gt_covmo_proj_geohash_to_lng(POS_LAST_LOC),''|'',gt_covmo_proj_geohash_to_lat(POS_LAST_LOC)) AS `FIRST_LAT_LON`			
					,NULL AS POS_FIRST_RSCP_SUM
					,NULL AS POS_FIRST_RSCP_CNT
					,NULL AS POS_FIRST_ECN0_SUM
					,NULL AS POS_FIRST_ECN0_CNT
					,NULL AS POS_LAST_RSCP_SUM
					,NULL AS POS_LAST_RSCP_CNT
					,NULL AS POS_LAST_ECN0_SUM
					,NULL AS POS_LAST_ECN0_CNT
					,POS_FIRST_S_RSRP_SUM AS POS_FIRST_S_RSRP_SUM
					,POS_FIRST_S_RSRP_CNT AS POS_FIRST_S_RSRP_CNT
					,POS_FIRST_S_RSRQ_SUM AS POS_FIRST_S_RSRQ_SUM
					,POS_FIRST_S_RSRQ_CNT AS POS_FIRST_S_RSRQ_CNT
					,POS_LAST_S_RSRP_SUM AS POS_LAST_S_RSRP_SUM
					,POS_LAST_S_RSRP_CNT AS POS_LAST_S_RSRP_CNT
					,POS_LAST_S_RSRQ_SUM AS POS_LAST_S_RSRQ_SUM
					,POS_LAST_S_RSRQ_CNT AS POS_LAST_S_RSRQ_CNT
					,NULL AS POS_FIRST_RSCP_SUM
					,NULL AS POS_FIRST_RSCP_CNT
					,NULL AS POS_FIRST_ECN0_SUM
					,NULL AS POS_FIRST_ECN0_CNT
					,NULL AS POS_LAST_RSCP_SUM
					,NULL AS POS_LAST_RSCP_CNT
					,NULL AS POS_LAST_ECN0_SUM
					,NULL AS POS_LAST_ECN0_CNT
					,SUM(POS_FIRST_S_RSRP_SUM) AS POS_FIRST_S_RSRP_SUM
					,SUM(POS_FIRST_S_RSRP_CNT) AS POS_FIRST_S_RSRP_CNT
					,SUM(POS_FIRST_S_RSRQ_SUM) AS POS_FIRST_S_RSRQ_SUM
					,SUM(POS_FIRST_S_RSRQ_CNT) AS POS_FIRST_S_RSRQ_CNT
					,SUM(POS_LAST_S_RSRP_SUM) AS POS_LAST_S_RSRP_SUM
					,SUM(POS_LAST_S_RSRP_CNT) AS POS_LAST_S_RSRP_CNT
					,SUM(POS_LAST_S_RSRQ_SUM) AS POS_LAST_S_RSRQ_SUM
					,SUM(POS_LAST_S_RSRQ_CNT) AS POS_LAST_S_RSRQ_CNT
				FROM (',STR_SEL_IMSI_GRP_LTE,') B
				WHERE IMSI IS NOT NULL
				GROUP BY IMSI;'); 
		END IF;	
		
		IF KPI_ID=110022 THEN 
			SET @SqlCmd=CONCAT('SELECT ENODEB_ID,CELL_ID FROM ',GT_DB,'.rpt_cell_start_dy_def GROUP BY ENODEB_ID,CELL_ID;');
		END IF;	
		
		IF KPI_ID=110023 THEN 
			SET @SqlCmd=CONCAT('SELECT 
			  gt_covmo_distance (
			      nt.LONGITUDE,
			      nt.LATITUDE,
			      gt_covmo_proj_geohash_to_lng (fact.POS_LAST_LOC),
			      gt_covmo_proj_geohash_to_lat (fact.POS_LAST_LOC)
			    ) / 1000 DIV 1 + 1 AS distance,
				COUNT(*) AS COUNT 
			    FROM
			       ',GT_DB,'.table_call_lte AS fact LEFT JOIN ',NT_DB,'.nt_antenna_current_lte AS nt ON nt.CELL_ID=fact.POS_LAST_S_CELL AND nt.ENODEB_ID=fact.POS_LAST_S_ENODEB
				WHERE IRAT_TO_UMTS_ATTEMPT > 0 AND POS_LAST_S_CELL = ',CELL_GID,' AND POS_LAST_S_ENODEB = ',ENODEB_ID,'
				GROUP BY distance ;');
		END IF;
		
		IF KPI_ID=110025 THEN 
			SET @v_i=START_HOUR;
			SET @v_i_Max=END_HOUR;
			SET @v_j=0;						
			WHILE @v_i <= @v_i_Max DO
			BEGIN	
				WHILE @v_j<60  DO
				BEGIN		
					IF (@v_i=@v_i_Max AND @v_j=45) THEN 
						SET @UNION=' '; 
					ELSE
						SET @UNION=' UNION ALL '; 
					END IF;		
					SET STR_SEL_IMSI_MR_LTE=CONCAT(STR_SEL_IMSI_MR_LTE,'SELECT ',STR_IMSI_MR_LTE,' FROM ',GT_DB,'.',TABLE_POS_IMSI_LTE,'',CONCAT('_',LPAD(@v_i,2,'0'),LPAD(@v_j,2,'0')),' A ',CASE WHEN (IMSI<>'' OR IMSI_STR<> '') THEN ' FORCE INDEX (IX_IMSI)' ELSE '' END,' WHERE DATA_HOUR=', @v_i,FILTER_STR,@UNION); 
					SET @v_j=@v_j+15;
				END;
				END WHILE;					
				SET @v_j=0;
				SET @v_i=@v_i+1;
			END;
			END WHILE;	
			SET @SqlCmd=STR_SEL_IMSI_MR_LTE;	
		END IF;
		
		IF KPI_ID=110026 THEN 
			SET @v_i=START_HOUR;
			SET @v_i_Max=END_HOUR;
			SET @v_j=0;
			WHILE @v_i <= @v_i_Max DO
			BEGIN	
				WHILE @v_j<60  DO
				BEGIN		
					IF (@v_i=@v_i_Max AND @v_j=45) THEN 
						SET @UNION=' '; 
					ELSE
						SET @UNION=' UNION ALL '; 
					END IF;
					SET STR_SEL_IMSI_MR_GRP_LTE=CONCAT(STR_SEL_IMSI_MR_GRP_LTE,'SELECT ',STR_IMSI_MR_GRP_LTE,' FROM ',GT_DB,'.',TABLE_POS_IMSI_LTE,'',CONCAT('_',LPAD(@v_i,2,'0'),LPAD(@v_j,2,'0')),' A ',CASE WHEN (IMSI<>'' OR IMSI_STR<> '') THEN ' FORCE INDEX (IX_IMSI)' ELSE '' END,' WHERE DATA_HOUR=', @v_i,FILTER_STR,' GROUP BY CALL_ID ',@UNION); 
					SET @v_j=@v_j+15;
				END;
				END WHILE;					
				SET @v_j=0;
				SET @v_i=@v_i+1;
			END;
			END WHILE;
			
			SET @SqlCmd=CONCAT('
				SELECT  CALL_ID,
							IMSI,
							HANDSET,
							START_TIME,
							END_TIME,
							CALL_TYPE,
							CALL_STATUS,
							RSRP_SUM,
							RSRP_CNT,
							RSRQ_SUM,
							RSRQ_CNT,
							CQI,
							CQI_SAMPLE,
							SINR_PUSCH,
							SINR_PUSCH_SAMPLE,
							SINR_PUCCH,
							SINR_PUCCH_SAMPLE,
							UL_VOLUME,
							UL_TRAFFIC_DUR,
							DL_VOLUME,
							DL_TRAFFIC_DUR,
							PU AS PU_ID,
							LONGITUDE,
							LATITUDE,
							DATA_DATE
				FROM (',STR_SEL_IMSI_MR_GRP_LTE,') B;'); 
		END IF;
		IF KPI_ID=110027 THEN 
			IF SPECIAL_IMSI<>1 THEN 
				SET @v_i=START_HOUR;
				SET @v_i_Max=END_HOUR;
				SET @v_j=0;
				WHILE @v_i <= @v_i_Max DO
				BEGIN	
					WHILE @v_j<60  DO
					BEGIN		
						IF (@v_i=@v_i_Max AND @v_j=45) THEN 
							SET @UNION=' '; 
						ELSE
							SET @UNION=' UNION ALL '; 
						END IF;
						SET STR_SEL_VIP_GRP_LTE=CONCAT(STR_SEL_VIP_GRP_LTE,'SELECT ',STR_VIP_GRP_LTE,' FROM ',GT_DB,'.',TABLE_CALL_IMSI_LTE,'',CONCAT('_',LPAD(@v_i,2,'0'),LPAD(@v_j,2,'0')),' A ',CASE WHEN (IMSI<>'' OR IMSI_STR<> '') THEN ' FORCE INDEX (IMSI)' ELSE '' END,' WHERE DATA_HOUR=', @v_i,FILTER_STR,' AND A.`POS_LAST_TILE`>0 ',@UNION); 
						SET @v_j=@v_j+15;
					END;
					END WHILE;					
					SET @v_j=0;
					SET @v_i=@v_i+1;
				END;
				END WHILE;
			ELSEIF SPECIAL_IMSI=1  THEN 
				SET @v_i=START_HOUR;
				SET @v_i_Max=END_HOUR;
				SET @v_j=0;
				WHILE @v_i <= @v_i_Max DO
				BEGIN	
					WHILE @v_j<60  DO
					BEGIN		
						IF (@v_i=@v_i_Max AND @v_j=45) THEN 
							SET @UNION=' '; 
						ELSE
							SET @UNION=' UNION ALL '; 
						END IF;
						SET STR_SEL_VIP_GRP_LTE=CONCAT(STR_SEL_VIP_GRP_LTE,'SELECT ',STR_VIP_GRP_LTE,' FROM ',GT_DB,'.',TABLE_CALL_IMSI_LTE,'',CONCAT('_',LPAD(@v_i,2,'0'),LPAD(@v_j,2,'0'),'_imsig',ABS(IMSI_GID)), ' A ',CASE WHEN (IMSI<>'' OR IMSI_STR<> '') THEN ' FORCE INDEX (IMSI)' ELSE '' END,' WHERE DATA_HOUR=', @v_i,FILTER_STR,@UNION); 
						SET @v_j=@v_j+15;				
					END;
					END WHILE;					
					SET @v_j=0;
					SET @v_i=@v_i+1;
				END;
				END WHILE;
			END IF;
			SET @SqlCmd=CONCAT('
				SELECT  B.IMSI AS `IMSI`,
					B.DATA_DATE AS `DATA_DATE`,
					B.IMEI AS `IMEI`,
					SUM(B.TOTAL_CNT) AS TOTAL_CNT,
					SUM(B.DROP_CNT) AS DROP_CNT,
					SUM(B.NB_TOTAL_CNT) AS NB_TOTAL_CNT,
					SUM(B.BLOCK_CNT) AS BLOCK_CNT,
					SUM(B.CSF_CNT) AS CSF_CNT,
					SUM(B.CSS_CNT) AS CSS_CNT,
					SUM(B.UMTS_IRAT_HHO_ATTEMPT) AS UMTS_IRAT_HHO_ATTEMPT,
					SUM(B.UMTS_IRAT_HHO_FAIL) AS UMTS_IRAT_HHO_FAIL,
					SUM(B.UMTS_IRAT_HHO_SUCCESS) AS UMTS_IRAT_HHO_SUCCESS,
					SUM(B.UMTS_BLOCK_CNT) AS UMTS_BLOCK_CNT,
					SUM(B.UMTS_TOTAL_CNT) AS UMTS_TOTAL_CNT,
					SUM(B.UMTS_CSS_CNT) AS UMTS_CSS_CNT,
					SUM(B.UMTS_CSF_CNT) AS UMTS_CSF_CNT,
					SUM(B.UMTS_DROP_CNT) AS UMTS_DROP_CNT,
					SUM(B.UMTS_NB_TOTAL_CNT) AS UMTS_NB_TOTAL_CNT,
					SUM(B.LTE_IRAT_TO_UMTS_ATTEMPT) AS LTE_IRAT_TO_UMTS_ATTEMPT,
					SUM(B.LTE_IRAT_TO_UMTS_FAILURE) AS LTE_IRAT_TO_UMTS_FAILURE,
					SUM(B.LTE_BLOCK_CNT) AS LTE_BLOCK_CNT,
					SUM(B.LTE_TOTAL_CNT) AS LTE_TOTAL_CNT,
					SUM(B.LTE_CSS_CNT) AS LTE_CSS_CNT,
					SUM(B.LTE_CSF_CNT) AS LTE_CSF_CNT,
					SUM(B.LTE_DROP_CNT) AS LTE_DROP_CNT,
					SUM(B.LTE_NB_TOTAL_CNT) AS LTE_NB_TOTAL_CNT,
					B.PU AS PU,
					B.DATA_DATE AS ds_date,
					B.TECH_MASK AS `TECH_MASK`,
					MAX(CONCAT(IFNULL(B.POS_FIRST_LON,'' ''),''|'',IFNULL(B.POS_FIRST_LAT,'' ''))) AS FIRST_LON_LAT,
					MAX(CONCAT(IFNULL(B.POS_LAST_LON,'' ''),''|'',IFNULL(B.POS_LAST_LAT,'' ''))) AS LAST_LON_LAT
				FROM (',STR_SEL_VIP_GRP_LTE,') B
				GROUP BY IMSI;'); 
		END IF;		
	END;	
	ELSEIF @TECHNOLOGY='GSM' THEN 
	BEGIN
		IF KPI_ID=110001 THEN 
			SET @SqlCmd=CONCAT('
					SELECT CALL_ID
						,CONCAT(A.START_TIME,''.'',LPAD(A.START_TIME_MS,3,0)) AS `START_TIME`
						,CONCAT(A.END_TIME,''.'',LPAD(A.END_TIME_MS,3,0)) AS `END_TIME`
						,ROUND((`DURATION`/1000),2) AS DURATION
						,NULL AS `CS_TRAFFIC_TIME`
						,A.CALL_SETUP_TIME AS `CALL_SETUP_TIME`
						,A.IMSI AS `IMSI`
						,A.IMEI AS `IMEI`
						,A.IMSI AS `MSISDN`
						,A.`MAKE_ID`
						,A.`MODEL_ID`
						,1 AS `TECH_MASK`
						,(CASE WHEN call_type=10 THEN ''Voice'' WHEN call_type=11 THEN ''CS_Data'' 
							WHEN call_type=13 THEN ''PS_Data'' WHEN call_type=14 THEN ''Multi RAB''  
							WHEN call_type=15 THEN ''Signalling'' WHEN call_type=16 THEN ''SMS'' 
							WHEN call_type=99 THEN ''Others'' ELSE ''Unkonwn'' END) AS call_type
						,APN AS `APN`
						,(CASE WHEN call_status=1 THEN ''Normal'' 
							WHEN call_status=2 THEN ''Drop'' 
							WHEN call_status=3 THEN ''Block'' 
							WHEN call_status=6 THEN ''SetupFailure'' 
							ELSE ''Unspecified'' END) AS call_status
						,IFNULL(C.`CAUSE_NAME`, ''N/A'') AS `RELEASE_CAUSE`
						,CONCAT(A.`MCC`,''-'',A.`MNC`,''-'',A.`START_BSC_ID`,''-'',A.`START_CELL_ID`) AS `START_CELL`
						,A.POS_FIRST_LOC AS `POS_FIRST_LOC`
						,POS_FIRST_RXLEV_FULL_DOWNLINK AS `START_RXLEV_RSCP_RSRP_dBn`
						,POS_FIRST_RXQUAL_FULL_DOWNLINK AS `START_RXQUAL_ECN0_RSRQ_dB`
						,CONCAT(A.`MCC`,''-'',A.`MNC`,''-'',A.`POS_LAST_BSC`,''-'',A.`POS_LAST_CELL`) AS `END_CELL`
						,POS_LAST_LOC AS `POS_LAST_LOC`
						,POS_LAST_RXLEV_FULL_DOWNLINK AS `END_RXLEV_RSCP_RSRP_dBm`
						,POS_LAST_RXQUAL_FULL_DOWNLINK AS `END_RXQUAL_ECN0_RSRQ_dB`
						,NULL AS `DL_TRAFFIC_VOLUME_MB`
						,NULL AS `DL_THROUGHPUT_Kbps`
						,NULL AS `DL_THROUGHPUT_MAX_Kbps`
						,NULL AS `UL_TRAFFIC_VOLUME_MB`
						,NULL AS `UL_THROUGHPUT_KBPS`
						,NULL AS `UL_THROUGHPUT_MAX_Kbps`
						,NULL AS `INTRA_FREQ_HO_ATTEMPT`	
						,NULL AS `INTRA_FREQ_HO_FAILURE`
						,HO_ATTEMPT_COUNT AS INTER_FREQ_HO_ATTEMPT
						,HO_FAILURE_COUNT AS INTER_FREQ_HO_FAILURE
						,NULL AS `IRAT_HO_ATTEMPT`
						,NULL AS `IRAT_HO_FAILURE`
						,(CASE WHEN A.`INDOOR`=1 THEN ''Indoor'' WHEN A.`INDOOR`=0 AND A.`MOVING`=0 THEN ''Stationary'' 
						WHEN A.`INDOOR`=0 AND A.`MOVING`=1 THEN ''Moving'' WHEN A.`INDOOR`=0 AND A.`MOVING` IS NULL THEN ''Outdoor'' ELSE ''N/A'' END) AS `INDOOR`
						,`MOVING` AS MOVING
						,(CASE WHEN A.`MOVING_TYPE`=1 THEN ''Walking'' WHEN A.`MOVING_TYPE`=2 THEN ''In Vehicle'' ELSE ''N/A'' END)AS `MOVING_TYPE`
						,NULL AS B_PARTY_NUMBER
						,`DATA_DATE` AS DATA_DATE
						,`DATA_HOUR` AS DATA_HOUR
						,RIGHT(CONCAT(''00'',A.BATCH),4) AS `BATCH`
						,NULL AS `CELL_UPDATE_CAUSE`						
						,NULL AS RAB_SEQ_ID
						,DATA_DATE AS ds_date
						,POS_FIRST_CELL
						,POS_LAST_CELL
						,START_CELL_ID
						,END_CELL_ID
						,NULL AS `MANUFACTURER`
						,NULL AS `MODEL`
						,',PU_ID,' AS pu					
					FROM ',GT_DB,'.',TABLE_CALL_IMSI_GSM,' A 
					LEFT JOIN gt_covmo.`dim_release_result_cause` C
					ON A.`RELEASE_RESULT_CAUSE`=C.`CAUSE_CODE`
					WHERE ',FILTER_STR,
				CASE  WHEN WITHDUMP=1 AND SPECIAL_IMSI <>1 THEN 
					CONCAT(' UNION ALL
						SELECT CALL_ID
						,A.START_TIME AS `START_TIME`
						,A.END_TIME AS `END_TIME`
						,ROUND((`DURATION`/1000),2) AS DURATION
						,NULL AS `CS_TRAFFIC_TIME`
						,A.CALL_SETUP_TIME AS `CALL_SETUP_TIME`
						,A.IMSI AS `IMSI`
						,A.IMEI AS `IMEI`
						,A.IMSI AS `MSISDN`
						,NULL AS `MAKE_ID`
						,NULL AS `MODEL_ID`
						,1 AS `TECH_MASK`
						,(CASE WHEN call_type=10 THEN ''Voice'' WHEN call_type=11 THEN ''CS_Data'' 
							WHEN call_type=13 THEN ''PS_Data'' WHEN call_type=14 THEN ''Multi RAB''  
							WHEN call_type=15 THEN ''Signalling'' WHEN call_type=16 THEN ''SMS'' 
							WHEN call_type=99 THEN ''Others'' ELSE ''Unkonwn'' END) AS call_type
						,APN AS `APN`
						,(CASE WHEN call_status=1 THEN ''Normal'' 
							WHEN call_status=2 THEN ''Drop'' 
							WHEN call_status=3 THEN ''Block'' 
							WHEN call_status=6 THEN ''SetupFailure'' 
							ELSE ''Unspecified'' END) AS call_status
						,IFNULL(C.`CAUSE_NAME`, ''N/A'') AS `RELEASE_CAUSE`
						,CONCAT(A.`MCC`,''-'',A.`MNC`,''-'',A.`START_BSC_ID`,''-'',A.`START_CELL_ID`) AS `START_CELL`
						,A.POS_FIRST_LOC AS `POS_FIRST_LOC`
						,POS_FIRST_RXLEV_FULL_DOWNLINK AS `START_RXLEV_RSCP_RSRP_dBn`
						,POS_FIRST_RXQUAL_FULL_DOWNLINK AS `START_RXQUAL_ECN0_RSRQ_dB`
						,CONCAT(A.`MCC`,''-'',A.`MNC`,''-'',A.`POS_LAST_BSC`,''-'',A.`POS_LAST_CELL`) AS `END_CELL`
						,POS_LAST_LOC AS `POS_LAST_LOC`
						,POS_LAST_RXLEV_FULL_DOWNLINK AS `END_RXLEV_RSCP_RSRP_dBm`
						,POS_LAST_RXQUAL_FULL_DOWNLINK AS `END_RXQUAL_ECN0_RSRQ_dB`
						,NULL AS `DL_TRAFFIC_VOLUME_MB`
						,NULL AS `DL_THROUGHPUT_Kbps`
						,NULL AS `DL_THROUGHPUT_MAX_Kbps`
						,NULL AS `UL_TRAFFIC_VOLUME_MB`
						,NULL AS `UL_THROUGHPUT_KBPS`
						,NULL AS `UL_THROUGHPUT_MAX_Kbps`
						,NULL AS `INTRA_FREQ_HO_ATTEMPT`	
						,NULL AS `INTRA_FREQ_HO_FAILURE`
						,HO_ATTEMPT_COUNT AS INTER_FREQ_HO_ATTEMPT
						,HO_FAILURE_COUNT AS INTER_FREQ_HO_FAILURE
						,NULL AS `IRAT_HO_ATTEMPT`
						,NULL AS `IRAT_HO_FAILURE`
						,(CASE WHEN A.`INDOOR`=1 THEN ''Indoor'' WHEN A.`INDOOR`=0 AND A.`MOVING`=0 THEN ''Stationary'' 
						WHEN A.`INDOOR`=0 AND A.`MOVING`=1 THEN ''Moving'' WHEN A.`INDOOR`=0 AND A.`MOVING` IS NULL THEN ''Outdoor'' ELSE ''N/A'' END) AS `INDOOR`
						,`MOVING` AS MOVING
						,(CASE WHEN A.`MOVING_TYPE`=1 THEN ''Walking'' WHEN A.`MOVING_TYPE`=2 THEN ''In Vehicle'' ELSE ''N/A'' END)AS `MOVING_TYPE`
						,NULL AS B_PARTY_NUMBER
						,`DATA_DATE` AS DATA_DATE
						,`DATA_HOUR` AS DATA_HOUR
						,RIGHT(CONCAT(''00'',A.BATCH),4) AS `BATCH`
						,NULL AS  `CELL_UPDATE_CAUSE`	
						,NULL AS RAB_SEQ_ID
						,DATA_DATE AS ds_date
						,POS_FIRST_CELL
						,POS_LAST_CELL
						,START_CELL_ID
						,END_CELL_ID
						,NULL,NUll
						,',PU_ID,' AS pu					
						FROM ',GT_DB,'.table_call_nopos_gsm A 
						LEFT JOIN gt_covmo.`dim_release_result_cause` C
						ON A.`RELEASE_RESULT_CAUSE`=C.`CAUSE_CODE`
						WHERE ',FILTER_STR)
				ELSE '' END,
				';');
		END IF;		
		IF KPI_ID=110005 THEN 
			SET @SqlCmd=CONCAT('
					SELECT 
						A.IMSI AS `IMSI`
						,A.DATA_DATE AS `DATA_DATE`
						,MAX(CONCAT(IFNULL(A.MAKE_ID,'' ''),''|'',IFNULL(A.MODEL_ID,'' ''),''|'',IFNULL(A.IMEI,'' ''))) AS `HANDSET`
						,A.IMSI AS `MSISDN`
						,COUNT(A.CALL_ID) AS `Total_Call_Count`
						,SUM(IF(A.CALL_STATUS=2,1,0)) AS `Drop_Call_Count`
						,SUM(IF(A.CALL_STATUS=3,1,0)) AS `Block_Call_Count`
						,NULL AS `Total_DL_Data_Volume`
						,NULL AS `Total_UL_Data_Volume`
						,NULL AS `MAX_DL_THROUGHPUT`
						,NULL AS `MAX_UL_THROUGHPUT`
						,NULL AS `UMTS_Call_Drop_Rate_1`
						,NULL AS `UMTS_Call_Drop_Rate_2`
						,NULL AS `UMTS_CSSR_1`
						,NULL AS `UMTS_CSSR_2`
						,NULL AS `UMTS_Blocked_Call_Rate_1`
						,NULL AS `UMTS_Blocked_Call_Rate_2`
						,NULL AS `UMTS_Soft_Handover_SR_1`
						,NULL AS `UMTS_Soft_Handover_SR_2`
						,NULL AS `UMTS_Softer_handover_SR_1`
						,NULL AS `UMTS_Softer_handover_SR_2`
						,NULL AS `UMTS_Inter_Freq_Handover_SR_1`
						,NULL AS `UMTS_Inter_Freq_Handover_SR_2`
						,NULL AS `UMTS_IRAT_handover_SR_1`
						,NULL AS `UMTS_IRAT_handover_SR_2`
						,NULL AS `LTE_Call_Drop_Rate_1`
						,NULL AS `LTE_Call_Drop_Rate_2`
						,NULL AS `LTE_CSSR_1`
						,NULL AS `LTE_CSSR_2`
						,NULL AS `LTE_Blocked_Call_Rate_1` 
						,NULL AS `LTE_Blocked_Call_Rate_2`
						,NULL AS `LTE_Inter_Feq_Handover_SR_1`
						,NULL AS `LTE_Inter_Feq_Handover_SR_2`
						,NULL AS `LTE_Intra_Feq_Handover_SR_1`
						,NULL AS `LTE_Intra_Feq_Handover_SR_2`
						,NULL AS `LTE_IRAT_Handover_SR_3G_1`
						,NULL AS `LTE_IRAT_Handover_SR_3G_2`
						,NULL AS `LTE_IRAT_Handover_SR_2G_1`
						,NULL AS `LTE_IRAT_Handover_SR_2G_2`
						,IFNULL(SUM(IF(A.call_status=2,1,0)),0) AS `GSM_Call_Drop_Rate_1`
						,IFNULL(SUM(IF(A.call_status IN (1,2,4),1,0)),0) AS `GSM_Call_Drop_Rate_2`
						,IFNULL(SUM(IF(A.call_status IN (3,6),1,0)),0) AS `GSM_CSSR_1`
						,COUNT(*) AS `GSM_CSSR_2`
						,IFNULL(SUM(IF(A.call_status=3,1,0)),0) AS `GSM_Blocked_Call_Rate_1`
						,COUNT(*) AS `GSM_Blocked_Call_Rate_2`
						,IFNULL(SUM(HO_FAILURE_COUNT),0) AS `GSM_Inter_Freq_Handover_SR_1`
						,IFNULL(SUM(HO_ATTEMPT_COUNT),0) AS `GSM_Inter_Freq_Handover_SR_2`
						,',PU_ID,' AS PU
						,DATA_DATE AS ds_date
						,1 AS `TECH_MASK`
						,A.POS_FIRST_LOC AS POS_FIRST_LOC
						,A.POS_LAST_LOC AS POS_LAST_LOC 
					FROM ',GT_DB,'.',TABLE_CALL_IMSI_GSM,' A 
					WHERE ',FILTER_STR, ' GROUP BY IMSI ',
				CASE  WHEN WITHDUMP=1 AND SPECIAL_IMSI <>1 THEN 
					CONCAT(' UNION ALL
						SELECT A.IMSI AS `IMSI`
							,A.DATA_DATE AS `DATA_DATE`
							,NULL AS `HANDSET`
							,A.IMSI AS `MSISDN`
							,COUNT(A.CALL_ID) AS `Total_Call_Count`
							,SUM(IF(A.CALL_STATUS=2,1,0)) AS `Drop_Call_Count`
							,SUM(IF(A.CALL_STATUS=3,1,0)) AS `Block_Call_Count`
							,NULL AS `Total_DL_Data_Volume`
							,NULL AS `Total_UL_Data_Volume`
							,NULL AS `MAX_DL_THROUGHPUT`
							,NULL AS `MAX_UL_THROUGHPUT`
							,NULL AS `UMTS_Call_Drop_Rate_1`
							,NULL AS `UMTS_Call_Drop_Rate_2`
							,NULL AS `UMTS_CSSR_1`
							,NULL AS `UMTS_CSSR_2`
							,NULL AS `UMTS_Blocked_Call_Rate_1`
							,NULL AS `UMTS_Blocked_Call_Rate_2`
							,NULL AS `UMTS_Soft_Handover_SR_1`
							,NULL AS `UMTS_Soft_Handover_SR_2`
							,NULL AS `UMTS_Softer_handover_SR_1`
							,NULL AS `UMTS_Softer_handover_SR_2`
							,NULL AS `UMTS_Inter_Freq_Handover_SR_1`
							,NULL AS `UMTS_Inter_Freq_Handover_SR_2`
							,NULL AS `UMTS_IRAT_handover_SR_1`
							,NULL AS `UMTS_IRAT_handover_SR_2`
							,NULL AS `LTE_Call_Drop_Rate_1`
							,NULL AS `LTE_Call_Drop_Rate_2`
							,NULL AS `LTE_CSSR_1`
							,NULL AS `LTE_CSSR_2`
							,NULL AS `LTE_Blocked_Call_Rate_1` 
							,NULL AS `LTE_Blocked_Call_Rate_2`
							,NULL AS `LTE_Inter_Feq_Handover_SR_1`
							,NULL AS `LTE_Inter_Feq_Handover_SR_2`
							,NULL AS `LTE_Intra_Feq_Handover_SR_1`
							,NULL AS `LTE_Intra_Feq_Handover_SR_2`
							,NULL AS `LTE_IRAT_Handover_SR_3G_1`
							,NULL AS `LTE_IRAT_Handover_SR_3G_2`
							,NULL AS `LTE_IRAT_Handover_SR_2G_1`
							,NULL AS `LTE_IRAT_Handover_SR_2G_2`
							,IFNULL(SUM(IF(A.call_status=2,1,0)),0) AS `GSM_Call_Drop_Rate_1`
							,IFNULL(SUM(IF(A.call_status IN (1,2,4),1,0)),0) AS `GSM_Call_Drop_Rate_2`
							,IFNULL(SUM(IF(A.call_status IN (3,6),1,0)),0) AS `GSM_CSSR_1`
							,COUNT(*) AS `GSM_CSSR_2`
							,IFNULL(SUM(IF(A.call_status=3,1,0)),0) AS `GSM_Blocked_Call_Rate_1`
							,COUNT(*) AS `GSM_Blocked_Call_Rate_2`
							,IFNULL(SUM(HO_FAILURE_COUNT),0) AS `GSM_Inter_Freq_Handover_SR_1`
							,IFNULL(SUM(HO_ATTEMPT_COUNT),0) AS `GSM_Inter_Freq_Handover_SR_2`
							,',PU_ID,' AS PU
							,DATA_DATE AS ds_date
							,1 AS `TECH_MASK`
							,A.POS_FIRST_LOC AS POS_FIRST_LOC
							,A.POS_LAST_LOC AS POS_LAST_LOC 
						FROM ',GT_DB,'.table_call_nopos_gsm A 
						WHERE ',FILTER_STR, ' GROUP BY IMSI ')
				ELSE '' END,
				' ;');  		
		END IF;
		IF KPI_ID=110014 THEN 
			SET @SqlCmd=CONCAT('
								SELECT 
									A.CALL_ID AS `CALL_ID`
									,NULL AS `SEQ_ID`					
									,A.MOVING_TYPE AS `MOVING_TYPE` 
									,',PU_ID,' AS `PU`
									,1 AS `TECH_MASK`
									,A.DATA_DATE AS ds_date		
									,POS_FIRST_LOC AS `POS_FIRST_LOC`
									,POS_LAST_LOC AS `POS_LAST_LOC`	
								FROM ',GT_DB,'.',TABLE_CALL_IMSI_GSM,' A 
								WHERE ',FILTER_STR,';');	
		END IF;	
		IF KPI_ID=110017 THEN 
			SET @SqlCmd=CONCAT('
								SELECT 						
								  nt.BSC_ID AS `BSC_ID`,
								  nt.CELL_ID AS `CELL_ID`,
								  nt.CELL_NAME AS `CELL_NAME`,
								  nt.ACITVE_STATE AS `ACTIVE_STATUS`,
								  0 AS `CALL_CNT`,	
								(CASE WHEN nt.CM_ADMIN_STATE=1 THEN ''Locked''
									WHEN nt.CM_ADMIN_STATE=0 THEN ''Unlocked''
									ELSE '''' END) AS Administrative_state,
								(CASE WHEN nt.CM_OPERATION_STATE=1 THEN ''Enable''
									WHEN nt.CM_OPERATION_STATE=0 THEN ''Disable''
									ELSE '''' END) AS OPERSTATE_ENABLE,	
								 ''' , DS_DATE,''' AS ds_date,
								',PU_ID,' AS pu,
								  1 AS TECH_MASK 
								FROM ',NT_DB,'.nt2_cell_gsm nt 
								LEFT JOIN
								(
									SELECT CELL_ID,BSC_ID,DATA_DATE AS ds_date
									FROM ',GT_DB,'.',CASE WHEN DY_FLAG=1 THEN 'table_tile_start_gsm_dy_c_def A' ELSE 'table_tile_start_gsm_c_def A' END,' 
									WHERE ',CASE WHEN FILTER_STR='' OR FILTER_STR IS NULL THEN '' ELSE CONCAT(FILTER_STR,'') END,' 
									GROUP BY a.CELL_ID,a.BSC_ID
								) dat 	
								ON dat.CELL_ID = nt.CELL_ID AND dat.BSC_ID = nt.BSC_ID 
								WHERE nt.BSC_ID=',PU_ID,' AND dat.CELL_ID IS NULL ;');
		END IF;
		IF KPI_ID=110018 THEN 
			SET @SqlCmd=CONCAT('
								SELECT 						
								  nt.BSC_ID AS `BSC_ID`,
								  nt.CELL_ID AS `CELL_ID`,
								  nt.CELL_NAME AS `CELL_NAME`,
								  nt.ACITVE_STATE AS `ACTIVE_STATUS`,
								  0 AS `CALL_CNT`,	
								(CASE WHEN nt.CM_ADMIN_STATE=1 THEN ''Locked''
									WHEN nt.CM_ADMIN_STATE=0 THEN ''Unlocked''
									ELSE '''' END) AS Administrative_state,
								(CASE WHEN nt.CM_OPERATION_STATE=1 THEN ''Enable''
									WHEN nt.CM_OPERATION_STATE=0 THEN ''Disable''
									ELSE '''' END) AS OPERSTATE_ENABLE,	
								 ''' , DS_DATE,''' AS ds_date,
								',PU_ID,' AS pu,
								  1 AS TECH_MASK 
								FROM ',NT_DB,'.nt2_cell_gsm nt 
								LEFT JOIN
								(
									SELECT CELL_ID,BSC_ID,DATA_DATE AS ds_date
									FROM ',GT_DB,'.',CASE WHEN DY_FLAG=1 THEN 'table_tile_start_gsm_dy_c_def A' ELSE 'table_tile_start_gsm_c_def A' END,' 
									WHERE ',CASE WHEN FILTER_STR='' OR FILTER_STR IS NULL THEN '' ELSE CONCAT(FILTER_STR,'') END,' 
									GROUP BY a.CELL_ID,a.BSC_ID
								) dat 	
								ON dat.CELL_ID = nt.CELL_ID AND dat.BSC_ID = nt.BSC_ID 
								WHERE nt.BSC_ID=',PU_ID,' AND dat.CELL_ID IS NULL;');
		END IF;	
		IF KPI_ID=110020 THEN 
			SET @SqlCmd=CONCAT('
							SELECT 
								A.IMSI AS `IMSI`
								,A.DATA_DATE AS `DATA_DATE`
								,MAX(CONCAT(IFNULL(A.MAKE_ID,'' ''),''|'',IFNULL(A.MODEL_ID,'' ''),''|'',IFNULL(A.IMEI,'' ''))) AS `HANDSET`
								,A.IMSI AS `MSISDN`
								,COUNT(A.CALL_ID) AS `Total_Call_Count`
								,SUM(IF(A.CALL_STATUS=2,1,0)) AS `Drop_Call_Count`
								,SUM(IF(A.CALL_STATUS=3,1,0)) AS `Block_Call_Count`
								,NULL AS `Total_DL_Data_Volume` 
								,NULL AS `Total_UL_Data_Volume`					
								,NULL AS `MAX_DL_THROUGHPUT`				
								,NULL AS `MAX_UL_THROUGHPUT`
								,NULL AS `UMTS_Call_Drop_Rate_1`
								,NULL AS `UMTS_Call_Drop_Rate_2`
								,NULL AS `UMTS_CSSR_1`
								,NULL AS `UMTS_CSSR_2`
								,NULL AS `UMTS_Blocked_Call_Rate_1`
								,NULL AS `UMTS_Blocked_Call_Rate_2`
								,NULL AS `UMTS_Soft_Handover_SR_1`
								,NULL AS `UMTS_Soft_Handover_SR_2`
								,NULL AS `UMTS_Softer_handover_SR_1`
								,NULL AS `UMTS_Softer_handover_SR_2`
								,NULL AS `UMTS_Inter_Freq_Handover_SR_1`
								,NULL AS `UMTS_Inter_Freq_Handover_SR_2`
								,NULL AS `UMTS_IRAT_handover_SR_1`
								,NULL AS `UMTS_IRAT_handover_SR_2`
								,NULL AS `LTE_Call_Drop_Rate_1`
								,NULL AS `LTE_Call_Drop_Rate_2`
								,NULL AS `LTE_CSSR_1`
								,NULL AS `LTE_CSSR_2`
								,NULL AS `LTE_Blocked_Call_Rate_1` 
								,NULL AS `LTE_Blocked_Call_Rate_2`
								,NULL AS `LTE_Inter_Feq_Handover_SR_1`
								,NULL AS `LTE_Inter_Feq_Handover_SR_2`
								,NULL AS `LTE_Intra_Feq_Handover_SR_1`
								,NULL AS `LTE_Intra_Feq_Handover_SR_2`
								,NULL AS `LTE_IRAT_Handover_SR_3G_1`
								,NULL AS `LTE_IRAT_Handover_SR_3G_2`
								,NULL AS `LTE_IRAT_Handover_SR_2G_1`
								,NULL AS `LTE_IRAT_Handover_SR_2G_2`
								,IFNULL(SUM(IF(A.call_status=2,1,0)),0) AS `GSM_Call_Drop_Rate_1`
								,IFNULL(SUM(IF(A.call_status IN (1,2,4),1,0)),0) AS `GSM_Call_Drop_Rate_2`
								,IFNULL(SUM(IF(A.call_status IN (3,6),1,0)),0) AS `GSM_CSSR_1`
								,COUNT(*) AS `GSM_CSSR_2`
								,IFNULL(SUM(IF(A.call_status=3,1,0)),0) AS `GSM_Blocked_Call_Rate_1`
								,COUNT(*) AS `GSM_Blocked_Call_Rate_2`
								,IFNULL(SUM(HO_FAILURE_COUNT),0) AS `GSM_Inter_Freq_Handover_SR_1`
								,IFNULL(SUM(HO_ATTEMPT_COUNT),0) AS `GSM_Inter_Freq_Handover_SR_2`
								,',PU_ID,' AS PU
								,DATA_DATE AS ds_date
								,1 AS `TECH_MASK`
								,A.POS_FIRST_LOC AS POS_FIRST_LOC
								,A.POS_LAST_LOC AS POS_LAST_LOC					
							FROM ',GT_DB,'.',TABLE_CALL_IMSI_GSM,' A 				
							WHERE ',FILTER_STR, ' GROUP BY IMSI,DATA_DATE',
						CASE  WHEN WITHDUMP=1 AND SPECIAL_IMSI <>1 THEN 
							CONCAT(' UNION ALL
								SELECT A.IMSI AS `IMSI`
									,A.DATA_DATE AS `DATA_DATE`
									,NULL AS `HANDSET`
									,A.IMSI AS `MSISDN`
									,COUNT(A.CALL_ID) AS `Total_Call_Count`
									,SUM(IF(A.CALL_STATUS=2,1,0)) AS `Drop_Call_Count`
									,SUM(IF(A.CALL_STATUS=3,1,0)) AS `Block_Call_Count`
									,NULL AS `Total_DL_Data_Volume` 
									,NULL AS `Total_UL_Data_Volume`					
									,NULL AS `MAX_DL_THROUGHPUT`				
									,NULL AS `MAX_UL_THROUGHPUT`	
									,NULL AS `UMTS_Call_Drop_Rate_1`
									,NULL AS `UMTS_Call_Drop_Rate_2`
									,NULL AS `UMTS_CSSR_1`
									,NULL AS `UMTS_CSSR_2`
									,NULL AS `UMTS_Blocked_Call_Rate_1`
									,NULL AS `UMTS_Blocked_Call_Rate_2`
									,NULL AS `UMTS_Soft_Handover_SR_1`
									,NULL AS `UMTS_Soft_Handover_SR_2`
									,NULL AS `UMTS_Softer_handover_SR_1`
									,NULL AS `UMTS_Softer_handover_SR_2`
									,NULL AS `UMTS_Inter_Freq_Handover_SR_1`
									,NULL AS `UMTS_Inter_Freq_Handover_SR_2`
									,NULL AS `UMTS_IRAT_handover_SR_1`
									,NULL AS `UMTS_IRAT_handover_SR_2`
									,NULL AS `LTE_Call_Drop_Rate_1`
									,NULL AS `LTE_Call_Drop_Rate_2`
									,NULL AS `LTE_CSSR_1`
									,NULL AS `LTE_CSSR_2`
									,NULL AS `LTE_Blocked_Call_Rate_1` 
									,NULL AS `LTE_Blocked_Call_Rate_2`
									,NULL AS `LTE_Inter_Feq_Handover_SR_1`
									,NULL AS `LTE_Inter_Feq_Handover_SR_2`
									,NULL AS `LTE_Intra_Feq_Handover_SR_1`
									,NULL AS `LTE_Intra_Feq_Handover_SR_2`
									,NULL AS `LTE_IRAT_Handover_SR_3G_1`
									,NULL AS `LTE_IRAT_Handover_SR_3G_2`
									,NULL AS `LTE_IRAT_Handover_SR_2G_1`
									,NULL AS `LTE_IRAT_Handover_SR_2G_2`
									,IFNULL(SUM(IF(A.call_status=2,1,0)),0) AS `GSM_Call_Drop_Rate_1`
									,IFNULL(SUM(IF(A.call_status IN (1,2,4),1,0)),0) AS `GSM_Call_Drop_Rate_2`
									,IFNULL(SUM(IF(A.call_status IN (3,6),1,0)),0) AS `GSM_CSSR_1`
									,COUNT(*) AS `GSM_CSSR_2`
									,IFNULL(SUM(IF(A.call_status=3,1,0)),0) AS `GSM_Blocked_Call_Rate_1`
									,COUNT(*) AS `GSM_Blocked_Call_Rate_2`
									,IFNULL(SUM(HO_FAILURE_COUNT),0) AS `GSM_Inter_Freq_Handover_SR_1`
									,IFNULL(SUM(HO_ATTEMPT_COUNT),0) AS `GSM_Inter_Freq_Handover_SR_2`
									,',PU_ID,' AS PU
									,DATA_DATE AS ds_date
									,1 AS `TECH_MASK`
									,A.POS_FIRST_LOC AS POS_FIRST_LOC
									,A.POS_LAST_LOC AS POS_LAST_LOC					
								FROM ',GT_DB,'.table_call_nopos_gsm A 						
								WHERE ',FILTER_STR, ' GROUP BY IMSI,DATA_DATE')
						ELSE '' END,
						' ;');  
		END IF;		
		IF KPI_ID=110021 THEN 	
			IF (DATA_DATE <> DATE(NOW())) THEN
				IF (START_HOUR=0 AND END_HOUR=23) THEN 
					SET DY_FLAG=1;	
				ELSE 
					SET DY_FLAG='';
				END IF;	
			ELSE 
				SET DY_FLAG='';
			END IF;
	
			IF DY_FLAG=1 THEN 
				SET STR_SEL_IMSI_AGG_RPT=CONCAT(STR_SEL_IMSI_AGG_RPT,'SELECT ',STR_IMSI_AGG_RPT,' FROM ',GT_DB,'.table_imsi_aggregated_dy A WHERE 1 ',FILTER_STR,''); 				
			ELSE 			
				SET @v_i=START_HOUR;
				SET @v_i_Max=END_HOUR;
				SET @v_j=0;							
				WHILE @v_i <= @v_i_Max DO
				BEGIN 
					IF (@v_i=@v_i_Max ) THEN 
						SET @UNION=' '; 
					ELSE
						SET @UNION=' UNION ALL '; 
					END IF;	
					SET STR_SEL_IMSI_AGG_RPT=CONCAT(STR_SEL_IMSI_AGG_RPT,'SELECT ',STR_IMSI_AGG_RPT,' FROM ',GT_DB,'.table_imsi_aggregated_hr',CONCAT('_',LPAD(@v_i,2,'0')),' A  WHERE DATA_HOUR=', @v_i,FILTER_STR,'',@UNION); 
					SET @v_i=@v_i+1;
				END;
				END WHILE;	
			END IF;	
			SET @SqlCmd=STR_SEL_IMSI_AGG_RPT;
			SET @SqlCmd=CONCAT('
				SELECT  IMSI AS `IMSI`
					,DATA_DATE AS `DATA_DATE`
					,MAX(B.`HANDSET`) AS `HANDSET`
					,B.IMSI AS `MSISDN`
					,SUM(B.`Total_Call_Count`) AS `Total_Call_Count`
					,SUM(B.`Drop_Call_Count`) AS `Drop_Call_Count`
					,SUM(B.`Block_Call_Count`) AS `Block_Call_Count`
					,SUM(B.`Total_DL_Data_Volume`) AS `Total_DL_Data_Volume` 
					,SUM(B.`Total_UL_Data_Volume`) AS `Total_UL_Data_Volume`					
					,MAX(B.`MAX_DL_THROUGHPUT`) AS `MAX_DL_THROUGHPUT`				
					,MAX(B.`MAX_UL_THROUGHPUT`) AS `MAX_UL_THROUGHPUT`	
					,SUM(B.`UMTS_Call_Drop_Rate_1`) AS `UMTS_Call_Drop_Rate_1`
					,SUM(B.`UMTS_Call_Drop_Rate_2`) AS `UMTS_Call_Drop_Rate_2`
					,SUM(B.`UMTS_CSSR_1`) AS `UMTS_CSSR_1`
					,SUM(B.`UMTS_CSSR_2`) AS `UMTS_CSSR_2`
					,SUM(B.`UMTS_Blocked_Call_Rate_1`) AS `UMTS_Blocked_Call_Rate_1`
					,SUM(B.`UMTS_Blocked_Call_Rate_2`) AS `UMTS_Blocked_Call_Rate_2`
					,SUM(B.`UMTS_Soft_Handover_SR_1`) AS `UMTS_Soft_Handover_SR_1`
					,SUM(B.`UMTS_Soft_Handover_SR_2`) AS `UMTS_Soft_Handover_SR_2`
					,SUM(B.`UMTS_Softer_handover_SR_1`) AS `UMTS_Softer_handover_SR_1`
					,SUM(B.`UMTS_Softer_handover_SR_2`) AS `UMTS_Softer_handover_SR_2`
					,SUM(B.`UMTS_Inter_Freq_Handover_SR_1`)AS `UMTS_Inter_Freq_Handover_SR_1`
					,SUM(B.`UMTS_Inter_Freq_Handover_SR_2`) AS `UMTS_Inter_Freq_Handover_SR_2`
					,SUM(B.`UMTS_IRAT_handover_SR_1`) AS `UMTS_IRAT_handover_SR_1`
					,SUM(B.`UMTS_IRAT_handover_SR_2`) AS `UMTS_IRAT_handover_SR_2`
					,SUM(B.`LTE_Call_Drop_Rate_1`) AS `LTE_Call_Drop_Rate_1`
					,SUM(B.`LTE_Call_Drop_Rate_2`) AS `LTE_Call_Drop_Rate_2`
					,SUM(B.`LTE_CSSR_1`) AS `LTE_CSSR_1`
					,SUM(B.`LTE_CSSR_2`)  AS `LTE_CSSR_2`
					,SUM(B.`LTE_Blocked_Call_Rate_1`) AS `LTE_Blocked_Call_Rate_1` 
					,SUM(B.`LTE_Blocked_Call_Rate_2`) AS `LTE_Blocked_Call_Rate_2`
					,SUM(B.`LTE_Inter_Feq_Handover_SR_1`) AS `LTE_Inter_Feq_Handover_SR_1`
					,SUM(B.`LTE_Inter_Feq_Handover_SR_2`) AS `LTE_Inter_Feq_Handover_SR_2`
					,SUM(B.`LTE_Intra_Feq_Handover_SR_1`) AS `LTE_Intra_Feq_Handover_SR_1`
					,SUM(B.`LTE_Intra_Feq_Handover_SR_2`) AS `LTE_Intra_Feq_Handover_SR_2`
					,SUM(B.`LTE_IRAT_Handover_SR_3G_1`) AS `LTE_IRAT_Handover_SR_3G_1`
					,SUM(B.`LTE_IRAT_Handover_SR_3G_2`) AS `LTE_IRAT_Handover_SR_3G_2`
					,SUM(B.`LTE_IRAT_Handover_SR_2G_1`) AS `LTE_IRAT_Handover_SR_2G_1`
					,SUM(B.`LTE_IRAT_Handover_SR_2G_2`) AS `LTE_IRAT_Handover_SR_2G_2`
					,SUM(B.`GSM_Call_Drop_Rate_1`) AS `GSM_Call_Drop_Rate_1`
					,SUM(B.`GSM_Call_Drop_Rate_2`) AS `GSM_Call_Drop_Rate_2`
					,SUM(B.`GSM_CSSR_1`) AS `GSM_CSSR_1`
					,SUM(B.`GSM_CSSR_2`) AS `GSM_CSSR_2`
					,SUM(B.`GSM_Blocked_Call_Rate_1`) AS `GSM_Blocked_Call_Rate_1`
					,SUM(B.`GSM_Blocked_Call_Rate_2`) AS `GSM_Blocked_Call_Rate_2`
					,SUM(B.`GSM_Inter_Freq_Handover_SR_1`) AS `GSM_Inter_Freq_Handover_SR_1`
					,SUM(B.`GSM_Inter_Freq_Handover_SR_2`) AS `GSM_Inter_Freq_Handover_SR_2`
					,PU AS PU
					,DATA_DATE AS ds_date
					,MAX(TECH_MASK) AS `TECH_MASK`
					,MAX(FIRST_LAT_LON) AS `FIRST_LAT_LON`
					,MAX(LAST_LAT_LON) AS `LAST_LAT_LON`
					,POS_FIRST_RSCP_SUM AS POS_FIRST_RSCP_SUM
					,POS_FIRST_RSCP_CNT AS POS_FIRST_RSCP_CNT
					,POS_FIRST_ECN0_SUM AS POS_FIRST_ECN0_SUM
					,POS_FIRST_ECN0_CNT AS POS_FIRST_ECN0_CNT
					,POS_LAST_RSCP_SUM AS POS_LAST_RSCP_SUM
					,POS_LAST_RSCP_CNT AS POS_LAST_RSCP_CNT
					,POS_LAST_ECN0_SUM AS POS_LAST_ECN0_SUM
					,POS_LAST_ECN0_CNT AS POS_LAST_ECN0_CNT
					,POS_FIRST_S_RSRP_SUM AS POS_FIRST_S_RSRP_SUM
					,POS_FIRST_S_RSRP_CNT AS POS_FIRST_S_RSRP_CNT
					,POS_FIRST_S_RSRQ_SUM AS POS_FIRST_S_RSRQ_SUM
					,POS_FIRST_S_RSRQ_CNT AS POS_FIRST_S_RSRQ_CNT
					,POS_LAST_S_RSRP_SUM AS POS_LAST_S_RSRP_SUM
					,POS_LAST_S_RSRP_CNT AS POS_LAST_S_RSRP_CNT
					,POS_LAST_S_RSRQ_SUM AS POS_LAST_S_RSRQ_SUM
					,POS_LAST_S_RSRQ_CNT AS POS_LAST_S_RSRQ_CNT
				FROM (',STR_SEL_IMSI_AGG_RPT,') B
				WHERE IMSI IS NOT NULL
				GROUP BY IMSI;'); 
		END IF;	
	END;
	END IF;
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
		
	INSERT INTO `gt_gw_main`.`tbl_rpt_qrystr`
		(`KPI_ID`,`RNC`,`START_DATE`,`END_DATE`,`START_HOUR`,`END_HOUR`,`SOURCE_TYPE`,`SERVICE`,`PID`,`QryStr`,`ID`,`SP_NAME`,`CreateTime`)
	VALUES (KPI_ID,
			PU_ID,
			gt_strtok(GT_DB,3,'_'),
			gt_strtok(GT_DB,3,'_'),
			START_HOUR,
			END_HOUR,
			SOURCE_TYPE,
			SERVICE,
			WORKER_ID,
			@SqlCmd,
			0,
			'SP_KPI_multi_remote',
			NOW());	
	INSERT INTO gt_gw_main.SP_LOG VALUES(GT_DB,'SP_KPI_multi_remote','End', NOW());	
END$$
DELIMITER ;
