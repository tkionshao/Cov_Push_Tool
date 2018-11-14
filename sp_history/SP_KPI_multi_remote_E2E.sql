DELIMITER $$
USE `gt_gw_main`$$
DROP PROCEDURE IF EXISTS `SP_CreateDB_LTE`$$
CREATE DEFINER=`covmo`@`%` PROCEDURE `SP_KPI_multi_remote_E2E`(IN GT_DB VARCHAR(100),IN KPI_ID INT(11),IN START_TIME VARCHAR(50),IN END_TIME VARCHAR(50)
							,IN GT_COVMO VARCHAR(20),TECH_NAME VARCHAR(10),IN IMSI_STR TEXT)
BEGIN	
	DECLARE WORKER_ID VARCHAR(10) DEFAULT CONNECTION_ID();
	DECLARE PU_ID INT;
	DECLARE NT_DB VARCHAR(100);
	DECLARE FILTER_STR VARCHAR(10000);
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
	
	
	
	DECLARE v_START_HOUR TINYINT(11) DEFAULT HOUR(START_TIME);
	DECLARE v_START_MIN TINYINT(11) DEFAULT MINUTE(START_TIME);
	DECLARE v_START_SEC TINYINT(11) DEFAULT SECOND(START_TIME);
	DECLARE v_END_HOUR TINYINT(11) DEFAULT HOUR(END_TIME);
	DECLARE v_END_MIN TINYINT(11) DEFAULT MINUTE(END_TIME);
	DECLARE v_END_SEC TINYINT(11) DEFAULT SECOND(END_TIME);
	
	IF  TECH_NAME='GSM' THEN
		SET TABLE_CALL_IMSI_GSM='table_call_gsm';
	ELSEIF  TECH_NAME='UMTS' THEN
		SET TABLE_CALL_IMSI_UMTS='table_call';
	ELSEIF  TECH_NAME='LTE' THEN
		SET TABLE_CALL_IMSI_LTE='table_call_lte';			
	END IF;	
	
	
	
	
	SELECT gt_strtok(GT_DB,2,'_') INTO PU_ID;
	
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
				CONCAT(A.`MCC`,''-'',A.`MNC`,''-'',A.`START_ENODEB_ID`,''-'',A.`START_CELL_ID`) AS START_CELL,
				A.POS_FIRST_LOC AS POS_FIRST_LOC,
				A.`POS_FIRST_RSRP` AS START_RXLEV_RSCP_RSRP_dBn,
				A.`POS_FIRST_RSRQ` AS START_RXQUAL_ECN0_RSRQ_dB,
				CONCAT(A.`MCC`,''-'',A.`MNC`,''-'',A.`POS_LAST_S_ENODEB`,''-'',A.`POS_LAST_S_CELL`) AS END_CELL,
				A.POS_LAST_LOC AS POS_LAST_LOC,
				A.`POS_LAST_RSRP` AS END_RXLEV_RSCP_RSRP_dBn,
				A.`POS_LAST_RSRQ` AS END_RXQUAL_ECN0_RSRQ_dB,
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
				NULL AS  `CELL_UPDATE_CAUSE`,		
				NULL AS RAB_SEQ_ID,
				DATA_DATE AS ds_date,
				CONCAT(A.POS_FIRST_CELL,''@'',A.`POS_FIRST_ENODEB`) AS POS_FIRST_CELL,
				CONCAT(A.POS_LAST_CELL,''@'',A.`POS_LAST_ENODEB`) AS POS_LAST_CELL,
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
				IF(B.ERAB_STATUS IS NULL,CONCAT(A.`MCC`,''-'',A.`MNC`,''-'',A.`START_ENODEB_ID`,''-'',A.`START_CELL_ID`),CONCAT(A.`MCC`,''-'',A.`MNC`,''-'',B.`ERAB_START_SERVING_ENODEB`,''-'',B.`ERAB_START_SERVING_CELL`)) AS START_CELL,
				IF(B.ERAB_STATUS IS NULL,A.POS_FIRST_LOC,B.`ERAB_START_LOC`) AS POS_FIRST_LOC,
				IF(B.ERAB_STATUS IS NULL,A.`POS_FIRST_RSRP`,B.`ERAB_START_SERVING_RSRP`) AS START_RXLEV_RSCP_RSRP_dBn,
				IF(B.ERAB_STATUS IS NULL,A.`POS_FIRST_RSRQ`,B.`ERAB_START_SERVING_RSRQ`) AS START_RXQUAL_ECN0_RSRQ_dB,
				IF(B.ERAB_STATUS IS NULL,CONCAT(A.`MCC`,''-'',A.`MNC`,''-'',A.`POS_LAST_S_ENODEB`,''-'',A.`POS_LAST_S_CELL`),CONCAT(A.`MCC`,''-'',A.`MNC`,''-'',B.`ERAB_END_SERVING_ENODEB`,''-'',B.`ERAB_END_SERVING_CELL`)) AS END_CELL,
				IF(B.ERAB_STATUS IS NULL,A.POS_LAST_LOC,B.`ERAB_END_LOC`) AS POS_LAST_LOC,
				IF(B.ERAB_STATUS IS NULL,A.`POS_LAST_RSRP`,B.`ERAB_END_SERVING_RSRP`) AS END_RXLEV_RSCP_RSRP_dBn,
				IF(B.ERAB_STATUS IS NULL,A.`POS_LAST_RSRQ`,B.`ERAB_END_SERVING_RSRQ`) AS END_RXQUAL_ECN0_RSRQ_dB,
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
				NULL AS  `CELL_UPDATE_CAUSE`,        
				NULL AS RAB_SEQ_ID,
				A.DATA_DATE AS ds_date,
				IF(B.ERAB_STATUS IS NULL,CONCAT(A.POS_FIRST_CELL,''@'',A.`POS_FIRST_ENODEB`),CONCAT(B.`ERAB_START_SERVING_CELL`,''@'',B.`ERAB_START_SERVING_ENODEB`)) AS POS_FIRST_CELL,
				IF(B.ERAB_STATUS IS NULL,CONCAT(A.POS_LAST_CELL,''@'',A.`POS_LAST_ENODEB`),CONCAT(B.`ERAB_END_SERVING_CELL`,''@'',B.`ERAB_END_SERVING_ENODEB`)) AS POS_LAST_CELL,
				CONCAT(A.START_CELL_ID,''@'',A.`START_ENODEB_ID`) AS START_CELL_ID,
				CONCAT(A.END_CELL_ID,''@'',A.`END_ENODEB_ID`) AS END_CELL_ID,
				NULL AS `MANUFACTURER`,
				NULL AS `MODEL`,
				',PU_ID,' AS PU');
	
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
						CASE WHEN CELL_UPDATE_CAUSE & 64>0 THEN ''unrecoverable RLC error,'' ELSE '''' END))
						,LENGTH(CONCAT(CASE WHEN CELL_UPDATE_CAUSE & 1>0 THEN ''cell reselection,'' ELSE '''' END,
						CASE WHEN CELL_UPDATE_CAUSE & 2>0 THEN ''periodic cell,'' ELSE '''' END,
						CASE WHEN CELL_UPDATE_CAUSE & 4>0 THEN ''uplink DATA transmission,'' ELSE '''' END,
						CASE WHEN CELL_UPDATE_CAUSE & 8>0 THEN ''paging response,'' ELSE '''' END,
						CASE WHEN CELL_UPDATE_CAUSE & 16>0 THEN ''re-entered service AREA,'' ELSE '''' END,
						CASE WHEN CELL_UPDATE_CAUSE & 32>0 THEN ''radio link failure,'' ELSE '''' END,
						CASE WHEN CELL_UPDATE_CAUSE & 64>0 THEN ''unrecoverable RLC error,'' ELSE '''' END))-1)  AS  `CELL_UPDATE_CAUSE`
					,RAB_SEQ_ID AS `RAB_SEQ_ID`
					,DATA_DATE AS ds_date
					,POS_FIRST_CELL AS POS_FIRST_CELL
					,POS_LAST_CELL AS POS_LAST_CELL
					,START_CELL_ID
					,END_CELL_ID
					,NULL AS `MANUFACTURER`,NULL AS `MODEL`
					,',PU_ID,' AS pu');
	
	
	
	
	
	IF (HOUR(START_TIME)=0 AND MINUTE(START_TIME)=0 AND SECOND(START_TIME)=0) AND (HOUR(END_TIME)=0 AND MINUTE(END_TIME)=0 AND SECOND(END_TIME)=0) THEN 
		SET DY_FLAG=1;
		IF (KPI_ID=110001 OR KPI_ID=110005 OR KPI_ID=110014 OR KPI_ID=110020) AND TECH_NAME IN('LTE','UMTS') THEN
			SET FILTER_STR=CONCAT(
					CASE 
					      WHEN (IMSI_STR<>'') THEN IN_STR('A.IMSI',IN_QUOTE(IMSI_STR)) 
						ELSE ''
					 END
	
					);
		 END IF;
	
	
		
	
	
	ELSE 	
		
		
		IF (KPI_ID=110001 OR KPI_ID=110005 OR KPI_ID=110014 OR KPI_ID=110020) AND TECH_NAME IN ('LTE','UMTS') THEN
			SET FILTER_STR=CONCAT(
					CASE WHEN v_START_MIN=0 AND v_START_SEC=0 AND @v_END_MIN=0 AND v_END_SEC=0 THEN '' ELSE CONCAT(' AND A.START_TIME>=''',START_TIME,''' AND A.START_TIME<=''',END_TIME,'''') END
					,CASE 
					      WHEN (IMSI_STR<>'') THEN IN_STR('A.IMSI',IN_QUOTE(IMSI_STR)) 
					      ELSE ''
					 END
	
					);
		
		END IF;
		
	END IF;
	
	SET @TECHNOLOGY=TECH_NAME;
	
	INSERT INTO gt_gw_main.SP_LOG VALUES(GT_DB,'SP_KPI_multi_remote_E2E','Start', NOW());
	IF @TECHNOLOGY='UMTS' THEN 
	BEGIN
		IF  KPI_ID=110001  THEN 
	
			SET @v_i=HOUR(START_TIME);
			SET @v_i_Max=23;
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
					SET STR_SEL_IMSI_UMTS=CONCAT(STR_SEL_IMSI_UMTS,'SELECT ',STR_IMSI_UMTS,' FROM ',GT_DB,'.',TABLE_CALL_IMSI_UMTS,'',CONCAT('_',LPAD(@v_i,2,'0'),LPAD(@v_j,2,'0')),' A FORCE INDEX (IMSI) WHERE DATA_HOUR=', @v_i,FILTER_STR,@UNION); 
					SET @v_j=@v_j+15;
				END;
				END WHILE;
					
				SET @v_j=0;
				SET @v_i=@v_i+1;
			END;
			END WHILE;	
			
			
			SET @SqlCmd=STR_SEL_IMSI_UMTS;
		END IF;
	END;
	
	ELSEIF @TECHNOLOGY='LTE' THEN 
	BEGIN
		IF  KPI_ID=110001  THEN 
			
			SET @v_i=HOUR(START_TIME);
			SET @v_i_Max=23;
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
						SET STR_SEL_IMSI_LTE=CONCAT(STR_SEL_IMSI_LTE,'SELECT ',STR_IMSI_LTE,' FROM ',GT_DB,'.',TABLE_CALL_IMSI_LTE,'',CONCAT('_',LPAD(@v_i,2,'0'),LPAD(@v_j,2,'0')),' A FORCE INDEX (IMSI) WHERE DATA_HOUR=', @v_i,FILTER_STR,@UNION); 
						SET @v_j=@v_j+15;
				
				END;
				END WHILE;
					
				SET @v_j=0;
				SET @v_i=@v_i+1;
			END;
			END WHILE;	
			
			SET @SqlCmd=STR_SEL_IMSI_LTE;	
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
	
	INSERT INTO gt_gw_main.SP_LOG VALUES(GT_DB,'SP_KPI_multi_remote_E2E','End', NOW());
	
END$$
DELIMITER ;
