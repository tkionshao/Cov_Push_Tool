CREATE DEFINER=`covmo`@`%` PROCEDURE `SP_Process_Daily_session`(IN GT_DB VARCHAR(100),IN VENDER_ID INT,IN GW_IP VARCHAR(50),IN GT_COVMO VARCHAR(100),IN RUN_STATUS VARCHAR(10))
BEGIN
	DECLARE START_TIME DATETIME DEFAULT SYSDATE();
	DECLARE SH_EH VARCHAR(9) DEFAULT RIGHT(GT_DB,9);
	DECLARE SH VARCHAR(4) DEFAULT gt_strtok(SH_EH,1,'_');
	DECLARE FROM_GT_DB VARCHAR(100) DEFAULT GT_DB;
	DECLARE TO_GT_DB VARCHAR(100);
	DECLARE HUA_EXPORT_FLAG VARCHAR(10);
	DECLARE CUSTOMER_USER_FLAG VARCHAR(50);
	DECLARE RNC_ID INT; 
	
	SELECT gt_strtok(GT_DB,2,'_') INTO RNC_ID;
	SELECT LOWER(`value`) INTO CUSTOMER_USER_FLAG  FROM gt_gw_main.integration_param WHERE gt_group = 'sp' AND gt_name = 'special_imsi' ;
	SELECT LOWER(`value`) INTO HUA_EXPORT_FLAG  FROM gt_gw_main.integration_param WHERE gt_group = 'sp' AND gt_name = 'hua_export' ;
	SELECT REPLACE(GT_DB,SH_EH,'0000_0000') INTO TO_GT_DB;
	
	INSERT INTO gt_gw_main.sp_log VALUES(FROM_GT_DB,'SP_Process_Daily_session','START', NOW());
	
	IF HUA_EXPORT_FLAG = 'true' THEN CALL gt_gw_main.`SP_Sub_Table_Call_Export`(FROM_GT_DB,2,GT_COVMO);	END IF;
	
	CALL gt_gw_main.SP_Sub_Generate_LU_Reject(FROM_GT_DB,TO_GT_DB);
	CALL gt_gw_main.SP_Insert_Into_Roamer(FROM_GT_DB,TO_GT_DB,'umts');
	CALL gt_gw_main.SP_Generate_IMSI_PU(FROM_GT_DB,2);
	INSERT INTO gt_gw_main.sp_log VALUES(FROM_GT_DB,'SP_Process_Daily_session','GW_IR', NOW());
	SET GW_IP = CONCAT('http://',GW_IP,':8989');
	UPDATE `gt_gw_main`.`session_information` SET `DATA_VENDOR` = VENDER_ID,`GW_IP` = GW_IP WHERE `SESSION_DB` = TO_GT_DB;	
	INSERT INTO gt_gw_main.sp_log VALUES(FROM_GT_DB,'SP_Process_Daily_session','INSERT table_protocol_failure_event', NOW());	
	SET @SqlCmd=CONCAT('TRUNCATE TABLE ',TO_GT_DB,'.table_protocol_failure_event_',SH);
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('INSERT INTO ',TO_GT_DB,'.table_protocol_failure_event_',SH,'
			(CALL_ID,RAB_SEQ_ID,DATA_DATE,DATA_HOUR,IMSI,IMEI,CALL_TYPE,CALL_STATUS,
			APN,INDOOR,MOVING,MOVING_TYPE,POS_LAST_RNC,POS_LAST_CELL,POS_LAST_LOC,
			RRC_FAILURE_CAUSE_LAST,RNSAP_FAILURE_CAUSE_LAST,RANAP_FAILURE_CAUSE_LAST,
			NBAP_FAILURE_CAUSE_LAST,NAS_CAUSE_CC,NAS_CAUSE_SM ,BATCH)
		SELECT 
			CALL_ID,RAB_SEQ_ID,DATA_DATE,DATA_HOUR,IMSI,IMEI,CALL_TYPE,CALL_STATUS,
			APN,INDOOR,MOVING,MOVING_TYPE,POS_LAST_RNC,POS_LAST_CELL,POS_LAST_LOC,
			RRC_FAILURE_CAUSE_LAST,RNSAP_FAILURE_CAUSE_LAST,RANAP_FAILURE_CAUSE_LAST,
			NBAP_FAILURE_CAUSE_LAST,NAS_CAUSE_CC,NAS_CAUSE_SM,',SH,'
		FROM
		  ',TO_GT_DB,'.table_call_',SH,' AS fact 
		WHERE RRC_FAILURE_CAUSE_LAST IS NOT NULL 
		  OR RNSAP_FAILURE_CAUSE_LAST IS NOT NULL 
		  OR RANAP_FAILURE_CAUSE_LAST IS NOT NULL 
		  OR NBAP_FAILURE_CAUSE_LAST IS NOT NULL 
		  OR NAS_CAUSE_CC IS NOT NULL 
		  OR NAS_CAUSE_SM IS NOT NULL ;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	INSERT INTO gt_gw_main.sp_log VALUES(FROM_GT_DB,'SP_Process_Daily_session','dy session_information', NOW());
	SET @FILE_STARTTIME=CONCAT(SUBSTRING(gt_strtok(FROM_GT_DB,3,'_'),1,4),'-',SUBSTRING(gt_strtok(FROM_GT_DB,3,'_'),5,2),'-',SUBSTRING(gt_strtok(FROM_GT_DB,3,'_'),7,2),' ',SUBSTRING(gt_strtok(FROM_GT_DB,4,'_'),1,2),':',SUBSTRING(gt_strtok(FROM_GT_DB,4,'_'),3,2),':00');
	SET @FILE_ENDTIME=CONCAT(SUBSTRING(gt_strtok(FROM_GT_DB,3,'_'),1,4),'-',SUBSTRING(gt_strtok(FROM_GT_DB,3,'_'),5,2),'-',SUBSTRING(gt_strtok(FROM_GT_DB,3,'_'),7,2),' ',SUBSTRING(gt_strtok(FROM_GT_DB,5,'_'),1,2),':',SUBSTRING(gt_strtok(FROM_GT_DB,5,'_'),3,2),':00');
	
	SET @SqlCmd = CONCAT('SELECT GW_URI,AP_URI,DS_AP_URI INTO @RNC_TO_SEC_GW_URI,@RNC_TO_SEC_AP_URI,@RNC_TO_SEC_DS_AP_URI FROM gt_gw_main.rnc_information WHERE rnc=',RNC_ID,' and technology=''umts'';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	
	SET @SqlCmd=CONCAT('INSERT INTO ',TO_GT_DB,'.session_information 
					    (`SESSION_ID`,
					     `SESSION_DB`,
					     `RNC`,
					     `FILE_STARTTIME`,
					     `FILE_ENDTIME`,
					     `IMPORT_TIME`,
					     `SESSION_TYPE`,
						 `GW_URI`,
						 `AP_URI`,
						 `DS_AP_URI`)
				VALUES (',CONNECTION_ID(),',
					''',FROM_GT_DB,''',
					''',RNC_ID,''',
					''',@FILE_STARTTIME,''',
					''',@FILE_ENDTIME,''',
					''',NOW(),''',
					''TEMP'',
					''',@RNC_TO_SEC_GW_URI,''',
					''',@RNC_TO_SEC_AP_URI,''',
					''',@RNC_TO_SEC_DS_AP_URI,''');');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	INSERT INTO gt_gw_main.SP_LOG VALUES(FROM_GT_DB,'SP_Process_Daily_session',CONCAT('Done: ',TIMESTAMPDIFF(SECOND,START_TIME,SYSDATE()),' seconds.'), NOW());
