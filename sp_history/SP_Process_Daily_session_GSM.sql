DELIMITER $$
USE `gt_gw_main`$$
DROP PROCEDURE IF EXISTS `SP_CreateDB_LTE`$$
CREATE DEFINER=`covmo`@`%` PROCEDURE `SP_Process_Daily_session_GSM`(IN GT_DB VARCHAR(100),IN VENDER_ID INT,IN GW_IP VARCHAR(50),IN GT_COVMO VARCHAR(100),IN RERUN VARCHAR(10))
BEGIN
	DECLARE START_TIME DATETIME DEFAULT SYSDATE();
	DECLARE SH_EH VARCHAR(9) DEFAULT RIGHT(GT_DB,9);
	DECLARE FROM_GT_DB VARCHAR(100) DEFAULT GT_DB;
	DECLARE RNC_ID INT;
	DECLARE STARTHOUR SMALLINT(6) DEFAULT SUBSTRING(RIGHT(GT_DB,18),10,2);
	DECLARE ENDHOUR VARCHAR(2) DEFAULT IF(SUBSTRING(RIGHT(GT_DB,18),15,2)='00','24',SUBSTRING(RIGHT(GT_DB,18),15,2));
	DECLARE IMSI_IMEI_DIFF_FLAG VARCHAR(10);
	
	INSERT INTO gt_gw_main.sp_log VALUES(FROM_GT_DB,'SP_Process_Daily_session_GSM','START', NOW());
	SELECT REPLACE(GT_DB,SH_EH,'0000_0000') INTO GT_DB;
	SELECT gt_strtok(GT_DB,2,'_') INTO RNC_ID;
	SELECT LOWER(`value`) INTO IMSI_IMEI_DIFF_FLAG  FROM gt_gw_main.integration_param WHERE gt_group = 'sp' AND gt_name = 'imsi_imei_diff' ;
	
	CALL gt_gw_main.SP_Insert_Into_Roamer(FROM_GT_DB,GT_DB,'gsm');
	
	INSERT INTO gt_gw_main.sp_log VALUES(FROM_GT_DB,'SP_Process_Daily_session_GSM','UPDATE DATA_VENDOR & GW_IP', NOW());
	SET GW_IP = CONCAT('http://',GW_IP,':8989');
	SET @SqlCmd=CONCAT('UPDATE `gt_gw_main`.`session_information` 
				SET `DATA_VENDOR` = ''',VENDER_ID,''' ,
				`GW_IP` = ''',GW_IP,''' 
			WHERE `SESSION_DB` = ''',GT_DB,''';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @FILE_STARTTIME=CONCAT(SUBSTRING(gt_strtok(FROM_GT_DB,3,'_'),1,4),'-',SUBSTRING(gt_strtok(FROM_GT_DB,3,'_'),5,2),'-',SUBSTRING(gt_strtok(FROM_GT_DB,3,'_'),7,2),' ',SUBSTRING(gt_strtok(FROM_GT_DB,4,'_'),1,2),':',SUBSTRING(gt_strtok(FROM_GT_DB,4,'_'),3,2),':00');
	SET @FILE_ENDTIME=CONCAT(SUBSTRING(gt_strtok(FROM_GT_DB,3,'_'),1,4),'-',SUBSTRING(gt_strtok(FROM_GT_DB,3,'_'),5,2),'-',SUBSTRING(gt_strtok(FROM_GT_DB,3,'_'),7,2),' ',SUBSTRING(gt_strtok(FROM_GT_DB,5,'_'),1,2),':',SUBSTRING(gt_strtok(FROM_GT_DB,5,'_'),3,2),':00');
	
	SET @SqlCmd = CONCAT('SELECT GW_URI,AP_URI,DS_AP_URI INTO @RNC_TO_SEC_GW_URI,@RNC_TO_SEC_AP_URI,@RNC_TO_SEC_DS_AP_URI FROM gt_gw_main.rnc_information WHERE rnc=',RNC_ID,' and technology=''gsm'';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('INSERT INTO ',GT_DB,'.session_information 
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
	
	INSERT INTO gt_gw_main.sp_log VALUES(FROM_GT_DB,'SP_Process_Daily_session_GSM','IMSI Diff', NOW());
-- 	CALL gt_gw_main.SP_Generate_IMSI_PU(FROM_GT_DB,1);
	IF IMSI_IMEI_DIFF_FLAG = 'true' THEN 
		CALL gt_gw_main.`SP_Generate_IMSI_IMEI`(FROM_GT_DB,1);
	ELSE
		CALL gt_gw_main.SP_Generate_IMSI_PU(FROM_GT_DB,1);
	END IF;	
	
	INSERT INTO gt_gw_main.SP_LOG VALUES(FROM_GT_DB,'SP_Process_Daily_session_GSM',CONCAT('Done: ',TIMESTAMPDIFF(SECOND,START_TIME,SYSDATE()),' seconds.'), NOW());
	
END$$
DELIMITER ;
