DELIMITER $$
USE `gt_gw_main`$$
DROP PROCEDURE IF EXISTS `SP_CreateDB_LTE`$$
CREATE DEFINER=`covmo`@`%` PROCEDURE `SP_Insert_Into_Daily_DB_GSM`(IN GT_DB VARCHAR(100),IN VENDER_ID INT,IN GW_IP VARCHAR(50),IN RUN_STATUS VARCHAR(10),IN GT_COVMO VARCHAR(100))
BEGIN
	DECLARE START_TIME DATETIME DEFAULT SYSDATE();
	DECLARE SH_EH VARCHAR(9) DEFAULT RIGHT(GT_DB,9);
	DECLARE SH SMALLINT(6) DEFAULT gt_strtok(SH_EH,1,'_');
	DECLARE FROM_GT_DB VARCHAR(100) DEFAULT GT_DB;
	DECLARE DB_FLAG INT DEFAULT 0;
	DECLARE RNC_ID INT; 
	
	SELECT REPLACE(GT_DB,SH_EH,'0000_0000') INTO GT_DB;
	SELECT gt_strtok(GT_DB,2,'_') INTO RNC_ID;
	
	INSERT INTO gt_gw_main.sp_log VALUES(FROM_GT_DB,'SP_Insert_Into_Daily_DB_GSM','START', NOW());
	
	INSERT INTO gt_gw_main.sp_log VALUES(FROM_GT_DB,'SP_Insert_Into_Daily_DB_GSM','Update DATA_VENDOR & GW_IP', NOW());
	SET GW_IP = CONCAT('http://',GW_IP,':8989');
	SET @SqlCmd=CONCAT('UPDATE `gt_gw_main`.`session_information` 
				SET `DATA_VENDOR` = ''',VENDER_ID,''' ,
				`GW_IP` = ''',GW_IP,''' 
			WHERE `SESSION_DB` = ''',GT_DB,''';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SELECT COUNT(*) INTO DB_FLAG FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = GT_DB;
	IF DB_FLAG = 0 THEN
 		CALL SP_CreateDB_GSM(RNC_ID,RIGHT(GT_DB,18),'DAILY');
		SELECT 1 AS Message;
	ELSE
		SELECT 0 AS Message;
	END IF;
	
	INSERT INTO gt_gw_main.sp_log VALUES(FROM_GT_DB,'SP_Insert_Into_Daily_DB_GSM','INSERT table_call_gsm', NOW());
	SET @SqlCmd=CONCAT('INSERT INTO   ',GT_DB,'.table_call_gsm 
	SELECT
		*
	FROM ',FROM_GT_DB,'.table_call_gsm_update;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	INSERT INTO gt_gw_main.sp_log VALUES(FROM_GT_DB,'SP_Insert_Into_Daily_DB_GSM','INSERT table_call_gsm', NOW());
	
	SET @SqlCmd=CONCAT('INSERT INTO   ',GT_DB,'.table_call_gsm_dump SELECT * FROM ',FROM_GT_DB,'.table_call_gsm_dump;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	INSERT INTO gt_gw_main.sp_log VALUES(FROM_GT_DB,'SP_Insert_Into_Daily_DB_GSM','INSERT table_position_gsm', NOW());
	
	SET @SqlCmd=CONCAT('INSERT INTO   ',GT_DB,'.table_position_gsm SELECT * FROM ',FROM_GT_DB,'.table_position_gsm;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	INSERT INTO gt_gw_main.sp_log VALUES(FROM_GT_DB,'SP_Insert_Into_Daily_DB_GSM','INSERT table_tile_fp_position_gsm', NOW());
	
	SET @SqlCmd=CONCAT('INSERT INTO   ',GT_DB,'.table_tile_fp_gsm_update SELECT * FROM ',FROM_GT_DB,'.table_tile_fp_gsm_update;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	INSERT INTO gt_gw_main.sp_log VALUES(FROM_GT_DB,'SP_Insert_Into_Daily_DB_GSM','INSERT rpt_cell_relation_gsm', NOW());
	
	SET @SqlCmd=CONCAT('INSERT INTO   ',GT_DB,'.rpt_cell_relation_gsm SELECT * FROM ',FROM_GT_DB,'.rpt_cell_relation_gsm;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	INSERT INTO gt_gw_main.sp_log VALUES(FROM_GT_DB,'SP_Insert_Into_Daily_DB_GSM','INSERT opt_nbr_relation_gsm', NOW());
	
	SET @SqlCmd=CONCAT('INSERT INTO   ',GT_DB,'.opt_nbr_relation_gsm SELECT * FROM ',FROM_GT_DB,'.opt_nbr_relation_gsm;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	INSERT INTO gt_gw_main.sp_log VALUES(FROM_GT_DB,'SP_Insert_Into_Daily_DB_GSM','INSERT table_call_nopos_gsm', NOW());
	
	SET @SqlCmd=CONCAT('INSERT INTO   ',GT_DB,'.table_call_nopos_gsm SELECT * FROM ',FROM_GT_DB,'.table_call_nopos_gsm;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;	
	
	CALL gt_gw_main.SP_Insert_Into_Roamer(FROM_GT_DB,GT_DB,'gsm');
	CALL gt_gw_main.SP_Generate_IMSI_PU(FROM_GT_DB,1);
	
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
	
	CALL gt_gw_main.SP_Sub_Update_CallTable_GSM(FROM_GT_DB,GT_COVMO);
	
	INSERT INTO gt_gw_main.SP_LOG VALUES(FROM_GT_DB,'SP_Insert_Into_Daily_DB_GSM',CONCAT('Done: ',TIMESTAMPDIFF(SECOND,START_TIME,SYSDATE()),' seconds.'), NOW());
	
END$$
DELIMITER ;
