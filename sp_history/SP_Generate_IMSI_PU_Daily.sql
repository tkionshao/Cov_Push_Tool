DELIMITER $$
USE `gt_gw_main`$$
DROP PROCEDURE IF EXISTS `SP_CreateDB_LTE`$$
CREATE DEFINER=`covmo`@`%` PROCEDURE `SP_Generate_IMSI_PU_Daily`(IN GT_DB_QRT VARCHAR(100),TECH_MASK TINYINT(2))
BEGIN
	DECLARE PU_ID INT;
	DECLARE GT_DB VARCHAR(100);
	DECLARE WORKER_ID VARCHAR(10) DEFAULT CONNECTION_ID();
	DECLARE START_TIME DATETIME DEFAULT SYSDATE();
	DECLARE STEP_START_TIME DATETIME DEFAULT SYSDATE();
	DECLARE SH_EH CHAR(9) DEFAULT RIGHT(GT_DB_QRT,9);
	DECLARE SH_HOUR TINYINT(4) DEFAULT CAST(LEFT(gt_strtok(SH_EH,1,'_'),2) AS SIGNED);
	DECLARE SH_BATCH SMALLINT(6) DEFAULT CAST(gt_strtok(SH_EH,1,'_') AS SIGNED);
	DECLARE DATE_SH_EH VARCHAR(25) DEFAULT RIGHT(GT_DB_QRT,18);
	DECLARE ORG_DATA_DATE CHAR(8) DEFAULT SUBSTRING(DATE_SH_EH,1,8);
	DECLARE DATA_DATE CHAR(10) DEFAULT CONCAT(SUBSTRING(ORG_DATA_DATE,1,4),'-',SUBSTRING(ORG_DATA_DATE,5,2),'-',SUBSTRING(ORG_DATA_DATE,7,2));
	
	SELECT REPLACE(GT_DB_QRT,SH_EH,'0000_0000') INTO GT_DB;
	SELECT gt_strtok(GT_DB,2,'_') INTO PU_ID;
	INSERT INTO gt_gw_main.SP_LOG VALUES(GT_DB_QRT,'SP_Generate_IMSI_PU_Daily',CONCAT(GT_DB_QRT,' START'), START_TIME);
	SET STEP_START_TIME := SYSDATE();
	
	SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS ',GT_DB,'.tmp_imsi_',PU_ID,'_',WORKER_ID,';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('CREATE TEMPORARY TABLE ',GT_DB,'.tmp_imsi_',PU_ID,'_',WORKER_ID,'
				SELECT IMSI FROM ',GT_DB,'.table_call_imsi_',CASE TECH_MASK WHEN 1 THEN 'gsm' WHEN 2 THEN 'umts' WHEN 4 THEN 'lte' ELSE '' END,' 
				GROUP BY IMSI;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	INSERT INTO gt_gw_main.SP_LOG VALUES(GT_DB_QRT,'SP_Generate_IMSI_PU_Daily',CONCAT('CREATE tmp table ',GT_DB,'.tmp_imsi_',PU_ID,'_',WORKER_ID,' cost:',TIMESTAMPDIFF(SECOND,STEP_START_TIME,SYSDATE()),' sec.'), NOW());
	SET STEP_START_TIME := SYSDATE();
	SET DATE_SH_EH=CONCAT(DATE_SH_EH,'_',TECH_MASK);
	
	SET @SqlCmd=CONCAT('DROP TABLE IF EXISTS ',GT_DB,'.table_imsi_diff_',PU_ID,'_',DATE_SH_EH,';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('DROP TABLE IF EXISTS ',GT_DB,'.table_imsi_diff_',PU_ID,'_',DATE_SH_EH,'_tmp;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('CREATE TABLE ',GT_DB,'.table_imsi_diff_',PU_ID,'_',DATE_SH_EH,'_tmp (
				  `IMSI` VARCHAR(20) NOT NULL,
				  `DATA_DATE` DATE NOT NULL,
				  `PU_ID` MEDIUMINT(9) DEFAULT NULL,    
				  `DATA_DATE_TS` INT(11) NOT NULL,    
				  `TECH_MASK` TINYINT(2) NOT NULL
				) ENGINE=MYISAM DEFAULT CHARSET=latin1 DELAY_KEY_WRITE=1;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('INSERT INTO ',GT_DB,'.table_imsi_diff_',PU_ID,'_',DATE_SH_EH,'_tmp 
				SELECT IMSI,''',DATA_DATE,''',',PU_ID,',UNIX_TIMESTAMP(''',DATA_DATE,'''),',TECH_MASK,' FROM ',GT_DB,'.tmp_imsi_',PU_ID,'_',WORKER_ID,' A;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	INSERT INTO gt_gw_main.SP_LOG VALUES(GT_DB_QRT,'SP_Generate_IMSI_PU_Daily',CONCAT('INSERT table ',GT_DB,'.table_imsi cost:',TIMESTAMPDIFF(SECOND,STEP_START_TIME,SYSDATE()),' sec.'), NOW());
	SET STEP_START_TIME := SYSDATE();
	
	SET @SqlCmd=CONCAT('RENAME TABLE ',GT_DB,'.table_imsi_diff_',PU_ID,'_',DATE_SH_EH,'_tmp  TO ',GT_DB,'.table_imsi_diff_',PU_ID,'_',DATE_SH_EH,';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	INSERT INTO gt_gw_main.SP_LOG VALUES(GT_DB_QRT,'SP_Generate_IMSI_PU_Daily',CONCAT('rename table ',GT_DB,'.table_imsi_diff_',PU_ID,'_',DATE_SH_EH,'_tmp cost:',TIMESTAMPDIFF(SECOND,STEP_START_TIME,SYSDATE()),' sec.'), NOW());
	SET STEP_START_TIME := SYSDATE();
	
	SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS ',GT_DB,'.tmp_imsi_',PU_ID,'_',WORKER_ID,';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	INSERT INTO gt_gw_main.SP_LOG VALUES(GT_DB_QRT,'SP_Generate_IMSI_PU_Daily',CONCAT(GT_DB_QRT,' Done: ',TIMESTAMPDIFF(SECOND,START_TIME,SYSDATE()),' seconds.'), NOW());
END$$
DELIMITER ;
