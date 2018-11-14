DELIMITER $$
USE `gt_gw_main`$$
DROP PROCEDURE IF EXISTS `SP_CreateDB_LTE`$$
CREATE DEFINER=`covmo`@`%` PROCEDURE `SP_Insert_Into_Roamer`(IN FROM_GT_DB VARCHAR(100),IN TO_GT_DB VARCHAR(100), IN TECH VARCHAR(4))
BEGIN
	DECLARE START_TIME DATETIME DEFAULT SYSDATE();
	DECLARE SOURCE_TABLE VARCHAR(30);
	DECLARE TARGET_TABLE VARCHAR(30);
	DECLARE SH_EH VARCHAR(9) DEFAULT RIGHT(FROM_GT_DB,9);
	DECLARE SH VARCHAR(4) DEFAULT gt_strtok(SH_EH,1,'_');
	
	INSERT INTO gt_gw_main.sp_log VALUES(TO_GT_DB,'SP_Insert_Into_Roamer','START', NOW());
	
	IF TECH = 'gsm' THEN


		SET FROM_GT_DB = TO_GT_DB;
		SET SOURCE_TABLE = CONCAT('table_call_gsm_',SH);
		SET TARGET_TABLE = CONCAT('table_roamer_call_gsm_',SH);
	ELSEIF TECH = 'umts' THEN
		SET FROM_GT_DB = TO_GT_DB;
		SET SOURCE_TABLE = CONCAT('table_call_',SH);
		SET TARGET_TABLE = CONCAT('table_roamer_call_',SH);
	ELSEIF TECH = 'lte' THEN
		SET FROM_GT_DB = TO_GT_DB;
		SET SOURCE_TABLE = CONCAT('table_call_lte_',SH);
		SET TARGET_TABLE = CONCAT('table_roamer_call_lte_',SH);
	END IF;
	
	SET @SqlCmd=CONCAT('INSERT INTO   ',TO_GT_DB,'.',TARGET_TABLE,' SELECT * FROM ',FROM_GT_DB,'.',SOURCE_TABLE,' A WHERE
			NOT EXISTS (SELECT * FROM gt_covmo.sys_mnc B WHERE A.MCC = B.MCC AND A.MNC = B.MNC) AND A.MCC IS NOT NULL;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	INSERT INTO gt_gw_main.SP_LOG VALUES(TO_GT_DB,'SP_Insert_Into_Roamer',CONCAT('Done: ',TIMESTAMPDIFF(SECOND,START_TIME,SYSDATE()),' seconds.'), NOW());
	
END$$
DELIMITER ;
