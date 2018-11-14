DELIMITER $$
USE `gt_gw_main`$$
DROP PROCEDURE IF EXISTS `SP_CreateDB_LTE`$$
CREATE DEFINER=`covmo`@`%` PROCEDURE `SP_Sub_Update_APN_LTE`(IN GT_DB VARCHAR(100),IN GT_COVMO VARCHAR(100))
BEGIN
	DECLARE GT_SESSION_ID INT;
	DECLARE WORKER_ID VARCHAR(10) DEFAULT CONNECTION_ID();
	DECLARE SH_EH VARCHAR(9) DEFAULT RIGHT(GT_DB,9);
	DECLARE SH VARCHAR(4) DEFAULT gt_strtok(SH_EH,1,'_');
	DECLARE DY_GT_DB VARCHAR(100);
	
	SELECT REPLACE(GT_DB,SH_EH,'0000_0000') INTO DY_GT_DB;
	INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'SP_Sub_Update_APN_LTE',CONCAT('INSERT DATA TO tmp_dim_imsi_apn','_',WORKER_ID), NOW());
	SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS ',DY_GT_DB,'.tmp_dim_apn','_',WORKER_ID,';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
		
	SET @SqlCmd=CONCAT('CREATE TEMPORARY TABLE ',DY_GT_DB,'.`tmp_dim_apn','_',WORKER_ID,'` ENGINE=MYISAM
				SELECT * FROM ',GT_COVMO,'.dim_apn;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('CREATE INDEX IX_APN ON ',DY_GT_DB,'.`tmp_dim_apn','_',WORKER_ID,'` (APN);');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('UPDATE ',DY_GT_DB,'.table_call_lte_',SH,' A FORCE INDEX(APN)
					,  ',DY_GT_DB,'.`tmp_dim_apn','_',WORKER_ID,'` B FORCE INDEX(IX_APN)
				SET A.APN = B.APN_ID 
				WHERE A.ACCESS_POINT_NAME = B.APN;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS  ',DY_GT_DB,'.tmp_dim_apn','_',WORKER_ID,';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'SP_Sub_Update_APN_LTE','Done', NOW());
	
END$$
DELIMITER ;
