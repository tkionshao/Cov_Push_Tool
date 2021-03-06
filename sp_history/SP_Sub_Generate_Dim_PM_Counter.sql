DELIMITER $$
USE `gt_gw_main`$$
DROP PROCEDURE IF EXISTS `SP_CreateDB_LTE`$$
CREATE DEFINER=`covmo`@`%` PROCEDURE `SP_Sub_Generate_Dim_PM_Counter`(IN GT_DB VARCHAR(100),IN GT_COVMO VARCHAR(100))
a_label:
BEGIN
	DECLARE START_TIME DATETIME DEFAULT SYSDATE();
  	INSERT INTO gt_gw_main.SP_LOG VALUES(GT_DB,'SP_Sub_Generate_Dim_PM_Counter','create Dim_PM_Counter in NT db', NOW());
	SET @SqlCmd=CONCAT('SELECT COUNT(*) INTO @TBL_EXIST_UMTS FROM information_schema.`TABLES`
	WHERE TABLE_SCHEMA=''',GT_DB,''' 
	AND TABLE_NAME=''dim_pm_ericsson_umts'';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	SET @SqlCmd=CONCAT('SELECT COUNT(*) INTO @TBL_EXIST_LTE FROM information_schema.`TABLES`
	WHERE TABLE_SCHEMA=''',GT_DB,''' 
	AND TABLE_NAME=''dim_pm_ericsson_lte'';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	IF @TBL_EXIST_UMTS=0 THEN 		
		SET @SqlCmd=CONCAT('CREATE TABLE IF NOT EXISTS ',GT_DB,'.dim_pm_ericsson_umts 
		SELECT *
		FROM ',GT_COVMO,'.dim_pm_ericsson_umts;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		
	END IF;
	IF @TBL_EXIST_LTE=0 THEN 		
		SET @SqlCmd=CONCAT('CREATE TABLE IF NOT EXISTS ',GT_DB,'.dim_pm_ericsson_lte 
		SELECT *
		FROM ',GT_COVMO,'.dim_pm_ericsson_lte;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		
	END IF;	
	INSERT INTO gt_gw_main.SP_LOG VALUES(GT_DB,'SP_Sub_Generate_Dim_PM_Counter',CONCAT('Done: ',TIMESTAMPDIFF(SECOND,START_TIME,SYSDATE()),' seconds.'), NOW());	
END$$
DELIMITER ;
