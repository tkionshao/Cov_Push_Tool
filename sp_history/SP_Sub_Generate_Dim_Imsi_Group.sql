DELIMITER $$
USE `gt_gw_main`$$
DROP PROCEDURE IF EXISTS `SP_CreateDB_LTE`$$
CREATE DEFINER=`covmo`@`%` PROCEDURE `SP_Sub_Generate_Dim_Imsi_Group`(IN GT_DB VARCHAR(100),IN GT_COVMO VARCHAR(100),IN TECH VARCHAR(10))
a_label:
BEGIN
	DECLARE START_TIME DATETIME DEFAULT SYSDATE();
	
  	INSERT INTO gt_gw_main.SP_LOG VALUES(GT_DB,'SP_Sub_Generate_dim_imsi_group','create dim_imsi_group in NT db', NOW());
	SET @SqlCmd=CONCAT('SELECT COUNT(*) INTO @TBL_EXIST FROM information_schema.`TABLES`
				WHERE TABLE_SCHEMA=''',GT_DB,''' 
				AND TABLE_NAME=''dim_imsi_group'';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	IF @TBL_EXIST=0 THEN 		
		SET @SqlCmd=CONCAT('CREATE TABLE IF NOT EXISTS ',GT_DB,'.dim_imsi_group 
			SELECT  `IMPORT_TIME`, `GROUP_ID`,`GROUP_NAME`,`IN_USE`
			FROM ',GT_COVMO,'.dim_imsi_group WHERE 1<>1;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		
		SET @SqlCmd=CONCAT('INSERT INTO ',GT_DB,'.dim_imsi_group
					( `IMPORT_TIME`, `GROUP_ID`,`GROUP_NAME`,`IN_USE`)
				SELECT  `IMPORT_TIME`, `GROUP_ID`,`GROUP_NAME`,`IN_USE`
				FROM ',GT_COVMO,'.dim_imsi_group;');	
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
	END IF;
	INSERT INTO gt_gw_main.SP_LOG VALUES(GT_DB,'SP_Sub_Generate_dim_imsi_group',CONCAT('Done: ',TIMESTAMPDIFF(SECOND,START_TIME,SYSDATE()),' seconds.'), NOW());	
END$$
DELIMITER ;
