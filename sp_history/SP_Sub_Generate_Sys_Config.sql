DELIMITER $$
USE `gt_gw_main`$$
DROP PROCEDURE IF EXISTS `SP_CreateDB_LTE`$$
CREATE DEFINER=`covmo`@`%` PROCEDURE `SP_Sub_Generate_Sys_Config`(IN GT_DB VARCHAR(100),IN GT_COVMO VARCHAR(100),IN TECH VARCHAR(10))
a_label:
BEGIN
	DECLARE START_TIME DATETIME DEFAULT SYSDATE();
	
  	INSERT INTO gt_gw_main.SP_LOG VALUES(GT_DB,'SP_Sub_Generate_Sys_Config','create sys_config in NT db', NOW());
	SET @SqlCmd=CONCAT('SELECT COUNT(*) INTO @TBL_EXIST FROM information_schema.`TABLES`
				WHERE TABLE_SCHEMA=''',GT_DB,''' 
				AND TABLE_NAME=''sys_config'';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	IF @TBL_EXIST=0 THEN 		
		SET @SqlCmd=CONCAT('CREATE TABLE IF NOT EXISTS ',GT_DB,'.sys_config 
			SELECT `group_name`,`tech_mask`,`att_name`,`att_value`,`att_unit`,`category`,`visible`,`readonly`
			FROM ',GT_COVMO,'.sys_config WHERE 1<>1;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		SET @SqlCmd=CONCAT('INSERT INTO ',GT_DB,'.sys_config
					(`group_name`,`tech_mask`,`att_name`,`att_value`,`att_unit`,`category`,`visible`,`readonly`)
				SELECT `group_name`,`tech_mask`,`att_name`,`att_value`,`att_unit`,`category`,`visible`,`readonly`
				FROM ',GT_COVMO,'.sys_config;');
		
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
	END IF;
	INSERT INTO gt_gw_main.SP_LOG VALUES(GT_DB,'SP_Sub_Generate_Sys_Config',CONCAT('Done: ',TIMESTAMPDIFF(SECOND,START_TIME,SYSDATE()),' seconds.'), NOW());	
END$$
DELIMITER ;
