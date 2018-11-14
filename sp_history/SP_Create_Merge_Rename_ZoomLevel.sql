DELIMITER $$
USE `gt_gw_main`$$
DROP PROCEDURE IF EXISTS `SP_CreateDB_LTE`$$
CREATE DEFINER=`covmo`@`%` PROCEDURE `SP_Create_Merge_Rename_ZoomLevel`(IN GT_DB VARCHAR(100),IN ZL CHAR(2),IN TBL_NAME VARCHAR(100))
BEGIN
	DECLARE REP_NAME VARCHAR(100) DEFAULT 'tile';
 	DECLARE TARGET_TBL_NAME VARCHAR(100) DEFAULT CONCAT('tile',ZL);
 	
	SET @SqlCmd=CONCAT('CREATE TABLE ',GT_DB,'.',REPLACE(TBL_NAME,REP_NAME,TARGET_TBL_NAME),' LIKE ',GT_DB,'.',TBL_NAME,';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
END$$
DELIMITER ;
