DELIMITER $$
USE `gt_gw_main`$$
DROP PROCEDURE IF EXISTS `SP_CreateDB_LTE`$$
CREATE DEFINER=`covmo`@`%` PROCEDURE `SP_IMSI_TABLE_CREATE`()
do_nothing:
BEGIN 
	-- DECLARE START_TIME DATETIME DEFAULT SYSDATE();
	-- SET @SqlCmd=CONCAT('CREATE TABLE IF NOT EXISTS ',gt_global_imsi,'.`table_imsi_pu_test` (				
	
	SET @SqlCmd=CONCAT('CREATE TABLE IF NOT EXISTS',gt_global_imsi,'.`Testing` )(
		`ID` INT(11)
		)ENGINE=MYISAM DEFAULT CHARSET=latin1 DELAY_KEY_WRITE=1',';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
    
END$$
DELIMITER ;
