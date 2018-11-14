DELIMITER $$
USE `gt_gw_main`$$
DROP PROCEDURE IF EXISTS `SP_CreateDB_LTE`$$
CREATE DEFINER=`covmo`@`%` PROCEDURE `SP_Version`()
BEGIN	
	SELECT "2014/3 Stored Procedure(Groundhog)" AS Update_Message;	
END$$
DELIMITER ;
