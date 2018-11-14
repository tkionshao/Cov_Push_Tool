DELIMITER $$
USE `gt_gw_main`$$
DROP PROCEDURE IF EXISTS `SP_CreateDB_LTE`$$
CREATE DEFINER=`covmo`@`%` PROCEDURE `SP_Generate_Report_Optimization_GSM`(IN gt_db VARCHAR(100),IN GT_COVMO VARCHAR(100))
BEGIN	
			 
	INSERT INTO gt_gw_main.SP_LOG VALUES(GT_DB,'SP_Generate_Report_Optimization_GSM','START', START_TIME);
	
	INSERT INTO gt_gw_main.SP_LOG VALUES(GT_DB,'SP_Generate_Report_Optimization_GSM',CONCAT('Done: ',TIMESTAMPDIFF(SECOND,START_TIME,SYSDATE()),' seconds.'), NOW());		
END$$
DELIMITER ;
