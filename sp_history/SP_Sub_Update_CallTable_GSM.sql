DELIMITER $$
USE `gt_gw_main`$$
DROP PROCEDURE IF EXISTS `SP_CreateDB_LTE`$$
CREATE DEFINER=`covmo`@`%` PROCEDURE `SP_Sub_Update_CallTable_GSM`(IN GT_DB VARCHAR(100),IN GT_COVMO VARCHAR(100))
BEGIN
	DECLARE START_TIME DATETIME DEFAULT SYSDATE();
	DECLARE HUA_EXPORT_FLAG VARCHAR(10);
	SELECT LOWER(`value`) INTO HUA_EXPORT_FLAG  FROM gt_gw_main.integration_param WHERE gt_group = 'sp' AND gt_name = 'hua_export' ;
	
	INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'SP_Sub_Update_CallTable_GSM','Start', NOW());
	
	IF HUA_EXPORT_FLAG = 'true' THEN
		CALL gt_gw_main.`SP_Sub_Table_Call_Export`(GT_DB,1,GT_COVMO);
	END IF;
	
	INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'SP_Sub_Update_CallTable_GSM',CONCAT('Done: ',TIMESTAMPDIFF(SECOND,START_TIME,SYSDATE()),' seconds.'), NOW());
	
END$$
DELIMITER ;
