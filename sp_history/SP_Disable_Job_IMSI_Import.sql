DELIMITER $$
USE `gt_gw_main`$$
DROP PROCEDURE IF EXISTS `SP_CreateDB_LTE`$$
CREATE DEFINER=`covmo`@`%` PROCEDURE `SP_Disable_Job_IMSI_Import`()
BEGIN
	SELECT COUNT(*) INTO @EVENT_COUNT FROM INFORMATION_SCHEMA.EVENTS WHERE event_schema = 'gt_gw_main' AND event_NAME = 'job_imsi_import';
	IF @EVENT_COUNT > 0 THEN 
		ALTER EVENT gt_gw_main.job_imsi_import DISABLE;
	END IF; 
	DROP EVENT IF EXISTS gt_schedule.job_imsi_import;
	
	SELECT COUNT(*) INTO @EVENT_COUNT2 FROM INFORMATION_SCHEMA.EVENTS WHERE event_schema = 'gt_gw_main' AND event_NAME = 'job_imsi_tbl_prevent';
	IF @EVENT_COUNT2 > 0 THEN 
		ALTER EVENT gt_gw_main.job_imsi_tbl_prevent DISABLE;
	END IF; 
	DROP EVENT IF EXISTS gt_schedule.job_imsi_tbl_prevent;
	
	SELECT COUNT(*) INTO @EVENT_COUNT3 FROM INFORMATION_SCHEMA.EVENTS WHERE event_schema = 'gt_gw_main' AND event_NAME = 'job_purge_imsi_pu';
	IF @EVENT_COUNT3 > 0 THEN 
		ALTER EVENT gt_gw_main.job_purge_imsi_pu DISABLE;
	END IF; 
	DROP EVENT IF EXISTS gt_schedule.job_purge_imsi_pu;
	
	
END$$
DELIMITER ;
