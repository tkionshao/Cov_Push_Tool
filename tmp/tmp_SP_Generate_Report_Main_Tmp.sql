CREATE DEFINER=`covmo`@`%` PROCEDURE `SP_Generate_Report_Main_Tmp`(IN TMP_GT_DB VARCHAR(100), IN GT_DB VARCHAR(100),IN EVENT_NUM TINYINT(2),IN POS_NUM TINYINT(2))
BEGIN
	DECLARE START_TIME DATETIME DEFAULT SYSDATE();
 
	INSERT INTO gt_gw_main.SP_LOG VALUES(TMP_GT_DB,'SP_Generate_Report_Main_Tmp','Start', START_TIME);
	
	CALL gt_gw_main.SP_Sub_Generate_FP(TMP_GT_DB,GT_DB,EVENT_NUM,POS_NUM);
	CALL gt_gw_main.SP_Sub_Generate_Failure_Cause_AGR(TMP_GT_DB,GT_DB);
	
	INSERT INTO gt_gw_main.SP_LOG VALUES(TMP_GT_DB,'SP_Generate_Report_Main_Tmp',CONCAT('Done: ',TIMESTAMPDIFF(SECOND,START_TIME,SYSDATE()),' seconds.'), NOW());
	
	
	
	
	
