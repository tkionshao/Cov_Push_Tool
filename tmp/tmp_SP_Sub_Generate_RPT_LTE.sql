CREATE DEFINER=`covmo`@`%` PROCEDURE `SP_Sub_Generate_RPT_LTE`(IN RPT_TBL_NAME VARCHAR(100),IN TMP_GT_DB VARCHAR(100),IN GT_DB VARCHAR(100),FLAG VARCHAR(100))
do_nothing:
BEGIN
	DECLARE START_TIME DATETIME DEFAULT SYSDATE();
	DECLARE STEP_START_TIME DATETIME DEFAULT SYSDATE();
	DECLARE WORKER_ID VARCHAR(10) DEFAULT CONNECTION_ID();
	DECLARE SP_Process VARCHAR(100);
	DECLARE v_1 INT;
	DECLARE i INT;
	DECLARE UNION_STR VARCHAR(1000) DEFAULT '';
	DECLARE UNION_DEF_STR VARCHAR(1000) DEFAULT '';
	DECLARE `ENGINE` VARCHAR(10) DEFAULT ''; 
	DECLARE DEF_ENGINE VARCHAR(10) DEFAULT '';
	
	
	INSERT INTO gt_gw_main.SP_LOG VALUES(TMP_GT_DB,'SP_Sub_Generate_RPT_LTE','Start', START_TIME);
	IF RPT_TBL_NAME ='rpt_cell_lte' THEN
		BEGIN  
			SET @str=RPT_TBL_NAME;
			CALL `gt_gw_main`.`SP_Sub_Gen_CELL_LTE_PARALLEL_AWK`(@str,TMP_GT_DB,GT_DB,FLAG,0,1);
			SELECT 1 AS isSuccess, '' AS errorMessage;			
		END;
	
	
			INSERT INTO gt_gw_main.SP_LOG VALUES(TMP_GT_DB,'SP_Sub_Generate_RPT_LTE',CONCAT('Create ',RPT_TBL_NAME,' merge COST: ',TIMESTAMPDIFF(SECOND,STEP_START_TIME,SYSDATE()),' seconds.'), NOW());
			SET STEP_START_TIME := SYSDATE();
		
			SELECT 0 AS isSuccess,CONCAT('SP_Sub_Generate_RPT_LTE - Dispatch Not fully succeed! TABLE:',RPT_TBL_NAME,',FLAG:',FLAG) AS errorMessage;
		
		
	END IF; 
	
	INSERT INTO gt_gw_main.SP_LOG VALUES(TMP_GT_DB,'SP_Sub_Generate_RPT_LTE',CONCAT('Done:',RPT_TBL_NAME,',',TMP_GT_DB,',',FLAG,' ',TIMESTAMPDIFF(SECOND,START_TIME,SYSDATE()),' seconds.'), NOW());
