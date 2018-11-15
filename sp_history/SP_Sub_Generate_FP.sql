DELIMITER $$
USE `gt_gw_main`$$
DROP PROCEDURE IF EXISTS `SP_CreateDB_LTE`$$
CREATE DEFINER=`covmo`@`%` PROCEDURE `SP_Sub_Generate_FP`(IN TMP_GT_DB VARCHAR(100),IN GT_DB VARCHAR(100),IN EVENT_NUM TINYINT(2),IN POS_NUM TINYINT(2))
do_nothing:
BEGIN
	DECLARE START_TIME DATETIME DEFAULT SYSDATE();
	DECLARE v_1,v_2,v_3 INT;
	DECLARE SP_Process VARCHAR(100);
	
	INSERT INTO gt_gw_main.SP_LOG VALUES(TMP_GT_DB,'SP_Sub_Generate_FP','Start', START_TIME);
	BEGIN
	
	
	SET SP_Process = 'SP_Sub_Generate_FP_PARALLEL';
	
	SELECT GET_LOCK('GT_JOB_CREATE_LOCK', 60) INTO @bb;
	CALL gt_schedule.sp_job_create(CONCAT(SP_Process,'-1'),TMP_GT_DB);
	SET v_1 :=@JOB_ID;
	SELECT RELEASE_LOCK('GT_JOB_CREATE_LOCK') INTO @bb;
	
	SELECT GET_LOCK('GT_JOB_CREATE_LOCK', 60) INTO @bb;
	CALL gt_schedule.sp_job_create(CONCAT(SP_Process,'-2'),TMP_GT_DB);
	SET v_2 :=@JOB_ID;
	SELECT RELEASE_LOCK('GT_JOB_CREATE_LOCK') INTO @bb;
	CALL gt_schedule.sp_job_add_task(CONCAT('CALL `gt_gw_main`.SP_Sub_Generate_RPT(''',SP_Process,''',',POS_NUM,',''table_tile_fp'',''',TMP_GT_DB,''',''',GT_DB,''');'),v_1);
	CALL gt_schedule.sp_job_add_task(CONCAT('CALL `gt_gw_main`.SP_Sub_Generate_RPT(''',SP_Process,''',',POS_NUM,',''table_tile_fp_t'',''',TMP_GT_DB,''',''',GT_DB,''');'),v_2);
	CALL gt_schedule.sp_job_add_task(CONCAT('CALL `gt_gw_main`.SP_Sub_Generate_RPT(''',SP_Process,''',',POS_NUM,',''table_tile_fp_c'',''',TMP_GT_DB,''',''',GT_DB,''');'),v_2);
	
	INSERT INTO gt_gw_main.SP_LOG VALUES(TMP_GT_DB,'SP_Sub_Generate_FP','STEP1', NOW());
	CALL gt_schedule.sp_job_start(v_1);
	CALL gt_schedule.sp_job_enable_event();
	CALL gt_schedule.sp_job_wait(v_1);
	
	CALL gt_gw_main.SP_Sub_Check_Report_State(GT_DB,v_1);
	SET @strtbl = 'table_tile_fp_update';
	CALL gt_gw_main.SP_Create_MergeTable(TMP_GT_DB,CONCAT('',@strtbl,''),'table_tile_fp_update_1');
	
	INSERT INTO gt_gw_main.SP_LOG VALUES(TMP_GT_DB,'SP_Sub_Generate_FP','STEP2', NOW());
	CALL gt_schedule.sp_job_start(v_2);
	CALL gt_schedule.sp_job_wait(v_2);
	CALL gt_schedule.sp_job_disable_event();
	
	CALL gt_gw_main.SP_Sub_Check_Report_State(GT_DB,v_2);
	
	SET @strtbl = 'table_tile_fp_c';
	CALL gt_gw_main.SP_Create_MergeTable(TMP_GT_DB,CONCAT('',@strtbl,'_mgr'),'table_tile_fp_c');	
	
	
	INSERT INTO gt_gw_main.SP_LOG VALUES(TMP_GT_DB,'SP_Sub_Generate_FP','STEP3', NOW());
	CALL `gt_gw_main`.SP_Sub_Generate_FP_PARALLEL('table_tile_fp_dy_c',TMP_GT_DB,GT_DB,0,0);	
	CALL `gt_gw_main`.SP_Sub_Generate_FP_PARALLEL('table_tile_fp_c_mgr',TMP_GT_DB,GT_DB,0,0);
	
	END;
	
	INSERT INTO gt_gw_main.SP_LOG VALUES(TMP_GT_DB,'SP_Sub_Generate_FP',CONCAT('Done: ',TIMESTAMPDIFF(SECOND,START_TIME,SYSDATE()),' seconds.'), NOW());	
END$$
DELIMITER ;
