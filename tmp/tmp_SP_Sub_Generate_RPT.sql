CREATE DEFINER=`covmo`@`%` PROCEDURE `SP_Sub_Generate_RPT`(IN SP_Process VARCHAR(100), IN TABLE_CNT INT, IN RPT_TBL_NAME VARCHAR(100),
			IN TMP_GT_DB VARCHAR(100),IN GT_DB VARCHAR(100))
do_nothing:
BEGIN
	DECLARE START_TIME DATETIME DEFAULT SYSDATE();
	DECLARE STEP_START_TIME DATETIME DEFAULT SYSDATE();
	DECLARE v_1 INT;
	DECLARE I INT;
	DECLARE DICT_GT_DB VARCHAR(100);
	DECLARE UNION_STR VARCHAR(1000) DEFAULT '';
	
	INSERT INTO gt_gw_main.SP_LOG VALUES(TMP_GT_DB,'SP_Sub_Generate_RPT','Start', START_TIME);
	BEGIN
	SELECT GET_LOCK('GT_JOB_CREATE_LOCK', 60) INTO @bb;
	CALL gt_schedule.sp_job_create('SP_Sub_Generate_RPT',TMP_GT_DB);
	SET v_1 :=@JOB_ID;
	SELECT RELEASE_LOCK('GT_JOB_CREATE_LOCK') INTO @bb;
	SET I:=1;
	WHILE (I<=TABLE_CNT) DO
		CALL gt_schedule.sp_job_add_task(CONCAT('CALL `gt_gw_main`.',SP_Process,'(''',RPT_TBL_NAME,''',''',TMP_GT_DB,''',''',GT_DB,''',',I,',0);'),v_1);
		SET I :=I+1;
	END WHILE ;	
	
	CALL gt_schedule.sp_job_start(v_1);
	CALL gt_schedule.sp_job_enable_event();
	CALL gt_schedule.sp_job_wait(v_1);
	CALL gt_schedule.sp_job_disable_event();
	INSERT INTO gt_gw_main.SP_LOG VALUES(TMP_GT_DB,'SP_Sub_Generate_RPT',CONCAT('Insert ',RPT_TBL_NAME,' COST: ',TIMESTAMPDIFF(SECOND,STEP_START_TIME,SYSDATE()),' seconds.'), NOW());
	SET STEP_START_TIME := SYSDATE();
	
	SET @SqlCmd=CONCAT('SELECT `STATUS` INTO @JOB_STATUS FROM `gt_schedule`.`job_history` WHERE ID=',v_1,';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	IF @JOB_STATUS = 'FINISHED' THEN 
		SET @SqlCmd=CONCAT('INSERT INTO ',GT_DB,'.`report_process_state`(`SESSION_DB`,`JOB_ID`,`J_ERR_MSG`,`J_STATUS`) VALUES (''',TMP_GT_DB,''',',v_1,',NULL,1);');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
	
		INSERT INTO gt_gw_main.SP_LOG VALUES(TMP_GT_DB,'SP_Sub_Generate_RPT',CONCAT('Create ',RPT_TBL_NAME,' merge COST: ',TIMESTAMPDIFF(SECOND,STEP_START_TIME,SYSDATE()),' seconds.'), NOW());
		SET STEP_START_TIME := SYSDATE();
	ELSE 
		SET @SqlCmd=CONCAT('INSERT INTO ',GT_DB,'.`report_process_state`(`SESSION_DB`,`JOB_ID`,`J_ERR_MSG`,`J_STATUS`) VALUES (''',TMP_GT_DB,''',',v_1,',''',RPT_TBL_NAME,' Error'',0);');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;	
	
		SELECT 0 AS isSuccess,CONCAT('SP_Sub_Generate_RPT - Dispatch Not fully succeed! TABLE:',RPT_TBL_NAME) AS errorMessage;
	END IF;
	END;
	INSERT INTO gt_gw_main.SP_LOG VALUES(TMP_GT_DB,'SP_Sub_Generate_RPT',CONCAT('Done:',RPT_TBL_NAME,',',TMP_GT_DB,' ',TIMESTAMPDIFF(SECOND,START_TIME,SYSDATE()),' seconds.'), NOW());
