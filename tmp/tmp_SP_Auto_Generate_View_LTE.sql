CREATE DEFINER=`covmo`@`%` PROCEDURE `SP_Auto_Generate_View_LTE`(IN GT_DB VARCHAR(100),IN GT_COVMO VARCHAR(100),IN DP_GW_URI VARCHAR(100))
BEGIN
	DECLARE v_1 INT;
	DECLARE l_GW_URI VARCHAR(512);
	DECLARE S_IP VARCHAR(20);
	DECLARE S_PORT VARCHAR(10);
	DECLARE S_TBL_NAME VARCHAR(100);
	DECLARE START_TIME DATETIME DEFAULT SYSDATE();	
	DECLARE SH_EH VARCHAR(9) DEFAULT RIGHT(GT_DB,9);
	DECLARE N_GT_DB VARCHAR(100) DEFAULT GT_DB;
	
	SELECT GW_URI INTO l_GW_URI FROM gt_gw_main.rnc_information WHERE RNC = gt_strtok(GT_DB,2,'_') AND TECHNOLOGY = 'LTE';
	SET l_GW_URI = DP_GW_URI;
	SELECT REPLACE(GT_DB,SH_EH,'0000_0000') INTO N_GT_DB;
	
	INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'SP_Auto_Generate_View','Start', NOW());
	ALTER EVENT gt_schedule.job_worker ENABLE  ;
	
	CALL gt_schedule.sp_job_create('SP_Connection_Test',GT_DB);
	SET v_1 = @JOB_ID;
	
	SET S_IP = REPLACE(gt_strtok(l_GW_URI,3,':'),'/','');
	SET S_PORT = REPLACE(gt_strtok(l_GW_URI,4,':'),'/','');
	SET S_TBL_NAME = CONCAT('session_information_',S_IP,'_',S_PORT);
	
	INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'SP_Auto_Generate_View',S_TBL_NAME, NOW());
	
	CALL gt_schedule.sp_job_add_task(CONCAT('CALL GT_GW_MAIN.SP_Auto_Generate_Check_Connection(''',S_TBL_NAME,''');'),v_1);
 
	INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'SP_Auto_Generate_View','Job Start', NOW());
 
	CALL gt_schedule.sp_job_start(v_1);
	CALL gt_schedule.sp_job_wait(v_1);
	
	INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'SP_Auto_Generate_View','Update gt_covmo data', NOW());
	
	SET @SqlCmd=CONCAT('REPLACE INTO ',GT_COVMO,'.session_information
			(SESSION_ID,SESSION_DB,RNC,FILE_STARTTIME,FILE_ENDTIME,STATUS,IMPORT_TIME,SESSION_START,SESSION_END,POSITION_VERSION,POSITION_START,POSITION_END,
			SP_VERSION,SP_STARTTIME,SP_ENDTIME,ORG_DB,ORG_SESSION_NAME,ORG_SESSION_IP,ORG_SESSION_PORT,REAL_SESSION_NAME,DATA_VENDOR,DELETABLE,GW_IP,RNC_VERSION,
			SESSION_TYPE,TECHNOLOGY,GW_URI,AP_URI,DS_AP_URI)
		SELECT B.*
		FROM gt_gw_main.`',S_TBL_NAME,'` B 
		WHERE B.RNC=',gt_strtok(GT_DB,2,'_'),';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt; 
	
	SET @SqlCmd=CONCAT('REPLACE INTO ',GT_COVMO,'.table_call_cnt
			(DATA_DATE,DATA_HOUR,PU_ID,SERVICETYPE,TOT_CALL_CNT,TECH_MASK,NOTE)
		SELECT B.*
		FROM gt_gw_main.',CONCAT('`table_call_cnt_',S_IP,'_',S_PORT),'` B 
		WHERE B.PU_ID=',gt_strtok(GT_DB,2,'_'),';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt; 
	
	SELECT COUNT(*) INTO @CHECK_CNT FROM `gt_schedule`.`job_task`;
	
	IF @CHECK_CNT = 0 THEN
		ALTER EVENT gt_schedule.job_worker DISABLE  ;
	END IF;
	
	INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'SP_Auto_Generate_View',CONCAT('Done: ',TIMESTAMPDIFF(SECOND,START_TIME,SYSDATE()),' seconds.'), NOW());
	
