CREATE DEFINER=`covmo`@`%` PROCEDURE `SP_Check_SysConfig`(IN STEP CHAR(5),IN FILEDATE VARCHAR(18),IN GT_DB VARCHAR(50),IN FROM_GT_DB VARCHAR(50))
BEGIN
	DECLARE DATA_DATE VARCHAR(50);
	INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'SP_Check_SysConfig','Start', NOW());
	
	IF STEP = 'STEP1' THEN
		SELECT GET_LOCK('GT_JOB_SYSCONFIG_LOCK', 60); 
		INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'SP_Check_SysConfig','STEP1', NOW());
		SET DATA_DATE = LEFT(FILEDATE,8);
	
		SET @SqlCmd =CONCAT('SELECT COUNT(*) INTO @sys_config_cnt FROM gt_gw_main.sys_config_log WHERE update_date = ''',DATA_DATE,''';');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		
		IF @sys_config_cnt = 0 THEN
			INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'SP_Check_SysConfig','sys_config_cnt=0', NOW());
		
			SET @SqlCmd =CONCAT('DROP TABLE IF EXISTS gt_gw_main.sys_config;');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
			
			SET @SqlCmd =CONCAT('CREATE TABLE gt_gw_main.sys_config ENGINE=MYISAM
					     SELECT * FROM gt_covmo.sys_config;');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
		
			SET @SqlCmd =CONCAT('INSERT INTO gt_gw_main.sys_config_log (update_date) values (''',FILEDATE,''')');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
		ELSE
			INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'SP_Check_SysConfig','sys_config_cnt=1', NOW());
		END IF;
		
		SET @SqlCmd =CONCAT('CREATE TABLE ',GT_DB,'.sys_config ENGINE=MYISAM
				     SELECT * FROM gt_gw_main.sys_config;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		SELECT RELEASE_LOCK('GT_JOB_SYSCONFIG_LOCK'); 
	ELSEIF STEP = 'STEP2' THEN
		INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'SP_Check_SysConfig','STEP2', NOW());
		SET @SqlCmd=CONCAT('SELECT COUNT(*) INTO @CHK FROM information_schema.TABLES WHERE TABLE_SCHEMA = ''',GT_DB,''' AND TABLE_NAME = ''sys_config'';');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt; 
		IF @CHK = 0 THEN
			INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'SP_Check_SysConfig','INSERT sys_config start', NOW());
		
			SET @SqlCmd =CONCAT('CREATE TABLE ',GT_DB,'.sys_config ENGINE=MYISAM
					     SELECT * FROM GT_COVMO.sys_config;');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
		END IF;
	
	END IF;
	
	INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'SP_Check_SysConfig','End', NOW());
