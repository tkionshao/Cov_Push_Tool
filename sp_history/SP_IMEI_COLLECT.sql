DELIMITER $$
USE `gt_gw_main`$$
DROP PROCEDURE IF EXISTS `SP_CreateDB_LTE`$$
CREATE DEFINER=`covmo`@`%` PROCEDURE `SP_IMEI_COLLECT`()
a_label:
BEGIN
	DECLARE done INT DEFAULT 0; 
	DECLARE START_TIME DATETIME DEFAULT SYSDATE();
	DECLARE UNION_STR VARCHAR(5000) DEFAULT '';
	DECLARE GT_DB VARCHAR(100) DEFAULT 'gt_temp_cache';
	DECLARE CCQ_TABLE VARCHAR(100) DEFAULT 'rpt_imei';
	DECLARE WORKER_ID VARCHAR(10) DEFAULT CONNECTION_ID();
	DECLARE i INT DEFAULT 0;
	DECLARE v_1 INT DEFAULT 0;
	DECLARE PU_ALL VARCHAR(1000) DEFAULT '';
	DECLARE ALLPU TINYINT(2) DEFAULT 0;
	
	DECLARE SP_ERROR CONDITION FOR SQLSTATE '99996';
	DECLARE KPI_ERROR CONDITION FOR SQLSTATE '99999';
	
	INSERT INTO gt_gw_main.sp_log VALUES('gt_gw_main','SP_IMEI_COLLECT','Cross Query Start', NOW());
	
	SET @SqlCmd=CONCAT('SELECT GROUP_CONCAT(DISTINCT INET_ATON(REPLACE(gt_strtok(B.`GW_URI`,3,'':''),''/'','''')) SEPARATOR '','' ) INTO @PU_GC 
				FROM `gt_covmo`.`session_information` B WHERE B.`GW_URI` IS NOT NULL
				/*AND INET_ATON(REPLACE(gt_strtok(B.`GW_URI`,3,'':''),''/'',''''))<>3232290061*/ ;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	SET PU_ALL=IFNULL(@PU_GC,'');
	SET ALLPU=1;
	
	IF IFNULL(@PU_GC,0)=0 THEN 
		SELECT 'No Data available!' AS Result;
		LEAVE a_label;
	ELSE
		
		CALL gt_schedule.sp_job_create('SP_IMEI_COLLECT',GT_DB);
		SET @V_Multi_PU = @JOB_ID;	
		SET @v_i=1;
		SET @Quotient_v=1;	
		SET @v_R_Max=gt_covmo_csv_count(PU_ALL,',');
		
		WHILE @v_i <= @v_R_Max DO
		BEGIN
			
						SET @SqlCmd=CONCAT('DROP TABLE IF EXISTS ',GT_DB,'.',CCQ_TABLE,'_',gt_covmo_csv_get(PU_ALL,@v_i),';');
						PREPARE Stmt FROM @SqlCmd;
						EXECUTE Stmt;
						DEALLOCATE PREPARE Stmt;
						 
						SET @SqlCmd=CONCAT('CREATE TABLE ',GT_DB,'.',CCQ_TABLE,'_',gt_covmo_csv_get(PU_ALL,@v_i),
							'(`TAC` varchar(8) DEFAULT NULL) ENGINE=MYISAM DEFAULT CHARSET=utf8;');
						PREPARE Stmt FROM @SqlCmd;
						EXECUTE Stmt;
						DEALLOCATE PREPARE Stmt;
												
						CALL gt_schedule.sp_job_add_task(CONCAT('CALL gt_gw_main.SP_IMEI_COLLECT_Parallel(''',gt_covmo_csv_get(PU_ALL,@v_i),''',''',GT_DB,'.',CCQ_TABLE,'_',gt_covmo_csv_get(PU_ALL,@v_i),''');'),@V_Multi_PU); 
						
						IF (@v_i=1) THEN 
							SET UNION_STR:=CONCAT(GT_DB,'.',CCQ_TABLE,'_',gt_covmo_csv_get(PU_ALL,@v_i),',');
						ELSE 
							SET UNION_STR:=CONCAT(UNION_STR,GT_DB,'.',CCQ_TABLE,'_',gt_covmo_csv_get(PU_ALL,@v_i),',');
						END IF; 
			SET @v_i=@v_i+@Quotient_v;
		END;
		END WHILE;
		SET UNION_STR =LEFT(UNION_STR,LENGTH(UNION_STR)-1);
		CALL gt_schedule.sp_job_enable_event();
		CALL gt_schedule.sp_job_start(@V_Multi_PU);
		CALL gt_schedule.sp_job_wait(@V_Multi_PU);
		CALL gt_schedule.sp_job_disable_event();
	 
		SET @SqlCmd=CONCAT('SELECT `STATUS` INTO @JOB_STATUS FROM `gt_schedule`.`job_history` WHERE ID=',@V_Multi_PU,';');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		
		IF @JOB_STATUS='FINISHED' THEN 
			SET @SqlCmd=CONCAT('DROP TABLE IF EXISTS ',GT_DB,'.',CCQ_TABLE,';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
				SET @SqlCmd=CONCAT('CREATE TABLE ',GT_DB,'.',CCQ_TABLE,'(`TAC` varchar(8) DEFAULT NULL) ENGINE=MRG_MYISAM DEFAULT CHARSET=utf8 INSERT_METHOD=FIRST UNION=(',UNION_STR,')');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt; 
				
			SET @SqlCmd=CONCAT('Succeed! "',GT_DB,'.',CCQ_TABLE,'" is ready. You can execute "select IMEI from ',GT_DB,'.',CCQ_TABLE,'  GROUP BY IMEI;" to get result. ');
			SELECT  @SqlCmd;
		ELSE
			INSERT INTO `gt_gw_main`.`tbl_rpt_error` (`PID`,`CreateTime`,`error_str`) VALUES (WORKER_ID,NOW(),CONCAT(WORKER_ID,' Main Parallel Jobs Fail - SP_IMEI_COLLECT'));
			SIGNAL SP_ERROR
				SET MESSAGE_TEXT = 'Main Parallel Jobs Fail - SP_IMEI_COLLECT';
		END IF;
	END IF;
	INSERT INTO gt_gw_main.sp_log VALUES('gt_gw_main','SP_IMEI_COLLECT',CONCAT('123',' Done: ',TIMESTAMPDIFF(SECOND,START_TIME,SYSDATE()),' seconds.'), NOW());
END$$
DELIMITER ;
