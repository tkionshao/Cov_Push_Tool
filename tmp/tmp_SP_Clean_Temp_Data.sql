CREATE DEFINER=`covmo`@`%` PROCEDURE `SP_Clean_Temp_Data`(IN KEEP_DATE INT)
BEGIN	
	DECLARE START_TIME DATETIME DEFAULT SYSDATE();
	DECLARE tbl_name VARCHAR(100);
	DECLARE no_more_maps INT DEFAULT 0;
	
	DECLARE cur2 CURSOR FOR 		
		SELECT TABLE_NAME
		FROM information_schema.`TABLES`
		WHERE table_schema='gt_gw_main' AND table_name LIKE 'tbl_gt_mrg_%'
			UNION ALL		
		SELECT TABLE_NAME
		FROM information_schema.`TABLES`
		WHERE table_schema='gt_gw_main' AND table_name LIKE 'tmp_tbl_join_%'
			UNION ALL		
		SELECT TABLE_NAME
		FROM information_schema.`TABLES`
		WHERE table_schema='gt_gw_main'
		AND table_name NOT IN ('tbl_rpt_error','tbl_rpt_idx','tbl_rpt_main','tbl_rpt_main','tbl_rpt_main_col','tbl_rpt_qrystr','tbl_rpt_tmp','tbl_rpt_tmp_col','tbl_special_kpi','tbl_rpt_other')
		AND table_name LIKE 'tbl_%';
	
	DECLARE cur3 CURSOR FOR 		
		SELECT TABLE_NAME
		FROM information_schema.`TABLES`
		WHERE table_schema='gt_temp_cache' AND table_name LIKE 'rpt_ccq_%';
	
	DECLARE CONTINUE HANDLER FOR NOT FOUND SET no_more_maps = 1;
	
	INSERT INTO gt_gw_main.SP_LOG VALUES('GHTINC','SP_Clean_Temp_Data','Start', START_TIME);
	
	INSERT INTO gt_gw_main.SP_LOG VALUES('GHTINC','SP_Clean_Temp_Data','del step1', NOW());
	OPEN cur2;
	SET no_more_maps = 0;
	dept_loop:REPEAT
                FETCH cur2 INTO tbl_name;
                IF no_more_maps = 0 THEN
                        IF no_more_maps THEN
				LEAVE dept_loop;
			END IF;
			SET @SqlCmd=CONCAT('DROP TABLE IF EXISTS gt_gw_main.',tbl_name,';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
                END IF;
        UNTIL no_more_maps
        END REPEAT dept_loop;
        CLOSE cur2;
        
	INSERT INTO gt_gw_main.SP_LOG VALUES('GHTINC','SP_Clean_Temp_Data','del step2', NOW());
	OPEN cur3;
	SET no_more_maps = 0;
	dept_loop:REPEAT
                FETCH cur3 INTO tbl_name;
                IF no_more_maps = 0 THEN
                        IF no_more_maps THEN
				LEAVE dept_loop;
			END IF;
			SET @SqlCmd=CONCAT('DROP TABLE IF EXISTS `gt_temp_cache`.',tbl_name,';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
                END IF;
        UNTIL no_more_maps
        END REPEAT dept_loop;
        CLOSE cur3;
        SET no_more_maps=0;
	INSERT INTO gt_gw_main.SP_LOG VALUES('GHTINC','SP_Clean_Temp_Data','del step3', NOW());
	
	SET @SqlCmd=CONCAT('DELETE FROM gt_gw_main.sp_log WHERE start_time < ADDDATE(NOW(),INTERVAL - ',KEEP_DATE,' DAY );');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	INSERT INTO gt_gw_main.SP_LOG VALUES('GHTINC','SP_Clean_Temp_Data','del step4', NOW());
	
	SET @SqlCmd=CONCAT('DELETE FROM gt_gw_main.tbl_rpt_error WHERE CreateTime < ADDDATE(NOW(),INTERVAL - ',KEEP_DATE,' DAY );');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	INSERT INTO gt_gw_main.SP_LOG VALUES('GHTINC','SP_Clean_Temp_Data','del step5', NOW());
	
	SET @SqlCmd=CONCAT('DELETE FROM gt_gw_main.tbl_rpt_qrystr WHERE CreateTime < ADDDATE(NOW(),INTERVAL - ',KEEP_DATE,' DAY );');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
 
	INSERT INTO gt_gw_main.SP_LOG VALUES('GHTINC','SP_Clean_Temp_Data','del step6', NOW());
	
	SET @SqlCmd=CONCAT('DELETE FROM gt_schedule.job_history WHERE START_TIME < ADDDATE(NOW(),INTERVAL - ',KEEP_DATE,' DAY );');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('DELETE FROM gt_schedule.job_task_history WHERE START_TIME < ADDDATE(NOW(),INTERVAL - ',KEEP_DATE,' DAY );');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	INSERT INTO gt_gw_main.SP_LOG VALUES('GHTINC','SP_Clean_Temp_Data','del step7', NOW());
	
	SET @SqlCmd=CONCAT('DELETE FROM gt_schedule.job WHERE id NOT IN (SELECT DISTINCT job_id FROM gt_schedule.job_task);');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('DELETE FROM gt_gw_main.queue WHERE lastModified < DATE_ADD(NOW(), INTERVAL - ',KEEP_DATE,' DAY);');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	INSERT INTO gt_gw_main.SP_LOG VALUES('GHTINC','SP_Clean_Temp_Data','del step8', NOW());
	SET @SqlCmd=CONCAT('DELETE FROM gt_gw_main.job_log WHERE endtime < DATE_ADD(NOW(), INTERVAL - ',KEEP_DATE,' DAY);');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	INSERT INTO gt_gw_main.SP_LOG VALUES('GHTINC','SP_Clean_Temp_Data','del step9', NOW());
	SET @SqlCmd=CONCAT('DELETE FROM gt_gw_main.integration_log WHERE TIMESTAMP < DATE_ADD(NOW(), INTERVAL - ',KEEP_DATE,' DAY);');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	INSERT INTO gt_gw_main.SP_LOG VALUES('GHTINC','SP_Clean_Temp_Data','del step10', NOW());	
	SET @SqlCmd=CONCAT('DELETE FROM gt_gw_main.gph_event_st WHERE DATE_TIME < DATE_ADD(NOW(), INTERVAL - ',KEEP_DATE,' DAY);');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	INSERT INTO gt_gw_main.SP_LOG VALUES('GHTINC','SP_Clean_Temp_Data','del step11', NOW());
	TRUNCATE TABLE `gt_schedule`.`tbl_err`;
	
	INSERT INTO gt_gw_main.SP_LOG VALUES('GHTINC','SP_Clean_Temp_Data','del step12', NOW());
	CALL gt_gw_main.`SP_Purge_IMSI`();
	
	INSERT INTO gt_gw_main.SP_LOG VALUES('GHTINC','SP_Clean_Temp_Data','del step13', NOW());
	
	SET @SqlCmd=CONCAT('DELETE FROM `gt_schedule`.`job` WHERE `STATUS` = ''PREPARE'' AND CREATE_TIME < DATE_ADD(NOW(), INTERVAL - 1 DAY);');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('DELETE FROM `gt_schedule`.`job_task` WHERE `STATUS` = ''PREPARE'' AND CREATE_TIME < DATE_ADD(NOW(), INTERVAL - 1 DAY);');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	INSERT INTO gt_gw_main.SP_LOG VALUES('GHTINC','SP_Clean_Temp_Data',CONCAT('Done: ',TIMESTAMPDIFF(SECOND,START_TIME,SYSDATE()),' seconds.'), NOW());		
