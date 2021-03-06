DELIMITER $$
USE `gt_global_statistic`$$
DROP PROCEDURE IF EXISTS `SP_CreateDB_LTE`$$
CREATE DEFINER=`covmo`@`%` PROCEDURE `SP_Generate_Global_Statistic_DS_dsp_MMR`(IN MAIN_WORKER_ID VARCHAR(10),IN FLAG TINYINT(2),IN exDate VARCHAR(10),in RPT_TYPE CHAR(4),IN DS_FLAG TINYINT(2),PENDING_FLAG TINYINT(2),IN TileResolution VARCHAR(10))
BEGIN	
	DECLARE START_TIME DATETIME DEFAULT SYSDATE();
	DECLARE SESSION_NAME VARCHAR(100);
	DECLARE WORKER_ID VARCHAR(10) DEFAULT CONNECTION_ID();
	DECLARE SP_Process VARCHAR(100);
	DECLARE SP_Process_T VARCHAR(100);
	DECLARE SP_Process_C VARCHAR(100);
	
	DECLARE SP_ERROR CONDITION FOR SQLSTATE '99996';
	DECLARE KPI_ERROR CONDITION FOR SQLSTATE '99999';
	
	DECLARE v_DATA_DATE VARCHAR(10) DEFAULT NULL;
	DECLARE v_DATA_HOUR TINYINT(2) DEFAULT NULL;
	DECLARE v_PU_ID MEDIUMINT(9) DEFAULT NULL;
	DECLARE v_TECH_MASK TINYINT(2) DEFAULT NULL;
	DECLARE v_i TINYINT(4) DEFAULT 0;
	SET SESSION group_concat_max_len=102400; 
	
	INSERT INTO gt_gw_main.SP_LOG VALUES('gt_global_statistic','SP_Generate_Global_Statistic_DS_dsp_MMR','START', START_TIME);
	SET @SqlCmd=CONCAT('SELECT GROUP_CONCAT(CONCAT(`DATA_DATE`,'','',`DATA_HOUR`,'','',`PU_ID`,'','',`TECH_MASK`) SEPARATOR ''|'' ) INTO @PU_STR
				FROM gt_global_statistic.table_running_task_ds a;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	IF LENGTH(@PU_STR) >0 THEN 
		CALL gt_schedule.`sp_job_enable_event`();
		SET @JOB_ID=0;
		if RPT_TYPE='TILE' THEN
			CALL gt_schedule.sp_job_create('SP_Generate_Global_Statistic','SPIDER_Parallel_DS');
			SET SP_Process_T = 'SP_Generate_Global_Statistic_DS_Sub_TmpTbl_Tile';
		end if;			
		SET @V_Multi_DS_PU = @JOB_ID;
	END IF;
	SET @v_i=1;
	SET @cnt_tile_1=0;
	SET @cnt_tile_2=0;
	SET @cnt_tile_4=0;
	SET @v_R_Max=gt_covmo_csv_count(@PU_STR,'|');
	WHILE @v_i <= @v_R_Max DO
	BEGIN
		SET v_DATA_DATE:=gt_covmo_csv_get(gt_strtok(@PU_STR, @v_i, '|'),1);		
		SET v_DATA_HOUR:=gt_covmo_csv_get(gt_strtok(@PU_STR, @v_i, '|'),2);
		SET v_PU_ID:=gt_covmo_csv_get(gt_strtok(@PU_STR, @v_i, '|'),3);
		SET v_TECH_MASK:=gt_covmo_csv_get(gt_strtok(@PU_STR, @v_i, '|'),4);
		SET SESSION_NAME = CONCAT('gt_',v_PU_ID,'_',DATE_FORMAT(v_DATA_DATE,'%Y%m%d'),'_0000_0000');
		
		IF RPT_TYPE='TILE' THEN
			CALL gt_schedule.sp_job_add_task(CONCAT('CALL `gt_global_statistic`.',SP_Process_T,'(''',v_DATA_DATE,''',',v_DATA_HOUR,',',v_PU_ID,',',v_TECH_MASK,',''',SESSION_NAME,''',''',MAIN_WORKER_ID,''',1,''',TileResolution,''');'),@V_Multi_DS_PU);
			IF v_TECH_MASK=1 THEN SET @cnt_tile_1=@cnt_tile_1+1; END IF;
			IF v_TECH_MASK=2 THEN SET @cnt_tile_2=@cnt_tile_2+1; END IF;
			IF v_TECH_MASK=4 THEN SET @cnt_tile_4=@cnt_tile_4+1; END IF;
		END IF;	
		SET @v_i=@v_i+1;
	END;
	END WHILE;
	
	IF @v_i>1 THEN
		CALL gt_schedule.sp_job_start(@V_Multi_DS_PU);
		CALL gt_schedule.sp_job_wait(@V_Multi_DS_PU);
		CALL gt_schedule.`sp_job_disable_event`();
		SET @SqlCmd=CONCAT('SELECT `STATUS` INTO @JOB_STATUS FROM `gt_schedule`.`job_history` WHERE ID=',@V_Multi_DS_PU,';');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		
		IF @JOB_STATUS <> 'FINISHED' THEN 
		
			SET @SqlCmd=CONCAT('SELECT SUM(IF(`STATUS`=''FINISHED'',1,0)),SUM(IF(`STATUS`=''FAILED'',1,0)) INTO @STATUS_S_TILE,@STATUS_F_TILE FROM `gt_schedule`.`job_task_history` WHERE `JOB_ID`=',@V_Multi_DS_PU,' GROUP BY `JOB_ID`;');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;			
			INSERT INTO `gt_gw_main`.`tbl_rpt_error` (`PID`,`CreateTime`,`error_str`) VALUES (@V_Multi_DS_PU,NOW(),CONCAT('SP_Generate_Global_Statistic_DS_dsp - Not successed !!! DS_TILE_WORK_ID=',@V_Multi_DS_PU,', Success=',@STATUS_S_TILE,' Fail=',@STATUS_F_TILE));
			
			SELECT CONCAT('SP_Generate_Global_Statistic_DS_dsp - Not fully successed !!! DS_TILE_WORK_ID=',@V_Multi_DS_PU,', Success=',@STATUS_S_TILE,' Fail=',@STATUS_F_TILE) AS Str;
		END IF;
		
		CALL gt_schedule.`sp_job_enable_event`();
		
		SET @JOB_ID=0;
		IF RPT_TYPE='TILE' THEN
			SET @V_Multi_DS_T_Aggr=0;
			CALL gt_schedule.sp_job_create('SP_Generate_Global_Statistic','Multi_DS_T_Aggr');
			SET @V_Multi_DS_T_Aggr = @JOB_ID;
			if @cnt_tile_1>0 then 
				CALL gt_schedule.sp_job_add_task(CONCAT('CALL `gt_global_statistic`.`SP_Generate_Global_Statistic_DS_Sub_tile_Aggr`(1,''',MAIN_WORKER_ID,''',',FLAG,',NULL,',DS_FLAG,',',PENDING_FLAG,',''',TileResolution,''');'),@V_Multi_DS_T_Aggr);
			end if;
			if @cnt_tile_2>0 then 
				CALL gt_schedule.sp_job_add_task(CONCAT('CALL `gt_global_statistic`.`SP_Generate_Global_Statistic_DS_Sub_tile_Aggr`(2,''',MAIN_WORKER_ID,''',',FLAG,',NULL,',DS_FLAG,',',PENDING_FLAG,',''',TileResolution,''');'),@V_Multi_DS_T_Aggr);
			end if;
			if @cnt_tile_4>0 then 
				CALL gt_schedule.sp_job_add_task(CONCAT('CALL `gt_global_statistic`.`SP_Generate_Global_Statistic_DS_Sub_tile_Aggr`(4,''',MAIN_WORKER_ID,''',',FLAG,',NULL,',DS_FLAG,',',PENDING_FLAG,',''',TileResolution,''');'),@V_Multi_DS_T_Aggr);
			end if;				
			
			CALL gt_schedule.sp_job_start(@V_Multi_DS_T_Aggr);
			CALL gt_schedule.sp_job_wait(@V_Multi_DS_T_Aggr);
		END IF;	
		
		CALL gt_schedule.`sp_job_disable_event`();	
		
	ELSE 
		SELECT 'No New data Imported !!!' AS Str;
	END IF;
	INSERT INTO gt_gw_main.SP_LOG VALUES('gt_global_statistic','SP_Generate_Global_Statistic_DS_dsp_MMR',CONCAT(MAIN_WORKER_ID,' Done: ',TIMESTAMPDIFF(SECOND,START_TIME,SYSDATE()),' seconds.'), NOW());
END$$
DELIMITER ;
