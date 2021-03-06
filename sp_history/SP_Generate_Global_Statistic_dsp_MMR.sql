DELIMITER $$
USE `gt_global_statistic`$$
DROP PROCEDURE IF EXISTS `SP_CreateDB_LTE`$$
CREATE DEFINER=`covmo`@`%` PROCEDURE `SP_Generate_Global_Statistic_dsp_MMR`(IN MAIN_WORKER_ID VARCHAR(10),IN FLAG TINYINT(2),IN exDate VARCHAR(10),IN RPT_TYPE VARCHAR(10),IN DS_FLAG TINYINT(2),PENDING_FLAG TINYINT(2),IN IMSI_CELL TINYINT(2),IN TileResolution VARCHAR(10))
BEGIN	
	DECLARE START_TIME DATETIME DEFAULT SYSDATE();
	DECLARE SESSION_NAME VARCHAR(100);
	DECLARE WORKER_ID VARCHAR(10) DEFAULT CONNECTION_ID();
	DECLARE SP_Process VARCHAR(100);
	DECLARE SP_Process_T VARCHAR(100);
	DECLARE SP_Process_C VARCHAR(100);
	DECLARE SP_Process_R VARCHAR(100);
	DECLARE SP_Process_F VARCHAR(100);
	DECLARE SP_Process_S VARCHAR(100);
	
	DECLARE SP_ERROR CONDITION FOR SQLSTATE '99996';
	DECLARE KPI_ERROR CONDITION FOR SQLSTATE '99999';
	
	DECLARE v_DATA_DATE VARCHAR(10) DEFAULT NULL;
	DECLARE v_DATA_HOUR TINYINT(2) DEFAULT NULL;
	DECLARE v_PU_ID MEDIUMINT(9) DEFAULT NULL;
	DECLARE v_TECH_MASK TINYINT(2) DEFAULT NULL;
	DECLARE v_i TINYINT(4) DEFAULT 0;
	DECLARE  memory_usage INT DEFAULT 0;
	SET SESSION group_concat_max_len=102400; 
	SELECT REPLACE(sys_eval('df -h | grep shm | awk ''{print $5}'''),'%','') INTO memory_usage;
	IF memory_usage > 60
	THEN 
		SET @SqlCmd = CONCAT('CREATE TABLE IF NOT EXISTS  gt_global_statistic.`shm_usage_log` (
						  `MEM_USAGE` INT(7) NOT NULL DEFAULT ''0'',
						  `INSERT_TIME` DATETIME DEFAULT NULL,
						   MAIN_WORKER_ID   VARCHAR(10) DEFAULT NULL,
						   FLAG TINYINT(2) DEFAULT NULL,
						   exDate VARCHAR(10) DEFAULT NULL,
						   RPT_TYPE VARCHAR(10) DEFAULT NULL
						) ENGINE=MYISAM DEFAULT CHARSET=utf8;');
		PREPARE stmt FROM @SqlCmd;
		EXECUTE stmt;
		DEALLOCATE PREPARE stmt;	
	
		SET @SqlCmd = CONCAT('
						INSERT INTO  gt_global_statistic.`shm_usage_log` 
								 
						SELECT ''',memory_usage,''',''',NOW(),''',
						''',MAIN_WORKER_ID,''',
						''',FLAG,''',
						''',exDate,''',
						''',RPT_TYPE,'''
						;');
		PREPARE stmt FROM @SqlCmd;
		EXECUTE stmt;
		DEALLOCATE PREPARE stmt;
			
	ELSE
	
	
		INSERT INTO gt_gw_main.SP_LOG VALUES('gt_global_statistic','SP_Generate_Global_Statistic_dsp_MMR','START', START_TIME);
		IF FLAG IN (0,1) THEN 
					
			SET @SqlCmd=CONCAT('SELECT GROUP_CONCAT(CONCAT(`DATA_DATE`,'','',`DATA_HOUR`,'','',`PU_ID`,'','',`TECH_MASK`) SEPARATOR ''|'' ) INTO @PU_STR
						FROM gt_global_statistic.tmp_table_call_cnt_',MAIN_WORKER_ID,' a;');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
			
			IF LENGTH(@PU_STR) >0 THEN 
				CALL gt_schedule.`sp_job_enable_event`();
				SET @JOB_ID=0;
				IF RPT_TYPE='IMSI' THEN
					CALL gt_schedule.sp_job_create('SP_Generate_Global_Statistic','SPIDER_Parallel_imsi');  
					SET SP_Process = 'SP_Generate_Global_Statistic_Sub_TmpTbl_IMSI';
				ELSEIF RPT_TYPE='TILE' THEN
					CALL gt_schedule.sp_job_create('SP_Generate_Global_Statistic','SPIDER_Parallel_tile');
					SET SP_Process_T = 'SP_Generate_Global_Statistic_Sub_TmpTbl_Tile';
				ELSEIF RPT_TYPE='CELL' THEN
					CALL gt_schedule.sp_job_create('SP_Generate_Global_Statistic','SPIDER_Parallel_cell');
					SET SP_Process_C = 'SP_Generate_Global_Statistic_Sub_TmpTbl_Cell';
				ELSEIF RPT_TYPE='ROAMER' THEN
					CALL gt_schedule.sp_job_create('SP_Generate_Global_Statistic','SPIDER_Parallel_roamer');
					SET SP_Process_R = 'SP_Generate_Global_Statistic_Sub_TmpTbl_Roamer';
				ELSEIF RPT_TYPE='FAILCAUSE' THEN
					CALL gt_schedule.sp_job_create('SP_Generate_Global_Statistic','SPIDER_Parallel_failcause');
					SET SP_Process_F = 'SP_Generate_Global_Statistic_Sub_TmpTbl_failcause';
				ELSEIF RPT_TYPE='SUBSCRIBER' THEN
					CALL gt_schedule.sp_job_create('SP_Generate_Global_Statistic','SPIDER_Parallel_subsriber');
					SET SP_Process_S = 'SP_Generate_Global_Statistic_Sub_TmpTbl_subscriber';
				END IF;			
				SET @V_Multi_PU = @JOB_ID; 
				
			END IF;
			SET @v_i=1;
			SET @cnt_imsi_1=0;
			SET @cnt_imsi_2=0;
			SET @cnt_imsi_4=0;
			SET @cnt_tile_1=0;
			SET @cnt_tile_2=0;
			SET @cnt_tile_4=0;
			SET @cnt_cell_1=0;
			SET @cnt_cell_2=0;
			SET @cnt_cell_4=0;
			SET @cnt_roamer_1=0;
			SET @cnt_roamer_2=0;
			SET @cnt_roamer_4=0;
			SET @cnt_failcause_1=0;
			SET @cnt_failcause_2=0;
			SET @cnt_failcause_4=0;
			SET @cnt_subscriber_1=0;
			SET @cnt_subscriber_2=0;
			SET @cnt_subscriber_4=0;
			SET @v_R_Max=gt_covmo_csv_count(@PU_STR,'|');
			WHILE @v_i <= @v_R_Max DO
			BEGIN
				
				SET v_DATA_DATE:=gt_covmo_csv_get(gt_strtok(@PU_STR, @v_i, '|'),1);		
				SET v_DATA_HOUR:=gt_covmo_csv_get(gt_strtok(@PU_STR, @v_i, '|'),2);
				SET v_PU_ID:=gt_covmo_csv_get(gt_strtok(@PU_STR, @v_i, '|'),3);
				SET v_TECH_MASK:=gt_covmo_csv_get(gt_strtok(@PU_STR, @v_i, '|'),4);
				SET SESSION_NAME = CONCAT('gt_',v_PU_ID,'_',DATE_FORMAT(v_DATA_DATE,'%Y%m%d'),'_0000_0000');
				
				IF RPT_TYPE='IMSI' THEN
					CALL gt_schedule.sp_job_add_task(CONCAT('CALL `gt_global_statistic`.',SP_Process,'(''',v_DATA_DATE,''',',v_DATA_HOUR,',',v_PU_ID,',',v_TECH_MASK,',''',SESSION_NAME,''',''',MAIN_WORKER_ID,''',',IMSI_CELL,');'),@V_Multi_PU);
					IF v_TECH_MASK=1 THEN SET @cnt_imsi_1=@cnt_imsi_1+1; END IF;
					IF v_TECH_MASK=2 THEN SET @cnt_imsi_2=@cnt_imsi_2+1; END IF;
					IF v_TECH_MASK=4 THEN SET @cnt_imsi_4=@cnt_imsi_4+1; END IF;
				ELSEIF RPT_TYPE='TILE' THEN
					CALL gt_schedule.sp_job_add_task(CONCAT('CALL `gt_global_statistic`.',SP_Process_T,'(''',v_DATA_DATE,''',',v_DATA_HOUR,',',v_PU_ID,',',v_TECH_MASK,',''',SESSION_NAME,''',''',MAIN_WORKER_ID,''',''',TileResolution,''');'),@V_Multi_PU);
					IF v_TECH_MASK=1 THEN SET @cnt_tile_1=@cnt_tile_1+1; END IF;
					IF v_TECH_MASK=2 THEN SET @cnt_tile_2=@cnt_tile_2+1; END IF;
					IF v_TECH_MASK=4 THEN SET @cnt_tile_4=@cnt_tile_4+1; END IF;				
				ELSEIF RPT_TYPE='CELL' THEN 
					CALL gt_schedule.sp_job_add_task(CONCAT('CALL `gt_global_statistic`.',SP_Process_C,'(''',v_DATA_DATE,''',',v_DATA_HOUR,',',v_PU_ID,',',v_TECH_MASK,',''',SESSION_NAME,''',''',MAIN_WORKER_ID,''');'),@V_Multi_PU);
					IF v_TECH_MASK=1 THEN SET @cnt_cell_1=@cnt_cell_1+1; END IF;
					IF v_TECH_MASK=2 THEN SET @cnt_cell_2=@cnt_cell_2+1; END IF;
					IF v_TECH_MASK=4 THEN SET @cnt_cell_4=@cnt_cell_4+1; END IF;
				ELSEIF RPT_TYPE='ROAMER' THEN 
					CALL gt_schedule.sp_job_add_task(CONCAT('CALL `gt_global_statistic`.',SP_Process_R,'(''',v_DATA_DATE,''',',v_DATA_HOUR,',',v_PU_ID,',',v_TECH_MASK,',''',SESSION_NAME,''',''',MAIN_WORKER_ID,''',''',TileResolution,''');'),@V_Multi_PU);
					IF v_TECH_MASK=1 THEN SET @cnt_roamer_1=@cnt_roamer_1+1; END IF;
					IF v_TECH_MASK=2 THEN SET @cnt_roamer_2=@cnt_roamer_2+1; END IF;
					IF v_TECH_MASK=4 THEN SET @cnt_roamer_4=@cnt_roamer_4+1; END IF;
				ELSEIF RPT_TYPE='FAILCAUSE' THEN 
					CALL gt_schedule.sp_job_add_task(CONCAT('CALL `gt_global_statistic`.',SP_Process_F,'(''',v_DATA_DATE,''',',v_DATA_HOUR,',',v_PU_ID,',',v_TECH_MASK,',''',SESSION_NAME,''',''',MAIN_WORKER_ID,''');'),@V_Multi_PU);
					IF v_TECH_MASK=1 THEN SET @cnt_failcause_1=@cnt_failcause_1+1; END IF;
					IF v_TECH_MASK=2 THEN SET @cnt_failcause_2=@cnt_failcause_2+1; END IF;
					IF v_TECH_MASK=4 THEN SET @cnt_failcause_4=@cnt_failcause_4+1; END IF;
				ELSEIF RPT_TYPE='SUBSCRIBER' THEN
					CALL gt_schedule.sp_job_add_task(CONCAT('CALL `gt_global_statistic`.',SP_Process_S,'(''',v_DATA_DATE,''',',v_DATA_HOUR,',',v_PU_ID,',',v_TECH_MASK,',''',SESSION_NAME,''',''',MAIN_WORKER_ID,''',',IMSI_CELL,');'),@V_Multi_PU);
					IF v_TECH_MASK=1 THEN SET @cnt_subscriber_1=@cnt_subscriber_1+1; END IF;
					IF v_TECH_MASK=2 THEN SET @cnt_subscriber_2=@cnt_subscriber_2+1; END IF;
					IF v_TECH_MASK=4 THEN SET @cnt_subscriber_4=@cnt_subscriber_4+1; END IF;
				END IF;	
				SET @v_i=@v_i+1;
			END;
			END WHILE;
			
			IF @v_i>1 THEN
				CALL gt_schedule.sp_job_start(@V_Multi_PU);
				CALL gt_schedule.sp_job_wait(@V_Multi_PU);
				CALL gt_schedule.`sp_job_disable_event`();
				SET @SqlCmd=CONCAT('SELECT `STATUS` INTO @JOB_STATUS FROM `gt_schedule`.`job_history` WHERE ID=',@V_Multi_PU,';');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				
				IF @JOB_STATUS <> 'FINISHED' THEN 
				
					SET @SqlCmd=CONCAT('SELECT SUM(IF(`STATUS`=''FINISHED'',1,0)),SUM(IF(`STATUS`=''FAILED'',1,0)) INTO @STATUS_S_TILE,@STATUS_F_TILE FROM `gt_schedule`.`job_task_history` WHERE `JOB_ID`=',@V_Multi_PU,' GROUP BY `JOB_ID`;');
					PREPARE Stmt FROM @SqlCmd;
					EXECUTE Stmt;
					DEALLOCATE PREPARE Stmt;			
					INSERT INTO `gt_gw_main`.`tbl_rpt_error` (`PID`,`CreateTime`,`error_str`) VALUES (@V_Multi_PU,NOW(),CONCAT('SP_Generate_Global_Statistic_dsp - NOT successed !!! IMSI_WORK_ID=',@V_Multi_PU,', Success=',@STATUS_S_TILE,' Fail=',@STATUS_F_TILE));
					
					SELECT CONCAT('SP_Generate_Global_Statistic_dsp - NOT fully successed !!! IMSI_TILE_WORK_ID=',@V_Multi_PU,', Success=',@STATUS_S_TILE,' Fail=',@STATUS_F_TILE) AS Str;
				END IF;
				
				CALL gt_schedule.`sp_job_enable_event`();
				
				SET @JOB_ID=0;
				IF RPT_TYPE='IMSI' THEN
					SET @V_Multi_M_Aggr=0;
					CALL gt_schedule.sp_job_create('SP_Generate_Global_Statistic','Multi_M_Aggr');
					SET @V_Multi_M_Aggr = @JOB_ID;
					IF @cnt_imsi_1>0 THEN 
						CALL gt_schedule.sp_job_add_task(CONCAT('CALL `gt_global_statistic`.`SP_Generate_Global_Statistic_Sub_imsi_Aggr`(1,''',MAIN_WORKER_ID,''',',FLAG,',NULL,',PENDING_FLAG,',',IMSI_CELL,');'),@V_Multi_M_Aggr);
					END IF;
					IF @cnt_imsi_2>0 THEN 
						CALL gt_schedule.sp_job_add_task(CONCAT('CALL `gt_global_statistic`.`SP_Generate_Global_Statistic_Sub_imsi_Aggr`(2,''',MAIN_WORKER_ID,''',',FLAG,',NULL,',PENDING_FLAG,',',IMSI_CELL,');'),@V_Multi_M_Aggr);
					END IF;				
					IF @cnt_imsi_4>0 THEN 
						CALL gt_schedule.sp_job_add_task(CONCAT('CALL `gt_global_statistic`.`SP_Generate_Global_Statistic_Sub_imsi_Aggr`(4,''',MAIN_WORKER_ID,''',',FLAG,',NULL,',PENDING_FLAG,',',IMSI_CELL,');'),@V_Multi_M_Aggr);
					END IF;				
					CALL gt_schedule.sp_job_start(@V_Multi_M_Aggr);
					CALL gt_schedule.sp_job_wait(@V_Multi_M_Aggr);
				ELSEIF RPT_TYPE='TILE' THEN
					SET @V_Multi_T_Aggr=0;
					CALL gt_schedule.sp_job_create('SP_Generate_Global_Statistic','Multi_T_Aggr');
					SET @V_Multi_T_Aggr = @JOB_ID;
					IF @cnt_tile_1>0 THEN 
						CALL gt_schedule.sp_job_add_task(CONCAT('CALL `gt_global_statistic`.`SP_Generate_Global_Statistic_Sub_tile_Aggr`(1,''',MAIN_WORKER_ID,''',',FLAG,',NULL,',DS_FLAG,',',PENDING_FLAG,',''',TileResolution,''');'),@V_Multi_T_Aggr);
					END IF;
					IF @cnt_tile_2>0 THEN 
						CALL gt_schedule.sp_job_add_task(CONCAT('CALL `gt_global_statistic`.`SP_Generate_Global_Statistic_Sub_tile_Aggr`(2,''',MAIN_WORKER_ID,''',',FLAG,',NULL,',DS_FLAG,',',PENDING_FLAG,',''',TileResolution,''');'),@V_Multi_T_Aggr);
					END IF;
					IF @cnt_tile_4>0 THEN 
						CALL gt_schedule.sp_job_add_task(CONCAT('CALL `gt_global_statistic`.`SP_Generate_Global_Statistic_Sub_tile_Aggr`(4,''',MAIN_WORKER_ID,''',',FLAG,',NULL,',DS_FLAG,',',PENDING_FLAG,',''',TileResolution,''');'),@V_Multi_T_Aggr);
					END IF;				
					
					CALL gt_schedule.sp_job_start(@V_Multi_T_Aggr);
					CALL gt_schedule.sp_job_wait(@V_Multi_T_Aggr);
				ELSEIF RPT_TYPE='CELL' THEN
					SET @V_Multi_C_Aggr = 0;
					CALL gt_schedule.sp_job_create('SP_Generate_Global_Statistic','Multi_C_Aggr');
					SET @V_Multi_C_Aggr = @JOB_ID;
					IF @cnt_cell_1>0 THEN 
						CALL gt_schedule.sp_job_add_task(CONCAT('CALL `gt_global_statistic`.`SP_Generate_Global_Statistic_Sub_cell_Aggr`(1,''',MAIN_WORKER_ID,''',',FLAG,',NULL,',DS_FLAG,',',PENDING_FLAG,');'),@V_Multi_C_Aggr);
					END IF;
					IF @cnt_cell_2>0 THEN 
						CALL gt_schedule.sp_job_add_task(CONCAT('CALL `gt_global_statistic`.`SP_Generate_Global_Statistic_Sub_cell_Aggr`(2,''',MAIN_WORKER_ID,''',',FLAG,',NULL,',DS_FLAG,',',PENDING_FLAG,');'),@V_Multi_C_Aggr);
					END IF;
					IF @cnt_cell_4>0 THEN 
						CALL gt_schedule.sp_job_add_task(CONCAT('CALL `gt_global_statistic`.`SP_Generate_Global_Statistic_Sub_cell_Aggr`(4,''',MAIN_WORKER_ID,''',',FLAG,',NULL,',DS_FLAG,',',PENDING_FLAG,');'),@V_Multi_C_Aggr);
					END IF;
					CALL gt_schedule.sp_job_start(@V_Multi_C_Aggr);
					CALL gt_schedule.sp_job_wait(@V_Multi_C_Aggr);
				ELSEIF RPT_TYPE='ROAMER' THEN
					SET @V_Multi_R_Aggr=0;
					CALL gt_schedule.sp_job_create('SP_Generate_Global_Statistic','Multi_R_Aggr');
					SET @V_Multi_R_Aggr = @JOB_ID;
					IF @cnt_roamer_1>0 THEN 
						CALL gt_schedule.sp_job_add_task(CONCAT('CALL `gt_global_statistic`.`SP_Generate_Global_Statistic_Sub_roamer_Aggr`(1,''',MAIN_WORKER_ID,''',',FLAG,',NULL,',DS_FLAG,',',PENDING_FLAG,',''',TileResolution,''');'),@V_Multi_R_Aggr);
					END IF;
					IF @cnt_roamer_2>0 THEN 
						CALL gt_schedule.sp_job_add_task(CONCAT('CALL `gt_global_statistic`.`SP_Generate_Global_Statistic_Sub_roamer_Aggr`(2,''',MAIN_WORKER_ID,''',',FLAG,',NULL,',DS_FLAG,',',PENDING_FLAG,',''',TileResolution,''');'),@V_Multi_R_Aggr);
					END IF;
					IF @cnt_roamer_4>0 THEN 
						CALL gt_schedule.sp_job_add_task(CONCAT('CALL `gt_global_statistic`.`SP_Generate_Global_Statistic_Sub_roamer_Aggr`(4,''',MAIN_WORKER_ID,''',',FLAG,',NULL,',DS_FLAG,',',PENDING_FLAG,',''',TileResolution,''');'),@V_Multi_R_Aggr);
					END IF;				
					
					CALL gt_schedule.sp_job_start(@V_Multi_R_Aggr);
					CALL gt_schedule.sp_job_wait(@V_Multi_R_Aggr);
				ELSEIF RPT_TYPE='FAILCAUSE' THEN
					SET @V_Multi_F_Aggr=0;
					CALL gt_schedule.sp_job_create('SP_Generate_Global_Statistic','Multi_F_Aggr');
					SET @V_Multi_F_Aggr = @JOB_ID;
					IF @cnt_failcause_2>0 THEN 
						CALL gt_schedule.sp_job_add_task(CONCAT('CALL `gt_global_statistic`.`SP_Generate_Global_Statistic_Sub_failcause_Aggr`(2,''',MAIN_WORKER_ID,''',',FLAG,',NULL,',DS_FLAG,',',PENDING_FLAG,');'),@V_Multi_F_Aggr);
					END IF;
					IF @cnt_failcause_4>0 THEN 
						CALL gt_schedule.sp_job_add_task(CONCAT('CALL `gt_global_statistic`.`SP_Generate_Global_Statistic_Sub_failcause_Aggr`(4,''',MAIN_WORKER_ID,''',',FLAG,',NULL,',DS_FLAG,',',PENDING_FLAG,');'),@V_Multi_F_Aggr);
					END IF;				
					
					CALL gt_schedule.sp_job_start(@V_Multi_F_Aggr);
					CALL gt_schedule.sp_job_wait(@V_Multi_F_Aggr);
				ELSEIF RPT_TYPE='SUBSCRIBER' THEN
					SET @V_Multi_S_Aggr=0;
					CALL gt_schedule.sp_job_create('SP_Generate_Global_Statistic','Multi_S_Aggr');
					SET @V_Multi_S_Aggr = @JOB_ID;
					IF @cnt_subscriber_2>0 THEN 
						CALL gt_schedule.sp_job_add_task(CONCAT('CALL `gt_global_statistic`.`SP_Generate_Global_Statistic_Sub_subscriber_Aggr`(2,''',MAIN_WORKER_ID,''',',FLAG,',NULL,',DS_FLAG,',',PENDING_FLAG,');'),@V_Multi_S_Aggr);
					END IF;
					IF @cnt_subscriber_4>0 THEN 
						CALL gt_schedule.sp_job_add_task(CONCAT('CALL `gt_global_statistic`.`SP_Generate_Global_Statistic_Sub_subscriber_Aggr`(4,''',MAIN_WORKER_ID,''',',FLAG,',NULL,',DS_FLAG,',',PENDING_FLAG,');'),@V_Multi_S_Aggr);
					END IF;				
					
					CALL gt_schedule.sp_job_start(@V_Multi_S_Aggr);
					CALL gt_schedule.sp_job_wait(@V_Multi_S_Aggr);
				END IF;	
				
				CALL gt_schedule.`sp_job_disable_event`();	
				
			ELSE 
				SELECT 'NO NEW DATA Imported !!!' AS Str;
			END IF;
		ELSE 
			SET @SqlCmd=CONCAT('SELECT GROUP_CONCAT(`TECH_MASK`) INTO @TECH_STR
						FROM gt_global_statistic.tmp_table_call_cnt_',MAIN_WORKER_ID,' a;');
		
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
			
		
			CALL gt_schedule.`sp_job_enable_event`();
				SET @JOB_ID=0;
				IF RPT_TYPE='IMSI' THEN
					SET @V_Multi_M_Aggr=0;
					CALL gt_schedule.sp_job_create('SP_Generate_Global_Statistic','Multi_M_Aggr');
					SET @V_Multi_M_Aggr = @JOB_ID;
						IF LOCATE('1',@TECH_STR)>0 THEN 
							CALL gt_schedule.sp_job_add_task(CONCAT('CALL `gt_global_statistic`.`SP_Generate_Global_Statistic_Sub_imsi_Aggr`(1,''',MAIN_WORKER_ID,''',2,''',exDate,''',',PENDING_FLAG,',',IMSI_CELL,');'),@V_Multi_M_Aggr);
						END IF;
						IF LOCATE('2',@TECH_STR)>0 THEN  
							CALL gt_schedule.sp_job_add_task(CONCAT('CALL `gt_global_statistic`.`SP_Generate_Global_Statistic_Sub_imsi_Aggr`(2,''',MAIN_WORKER_ID,''',2,''',exDate,''',',PENDING_FLAG,',',IMSI_CELL,');'),@V_Multi_M_Aggr);
						END IF;
						IF LOCATE('4',@TECH_STR)>0 THEN 
							CALL gt_schedule.sp_job_add_task(CONCAT('CALL `gt_global_statistic`.`SP_Generate_Global_Statistic_Sub_imsi_Aggr`(4,''',MAIN_WORKER_ID,''',2,''',exDate,''',',PENDING_FLAG,',',IMSI_CELL,');'),@V_Multi_M_Aggr);
						END IF;
					CALL gt_schedule.sp_job_start(@V_Multi_M_Aggr);
					CALL gt_schedule.sp_job_wait(@V_Multi_M_Aggr);
				ELSEIF RPT_TYPE='TILE' THEN
					SET @V_Multi_T_Aggr=0;
					CALL gt_schedule.sp_job_create('SP_Generate_Global_Statistic','Multi_T_Aggr');
					SET @V_Multi_T_Aggr = @JOB_ID;
						IF LOCATE('1',@TECH_STR)>0 THEN
							CALL gt_schedule.sp_job_add_task(CONCAT('CALL `gt_global_statistic`.`SP_Generate_Global_Statistic_Sub_tile_Aggr`(1,''',MAIN_WORKER_ID,''',2,''',exDate,''',',DS_FLAG,',',PENDING_FLAG,',''',TileResolution,''');'),@V_Multi_T_Aggr);
						END IF;
						IF LOCATE('2',@TECH_STR)>0 THEN 
							CALL gt_schedule.sp_job_add_task(CONCAT('CALL `gt_global_statistic`.`SP_Generate_Global_Statistic_Sub_tile_Aggr`(2,''',MAIN_WORKER_ID,''',2,''',exDate,''',',DS_FLAG,',',PENDING_FLAG,',''',TileResolution,''');'),@V_Multi_T_Aggr);
						END IF;
						IF LOCATE('4',@TECH_STR)>0 THEN 
							CALL gt_schedule.sp_job_add_task(CONCAT('CALL `gt_global_statistic`.`SP_Generate_Global_Statistic_Sub_tile_Aggr`(4,''',MAIN_WORKER_ID,''',2,''',exDate,''',',DS_FLAG,',',PENDING_FLAG,',''',TileResolution,''');'),@V_Multi_T_Aggr);
						END IF;
					
					CALL gt_schedule.sp_job_start(@V_Multi_T_Aggr);
					CALL gt_schedule.sp_job_wait(@V_Multi_T_Aggr);
				ELSEIF RPT_TYPE='CELL' THEN
					SET @V_Multi_C_Aggr = 0;
					CALL gt_schedule.sp_job_create('SP_Generate_Global_Statistic','Multi_C_Aggr');
					SET @V_Multi_C_Aggr = @JOB_ID;				
						IF LOCATE('1',@TECH_STR)>0 THEN
							CALL gt_schedule.sp_job_add_task(CONCAT('CALL `gt_global_statistic`.`SP_Generate_Global_Statistic_Sub_cell_Aggr`(1,''',MAIN_WORKER_ID,''',2,''',exDate,''',',DS_FLAG,',',PENDING_FLAG,');'),@V_Multi_C_Aggr);
						END IF;
						IF LOCATE('2',@TECH_STR)>0 THEN 
							CALL gt_schedule.sp_job_add_task(CONCAT('CALL `gt_global_statistic`.`SP_Generate_Global_Statistic_Sub_cell_Aggr`(2,''',MAIN_WORKER_ID,''',2,''',exDate,''',',DS_FLAG,',',PENDING_FLAG,');'),@V_Multi_C_Aggr);
						END IF;
						IF LOCATE('4',@TECH_STR)>0 THEN 
							CALL gt_schedule.sp_job_add_task(CONCAT('CALL `gt_global_statistic`.`SP_Generate_Global_Statistic_Sub_cell_Aggr`(4,''',MAIN_WORKER_ID,''',2,''',exDate,''',',DS_FLAG,',',PENDING_FLAG,');'),@V_Multi_C_Aggr);
						END IF;				
					CALL gt_schedule.sp_job_start(@V_Multi_C_Aggr);
					CALL gt_schedule.sp_job_wait(@V_Multi_C_Aggr);
				ELSEIF RPT_TYPE='ROAMER' THEN
					SET @V_Multi_R_Aggr=0;
					CALL gt_schedule.sp_job_create('SP_Generate_Global_Statistic','Multi_R_Aggr');
					SET @V_Multi_R_Aggr = @JOB_ID;
						IF LOCATE('1',@TECH_STR)>0 THEN				
							CALL gt_schedule.sp_job_add_task(CONCAT('CALL `gt_global_statistic`.`SP_Generate_Global_Statistic_Sub_roamer_Aggr`(1,''',MAIN_WORKER_ID,''',2,''',exDate,''',',DS_FLAG,',',PENDING_FLAG,',''',TileResolution,''');'),@V_Multi_R_Aggr);
						END IF;
						IF LOCATE('2',@TECH_STR)>0 THEN 
							CALL gt_schedule.sp_job_add_task(CONCAT('CALL `gt_global_statistic`.`SP_Generate_Global_Statistic_Sub_roamer_Aggr`(2,''',MAIN_WORKER_ID,''',2,''',exDate,''',',DS_FLAG,',',PENDING_FLAG,',''',TileResolution,''');'),@V_Multi_R_Aggr);
						END IF;
						IF LOCATE('4',@TECH_STR)>0 THEN 
							CALL gt_schedule.sp_job_add_task(CONCAT('CALL `gt_global_statistic`.`SP_Generate_Global_Statistic_Sub_roamer_Aggr`(4,''',MAIN_WORKER_ID,''',2,''',exDate,''',',DS_FLAG,',',PENDING_FLAG,',''',TileResolution,''');'),@V_Multi_R_Aggr);
						END IF;
					CALL gt_schedule.sp_job_start(@V_Multi_R_Aggr);
					CALL gt_schedule.sp_job_wait(@V_Multi_R_Aggr);
				END IF;	
			CALL gt_schedule.`sp_job_disable_event`();			
		END IF;
	END IF;
	INSERT INTO gt_gw_main.SP_LOG VALUES('gt_global_statistic','SP_Generate_Global_Statistic_dsp_MMR',CONCAT(MAIN_WORKER_ID,' Done: ',TIMESTAMPDIFF(SECOND,START_TIME,SYSDATE()),' seconds.'), NOW());
END$$
DELIMITER ;
