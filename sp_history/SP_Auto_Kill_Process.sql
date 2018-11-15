DELIMITER $$
USE `gt_gw_main`$$
DROP PROCEDURE IF EXISTS `SP_CreateDB_LTE`$$
CREATE DEFINER=`covmo`@`%` PROCEDURE `SP_Auto_Kill_Process`( IN PROCESSTIME INT(11), IN TABLE_EXISTS_HOUR TINYINT(2))
BEGIN
	DECLARE START_TIME VARCHAR(30) DEFAULT CONCAT('''',SYSDATE(),'''');	
	DECLARE START_TIME2 VARCHAR(30) DEFAULT SYSDATE();	
	DECLARE data_date VARCHAR(20) DEFAULT DATE_FORMAT(NOW(),'%Y%m%d');
	DECLARE data_date_ytd VARCHAR(20) DEFAULT DATE_FORMAT(SUBDATE(data_date,1),'%Y%m%d');		
	DECLARE data_hour TINYINT(4) DEFAULT HOUR(SYSDATE());	
	DECLARE v_group_db_name VARCHAR(100) DEFAULT '';
	DECLARE no_more_maps INT DEFAULT 0;
	DECLARE tbl_name VARCHAR(100);
	DECLARE STEP_START_TIME DATETIME DEFAULT SYSDATE();
	
	INSERT INTO gt_gw_main.sp_log VALUES('gt_gw_main','SP_Auto_Kill_Process','SP_Auto_Kill_Process Start ', NOW());
	
	SET SESSION group_concat_max_len=@@max_allowed_packet;
	/*Kill expired process of Nation_wide*/
	INSERT INTO gt_gw_main.SP_LOG VALUES('gt_gw_main','SP_Auto_Kill_Process','Kill expired process of Nation_wide Start ', NOW());
	SET STEP_START_TIME := SYSDATE();	
	SET START_TIME2 := SYSDATE();	
	SET @SqlCmd=CONCAT('SELECT COUNT(*) INTO @V_NW_PORT FROM information_schema.`SCHEMATA`
					WHERE SCHEMA_NAME=''gt_global_statistic_g1'' ;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	IF @V_NW_PORT>0 THEN
		SET @SqlCmd=CONCAT('SELECT GROUP_CONCAT(''KILL '',ID, '';'' SEPARATOR ''|''),GROUP_CONCAT(ID SEPARATOR ''|'')  into @delete_str,@delete_id_str 
						FROM information_schema.PROCESSLIST  
						WHERE `TIME` > ',PROCESSTIME,' AND db= ''gt_global_statistic''; ');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
								
		IF @delete_str IS NOT NULL THEN		
			SET @v_i=1;
			SET @v_R_Max=(CHAR_LENGTH(@delete_str) - CHAR_LENGTH(REPLACE(@delete_str,'|','')))/(CHAR_LENGTH('|'))+1;
			WHILE @v_i <= @v_R_Max DO
				BEGIN
					SET @SqlCmd = CONCAT('CREATE TABLE IF NOT EXISTS  gt_global_statistic.`process_killed_log` (
									  `ID` BIGINT(4) NOT NULL DEFAULT ''0'',
									  `USER` VARCHAR(16) NOT NULL DEFAULT '''',
									  `HOST` VARCHAR(64) NOT NULL DEFAULT '''',
									  `DB` VARCHAR(64) DEFAULT NULL,
									  `COMMAND` VARCHAR(16) NOT NULL DEFAULT '''',
									  `TIME` INT(7) NOT NULL DEFAULT ''0'',
									  `STATE` VARCHAR(64) DEFAULT NULL,
									  `INFO` LONGTEXT DEFAULT NULL,
									  `INSERT_TIME` DATETIME DEFAULT NULL
									) ENGINE=MYISAM DEFAULT CHARSET=utf8;');
					PREPARE stmt FROM @SqlCmd;
					EXECUTE stmt;
					DEALLOCATE PREPARE stmt;
		
					SET @KILL_ID=SPLIT_STR(@delete_id_str,'|',@v_i);
		
					SET @SqlCmd=CONCAT('SELECT COUNT(*) INTO @DS_FLAG 
								FROM information_schema.PROCESSLIST 
								WHERE ID = ',@KILL_ID,' 
								AND db= ''gt_global_statistic'';');
					PREPARE Stmt FROM @SqlCmd;
					EXECUTE Stmt;
					DEALLOCATE PREPARE Stmt;
									
					IF @DS_FLAG>0 THEN 		
						SET @SqlCmd = CONCAT('
								INSERT INTO  gt_global_statistic.`process_killed_log` 
								(`ID` ,`USER` ,`HOST`,`DB`,`COMMAND`,`TIME`,`STATE`,`INFO`,`INSERT_TIME`) 				 
								SELECT `ID` ,`USER` ,`HOST`,`DB`,`COMMAND`,`TIME`,`STATE`,`INFO`,''',NOW(),'''
								FROM information_schema.PROCESSLIST 
								WHERE ID = ',@KILL_ID,'
								;');
						PREPARE stmt FROM @SqlCmd;
						EXECUTE stmt;
						DEALLOCATE PREPARE stmt;
						INSERT INTO gt_gw_main.SP_LOG VALUES('gt_gw_main','SP_Auto_Kill_Process',CONCAT('Process ID: ',@KILL_ID,' TRUNCATE table_running_task_ds'), NOW());
					END IF;
					
					SET @SqlCmd=SPLIT_STR(@delete_str,'|',@v_i);	
					PREPARE Stmt FROM @SqlCmd;
					EXECUTE Stmt;
					DEALLOCATE PREPARE Stmt;			
					SET @v_i=@v_i+1; 
				END;
			END WHILE;	
			INSERT INTO gt_gw_main.SP_LOG VALUES('gt_gw_main','SP_Auto_Kill_Process',CONCAT('',IFNULL(@v_R_Max,0),' Process has killed'), NOW());
		ELSE
			INSERT INTO gt_gw_main.SP_LOG VALUES('gt_gw_main','SP_Auto_Kill_Process','No Process killed', NOW());
		END IF;
		INSERT INTO gt_gw_main.SP_LOG VALUES('gt_gw_main','SP_Auto_Kill_Process',CONCAT('Kill expired process of Nation_wide cost:',TIMESTAMPDIFF(SECOND,STEP_START_TIME,SYSDATE()),' sec.'), NOW());
		SET STEP_START_TIME := SYSDATE();	
		/*Kill expired tables of Nation_wide in gt_aggregate_db*/
		SET @SqlCmd = CONCAT('CREATE TABLE IF NOT EXISTS  gt_global_statistic.`failed_table_droped_log` (
						`DB_NAME` VARCHAR(50) DEFAULT NULL,
						`TABLE_NAME` VARCHAR(50) DEFAULT NULL,
						`INSERT_TIME` DATETIME DEFAULT NULL
						) ENGINE=MYISAM DEFAULT CHARSET=utf8;');
		PREPARE stmt FROM @SqlCmd;
		EXECUTE stmt;
		DEALLOCATE PREPARE stmt;
		
		SET @SqlCmd=CONCAT('SELECT GROUP_CONCAT(DISTINCT TABLE_NAME) into @drop_str
						FROM information_schema.TABLES 
						WHERE TABLE_SCHEMA = ''gt_aggregate_db''
						AND UPDATE_TIME < DATE_SUB(',START_TIME,',INTERVAL ',TABLE_EXISTS_HOUR,' HOUR) 
						AND TABLE_NAME <> ''tmp_dim_handset'' ;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		
		IF @drop_str IS NOT NULL THEN 
			SET @v_i=1;
			SET @v_R_Max=(CHAR_LENGTH(@drop_str) - CHAR_LENGTH(REPLACE(@drop_str,',','')))/(CHAR_LENGTH(','))+1;
			WHILE @v_i <= @v_R_Max DO
				BEGIN
					SET @v_table=SPLIT_STR(@drop_str,'|',@v_i);
					SET @SqlCmd=CONCAT('DROP TABLE IF EXISTS gt_aggregate_db.',@v_table,';');
					PREPARE Stmt FROM @SqlCmd;
					EXECUTE Stmt;
					DEALLOCATE PREPARE Stmt;
		
					SET @SqlCmd = CONCAT('
							INSERT INTO  gt_global_statistic.`failed_table_droped_log` 
							(`DB_NAME` ,`TABLE_NAME` ,`INSERT_TIME`) 				 
							SELECT ''gt_aggregate_db'' ,''',@v_table,''' ,''',NOW(),'''
							;');
					PREPARE stmt FROM @SqlCmd;
					EXECUTE stmt;
					DEALLOCATE PREPARE stmt;		
					SET @v_i=@v_i+1; 
				END;
			END WHILE;			 
			INSERT INTO gt_gw_main.SP_LOG VALUES('gt_gw_main','SP_Auto_Kill_Process',CONCAT('',CAST(IFNULL(@v_R_Max,0) AS SIGNED),' tables in shm db has droped'), NOW());
		ELSE 
			INSERT INTO gt_gw_main.SP_LOG VALUES('gt_gw_main','SP_Auto_Kill_Process','DB : gt_aggregate_db No table droped', NOW());
		END IF;
		
		INSERT INTO gt_gw_main.SP_LOG VALUES('gt_gw_main','SP_Auto_Kill_Process',CONCAT('Kill expired tables of Nation_wide in gt_aggregate_db cost:',TIMESTAMPDIFF(SECOND,STEP_START_TIME,SYSDATE()),' sec.'), NOW());
		SET STEP_START_TIME := SYSDATE();
		/*Kill expired temporary tables of Nation_wide in gt_global_statistic*/
		SET @SqlCmd=CONCAT('SELECT GROUP_CONCAT(DISTINCT TABLE_NAME) into @drop_str
						FROM information_schema.TABLES 
						WHERE TABLE_SCHEMA = ''gt_global_statistic''
						AND 
						(
							(UPDATE_TIME < DATE_SUB(',START_TIME,',INTERVAL ',TABLE_EXISTS_HOUR,' HOUR) AND TABLE_NAME LIKE ''%',data_date,'%'')
							OR 
							(UPDATE_TIME < DATE_SUB(',START_TIME,',INTERVAL ',TABLE_EXISTS_HOUR,' HOUR) AND TABLE_NAME LIKE ''%',data_date_ytd,'%'')
							OR 
							(UPDATE_TIME < DATE_SUB(',START_TIME,',INTERVAL ',TABLE_EXISTS_HOUR,' HOUR) AND TABLE_NAME LIKE ''%tmp_update_regid%'')
							OR 
							(UPDATE_TIME < DATE_SUB(',START_TIME,',INTERVAL ',TABLE_EXISTS_HOUR,' HOUR) AND TABLE_NAME LIKE ''%tmp_table_call%'')
						);');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
				
		IF @drop_str IS NOT NULL THEN 
			SET @v_i=1;
			SET @v_R_Max=(CHAR_LENGTH(@drop_str) - CHAR_LENGTH(REPLACE(@drop_str,',','')))/(CHAR_LENGTH(','))+1;
			WHILE @v_i <= @v_R_Max DO
				BEGIN
					SET @v_table=SPLIT_STR(@drop_str,'|',@v_i);	
					SET @SqlCmd=CONCAT('DROP TABLE IF EXISTS gt_global_statistic.',@v_table,';');
					PREPARE Stmt FROM @SqlCmd;
					EXECUTE Stmt;
					DEALLOCATE PREPARE Stmt;
		
					SET @SqlCmd = CONCAT('
							INSERT INTO  gt_global_statistic.`failed_table_droped_log` 
							(`DB_NAME` ,`TABLE_NAME` ,`INSERT_TIME`) 				 
							SELECT ''gt_global_statistic'' ,''',@v_table,''' ,''',NOW(),'''
							;');
					PREPARE stmt FROM @SqlCmd;
					EXECUTE stmt;
					DEALLOCATE PREPARE stmt;		
					SET @v_i=@v_i+1; 
				END;
			END WHILE;	
			INSERT INTO gt_gw_main.SP_LOG VALUES('gt_gw_main','SP_Auto_Kill_Process',CONCAT('',CAST(IFNULL(@v_R_Max,0) AS SIGNED),' gt_global_statistic tmp table has droped'), NOW());
		ELSE 
			INSERT INTO gt_gw_main.SP_LOG VALUES('gt_gw_main','SP_Auto_Kill_Process','DB : gt_global_statistic No table droped', NOW());
		END IF;		
		INSERT INTO gt_gw_main.SP_LOG VALUES('gt_gw_main','SP_Auto_Kill_Process',CONCAT('Kill expired temporary tables of Nation_wide in gt_global_statistic cost:',TIMESTAMPDIFF(SECOND,STEP_START_TIME,SYSDATE()),' sec.'), NOW());
		SET STEP_START_TIME := SYSDATE();
		
		/*Kill expired temporary tables of Nation_wide in gt_global_statistic_gx*/
		SET @SqlCmd=CONCAT('SELECT GROUP_CONCAT(DISTINCT `group_id` ORDER BY `group_id` SEPARATOR ''|'') INTO @REG_GROUP FROM gt_global_statistic.`usr_polygon_reg_3`;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
								
		SET @v_reg_m=1;
		SET @v_reg_Max=(CHAR_LENGTH(@REG_GROUP) - CHAR_LENGTH(REPLACE(@REG_GROUP,'|','')))/(CHAR_LENGTH('|'))+1;
		WHILE @v_reg_m <= @v_reg_Max DO
		BEGIN		
			SET v_group_db_name=CONCAT('gt_global_statistic_g',SPLIT_STR(@REG_GROUP,'|',@v_reg_m));
			SET @SqlCmd=CONCAT('SELECT COUNT(schema_name) INTO @v_group_db_name FROM information_schema.SCHEMATA
							WHERE schema_name = ''',v_group_db_name,''';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;	
			
			IF @v_group_db_name >0 THEN
				SET @SqlCmd=CONCAT('SELECT GROUP_CONCAT(DISTINCT TABLE_NAME) into @drop_str
								FROM information_schema.TABLES 
								WHERE TABLE_SCHEMA = ''',v_group_db_name,'''
								AND (UPDATE_TIME < DATE_SUB(',START_TIME,',INTERVAL ',TABLE_EXISTS_HOUR,' HOUR) AND TABLE_NAME LIKE ''%tmp_%'')
								;');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
			
				IF @drop_str IS NOT NULL THEN 
					SET @v_i=1;
					SET @v_R_Max=(CHAR_LENGTH(@drop_str) - CHAR_LENGTH(REPLACE(@drop_str,',','')))/(CHAR_LENGTH(','))+1;
					WHILE @v_i <= @v_R_Max DO
						BEGIN
							SET @v_table=SPLIT_STR(@drop_str,',',@v_i);				
							SET @SqlCmd=CONCAT('DROP TABLE IF EXISTS ',v_group_db_name,'.',@v_table,';');
							PREPARE Stmt FROM @SqlCmd;
							EXECUTE Stmt;
							DEALLOCATE PREPARE Stmt;
				
							SET @SqlCmd = CONCAT('
									INSERT INTO  gt_global_statistic.`failed_table_droped_log` 
									(`DB_NAME` ,`TABLE_NAME` ,`INSERT_TIME`) 				 
									SELECT ''',v_group_db_name,''' ,''',@v_table,''' ,''',NOW(),'''
									;');
							PREPARE stmt FROM @SqlCmd;
							EXECUTE stmt;
							DEALLOCATE PREPARE stmt;
							SET @v_i=@v_i+1; 
						END;
					END WHILE;	
					INSERT INTO gt_gw_main.SP_LOG VALUES('gt_gw_main','SP_Auto_Kill_Process',CONCAT('',CAST(IFNULL(@v_R_Max,0) AS SIGNED),' ',v_group_db_name,' tmp table has droped'), NOW());
				ELSE 
					INSERT INTO gt_gw_main.SP_LOG VALUES('gt_gw_main','SP_Auto_Kill_Process',CONCAT('DB : ',v_group_db_name,' No table droped'), NOW());
				END IF;
			END IF;
			SET @v_reg_m=@v_reg_m+1;
		END;
		END WHILE;	
		INSERT INTO gt_gw_main.SP_LOG VALUES('gt_gw_main','SP_Auto_Kill_Process',CONCAT('Kill expired temporary tables of Nation_wide in gt_global_statistic_gx cost:',TIMESTAMPDIFF(SECOND,STEP_START_TIME,SYSDATE()),' sec.'), NOW());
		/*delete gt_schedule table for Nation Wide*/
		INSERT INTO gt_gw_main.SP_LOG VALUES('gt_gw_main','SP_Auto_Kill_Process','delete gt_schedule table for Nation Wide Start', NOW());
		SET STEP_START_TIME := SYSDATE();
		SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS gt_global_statistic.tmp_delete_processlist;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;	
		
		SET @SqlCmd=CONCAT('CREATE TEMPORARY TABLE gt_global_statistic.tmp_delete_processlist 
						SELECT (AA.WORKER_ID),  `DS` FROM
						(
							SELECT DISTINCT(A.WORKER_ID),0 AS `DS`
							FROM gt_global_statistic.table_running_task A
							WHERE NOT EXISTS
							(
								SELECT NULL 
								FROM `information_schema`.PROCESSLIST B
								WHERE A.WORKER_ID=B.ID
							)
							UNION 
							SELECT DISTINCT(C.WORKER_ID),1 AS `DS`
							FROM gt_global_statistic.table_running_task_ds C
							WHERE NOT EXISTS
							(
								SELECT NULL 
								FROM `information_schema`.PROCESSLIST D
								WHERE C.WORKER_ID=D.ID
							)
						) AA;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;	
			
		SET @SqlCmd=CONCAT('SELECT GROUP_CONCAT(CONCAT(`WORKER_ID`,'','',`DS`) SEPARATOR ''|'' ) INTO @WORKER_ID_STR
					FROM gt_global_statistic.tmp_delete_processlist ;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		
		IF @WORKER_ID_STR IS NOT NULL THEN
			SET @w_i=1;
			SET @w_R_Max=(CHAR_LENGTH(@WORKER_ID_STR) - CHAR_LENGTH(REPLACE(@WORKER_ID_STR,'|','')))/(CHAR_LENGTH('|'))+1;
			WHILE @w_i <= @w_R_Max DO
				BEGIN
					SET @WORKER_ID=SPLIT_STR(SPLIT_STR(@WORKER_ID_STR,'|',@w_i),',',1);
					SET @DS=SPLIT_STR(SPLIT_STR(@WORKER_ID_STR,'|',@w_i),',',2);
					
					IF @DS=0 THEN 
						SET @SqlCmd=CONCAT('DELETE FROM gt_global_statistic.table_running_task 
									WHERE WORKER_ID = ',@WORKER_ID,';
								');
						PREPARE Stmt FROM @SqlCmd;
						EXECUTE Stmt;
						DEALLOCATE PREPARE Stmt;
					END IF;
					
					IF @DS=1 THEN 
						SET @SqlCmd=CONCAT('DELETE FROM gt_global_statistic.table_running_task_ds 
									WHERE WORKER_ID = ',@WORKER_ID,';
								');
						PREPARE Stmt FROM @SqlCmd;
						EXECUTE Stmt;
						DEALLOCATE PREPARE Stmt;
					END IF;
					
					SET @SqlCmd=CONCAT('SELECT  GROUP_CONCAT(''KILL '',WORKER_ID, '';'' SEPARATOR ''|''),GROUP_CONCAT(WORKER_ID SEPARATOR ''|'')  INTO @delete_str,@delete_id_str  FROM gt_schedule.job_task 
								WHERE JOB_ID = ',@WORKER_ID,';
							');
					PREPARE Stmt FROM @SqlCmd;
					EXECUTE Stmt;
					DEALLOCATE PREPARE Stmt;
					
					IF @delete_str IS NOT NULL THEN 
						SET @v_i=1;
						SET @v_R_Max=(CHAR_LENGTH(@delete_str) - CHAR_LENGTH(REPLACE(@delete_str,'|','')))/(CHAR_LENGTH('|'))+1;
						WHILE @v_i <= @v_R_Max DO
							BEGIN
								SET @SqlCmd = CONCAT('CREATE TABLE IF NOT EXISTS  gt_global_statistic.`process_killed_log` (
												  `ID` BIGINT(4) NOT NULL DEFAULT ''0'',
												  `USER` VARCHAR(16) NOT NULL DEFAULT '''',
												  `HOST` VARCHAR(64) NOT NULL DEFAULT '''',
												  `DB` VARCHAR(64) DEFAULT NULL,
												  `COMMAND` VARCHAR(16) NOT NULL DEFAULT '''',
												  `TIME` INT(7) NOT NULL DEFAULT ''0'',
												  `STATE` VARCHAR(64) DEFAULT NULL,
												  `INFO` LONGTEXT DEFAULT NULL,
												  `INSERT_TIME` DATETIME DEFAULT NULL
												) ENGINE=MYISAM DEFAULT CHARSET=utf8;');
								PREPARE stmt FROM @SqlCmd;
								EXECUTE stmt;
								DEALLOCATE PREPARE stmt;
					
								SET @KILL_ID=SPLIT_STR(@delete_id_str,'|',@v_i);
					
								SET @SqlCmd=CONCAT('SELECT COUNT(*) INTO @DS_FLAG 
												FROM information_schema.PROCESSLIST 
												WHERE ID = ',@KILL_ID,' 
												AND db= ''gt_global_statistic'';');
								PREPARE Stmt FROM @SqlCmd;
								EXECUTE Stmt;
								DEALLOCATE PREPARE Stmt;
												
								IF @DS_FLAG>0 THEN 		
									SET @SqlCmd = CONCAT('INSERT INTO  gt_global_statistic.`process_killed_log` 
													(`ID` ,`USER` ,`HOST`,`DB`,`COMMAND`,`TIME`,`STATE`,`INFO`,`INSERT_TIME`) 				 
													SELECT `ID` ,`USER` ,`HOST`,`DB`,`COMMAND`,`TIME`,`STATE`,`INFO`,''',NOW(),'''
													FROM information_schema.PROCESSLIST 
													WHERE ID = ',@KILL_ID,'
													;');
									PREPARE stmt FROM @SqlCmd;
									EXECUTE stmt;
									DEALLOCATE PREPARE stmt;
									INSERT INTO gt_gw_main.SP_LOG VALUES('gt_gw_main','SP_Auto_Kill_Process',CONCAT('Process ID: ',@KILL_ID,' TRUNCATE table_running_task_ds'), NOW());
								END IF;
								
								SET @SqlCmd=SPLIT_STR(@delete_str,'|',@v_i);	
								PREPARE Stmt FROM @SqlCmd;
								EXECUTE Stmt;
								DEALLOCATE PREPARE Stmt;	
								SET @v_i=@v_i+1; 
							END;
						END WHILE;
					END IF;
					
					SET @SqlCmd=CONCAT('DELETE FROM gt_schedule.job_task 
								WHERE `JOB_ID` = ',@WORKER_ID,';
							');
					PREPARE Stmt FROM @SqlCmd;
					EXECUTE Stmt;
					DEALLOCATE PREPARE Stmt;
					
					SET @SqlCmd=CONCAT('DELETE FROM gt_schedule.job 
								WHERE ID = ',@V_JOB_ID,';
							');
					PREPARE Stmt FROM @SqlCmd;
					EXECUTE Stmt;
					DEALLOCATE PREPARE Stmt;					
					SET @w_i=@w_i+1; 
				END;
			END WHILE;	
			INSERT INTO gt_gw_main.SP_LOG VALUES('gt_gw_main','SP_Auto_Kill_Process',CONCAT('',IFNULL(@w_R_Max,0),' task has purged'), NOW());
		ELSE
			INSERT INTO gt_gw_main.SP_LOG VALUES('gt_gw_main','SP_Auto_Kill_Process','No task has purged', NOW());
		END IF;	
		INSERT INTO gt_gw_main.SP_LOG VALUES('gt_gw_main','SP_Auto_Kill_Process',CONCAT('delete gt_schedule table for Nation Wide cost:',TIMESTAMPDIFF(SECOND,STEP_START_TIME,SYSDATE()),' sec.'), NOW());
		SET STEP_START_TIME := SYSDATE();		
	END IF;
	INSERT INTO gt_gw_main.SP_LOG VALUES('gt_gw_main','SP_Auto_Kill_Process',CONCAT('Nationwide DB Done: ',TIMESTAMPDIFF(SECOND,START_TIME2,SYSDATE()),' seconds.'), NOW());
	
	/*Kill expired process for Element*/
	INSERT INTO gt_gw_main.sp_log VALUES('gt_gw_main','SP_Auto_Kill_Process','Kill expired process for Element Start ', NOW());
	SET START_TIME2 := SYSDATE();	
	SET @SqlCmd=CONCAT('SELECT GROUP_CONCAT(''KILL '',ID, '';'' SEPARATOR ''|''),GROUP_CONCAT(ID SEPARATOR ''|'')  into @delete_str,@delete_id_str 
				FROM information_schema.PROCESSLIST 
				WHERE `TIME` > ',PROCESSTIME,' AND USER=''covmo'' AND db <> ''gt_global_statistic'';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
							
	IF @delete_str IS NOT NULL THEN	
		SET @v_i=1;
		SET @v_R_Max=(CHAR_LENGTH(@delete_str) - CHAR_LENGTH(REPLACE(@delete_str,'|','')))/(CHAR_LENGTH('|'))+1;	
		WHILE @v_i <= @v_R_Max DO
			BEGIN
				SET @SqlCmd = CONCAT('CREATE TABLE IF NOT EXISTS  gt_gw_main.`process_killed_log` (
								  `ID` BIGINT(4) NOT NULL DEFAULT ''0'',
								  `USER` VARCHAR(16) NOT NULL DEFAULT '''',
								  `HOST` VARCHAR(64) NOT NULL DEFAULT '''',
								  `DB` VARCHAR(64) DEFAULT NULL,
								  `COMMAND` VARCHAR(16) NOT NULL DEFAULT '''',
								  `TIME` INT(7) NOT NULL DEFAULT ''0'',
								  `STATE` VARCHAR(64) DEFAULT NULL,
								  `INFO` LONGTEXT DEFAULT NULL,
								  `INSERT_TIME` DATETIME DEFAULT NULL
								) ENGINE=MYISAM DEFAULT CHARSET=utf8;');
				PREPARE stmt FROM @SqlCmd;
				EXECUTE stmt;
				DEALLOCATE PREPARE stmt;
	
				SET @KILL_ID=SPLIT_STR(@delete_id_str,'|',@v_i);
				SET @SqlCmd = CONCAT('
						INSERT INTO  gt_gw_main.`process_killed_log` 
						(`ID` ,`USER` ,`HOST`,`DB`,`COMMAND`,`TIME`,`STATE`,`INFO`,`INSERT_TIME`) 				 
						SELECT `ID` ,`USER` ,`HOST`,`DB`,`COMMAND`,`TIME`,`STATE`,`INFO`,''',NOW(),'''
						FROM information_schema.PROCESSLIST 
						WHERE ID = ',@KILL_ID,'
						;');
				PREPARE stmt FROM @SqlCmd;
				EXECUTE stmt;
				DEALLOCATE PREPARE stmt;	
				
				SET @SqlCmd=SPLIT_STR(@delete_str,'|',@v_i);		
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;		
				SET @v_i=@v_i+1; 
			END;
		END WHILE;	
		INSERT INTO gt_gw_main.SP_LOG VALUES('gt_gw_main','SP_Auto_Kill_Process',CONCAT(@v_R_Max,' Process has killed'), NOW());		
	ELSE
		INSERT INTO gt_gw_main.SP_LOG VALUES('gt_gw_main','SP_Auto_Kill_Process','No Process killed', NOW());
	END IF;
	INSERT INTO gt_gw_main.SP_LOG VALUES('gt_gw_main','SP_Auto_Kill_Process',CONCAT('Kill expired process for Element cost:',TIMESTAMPDIFF(SECOND,STEP_START_TIME,SYSDATE()),' sec.'), NOW());
	SET STEP_START_TIME := SYSDATE();
	/*delete gt_schedule table for Element*/
	INSERT INTO gt_gw_main.SP_LOG VALUES('gt_gw_main','SP_Auto_Kill_Process','delete gt_schedule table for Element', NOW());
	
	SET @SqlCmd=CONCAT('SELECT GROUP_CONCAT(`WORKER_ID` SEPARATOR '','' ) INTO @WORKER_ID_STR
					FROM gt_schedule.job_task A
					WHERE NOT EXISTS
					(
						SELECT NULL 
						FROM `information_schema`.PROCESSLIST B
						WHERE A.WORKER_ID=B.ID
					);');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	SET @w_R_Max=(CHAR_LENGTH(@WORKER_ID_STR) - CHAR_LENGTH(REPLACE(@WORKER_ID_STR,',','')))/(CHAR_LENGTH(','))+1;
		
	IF @WORKER_ID_STR IS NOT NULL THEN
		SET @SqlCmd=CONCAT('SELECT GROUP_CONCAT(`JOB_ID` SEPARATOR '','' ) INTO @V_JOB_ID
					FROM gt_schedule.job_task A
					WHERE WORKER_ID IN (',@WORKER_ID_STR,');');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		
		SET @SqlCmd=CONCAT('REPLACE INTO gt_schedule.job_task_history
					SELECT * FROM gt_schedule.job_task 
					WHERE WORKER_ID IN (',@WORKER_ID_STR,');');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		
		SET @SqlCmd=CONCAT('REPLACE INTO gt_schedule.job_history 
					SELECT * FROM gt_schedule.job 
					WHERE ID IN (',@V_JOB_ID,');');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		
		SET @SqlCmd=CONCAT('DELETE FROM gt_schedule.job_task WHERE WORKER_ID IN (',@WORKER_ID_STR,');');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
					
		SET @SqlCmd=CONCAT('UPDATE gt_schedule.job_task 
					SET `STATUS`=''FAILED''
					WHERE WORKER_ID IN (',@WORKER_ID_STR,') AND `STATUS`=''RUNNING'' ;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;			
		
		SET @SqlCmd=CONCAT('DELETE FROM gt_schedule.job_task 
					WHERE WORKER_ID IN (',@WORKER_ID_STR,');');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
					
		SET @SqlCmd=CONCAT('UPDATE gt_schedule.`job_history` 
					SET `STATUS`=''FAILED''
					WHERE ID IN (',@V_JOB_ID,') AND `STATUS`=''RUNNING'' ;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		
		IF @V_JOB_ID IS NOT NULL THEN
			SET @SqlCmd=CONCAT('DELETE FROM gt_schedule.job WHERE ID IN (',@V_JOB_ID,');');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;			
		END IF;
		INSERT INTO gt_gw_main.SP_LOG VALUES('gt_schedule','SP_Auto_Kill_Process',CONCAT(IFNULL(@w_R_Max,0),' task has purged'), NOW());
	ELSE
		INSERT INTO gt_gw_main.SP_LOG VALUES('gt_schedule','SP_Auto_Kill_Process','No task has purged', NOW());
		SET @SqlCmd=CONCAT('SELECT GROUP_CONCAT(`CONNECTION_ID` SEPARATOR '','' ) INTO @JOB_ID_STR
					FROM gt_schedule.job A
					WHERE NOT EXISTS
					(
						SELECT NULL 
						FROM `information_schema`.PROCESSLIST B
						WHERE A.`CONNECTION_ID`=B.ID
					);');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		SET @w_R_Max=(CHAR_LENGTH(@JOB_ID_STR) - CHAR_LENGTH(REPLACE(@JOB_ID_STR,',','')))/(CHAR_LENGTH(','))+1;
	
		IF @JOB_ID_STR IS NOT NULL THEN
			SET @SqlCmd=CONCAT('REPLACE INTO gt_schedule.job_history
						SELECT * FROM gt_schedule.job 
						WHERE CONNECTION_ID IN (',@JOB_ID_STR,');');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT('DELETE FROM gt_schedule.job WHERE CONNECTION_ID IN (',@JOB_ID_STR,');');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			INSERT INTO gt_gw_main.SP_LOG VALUES('gt_schedule','SP_Auto_Kill_Process',CONCAT(IFNULL(@w_R_Max,0),' job has purged'), NOW());
		END IF;
	END IF;
	
	INSERT INTO gt_gw_main.SP_LOG VALUES('gt_gw_main','SP_Auto_Kill_Process',CONCAT('Element DB Done: ',TIMESTAMPDIFF(SECOND,START_TIME2,SYSDATE()),' seconds.'), NOW());
	INSERT INTO gt_gw_main.SP_LOG VALUES('gt_gw_main','SP_Auto_Kill_Process',CONCAT('Done: ',TIMESTAMPDIFF(SECOND,START_TIME2,SYSDATE()),' seconds.'), NOW());
END$$
DELIMITER ;
