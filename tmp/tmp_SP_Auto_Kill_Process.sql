CREATE DEFINER=`covmo`@`%` PROCEDURE `SP_Auto_Kill_Process`( IN PROCESSTIME INT(11))
BEGIN
	DECLARE START_TIME DATETIME DEFAULT SYSDATE();	
	
	INSERT INTO gt_gw_main.sp_log VALUES('gt_gw_main','SP_Auto_Kill_Process','SP_Auto_Kill_Process Start ', NOW());
	
	SET @SqlCmd=CONCAT('SELECT GROUP_CONCAT(''KILL '',ID, '';'' SEPARATOR ''|'') into @delete_str 
				FROM information_schema.PROCESSLIST 
				WHERE `TIME` > ',PROCESSTIME,';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
							
	IF @delete_str IS NOT NULL THEN
	
		SET @v_i=1;
		SET @v_R_Max=gt_covmo_csv_count(@delete_str,'|');
	
		WHILE @v_i <= @v_R_Max DO
			BEGIN
				SET @SqlCmd = CONCAT('
						  CREATE TABLE IF NOT EXISTS  gt_gw_main.`process_killed_log` (
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
	
				SET @KILL_ID=gt_strtok(gt_strtok(gt_strtok(@delete_str,1,'|'),2,' '),1,';');
	
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
				
				SET @SqlCmd=gt_strtok(@delete_str,@v_i,'|');			
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
		
				SET @v_i=@v_i+1; 
			END;
		END WHILE;	
	INSERT INTO gt_gw_main.SP_LOG VALUES('gt_gw_main','SP_Auto_Kill_Process',CONCAT('',@v_R_Max,' Process has killed'), NOW());
		
	ELSE
	SELECT 'No process to delete!' AS Message;
	INSERT INTO gt_gw_main.SP_LOG VALUES('gt_gw_main','SP_Auto_Kill_Process','No Process killed', NOW());
	END IF;
	
	INSERT INTO gt_gw_main.SP_LOG VALUES('gt_gw_main','SP_Auto_Kill_Process',CONCAT('Done: ',TIMESTAMPDIFF(SECOND,START_TIME,SYSDATE()),' seconds.'), NOW());
