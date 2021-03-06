DELIMITER $$
USE `gt_gw_main`$$
DROP PROCEDURE IF EXISTS `SP_CreateDB_LTE`$$
CREATE DEFINER=`covmo`@`%` PROCEDURE `SP_Purge_IMSI`()
do_nothing:
BEGIN	
	DECLARE WORKER_ID VARCHAR(10) DEFAULT CONNECTION_ID();
	DECLARE START_TIME DATETIME DEFAULT SYSDATE();
	DECLARE STEP_START_TIME DATETIME DEFAULT SYSDATE();
	DECLARE GT_DB VARCHAR(20) DEFAULT 'gt_global_imsi';
	
	SET SESSION group_concat_max_len=1024000; 
	INSERT INTO gt_gw_main.SP_LOG VALUES(GT_DB,'SP_Purge_IMSI',CONCAT(WORKER_ID,' START'), START_TIME);
		
	SET @SqlCmd=CONCAT('SELECT (value+15) into @KeepDay FROM gt_gw_main.integration_param
				WHERE gt_group=''autoPurge'' AND gt_name=''dbHourlyDataKeepDay'';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @PAR_STR='';
				
	SET @SqlCmd=CONCAT('SELECT GROUP_CONCAT(PARTITION_NAME SEPARATOR '','' ) INTO @PAR_STR
				FROM information_schema.`PARTITIONS`
				WHERE TABLE_SCHEMA=''',GT_DB,''' AND TABLE_NAME=''table_imsi_pu''
				AND FROM_UNIXTIME(PARTITION_DESCRIPTION)<= DATE(DATE_ADD(NOW(),INTERVAL -',@KeepDay,' DAY))
				AND FROM_UNIXTIME(PARTITION_DESCRIPTION) > ''1970-02-01 00:00:00'';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	IF @PAR_STR <> '' OR @PAR_STR IS NOT NULL THEN 
		SET @v_i=1;
		SET @v_R_Max=(CHAR_LENGTH(@PAR_STR) - CHAR_LENGTH(REPLACE(@PAR_STR,',','')))/(CHAR_LENGTH(','))+1;
		WHILE @v_i <= @v_R_Max DO
		BEGIN
			SET @SqlCmd=CONCAT('ALTER TABLE ',GT_DB,'.table_imsi_pu DROP PARTITION ',SPLIT_STR(@PAR_STR,',',@v_i),';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt; 
			SET @v_i=@v_i+1;
		END;
		END WHILE;
	ELSE 
		LEAVE do_nothing;
	END IF;
	INSERT INTO gt_gw_main.SP_LOG VALUES('gt_global_imsi','SP_Purge_IMSI',CONCAT(WORKER_ID,' Done cost:',TIMESTAMPDIFF(SECOND,START_TIME,SYSDATE()),' sec.'), NOW());
	
END$$
DELIMITER ;
