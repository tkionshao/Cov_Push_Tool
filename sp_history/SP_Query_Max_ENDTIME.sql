DELIMITER $$
USE `gt_gw_main`$$
DROP PROCEDURE IF EXISTS `SP_CreateDB_LTE`$$
CREATE DEFINER=`covmo`@`%` PROCEDURE `SP_Query_Max_ENDTIME`(IN GT_DB VARCHAR(100))
BEGIN
	DECLARE WORKER_ID VARCHAR(10) DEFAULT CONNECTION_ID();
	
	CALL SP_Sub_Set_Session_Param(GT_DB);
	INSERT INTO gt_gw_main.sp_log VALUES( GT_DB,'SP_Query_Max_ENDTIME','Create', NOW());
	
	SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE  IF EXISTS ',GT_DB,'.tmp_table_call_',WORKER_ID,';');
	PREPARE stmt FROM @sqlcmd;
	EXECUTE stmt;
	DEALLOCATE PREPARE stmt;
	
	SET @SqlCmd=CONCAT('CREATE TEMPORARY TABLE ',GT_DB,'.`tmp_table_call_',WORKER_ID,'` 
	SELECT
		end_time
		,
		CASE 
		WHEN TIME(end_time) >= ''00:00'' AND TIME(end_time) < ''00:15'' THEN ''00:00'' WHEN TIME(end_time) >= ''00:15'' AND TIME(end_time) < ''00:30'' THEN ''00:15'' WHEN TIME(end_time) >= ''00:30'' AND TIME(end_time) < ''00:45'' THEN ''00:30'' WHEN TIME(end_time) >= ''00:45'' AND TIME(end_time) < ''01:00'' THEN ''00:45''
		WHEN TIME(end_time) >= ''01:00'' AND TIME(end_time) < ''01:15'' THEN ''01:00'' WHEN TIME(end_time) >= ''01:15'' AND TIME(end_time) < ''01:30'' THEN ''01:15'' WHEN TIME(end_time) >= ''01:30'' AND TIME(end_time) < ''01:45'' THEN ''01:30'' WHEN TIME(end_time) >= ''01:45'' AND TIME(end_time) < ''02:00'' THEN ''01:45''
		WHEN TIME(end_time) >= ''02:00'' AND TIME(end_time) < ''02:15'' THEN ''02:00'' WHEN TIME(end_time) >= ''02:15'' AND TIME(end_time) < ''02:30'' THEN ''02:15'' WHEN TIME(end_time) >= ''02:30'' AND TIME(end_time) < ''02:45'' THEN ''02:30'' WHEN TIME(end_time) >= ''02:45'' AND TIME(end_time) < ''03:00'' THEN ''02:45''
		WHEN TIME(end_time) >= ''03:00'' AND TIME(end_time) < ''03:15'' THEN ''03:00'' WHEN TIME(end_time) >= ''03:15'' AND TIME(end_time) < ''03:30'' THEN ''03:15'' WHEN TIME(end_time) >= ''03:30'' AND TIME(end_time) < ''03:45'' THEN ''03:30'' WHEN TIME(end_time) >= ''03:45'' AND TIME(end_time) < ''04:00'' THEN ''03:45''
		WHEN TIME(end_time) >= ''04:00'' AND TIME(end_time) < ''04:15'' THEN ''04:00'' WHEN TIME(end_time) >= ''04:15'' AND TIME(end_time) < ''04:30'' THEN ''04:15'' WHEN TIME(end_time) >= ''04:30'' AND TIME(end_time) < ''04:45'' THEN ''04:30'' WHEN TIME(end_time) >= ''04:45'' AND TIME(end_time) < ''05:00'' THEN ''04:45''
		WHEN TIME(end_time) >= ''05:00'' AND TIME(end_time) < ''05:15'' THEN ''05:00'' WHEN TIME(end_time) >= ''05:15'' AND TIME(end_time) < ''05:30'' THEN ''05:15'' WHEN TIME(end_time) >= ''05:30'' AND TIME(end_time) < ''05:45'' THEN ''05:30'' WHEN TIME(end_time) >= ''05:45'' AND TIME(end_time) < ''06:00'' THEN ''05:45''
		WHEN TIME(end_time) >= ''06:00'' AND TIME(end_time) < ''06:15'' THEN ''06:00'' WHEN TIME(end_time) >= ''06:15'' AND TIME(end_time) < ''06:30'' THEN ''06:15'' WHEN TIME(end_time) >= ''06:30'' AND TIME(end_time) < ''06:45'' THEN ''06:30'' WHEN TIME(end_time) >= ''06:45'' AND TIME(end_time) < ''07:00'' THEN ''06:45''
		WHEN TIME(end_time) >= ''07:00'' AND TIME(end_time) < ''07:15'' THEN ''07:00'' WHEN TIME(end_time) >= ''07:15'' AND TIME(end_time) < ''07:30'' THEN ''07:15'' WHEN TIME(end_time) >= ''07:30'' AND TIME(end_time) < ''07:45'' THEN ''07:30'' WHEN TIME(end_time) >= ''07:45'' AND TIME(end_time) < ''08:00'' THEN ''07:45''
		WHEN TIME(end_time) >= ''08:00'' AND TIME(end_time) < ''08:15'' THEN ''08:00'' WHEN TIME(end_time) >= ''08:15'' AND TIME(end_time) < ''08:30'' THEN ''08:15'' WHEN TIME(end_time) >= ''08:30'' AND TIME(end_time) < ''08:45'' THEN ''08:30'' WHEN TIME(end_time) >= ''08:45'' AND TIME(end_time) < ''09:00'' THEN ''08:45''
		WHEN TIME(end_time) >= ''09:00'' AND TIME(end_time) < ''09:15'' THEN ''09:00'' WHEN TIME(end_time) >= ''09:15'' AND TIME(end_time) < ''09:30'' THEN ''09:15'' WHEN TIME(end_time) >= ''09:30'' AND TIME(end_time) < ''09:45'' THEN ''09:30'' WHEN TIME(end_time) >= ''09:45'' AND TIME(end_time) < ''10:00'' THEN ''09:45''
		WHEN TIME(end_time) >= ''10:00'' AND TIME(end_time) < ''10:15'' THEN ''10:00'' WHEN TIME(end_time) >= ''10:15'' AND TIME(end_time) < ''10:30'' THEN ''10:15'' WHEN TIME(end_time) >= ''10:30'' AND TIME(end_time) < ''10:45'' THEN ''10:30'' WHEN TIME(end_time) >= ''10:45'' AND TIME(end_time) < ''11:00'' THEN ''10:45''
		WHEN TIME(end_time) >= ''11:00'' AND TIME(end_time) < ''11:15'' THEN ''11:00'' WHEN TIME(end_time) >= ''11:15'' AND TIME(end_time) < ''11:30'' THEN ''11:15'' WHEN TIME(end_time) >= ''11:30'' AND TIME(end_time) < ''11:45'' THEN ''11:30'' WHEN TIME(end_time) >= ''11:45'' AND TIME(end_time) < ''12:00'' THEN ''11:45''
		WHEN TIME(end_time) >= ''12:00'' AND TIME(end_time) < ''12:15'' THEN ''12:00'' WHEN TIME(end_time) >= ''12:15'' AND TIME(end_time) < ''12:30'' THEN ''12:15'' WHEN TIME(end_time) >= ''12:30'' AND TIME(end_time) < ''12:45'' THEN ''12:30'' WHEN TIME(end_time) >= ''12:45'' AND TIME(end_time) < ''13:00'' THEN ''12:45''
		WHEN TIME(end_time) >= ''13:00'' AND TIME(end_time) < ''13:15'' THEN ''13:00'' WHEN TIME(end_time) >= ''13:15'' AND TIME(end_time) < ''13:30'' THEN ''13:15'' WHEN TIME(end_time) >= ''13:30'' AND TIME(end_time) < ''13:45'' THEN ''13:30'' WHEN TIME(end_time) >= ''13:45'' AND TIME(end_time) < ''14:00'' THEN ''13:45''
		WHEN TIME(end_time) >= ''14:00'' AND TIME(end_time) < ''14:15'' THEN ''14:00'' WHEN TIME(end_time) >= ''14:15'' AND TIME(end_time) < ''14:30'' THEN ''14:15'' WHEN TIME(end_time) >= ''14:30'' AND TIME(end_time) < ''14:45'' THEN ''14:30'' WHEN TIME(end_time) >= ''14:45'' AND TIME(end_time) < ''15:00'' THEN ''14:45''
		WHEN TIME(end_time) >= ''15:00'' AND TIME(end_time) < ''15:15'' THEN ''15:00'' WHEN TIME(end_time) >= ''15:15'' AND TIME(end_time) < ''15:30'' THEN ''15:15'' WHEN TIME(end_time) >= ''15:30'' AND TIME(end_time) < ''15:45'' THEN ''15:30'' WHEN TIME(end_time) >= ''15:45'' AND TIME(end_time) < ''16:00'' THEN ''15:45''
		WHEN TIME(end_time) >= ''16:00'' AND TIME(end_time) < ''16:15'' THEN ''16:00'' WHEN TIME(end_time) >= ''16:15'' AND TIME(end_time) < ''16:30'' THEN ''16:15'' WHEN TIME(end_time) >= ''16:30'' AND TIME(end_time) < ''16:45'' THEN ''16:30'' WHEN TIME(end_time) >= ''16:45'' AND TIME(end_time) < ''17:00'' THEN ''16:45''
		WHEN TIME(end_time) >= ''17:00'' AND TIME(end_time) < ''17:15'' THEN ''17:00'' WHEN TIME(end_time) >= ''17:15'' AND TIME(end_time) < ''17:30'' THEN ''17:15'' WHEN TIME(end_time) >= ''17:30'' AND TIME(end_time) < ''17:45'' THEN ''17:30'' WHEN TIME(end_time) >= ''17:45'' AND TIME(end_time) < ''18:00'' THEN ''17:45''
		WHEN TIME(end_time) >= ''18:00'' AND TIME(end_time) < ''18:15'' THEN ''18:00'' WHEN TIME(end_time) >= ''18:15'' AND TIME(end_time) < ''18:30'' THEN ''18:15'' WHEN TIME(end_time) >= ''18:30'' AND TIME(end_time) < ''18:45'' THEN ''18:30'' WHEN TIME(end_time) >= ''18:45'' AND TIME(end_time) < ''19:00'' THEN ''18:45''
		WHEN TIME(end_time) >= ''19:00'' AND TIME(end_time) < ''19:15'' THEN ''19:00'' WHEN TIME(end_time) >= ''19:15'' AND TIME(end_time) < ''19:30'' THEN ''19:15'' WHEN TIME(end_time) >= ''19:30'' AND TIME(end_time) < ''19:45'' THEN ''19:30'' WHEN TIME(end_time) >= ''19:45'' AND TIME(end_time) < ''20:00'' THEN ''19:45''
		WHEN TIME(end_time) >= ''20:00'' AND TIME(end_time) < ''20:15'' THEN ''20:00'' WHEN TIME(end_time) >= ''20:15'' AND TIME(end_time) < ''20:30'' THEN ''20:15'' WHEN TIME(end_time) >= ''20:30'' AND TIME(end_time) < ''20:45'' THEN ''20:30'' WHEN TIME(end_time) >= ''20:45'' AND TIME(end_time) < ''21:00'' THEN ''20:45''
		WHEN TIME(end_time) >= ''21:00'' AND TIME(end_time) < ''21:15'' THEN ''21:00'' WHEN TIME(end_time) >= ''21:15'' AND TIME(end_time) < ''21:30'' THEN ''21:15'' WHEN TIME(end_time) >= ''21:30'' AND TIME(end_time) < ''21:45'' THEN ''21:30'' WHEN TIME(end_time) >= ''21:45'' AND TIME(end_time) < ''22:00'' THEN ''21:45''
		WHEN TIME(end_time) >= ''22:00'' AND TIME(end_time) < ''22:15'' THEN ''22:00'' WHEN TIME(end_time) >= ''22:15'' AND TIME(end_time) < ''22:30'' THEN ''22:15'' WHEN TIME(end_time) >= ''22:30'' AND TIME(end_time) < ''22:45'' THEN ''22:30'' WHEN TIME(end_time) >= ''22:45'' AND TIME(end_time) < ''23:00'' THEN ''22:45''
		WHEN TIME(end_time) >= ''23:00'' AND TIME(end_time) < ''23:15'' THEN ''23:00'' WHEN TIME(end_time) >= ''23:15'' AND TIME(end_time) < ''23:30'' THEN ''23:15'' WHEN TIME(end_time) >= ''23:30'' AND TIME(end_time) < ''23:45'' THEN ''23:30'' WHEN TIME(end_time) >= ''23:45'' AND TIME(end_time) < ''24:00'' THEN ''23:45''
		END k
	FROM ',GT_DB,'.table_call
	;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt; 
	
	INSERT INTO gt_gw_main.sp_log VALUES( GT_DB,'SP_Query_Max_ENDTIME','index', NOW());
	
	SET @SqlCmd=CONCAT('ALTER TABLE ',GT_DB,'.`tmp_table_call_',WORKER_ID,'` ADD INDEX `k`(k);');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	
	INSERT INTO gt_gw_main.sp_log VALUES( GT_DB,'SP_Query_Max_ENDTIME','select', NOW());
	
	SET @SqlCmd=CONCAT('
		SELECT k,MAX(end_time) AS max_end_time,COUNT(*) AS call_cnt
		FROM ',GT_DB,'.`tmp_table_call_',WORKER_ID,'`
		GROUP BY k
	;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	INSERT INTO gt_gw_main.sp_log VALUES( GT_DB,'SP_Query_Max_ENDTIME',CONCAT('Done: ',TIMESTAMPDIFF(SECOND,START_TIME,SYSDATE()),' seconds.'), NOW());
			
END$$
DELIMITER ;
