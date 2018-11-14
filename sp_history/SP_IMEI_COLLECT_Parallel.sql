DELIMITER $$
USE `gt_gw_main`$$
DROP PROCEDURE IF EXISTS `SP_CreateDB_LTE`$$
CREATE DEFINER=`covmo`@`%` PROCEDURE `SP_IMEI_COLLECT_Parallel`(IN IP_VAL INT UNSIGNED,IN target_table VARCHAR(100))
BEGIN
	DECLARE SP_ERROR CONDITION FOR SQLSTATE '99996';
	DECLARE KPI_ERROR CONDITION FOR SQLSTATE '99999';
	DECLARE PID VARCHAR(10) DEFAULT CONNECTION_ID();
	DECLARE EXIT HANDLER FOR 1146
	BEGIN 
		SELECT NULL;
	
		SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS tmp_materialization_',PID,';');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
	END;
	SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS tmp_materialization_',PID,';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('CREATE TEMPORARY TABLE tmp_materialization_',PID,' 
				(
					`TAC` varchar(8) DEFAULT NULL
-- 					,PRIMARY KEY (`TAC`)
				)	
			ENGINE=MYISAM;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('SELECT spider_bg_direct_sql(CONCAT(''SELECT LEFT(`IMEI`,8) AS TAC FROM `gt_gw_main`.`dim_imsi_imei` A WHERE A.IMEI IS NOT NULL AND A.IMEI <> '''''''' AND NOT EXISTS (SELECT NULL FROM `gt_covmo`.`dim_handset` B WHERE B.`tac`=LEFT(A.`IMEI`,8));
		'') 
		, ''tmp_materialization_',PID,'''
		, CONCAT(''HOST ''''',INET_NTOA(IP_VAL),''''', PORT ''''3307'''',USER ''''covmo'''', PASSWORD ''''covmo123'''''')
		) INTO @bb 
		;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	IF @bb=1 THEN 	
		SET @SqlCmd=CONCAT('insert into ',target_table,'(TAC)',' SELECT TAC FROM ',CONCAT('tmp_materialization_',PID),';');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
	ELSE
		INSERT INTO `gt_gw_main`.`tbl_rpt_error` (`PID`,`CreateTime`,`error_str`) VALUES (PID,NOW(),'Spider SP Execute Failed - SP_IMEI_COLLECT_Parallel');
		SIGNAL KPI_ERROR
			SET MESSAGE_TEXT = 'Spider SP Execute Failed - SP_IMEI_COLLECT_Parallel';
	END IF;
	
	SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS tmp_materialization_',PID,';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt; 	 
	
END$$
DELIMITER ;
