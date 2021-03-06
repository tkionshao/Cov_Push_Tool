DELIMITER $$
USE `gt_gw_main`$$
DROP PROCEDURE IF EXISTS `SP_CreateDB_LTE`$$
CREATE DEFINER=`covmo`@`%` PROCEDURE `SP_CovMo_Cross_Query_Parallel_POL`(IN gt_db VARCHAR(100),IN session_db VARCHAR(100),IN v_source_table VARCHAR(100) ,IN target_table VARCHAR(100),IN sql_str VARCHAR(10000),IN v_schema VARCHAR(10000),IN WORKER_ID VARCHAR(10),IN v_select VARCHAR(10000),IN TECH_NAME VARCHAR(10),IN PLOYGON_ID MEDIUMINT(9),IN IMSI_GID MEDIUMINT(9))
BEGIN
	DECLARE SP_ERROR CONDITION FOR SQLSTATE '99996';
	DECLARE KPI_ERROR CONDITION FOR SQLSTATE '99999';
	DECLARE PID VARCHAR(10) DEFAULT CONNECTION_ID();
	DECLARE CCQ_TABLE VARCHAR(100) DEFAULT 'RPT_CCQ';
	DECLARE EXIT HANDLER FOR 1146
	BEGIN 
		SELECT NULL;
	
		SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS tmp_materialization_',PID,';');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
	END;
	
	SET CCQ_TABLE:=CONCAT('rpt_ccq_',WORKER_ID);
	SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS tmp_materialization_',PID,';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('CREATE TEMPORARY TABLE tmp_materialization_',PID,' ',v_schema,' ENGINE=MYISAM;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	IF PLOYGON_ID>0 OR IMSI_GID>0 THEN 
		
		SET @SqlCmd=CONCAT('select `polygon_str` into @polygon_str from `gt_covmo`.`usr_polygon` WHERE `id`=',PLOYGON_ID,';');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		 
		SET @SqlCmd=CONCAT('SELECT spider_bg_direct_sql(CONCAT(''CALL gt_gw_main.SP_CovMo_Cross_Query_Parallel_Remote(''''',REPLACE(REPLACE(sql_str,"'","''''"),v_source_table,CONCAT(session_db,'.',v_source_table)),''''',''''',@polygon_str,''''',''''',IMSI_GID,''''');'') 
			, ''tmp_materialization_',PID,'''
			, CONCAT(''HOST '''''',REPLACE(gt_strtok(DS_AP_URI,3,'':''),''/'',''''),'''''', PORT '''''',REPLACE(gt_strtok(DS_AP_URI,4,'':''),''/'',''''),'''''',USER '''''',`DS_AP_USER`,'''''', PASSWORD '''''',`DS_AP_PASSWORD`,'''''''')
			) INTO @bb FROM `gt_covmo`.`rnc_information` WHERE `RNC`=',gt_strtok(session_db,2,'_'),' AND `TECHNOLOGY`=''',TECH_NAME,'''
			;');
	
	ELSE 
		SET @SqlCmd=CONCAT('SELECT spider_bg_direct_sql(''',REPLACE(REPLACE(sql_str,"'","''"),v_source_table,CONCAT(session_db,'.',v_source_table)),''' 
			, ''tmp_materialization_',PID,'''
			, CONCAT(''HOST '''''',REPLACE(gt_strtok(DS_AP_URI,3,'':''),''/'',''''),'''''', PORT '''''',REPLACE(gt_strtok(DS_AP_URI,4,'':''),''/'',''''),'''''',USER '''''',`DS_AP_USER`,'''''', PASSWORD '''''',`DS_AP_PASSWORD`,'''''''')
			) INTO @bb FROM `gt_covmo`.`rnc_information` WHERE `RNC`=',gt_strtok(session_db,2,'_'),' AND `TECHNOLOGY`=''',TECH_NAME,'''
			;');
	END IF;
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	IF @bb=1 THEN 
				
		SET @SqlCmd=CONCAT('insert into ',GT_DB,'.',target_table,v_select,' SELECT ',REPLACE(REPLACE(v_select,')',''),'(',''),' FROM ',CONCAT('tmp_materialization_',PID),';');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
	ELSE
		INSERT INTO `gt_gw_main`.`tbl_rpt_error` (`PID`,`CreateTime`,`error_str`) VALUES (PID,NOW(),'Spider SP Execute Failed - SP_KPI_Spider_RNC');
		SIGNAL KPI_ERROR
			SET MESSAGE_TEXT = 'Spider SP Execute Failed - SP_CovMo_Cross_Query_Parallel';
	END IF;
	
	SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS tmp_materialization_',PID,';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt; 	 
	
END$$
DELIMITER ;
