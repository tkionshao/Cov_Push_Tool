CREATE DEFINER=`covmo`@`%` PROCEDURE `SP_CovMo_Cross_Query_Parallel`(IN gt_db VARCHAR(100),IN session_db VARCHAR(100),IN v_source_table VARCHAR(100) ,IN target_table VARCHAR(100),IN sql_str LONGTEXT,IN v_schema VARCHAR(10000),IN WORKER_ID VARCHAR(10),IN v_select VARCHAR(10000),IN TECH_NAME VARCHAR(10),IN PLOYGON_ID  VARCHAR(100),IN IMSI_GID SMALLINT(6),IN CELL_GID SMALLINT(6),IN DS_AP_IP VARCHAR(20),IN DS_AP_PORT VARCHAR(5),IN DS_AP_USER VARCHAR(32),IN DS_AP_PASSWORD VARCHAR(32),IN LIMT_RAW_COUNT  SMALLINT(6))
BEGIN
	DECLARE KPI_ERROR CONDITION FOR SQLSTATE '99999';
	DECLARE PID VARCHAR(10) DEFAULT CONNECTION_ID();
	DECLARE CCQ_TABLE VARCHAR(100) DEFAULT 'RPT_CCQ';
	DECLARE EXIT HANDLER FOR 1146
	BEGIN 
		SELECT NULL;
		INSERT INTO `gt_gw_main`.`tbl_rpt_error` (`PID`,`CreateTime`,`error_str`) VALUES (PID,NOW(),'SP_CovMo_Cross_Query_Parallel - TABLE tmp_materialization_',PID,' doesnt exist');
		
		SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS tmp_materialization_',PID,';');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		
		SELECT '{tech:”ALL ”, name:”SP-Report”, status:”2”,message_id: “null”, message: “SP_CovMo_Cross_Query_Parallel Failed Table tmp_materialization_',PID,' doesnt exist. Check necessary table first.”, log_path: “”}' AS message;
	END;
	SET @@session.group_concat_max_len = @@global.max_allowed_packet;
	
	SET CCQ_TABLE:=CONCAT('rpt_ccq_',WORKER_ID);
	
	SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS tmp_materialization_',PID,';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('CREATE TEMPORARY TABLE tmp_materialization_',PID,' ',v_schema,' ENGINE=MYISAM CHARSET=UTF8 COLLATE  utf8_swedish_ci ;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	IF PLOYGON_ID>0 OR IMSI_GID>0 OR CELL_GID>0 THEN 
		SET @polygon_str = '';
		SET @imsi_group_id = '';
		SET @cell_group_id = '';
		IF PLOYGON_ID>0 THEN 
			SET @SqlCmd=CONCAT('select GROUP_CONCAT(`polygon_str` SEPARATOR ''|'') into @polygon_str from `gt_covmo`.`usr_polygon` WHERE `id` in (',PLOYGON_ID,');');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
		END IF;
		IF IMSI_GID>0 THEN
			SET @SqlCmd=CONCAT('SELECT GROUP_CONCAT(`IMSI`) into @imsi_group_id FROM `gt_covmo`.`dim_imsi` WHERE `GROUP_ID`=',IMSI_GID,';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
		END IF; 
		
		SET @SqlCmd=CONCAT('SELECT spider_bg_direct_sql(CONCAT(''CALL gt_gw_main.SP_CovMo_Cross_Query_Parallel_Remote(''''',REPLACE(REPLACE(sql_str,"'","''''"),v_source_table,CONCAT(session_db,'.',v_source_table)),''''',''''',@polygon_str,''''',''''',@imsi_group_id,''''',''''',CELL_GID,''''',''''',gt_strtok(session_db,2,'_'),''''',''''',PLOYGON_ID,''''',''''',LIMT_RAW_COUNT,''''');'') 
		, ''tmp_materialization_',PID,'''
		, CONCAT(''HOST ''''',DS_AP_IP,''''', PORT ''''',DS_AP_PORT,''''',USER ''''',DS_AP_USER,''''', PASSWORD ''''',DS_AP_PASSWORD,''''''')
		) INTO @bb ;');			
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
	ELSE 
		SET @SqlCmd=CONCAT('SELECT spider_bg_direct_sql(''',REPLACE(REPLACE(IF(LIMT_RAW_COUNT>0,CONCAT(sql_str,' LIMIT ',LIMT_RAW_COUNT),sql_str),"'","''"),v_source_table,CONCAT(session_db,'.',v_source_table)),''' 
		, ''tmp_materialization_',PID,'''
		, CONCAT(''HOST ''''',DS_AP_IP,''''', PORT ''''',DS_AP_PORT,''''',USER ''''',DS_AP_USER,''''', PASSWORD ''''',DS_AP_PASSWORD,''''''')
		) INTO @bb ;');				
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
	END IF;
	
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
