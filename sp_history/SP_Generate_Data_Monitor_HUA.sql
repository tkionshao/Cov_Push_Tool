DELIMITER $$
USE `operations_monitor`$$
DROP PROCEDURE IF EXISTS `SP_CreateDB_LTE`$$
CREATE DEFINER=`covmo`@`%` PROCEDURE `SP_Generate_Data_Monitor_HUA`( IN tbl_name VARCHAR(100),IN GW VARCHAR(100),IN DATA_TIME VARCHAR(100) )
BEGIN
		DECLARE SUB_DATE DATETIME;
		DECLARE WORKER_ID VARCHAR(10) DEFAULT CONNECTION_ID();	
		SET SUB_DATE = DATE_SUB(DATA_TIME,INTERVAL 1 DAY);
	
		SET @NT_DATE = DATE_FORMAT(DATA_TIME, '%Y%m%d');
		SET @NT_DB =CONCAT('gt_nt_',@NT_DATE);
		
		SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS operations_monitor.tmp_last_hour_hua_trace_',WORKER_ID,';');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
	
		SET @SqlCmd=CONCAT('CREATE TEMPORARY TABLE operations_monitor.tmp_last_hour_hua_trace_',WORKER_ID,' ENGINE=MYISAM AS  
					SELECT
					  `DATA_TIME`,
					  `ENODEB_ID`,
					  `CREATE_TIME`,
					  `ENODEB_NAME`,
					  `ENODEB_VENDOR`,
					  `ENODEB_IP`,
					  `CDGS`,
					  `CLUSTER_NAME_REGION`,
					  `CLUSTER_NAME_SUB_REGION`,
					  `SUB_REGION_ID`,
					  `REGION_ID`,
					  `PU_ID`,
			                  `PU_NAME`,
					  `HUA_FILESIZE`,
					  `HUA_FILECOUNT`,
					  `HUA_LAST_UPDATETIME`,
					  `LAST_TRACE_EVENT_TIME`,
					  `ACT_STATE`,
					  `FIRST_FLAG`,
					  `FIRST_UPDATE_TIME`,
					  `STOP`,
					  `STOP_TIME`,
					  `STOP_TO_ALIVE`
					FROM `operations_monitor`.`enodeb_history_detail`
					WHERE DATA_TIME  BETWEEN  DATE_SUB(''',DATA_TIME,''',INTERVAL 1.5 HOUR) AND  ''',DATA_TIME,'''  and ENODEB_VENDOR = ''huawei''   and CDGS  =  ''',GW,''' 
	
		;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
	
		SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS operations_monitor.tmp_hua_trace_',WORKER_ID,';');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
	
		SET @SqlCmd=CONCAT('CREATE TEMPORARY TABLE operations_monitor.tmp_hua_trace_',WORKER_ID,' ENGINE=MYISAM AS 
				   SELECT 
					      
					      a.DATA_TIME,
					      a.ENODEB_ID,
					      ''',DATA_TIME,''' AS CREATE_TIME,
					      ''Huawei'' AS ENODEB_VENDOR,
					      ''',GW,''' AS CDGS,
					      b.PU_ID,
			                      b.PU_NAME,
					      a.`HUA_FILESIZE`,
					      a.`HUA_FILECOUNT`,
					      a.`HUA_LAST_UPDATETIME`,
					      a.`HUA_LAST_UPDATETIME` AS LAST_TRACE_EVENT_TIME,
					      CASE WHEN b.FIRST_FLAG = 0 OR b.FIRST_FLAG IS NULL THEN 1 ELSE b.FIRST_FLAG END AS FIRST_FLAG,
					      CASE WHEN b.FIRST_FLAG = 0 OR b.FIRST_FLAG IS NULL THEN ''',DATA_TIME,'''
					      ELSE b.FIRST_UPDATE_TIME END AS FIRST_UPDATE_TIME,
					      CASE WHEN IFNULL(a.HUA_FILECOUNT,0) = IFNULL(b.HUA_FILECOUNT,0)  THEN 1 ELSE 0 END AS STOP,
					      CASE WHEN IFNULL(a.HUA_FILECOUNT,0) = IFNULL(b.HUA_FILECOUNT,0)  AND FIRST_FLAG = 1  AND b.STOP_TIME IS NULL THEN ''',DATA_TIME,'''  
					      ELSE b.STOP_TIME
					      END AS STOP_TIME,
					      CASE WHEN b.STOP = 1 AND IFNULL(a.HUA_FILECOUNT,0) > IFNULL(b.HUA_FILECOUNT,0) THEN 1 ELSE 0 END AS STOP_TO_ALIVE
					      FROM operations_monitor.',tbl_name,' a
					      LEFT JOIN
					      operations_monitor.tmp_last_hour_hua_trace_',WORKER_ID,' b
						ON a.ENODEB_ID = b.ENODEB_ID 
		;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		
		
		SET @SqlCmd=CONCAT('UPDATE  operations_monitor.tmp_hua_trace_',WORKER_ID,'
		SET ENODEB_ID = RIGHT(ENODEB_ID,6);');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		
		SET @SqlCmd=CONCAT('ALTER IGNORE TABLE operations_monitor.tmp_hua_trace_',WORKER_ID,' ADD UNIQUE(ENODEB_ID);');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		
	
	
		SET @SqlCmd=CONCAT('DROP TABLE IF EXISTS operations_monitor.nt_cell_lte_tmp_',WORKER_ID,' ;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
	
	
		
	
	
	
		SET @SqlCmd=CONCAT('INSERT ignore  INTO operations_monitor.enodeb_history_detail(
					      `DATA_TIME`,
					      `ENODEB_ID`,
					       CREATE_TIME,
					      `ENODEB_NAME`,
					      `ENODEB_VENDOR`,
					      `ENODEB_IP`,
					      `CDGS`,
					      `CLUSTER_NAME_REGION`,
					      `CLUSTER_NAME_SUB_REGION`,
					      `SUB_REGION_ID`,
					      `REGION_ID`,
					      `PU_ID`,
					      `PU_NAME`,
					      `HUA_FILESIZE`,
					      `HUA_FILECOUNT`,
					      `HUA_LAST_UPDATETIME`,
					      `LAST_TRACE_EVENT_TIME`,
					      `ACT_STATE`,
					      `FIRST_FLAG`,
					      `FIRST_UPDATE_TIME`,
					      `STOP`,
					      `STOP_TIME`,
					      `STOP_TO_ALIVE`) 
				SELECT 
					      
					      a.`DATA_TIME`,
					      a.`ENODEB_ID`,
					      a. CREATE_TIME,
					      b.`ENODEB_NAME`,
					      b.`ENODEB_VENDOR`,
					      b.IP AS `ENODEB_IP`,
					      a.`CDGS`,
					      b.`CLUSTER_NAME_REGION`,
					      b.`CLUSTER_NAME_SUB_REGION`,
					      b.`SUB_REGION_ID`,
					      b.`REGION_ID`,
					      b.`PU_ID`,
					      c.rnc_name as PU_NAME,
					      `HUA_FILESIZE`,
					      `HUA_FILECOUNT`,
					      `HUA_LAST_UPDATETIME`,
					      `LAST_TRACE_EVENT_TIME`,
					      b.`ACT_STATE`,
					      `FIRST_FLAG`,
					      `FIRST_UPDATE_TIME`,
					      `STOP`,
					      `STOP_TIME`,
					      `STOP_TO_ALIVE`
					       FROM operations_monitor.tmp_hua_trace_',WORKER_ID,' a
					       LEFT JOIN
					       ',@NT_DB,'.nt_cell_current_lte b
					       ON  a.ENODEB_ID = b.ENODEB_ID 
					       LEFT JOIN gt_gw_main.rnc_information c
					       on b.PU_ID = c.RNC ;');	
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
	
		
	
	
		SET @SqlCmd=CONCAT('INSERT INTO `operations_monitor`.`huawei_raw_summary`
					    (`DATA_TIME`,
					     `ENODEB_ID`,
					     `ENODEB_VENDOR`,
					     `GW`,
					     `PUID`,
					     `HUA_TRACE_ID`,
					     `HUA_FILESIZE`,
					     `HUA_FILECOUNT`,
					     `HUA_LAST_UPDATETIME`)
			SELECT
					  `DATA_TIME`,
					  `ENODEB_ID`,
					  `ENODEB_VENDOR`,
					  ''',GW,''' ,
					  `PUID`,
					  `HUA_TRACE_ID`,
					  `HUA_FILESIZE`,
					  `HUA_FILECOUNT`,
					  `HUA_LAST_UPDATETIME`
					FROM operations_monitor.',tbl_name,'
					;');
					
					PREPARE Stmt FROM @SqlCmd;
					EXECUTE Stmt;
					DEALLOCATE PREPARE Stmt;
	
	
	
		SET @SqlCmd=CONCAT('DROP TABLE IF EXISTS operations_monitor.tmp_hua_trace_',WORKER_ID,';');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
	
		SET @SqlCmd=CONCAT('DROP TABLE IF EXISTS operations_monitor.tmp_last_hour_hua_trace_',WORKER_ID,';');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		
	
	
	
	
	
END$$
DELIMITER ;
