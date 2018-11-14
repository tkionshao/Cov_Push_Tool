DELIMITER $$
USE `gt_global_statistic`$$
DROP PROCEDURE IF EXISTS `SP_CreateDB_LTE`$$
CREATE DEFINER=`covmo`@`%` PROCEDURE `SP_Generate_Global_Statistic_subscriber_umts`(IN DATA_DATE DATE,IN DATA_HOUR TINYINT(2),IN PU_ID MEDIUMINT(9),IN TECH_MASK TINYINT(2), IN GT_DB VARCHAR(100),IN IMSI_CELL TINYINT(2))
BEGIN
	DECLARE SP_Process VARCHAR(100) DEFAULT NULL;
	DECLARE WORKER_ID VARCHAR(10) DEFAULT CONNECTION_ID();
	DECLARE Spider_SP_ERROR CONDITION FOR SQLSTATE '99998';
	DECLARE KPI_ERROR CONDITION FOR SQLSTATE '99999' ;
	DECLARE ZOOM_LEVEL INT;
	SET @FLAG_IMSI_HR=0;
	SET @FLAG_IMSI_DY=0;
	SET @FLAG_MAKE_HR=0;
	SET @FLAG_MAKE_DY=0;
	INSERT INTO gt_gw_main.sp_log VALUES('gt_gw_main','SP_Generate_Global_Statistic_subscriber_umts',CONCAT(GT_DB,' CREATE TEMPORARY TABLE tmp_materialization_imsi_umts_',WORKER_ID), NOW());	
	
	SET SESSION group_concat_max_len=102400; 
	
	SET @SqlCmd=CONCAT('SELECT `att_value` INTO @ZOOM_LEVEL FROM gt_covmo.`sys_config` WHERE `group_name`=''system'' AND att_name = ''MapResolution'';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS ',GT_DB,'.tmp_materialization_subscriber_umts_',WORKER_ID,';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('CREATE TEMPORARY TABLE ',GT_DB,'.tmp_materialization_subscriber_umts_',WORKER_ID,' 
				(
							`DATA_DATE` date DEFAULT NULL,
							 `END_TIME` DATETIME DEFAULT NULL,							
							 `DURATION` MEDIUMINT(9) DEFAULT NULL,
							 `IMSI` varchar(20) DEFAULT NULL,
							 
							 `CELL_ID` VARCHAR(50) DEFAULT NULL,
							 `DROP_REASON` MEDIUMINT(9) DEFAULT NULL,
							 `LAST_RSCP` DOUBLE DEFAULT NULL,
							 `LAST_ECN0` DOUBLE DEFAULT NULL,
							 `FREQ_BAND` SMALLINT(6) DEFAULT NULL,
							 `TILE_ID` BIGINT(20) DEFAULT NULL	
					) ENGINE=MYISAM DEFAULT CHARSET=latin1 DELAY_KEY_WRITE=1;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	INSERT INTO gt_gw_main.sp_log VALUES('gt_gw_main','SP_Generate_Global_Statistic_subscriber_umts',CONCAT(GT_DB,' INSERT INTO tbl_',GT_DB,'_',WORKER_ID), NOW());	
	 	
	SET @SqlCmd=CONCAT('INSERT INTO ',GT_DB,'.tmp_materialization_subscriber_umts_',WORKER_ID,' 
				(	`DATA_DATE`,
					`END_TIME`,
					`DURATION`,
					`IMSI`,
					`CELL_ID`,
					`DROP_REASON`,
					`LAST_RSCP`,
					`LAST_ECN0`,
					`FREQ_BAND`,
					`TILE_ID`
					)
				SELECT
					`DATA_DATE`,
					`END_TIME`,
					`CS_CALL_DURA`,
					`IMSI`,
					CONCAT(`POS_LAST_CELL`,''-'',`POS_LAST_RNC`) AS END_CELL,
					IU_RELEASE_CAUSE,
					IFNULL(POS_LAST_RSCP,0),
					IFNULL(POS_LAST_ECN0,0),
					POS_LAST_FREQUENCY,
 					gt_covmo_proj_geohash_to_hex_geohash(POS_LAST_LOC,',@ZOOM_LEVEL,') AS TILE_ID 							
					FROM ',GT_DB,'.table_call
					WHERE DATA_HOUR =',DATA_HOUR,' AND IMSI IS NOT NULL AND IMSI<>''0'' AND  CALL_TYPE IN (10,11,23) AND CALL_STATUS = 2
				
					ORDER BY NULL;
				');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('SELECT * FROM ',GT_DB,'.tmp_materialization_subscriber_umts_',WORKER_ID,';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS ',GT_DB,'.tmp_materialization_subscriber_umts_',WORKER_ID,';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	INSERT INTO gt_gw_main.sp_log VALUES('gt_gw_main','SP_Generate_Global_Statistic_imsi_umts',CONCAT(GT_DB,' END'), NOW());
	
END$$
DELIMITER ;
