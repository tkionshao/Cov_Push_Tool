DELIMITER $$
USE `gt_global_statistic`$$
DROP PROCEDURE IF EXISTS `SP_CreateDB_LTE`$$
CREATE DEFINER=`covmo`@`%` PROCEDURE `SP_Generate_Global_Statistic_failcause_lte`(IN DATA_DATE DATE,IN DATA_HOUR TINYINT(2),IN PU_ID MEDIUMINT(9),IN TECH_MASK TINYINT(2), IN GT_DB VARCHAR(100))
BEGIN
	DECLARE SP_Process VARCHAR(100) DEFAULT NULL;
	DECLARE WORKER_ID VARCHAR(10) DEFAULT CONNECTION_ID();
	
  	CALL gt_gw_main.`SP_Sub_Set_Session_Param_LTE`('');
  	
	INSERT INTO gt_gw_main.sp_log VALUES('gt_gw_main','SP_Generate_Global_Statistic_failcause_lte',CONCAT(GT_DB,' CREATE TEMPORARY TABLE tmp_materialization_',WORKER_ID), NOW());	
		
	SET SESSION group_concat_max_len=102400; 
	
	SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS ',GT_DB,'.tmp_materialization_',WORKER_ID,';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('CREATE TEMPORARY TABLE ',GT_DB,'.tmp_materialization_',WORKER_ID,' (
				  `DATA_DATE` DATE NOT NULL,
				  `TILE_ID` BIGINT(20) NOT NULL,
				  `PU_ID` MEDIUMINT(9) NOT NULL,
				  `ENODEB_ID` mediumint(9) NOT NULL,
				  `CELL_ID` MEDIUMINT(9) NOT NULL,
				  `IMSI` VARCHAR(20) NOT NULL,
				  `IMEI` VARCHAR(20) NOT NULL,
				  `EVENT_ID` SMALLINT(6) NOT NULL,
				  `FAILURE_EVENT_ID` MEDIUMINT(9) NOT NULL,
				  `FAILURE_EVENT_CAUSE` MEDIUMINT(9) NOT NULL,
				  `FAILURE_CNT` MEDIUMINT(9) NOT NULL
				) ENGINE=MYISAM DEFAULT CHARSET=latin1;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	INSERT INTO gt_gw_main.sp_log VALUES('gt_gw_main','SP_Generate_Global_Statistic_failcause_lte',CONCAT(GT_DB,' INSERT INTO tbl_',GT_DB,'_',WORKER_ID), NOW());	
		
	SET @SqlCmd=CONCAT('INSERT INTO ',GT_DB,'.`tmp_materialization_',WORKER_ID,'` 
				SELECT
				  `DATA_DATE`,
				  `TILE_ID`,
				  ',PU_ID,' AS `PU_ID`,
				  `ENODEB_ID`,
				  `CELL_ID`,
				  `IMSI`,
				  `IMEI`,
				  `EVENT_ID`,
				  `FAILURE_EVENT_ID`,
				  `FAILURE_EVENT_CAUSE`,
				  1 AS `FAILURE_CNT`
				FROM ',GT_DB,'.`rpt_cell_tile_imsi_failure`
				WHERE DATA_HOUR =',DATA_HOUR,';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('SELECT * FROM ',GT_DB,'.tmp_materialization_',WORKER_ID,';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS ',GT_DB,'.tmp_materialization_',WORKER_ID,';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	INSERT INTO gt_gw_main.sp_log VALUES('gt_gw_main','SP_Generate_Global_Statistic_failcause_lte',CONCAT(GT_DB,' END'), NOW());
	
END$$
DELIMITER ;
