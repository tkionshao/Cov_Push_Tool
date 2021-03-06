DELIMITER $$
USE `gt_global_statistic`$$
DROP PROCEDURE IF EXISTS `SP_CreateDB_LTE`$$
CREATE DEFINER=`covmo`@`%` PROCEDURE `SP_Generate_Global_Statistic_subscriber_lte`(IN DATA_DATE DATE,IN DATA_HOUR TINYINT(2),IN PU_ID MEDIUMINT(9),IN TECH_MASK TINYINT(2), IN GT_DB VARCHAR(100),IN IMSI_CELL TINYINT(2))
BEGIN
	DECLARE SP_Process VARCHAR(100) DEFAULT NULL;
	DECLARE WORKER_ID VARCHAR(10) DEFAULT CONNECTION_ID();
	DECLARE Spider_SP_ERROR CONDITION FOR SQLSTATE '99998';
	DECLARE KPI_ERROR CONDITION FOR SQLSTATE '99999' ;
	DECLARE ZOOM_LEVEL INT;
	
	DECLARE STR_START_END_LTE MEDIUMTEXT DEFAULT '';
	DECLARE STR_SUM_LTE MEDIUMTEXT DEFAULT '';
	DECLARE STR_SEL_LTE MEDIUMTEXT DEFAULT '';
	
	SET @FLAG_IMSI_HR=0;
	SET @FLAG_IMSI_DY=0;
	SET @FLAG_MAKE_HR=0;
	SET @FLAG_MAKE_DY=0;
	
	SET STR_START_END_LTE=CONCAT('`DATA_DATE`,
					`END_TIME`,
					`DURATION`,
					`IMSI`,
					`POS_LAST_S_CELL`,
					`POS_LAST_S_ENODEB`,
					`UE_CONTEXT_RELEASE_CAUSE`,
					`POS_LAST_RSRP`,
					`POS_LAST_RSRQ`,
					`POS_LAST_S_EUTRABAND`,
 					`POS_LAST_TILE`');
	
	SET STR_SUM_LTE=CONCAT('`DATA_DATE`,
					`END_TIME`,
					`DURATION`,
					`IMSI`,
					CONCAT(`POS_LAST_S_CELL`,''-'',`POS_LAST_S_ENODEB`) AS CELL_ID,
					UE_CONTEXT_RELEASE_CAUSE,
					IFNULL(POS_LAST_RSRP,0),
					IFNULL(POS_LAST_RSRQ,0),
					POS_LAST_S_EUTRABAND,
 					POS_LAST_TILE AS TILE_ID ');
	
	INSERT INTO gt_gw_main.sp_log VALUES('gt_gw_main','SP_Generate_Global_Statistic_subscriber_lte',CONCAT(GT_DB,' CREATE TEMPORARY TABLE tmp_materialization_imsi_umts_',WORKER_ID), NOW());	
	
	SET SESSION group_concat_max_len=102400; 
	
	SET @SqlCmd=CONCAT('SELECT `att_value` INTO @ZOOM_LEVEL FROM gt_covmo.`sys_config` WHERE `group_name`=''system'' AND att_name = ''MapResolution'';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS ',GT_DB,'.tmp_materialization_subscriber_lte_',WORKER_ID,';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('CREATE TEMPORARY TABLE ',GT_DB,'.tmp_materialization_subscriber_lte_',WORKER_ID,' 
				(
							`DATA_DATE` date DEFAULT NULL,
							 `END_TIME` DATETIME DEFAULT NULL,							
							 `DURATION` MEDIUMINT(9) DEFAULT NULL,
							 `IMSI` varchar(20) DEFAULT NULL,							 
							 `CELL_ID` VARCHAR(50) DEFAULT NULL,
							 `DROP_REASON` MEDIUMINT(9) DEFAULT NULL,
							 `LAST_RSRP` DOUBLE DEFAULT NULL,
							 `LAST_RSRQ` DOUBLE DEFAULT NULL,
							 `FREQ_BAND` SMALLINT(6) DEFAULT NULL,
							 `TILE_ID` BIGINT(20) DEFAULT NULL	
					) ENGINE=MYISAM DEFAULT CHARSET=latin1 DELAY_KEY_WRITE=1;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	INSERT INTO gt_gw_main.sp_log VALUES('gt_gw_main','SP_Generate_Global_Statistic_subscriber_lte',CONCAT(GT_DB,' INSERT INTO tbl_',GT_DB,'_',WORKER_ID), NOW());	
	 	
	SET @v_i=DATA_HOUR;
	SET @v_i_Max=DATA_HOUR+1;
	SET @v_j=0;
				
	WHILE @v_i < @v_i_Max DO
	BEGIN	
		WHILE @v_j<60  DO
		BEGIN		
			IF (@v_j=45) THEN 
				SET @UNION=' '; 
			ELSE
				SET @UNION=' UNION ALL '; 
			END IF;	
			SET STR_SEL_LTE=CONCAT(STR_SEL_LTE,'SELECT ',STR_START_END_LTE,' FROM ',GT_DB,'.table_call_lte',CONCAT('_',LPAD(@v_i,2,'0'),LPAD(@v_j,2,'0')),' WHERE DATA_HOUR=', @v_i,' AND IMSI IS NOT NULL AND IMSI<>''0'' AND  CALL_TYPE IN (10,11,23) AND CALL_STATUS = 2',@UNION); 
			SET @v_j=@v_j+15;
		END;
		END WHILE;
			
		SET @v_j=0;
		SET @v_i=@v_i+1;
	END;
	END WHILE;	
	
	SET @SqlCmd=CONCAT('INSERT INTO ',GT_DB,'.tmp_materialization_subscriber_lte_',WORKER_ID,' 
				(	`DATA_DATE`,
					`END_TIME`,
					`DURATION`,
					`IMSI`,
					`CELL_ID`,
					`DROP_REASON`,
					`LAST_RSRP`,
					`LAST_RSRQ`,
					`FREQ_BAND`,
					`TILE_ID`
					)
				SELECT ',STR_SUM_LTE,' 
				FROM (',STR_SEL_LTE,') AA
					ORDER BY NULL;
				');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('SELECT * FROM ',GT_DB,'.tmp_materialization_subscriber_lte_',WORKER_ID,';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS ',GT_DB,'.tmp_materialization_subscriber_lte_',WORKER_ID,';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	INSERT INTO gt_gw_main.sp_log VALUES('gt_gw_main','SP_Generate_Global_Statistic_subscriber_lte',CONCAT(GT_DB,' END'), NOW());
	
END$$
DELIMITER ;
