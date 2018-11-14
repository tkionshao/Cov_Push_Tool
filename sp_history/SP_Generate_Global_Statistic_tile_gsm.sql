DELIMITER $$
USE `gt_gw_main`$$
DROP PROCEDURE IF EXISTS `SP_CreateDB_LTE`$$
CREATE DEFINER=`covmo`@`%` PROCEDURE `SP_Generate_Global_Statistic_tile_gsm`(IN DATA_DATE DATE,IN DATA_HOUR TINYINT(2),IN PU_ID MEDIUMINT(9),IN TECH_MASK TINYINT(2), IN GT_DB VARCHAR(100))
BEGIN
	DECLARE SP_Process VARCHAR(100) DEFAULT NULL;
	DECLARE WORKER_ID VARCHAR(10) DEFAULT CONNECTION_ID();
	DECLARE Spider_SP_ERROR CONDITION FOR SQLSTATE '99998';
	DECLARE KPI_ERROR CONDITION FOR SQLSTATE '99999' ;
	DECLARE ZOOM_LEVEL INT;
	
  	CALL gt_gw_main.`SP_Sub_Set_Session_Param_LTE`('');
  	
	INSERT INTO gt_gw_main.sp_log VALUES('gt_gw_main','SP_Generate_Global_Statistic_tile_gsm',CONCAT(GT_DB,' CREATE TEMPORARY TABLE tmp_materialization_',WORKER_ID), NOW());	
	
	SET @SqlCmd=CONCAT('SELECT `att_value` INTO @ZOOM_LEVEL FROM gt_covmo.`sys_config` WHERE `group_name`=''system'' AND att_name = ''MapResolution'';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET SESSION group_concat_max_len=102400; 
	
	SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS ',GT_DB,'.tmp_materialization_',WORKER_ID,';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('CREATE TEMPORARY TABLE ',GT_DB,'.tmp_materialization_',WORKER_ID,' (
				  `TILE_ID` BIGINT(20) NOT NULL,
				  `FREQUENCY` SMALLINT(6) NOT NULL,
				  `DATA_DATE` DATETIME NOT NULL,
				  `DATA_HOUR` TINYINT(2) NOT NULL,
				  `TILE_ID_16` BIGINT(20) DEFAULT NULL,
				  `TILE_ID_13` BIGINT(20) DEFAULT NULL,
				  INIT_CALL_CNT INT(11) DEFAULT NULL, 
				  END_CALL_CNT INT(11) DEFAULT NULL,
				  VOICE_CNT INT(11) DEFAULT NULL,
				  SIGNAL_CNT INT(11) DEFAULT NULL,
				  SMS_CNT INT(11) DEFAULT NULL,
				  GPRS_CNT INT(11) DEFAULT NULL,
				  OTHER_CNT INT(11) DEFAULT NULL,
				  BLOCK_CNT INT(11) DEFAULT NULL,
				  DROP_VOICE_CNT INT(11) DEFAULT NULL,
				  DROP_SIGNAL_CNT INT(11) DEFAULT NULL,
				  DROP_SMS_CNT INT(11) DEFAULT NULL,
				  DROP_GPRS_CNT INT(11) DEFAULT NULL,
				  DROP_OTHER_CNT INT(11) DEFAULT NULL,
				  NON_BLOCK_VOICE_CNT INT(11) DEFAULT NULL,
				  NON_BLOCK_SIGNAL_CNT INT(11) DEFAULT NULL,
				  NON_BLOCK_SMS_CNT INT(11) DEFAULT NULL,
				  NON_BLOCK_GPRS_CNT INT(11) DEFAULT NULL,
				  NON_BLOCK_OTHER_CNT INT(11) DEFAULT NULL,
				  CALL_DUR_SUM DOUBLE DEFAULT NULL,
				  RXLEV_SUM DOUBLE DEFAULT NULL,
				  RXLEV_CNT INT(11) DEFAULT NULL,
				  RXQUAL_SUM DOUBLE DEFAULT NULL,
				  RXQUAL_CNT INT(11) DEFAULT NULL,
				  `REG_1_ID` BIGINT(20) DEFAULT NULL,
				  `REG_2_ID` BIGINT(20) DEFAULT NULL,
				  `REG_3_ID` BIGINT(20) DEFAULT NULL,
				  PRIMARY KEY (`TILE_ID`,FREQUENCY,`DATA_DATE`,`DATA_HOUR`)
				) ENGINE=MYISAM DEFAULT CHARSET=latin1;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	INSERT INTO gt_gw_main.sp_log VALUES('gt_gw_main','SP_Generate_Global_Statistic_tile_gsm',CONCAT(GT_DB,' INSERT INTO tbl_',GT_DB,'_',WORKER_ID), NOW());	
	 
	
	SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS ',GT_DB,'.tmp_tile_gsm_start_',WORKER_ID,';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS ',GT_DB,'.tmp_tile_gsm_end_',WORKER_ID,';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('CREATE TEMPORARY TABLE ',GT_DB,'.tmp_tile_gsm_start_',WORKER_ID,' ENGINE=MYISAM AS
				SELECT
					gt_covmo_proj_geohash_to_hex_geohash(POS_FIRST_LOC,',@ZOOM_LEVEL,') AS TILE_ID
 					,gt_covmo_proj_geohash_to_hex_geohash(POS_FIRST_LOC,',@ZOOM_LEVEL-3,') AS TILE_ID_16
 					,gt_covmo_proj_geohash_to_hex_geohash(POS_FIRST_LOC,',@ZOOM_LEVEL-6,') AS TILE_ID_13
 					,POS_FIRST_BCCH_ARFCN AS FREQUENCY
					,DATA_DATE
					,DATA_HOUR
					,COUNT(*) AS INIT_CALL_CNT
					,SUM(IF(CALL_TYPE=10,1,0)) AS VOICE_CNT
					,SUM(IF(CALL_TYPE=15,1,0)) AS SIGNAL_CNT
					,SUM(IF(CALL_TYPE=16,1,0)) AS SMS_CNT
					,SUM(IF(CALL_TYPE=20,1,0)) AS GPRS_CNT
					,SUM(IF(CALL_TYPE NOT IN (10,15,16,20),1,0)) AS OTHER_CNT
					,SUM(IF(CALL_TYPE IN (10,11),DURATION,0)) AS CALL_DUR_SUM
					,SUM(IF(CALL_STATUS=3,1,0)) AS BLOCK_CNT
					,SUM(POS_FIRST_RXLEV_FULL_DOWNLINK) AS RXLEV_SUM
					,COUNT(POS_FIRST_RXLEV_FULL_DOWNLINK) AS RXLEV_CNT
					,SUM(POS_FIRST_RXQUAL_FULL_DOWNLINK) AS RXQUAL_SUM
					,COUNT(POS_FIRST_RXQUAL_FULL_DOWNLINK) AS RXQUAL_CNT
				FROM ',GT_DB,'.table_call_gsm
				WHERE DATA_HOUR =',DATA_HOUR,' AND POS_FIRST_LOC IS NOT NULL AND POS_FIRST_BSC=',PU_ID,' 
				GROUP BY gt_covmo_proj_geohash_to_hex_geohash(POS_FIRST_LOC,',@ZOOM_LEVEL,'),`POS_FIRST_BCCH_ARFCN`
				ORDER BY NULL;
				');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('CREATE TEMPORARY TABLE ',GT_DB,'.tmp_tile_gsm_end_',WORKER_ID,' ENGINE=MYISAM AS
				SELECT
					gt_covmo_proj_geohash_to_hex_geohash(POS_LAST_LOC,',@ZOOM_LEVEL,') AS TILE_ID
 					,gt_covmo_proj_geohash_to_hex_geohash(POS_LAST_LOC,',@ZOOM_LEVEL-3,') AS TILE_ID_16
 					,gt_covmo_proj_geohash_to_hex_geohash(POS_LAST_LOC,',@ZOOM_LEVEL-6,') AS TILE_ID_13
 					,POS_LAST_BCCH_ARFCN AS FREQUENCY
					,DATA_DATE
					,DATA_HOUR
					,COUNT(*) AS END_CALL_CNT
					,SUM(IF(CALL_STATUS=2 AND CALL_TYPE=10,1,0)) AS DROP_VOICE_CNT
					,SUM(IF(CALL_STATUS=2 AND CALL_TYPE=15,1,0)) AS DROP_SIGNAL_CNT
					,SUM(IF(CALL_STATUS=2 AND CALL_TYPE=16,1,0)) AS DROP_SMS_CNT
					,SUM(IF(CALL_STATUS=2 AND CALL_TYPE=20,1,0)) AS DROP_GPRS_CNT
					,SUM(IF(CALL_STATUS=2 AND CALL_TYPE NOT IN (10,15,16,20),1,0)) AS DROP_OTHER_CNT
					,SUM(IF(CALL_STATUS IN (1,2,4,5) AND CALL_TYPE=10,1,0)) AS `NON_BLOCK_VOICE_CNT`
					,SUM(IF(CALL_STATUS IN (1,2,4,5) AND CALL_TYPE=15,1,0)) AS `NON_BLOCK_SIGNAL_CNT`
					,SUM(IF(CALL_STATUS IN (1,2,4,5) AND CALL_TYPE=16,1,0)) AS `NON_BLOCK_SMS_CNT`
					,SUM(IF(CALL_STATUS IN (1,2,4,5) AND CALL_TYPE=20,1,0)) AS `NON_BLOCK_GPRS_CNT`
					,SUM(IF(CALL_STATUS IN (1,2,4,5) AND CALL_TYPE NOT IN (10,15,16,20),1,0)) AS `NON_BLOCK_OTHER_CNT`
				FROM ',GT_DB,'.table_call_gsm
				WHERE DATA_HOUR =',DATA_HOUR,' AND POS_LAST_LOC IS NOT NULL AND POS_LAST_BSC=',PU_ID,' 
 				GROUP BY gt_covmo_proj_geohash_to_hex_geohash(POS_LAST_LOC,',@ZOOM_LEVEL,'),`POS_LAST_BCCH_ARFCN`
				ORDER BY NULL;
				');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('CREATE INDEX `ix_tile_gsm_fq` ON ',GT_DB,'.tmp_tile_gsm_start_',WORKER_ID,' (TILE_ID,FREQUENCY);');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('CREATE INDEX `ix_tile_gsm_fq` ON ',GT_DB,'.tmp_tile_gsm_end_',WORKER_ID,' (TILE_ID,FREQUENCY);');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;	
	
	SET @SqlCmd=CONCAT('INSERT INTO ',GT_DB,'.`tmp_materialization_',WORKER_ID,'` 
				( TILE_ID,FREQUENCY,DATA_DATE,DATA_HOUR)
			SELECT TILE_ID,FREQUENCY,DATA_DATE,DATA_HOUR
			FROM 
			(
				SELECT TILE_ID,FREQUENCY,DATA_DATE,DATA_HOUR FROM ',GT_DB,'.tmp_tile_gsm_start_',WORKER_ID,'
 				UNION 
 				SELECT TILE_ID,FREQUENCY,DATA_DATE,DATA_HOUR FROM ',GT_DB,'.tmp_tile_gsm_end_',WORKER_ID,'
			) AA;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('UPDATE ',GT_DB,'.`tmp_materialization_',WORKER_ID,'` a,',GT_DB,'.`tmp_tile_gsm_start_',WORKER_ID,'` B
				SET 
				A.TILE_ID_16=B.TILE_ID_16
				,A.TILE_ID_13=B.TILE_ID_13
				,A.INIT_CALL_CNT=B.INIT_CALL_CNT
				,A.VOICE_CNT=B.VOICE_CNT
				,A.SIGNAL_CNT=B.SIGNAL_CNT
				,A.SMS_CNT=B.SMS_CNT
				,A.GPRS_CNT=B.GPRS_CNT
				,A.OTHER_CNT=B.OTHER_CNT
				,A.CALL_DUR_SUM=B.CALL_DUR_SUM
				,A.BLOCK_CNT=B.BLOCK_CNT
				,A.RXLEV_SUM=B.RXLEV_SUM
				,A.RXLEV_CNT=B.RXLEV_CNT
				,A.RXQUAL_SUM=B.RXQUAL_SUM
				,A.RXQUAL_CNT=B.RXQUAL_CNT
			WHERE A.TILE_ID=B.TILE_ID AND A.FREQUENCY=B.FREQUENCY;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('UPDATE ',GT_DB,'.`tmp_materialization_',WORKER_ID,'` a,',GT_DB,'.`tmp_tile_gsm_end_',WORKER_ID,'` B
				SET
				A.TILE_ID_16=CASE WHEN A.TILE_ID_16 IS NULL THEN B.TILE_ID_16 ELSE A.TILE_ID_16 END
				,A.TILE_ID_13=CASE WHEN A.TILE_ID_13 IS NULL THEN B.TILE_ID_13 ELSE A.TILE_ID_13 END
				,A.END_CALL_CNT=B.END_CALL_CNT
				,A.DROP_VOICE_CNT=B.DROP_VOICE_CNT
				,A.DROP_SIGNAL_CNT=B.DROP_SIGNAL_CNT
				,A.DROP_SMS_CNT=B.DROP_SMS_CNT
				,A.DROP_GPRS_CNT=B.DROP_GPRS_CNT
				,A.DROP_OTHER_CNT=B.DROP_OTHER_CNT
				,A.NON_BLOCK_VOICE_CNT=B.NON_BLOCK_VOICE_CNT
				,A.NON_BLOCK_SIGNAL_CNT=B.NON_BLOCK_SIGNAL_CNT
				,A.NON_BLOCK_SMS_CNT=B.NON_BLOCK_SMS_CNT
				,A.NON_BLOCK_GPRS_CNT=B.NON_BLOCK_GPRS_CNT
				,A.NON_BLOCK_OTHER_CNT=B.NON_BLOCK_OTHER_CNT
			WHERE A.TILE_ID=B.TILE_ID AND A.FREQUENCY=B.FREQUENCY;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('SELECT * FROM ',GT_DB,'.tmp_materialization_',WORKER_ID,' WHERE TILE_ID >0;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS ',GT_DB,'.tmp_materialization_',WORKER_ID,';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS ',GT_DB,'.tmp_tile_gsm_start_',WORKER_ID,';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS ',GT_DB,'.tmp_tile_gsm_end_',WORKER_ID,';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	INSERT INTO gt_gw_main.sp_log VALUES('gt_gw_main','SP_Generate_Global_Statistic_tile_gsm',CONCAT(GT_DB,' END'), NOW());
	
END$$
DELIMITER ;
