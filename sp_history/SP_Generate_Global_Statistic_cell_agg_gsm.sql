DELIMITER $$
USE `gt_global_statistic`$$
DROP PROCEDURE IF EXISTS `SP_CreateDB_LTE`$$
CREATE DEFINER=`covmo`@`%` PROCEDURE `SP_Generate_Global_Statistic_cell_agg_gsm`(IN DATA_DATE DATE,IN DATA_HOUR TINYINT(2),IN PU_ID MEDIUMINT(9),IN TECH_MASK TINYINT(2), IN GT_DB VARCHAR(100))
BEGIN
	DECLARE SP_Process VARCHAR(100) DEFAULT NULL;
	DECLARE WORKER_ID VARCHAR(10) DEFAULT CONNECTION_ID();
	DECLARE Spider_SP_ERROR CONDITION FOR SQLSTATE '99998';
	DECLARE KPI_ERROR CONDITION FOR SQLSTATE '99999' ;
	DECLARE ZOOM_LEVEL INT;
	INSERT INTO gt_gw_main.sp_log VALUES('gt_gw_main','SP_Generate_Global_Statistic_cell_agg_gsm',CONCAT(GT_DB,' CREATE TEMPORARY TABLE tmp_materialization_',WORKER_ID), NOW());	
	
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
				`DATA_DATE` datetime NOT NULL,
				`DATA_HOUR` tinyint(4) NOT NULL,
				`CELL_ID` mediumint(9) NOT NULL,
				`SITE_ID` varchar(20) DEFAULT NULL,
				`BSC_ID` mediumint(9) NOT NULL,
				`BCCH_ARFCN` smallint(5) unsigned DEFAULT NULL,
				`BANDINDEX` varchar(10) DEFAULT NULL,
				`INIT_CALL_CNT` int(11) DEFAULT NULL,
				`END_CALL_CNT` int(11) DEFAULT NULL,
				`VOICE_CNT` int(11) DEFAULT NULL,
				`SIGNAL_CNT` int(11) DEFAULT NULL,
				`SMS_CNT` int(11) DEFAULT NULL,
				`GPRS_CNT` int(11) DEFAULT NULL,
				`OTHER_CNT` int(11) DEFAULT NULL,
				`END_VOICE_CNT` int(11) DEFAULT NULL,
				`END_SIGNAL_CNT` int(11) DEFAULT NULL,
				`END_SMS_CNT` int(11) DEFAULT NULL,
				`END_GPRS_CNT` int(11) DEFAULT NULL,
				`END_OTHER_CNT` int(11) DEFAULT NULL,
				`BLOCK_VOICE_CNT` int(11) DEFAULT NULL,
				`BLOCK_GPRS_CNT` int(11) DEFAULT NULL,
				`BLOCK_SMS_CNT` int(11) DEFAULT NULL,
				`BLOCK_SIGNAL_CNT` int(11) DEFAULT NULL,
				`BLOCK_OTHER_CNT` int(11) DEFAULT NULL,
				`DROP_VOICE_CNT` int(11) DEFAULT NULL,
				`DROP_SIGNAL_CNT` int(11) DEFAULT NULL,
				`DROP_SMS_CNT` int(11) DEFAULT NULL,
				`DROP_GPRS_CNT` int(11) DEFAULT NULL,
				`DROP_OTHER_CNT` int(11) DEFAULT NULL,
				`NON_BLOCK_VOICE_CNT` int(11) DEFAULT NULL,
				`NON_BLOCK_SIGNAL_CNT` int(11) DEFAULT NULL,
				`NON_BLOCK_SMS_CNT` int(11) DEFAULT NULL,
				`NON_BLOCK_GPRS_CNT` int(11) DEFAULT NULL,
				`NON_BLOCK_OTHER_CNT` int(11) DEFAULT NULL,
				`RXLEV_SUM` double DEFAULT NULL,
				`RXLEV_CNT` int(11) DEFAULT NULL,
				`RXQUAL_SUM` double DEFAULT NULL,
				`RXQUAL_CNT` int(11) DEFAULT NULL,
				`CALL_SETUP_TIME_SUM` int(11) DEFAULT NULL,
				`CALL_SETUP_TIME_CNT` int(11) DEFAULT NULL,
				`CALL_SETUP_TIME_VOICE_SUM` int(11) DEFAULT NULL,
				`CALL_SETUP_TIME_VOICE_CNT` int(11) DEFAULT NULL,
				`CALL_SETUP_TIME_SIG_SUM` int(11) DEFAULT NULL,
				`CALL_SETUP_TIME_SIG_CNT` int(11) DEFAULT NULL,
				`CALL_SETUP_TIME_SMS_SUM` int(11) DEFAULT NULL,
				`CALL_SETUP_TIME_SMS_CNT` int(11) DEFAULT NULL,
				`CALL_SETUP_TIME_GPRS_SUM` int(11) DEFAULT NULL,
				`CALL_SETUP_TIME_GPRS_CNT` int(11) DEFAULT NULL,
				`CALL_SETUP_TIME_OTH_SUM` int(11) DEFAULT NULL,
				`CALL_SETUP_TIME_OTH_CNT` int(11) DEFAULT NULL,
				`SF_VOICE_CNT` int(11) DEFAULT NULL,
				`SF_DATA_CNT` int(11) DEFAULT NULL,
				`SF_SMS_CNT` int(11) DEFAULT NULL,
				`SF_SIGNAL_CNT` int(11) DEFAULT NULL,
				`SF_OTHER_CNT` int(11) DEFAULT NULL,
				`CALL_DUR_SUM` double DEFAULT NULL,
				`VOICE_DUR_SUM` double DEFAULT NULL,
				`DATA_DUR_SUM` double DEFAULT NULL
 				 ,PRIMARY KEY (`CELL_ID`,`BSC_ID`,`BCCH_ARFCN`,`BANDINDEX`,`DATA_DATE`,`DATA_HOUR`)
				) ENGINE=MYISAM DEFAULT CHARSET=latin1;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	INSERT INTO gt_gw_main.sp_log VALUES('gt_gw_main','SP_Generate_Global_Statistic_cell_agg_gsm',CONCAT(GT_DB,' INSERT INTO tbl_',GT_DB,'_',WORKER_ID), NOW());	
	 
	SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS ',GT_DB,'.tmp_cell_gsm_start_',WORKER_ID,';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS ',GT_DB,'.tmp_cell_gsm_end_',WORKER_ID,';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('CREATE TEMPORARY TABLE ',GT_DB,'.tmp_cell_gsm_start_',WORKER_ID,' ENGINE=MYISAM AS
				SELECT
					POS_FIRST_CELL AS `CELL_ID`
					,POS_FIRST_BSC AS `BSC_ID`
					,POS_FIRST_SITE AS `SITE_ID`
					,POS_FIRST_BCCH_ARFCN AS `BCCH_ARFCN`
					,POS_FIRST_BANDINDEX AS `BANDINDEX`
					,DATA_DATE
					,DATA_HOUR
					,COUNT(*) AS `INIT_CALL_CNT`
					,SUM(IF(CALL_TYPE=10,1,0)) AS VOICE_CNT
					,SUM(IF(CALL_TYPE=15,1,0)) AS SIGNAL_CNT
					,SUM(IF(CALL_TYPE=16,1,0)) AS SMS_CNT
					,SUM(IF(CALL_TYPE=20,1,0)) AS GPRS_CNT
					,SUM(IF(CALL_TYPE NOT IN (10,15,16,20),1,0)) AS OTHER_CNT
					,SUM(IF(CALL_STATUS=3 AND CALL_TYPE=10,1,0)) AS BLOCK_VOICE_CNT
					,SUM(IF(CALL_STATUS=3 AND CALL_TYPE=20,1,0)) AS BLOCK_GPRS_CNT
					,SUM(IF(CALL_STATUS=3 AND CALL_TYPE=16,1,0)) AS BLOCK_SMS_CNT
					,SUM(IF(CALL_STATUS=3 AND CALL_TYPE=15,1,0)) AS BLOCK_SIGNAL_CNT
					,SUM(IF(CALL_STATUS=3 AND CALL_TYPE NOT IN (10,15,16,20),1,0)) AS BLOCK_OTHER_CNT
					,SUM(POS_FIRST_RXLEV_FULL_DOWNLINK) AS RXLEV_SUM
					,COUNT(POS_FIRST_RXLEV_FULL_DOWNLINK) AS RXLEV_CNT
					,SUM(POS_FIRST_RXQUAL_FULL_DOWNLINK) AS RXQUAL_SUM
					,COUNT(POS_FIRST_RXQUAL_FULL_DOWNLINK) AS RXQUAL_CNT
					,SUM(CALL_SETUP_TIME) AS CALL_SETUP_TIME_SUM
					,SUM(IF(CALL_SETUP_TIME>=0,1,0)) AS CALL_SETUP_TIME_CNT
	
					,IFNULL(SUM(IF(CALL_TYPE =10,CALL_SETUP_TIME,0)),0) AS CALL_SETUP_TIME_VOICE_SUM
					,IFNULL(SUM(IF(CALL_SETUP_TIME>=0 AND CALL_TYPE =10,1,0)),0) AS CALL_SETUP_TIME_VOICE_CNT
					,IFNULL(SUM(IF(CALL_TYPE =15,CALL_SETUP_TIME,0)),0) AS CALL_SETUP_TIME_SIG_SUM
					,IFNULL(SUM(IF(CALL_SETUP_TIME>=0 AND CALL_TYPE =15,1,0)),0) AS CALL_SETUP_TIME_SIG_CNT
					,IFNULL(SUM(IF(CALL_TYPE =16,CALL_SETUP_TIME,0)),0) AS CALL_SETUP_TIME_SMS_SUM
					,IFNULL(SUM(IF(CALL_SETUP_TIME>=0 AND CALL_TYPE =16,1,0)),0) AS CALL_SETUP_TIME_SMS_CNT
					,IFNULL(SUM(IF(CALL_TYPE =20,CALL_SETUP_TIME,0)),0) AS CALL_SETUP_TIME_GPRS_SUM
					,IFNULL(SUM(IF(CALL_SETUP_TIME>=0 AND CALL_TYPE =20,1,0)),0) AS CALL_SETUP_TIME_GPRS_CNT	
					,IFNULL(SUM(IF(CALL_TYPE NOT IN (10,15,16,20),CALL_SETUP_TIME,0)),0) AS CALL_SETUP_TIME_OTH_SUM
					,IFNULL(SUM(IF(CALL_SETUP_TIME>=0 AND CALL_TYPE NOT IN (10,15,16,20),1,0)),0) AS CALL_SETUP_TIME_OTH_CNT
	
					,SUM(IF(CALL_STATUS=6 AND CALL_TYPE=10,1,0)) AS SF_VOICE_CNT
					,SUM(IF(CALL_STATUS=6 AND CALL_TYPE=20,1,0)) AS SF_DATA_CNT
					,SUM(IF(CALL_STATUS=6 AND CALL_TYPE=16,1,0)) AS SF_SMS_CNT
					,SUM(IF(CALL_STATUS=6 AND CALL_TYPE=15,1,0)) AS SF_SIGNAL_CNT
					,SUM(IF(CALL_STATUS=6 AND CALL_TYPE NOT IN (10,15,16,20),1,0)) AS SF_OTHER_CNT
				FROM ',GT_DB,'.table_call_gsm
				WHERE DATA_HOUR =',DATA_HOUR,' AND POS_FIRST_CELL IS NOT NULL AND POS_FIRST_BSC=',PU_ID,' 
				GROUP BY POS_FIRST_BSC,POS_FIRST_CELL,`POS_FIRST_BCCH_ARFCN`,`POS_FIRST_BANDINDEX`
				ORDER BY NULL;
				');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('CREATE TEMPORARY TABLE ',GT_DB,'.tmp_cell_gsm_end_',WORKER_ID,' ENGINE=MYISAM AS
				SELECT
					POS_LAST_CELL AS `CELL_ID`
					,POS_LAST_BSC AS `BSC_ID`
					,POS_LAST_SITE AS `SITE_ID`
					,POS_LAST_BCCH_ARFCN AS `BCCH_ARFCN`
					,POS_LAST_BANDINDEX AS `BANDINDEX`
					,DATA_DATE
					,DATA_HOUR
					,COUNT(*) AS `END_CALL_CNT`
					,SUM(IF(CALL_TYPE=10,1,0)) AS END_VOICE_CNT
					,SUM(IF(CALL_TYPE=15,1,0)) AS END_SIGNAL_CNT
					,SUM(IF(CALL_TYPE=16,1,0)) AS END_SMS_CNT
					,SUM(IF(CALL_TYPE=20,1,0)) AS END_GPRS_CNT
					,SUM(IF(CALL_TYPE NOT IN (10,15,16,20),1,0)) AS END_OTHER_CNT
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
				WHERE DATA_HOUR =',DATA_HOUR,' AND POS_LAST_CELL IS NOT NULL AND POS_LAST_BSC=',PU_ID,' 
 				GROUP BY POS_LAST_CELL,POS_LAST_BSC,POS_LAST_BCCH_ARFCN,POS_LAST_BANDINDEX
				ORDER BY NULL;
				');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('CREATE INDEX `ix_tile_gsm_fq` ON ',GT_DB,'.tmp_cell_gsm_start_',WORKER_ID,' (`CELL_ID`,`BSC_ID`,BCCH_ARFCN,BANDINDEX);');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('CREATE INDEX `ix_tile_gsm_fq` ON ',GT_DB,'.tmp_cell_gsm_end_',WORKER_ID,' (`CELL_ID`,`BSC_ID`,BCCH_ARFCN,BANDINDEX);');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;	
	
	SET @SqlCmd=CONCAT('INSERT INTO ',GT_DB,'.`tmp_materialization_',WORKER_ID,'` 
				( `CELL_ID`,`SITE_ID`,`BSC_ID`,BCCH_ARFCN,BANDINDEX,DATA_DATE,DATA_HOUR)
			SELECT `CELL_ID`,`SITE_ID`,`BSC_ID`,BCCH_ARFCN,BANDINDEX,DATA_DATE,DATA_HOUR
			FROM 
			(
				SELECT `CELL_ID`,`SITE_ID`,',PU_ID,' AS `BSC_ID`,BCCH_ARFCN,BANDINDEX,DATA_DATE,DATA_HOUR FROM ',GT_DB,'.tmp_cell_gsm_start_',WORKER_ID,'
 				UNION 
 				SELECT `CELL_ID`,`SITE_ID`,',PU_ID,' AS `BSC_ID`,BCCH_ARFCN,BANDINDEX,DATA_DATE,DATA_HOUR FROM ',GT_DB,'.tmp_cell_gsm_end_',WORKER_ID,'
			) AA;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('UPDATE ',GT_DB,'.`tmp_materialization_',WORKER_ID,'` a,',GT_DB,'.`tmp_cell_gsm_start_',WORKER_ID,'` B
				SET 
				A.INIT_CALL_CNT=B.INIT_CALL_CNT
				,A.VOICE_CNT=B.VOICE_CNT
				,A.SIGNAL_CNT=B.SIGNAL_CNT
				,A.SMS_CNT=B.SMS_CNT
				,A.GPRS_CNT=B.GPRS_CNT
				,A.OTHER_CNT=B.OTHER_CNT
				,A.BLOCK_VOICE_CNT=B.BLOCK_VOICE_CNT
				,A.BLOCK_GPRS_CNT=B.BLOCK_GPRS_CNT
				,A.BLOCK_SMS_CNT=B.BLOCK_SMS_CNT
				,A.BLOCK_SIGNAL_CNT=B.BLOCK_SIGNAL_CNT
				,A.BLOCK_OTHER_CNT=B.BLOCK_OTHER_CNT
				,A.RXLEV_SUM=B.RXLEV_SUM
				,A.RXLEV_CNT=B.RXLEV_CNT
				,A.RXQUAL_SUM=B.RXQUAL_SUM
				,A.RXQUAL_CNT=B.RXQUAL_CNT
				,A.CALL_SETUP_TIME_SUM=B.CALL_SETUP_TIME_SUM
				,A.CALL_SETUP_TIME_CNT=B.CALL_SETUP_TIME_CNT
				,A.CALL_SETUP_TIME_VOICE_SUM=B.CALL_SETUP_TIME_VOICE_SUM
				,A.CALL_SETUP_TIME_VOICE_CNT=B.CALL_SETUP_TIME_VOICE_CNT
				,A.CALL_SETUP_TIME_SIG_SUM=B.CALL_SETUP_TIME_SIG_SUM
				,A.CALL_SETUP_TIME_SIG_CNT=B.CALL_SETUP_TIME_SIG_CNT
				,A.CALL_SETUP_TIME_SMS_SUM=B.CALL_SETUP_TIME_SMS_SUM
				,A.CALL_SETUP_TIME_SMS_CNT=B.CALL_SETUP_TIME_GPRS_CNT
				,A.CALL_SETUP_TIME_GPRS_SUM=B.CALL_SETUP_TIME_GPRS_SUM
				,A.CALL_SETUP_TIME_GPRS_CNT=B.CALL_SETUP_TIME_GPRS_CNT
				,A.CALL_SETUP_TIME_OTH_SUM=B.CALL_SETUP_TIME_OTH_SUM
				,A.CALL_SETUP_TIME_OTH_CNT=B.CALL_SETUP_TIME_OTH_CNT
				,A.SF_VOICE_CNT=B.SF_VOICE_CNT
				,A.SF_DATA_CNT=B.SF_DATA_CNT
				,A.SF_SMS_CNT=B.SF_SMS_CNT
				,A.SF_SIGNAL_CNT=B.SF_SIGNAL_CNT
				,A.SF_OTHER_CNT=B.SF_OTHER_CNT 
			WHERE A.CELL_ID=B.CELL_ID AND A.BSC_ID=B.BSC_ID 
				AND A.BCCH_ARFCN=B.BCCH_ARFCN AND A.BANDINDEX=B.BANDINDEX;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('UPDATE ',GT_DB,'.`tmp_materialization_',WORKER_ID,'` a,',GT_DB,'.`tmp_cell_gsm_end_',WORKER_ID,'` B
				SET
				A.END_CALL_CNT=B.END_CALL_CNT
				,A.END_VOICE_CNT=B.END_VOICE_CNT
				,A.END_SIGNAL_CNT=B.END_SIGNAL_CNT
				,A.END_SMS_CNT=B.END_SMS_CNT
				,A.END_GPRS_CNT=B.END_GPRS_CNT
				,A.END_OTHER_CNT=B.END_OTHER_CNT
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
				WHERE A.CELL_ID=B.CELL_ID AND A.BSC_ID=B.BSC_ID 
				AND A.BCCH_ARFCN=B.BCCH_ARFCN AND A.BANDINDEX=B.BANDINDEX;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('SELECT * FROM ',GT_DB,'.tmp_materialization_',WORKER_ID,' WHERE CELL_ID >0;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS ',GT_DB,'.tmp_materialization_',WORKER_ID,';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS ',GT_DB,'.tmp_cell_gsm_start_',WORKER_ID,';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS ',GT_DB,'.tmp_cell_gsm_end_',WORKER_ID,';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	INSERT INTO gt_gw_main.sp_log VALUES('gt_gw_main','SP_Generate_Global_Statistic_cell_agg_gsm',CONCAT(GT_DB,' END'), NOW());
	
END$$
DELIMITER ;
