CREATE DEFINER=`covmo`@`%` PROCEDURE `SP_Generate_Global_Statistic_roamer_gsm`(IN DATA_DATE DATE,IN DATA_HOUR TINYINT(2),IN PU_ID MEDIUMINT(9),IN TECH_MASK TINYINT(2), IN GT_DB VARCHAR(100),IN TileResolution VARCHAR(10))
BEGIN
	DECLARE WORKER_ID VARCHAR(10) DEFAULT CONNECTION_ID();
	
  	CALL gt_gw_main.`SP_Sub_Set_Session_Param_LTE`('');
  	
  	
	INSERT INTO gt_gw_main.sp_log VALUES('gt_gw_main','SP_Generate_Global_Statistic_roamer_gsm',CONCAT(GT_DB,' CREATE TEMPORARY TABLE tmp_materialization_',WORKER_ID), NOW());	
	
	SET SESSION group_concat_max_len=102400; 	
	SET @ZOOM_LEVEL1 = gt_covmo_csv_get(TileResolution,1);
	SET @ZOOM_LEVEL2 = gt_covmo_csv_get(TileResolution,2);
	SELECT att_value INTO @ZOOM_LEVEL3 FROM gt_covmo.`sys_config` WHERE `group_name`='system' AND att_name = 'MapResolution';
	
	SET @TILE_ID_LVL1 = CONCAT('TILE_ID_',gt_covmo_csv_get(TileResolution,1));
	SET @TILE_ID_LVL2 = CONCAT('TILE_ID_',gt_covmo_csv_get(TileResolution,2));
	
	SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS ',GT_DB,'.tmp_materialization_',WORKER_ID,';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('CREATE TEMPORARY TABLE ',GT_DB,'.tmp_materialization_',WORKER_ID,' (
				  `TILE_ID` BIGINT(20) NOT NULL,
				  `MCC` char(3) DEFAULT NULL,
				  `MNC` varchar(3) DEFAULT NULL,
				  `FREQUENCY` SMALLINT(6) NOT NULL,
				  `BANDINDEX` varchar(10) DEFAULT NULL,
				  `DATA_DATE` DATETIME NOT NULL,
				  `DATA_HOUR` TINYINT(2) NOT NULL,
				  `',@TILE_ID_LVL2,'` BIGINT(20) DEFAULT NULL,
				  `',@TILE_ID_LVL1,'` BIGINT(20) DEFAULT NULL,
				  INIT_CALL_CNT int(11) DEFAULT NULL, 
				  END_CALL_CNT int(11) DEFAULT NULL,
				  VOICE_CNT int(11) DEFAULT NULL,
				  SIGNAL_CNT int(11) DEFAULT NULL,
				  SMS_CNT int(11) DEFAULT NULL,
				  GPRS_CNT int(11) DEFAULT NULL,
				  OTHER_CNT int(11) DEFAULT NULL,
				  BLOCK_CNT int(11) DEFAULT NULL,
				  DROP_VOICE_CNT int(11) DEFAULT NULL,
				  DROP_SIGNAL_CNT int(11) DEFAULT NULL,
				  DROP_SMS_CNT int(11) DEFAULT NULL,
				  DROP_GPRS_CNT int(11) DEFAULT NULL,
				  DROP_OTHER_CNT int(11) DEFAULT NULL,
				  NON_BLOCK_VOICE_CNT int(11) DEFAULT NULL,
				  NON_BLOCK_SIGNAL_CNT int(11) DEFAULT NULL,
				  NON_BLOCK_SMS_CNT int(11) DEFAULT NULL,
				  NON_BLOCK_GPRS_CNT int(11) DEFAULT NULL,
				  NON_BLOCK_OTHER_CNT int(11) DEFAULT NULL,
				  CALL_DUR_SUM double DEFAULT NULL,
				  CALL_SETUP_TIME_SUM int(11) DEFAULT NULL,
				  CALL_SETUP_TIME_CNT int(11) DEFAULT NULL,
				  CALL_SETUP_TIME_VOICE_SUM int(11) DEFAULT NULL,
				  CALL_SETUP_TIME_VOICE_CNT int(11) DEFAULT NULL,
				  CALL_SETUP_TIME_SIG_SUM int(11) DEFAULT NULL,
				  CALL_SETUP_TIME_SIG_CNT int(11) DEFAULT NULL,
				  CALL_SETUP_TIME_SMS_SUM int(11) DEFAULT NULL,
				  CALL_SETUP_TIME_SMS_CNT int(11) DEFAULT NULL,
				  CALL_SETUP_TIME_GPRS_SUM int(11) DEFAULT NULL,
				  CALL_SETUP_TIME_GPRS_CNT int(11) DEFAULT NULL,
				  CALL_SETUP_TIME_OTH_SUM int(11) DEFAULT NULL,
				  CALL_SETUP_TIME_OTH_CNT int(11) DEFAULT NULL,
				  `REG_1_ID` BIGINT(20) DEFAULT NULL,
				  `REG_2_ID` BIGINT(20) DEFAULT NULL,
				  `REG_3_ID` BIGINT(20) DEFAULT NULL,
				  PRIMARY KEY (`TILE_ID`,MCC,MNC,FREQUENCY,BANDINDEX,`DATA_DATE`,`DATA_HOUR`)
				) ENGINE=MYISAM DEFAULT CHARSET=latin1;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	INSERT INTO gt_gw_main.sp_log VALUES('gt_gw_main','SP_Generate_Global_Statistic_roamer_gsm',CONCAT(GT_DB,' INSERT INTO tbl_',GT_DB,'_',WORKER_ID), NOW());	
	 
	
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
					gt_geohash_ext(POS_FIRST_LOC,',@ZOOM_LEVEL3,') AS TILE_ID
					,MCC,MNC
 					,gt_geohash_ext(POS_FIRST_LOC,',@ZOOM_LEVEL2,') AS ',@TILE_ID_LVL2,'
 					,gt_geohash_ext(POS_FIRST_LOC,',@ZOOM_LEVEL1,') AS ',@TILE_ID_LVL1,'
 					,POS_FIRST_BCCH_ARFCN AS FREQUENCY
 					,POS_FIRST_BANDINDEX AS BANDINDEX
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
					,SUM(CALL_SETUP_TIME) AS CALL_SETUP_TIME_SUM
					,COUNT(CALL_SETUP_TIME) AS CALL_SETUP_TIME_CNT
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
				FROM ',GT_DB,'.table_roamer_call_gsm
				WHERE DATA_HOUR =',DATA_HOUR,' AND POS_FIRST_LOC IS NOT NULL AND POS_FIRST_BSC=',PU_ID,' 
				AND DURATION<100000000
				GROUP BY gt_geohash_ext(POS_FIRST_LOC,',@ZOOM_LEVEL3,'),`POS_FIRST_BCCH_ARFCN`,`POS_FIRST_BANDINDEX`
				ORDER BY NULL;
				');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('CREATE TEMPORARY TABLE ',GT_DB,'.tmp_tile_gsm_end_',WORKER_ID,' ENGINE=MYISAM AS
				SELECT
					gt_geohash_ext(POS_LAST_LOC,',@ZOOM_LEVEL3,') AS TILE_ID
					,MCC,MNC
 					,gt_geohash_ext(POS_LAST_LOC,',@ZOOM_LEVEL2,') AS ',@TILE_ID_LVL2,'
 					,gt_geohash_ext(POS_LAST_LOC,',@ZOOM_LEVEL1,') AS ',@TILE_ID_LVL1,'
 					,POS_LAST_BCCH_ARFCN AS FREQUENCY
 					,POS_LAST_BANDINDEX AS BANDINDEX
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
				FROM ',GT_DB,'.table_roamer_call_gsm
				WHERE DATA_HOUR =',DATA_HOUR,' AND POS_LAST_LOC IS NOT NULL AND POS_LAST_BSC=',PU_ID,' 
 				GROUP BY gt_geohash_ext(POS_LAST_LOC,',@ZOOM_LEVEL3,'),`POS_LAST_BCCH_ARFCN`,`POS_LAST_BANDINDEX`
				ORDER BY NULL;
				');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('CREATE INDEX `ix_tile_gsm_fq` ON ',GT_DB,'.tmp_tile_gsm_start_',WORKER_ID,' (TILE_ID,FREQUENCY,BANDINDEX);');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('CREATE INDEX `ix_tile_gsm_fq` ON ',GT_DB,'.tmp_tile_gsm_end_',WORKER_ID,' (TILE_ID,FREQUENCY,BANDINDEX);');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;	
	
	SET @SqlCmd=CONCAT('INSERT INTO ',GT_DB,'.`tmp_materialization_',WORKER_ID,'` 
				( TILE_ID,MCC,MNC,FREQUENCY,BANDINDEX,DATA_DATE,DATA_HOUR)
			SELECT TILE_ID,MCC,MNC,FREQUENCY,BANDINDEX,DATA_DATE,DATA_HOUR
			FROM 
			(
				SELECT TILE_ID,MCC,MNC,FREQUENCY,BANDINDEX,DATA_DATE,DATA_HOUR FROM ',GT_DB,'.tmp_tile_gsm_start_',WORKER_ID,'
 				UNION 
 				SELECT TILE_ID,MCC,MNC,FREQUENCY,BANDINDEX,DATA_DATE,DATA_HOUR FROM ',GT_DB,'.tmp_tile_gsm_end_',WORKER_ID,'
			) AA;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('UPDATE ',GT_DB,'.`tmp_materialization_',WORKER_ID,'` a,',GT_DB,'.`tmp_tile_gsm_start_',WORKER_ID,'` B
				SET 
				A.',@TILE_ID_LVL2,'=B.',@TILE_ID_LVL2,'
				,A.',@TILE_ID_LVL1,'=B.',@TILE_ID_LVL1,'
				,A.INIT_CALL_CNT=B.INIT_CALL_CNT
				,A.VOICE_CNT=B.VOICE_CNT
				,A.SIGNAL_CNT=B.SIGNAL_CNT
				,A.SMS_CNT=B.SMS_CNT
				,A.GPRS_CNT=B.GPRS_CNT
				,A.OTHER_CNT=B.OTHER_CNT
				,A.CALL_DUR_SUM=B.CALL_DUR_SUM
				,A.BLOCK_CNT=B.BLOCK_CNT
				,A.`CALL_SETUP_TIME_SUM`=B.CALL_SETUP_TIME_SUM
				,A.`CALL_SETUP_TIME_CNT`=B.CALL_SETUP_TIME_CNT
				,A.CALL_SETUP_TIME_VOICE_SUM=B.CALL_SETUP_TIME_VOICE_SUM
				,A.CALL_SETUP_TIME_VOICE_CNT=B.CALL_SETUP_TIME_VOICE_CNT
				,A.CALL_SETUP_TIME_SIG_SUM=B.CALL_SETUP_TIME_SIG_SUM
				,A.CALL_SETUP_TIME_SIG_CNT=B.CALL_SETUP_TIME_SIG_CNT
				,A.CALL_SETUP_TIME_SMS_SUM=B.CALL_SETUP_TIME_SMS_SUM
				,A.CALL_SETUP_TIME_SMS_CNT=B.CALL_SETUP_TIME_SMS_CNT
				,A.CALL_SETUP_TIME_GPRS_SUM=B.CALL_SETUP_TIME_GPRS_SUM
				,A.CALL_SETUP_TIME_GPRS_CNT=B.CALL_SETUP_TIME_GPRS_CNT
				,A.CALL_SETUP_TIME_OTH_SUM=B.CALL_SETUP_TIME_OTH_SUM
				,A.CALL_SETUP_TIME_OTH_CNT=B.CALL_SETUP_TIME_OTH_CNT
			WHERE A.TILE_ID=B.TILE_ID AND 
					A.MCC=B.MCC AND
					A.MNC=B.MNC AND
					A.FREQUENCY=B.FREQUENCY AND A.BANDINDEX=B.BANDINDEX;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('UPDATE ',GT_DB,'.`tmp_materialization_',WORKER_ID,'` a,',GT_DB,'.`tmp_tile_gsm_end_',WORKER_ID,'` B
				SET
				A.',@TILE_ID_LVL2,'=CASE WHEN A.',@TILE_ID_LVL2,' IS NULL THEN B.',@TILE_ID_LVL2,' ELSE A.',@TILE_ID_LVL2,' END
				,A.',@TILE_ID_LVL1,'=CASE WHEN A.',@TILE_ID_LVL1,' IS NULL THEN B.',@TILE_ID_LVL1,' ELSE A.',@TILE_ID_LVL1,' END
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
			WHERE A.TILE_ID=B.TILE_ID AND 
					A.MCC=B.MCC AND
					A.MNC=B.MNC AND
					A.FREQUENCY=B.FREQUENCY AND A.BANDINDEX=B.BANDINDEX;');
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
	INSERT INTO gt_gw_main.sp_log VALUES('gt_gw_main','SP_Generate_Global_Statistic_roamer_gsm',CONCAT(GT_DB,' END'), NOW());
	
