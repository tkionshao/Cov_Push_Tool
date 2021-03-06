DELIMITER $$
USE `gt_gw_main`$$
DROP PROCEDURE IF EXISTS `SP_CreateDB_LTE`$$
CREATE DEFINER=`covmo`@`%` PROCEDURE `SP_Generate_Global_Statistic_tile`(IN DATA_DATE DATE,IN DATA_HOUR TINYINT(2),IN PU_ID MEDIUMINT(9),IN TECH_MASK TINYINT(2), IN GT_DB VARCHAR(100))
BEGIN
	DECLARE SP_Process VARCHAR(100) DEFAULT NULL;
	DECLARE WORKER_ID VARCHAR(10) DEFAULT CONNECTION_ID();
	DECLARE Spider_SP_ERROR CONDITION FOR SQLSTATE '99998';
	DECLARE KPI_ERROR CONDITION FOR SQLSTATE '99999' ;
	DECLARE ZOOM_LEVEL INT;
	INSERT INTO gt_gw_main.sp_log VALUES('gt_gw_main','SP_Generate_Global_Statistic_tile',CONCAT(GT_DB,' CREATE TEMPORARY TABLE tmp_materialization_',WORKER_ID), NOW());	
	
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
				  `INIT_CALL_CNT` INT(11) DEFAULT NULL,
				  `END_CALL_CNT` INT(11) DEFAULT NULL,
				  `VOICE_CNT` INT(11) DEFAULT NULL,
				  `VIDEO_CNT` INT(11) DEFAULT NULL,
				  `PS_R99_CNT` INT(11) DEFAULT NULL,
				  `PS_HSPA_CNT` INT(11) DEFAULT NULL,
				  `M_RAB_CNT` INT(11) DEFAULT NULL, 
				  `SIGNAL_CNT` INT(11) DEFAULT NULL,
				  `SMS_CNT` INT(11) DEFAULT NULL,
				  `PS_OTHER_CNT` INT(11) DEFAULT NULL,
				  `CALL_DUR_SUM` DOUBLE DEFAULT NULL,
				  `CS_DUR_SUM` DOUBLE DEFAULT NULL,
				  `DROP_CNT` INT(11) DEFAULT NULL,
				  `BLOCK_CNT` INT(11) DEFAULT NULL,
				  `DROP_VOICE_CNT` INT(11) DEFAULT NULL,
				  `DROP_VIDEO_CNT` INT(11) DEFAULT NULL,
				  `DROP_PS_R99_CNT` INT(11) DEFAULT NULL,
				  `DROP_PS_HSPA_CNT` INT(11) DEFAULT NULL,
				  `DROP_M_RAB_CNT` INT(11) DEFAULT NULL,
				  `DROP_SIGNAL_CNT` INT(11) DEFAULT NULL,
				  `DROP_SMS_CNT` INT(11) DEFAULT NULL,
				  `DROP_PS_OTHER_CNT` INT(11) DEFAULT NULL,
				  `SHO_ATTEMPT_CNT` INT(11) DEFAULT NULL,
				  `SHO_FAILURE_CNT` INT(11) DEFAULT NULL,
				  `IFHO_ATTEMPT_CNT` INT(11) DEFAULT NULL,
				  `IFHO_FAILURE_CNT` INT(11) DEFAULT NULL,
				  `IRAT_ATTEMPT_CNT` INT(11) DEFAULT NULL,
				  `IRAT_FAILURE_CNT` INT(11) DEFAULT NULL,
				  `PS_UL_VOLUME_SUM` DOUBLE DEFAULT NULL,
				  `PS_DL_VOLUME_SUM` DOUBLE DEFAULT NULL,
				  `PS_UL_SPEED_MAX` DOUBLE DEFAULT NULL,
				  `PS_DL_SPEED_MAX` DOUBLE DEFAULT NULL,
				  `RSCP_SUM` DOUBLE DEFAULT NULL,
				  `RSCP_CNT` INT(11) DEFAULT NULL,
				  `ECNO_SUM` DOUBLE DEFAULT NULL,
				  `ECNO_CNT` INT(11) DEFAULT NULL,
				  `ACTIVE_SET_SUM` DOUBLE DEFAULT NULL,
				  `ACTIVE_SET_CNT` INT(11) DEFAULT NULL,
				  `POLLUTED_PILOT_CNT` INT(11) DEFAULT NULL,
				  `PILOT_DOM_SUM` DOUBLE DEFAULT NULL,
				  `PILOT_CNT` INT(11) DEFAULT NULL,
				  `T19_CNT` INT(11) DEFAULT NULL,
				  `UL_THROUPUT_SUM` DOUBLE DEFAULT NULL,
				  `UL_THROUPUT_CNT` INT(11) DEFAULT NULL,
				  `DL_THROUPUT_SUM` DOUBLE DEFAULT NULL,
				  `DL_THROUPUT_CNT` INT(11) DEFAULT NULL,
				  `NON_BLOCK_VOICE_CNT` INT(11) DEFAULT NULL,
				  `NON_BLOCK_VIDEO_CNT` INT(11) DEFAULT NULL,
				  `NON_BLOCK_PS_R99_CNT` INT(11) DEFAULT NULL,
				  `NON_BLOCK_PS_HSPA_CNT` INT(11) DEFAULT NULL,
				  `NON_BLOCK_M_RAB_CNT` INT(11) DEFAULT NULL,
				  `NON_BLOCK_SIGNAL_CNT` INT(11) DEFAULT NULL,
				  `NON_BLOCK_SMS_CNT` INT(11) DEFAULT NULL,
				  `NON_BLOCK_PS_OTHER_CNT` INT(11) DEFAULT NULL,
				  `PS_CNT` INT(11) DEFAULT NULL,
				  `DROP_PS_CNT` INT(11) DEFAULT NULL,
				  `REG_1_ID` BIGINT(20) DEFAULT NULL,
				  `REG_2_ID` BIGINT(20) DEFAULT NULL,
				  `REG_3_ID` BIGINT(20) DEFAULT NULL,
				  PRIMARY KEY (`TILE_ID`,FREQUENCY,`DATA_DATE`,`DATA_HOUR`)
				) ENGINE=MYISAM DEFAULT CHARSET=latin1;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	INSERT INTO gt_gw_main.sp_log VALUES('gt_gw_main','SP_Generate_Global_Statistic_tile',CONCAT(GT_DB,' INSERT INTO tbl_',GT_DB,'_',WORKER_ID), NOW());	
	 
	
	SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS ',GT_DB,'.tmp_tile_start_',WORKER_ID,';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS ',GT_DB,'.tmp_tile_end_',WORKER_ID,';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS ',GT_DB,'.tmp_tile_ifho_',WORKER_ID,';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS ',GT_DB,'.tmp_tile_fp_',WORKER_ID,';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('CREATE TEMPORARY TABLE ',GT_DB,'.tmp_tile_start_',WORKER_ID,' ENGINE=MYISAM AS
				SELECT
					gt_covmo_proj_geohash_to_hex_geohash(POS_FIRST_LOC,',@ZOOM_LEVEL,') AS TILE_ID
 					,gt_covmo_proj_geohash_to_hex_geohash(POS_FIRST_LOC,',@ZOOM_LEVEL-3,') AS TILE_ID_16
 					,gt_covmo_proj_geohash_to_hex_geohash(POS_FIRST_LOC,',@ZOOM_LEVEL-6,') AS TILE_ID_13
 					,POS_FIRST_FREQUENCY AS FREQUENCY
					,DATA_DATE
					,DATA_HOUR
					,COUNT(*) AS INIT_CALL_CNT
					,SUM(IF(CALL_TYPE=10,1,0)) AS VOICE_CNT
					,SUM(IF(CALL_TYPE=11,1,0)) AS VIDEO_CNT
					,SUM(IF(CALL_TYPE=12,1,0)) AS PS_R99_CNT
					,SUM(IF(CALL_TYPE=13,1,0)) AS PS_HSPA_CNT
					,SUM(IF(CALL_TYPE=14,1,0)) AS M_RAB_CNT
					,SUM(IF(CALL_TYPE=15,1,0)) AS SIGNAL_CNT
					,SUM(IF(CALL_TYPE=16,1,0)) AS SMS_CNT
					,SUM(IF(CALL_TYPE=18,1,0)) AS PS_OTHER_CNT
					,SUM(IF(CALL_STATUS=3,1,0)) AS BLOCK_CNT
					,SUM(RRC_CONNECT_DURATION) AS CALL_DUR_SUM
					,SUM(CS_CALL_DURA) AS CS_DUR_SUM
					,SUM(SHO) AS SHO_ATTEMPT_CNT
					,0 AS SHO_FAILURE_CNT
					,IFNULL(SUM(IF(CALL_TYPE IN (12,13,14,18),UL_TRAFFIC_VOLUME,0)),0) AS PS_UL_VOLUME_SUM
					,IFNULL(SUM(IF(CALL_TYPE IN (12,13,14,18),DL_TRAFFIC_VOLUME,0)),0) AS PS_DL_VOLUME_SUM
					,MAX(IF(CALL_TYPE IN (12,13,14,18),UL_THROUGHPUT_AVG,0)) AS PS_UL_SPEED_MAX
					,MAX(IF(CALL_TYPE IN (12,13,14,18),DL_THROUGHPUT_AVG,0)) AS PS_DL_SPEED_MAX
					,SUM(POS_FIRST_RSCP) AS RSCP_SUM
					,COUNT(POS_FIRST_RSCP) AS RSCP_CNT
					,SUM(POS_FIRST_ECN0) AS ECNO_SUM
					,COUNT(POS_FIRST_ECN0) AS ECNO_CNT
					,IFNULL(SUM(IF(CALL_TYPE IN (12,13,14,18),UL_THROUGHPUT_AVG,0)),0) AS UL_THROUPUT_SUM
					,IFNULL(COUNT(IF(CALL_TYPE IN (12,13,14,18),UL_THROUGHPUT_AVG,0)),0) AS UL_THROUPUT_CNT
					,IFNULL(SUM(IF(CALL_TYPE IN (12,13,14,18),DL_THROUGHPUT_AVG,0)),0) AS DL_THROUPUT_SUM
					,IFNULL(COUNT(IF(CALL_TYPE IN (12,13,14,18),DL_THROUGHPUT_AVG,0)),0) AS DL_THROUPUT_CNT
					,SUM(IF(CALL_TYPE IN (12,13,14),1,0)) AS PS_CNT
				FROM ',GT_DB,'.table_call
				WHERE DATA_HOUR =',DATA_HOUR,' AND POS_FIRST_LOC IS NOT NULL AND POS_FIRST_RNC=',PU_ID,' 
				GROUP BY gt_covmo_proj_geohash_to_hex_geohash(POS_FIRST_LOC,',@ZOOM_LEVEL,'),POS_FIRST_FREQUENCY				
				ORDER BY NULL;
				');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('CREATE TEMPORARY TABLE ',GT_DB,'.tmp_tile_end_',WORKER_ID,' ENGINE=MYISAM AS
				SELECT
					gt_covmo_proj_geohash_to_hex_geohash(POS_LAST_LOC,',@ZOOM_LEVEL,') AS TILE_ID
 					,gt_covmo_proj_geohash_to_hex_geohash(POS_LAST_LOC,',@ZOOM_LEVEL-3,') AS TILE_ID_16
 					,gt_covmo_proj_geohash_to_hex_geohash(POS_LAST_LOC,',@ZOOM_LEVEL-6,') AS TILE_ID_13
 					,POS_LAST_FREQUENCY AS FREQUENCY
					,DATA_DATE
					,DATA_HOUR
					,COUNT(*) AS END_CALL_CNT
					,SUM(IF(CALL_STATUS=2,1,0)) AS DROP_CNT
					,IFNULL(SUM(IRAT_HHO_ATTEMPT),0) AS IRAT_ATTEMPT_CNT
					,IFNULL(SUM(IRAT_HHO_ATTEMPT-`IRAT_HHO_SUCCESS`),0) AS IRAT_FAILURE_CNT
					,SUM(IF(CALL_STATUS=2 AND CALL_TYPE=10,1,0)) AS DROP_VOICE_CNT
					,SUM(IF(CALL_STATUS=2 AND CALL_TYPE=11,1,0)) AS DROP_VIDEO_CNT
					,SUM(IF(CALL_STATUS=2 AND CALL_TYPE=12,1,0)) AS DROP_PS_R99_CNT
					,SUM(IF(CALL_STATUS=2 AND CALL_TYPE=13,1,0)) AS DROP_PS_HSPA_CNT
					,SUM(IF(CALL_STATUS=2 AND CALL_TYPE=14,1,0)) AS DROP_M_RAB_CNT
					,SUM(IF(CALL_STATUS=2 AND CALL_TYPE=15,1,0)) AS DROP_SIGNAL_CNT
					,SUM(IF(CALL_STATUS=2 AND CALL_TYPE=16,1,0)) AS DROP_SMS_CNT
					,SUM(IF(CALL_STATUS=2 AND CALL_TYPE=18,1,0)) AS DROP_PS_OTHER_CNT
					,SUM(IF(CALL_STATUS IN (1,2,4,5) AND CALL_TYPE=10,1,0)) AS NON_BLOCK_VOICE_CNT
					,SUM(IF(CALL_STATUS IN (1,2,4,5) AND CALL_TYPE=11,1,0)) AS NON_BLOCK_VIDEO_CNT
					,SUM(IF(CALL_STATUS IN (1,2,4,5) AND CALL_TYPE=12,1,0)) AS NON_BLOCK_PS_R99_CNT
					,SUM(IF(CALL_STATUS IN (1,2,4,5) AND CALL_TYPE=13,1,0)) AS NON_BLOCK_PS_HSPA_CNT
					,SUM(IF(CALL_STATUS IN (1,2,4,5) AND CALL_TYPE=14,1,0)) AS NON_BLOCK_M_RAB_CNT
					,SUM(IF(CALL_STATUS IN (1,2,4,5) AND CALL_TYPE=15,1,0)) AS NON_BLOCK_SIGNAL_CNT
					,SUM(IF(CALL_STATUS IN (1,2,4,5) AND CALL_TYPE=16,1,0)) AS NON_BLOCK_SMS_CNT
					,SUM(IF(CALL_STATUS IN (1,2,4,5) AND CALL_TYPE=18,1,0)) AS NON_BLOCK_PS_OTHER_CNT
					,SUM(IF(CALL_STATUS=2 AND CALL_TYPE IN(12,13,14),1,0)) AS DROP_PS_CNT
				FROM ',GT_DB,'.table_call
				WHERE DATA_HOUR =',DATA_HOUR,' AND POS_LAST_LOC IS NOT NULL AND POS_LAST_RNC=',PU_ID,' 
 				GROUP BY gt_covmo_proj_geohash_to_hex_geohash(POS_LAST_LOC,',@ZOOM_LEVEL,'),POS_LAST_FREQUENCY				
				ORDER BY NULL;
				');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('CREATE TEMPORARY TABLE ',GT_DB,'.tmp_tile_ifho_',WORKER_ID,' ENGINE=MYISAM AS
				SELECT
					gt_covmo_proj_geohash_to_hex_geohash(POS_IFHO_LOC,',@ZOOM_LEVEL,') AS TILE_ID
					,gt_covmo_proj_geohash_to_hex_geohash(POS_IFHO_LOC,',@ZOOM_LEVEL-3,') AS TILE_ID_16
 					,gt_covmo_proj_geohash_to_hex_geohash(POS_IFHO_LOC,',@ZOOM_LEVEL-6,') AS TILE_ID_13
 					,POS_IFHO_FREQUENCY AS FREQUENCY
					,DATA_DATE
					,DATA_HOUR
					,IFNULL(SUM(IFHO),0) AS IFHO_ATTEMPT_CNT 
					,IFNULL((SUM(IFHO)-COUNT(POS_IFHO_CELL)),0) AS IFHO_FAILURE_CNT
				FROM ',GT_DB,'.table_call
				WHERE DATA_HOUR =',DATA_HOUR,' AND POS_IFHO_LOC IS NOT NULL AND POS_IFHO_RNC=',PU_ID,' 
				GROUP BY gt_covmo_proj_geohash_to_hex_geohash(POS_IFHO_LOC,',@ZOOM_LEVEL,'),POS_IFHO_FREQUENCY
				ORDER BY NULL;
				');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('CREATE TEMPORARY TABLE ',GT_DB,'.tmp_tile_fp_',WORKER_ID,' ENGINE=MYISAM AS
				SELECT
					`TILE_ID`,
 					gt_geohash_ext(`TILE_ID`,16) AS TILE_ID_16,
 					gt_geohash_ext(`TILE_ID`,13) AS TILE_ID_13,
 					`FREQUENCY`,
					`DATA_DATE`,
					`DATA_HOUR`,
					SUM(`AVG_AS_CNT`) AS `ACTIVE_SET_SUM`,
					SUM(`BEST_CNT`) AS `ACTIVE_SET_CNT`,
					SUM(`BEST_CNT`) AS `PILOT_CNT`,
					SUM(`PP_CNT`) AS `POLLUTED_PILOT_CNT`,
					SUM(`RSCP_GAP`) AS `PILOT_DOM_SUM`
				FROM ',GT_DB,'.`table_tile_fp`
				WHERE DATA_HOUR =',DATA_HOUR,' AND RNC_ID=',PU_ID,'  
				GROUP BY `TILE_ID`,FREQUENCY
				HAVING SUM(`BEST_CNT`)>1
				ORDER BY NULL;
				');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	SET @SqlCmd=CONCAT('CREATE INDEX `ix_tile_fq` ON ',GT_DB,'.tmp_tile_start_',WORKER_ID,' (TILE_ID,FREQUENCY);');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('CREATE INDEX `ix_tile_fq` ON ',GT_DB,'.tmp_tile_end_',WORKER_ID,' (TILE_ID,FREQUENCY);');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('CREATE INDEX `ix_tile_fq` ON ',GT_DB,'.tmp_tile_ifho_',WORKER_ID,' (TILE_ID,FREQUENCY);');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('CREATE INDEX `ix_tile_fq` ON ',GT_DB,'.tmp_tile_fp_',WORKER_ID,' (TILE_ID,FREQUENCY);');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	
	SET @SqlCmd=CONCAT('INSERT INTO ',GT_DB,'.`tmp_materialization_',WORKER_ID,'` 
				( TILE_ID,FREQUENCY,DATA_DATE,DATA_HOUR)
				SELECT TILE_ID,FREQUENCY,DATA_DATE,DATA_HOUR FROM ',GT_DB,'.tmp_tile_fp_',WORKER_ID,'
				;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('UPDATE ',GT_DB,'.`tmp_materialization_',WORKER_ID,'` a,',GT_DB,'.`tmp_tile_start_',WORKER_ID,'` B
				SET 
				A.TILE_ID_16=B.TILE_ID_16
				,A.TILE_ID_13=B.TILE_ID_13
				,A.INIT_CALL_CNT=B.INIT_CALL_CNT
				,A.VOICE_CNT=B.VOICE_CNT
				,A.VIDEO_CNT=B.VIDEO_CNT
				,A.PS_R99_CNT=B.PS_R99_CNT
				,A.PS_HSPA_CNT=B.PS_HSPA_CNT
				,A.M_RAB_CNT=B.M_RAB_CNT
				,A.SIGNAL_CNT=B.SIGNAL_CNT
				,A.SMS_CNT=B.SMS_CNT
				,A.PS_OTHER_CNT=B.PS_OTHER_CNT
				,A.CALL_DUR_SUM=B.CALL_DUR_SUM
				,A.CS_DUR_SUM=B.CS_DUR_SUM
				,A.BLOCK_CNT=B.BLOCK_CNT
				,A.SHO_ATTEMPT_CNT=B.SHO_ATTEMPT_CNT
				,A.SHO_FAILURE_CNT=B.SHO_FAILURE_CNT
				,A.PS_UL_VOLUME_SUM=B.PS_UL_VOLUME_SUM
				,A.PS_DL_VOLUME_SUM=B.PS_DL_VOLUME_SUM
				,A.PS_UL_SPEED_MAX=B.PS_UL_SPEED_MAX
				,A.PS_DL_SPEED_MAX=B.PS_DL_SPEED_MAX
				,A.RSCP_SUM=B.RSCP_SUM
				,A.RSCP_CNT=B.RSCP_CNT
				,A.ECNO_SUM=B.ECNO_SUM
				,A.ECNO_CNT=B.ECNO_CNT
				,A.UL_THROUPUT_SUM=B.UL_THROUPUT_SUM
				,A.UL_THROUPUT_CNT=B.UL_THROUPUT_CNT
				,A.DL_THROUPUT_SUM=B.DL_THROUPUT_SUM
				,A.DL_THROUPUT_CNT=B.DL_THROUPUT_CNT
				,A.PS_CNT=B.PS_CNT
			WHERE A.TILE_ID=B.TILE_ID AND A.FREQUENCY=B.FREQUENCY;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('UPDATE ',GT_DB,'.`tmp_materialization_',WORKER_ID,'` a,',GT_DB,'.`tmp_tile_end_',WORKER_ID,'` B
				SET
				A.TILE_ID_16=CASE WHEN A.TILE_ID_16 IS NULL THEN B.TILE_ID_16 ELSE A.TILE_ID_16 END
				,A.TILE_ID_13=CASE WHEN A.TILE_ID_13 IS NULL THEN B.TILE_ID_13 ELSE A.TILE_ID_13 END
				,A.END_CALL_CNT=B.END_CALL_CNT
				,A.IRAT_ATTEMPT_CNT=B.IRAT_ATTEMPT_CNT
				,A.IRAT_FAILURE_CNT=B.IRAT_FAILURE_CNT
				,A.DROP_VOICE_CNT=B.DROP_VOICE_CNT
				,A.DROP_VIDEO_CNT=B.DROP_VIDEO_CNT
				,A.DROP_PS_R99_CNT=B.DROP_PS_R99_CNT
				,A.DROP_PS_HSPA_CNT=B.DROP_PS_HSPA_CNT
				,A.DROP_M_RAB_CNT=B.DROP_M_RAB_CNT
				,A.DROP_SIGNAL_CNT=B.DROP_SIGNAL_CNT
				,A.DROP_SMS_CNT=B.DROP_SMS_CNT
				,A.DROP_PS_OTHER_CNT=B.DROP_PS_OTHER_CNT
				,A.NON_BLOCK_VOICE_CNT=B.NON_BLOCK_VOICE_CNT
				,A.NON_BLOCK_VIDEO_CNT=B.NON_BLOCK_VIDEO_CNT
				,A.NON_BLOCK_PS_R99_CNT=B.NON_BLOCK_PS_R99_CNT
				,A.NON_BLOCK_PS_HSPA_CNT=B.NON_BLOCK_PS_HSPA_CNT
				,A.NON_BLOCK_M_RAB_CNT=B.NON_BLOCK_M_RAB_CNT
				,A.NON_BLOCK_SIGNAL_CNT=B.NON_BLOCK_SIGNAL_CNT
				,A.NON_BLOCK_SMS_CNT=B.NON_BLOCK_SMS_CNT
				,A.NON_BLOCK_PS_OTHER_CNT=B.NON_BLOCK_PS_OTHER_CNT
				,A.DROP_PS_CNT=B.DROP_PS_CNT
			WHERE A.TILE_ID=B.TILE_ID AND A.FREQUENCY=B.FREQUENCY;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('UPDATE ',GT_DB,'.`tmp_materialization_',WORKER_ID,'` a,',GT_DB,'.`tmp_tile_ifho_',WORKER_ID,'` B
				SET
				A.TILE_ID_16=CASE WHEN A.TILE_ID_16 IS NULL THEN B.TILE_ID_16 ELSE A.TILE_ID_16 END
				,A.TILE_ID_13=CASE WHEN A.TILE_ID_13 IS NULL THEN B.TILE_ID_13 ELSE A.TILE_ID_13 END
				,A.IFHO_ATTEMPT_CNT=B.IFHO_ATTEMPT_CNT
				,A.IFHO_FAILURE_CNT=B.IFHO_FAILURE_CNT
			WHERE A.TILE_ID=B.TILE_ID AND A.FREQUENCY=B.FREQUENCY;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('UPDATE ',GT_DB,'.`tmp_materialization_',WORKER_ID,'` a,',GT_DB,'.`tmp_tile_fp_',WORKER_ID,'` B
				SET
				A.TILE_ID_16=CASE WHEN A.TILE_ID_16 IS NULL THEN B.TILE_ID_16 ELSE A.TILE_ID_16 END
				,A.TILE_ID_13=CASE WHEN A.TILE_ID_13 IS NULL THEN B.TILE_ID_13 ELSE A.TILE_ID_13 END
				,A.ACTIVE_SET_SUM=B.ACTIVE_SET_SUM
				,A.ACTIVE_SET_CNT=B.ACTIVE_SET_CNT
				,A.PILOT_CNT=B.PILOT_CNT
				,A.POLLUTED_PILOT_CNT=B.POLLUTED_PILOT_CNT
				,A.PILOT_DOM_SUM=B.PILOT_DOM_SUM
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
	
	SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS ',GT_DB,'.tmp_tile_start_',WORKER_ID,';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS ',GT_DB,'.tmp_tile_end_',WORKER_ID,';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS ',GT_DB,'.tmp_tile_ifho_',WORKER_ID,';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS ',GT_DB,'.tmp_tile_fp_',WORKER_ID,';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	INSERT INTO gt_gw_main.sp_log VALUES('gt_gw_main','SP_Generate_Global_Statistic_tile',CONCAT(GT_DB,' END'), NOW());
	
END$$
DELIMITER ;
