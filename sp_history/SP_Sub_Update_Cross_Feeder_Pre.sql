DELIMITER $$
USE `gt_gw_main`$$
DROP PROCEDURE IF EXISTS `SP_CreateDB_LTE`$$
CREATE DEFINER=`covmo`@`%` PROCEDURE `SP_Sub_Update_Cross_Feeder_Pre`(IN GT_DB VARCHAR(100))
BEGIN
	DECLARE START_TIME DATETIME DEFAULT SYSDATE();
	DECLARE WORKER_ID VARCHAR(10) DEFAULT CONNECTION_ID();
	DECLARE CURRENT_NT_DB VARCHAR(50) DEFAULT CONCAT('gt_nt_',gt_strtok(GT_DB,3,'_'));
	
	CALL SP_Sub_Set_Session_Param_LTE(GT_DB);	
	
	INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'SP_Sub_Update_Cross_Feeder_Pre','Start ', NOW());
	INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'SP_Sub_Update_Cross_Feeder_Pre','S1 ', NOW());
	
	SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS ',GT_DB,'.tmp_pos_enb_distinct_',WORKER_ID,';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('CREATE TEMPORARY TABLE ',GT_DB,'.tmp_pos_enb_distinct_',WORKER_ID,' 
			(ENODEB_ID MEDIUMINT(9) DEFAULT NULL,
			  CELL_ID MEDIUMINT(9) DEFAULT NULL,
			  posLat DOUBLE,
			  posLng DOUBLE,
			  cause_f1  TINYINT(4),
			  cause_f2  TINYINT(4),
			  SERVING_FLAG TINYINT(4)
			  )
			ENGINE=MYISAM;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('INSERT INTO ',GT_DB,'.tmp_pos_enb_distinct_',WORKER_ID,'
		(ENODEB_ID,CELL_ID,posLat,posLng,cause_f1,cause_f2,SERVING_FLAG)
		SELECT enodeb_id, cell_id
			,gt_covmo_proj_geohash_to_lat(loc_id) AS posLat
			,gt_covmo_proj_geohash_to_lng(loc_id) AS posLng   
			,((cause>>38)&0x03) AS cause_f1,((CAUSE)&0x0f) AS cause_f2,1
		FROM ',GT_DB,'.table_position_convert_serving_lte
		WHERE event_id IN (200);');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	SET @SqlCmd=CONCAT('CREATE INDEX idx_pos_enb_distinct ON ',GT_DB,'.tmp_pos_enb_distinct_',WORKER_ID,'(cause_f1,cause_f2,serving_flag,enodeb_id, cell_id);');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'SP_Sub_Update_Cross_Feeder_Pre','S2 ', NOW());
	
	SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS ',GT_DB,'.tmp_nt_',WORKER_ID,';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	SET @SqlCmd=CONCAT('
			CREATE TEMPORARY TABLE ',GT_DB,'.tmp_nt_',WORKER_ID,' 
			(ENODEB_ID MEDIUMINT(9) DEFAULT NULL,
			  CELL_ID MEDIUMINT(9) DEFAULT NULL,
			  LONGITUDE DOUBLE,
			  LATITUDE DOUBLE,
			  AZIMUTH  DOUBLE
			  )
			ENGINE=MYISAM ;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	SET @SqlCmd=CONCAT('
			INSERT INTO ',GT_DB,'.tmp_nt_',WORKER_ID,'
			(ENODEB_ID,CELL_ID,LONGITUDE,LATITUDE,AZIMUTH)
			SELECT ENODEB_ID,CELL_ID,LONGITUDE,LATITUDE,AZIMUTH FROM ',CURRENT_NT_DB,'.nt_antenna_current_lte;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('CREATE INDEX idx_nt ON ',GT_DB,'.tmp_nt_',WORKER_ID,'(enodeb_id, cell_id);');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS ',GT_DB,'.tmp_table_cross_feeder_',WORKER_ID,';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	SET @SqlCmd=CONCAT('
			CREATE TEMPORARY TABLE ',GT_DB,'.tmp_table_cross_feeder_',WORKER_ID,' LIKE ',GT_DB,'.opt_cf_pre_agg_report_lte');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	SET @SqlCmd=CONCAT('
			INSERT INTO ',GT_DB,'.tmp_table_cross_feeder_',WORKER_ID,'
			(`ENODEB_ID`,`CELL_ID`,`POS_MR_ANGLE`,`MR_COUNT`,`AZIMUTH`)
			SELECT 
				A.ENODEB_ID ,
				A.CELL_ID ,
				gt_covmo_angle(B.LONGITUDE,B.LATITUDE,A.posLng,A.posLat) AS pos_angle,
				COUNT(*) AS num ,
				B.AZIMUTH
			FROM ',GT_DB,'.tmp_pos_enb_distinct_',WORKER_ID,' A
			LEFT JOIN ',GT_DB,'.tmp_nt_',WORKER_ID,' B
			ON A.ENODEB_ID=B.ENODEB_ID AND A.CELL_ID=B.CELL_ID
			WHERE A.cause_f1=1 AND A.cause_f2<>1 
			GROUP BY A.ENODEB_ID,A.CELL_ID,pos_angle,B.AZIMUTH;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('
			UPDATE ',GT_DB,'.tmp_table_cross_feeder_',WORKER_ID,'
			SET CAL_AZIMUTH = CASE WHEN (AZIMUTH > 180) THEN (AZIMUTH - 360) ELSE AZIMUTH END;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	SET @SqlCmd=CONCAT('
			UPDATE ',GT_DB,'.tmp_table_cross_feeder_',WORKER_ID,'
			SET CAL_POS_MR_ANGLE = CASE WHEN ((POS_MR_ANGLE - CAL_AZIMUTH) > 180) THEN ((POS_MR_ANGLE - CAL_AZIMUTH) - 360)
						ELSE (POS_MR_ANGLE - CAL_AZIMUTH) END;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	SET @SqlCmd=CONCAT('
			INSERT INTO ',GT_DB,'.opt_cf_pre_agg_report_lte
			(`ENODEB_ID`,`CELL_ID`,`POS_MR_ANGLE`,`MR_COUNT`,`AZIMUTH`,`CAL_AZIMUTH`,`CAL_POS_MR_ANGLE`)
			SELECT 
				`ENODEB_ID`,`CELL_ID`,`POS_MR_ANGLE`,`MR_COUNT`,`AZIMUTH`,`CAL_AZIMUTH`,`CAL_POS_MR_ANGLE` 
			FROM ',GT_DB,'.tmp_table_cross_feeder_',WORKER_ID,';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'SP_Sub_Update_Cross_Feeder_Pre',CONCAT('Done: ',TIMESTAMPDIFF(SECOND,START_TIME,SYSDATE()),' seconds.'), NOW());
	
END$$
DELIMITER ;
