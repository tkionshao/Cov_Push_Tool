DELIMITER $$
USE `gt_gw_main`$$
DROP PROCEDURE IF EXISTS `SP_CreateDB_LTE`$$
CREATE DEFINER=`covmo`@`%` PROCEDURE `SP_Sub_Update_Cross_Feeder`(IN GT_DB VARCHAR(100),IN GT_COVMO VARCHAR(100))
BEGIN
	DECLARE START_TIME DATETIME DEFAULT SYSDATE();
	DECLARE WORKER_ID VARCHAR(10) DEFAULT CONNECTION_ID();
	DECLARE CURRENT_NT_DB VARCHAR(50) DEFAULT CONCAT('gt_nt_',gt_strtok(GT_DB,3,'_'));
	
	CALL SP_Sub_Set_Session_Param_LTE(GT_DB);	
	
	
	
	SET @SqlCmd=CONCAT('SELECT att_value INTO @THRESHOLD_RATIO FROM ',CURRENT_NT_DB,'.sys_config WHERE att_name=''threshold_ratio'' AND group_name=''cf'';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;	
	
	
	INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'SP_Sub_Update_Cross_Feeder','Start ', NOW());
	INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'SP_Sub_Update_Cross_Feeder','S1 ', NOW());
	
	SET @SqlCmd=CONCAT('DROP TABLE IF EXISTS ',GT_DB,'.tmp_pair_',WORKER_ID,';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('CREATE TABLE ',GT_DB,'.tmp_pair_',WORKER_ID,'
			(`enodeb_id` MEDIUMINT(9) DEFAULT NULL,
			`cell_id` MEDIUMINT(9) DEFAULT NULL,
			avg_angle DOUBLE,
			azimuth DOUBLE,
			mr_count  BIGINT(20),
			DL_EARFCN SMALLINT(6) DEFAULT NULL
			)ENGINE=MYISAM;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('INSERT INTO ',GT_DB,'.tmp_pair_',WORKER_ID,'
			(enodeb_id,cell_id,avg_angle,azimuth,mr_count)
			SELECT enodeb_id,cell_id,  MOD(SUM(CAL_POS_MR_ANGLE*MR_COUNT)/SUM(MR_COUNT)+CAL_AZIMUTH+360,360) AS avg_angle, cf.azimuth , SUM(mr_count) AS mr_count
			FROM ',GT_DB,'.opt_cf_pre_agg_report_lte AS cf 
			WHERE cf.enodeb_id IS NOT NULL
			GROUP BY cf.enodeb_id,cf.cell_id;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('UPDATE ',GT_DB,'.tmp_pair_',WORKER_ID,' A, ',CURRENT_NT_DB,'.nt_cell_current_lte B
			SET A.DL_EARFCN = B.DL_EARFCN
			WHERE A.enodeb_id = B.enodeb_id AND A.cell_id = B.cell_id;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('CREATE INDEX idx_enodeb_id2 ON ',GT_DB,'.tmp_pair_',WORKER_ID,'(enodeb_id);');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	SET @SqlCmd=CONCAT('
		INSERT INTO ',GT_DB,'.opt_cf_report_lte 
		(enodeb_id,cell_id_a,cell_id_b,cell_a_azimuth,cell_b_azimuth,cell_a_avg_angle,cell_b_avg_angle,angle_diff,threshold,mr_count)
		SELECT 
			pair_1.enodeb_id AS enodeb_id, 
			pair_1.cell_id AS cell_id_a,
			pair_2.cell_id AS cell_id_b,
			pair_1.azimuth AS cell_a_azimuth,
			pair_2.azimuth AS cell_b_azimuth,
			pair_1.avg_angle AS cell_a_avg_angle,
			pair_2.avg_angle AS cell_b_avg_angle,
			(
				CASE WHEN 
					pair_1.avg_angle>=pair_2.avg_angle
				THEN 
					IF (pair_1.avg_angle-pair_2.avg_angle>=180,360-(pair_1.avg_angle-pair_2.avg_angle),pair_1.avg_angle-pair_2.avg_angle)
				ELSE
					IF (pair_2.avg_angle-pair_1.avg_angle>=180,360-(pair_2.avg_angle-pair_1.avg_angle),pair_2.avg_angle-pair_1.avg_angle)
				END
			)AS angle_diff,
			(
				CASE WHEN 
					pair_1.azimuth>=pair_2.azimuth
				THEN 
					IF (pair_1.azimuth-pair_2.azimuth>=180,(360-(pair_1.azimuth-pair_2.azimuth))*',@THRESHOLD_RATIO,',(pair_1.azimuth-pair_2.azimuth)*',@THRESHOLD_RATIO,')
				ELSE
					IF (pair_2.azimuth-pair_1.azimuth>=180,(360-(pair_2.azimuth-pair_1.azimuth))*',@THRESHOLD_RATIO,',(pair_2.azimuth-pair_1.azimuth)*',@THRESHOLD_RATIO,')
				END
			)AS threshold,
			pair_1.mr_count+pair_2.mr_count AS mr_count
			
		FROM ',GT_DB,'.tmp_pair_',WORKER_ID,' AS pair_1,',GT_DB,'.tmp_pair_',WORKER_ID,' AS pair_2
		WHERE  pair_1.enodeb_id = pair_2.enodeb_id AND pair_1.cell_id < pair_2.cell_id AND pair_1.azimuth != pair_2.azimuth AND pair_1.DL_EARFCN = pair_2.DL_EARFCN;
	');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('UPDATE ',GT_DB,'.opt_cf_report_lte
		SET cross_feeder_flag = IF(angle_diff<threshold,1,0);');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('UPDATE ',GT_DB,'.`opt_cf_report_lte` A, ',CURRENT_NT_DB,'.nt_cell_current_lte B
			SET 
				A.PU_ID = B.PU_ID,
				A.SUB_REGION_ID = B.SUB_REGION_ID
			WHERE A.ENODEB_ID = B.ENODEB_ID AND A.CELL_ID_A = B.CELL_ID;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('DROP TABLE IF EXISTS ',GT_DB,'.tmp_pair_',WORKER_ID,';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'SP_Sub_Update_Cross_Feeder',CONCAT('Done: ',TIMESTAMPDIFF(SECOND,START_TIME,SYSDATE()),' seconds.'), NOW());
	
END$$
DELIMITER ;
