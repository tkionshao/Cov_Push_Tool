DELIMITER $$
USE `gt_gw_main`$$
DROP PROCEDURE IF EXISTS `SP_CreateDB_LTE`$$
CREATE DEFINER=`covmo`@`%` PROCEDURE `SP_Sub_Generate_Overshooting_Severity_LTE`(IN GT_DB VARCHAR(100),IN GT_COVMO VARCHAR(10),KIND VARCHAR(10))
BEGIN	
	DECLARE RNC_ID INT;
 	DECLARE O_GT_DB VARCHAR(100) DEFAULT GT_DB;
	DECLARE SH_EH VARCHAR(9) DEFAULT RIGHT(GT_DB,9);	
	DECLARE START_TIME DATETIME DEFAULT SYSDATE();
	DECLARE STEP_START_TIME DATETIME DEFAULT SYSDATE();
	DECLARE CURRENT_NT_DB VARCHAR(50) DEFAULT CONCAT('gt_nt_',gt_strtok(GT_DB,3,'_'));
	DECLARE v_DATA_DATE VARCHAR(30);
	DECLARE WeekStart CHAR(20);
	DECLARE WeekEnd CHAR(20);
	DECLARE	WEEK_DB VARCHAR(50);
	DECLARE WORKER_ID VARCHAR(10) DEFAULT CONNECTION_ID();
	
	-- DECLARE OVERSHOOT_JUDGE_FLAG VARCHAR(10);
	DECLARE OVERSHOOT_COL_STR VARCHAR(1500) DEFAULT 
		'`T1_CALL_CNT`,
		`T0_CALL_CNT`,
		`AVG_DISTANCE`,
		`MAX_DISTANCE`,
		`BASE_DISTANCE`,
		`T0_TILE_CNT`,
		`T1_TILE_CNT`,
		`W_KPI`,
		`T1_RSRP_TILE_COUNT`';
	
	DECLARE OVERSHOOT_EVENT_COL_STR VARCHAR(1500) DEFAULT 
	'`SERVING_T1_EVENT_CNT`,
	`SERVING_T0_EVENT_CNT`,
	`SERVING_T1_TILE_CNT`,
	`SERVING_T0_TILE_CNT`,
	`BEST_T1_EVENT_CNT`,
	`BEST_T0_EVENT_CNT`,
	`BEST_T1_TILE_CNT`,
	`BEST_T0_TILE_CNT`,
	`SERVING_OVER_DISTANCE_MAX`,
	`SERVING_MAINBEAM_OVER_DISTANCE_MAX`,
	`BEST_OVER_DISTANCE_MAX`,
	`BEST_MAINBEAM_OVER_DISTANCE_MAX`,
	`SERVING_OVER_DISTANCE_AVG`,
	`SERVING_MAINBEAM_OVER_DISTANCE_AVG`,
	`BEST_OVER_DISTANCE_AVG`,
	`BEST_MAINBEAM_OVER_DISTANCE_AVG`';
	
	DECLARE OVERSHOOT_EVENT_COL_FORMULA_STR VARCHAR(5000) DEFAULT 
	'SUM(OVER_LOC_CNT) AS `SERVING_T1_EVENT_CNT`,
	SUM(NON_OVER_LOC_CNT) AS `SERVING_T0_EVENT_CNT`,
	SUM(CASE WHEN OVER_LOC_CNT >0 THEN 1 ELSE 0 END) AS `SERVING_T1_TILE_CNT`,
	SUM(CASE WHEN NON_OVER_LOC_CNT>0 AND OVER_LOC_CNT=0 THEN 1 ELSE 0 END) AS `SERVING_T0_TILE_CNT`,
	SUM(BEST_OVER_LOC_CNT) AS `BEST_T1_EVENT_CNT`,
	SUM(BEST_NON_OVER_LOC_CNT) AS `BEST_T0_EVENT_CNT`,
	SUM(CASE WHEN BEST_OVER_LOC_CNT>0 THEN 1 ELSE 0 END) AS `BEST_T1_TILE_CNT`,
	SUM(CASE WHEN BEST_NON_OVER_LOC_CNT>0 AND BEST_OVER_LOC_CNT=0 THEN 1 ELSE 0 END) AS `BEST_T0_TILE_CNT`,
	MAX( CASE WHEN OVER_LOC_CNT>0 THEN gt_covmo_distance(gt_covmo_proj_hex_geohash_to_lng(TILE_ID, 19) ,gt_covmo_proj_hex_geohash_to_lat(TILE_ID, 19), b.LONGITUDE,b.LATITUDE)
	ELSE NULL END) AS SERVING_OVER_MAX_DISTANCE,
	MAX( CASE WHEN MAINBEAM_OVER_LOC_CNT>0 THEN gt_covmo_distance(gt_covmo_proj_hex_geohash_to_lng(TILE_ID, 19) ,gt_covmo_proj_hex_geohash_to_lat(TILE_ID, 19), b.LONGITUDE,b.LATITUDE) 
	ELSE NULL END) AS SERVING_MAINBEAM_OVER_MAX_DISTANCE,
	MAX( CASE WHEN BEST_OVER_LOC_CNT>0 THEN gt_covmo_distance(gt_covmo_proj_hex_geohash_to_lng(TILE_ID, 19) ,gt_covmo_proj_hex_geohash_to_lat(TILE_ID, 19), b.LONGITUDE,b.LATITUDE) 
	ELSE NULL END) AS BEST_OVER_MAX_DISTANCE,
	MAX( CASE WHEN BEST_MAINBEAM_OVER_LOC_CNT>0 THEN gt_covmo_distance(gt_covmo_proj_hex_geohash_to_lng(TILE_ID, 19) ,gt_covmo_proj_hex_geohash_to_lat(TILE_ID, 19), b.LONGITUDE,b.LATITUDE) 
	ELSE NULL END) AS BEST_MAINBEAM_OVER_MAX_DISTANCE,
	SUM(CASE WHEN OVER_LOC_CNT>0 THEN gt_covmo_distance(gt_covmo_proj_hex_geohash_to_lng(TILE_ID, 19) ,gt_covmo_proj_hex_geohash_to_lat(TILE_ID, 19), b.LONGITUDE,b.LATITUDE)
	ELSE NULL END)/SUM( CASE WHEN OVER_LOC_CNT>0 THEN 1 ELSE NULL END) AS  SERVING_OVER_DISTANCE_AVG,
	SUM(CASE WHEN MAINBEAM_OVER_LOC_CNT>0 THEN gt_covmo_distance(gt_covmo_proj_hex_geohash_to_lng(TILE_ID, 19) ,gt_covmo_proj_hex_geohash_to_lat(TILE_ID, 19), b.LONGITUDE,b.LATITUDE) 
	ELSE NULL END) /SUM( CASE WHEN MAINBEAM_OVER_LOC_CNT>0 THEN 1 ELSE NULL END) AS SERVING_MAINBEAM_OVER_DISTANCE_AVG,
	SUM(CASE WHEN BEST_OVER_LOC_CNT>0 THEN gt_covmo_distance(gt_covmo_proj_hex_geohash_to_lng(TILE_ID, 19) ,gt_covmo_proj_hex_geohash_to_lat(TILE_ID, 19), b.LONGITUDE,b.LATITUDE) 
	ELSE NULL END)/SUM(CASE WHEN BEST_OVER_LOC_CNT>0 THEN 1 ELSE NULL END) AS BEST_OVER_DISTANCE_AVG,
	SUM(CASE WHEN BEST_MAINBEAM_OVER_LOC_CNT>0 THEN gt_covmo_distance(gt_covmo_proj_hex_geohash_to_lng(TILE_ID, 19) ,gt_covmo_proj_hex_geohash_to_lat(TILE_ID, 19), b.LONGITUDE,b.LATITUDE) 
	ELSE NULL END)/ SUM(CASE WHEN BEST_MAINBEAM_OVER_LOC_CNT>0 THEN 1 ELSE NULL END)  AS BEST_MAINBEAM_OVER_DISTANCE_AVG';
 
	DECLARE CELL_TILE_DY_DEF_KEY_COL_STR VARCHAR(1000) DEFAULT 
		'
		DATA_DATE ,
		SUB_REGION_ID,
		EUTRABAND,
		EARFCN,
		TILE_ID,
		ENODEB_ID,
		CELL_ID,
		CELL_NAME';
	
	
	DECLARE START_COL_STR VARCHAR(500) DEFAULT 
		'
		OVER_LOC_CALL_CNT,
		NONOVER_LOC_CALL_CNT ,
		TILE_OVER_FLAG,
		OVER_DISTANCE_SUM,	
		OVER_DISTANCE_MAX';
	
	DECLARE POS_COL_STR VARCHAR(1000) DEFAULT 
		'`OVER_LOC_CNT`,
		`NON_OVER_LOC_CNT`,
		`MAINBEAM_OVER_LOC_CNT` ,
		`MAINBEAM_NON_OVER_LOC_CNT`,
		`BEST_OVER_LOC_CNT`,
		`BEST_NON_OVER_LOC_CNT`,
		`BEST_MAINBEAM_OVER_LOC_CNT`, 
		`BEST_MAINBEAM_NON_OVER_LOC_CNT`,
		`TILE_MAINBEAM`';
	
	
	DECLARE OVERSHOOT_MAP_EVENT_COL_STR VARCHAR(5000) DEFAULT 
		'OVER_LOC_CNT,
		NON_OVER_LOC_CNT,
		MAINBEAM_OVER_LOC_CNT,
		MAINBEAM_NON_OVER_LOC_CNT,
		BEST_OVER_LOC_CNT,
		BEST_NON_OVER_LOC_CNT,
		BEST_MAINBEAM_OVER_LOC_CNT,
		BEST_MAINBEAM_NON_OVER_LOC_CNT,
		TILE_MAINBEAM,
		MR_4G_RSRP_BEST_CNT,
		MR_4G_RSRP_BEST_SUM,
		MR_4G_RSRQ_BEST_CNT,
		MR_4G_RSRQ_BEST_SUM';
	
	
	DECLARE OVERSHOOT_MAP_CALL_COL_STR VARCHAR(2000) DEFAULT 
		'POS_FIRST_S_RSRP,
		POS_FIRST_S_RSRP_CNT,
		OVER_LOC_CALL_CNT,
		NONOVER_LOC_CALL_CNT,
		TILE_OVER_FLAG,
		OVER_DISTANCE_SUM,
		OVER_DISTANCE_MAX,
		POS_FIRST_S_RSRQ,
		POS_FIRST_S_RSRQ_CNT';
	
	SET @SqlCmd=CONCAT('SELECT att_value INTO @SYS_CONFIG_TILE FROM ',CURRENT_NT_DB,'.`sys_config`
						WHERE group_name=''system'' AND att_name=''MapResolution'';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;	
	
	IF gt_covmo_csv_count(@SYS_CONFIG_TILE,',') =3 THEN
	
		SET @ZOOM_LEVEL = gt_covmo_csv_get(@SYS_CONFIG_TILE,3);		
	ELSE
		SET @ZOOM_LEVEL = 19;
	END IF;
	
	
	INSERT INTO gt_gw_main.SP_LOG VALUES(O_GT_DB,'SP_SUB_GENERATE_OVERSHOOTING_SEVERITY_LTE','Call base Start', START_TIME);
	SET STEP_START_TIME := SYSDATE();
	SELECT gt_strtok(GT_DB,2,'_') INTO RNC_ID;
	SET @SQLCMD =CONCAT('DROP TEMPORARY TABLE IF EXISTS ',GT_DB,'.tmp_overshooting_severity_lte;');
	PREPARE STMT FROM @SQLCMD;
	EXECUTE STMT;
	DEALLOCATE PREPARE STMT; 
	
	
	SET @SQLCMD =CONCAT(' SELECT LOWER(att_value) INTO  @OVERSHOOT_JUDGE_FLAG
				FROM ',CURRENT_NT_DB,'.`sys_config` 
				WHERE att_name=''OvershootTriggerMethod'';');
	PREPARE STMT FROM @SQLCMD;
	EXECUTE STMT;
	DEALLOCATE PREPARE STMT;
	
		
	IF KIND IN ('DAILY','WEEK')
	
	THEN 
	
	SET @SqlCmd=CONCAT('TRUNCATE TABLE  ',GT_DB,'.`rpt_cell_tile_event_overshoot_d1`');	
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	
	SET @SqlCmd=CONCAT('TRUNCATE TABLE  ',GT_DB,'.`rpt_cell_tile_call_overshoot_d1`');	
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	
	SET @SqlCmd=CONCAT('TRUNCATE TABLE  ',GT_DB,'.`table_overshooting_severity_event_lte_d1`');	
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	
	SET @SqlCmd=CONCAT('TRUNCATE TABLE  ',GT_DB,'.`table_overshooting_severity_lte_d1`');	
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	
	
	SET @SQLCMD =CONCAT(' CREATE TEMPORARY TABLE ',GT_DB,'.tmp_overshooting_severity_lte (
				  `DATA_DATE` DATE DEFAULT NULL,
				  `EUTRABAND` SMALLINT(6) DEFAULT NULL,
				  `EARFCN` MEDIUMINT(9) DEFAULT NULL,
				  `ENODEB_NAME` VARCHAR(64) CHARACTER SET utf8 DEFAULT NULL,
				  `ENODEB_ID` MEDIUMINT(9) DEFAULT NULL,
				  `CELL_NAME` VARCHAR(50) CHARACTER SET utf8 DEFAULT NULL,
				  `CELL_ID` TINYINT(4) DEFAULT NULL,
				  `PU_ID` MEDIUMINT(9) DEFAULT NULL,
				  `SUB_REGION_ID` MEDIUMINT(9) DEFAULT NULL,
				  `T1_CALL_CNT` DOUBLE DEFAULT NULL,
				  `T0_CALL_CNT` DOUBLE DEFAULT NULL,
				  `AVG_DISTANCE` DOUBLE DEFAULT NULL,
				  `MAX_DISTANCE` DOUBLE DEFAULT NULL,
				  `BASE_DISTANCE` DOUBLE DEFAULT NULL,
				  `T0_TILE_CNT` BIGINT(20) DEFAULT NULL,
				  `T1_TILE_CNT` BIGINT(20) DEFAULT NULL,
				  `W_KPI` DOUBLE DEFAULT NULL,
				  `T1_RSRP_TILE_COUNT` BIGINT(20) DEFAULT NULL,
				  KEY `IX_CELL` (`ENODEB_ID`,`CELL_ID`)
				) ENGINE=MYISAM DEFAULT CHARSET=latin1 DELAY_KEY_WRITE=1;');
	PREPARE STMT FROM @SQLCMD;
	EXECUTE STMT;
	DEALLOCATE PREPARE STMT; 
	
	SET @SqlCmd=CONCAT('INSERT INTO ',GT_DB,'.tmp_overshooting_severity_lte(
				  `DATA_DATE`,
				  `EUTRABAND`,
				  `EARFCN`,
				  `ENODEB_NAME`,
				  `ENODEB_ID`,
				  `CELL_NAME`,
				  `CELL_ID`,
				  `PU_ID`,
				  `SUB_REGION_ID`,
				 ',OVERSHOOT_COL_STR,')	
			SELECT 
				  `DATA_DATE`,
				  `EUTRABAND`,
				  `EARFCN`,
				   NULL AS `ENODEB_NAME`,
				  `ENODEB_ID`,
				  `CELL_NAME`,
				  `CELL_ID`,
				   NULL AS `PU_ID`,
				  `SUB_REGION_ID`,
				   SUM(`OVER_LOC_CALL_CNT`) AS T1_CALL_CNT,
				   SUM(`NONOVER_LOC_CALL_CNT`) AS T0_CALL_CNT,
				  SUM(`OVER_DISTANCE_SUM`)/SUM(`OVER_LOC_CALL_CNT`) AS 	AVG_DISTANCE,
				MAX(OVER_DISTANCE_MAX) AS MAX_DISTANCE,
				NULL AS BASE_DISTANCE,
				SUM(CASE WHEN TILE_OVER_FLAG is NOT NULL THEN 1 ELSE 0 END) AS T0_TILE_CNT,
				SUM(TILE_OVER_FLAG) AS T1_TILE_CNT,
				NULL AS W_KPI,
				NULL AS T1_RSRP_TILE_COUNT
				FROM ',GT_DB,'.',CASE WHEN  KIND='DAILY' THEN 'rpt_cell_tile_start_dy_def'   WHEN  KIND='WEEK' THEN 'rpt_cell_tile_start_wk' END,'
 				GROUP BY `DATA_DATE`,`EUTRABAND`,`EARFCN`,`ENODEB_ID`,`CELL_ID`,`SUB_REGION_ID`
				ORDER BY NULL;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt; 
	
	SET @SQLCMD =CONCAT('DROP TEMPORARY TABLE IF EXISTS ',GT_DB,'.TMP_NT_CURRENT;');
	PREPARE STMT FROM @SQLCMD;
	EXECUTE STMT;
	DEALLOCATE PREPARE STMT; 
	
	SET @SQLCMD =CONCAT(' CREATE TEMPORARY TABLE ',	GT_DB,'.TMP_NT_CURRENT ENGINE=MYISAM
				SELECT A.`ENODEB_ID`,A.`CELL_ID`,A.`ENODEB_NAME`,A.PU_ID,B.PATHLOSS_DISTANCE * (1.5-A.SITE_DENSITY_TYPE/10) AS BASE_DISTANCE
					,B.ANTENNA_RADIUS,B.CLOSED_RADIUS
				FROM ',CURRENT_NT_DB,'.`nt_cell_current_lte` A
				LEFT JOIN ',CURRENT_NT_DB,'.`nt_antenna_current_lte` B
				ON A.`ENODEB_ID`=B.`ENODEB_ID` AND A.`CELL_ID`=B.`CELL_ID` AND A.INDOOR = 0;');
	PREPARE STMT FROM @SQLCMD;
	EXECUTE STMT;
	DEALLOCATE PREPARE STMT;	
	
	SET @SqlCmd=CONCAT('UPDATE ',GT_DB,'.`tmp_overshooting_severity_lte` A, ',GT_DB,'.TMP_NT_CURRENT B
				SET 
					A.PU_ID = B.PU_ID,
					A.ENODEB_NAME  = B.ENODEB_NAME,
					A.BASE_DISTANCE  = B.BASE_DISTANCE
				WHERE A.ENODEB_ID = B.ENODEB_ID AND A.CELL_ID = B.CELL_ID ;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('INSERT INTO ',GT_DB,'.',CASE WHEN  KIND='DAILY' THEN 'table_overshooting_severity_lte_d1' WHEN  KIND='WEEK' THEN 'table_overshooting_severity_lte_wk' END,'(
				  `DATA_DATE`,
				  `EUTRABAND`,
				  `EARFCN`,
				  `ENODEB_NAME`,
				  `ENODEB_ID`,
				  `CELL_NAME`,
				  `CELL_ID`,
				  `PU_ID`,
				  `SUB_REGION_ID`,
				  ',OVERSHOOT_COL_STR,')	
			SELECT 
				  `DATA_DATE`,
				  `EUTRABAND`,
				  `EARFCN`,
				  `ENODEB_NAME`,
				  `ENODEB_ID`,
				  `CELL_NAME`,
				  `CELL_ID`,
				  `PU_ID`,
				  `SUB_REGION_ID`,
				  ',OVERSHOOT_COL_STR,'
				FROM ',GT_DB,'.tmp_overshooting_severity_lte;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	
	SET @SqlCmd=CONCAT('INSERT INTO ',GT_DB,'.rpt_cell_tile_event_overshoot_d1(
				  ',CELL_TILE_DY_DEF_KEY_COL_STR,'
				,',OVERSHOOT_MAP_EVENT_COL_STR,')	
			SELECT  ',CELL_TILE_DY_DEF_KEY_COL_STR,'
				,',OVERSHOOT_MAP_EVENT_COL_STR,'
				FROM ',GT_DB,'.rpt_cell_tile_position_dy_def
			WHERE 
			(OVER_LOC_CNT >0 OR MAINBEAM_OVER_LOC_CNT>0 OR BEST_OVER_LOC_CNT>0 OR BEST_MAINBEAM_OVER_LOC_CNT>0);');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	
	SET @SqlCmd=CONCAT('INSERT INTO ',GT_DB,'.rpt_cell_tile_call_overshoot_d1(
				  ',CELL_TILE_DY_DEF_KEY_COL_STR,'
				,',OVERSHOOT_MAP_CALL_COL_STR,')	
			SELECT  ',CELL_TILE_DY_DEF_KEY_COL_STR,'
				,',OVERSHOOT_MAP_CALL_COL_STR,'
				FROM ',GT_DB,'.rpt_cell_tile_start_dy_def 
			WHERE 
				OVER_LOC_CALL_CNT>0 ;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
					
	INSERT INTO gt_gw_main.SP_LOG VALUES(O_GT_DB,'SP_SUB_GENERATE_OVERSHOOTING_SEVERITY_LTE',CONCAT('call base Done cost: ',TIMESTAMPDIFF(SECOND,STEP_START_TIME,SYSDATE()),' sec.'), NOW());
				
	INSERT INTO gt_gw_main.SP_LOG VALUES(O_GT_DB,'SP_SUB_GENERATE_OVERSHOOTING_SEVERITY_LTE','Event base Start', SYSDATE());
	SET STEP_START_TIME := SYSDATE();
		
	SET @SQLCMD =CONCAT('truncate table ',GT_DB,'.',CASE WHEN  KIND='DAILY' THEN 'table_overshooting_severity_event_lte' WHEN  KIND='WEEK' THEN 'table_overshooting_severity_event_lte_wk' END,';');
	PREPARE STMT FROM @SQLCMD;
	EXECUTE STMT;
	DEALLOCATE PREPARE STMT; 
	
	SET @SQLCMD =CONCAT('DROP TEMPORARY TABLE IF EXISTS ',GT_DB,'.tmp_overshooting_severity_event_lte;');
	PREPARE STMT FROM @SQLCMD;
	EXECUTE STMT;
	DEALLOCATE PREPARE STMT; 
	
	SET @SQLCMD =CONCAT(' CREATE TEMPORARY TABLE ',GT_DB,'.tmp_overshooting_severity_event_lte (
					`DATA_DATE` DATE DEFAULT NULL,
					`EUTRABAND` SMALLINT(6) DEFAULT NULL,
					`EARFCN` MEDIUMINT(9) DEFAULT NULL,
					`ENODEB_NAME` VARCHAR(64) CHARACTER SET utf8 DEFAULT NULL,
					`ENODEB_ID` MEDIUMINT(9) DEFAULT NULL,
					`CELL_NAME` VARCHAR(50) CHARACTER SET utf8 DEFAULT NULL,
					`CELL_ID` TINYINT(4) DEFAULT NULL,
					`PU_ID` MEDIUMINT(9) DEFAULT NULL,
					`SUB_REGION_ID` MEDIUMINT(9) DEFAULT NULL,
					`ANTENNA_RADIUS` FLOAT DEFAULT NULL,
					`CLOSED_RADIUS` FLOAT DEFAULT NULL,
					`SERVING_T1_EVENT_CNT` MEDIUMINT(9) DEFAULT NULL,
					`SERVING_T0_EVENT_CNT` MEDIUMINT(9) DEFAULT NULL,
					`SERVING_T1_TILE_CNT` MEDIUMINT(9) DEFAULT NULL,
					`SERVING_T0_TILE_CNT` MEDIUMINT(9) DEFAULT NULL,
					`BEST_T1_EVENT_CNT` MEDIUMINT(9) DEFAULT NULL,
					`BEST_T0_EVENT_CNT` MEDIUMINT(9) DEFAULT NULL,
					`BEST_T1_TILE_CNT` MEDIUMINT(9) DEFAULT NULL,
					`BEST_T0_TILE_CNT` MEDIUMINT(9) DEFAULT NULL,
					`SERVING_OVER_DISTANCE_MAX` float DEFAULT NULL,
					`SERVING_MAINBEAM_OVER_DISTANCE_MAX`  float DEFAULT NULL,
					`BEST_OVER_DISTANCE_MAX`  float DEFAULT NULL,
					`BEST_MAINBEAM_OVER_DISTANCE_MAX`  float DEFAULT NULL,
					`SERVING_OVER_DISTANCE_AVG`  float DEFAULT NULL,
					`SERVING_MAINBEAM_OVER_DISTANCE_AVG`  float DEFAULT NULL,
					`BEST_OVER_DISTANCE_AVG`  float DEFAULT NULL,
					`BEST_MAINBEAM_OVER_DISTANCE_AVG`  float DEFAULT NULL,
					KEY IX_CELL (`ENODEB_ID`,`CELL_ID`)
				) ENGINE=MYISAM DEFAULT CHARSET=latin1 DELAY_KEY_WRITE=1;');
	PREPARE STMT FROM @SQLCMD;
	EXECUTE STMT;
	DEALLOCATE PREPARE STMT; 
	
	SET @SqlCmd=CONCAT('INSERT INTO ',GT_DB,'.tmp_overshooting_severity_event_lte(
				  `DATA_DATE`,
				  `EUTRABAND`,
				  `EARFCN`,
				  `ENODEB_NAME`,
				  `ENODEB_ID`,
				  `CELL_NAME`,
				  `CELL_ID`,
				  `PU_ID`,
				  `SUB_REGION_ID`,
				 ',OVERSHOOT_EVENT_COL_STR,')	
			SELECT 
				  `DATA_DATE`,
				  `EUTRABAND`,
				  `EARFCN`,
				   NULL AS `ENODEB_NAME`,
				  a.`ENODEB_ID`,
				  `CELL_NAME`,
				  a.`CELL_ID`,
				   NULL AS `PU_ID`,
				  `SUB_REGION_ID`,
					SUM(OVER_LOC_CNT) AS `SERVING_T1_EVENT_CNT`,
					SUM(NON_OVER_LOC_CNT) AS `SERVING_T0_EVENT_CNT`,
					SUM(CASE WHEN OVER_LOC_CNT >0 THEN 1 ELSE 0 END) AS `SERVING_T1_TILE_CNT`,
					SUM(CASE WHEN NON_OVER_LOC_CNT>0 AND OVER_LOC_CNT=0 THEN 1 ELSE 0 END) AS `SERVING_T0_TILE_CNT`,
					SUM(BEST_OVER_LOC_CNT) AS `BEST_T1_EVENT_CNT`,
					SUM(BEST_NON_OVER_LOC_CNT) AS `BEST_T0_EVENT_CNT`,
					SUM(CASE WHEN BEST_OVER_LOC_CNT>0 THEN 1 ELSE 0 END) AS `BEST_T1_TILE_CNT`,
					SUM(CASE WHEN BEST_NON_OVER_LOC_CNT>0 AND BEST_OVER_LOC_CNT=0 THEN 1 ELSE 0 END) AS `BEST_T0_TILE_CNT`,
					MAX( CASE WHEN OVER_LOC_CNT>0 THEN gt_covmo_distance(gt_covmo_proj_hex_geohash_to_lng(TILE_ID, ',@ZOOM_LEVEL,') ,gt_covmo_proj_hex_geohash_to_lat(TILE_ID, ',@ZOOM_LEVEL,'), b.LONGITUDE,b.LATITUDE)
					ELSE NULL END) AS SERVING_OVER_MAX_DISTANCE,
					MAX( CASE WHEN MAINBEAM_OVER_LOC_CNT>0 THEN gt_covmo_distance(gt_covmo_proj_hex_geohash_to_lng(TILE_ID, ',@ZOOM_LEVEL,') ,gt_covmo_proj_hex_geohash_to_lat(TILE_ID,',@ZOOM_LEVEL,'), b.LONGITUDE,b.LATITUDE) 
					ELSE NULL END) AS SERVING_MAINBEAM_OVER_MAX_DISTANCE,
					MAX( CASE WHEN BEST_OVER_LOC_CNT>0 THEN gt_covmo_distance(gt_covmo_proj_hex_geohash_to_lng(TILE_ID, ',@ZOOM_LEVEL,') ,gt_covmo_proj_hex_geohash_to_lat(TILE_ID, ',@ZOOM_LEVEL,'), b.LONGITUDE,b.LATITUDE) 
					ELSE NULL END) AS BEST_OVER_MAX_DISTANCE,
					MAX( CASE WHEN BEST_MAINBEAM_OVER_LOC_CNT>0 THEN gt_covmo_distance(gt_covmo_proj_hex_geohash_to_lng(TILE_ID, ',@ZOOM_LEVEL,') ,gt_covmo_proj_hex_geohash_to_lat(TILE_ID, ',@ZOOM_LEVEL,'), b.LONGITUDE,b.LATITUDE) 
					ELSE NULL END) AS BEST_MAINBEAM_OVER_MAX_DISTANCE,
					SUM(CASE WHEN OVER_LOC_CNT>0 THEN gt_covmo_distance(gt_covmo_proj_hex_geohash_to_lng(TILE_ID, ',@ZOOM_LEVEL,') ,gt_covmo_proj_hex_geohash_to_lat(TILE_ID, ',@ZOOM_LEVEL,'), b.LONGITUDE,b.LATITUDE)
					ELSE NULL END)/SUM( CASE WHEN OVER_LOC_CNT>0 THEN 1 ELSE NULL END) AS  SERVING_OVER_DISTANCE_AVG,
					SUM(CASE WHEN MAINBEAM_OVER_LOC_CNT>0 THEN gt_covmo_distance(gt_covmo_proj_hex_geohash_to_lng(TILE_ID, ',@ZOOM_LEVEL,') ,gt_covmo_proj_hex_geohash_to_lat(TILE_ID, ',@ZOOM_LEVEL,'), b.LONGITUDE,b.LATITUDE) 
					ELSE NULL END) /SUM( CASE WHEN MAINBEAM_OVER_LOC_CNT>0 THEN 1 ELSE NULL END) AS SERVING_MAINBEAM_OVER_DISTANCE_AVG,
					SUM(CASE WHEN BEST_OVER_LOC_CNT>0 THEN gt_covmo_distance(gt_covmo_proj_hex_geohash_to_lng(TILE_ID, ',@ZOOM_LEVEL,') ,gt_covmo_proj_hex_geohash_to_lat(TILE_ID, ',@ZOOM_LEVEL,'), b.LONGITUDE,b.LATITUDE) 
					ELSE NULL END)/SUM(CASE WHEN BEST_OVER_LOC_CNT>0 THEN 1 ELSE NULL END) AS BEST_OVER_DISTANCE_AVG,
					SUM(CASE WHEN BEST_MAINBEAM_OVER_LOC_CNT>0 THEN gt_covmo_distance(gt_covmo_proj_hex_geohash_to_lng(TILE_ID, ',@ZOOM_LEVEL,') ,gt_covmo_proj_hex_geohash_to_lat(TILE_ID, ',@ZOOM_LEVEL,'), b.LONGITUDE,b.LATITUDE) 
					ELSE NULL END)/ SUM(CASE WHEN BEST_MAINBEAM_OVER_LOC_CNT>0 THEN 1 ELSE NULL END)  AS BEST_MAINBEAM_OVER_DISTANCE_AVG
				FROM ',GT_DB,'.',CASE WHEN  KIND='DAILY' THEN 'rpt_cell_tile_position_dy_def a'   
						WHEN  KIND='WEEK' THEN 'rpt_cell_tile_position_wk_def a' END,'
				LEFT JOIN  ',CURRENT_NT_DB,'.nt_antenna_current_lte b
				ON a.ENODEB_ID = b.ENODEB_ID AND a.CELL_ID = b.CELL_ID
				GROUP BY `DATA_DATE`,`EUTRABAND`,`EARFCN`,`ENODEB_ID`,`CELL_ID`,`SUB_REGION_ID`
				ORDER BY NULL;');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('UPDATE ',GT_DB,'.`tmp_overshooting_severity_event_lte` A, ',GT_DB,'.TMP_NT_CURRENT B
			SET 
				A.PU_ID = B.PU_ID,
				A.ENODEB_NAME  = B.ENODEB_NAME,
				A.ANTENNA_RADIUS  = B.ANTENNA_RADIUS,
				A.CLOSED_RADIUS  = B.CLOSED_RADIUS
			WHERE A.ENODEB_ID = B.ENODEB_ID AND A.CELL_ID = B.CELL_ID ;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('INSERT INTO ',GT_DB,'.',CASE WHEN  KIND='DAILY' THEN 'table_overshooting_severity_event_lte_d1' WHEN  KIND='WEEK' THEN 'table_overshooting_severity_event_lte_wk' END,'(
				  `DATA_DATE`,
				  `EUTRABAND`,
				  `EARFCN`,
				  `ENODEB_NAME`,
				  `ENODEB_ID`,
				  `CELL_NAME`,
				  `CELL_ID`,
				  `PU_ID`,
				  `SUB_REGION_ID`,
				  `ANTENNA_RADIUS`,
				  `CLOSED_RADIUS`,
				  ',OVERSHOOT_EVENT_COL_STR,'
				)	
			SELECT 
				  `DATA_DATE`,
				  `EUTRABAND`,
				  `EARFCN`,
				  `ENODEB_NAME`,
				  `ENODEB_ID`,
				  `CELL_NAME`,
				  `CELL_ID`,
				  `PU_ID`,
				  `SUB_REGION_ID`,
				  `ANTENNA_RADIUS`,
				  `CLOSED_RADIUS`,
				  ',OVERSHOOT_EVENT_COL_STR,'
				FROM ',GT_DB,'.tmp_overshooting_severity_event_lte;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt; 
	
		
		IF  @OVERSHOOT_JUDGE_FLAG = 'CELL_RADIUS'
		THEN 
		
		SET @SqlCmd=CONCAT('UPDATE ',GT_DB,'.`table_overshooting_severity_event_lte_d1` A, ',CURRENT_NT_DB,'.nt_cell_current_lte B
				SET 
					A.ANTENNA_RADIUS  = B.CELL_RADIUS,
					A.CLOSED_RADIUS  = B.CELL_RADIUS
				WHERE A.ENODEB_ID = B.ENODEB_ID AND A.CELL_ID = B.CELL_ID ;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		
		END IF;	
		
	ELSEIF KIND IN ('D7')
	
		THEN
	
	
		SET @SqlCmd=CONCAT('TRUNCATE TABLE  ',GT_DB,'.`rpt_cell_tile_event_overshoot_d7`');	
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		
		
		SET @SqlCmd=CONCAT('TRUNCATE TABLE  ',GT_DB,'.`rpt_cell_tile_call_overshoot_d7`');	
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		
		
		SET @SqlCmd=CONCAT('TRUNCATE TABLE  ',GT_DB,'.`table_overshooting_severity_event_lte_d7`');	
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		
		
		SET @SqlCmd=CONCAT('TRUNCATE TABLE  ',GT_DB,'.`table_overshooting_severity_lte_d7`');	
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
	
	
		SET @FILE_STARTTIME=CONCAT(SUBSTRING(gt_strtok(GT_DB,3,'_'),1,4),'-',SUBSTRING(gt_strtok(GT_DB,3,'_'),5,2),'-',SUBSTRING(gt_strtok(GT_DB,3,'_'),7,2),' ',SUBSTRING(gt_strtok(GT_DB,4,'_'),1,2),':',SUBSTRING(gt_strtok(GT_DB,4,'_'),3,2),':00');
		SET @rnc= gt_strtok(GT_DB,2,'_');	
			
			
		SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS  ',GT_DB,'.`tmp_rpt_cell_tile_start_',WORKER_ID,'`;'); 
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
			
	
	
		SET @SqlCmd=CONCAT('CREATE TEMPORARY  TABLE ',GT_DB,'.`tmp_rpt_cell_tile_start_',WORKER_ID,'` like ',GT_DB,'.rpt_cell_tile_start_dy_def;');	
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
	
	
		SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS  ',GT_DB,'.`tmp_rpt_cell_tile_position_',WORKER_ID,'`;'); 
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
			
	
	
		SET @SqlCmd=CONCAT('CREATE TEMPORARY  TABLE ',GT_DB,'.`tmp_rpt_cell_tile_position_',WORKER_ID,'` like ',GT_DB,'.rpt_cell_tile_position_dy_def;');	
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
			
			
		SET @SqlCmd=CONCAT('SELECT GROUP_CONCAT(session_DB SEPARATOR ''|'') into @session_db_str FROM gt_gw_main.session_information A  RIGHT JOIN information_schema.TABLES B
			ON A.session_db = B.table_schema
			WHERE A.file_starttime <= ''',@FILE_STARTTIME,'''  AND A.file_starttime >=  DATE_SUB(''',@FILE_STARTTIME,''',INTERVAL 7 DAY )
			AND A.rnc = ''',@rnc,''' AND B.table_name = ''table_overshooting_severity_lte'';');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		
			
		SET @v_i=1;
		SET @v_R_Max=gt_covmo_csv_count(@session_db_str,'|');
			WHILE @v_i <= @v_R_Max DO
			BEGIN
			SET @v_i_minus=@v_i-1;
			SET @session_DB=SUBSTRING(SUBSTRING_INDEX(@session_db_str,'|',@v_i),LENGTH(SUBSTRING_INDEX(@session_db_str,'|',@v_i_minus))+ 1);
			SET @session_DB=REPLACE(@session_DB,'|','');
			
			
			SET @SqlCmd=CONCAT('SELECT COUNT(*)  into @db_exists FROM information_schema.SCHEMATA WHERE SCHEMA_NAME = ''',@session_DB,''';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
			
				IF  @db_exists = 0
				
					THEN
						SELECT 1;
					ELSE
					
					SET @SqlCmd=CONCAT('INSERT INTO ',GT_DB,'.`tmp_rpt_cell_tile_start_',WORKER_ID,'`
					(',CELL_TILE_DY_DEF_KEY_COL_STR,',',START_COL_STR,')
					SELECT 
							',CELL_TILE_DY_DEF_KEY_COL_STR,',
							',START_COL_STR,'
					FROM ',@session_DB,'.rpt_cell_tile_start_dy_def
					ON DUPLICATE KEY UPDATE 
					`tmp_rpt_cell_tile_start_',WORKER_ID,'`.OVER_LOC_CALL_CNT=`tmp_rpt_cell_tile_start_',WORKER_ID,'`.OVER_LOC_CALL_CNT+VALUES(OVER_LOC_CALL_CNT),
					`tmp_rpt_cell_tile_start_',WORKER_ID,'`.NONOVER_LOC_CALL_CNT=`tmp_rpt_cell_tile_start_',WORKER_ID,'`.NONOVER_LOC_CALL_CNT+VALUES(NONOVER_LOC_CALL_CNT),
					`tmp_rpt_cell_tile_start_',WORKER_ID,'`.TILE_OVER_FLAG=CASE WHEN  VALUES(TILE_OVER_FLAG) = 1  OR `tmp_rpt_cell_tile_start_',WORKER_ID,'`.TILE_OVER_FLAG = 1  THEN 1 ELSE 0 END,
					`tmp_rpt_cell_tile_start_',WORKER_ID,'`.OVER_DISTANCE_SUM=`tmp_rpt_cell_tile_start_',WORKER_ID,'`.OVER_DISTANCE_SUM+VALUES(OVER_DISTANCE_SUM),
					`tmp_rpt_cell_tile_start_',WORKER_ID,'`.OVER_DISTANCE_MAX=CASE WHEN  VALUES(OVER_DISTANCE_MAX) > `tmp_rpt_cell_tile_start_',WORKER_ID,'`.OVER_DISTANCE_MAX THEN VALUES(OVER_DISTANCE_MAX)
					ELSE `tmp_rpt_cell_tile_start_',WORKER_ID,'`.OVER_DISTANCE_MAX END
					;');
					PREPARE Stmt FROM @SqlCmd;
					EXECUTE Stmt;
					DEALLOCATE PREPARE Stmt; 
	
	
					SET @SqlCmd=CONCAT('INSERT INTO ',GT_DB,'.`tmp_rpt_cell_tile_position_',WORKER_ID,'`
					(',CELL_TILE_DY_DEF_KEY_COL_STR,',',POS_COL_STR,')
					SELECT 
							',CELL_TILE_DY_DEF_KEY_COL_STR,',
							',POS_COL_STR,'
					FROM ',@session_DB,'.rpt_cell_tile_position_dy_def
					ON DUPLICATE KEY UPDATE 
					`tmp_rpt_cell_tile_position_',WORKER_ID,'`.OVER_LOC_CNT=`tmp_rpt_cell_tile_position_',WORKER_ID,'`.OVER_LOC_CNT+VALUES(OVER_LOC_CNT),
					`tmp_rpt_cell_tile_position_',WORKER_ID,'`.NON_OVER_LOC_CNT=`tmp_rpt_cell_tile_position_',WORKER_ID,'`.NON_OVER_LOC_CNT+VALUES(NON_OVER_LOC_CNT),
					`tmp_rpt_cell_tile_position_',WORKER_ID,'`.MAINBEAM_OVER_LOC_CNT=`tmp_rpt_cell_tile_position_',WORKER_ID,'`.MAINBEAM_OVER_LOC_CNT+VALUES(MAINBEAM_OVER_LOC_CNT),
					`tmp_rpt_cell_tile_position_',WORKER_ID,'`.MAINBEAM_NON_OVER_LOC_CNT=`tmp_rpt_cell_tile_position_',WORKER_ID,'`.MAINBEAM_NON_OVER_LOC_CNT+VALUES(MAINBEAM_NON_OVER_LOC_CNT),
					`tmp_rpt_cell_tile_position_',WORKER_ID,'`.BEST_OVER_LOC_CNT=`tmp_rpt_cell_tile_position_',WORKER_ID,'`.BEST_OVER_LOC_CNT+VALUES(BEST_OVER_LOC_CNT),
					`tmp_rpt_cell_tile_position_',WORKER_ID,'`.BEST_NON_OVER_LOC_CNT=`tmp_rpt_cell_tile_position_',WORKER_ID,'`.BEST_NON_OVER_LOC_CNT+VALUES(BEST_NON_OVER_LOC_CNT),
					`tmp_rpt_cell_tile_position_',WORKER_ID,'`.BEST_MAINBEAM_OVER_LOC_CNT=`tmp_rpt_cell_tile_position_',WORKER_ID,'`.BEST_MAINBEAM_OVER_LOC_CNT+VALUES(BEST_MAINBEAM_OVER_LOC_CNT),
					`tmp_rpt_cell_tile_position_',WORKER_ID,'`.TILE_MAINBEAM=CASE WHEN  VALUES(TILE_MAINBEAM) = 1  OR `tmp_rpt_cell_tile_position_',WORKER_ID,'`.TILE_MAINBEAM = 1  THEN 1 ELSE 0 END;');	
					PREPARE Stmt FROM @SqlCmd;
					EXECUTE Stmt;
					DEALLOCATE PREPARE Stmt; 
	
					SET @SqlCmd=CONCAT('CREATE  TABLE  IF NOT EXISTS ',@session_DB,'.`rpt_cell_tile_event_overshoot_d1` like ',@session_DB,'.rpt_cell_tile_position_dy_def;');	
					PREPARE Stmt FROM @SqlCmd;
					EXECUTE Stmt;
					DEALLOCATE PREPARE Stmt;
				
				
					SET @SqlCmd=CONCAT('CREATE  TABLE  IF NOT EXISTS ',@session_DB,'.`rpt_cell_tile_call_overshoot_d1` like ',@session_DB,'.rpt_cell_tile_start_dy_def;');	
					PREPARE Stmt FROM @SqlCmd;
					EXECUTE Stmt;
					DEALLOCATE PREPARE Stmt;
	
					SET @SqlCmd=CONCAT('INSERT INTO ',GT_DB,'.`rpt_cell_tile_event_overshoot_d7`
					(',CELL_TILE_DY_DEF_KEY_COL_STR,',',OVERSHOOT_MAP_EVENT_COL_STR,')
					SELECT 
							',CELL_TILE_DY_DEF_KEY_COL_STR,',
							',OVERSHOOT_MAP_EVENT_COL_STR,'
					FROM ',@session_DB,'.rpt_cell_tile_event_overshoot_d1
					ON DUPLICATE KEY UPDATE 
					rpt_cell_tile_event_overshoot_d7.OVER_LOC_CNT=rpt_cell_tile_event_overshoot_d7.OVER_LOC_CNT+VALUES(OVER_LOC_CNT),
					rpt_cell_tile_event_overshoot_d7.NON_OVER_LOC_CNT=rpt_cell_tile_event_overshoot_d7.NON_OVER_LOC_CNT+VALUES(NON_OVER_LOC_CNT),
					rpt_cell_tile_event_overshoot_d7.MAINBEAM_OVER_LOC_CNT=rpt_cell_tile_event_overshoot_d7.MAINBEAM_OVER_LOC_CNT+VALUES(MAINBEAM_OVER_LOC_CNT),
					rpt_cell_tile_event_overshoot_d7.MAINBEAM_NON_OVER_LOC_CNT=rpt_cell_tile_event_overshoot_d7.MAINBEAM_NON_OVER_LOC_CNT+VALUES(MAINBEAM_NON_OVER_LOC_CNT),
					rpt_cell_tile_event_overshoot_d7.BEST_OVER_LOC_CNT=rpt_cell_tile_event_overshoot_d7.BEST_OVER_LOC_CNT+VALUES(BEST_OVER_LOC_CNT),
					rpt_cell_tile_event_overshoot_d7.BEST_NON_OVER_LOC_CNT=rpt_cell_tile_event_overshoot_d7.BEST_NON_OVER_LOC_CNT+VALUES(BEST_NON_OVER_LOC_CNT),
					rpt_cell_tile_event_overshoot_d7.BEST_MAINBEAM_OVER_LOC_CNT=rpt_cell_tile_event_overshoot_d7.BEST_MAINBEAM_OVER_LOC_CNT+VALUES(BEST_MAINBEAM_OVER_LOC_CNT),
					rpt_cell_tile_event_overshoot_d7.TILE_MAINBEAM= CASE WHEN  VALUES(TILE_MAINBEAM) = 1  OR rpt_cell_tile_event_overshoot_d7.TILE_MAINBEAM = 1  THEN 1 ELSE 0 END,
					rpt_cell_tile_event_overshoot_d7.MR_4G_RSRP_BEST_CNT=rpt_cell_tile_event_overshoot_d7.MR_4G_RSRP_BEST_CNT+VALUES(MR_4G_RSRP_BEST_CNT),
					rpt_cell_tile_event_overshoot_d7.MR_4G_RSRP_BEST_SUM=rpt_cell_tile_event_overshoot_d7.MR_4G_RSRP_BEST_SUM+VALUES(MR_4G_RSRP_BEST_SUM),
					rpt_cell_tile_event_overshoot_d7.MR_4G_RSRQ_BEST_CNT=rpt_cell_tile_event_overshoot_d7.MR_4G_RSRQ_BEST_CNT+VALUES(MR_4G_RSRQ_BEST_CNT),
					rpt_cell_tile_event_overshoot_d7.MR_4G_RSRQ_BEST_SUM=rpt_cell_tile_event_overshoot_d7.MR_4G_RSRQ_BEST_SUM+VALUES(MR_4G_RSRQ_BEST_SUM)');		
					PREPARE Stmt FROM @SqlCmd;
					EXECUTE Stmt;
					DEALLOCATE PREPARE Stmt; 
	
	
					SET @SqlCmd=CONCAT('INSERT INTO ',GT_DB,'.`rpt_cell_tile_call_overshoot_d7`
					(',CELL_TILE_DY_DEF_KEY_COL_STR,',',OVERSHOOT_MAP_CALL_COL_STR,')
					SELECT 
							',CELL_TILE_DY_DEF_KEY_COL_STR,',
							',OVERSHOOT_MAP_CALL_COL_STR,'
					FROM ',@session_DB,'.rpt_cell_tile_call_overshoot_d1
					ON DUPLICATE KEY UPDATE 
					rpt_cell_tile_call_overshoot_d7.POS_FIRST_S_RSRP=rpt_cell_tile_call_overshoot_d7.POS_FIRST_S_RSRP+VALUES(POS_FIRST_S_RSRP),
					rpt_cell_tile_call_overshoot_d7.POS_FIRST_S_RSRP_CNT=rpt_cell_tile_call_overshoot_d7.POS_FIRST_S_RSRP_CNT+VALUES(POS_FIRST_S_RSRP_CNT),
					rpt_cell_tile_call_overshoot_d7.OVER_LOC_CALL_CNT=rpt_cell_tile_call_overshoot_d7.OVER_LOC_CALL_CNT+VALUES(OVER_LOC_CALL_CNT),
					rpt_cell_tile_call_overshoot_d7.NONOVER_LOC_CALL_CNT=rpt_cell_tile_call_overshoot_d7.NONOVER_LOC_CALL_CNT+VALUES(NONOVER_LOC_CALL_CNT),
					rpt_cell_tile_call_overshoot_d7.TILE_OVER_FLAG=CASE WHEN  VALUES(TILE_OVER_FLAG) = 1  OR rpt_cell_tile_call_overshoot_d7.TILE_OVER_FLAG = 1  THEN 1 ELSE 0 END,
					rpt_cell_tile_call_overshoot_d7.OVER_DISTANCE_SUM=rpt_cell_tile_call_overshoot_d7.OVER_DISTANCE_SUM+VALUES(OVER_DISTANCE_SUM),
					rpt_cell_tile_call_overshoot_d7.OVER_DISTANCE_MAX=CASE WHEN  VALUES(OVER_DISTANCE_MAX) > rpt_cell_tile_call_overshoot_d7.OVER_DISTANCE_MAX THEN VALUES(OVER_DISTANCE_MAX)
					ELSE rpt_cell_tile_call_overshoot_d7.OVER_DISTANCE_MAX END,
					rpt_cell_tile_call_overshoot_d7.POS_FIRST_S_RSRQ=rpt_cell_tile_call_overshoot_d7.POS_FIRST_S_RSRQ+VALUES(POS_FIRST_S_RSRQ),
					rpt_cell_tile_call_overshoot_d7.POS_FIRST_S_RSRQ_CNT=rpt_cell_tile_call_overshoot_d7.POS_FIRST_S_RSRQ_CNT+VALUES(POS_FIRST_S_RSRQ_CNT)
					;');
					PREPARE Stmt FROM @SqlCmd;
					EXECUTE Stmt;
					DEALLOCATE PREPARE Stmt; 
				
				END IF;
			
				SET @v_i=@v_i+1; 
						
				
				END;
				END WHILE;
	
			SET @SqlCmd=CONCAT('TRUNCATE TABLE ',GT_DB,'.`table_overshooting_severity_lte_d7`;');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
			
			SET @SqlCmd=CONCAT('INSERT INTO ',GT_DB,'.table_overshooting_severity_lte_d7(
				  `DATA_DATE`,
				  `EUTRABAND`,
				  `EARFCN`,
				  `ENODEB_NAME`,
				  `ENODEB_ID`,
				  `CELL_NAME`,
				  `CELL_ID`,
				  `PU_ID`,
				  `SUB_REGION_ID`,
				 ',OVERSHOOT_COL_STR,')	
			SELECT 
				  `DATA_DATE`,
				  `EUTRABAND`,
				  `EARFCN`,
				   NULL AS `ENODEB_NAME`,
				  `ENODEB_ID`,
				  `CELL_NAME`,
				  `CELL_ID`,
				   NULL AS `PU_ID`,
				  `SUB_REGION_ID`,
				   SUM(`OVER_LOC_CALL_CNT`) AS T1_CALL_CNT,
				   SUM(`NONOVER_LOC_CALL_CNT`) AS T0_CALL_CNT,
				  SUM(`OVER_DISTANCE_SUM`)/SUM(`OVER_LOC_CALL_CNT`) AS 	AVG_DISTANCE,
				MAX(OVER_DISTANCE_MAX) AS MAX_DISTANCE,
				NULL AS BASE_DISTANCE,
				SUM(CASE WHEN TILE_OVER_FLAG is NOT NULL THEN 1 ELSE 0 END) AS T0_TILE_CNT,
				SUM(TILE_OVER_FLAG) AS T1_TILE_CNT,
				NULL AS W_KPI,
				NULL AS T1_RSRP_TILE_COUNT
				FROM ',GT_DB,'.`tmp_rpt_cell_tile_start_',WORKER_ID,'`
 				GROUP BY `EUTRABAND`,`EARFCN`,`ENODEB_ID`,`CELL_ID`,`SUB_REGION_ID`
				ORDER BY NULL;');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt; 
	
	
		SET @SQLCMD =CONCAT('DROP TEMPORARY TABLE IF EXISTS ',GT_DB,'.TMP_NT_CURRENT;');
		PREPARE STMT FROM @SQLCMD;
		EXECUTE STMT;
		DEALLOCATE PREPARE STMT; 
	
		SET @SQLCMD =CONCAT(' CREATE TEMPORARY TABLE ',GT_DB,'.TMP_NT_CURRENT ENGINE=MYISAM
				SELECT A.`ENODEB_ID`,A.`CELL_ID`,A.`ENODEB_NAME`,A.PU_ID,B.PATHLOSS_DISTANCE * (1.5-A.SITE_DENSITY_TYPE/10) AS BASE_DISTANCE
					,B.ANTENNA_RADIUS,B.CLOSED_RADIUS
				FROM ',CURRENT_NT_DB,'.`nt_cell_current_lte` A
				LEFT JOIN ',CURRENT_NT_DB,'.`nt_antenna_current_lte` B
				ON A.`ENODEB_ID`=B.`ENODEB_ID` AND A.`CELL_ID`=B.`CELL_ID` AND A.INDOOR = 0;');
		PREPARE STMT FROM @SQLCMD;
		EXECUTE STMT;
		DEALLOCATE PREPARE STMT;	
	
		SET @SqlCmd=CONCAT('UPDATE ',GT_DB,'.`table_overshooting_severity_lte_d7` A, ',GT_DB,'.TMP_NT_CURRENT B
				SET 
					A.PU_ID = B.PU_ID,
					A.ENODEB_NAME  = B.ENODEB_NAME,
					A.BASE_DISTANCE  = B.BASE_DISTANCE
				WHERE A.ENODEB_ID = B.ENODEB_ID AND A.CELL_ID = B.CELL_ID ;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
	
		SET @SqlCmd=CONCAT('INSERT INTO ',GT_DB,'.table_overshooting_severity_event_lte_d7(
				  `DATA_DATE`,
				  `EUTRABAND`,
				  `EARFCN`,
				  `ENODEB_NAME`,
				  `ENODEB_ID`,
				  `CELL_NAME`,
				  `CELL_ID`,
				  `PU_ID`,
				  `SUB_REGION_ID`,
				 ',OVERSHOOT_EVENT_COL_STR,')	
			SELECT 
				  `DATA_DATE`,
				  `EUTRABAND`,
				  `EARFCN`,
				   NULL AS `ENODEB_NAME`,
				  a.`ENODEB_ID`,
				  `CELL_NAME`,
				  a.`CELL_ID`,
				   NULL AS `PU_ID`,
				  `SUB_REGION_ID`,
					SUM(OVER_LOC_CNT) AS `SERVING_T1_EVENT_CNT`,
					SUM(NON_OVER_LOC_CNT) AS `SERVING_T0_EVENT_CNT`,
					SUM(CASE WHEN OVER_LOC_CNT >0 THEN 1 ELSE 0 END) AS `SERVING_T1_TILE_CNT`,
					SUM(CASE WHEN NON_OVER_LOC_CNT>0 AND OVER_LOC_CNT=0 THEN 1 ELSE 0 END) AS `SERVING_T0_TILE_CNT`,
					SUM(BEST_OVER_LOC_CNT) AS `BEST_T1_EVENT_CNT`,
					SUM(BEST_NON_OVER_LOC_CNT) AS `BEST_T0_EVENT_CNT`,
					SUM(CASE WHEN BEST_OVER_LOC_CNT>0 THEN 1 ELSE 0 END) AS `BEST_T1_TILE_CNT`,
					SUM(CASE WHEN BEST_NON_OVER_LOC_CNT>0 AND BEST_OVER_LOC_CNT=0 THEN 1 ELSE 0 END) AS `BEST_T0_TILE_CNT`,
					MAX( CASE WHEN OVER_LOC_CNT>0 THEN gt_covmo_distance(gt_covmo_proj_hex_geohash_to_lng(TILE_ID, ',@ZOOM_LEVEL,') ,gt_covmo_proj_hex_geohash_to_lat(TILE_ID, ',@ZOOM_LEVEL,'), b.LONGITUDE,b.LATITUDE)
					ELSE NULL END) AS SERVING_OVER_MAX_DISTANCE,
					MAX( CASE WHEN MAINBEAM_OVER_LOC_CNT>0 THEN gt_covmo_distance(gt_covmo_proj_hex_geohash_to_lng(TILE_ID, ',@ZOOM_LEVEL,') ,gt_covmo_proj_hex_geohash_to_lat(TILE_ID,',@ZOOM_LEVEL,'), b.LONGITUDE,b.LATITUDE) 
					ELSE NULL END) AS SERVING_MAINBEAM_OVER_MAX_DISTANCE,
					MAX( CASE WHEN BEST_OVER_LOC_CNT>0 THEN gt_covmo_distance(gt_covmo_proj_hex_geohash_to_lng(TILE_ID, ',@ZOOM_LEVEL,') ,gt_covmo_proj_hex_geohash_to_lat(TILE_ID, ',@ZOOM_LEVEL,'), b.LONGITUDE,b.LATITUDE) 
					ELSE NULL END) AS BEST_OVER_MAX_DISTANCE,
					MAX( CASE WHEN BEST_MAINBEAM_OVER_LOC_CNT>0 THEN gt_covmo_distance(gt_covmo_proj_hex_geohash_to_lng(TILE_ID, ',@ZOOM_LEVEL,') ,gt_covmo_proj_hex_geohash_to_lat(TILE_ID, ',@ZOOM_LEVEL,'), b.LONGITUDE,b.LATITUDE) 
					ELSE NULL END) AS BEST_MAINBEAM_OVER_MAX_DISTANCE,
					SUM(CASE WHEN OVER_LOC_CNT>0 THEN gt_covmo_distance(gt_covmo_proj_hex_geohash_to_lng(TILE_ID, ',@ZOOM_LEVEL,') ,gt_covmo_proj_hex_geohash_to_lat(TILE_ID, ',@ZOOM_LEVEL,'), b.LONGITUDE,b.LATITUDE)
					ELSE NULL END)/SUM( CASE WHEN OVER_LOC_CNT>0 THEN 1 ELSE NULL END) AS  SERVING_OVER_DISTANCE_AVG,
					SUM(CASE WHEN MAINBEAM_OVER_LOC_CNT>0 THEN gt_covmo_distance(gt_covmo_proj_hex_geohash_to_lng(TILE_ID, ',@ZOOM_LEVEL,') ,gt_covmo_proj_hex_geohash_to_lat(TILE_ID, ',@ZOOM_LEVEL,'), b.LONGITUDE,b.LATITUDE) 
					ELSE NULL END) /SUM( CASE WHEN MAINBEAM_OVER_LOC_CNT>0 THEN 1 ELSE NULL END) AS SERVING_MAINBEAM_OVER_DISTANCE_AVG,
					SUM(CASE WHEN BEST_OVER_LOC_CNT>0 THEN gt_covmo_distance(gt_covmo_proj_hex_geohash_to_lng(TILE_ID, ',@ZOOM_LEVEL,') ,gt_covmo_proj_hex_geohash_to_lat(TILE_ID, ',@ZOOM_LEVEL,'), b.LONGITUDE,b.LATITUDE) 
					ELSE NULL END)/SUM(CASE WHEN BEST_OVER_LOC_CNT>0 THEN 1 ELSE NULL END) AS BEST_OVER_DISTANCE_AVG,
					SUM(CASE WHEN BEST_MAINBEAM_OVER_LOC_CNT>0 THEN gt_covmo_distance(gt_covmo_proj_hex_geohash_to_lng(TILE_ID, ',@ZOOM_LEVEL,') ,gt_covmo_proj_hex_geohash_to_lat(TILE_ID, ',@ZOOM_LEVEL,'), b.LONGITUDE,b.LATITUDE) 
					ELSE NULL END)/ SUM(CASE WHEN BEST_MAINBEAM_OVER_LOC_CNT>0 THEN 1 ELSE NULL END)  AS BEST_MAINBEAM_OVER_DISTANCE_AVG
				FROM ',GT_DB,'.`tmp_rpt_cell_tile_position_',WORKER_ID,'` a
				LEFT JOIN  ',CURRENT_NT_DB,'.nt_antenna_current_lte b
				ON a.ENODEB_ID = b.ENODEB_ID AND a.CELL_ID = b.CELL_ID
				GROUP BY `EUTRABAND`,`EARFCN`,`ENODEB_ID`,`CELL_ID`,`SUB_REGION_ID`
				ORDER BY NULL;');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
		SET @SqlCmd=CONCAT('UPDATE ',GT_DB,'.`table_overshooting_severity_event_lte_d7` A, ',GT_DB,'.TMP_NT_CURRENT B
			SET 
				A.PU_ID = B.PU_ID,
				A.ENODEB_NAME  = B.ENODEB_NAME,
				A.ANTENNA_RADIUS  = B.ANTENNA_RADIUS,
				A.CLOSED_RADIUS  = B.CLOSED_RADIUS
			WHERE A.ENODEB_ID = B.ENODEB_ID AND A.CELL_ID = B.CELL_ID ;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
	
			IF  @OVERSHOOT_JUDGE_FLAG = 'CELL_RADIUS'
				THEN 
				
				SET @SqlCmd=CONCAT('UPDATE ',GT_DB,'.`table_overshooting_severity_event_lte_d7` A, ',CURRENT_NT_DB,'.nt_cell_current_lte B
						SET 
							A.ANTENNA_RADIUS  = B.CELL_RADIUS,
							A.CLOSED_RADIUS  = B.CELL_RADIUS
						WHERE A.ENODEB_ID = B.ENODEB_ID AND A.CELL_ID = B.CELL_ID ;');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
			
			END IF;
			
	SET @SqlCmd=CONCAT('DROP TABLE IF EXISTS  ',GT_DB,'.`tmp_table_overshooting_severity_lte_',WORKER_ID,'`;'); 
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	
	END IF;
			
	INSERT INTO gt_gw_main.SP_LOG VALUES(O_GT_DB,'SP_SUB_GENERATE_OVERSHOOTING_SEVERITY_LTE',CONCAT('Event base Done: ',TIMESTAMPDIFF(SECOND,STEP_START_TIME,SYSDATE()),' seconds.'), NOW());	
		
END$$
DELIMITER ;
