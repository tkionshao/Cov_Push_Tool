DELIMITER $$
USE `gt_gw_main`$$
DROP PROCEDURE IF EXISTS `SP_CreateDB_LTE`$$
CREATE DEFINER=`covmo`@`%` PROCEDURE `SP_Qlikview_Tile_Reg_Map`(IN TBL_NAME VARCHAR(100),IN WORKER_ID VARCHAR(10),GROUP_ID TINYINT(4))
a_label:
BEGIN
	DECLARE START_TIME DATETIME DEFAULT SYSDATE();
	DECLARE STEP_START_TIME DATETIME DEFAULT SYSDATE();
	DECLARE SUB_WORKER_ID VARCHAR(10) DEFAULT CONNECTION_ID();
	DECLARE CURRENT_NT_DB VARCHAR(50) DEFAULT (SELECT MAX(SCHEMA_NAME) AS `Database` FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME LIKE 'gt_nt%');
	
	CALL gt_gw_main.`SP_Sub_Set_Session_Param_LTE`('');
	
	SET @global_db='gt_global_statistic';
	
	INSERT INTO gt_gw_main.SP_LOG VALUES(@global_db,'SP_Qlikview_Tile_Reg_Map',CONCAT(TBL_NAME,',',WORKER_ID,',',SUB_WORKER_ID,', START'), START_TIME);
	SET STEP_START_TIME := SYSDATE();
	SET @SqlCmd=CONCAT('SELECT att_value INTO @SYS_CONFIG_TILE FROM ',CURRENT_NT_DB,'.`sys_config` WHERE `group_name`=''system'' AND att_name = ''MapResolution'';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
		
	IF gt_covmo_csv_count(@SYS_CONFIG_TILE,',') =3 THEN
		
		SET @SqlCmd=CONCAT('SELECT gt_covmo_csv_get(att_value,3) INTO @ZOOM_LEVEL FROM ',CURRENT_NT_DB,'.`sys_config` WHERE `group_name`=''system'' AND att_name = ''MapResolution'';');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
	ELSE 
		SET @SqlCmd=CONCAT('SELECT att_value INTO @ZOOM_LEVEL FROM ',CURRENT_NT_DB,'.`sys_config` WHERE `group_name`=''system'' AND att_name = ''MapResolution'';');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
	END IF;
	
	SET @SqlCmd=CONCAT('SELECT COUNT(*) INTO @v_REG3 FROM ',@global_db,'.`usr_polygon_reg_3_V4`;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt; 
	
	IF @v_REG3=0 THEN 
		LEAVE a_label;
	END IF;
	
	SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS ',@global_db,'.`tmp_tile_3_lte_',WORKER_ID,'_',SUB_WORKER_ID,'`;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt; 
	
	SET @SqlCmd=CONCAT('CREATE TEMPORARY TABLE ',@global_db,'.`tmp_tile_3_lte_',WORKER_ID,'_',SUB_WORKER_ID,'` (
				  `tile_id` BIGINT(20) NOT NULL,
				  `lon_lat` VARCHAR(70) DEFAULT NULL,
				  PRIMARY KEY (`tile_id`)
			) ENGINE=MYISAM DEFAULT CHARSET=latin1;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd =CONCAT('ALTER TABLE ',@global_db,'.`tmp_tile_3_lte_',WORKER_ID,'_',SUB_WORKER_ID,'` DISABLE KEYS;') ;
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	INSERT INTO gt_gw_main.SP_LOG VALUES(@global_db,'SP_Qlikview_Tile_Reg_Map',CONCAT('tmp_tile_3_',WORKER_ID,'_',SUB_WORKER_ID,' DISABLE KEYS cost:',TIMESTAMPDIFF(SECOND,STEP_START_TIME,SYSDATE()),' sec.'), NOW());
	SET STEP_START_TIME := SYSDATE();
			
	SET @SqlCmd=CONCAT('INSERT INTO ',@global_db,'.`tmp_tile_3_lte_',WORKER_ID,'_',SUB_WORKER_ID,'`  
				(`tile_id` ,`lon_lat` )
				SELECT tile_id ,CONCAT(''POINT('',(gt_covmo_proj_geohash_to_lng(gt_covmo_hex_geohash_upperbound(TILE_ID,',@ZOOM_LEVEL,'))+gt_covmo_proj_geohash_to_lng(gt_covmo_hex_geohash_lowerbound(TILE_ID,',@ZOOM_LEVEL,')))/2,'' '',(gt_covmo_proj_geohash_to_lat(gt_covmo_hex_geohash_upperbound(TILE_ID,',@ZOOM_LEVEL,'))+gt_covmo_proj_geohash_to_lat(gt_covmo_hex_geohash_lowerbound(TILE_ID,',@ZOOM_LEVEL,')))/2,'')'') AS lon_lat 
				FROM 
				(
					SELECT DISTINCT tile_id FROM ',TBL_NAME,' A 
					WHERE NOT EXISTS 
						(
							SELECT NULL FROM ',@global_db,'.`table_region_tile_g` C FORCE INDEX(idx_reg_tile)
							WHERE C.`tile_id`=A.`tile_id` AND C.group_id IN (0,',GROUP_ID,')
						)
				) A;
				');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	INSERT INTO gt_gw_main.SP_LOG VALUES(@global_db,'SP_Qlikview_Tile_Reg_Map',CONCAT('tmp_tile_3_',WORKER_ID,'_',SUB_WORKER_ID,' INSERT cost:',TIMESTAMPDIFF(SECOND,STEP_START_TIME,SYSDATE()),' sec.'), NOW());
	SET STEP_START_TIME := SYSDATE();
	
	SET @SqlCmd =CONCAT('ALTER TABLE ',@global_db,'.`tmp_tile_3_lte_',WORKER_ID,'_',SUB_WORKER_ID,'` ENABLE KEYS;') ;
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	INSERT INTO gt_gw_main.SP_LOG VALUES(@global_db,'SP_Qlikview_Tile_Reg_Map',CONCAT('tmp_tile_3_',WORKER_ID,'_',SUB_WORKER_ID,' ENABLE KEYS cost:',TIMESTAMPDIFF(SECOND,STEP_START_TIME,SYSDATE()),' sec.'), NOW());
	SET STEP_START_TIME := SYSDATE();
	
	SET @SqlCmd =CONCAT('INSERT IGNORE INTO ',@global_db,'.table_region_tile_g 
				    (`tile_id`,
				     `reg_1_id`,
				     `reg_2_id`,
				     `reg_3_id`,
				     `reg_level`,
				     `group_id`)
				SELECT A.`tile_id`,
					NULL AS reg_1_id,
					NULL AS reg_2_id,
					B.`id` AS reg_3_id,3,',GROUP_ID,' 
				FROM ',@global_db,'.tmp_tile_3_lte_',WORKER_ID,'_',SUB_WORKER_ID,' A
				,',@global_db,'.`usr_polygon_reg_3_V4` B
				WHERE gt_covmo_pointinpoly(
						ASWKB(GEOMFROMTEXT(A.`lon_lat`)),
						ASWKB(GEOMFROMTEXT(B.`polygon_str`)))=1 
					AND B.`id`=',GROUP_ID,'
				;') ;
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	INSERT INTO gt_gw_main.SP_LOG VALUES(@global_db,'SP_Qlikview_Tile_Reg_Map',CONCAT('table_region_tile_g,3,',WORKER_ID,'_',SUB_WORKER_ID,' INSERT cost:',TIMESTAMPDIFF(SECOND,STEP_START_TIME,SYSDATE()),' sec.'), NOW());
	SET STEP_START_TIME := SYSDATE();
	
	SET @SqlCmd=CONCAT('INSERT IGNORE INTO ',@global_db,'.table_region_tile_g 
				    (`tile_id`,
				     `reg_1_id`,
				     `reg_2_id`,
				     `reg_3_id`,
				     `reg_level`,
				     `group_id`)
				SELECT A.`tile_id`,
					NULL AS reg_1_id,
					NULL AS reg_2_id,
					NULL AS reg_3_id,0,0
				FROM ',@global_db,'.tmp_tile_3_lte_',WORKER_ID,'_',SUB_WORKER_ID,' A 
				WHERE NOT EXISTS 
						(
							SELECT NULL FROM ',@global_db,'.`table_region_tile_g` C FORCE INDEX(idx_reg_tile)
							WHERE C.`tile_id`=A.`tile_id` AND C.group_id IN (0,',GROUP_ID,')
						)
				;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	INSERT INTO gt_gw_main.SP_LOG VALUES(@global_db,'SP_Qlikview_Tile_Reg_Map',CONCAT('tmp_tile_0_',WORKER_ID,'_',SUB_WORKER_ID,' INSERT cost:',TIMESTAMPDIFF(SECOND,STEP_START_TIME,SYSDATE()),' sec.'), NOW());
	SET STEP_START_TIME := SYSDATE();
	
	SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS ',@global_db,'.`tmp_tile_3_lte_',WORKER_ID,'_',SUB_WORKER_ID,'`;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt; 
	SET @SqlCmd=CONCAT('UPDATE ',TBL_NAME,' A,',@global_db,'.`table_region_tile_g` B
				SET A.REG_ID=B.REG_3_ID
				WHERE A.`tile_id`=B.`tile_id` AND B.group_id IN (0,',GROUP_ID,') AND B.reg_level IN (1,2,3);
		;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	INSERT INTO gt_gw_main.SP_LOG VALUES(@global_db,'SP_Qlikview_Tile_Reg_Map',CONCAT(TBL_NAME,' UPDATE reg_id cost:',TIMESTAMPDIFF(SECOND,STEP_START_TIME,SYSDATE()),' sec.'), NOW());
	SET STEP_START_TIME := SYSDATE();
	
	INSERT INTO gt_gw_main.SP_LOG VALUES(@global_db,'SP_Qlikview_Tile_Reg_Map',CONCAT(TBL_NAME,',',WORKER_ID,'_',SUB_WORKER_ID,' Done: ',TIMESTAMPDIFF(SECOND,START_TIME,SYSDATE()),' seconds.'), NOW());
END$$
DELIMITER ;
