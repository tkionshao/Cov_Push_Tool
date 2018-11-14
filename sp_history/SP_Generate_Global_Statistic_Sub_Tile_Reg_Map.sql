DELIMITER $$
USE `gt_global_statistic`$$
DROP PROCEDURE IF EXISTS `SP_CreateDB_LTE`$$
CREATE DEFINER=`covmo`@`%` PROCEDURE `SP_Generate_Global_Statistic_Sub_Tile_Reg_Map`(IN TBL_NAME VARCHAR(100),IN WORKER_ID VARCHAR(10),IsIMSI BIT,GROUP_ID TINYINT(4))
a_label:
BEGIN
	DECLARE START_TIME DATETIME DEFAULT SYSDATE();
	DECLARE STEP_START_TIME DATETIME DEFAULT SYSDATE();
	DECLARE SUB_WORKER_ID VARCHAR(10) DEFAULT CONNECTION_ID();
	
	SET SESSION max_heap_table_size=4*1024*1024*1024;
	SET SESSION tmp_table_size=4*1024*1024*1024;
	SET SESSION group_concat_max_len=102400; 
	
	CALL gt_gw_main.`SP_Sub_Set_Session_Param_LTE`('');
	
	SET @global_db='gt_global_statistic';
	
	INSERT INTO gt_gw_main.SP_LOG VALUES(@global_db,'SP_Generate_Global_Statistic_Sub_Tile_Reg_Map',CONCAT(TBL_NAME,',',WORKER_ID,',',SUB_WORKER_ID,', START'), START_TIME);
	SET STEP_START_TIME := SYSDATE();
	
	
	SET @SqlCmd=CONCAT('SELECT COUNT(*) INTO @V_REGION_TILE_GID FROM information_schema.`TABLES`
								WHERE TABLE_SCHEMA=''',@global_db,''' 
								AND TABLE_NAME=''table_region_tile_g',GROUP_ID,''';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
				
	IF @V_REGION_TILE_GID=0 THEN 
		SET @SqlCmd=CONCAT('CREATE TABLE  ',@global_db,'.table_region_tile_g',GROUP_ID,'(
			  `tile_id` BIGINT(20) NOT NULL DEFAULT ''0'',
			  `reg_1_id` BIGINT(20) DEFAULT ''0'',
			  `reg_2_id` INT(20) DEFAULT ''0'',
			  `reg_3_id` BIGINT(20) DEFAULT ''0'',
			  `reg_level` TINYINT(4) DEFAULT NULL,
			  `group_id` TINYINT(4) DEFAULT NULL,
			   KEY `idx_reg_tile` (`tile_id`,`group_id`,`reg_1_id`,`reg_2_id`,`reg_3_id`))
			   ENGINE=MYISAM DEFAULT CHARSET=utf8 DELAY_KEY_WRITE=1;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
	
		IF GROUP_ID =1 THEN 
			SET @SqlCmd=CONCAT('INSERT IGNORE INTO ',@global_db,'.table_region_tile_g',GROUP_ID,'
					SELECT tile_id,reg_1_id,reg_2_id,reg_3_id,reg_level,group_id 
					FROM ',@global_db,'.table_region_tile_g
					WHERE group_id IN (0,1);');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
		ELSE 
			SET @SqlCmd=CONCAT('INSERT IGNORE INTO ',@global_db,'.table_region_tile_g',GROUP_ID,'
					SELECT tile_id,reg_1_id,reg_2_id,reg_3_id,reg_level,group_id 
					FROM ',@global_db,'.table_region_tile_g
					WHERE group_id =',GROUP_ID,';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
		END IF;	
	END IF;	
	IF IsIMSI THEN 
	SET @SqlCmd=CONCAT('UPDATE ',TBL_NAME,' A,',@global_db,'.`table_region_tile_g',GROUP_ID,'` B
				SET A.START_REG_1_ID=B.REG_1_ID,
					A.START_REG_2_ID=B.REG_2_ID,
					A.START_REG_3_ID=B.REG_3_ID
				WHERE A.`START_TILE_ID`=B.`TILE_ID` 
				AND B.group_id=',GROUP_ID,'
				AND B.reg_level IN (1,2,3);
		;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	INSERT INTO gt_gw_main.SP_LOG VALUES(@global_db,'SP_Generate_Global_Statistic_Sub_Tile_Reg_Map',CONCAT(TBL_NAME,' UPDATE start_reg_id cost:',TIMESTAMPDIFF(SECOND,STEP_START_TIME,SYSDATE()),' sec.'), NOW());
	SET STEP_START_TIME := SYSDATE();
	SET @SqlCmd=CONCAT('UPDATE ',TBL_NAME,' A,',@global_db,'.`table_region_tile_g',GROUP_ID,'` B
				SET A.END_REG_1_ID=B.REG_1_ID,
					A.END_REG_2_ID=B.REG_2_ID,
					A.END_REG_3_ID=B.REG_3_ID
				WHERE A.`END_TILE_ID`=B.`TILE_ID`  
				AND B.group_id=',GROUP_ID,' 
				AND B.reg_level IN (1,2,3);
		;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	INSERT INTO gt_gw_main.SP_LOG VALUES(@global_db,'SP_Generate_Global_Statistic_Sub_Tile_Reg_Map',CONCAT(TBL_NAME,' UPDATE end_reg_id cost:',TIMESTAMPDIFF(SECOND,STEP_START_TIME,SYSDATE()),' sec.'), NOW());
	SET STEP_START_TIME := SYSDATE();
	ELSE 
	SET @SqlCmd=CONCAT('SELECT `att_value` INTO @ZOOM_LEVEL FROM gt_covmo.`sys_config` WHERE `group_name`=''system'' AND att_name = ''MapResolution'';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('SELECT COUNT(*) INTO @v_REG3 FROM ',@global_db,'.`usr_polygon_reg_3` WHERE group_id=',GROUP_ID,';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt; 
	
	IF @v_REG3=0 THEN 
		LEAVE a_label;
	END IF;
		
	SET @SqlCmd=CONCAT('SELECT COUNT(*) INTO @v_REG2 FROM ',@global_db,'.`usr_polygon_reg_2` WHERE group_id=',GROUP_ID,';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt; 
	
	SET @SqlCmd=CONCAT('SELECT COUNT(*) INTO @v_REG1 FROM ',@global_db,'.`usr_polygon_reg_1` WHERE group_id=',GROUP_ID,';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt; 
			
	SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS ',@global_db,'.`tmp_tile_3_lte_',WORKER_ID,'_',SUB_WORKER_ID,'_g',GROUP_ID,'`;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt; 
	
	SET @SqlCmd=CONCAT('CREATE TEMPORARY TABLE ',@global_db,'.`tmp_tile_3_lte_',WORKER_ID,'_',SUB_WORKER_ID,'_g',GROUP_ID,'` (
				  `tile_id` BIGINT(20) NOT NULL,
				  PRIMARY KEY (`tile_id`)
			) ENGINE=MYISAM DEFAULT CHARSET=latin1;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd =CONCAT('ALTER TABLE ',@global_db,'.`tmp_tile_3_lte_',WORKER_ID,'_',SUB_WORKER_ID,'_g',GROUP_ID,'` DISABLE KEYS;') ;
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	INSERT INTO gt_gw_main.SP_LOG VALUES(@global_db,'SP_Generate_Global_Statistic_Sub_Tile_Reg_Map',CONCAT('tmp_tile_3_',WORKER_ID,'_',SUB_WORKER_ID,'_g',GROUP_ID,' DISABLE KEYS cost:',TIMESTAMPDIFF(SECOND,STEP_START_TIME,SYSDATE()),' sec.'), NOW());
	SET STEP_START_TIME := SYSDATE();
			
	SET @SqlCmd=CONCAT('INSERT INTO ',@global_db,'.`tmp_tile_3_lte_',WORKER_ID,'_',SUB_WORKER_ID,'_g',GROUP_ID,'`  
				(`tile_id` )
				SELECT tile_id 
				FROM 
				(
					SELECT DISTINCT tile_id FROM ',TBL_NAME,' A 
					WHERE NOT EXISTS 
						(
							SELECT NULL FROM ',@global_db,'.`table_region_tile_g',GROUP_ID,'` C FORCE INDEX(idx_reg_tile)
							WHERE C.`tile_id`=A.`tile_id` 
						)
				) A;
				');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	INSERT INTO gt_gw_main.SP_LOG VALUES(@global_db,'SP_Generate_Global_Statistic_Sub_Tile_Reg_Map',CONCAT('tmp_tile_3_',WORKER_ID,'_',SUB_WORKER_ID,'_g',GROUP_ID,' INSERT cost:',TIMESTAMPDIFF(SECOND,STEP_START_TIME,SYSDATE()),' sec.'), NOW());
	SET STEP_START_TIME := SYSDATE();
	
	SET @SqlCmd =CONCAT('ALTER TABLE ',@global_db,'.`tmp_tile_3_lte_',WORKER_ID,'_',SUB_WORKER_ID,'_g',GROUP_ID,'` ENABLE KEYS;') ;
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	INSERT INTO gt_gw_main.SP_LOG VALUES(@global_db,'SP_Generate_Global_Statistic_Sub_Tile_Reg_Map',CONCAT('tmp_tile_3_',WORKER_ID,'_',SUB_WORKER_ID,'_g',GROUP_ID,' ENABLE KEYS cost:',TIMESTAMPDIFF(SECOND,STEP_START_TIME,SYSDATE()),' sec.'), NOW());
	SET STEP_START_TIME := SYSDATE();
	/*
	SET @SqlCmd =CONCAT('INSERT IGNORE INTO ',@global_db,'.table_region_tile_g',GROUP_ID,' 
				    (`tile_id`,
				     `reg_1_id`,
				     `reg_2_id`,
				     `reg_3_id`,
				     `reg_level`,
				     `group_id`)
				SELECT A.`tile_id`,
					CASE WHEN (@v_REG1=0 OR @v_REG2=0) THEN NULL ELSE FLOOR(B.`parent_id`/1000) END AS reg_1_id,
					CASE WHEN @v_REG2=0 THEN NULL ELSE B.`parent_id` END AS reg_2_id,
					B.`id` AS reg_3_id,3,',GROUP_ID,' 
				FROM ',@global_db,'.tmp_tile_3_lte_',WORKER_ID,'_',SUB_WORKER_ID,'_g',GROUP_ID,' A
				,',@global_db,'.`usr_polygon_reg_3` B
				WHERE gt_covmo_pointinpoly(
						ASWKB(GEOMFROMTEXT(A.`lon_lat`)),
						ASWKB(GEOMFROMTEXT(B.`polygon_str`)))=1 
					AND B.`group_id`=',GROUP_ID,'
				;') ;
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	*/
	
	SET @SqlCmd=CONCAT('INSERT IGNORE INTO ',@global_db,'.table_region_tile_g',GROUP_ID,' 
			    (`tile_id`,
			     `reg_1_id`,
			     `reg_2_id`,
			     `reg_3_id`,
			     `reg_level`,
			     `group_id`)
			SELECT A.`tile_id`,
				0 AS reg_1_id,
				0 AS reg_2_id,
				0 AS reg_3_id,0,0
			FROM ',@global_db,'.`tmp_tile_3_lte_',WORKER_ID,'_',SUB_WORKER_ID,'_g',GROUP_ID,'` A 
			WHERE NOT EXISTS 
					(
						SELECT NULL FROM ',@global_db,'.`table_region_tile_g',GROUP_ID,'` C FORCE INDEX(idx_reg_tile)
						WHERE C.`tile_id`=A.`tile_id`
					)
			;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	INSERT INTO gt_gw_main.SP_LOG VALUES(@global_db,'SP_Generate_Global_Statistic_Sub_Tile_Reg_Map',CONCAT('table_region_tile_g',GROUP_ID,',3,',WORKER_ID,'_',SUB_WORKER_ID,' INSERT REG 0 cost:',TIMESTAMPDIFF(SECOND,STEP_START_TIME,SYSDATE()),' sec.'), NOW());
	SET STEP_START_TIME := SYSDATE();
	
	IF @v_REG2>0 THEN 
		SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS ',@global_db,'.`tmp_tile_2_lte_',WORKER_ID,'_',SUB_WORKER_ID,'`;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt; 
		SET @SqlCmd=CONCAT('CREATE TEMPORARY TABLE ',@global_db,'.`tmp_tile_2_lte_',WORKER_ID,'_',SUB_WORKER_ID,'` (
					  `tile_id` BIGINT(20) NOT NULL,
					  `lon_lat` VARCHAR(70) DEFAULT NULL,
					  PRIMARY KEY (`tile_id`)
				) ENGINE=MYISAM DEFAULT CHARSET=latin1;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		
		SET @SqlCmd =CONCAT('ALTER TABLE ',@global_db,'.`tmp_tile_2_lte_',WORKER_ID,'_',SUB_WORKER_ID,'` DISABLE KEYS;') ;
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		INSERT INTO gt_gw_main.SP_LOG VALUES(@global_db,'SP_Generate_Global_Statistic_Sub_Tile_Reg_Map',CONCAT('tmp_tile_2_',WORKER_ID,'_',SUB_WORKER_ID,' DISABLE KEYS cost:',TIMESTAMPDIFF(SECOND,STEP_START_TIME,SYSDATE()),' sec.'), NOW());
		SET STEP_START_TIME := SYSDATE();
				
		SET @SqlCmd=CONCAT('INSERT INTO ',@global_db,'.`tmp_tile_2_lte_',WORKER_ID,'_',SUB_WORKER_ID,'`  
					(`tile_id` ,`lon_lat` )
					SELECT tile_id ,CONCAT(''POINT('',(gt_covmo_proj_geohash_to_lng(gt_covmo_hex_geohash_upperbound(TILE_ID,',@ZOOM_LEVEL,'))+gt_covmo_proj_geohash_to_lng(gt_covmo_hex_geohash_lowerbound(TILE_ID,',@ZOOM_LEVEL,')))/2,'' '',(gt_covmo_proj_geohash_to_lat(gt_covmo_hex_geohash_upperbound(TILE_ID,',@ZOOM_LEVEL,'))+gt_covmo_proj_geohash_to_lat(gt_covmo_hex_geohash_lowerbound(TILE_ID,',@ZOOM_LEVEL,')))/2,'')'') AS lon_lat 
					FROM 
					(
						SELECT tile_id FROM ',@global_db,'.tmp_tile_3_lte_',WORKER_ID,'_',SUB_WORKER_ID,'_g',GROUP_ID,' A 
						WHERE NOT EXISTS 
							(
								SELECT NULL FROM ',@global_db,'.`table_region_tile_g',GROUP_ID,'` C FORCE INDEX(idx_reg_tile)
								WHERE C.`tile_id`=A.`tile_id`
							)
					) A;
					');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		INSERT INTO gt_gw_main.SP_LOG VALUES(@global_db,'SP_Generate_Global_Statistic_Sub_Tile_Reg_Map',CONCAT('tmp_tile_2_',WORKER_ID,'_',SUB_WORKER_ID,' INSERT cost:',TIMESTAMPDIFF(SECOND,STEP_START_TIME,SYSDATE()),' sec.'), NOW());
		SET STEP_START_TIME := SYSDATE();
		
		SET @SqlCmd =CONCAT('ALTER TABLE ',@global_db,'.`tmp_tile_2_lte_',WORKER_ID,'_',SUB_WORKER_ID,'` ENABLE KEYS;') ;
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		INSERT INTO gt_gw_main.SP_LOG VALUES(@global_db,'SP_Generate_Global_Statistic_Sub_Tile_Reg_Map',CONCAT('tmp_tile_2_',WORKER_ID,'_',SUB_WORKER_ID,' ENABLE KEYS cost:',TIMESTAMPDIFF(SECOND,STEP_START_TIME,SYSDATE()),' sec.'), NOW());
		SET STEP_START_TIME := SYSDATE();
				
		SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS ',@global_db,'.`tmp_tile_3_lte_',WORKER_ID,'_',SUB_WORKER_ID,'_g',GROUP_ID,'`;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt; 
					
		SET @SqlCmd =CONCAT('INSERT IGNORE INTO ',@global_db,'.table_region_tile_g',GROUP_ID,'  
					    (`tile_id`,
					     `reg_1_id`,
					     `reg_2_id`,
					     `reg_3_id`,
					     `reg_level`,
					     `group_id`)
					SELECT A.`tile_id`,
						B.`parent_id` AS reg_1_id,
						B.`id` AS reg_2_id,
						NULL AS reg_3_id,2,',GROUP_ID,' 
					FROM ',@global_db,'.tmp_tile_2_lte_',WORKER_ID,'_',SUB_WORKER_ID,' A
					,',@global_db,'.`usr_polygon_reg_2` B
					WHERE gt_covmo_pointinpoly(
							ASWKB(GEOMFROMTEXT(A.`lon_lat`)),
							ASWKB(GEOMFROMTEXT(B.`polygon_str`)))=1 
						AND B.`group_id`=',GROUP_ID,'
				;') ;
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		INSERT INTO gt_gw_main.SP_LOG VALUES(@global_db,'SP_Generate_Global_Statistic_Sub_Tile_Reg_Map',CONCAT('table_region_tile_g,2,',WORKER_ID,'_',SUB_WORKER_ID,' INSERT cost:',TIMESTAMPDIFF(SECOND,STEP_START_TIME,SYSDATE()),' sec.'), NOW());
		SET STEP_START_TIME := SYSDATE();
		
		IF @v_REG1>0 THEN 		
			SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS ',@global_db,'.`tmp_tile_1_lte_',WORKER_ID,'_',SUB_WORKER_ID,'`;');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt; 
			
			SET @SqlCmd=CONCAT('CREATE TEMPORARY TABLE ',@global_db,'.`tmp_tile_1_lte_',WORKER_ID,'_',SUB_WORKER_ID,'` (
						  `tile_id` BIGINT(20) NOT NULL,
						  `lon_lat` VARCHAR(70) DEFAULT NULL,
						  PRIMARY KEY (`tile_id`)
					) ENGINE=MYISAM DEFAULT CHARSET=latin1;');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
			
			SET @SqlCmd =CONCAT('ALTER TABLE ',@global_db,'.`tmp_tile_1_lte_',WORKER_ID,'_',SUB_WORKER_ID,'` DISABLE KEYS;') ;
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
			INSERT INTO gt_gw_main.SP_LOG VALUES(@global_db,'SP_Generate_Global_Statistic_Sub_Tile_Reg_Map',CONCAT('tmp_tile_1_',WORKER_ID,'_',SUB_WORKER_ID,' DISABLE KEYS cost:',TIMESTAMPDIFF(SECOND,STEP_START_TIME,SYSDATE()),' sec.'), NOW());
			SET STEP_START_TIME := SYSDATE();
					
			SET @SqlCmd=CONCAT('INSERT INTO ',@global_db,'.`tmp_tile_1_lte_',WORKER_ID,'_',SUB_WORKER_ID,'`  
						(`tile_id` ,`lon_lat` )
						SELECT tile_id ,CONCAT(''POINT('',(gt_covmo_proj_geohash_to_lng(gt_covmo_hex_geohash_upperbound(TILE_ID,',@ZOOM_LEVEL,'))+gt_covmo_proj_geohash_to_lng(gt_covmo_hex_geohash_lowerbound(TILE_ID,',@ZOOM_LEVEL,')))/2,'' '',(gt_covmo_proj_geohash_to_lat(gt_covmo_hex_geohash_upperbound(TILE_ID,',@ZOOM_LEVEL,'))+gt_covmo_proj_geohash_to_lat(gt_covmo_hex_geohash_lowerbound(TILE_ID,',@ZOOM_LEVEL,')))/2,'')'') AS lon_lat 
						FROM 
						(
							SELECT tile_id FROM ',@global_db,'.tmp_tile_2_lte_',WORKER_ID,'_',SUB_WORKER_ID,' A 
							WHERE NOT EXISTS 
								(
									SELECT NULL FROM ',@global_db,'.`table_region_tile_g',GROUP_ID,'` C FORCE INDEX(idx_reg_tile)
									WHERE C.`tile_id`=A.`tile_id` 
								)
						) A;
						');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
			INSERT INTO gt_gw_main.SP_LOG VALUES(@global_db,'SP_Generate_Global_Statistic_Sub_Tile_Reg_Map',CONCAT('tmp_tile_1_',WORKER_ID,'_',SUB_WORKER_ID,' INSERT cost:',TIMESTAMPDIFF(SECOND,STEP_START_TIME,SYSDATE()),' sec.'), NOW());
			SET STEP_START_TIME := SYSDATE();
			
			SET @SqlCmd =CONCAT('ALTER TABLE ',@global_db,'.`tmp_tile_1_lte_',WORKER_ID,'_',SUB_WORKER_ID,'` ENABLE KEYS;') ;
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
			INSERT INTO gt_gw_main.SP_LOG VALUES(@global_db,'SP_Generate_Global_Statistic_Sub_Tile_Reg_Map',CONCAT('tmp_tile_1_',WORKER_ID,'_',SUB_WORKER_ID,' ENABLE KEYS cost:',TIMESTAMPDIFF(SECOND,STEP_START_TIME,SYSDATE()),' sec.'), NOW());
			SET STEP_START_TIME := SYSDATE();
			
			SET @SqlCmd =CONCAT('INSERT IGNORE INTO ',@global_db,'.table_region_tile_g',GROUP_ID,' 
						    (`tile_id`,
						     `reg_1_id`,
						     `reg_2_id`,
						     `reg_3_id`,
						     `reg_level`,
						     `group_id`)
						SELECT A.`tile_id`,
							`id` AS reg_1_id,
							NULL AS reg_2_id,
							NULL AS reg_3_id,1,',GROUP_ID,' 
						FROM ',@global_db,'.tmp_tile_1_lte_',WORKER_ID,'_',SUB_WORKER_ID,' A
						,',@global_db,'.`usr_polygon_reg_1` B
						WHERE gt_covmo_pointinpoly(
								ASWKB(GEOMFROMTEXT(A.`lon_lat`)),
								ASWKB(GEOMFROMTEXT(B.`polygon_str`)))=1 
							AND B.`group_id`=',GROUP_ID,'
					;') ;
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
			INSERT INTO gt_gw_main.SP_LOG VALUES(@global_db,'SP_Generate_Global_Statistic_Sub_Tile_Reg_Map',CONCAT('table_region_tile_g,1,',WORKER_ID,'_',SUB_WORKER_ID,' INSERT cost:',TIMESTAMPDIFF(SECOND,STEP_START_TIME,SYSDATE()),' sec.'), NOW());
			SET STEP_START_TIME := SYSDATE();
					
			SET @SqlCmd=CONCAT('INSERT IGNORE INTO ',@global_db,'.table_region_tile_g',GROUP_ID,'  
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
						FROM ',@global_db,'.tmp_tile_1_lte_',WORKER_ID,'_',SUB_WORKER_ID,' A 
						WHERE NOT EXISTS 
								(
									SELECT NULL FROM ',@global_db,'.`table_region_tile_g',GROUP_ID,'` C FORCE INDEX(idx_reg_tile)
									WHERE C.`tile_id`=A.`tile_id` 
								)
						;');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
			INSERT INTO gt_gw_main.SP_LOG VALUES(@global_db,'SP_Generate_Global_Statistic_Sub_Tile_Reg_Map',CONCAT('tmp_tile_0_',WORKER_ID,'_',SUB_WORKER_ID,' INSERT cost:',TIMESTAMPDIFF(SECOND,STEP_START_TIME,SYSDATE()),' sec.'), NOW());
			SET STEP_START_TIME := SYSDATE();
								
			SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS ',@global_db,'.`tmp_tile_1_lte_',WORKER_ID,'_',SUB_WORKER_ID,'`;');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
		ELSE
			SET @SqlCmd=CONCAT('INSERT IGNORE INTO ',@global_db,'.table_region_tile_g',GROUP_ID,' 
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
						FROM ',@global_db,'.tmp_tile_2_lte_',WORKER_ID,'_',SUB_WORKER_ID,' A 
						WHERE NOT EXISTS 
								(
									SELECT NULL FROM ',@global_db,'.`table_region_tile_g',GROUP_ID,'` C FORCE INDEX(idx_reg_tile)
									WHERE C.`tile_id`=A.`tile_id`
								)
						;');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
			INSERT INTO gt_gw_main.SP_LOG VALUES(@global_db,'SP_Generate_Global_Statistic_Sub_Tile_Reg_Map',CONCAT('tmp_tile_0_',WORKER_ID,'_',SUB_WORKER_ID,' INSERT cost:',TIMESTAMPDIFF(SECOND,STEP_START_TIME,SYSDATE()),' sec.'), NOW());
			SET STEP_START_TIME := SYSDATE();			
		END IF;
								
		SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS ',@global_db,'.`tmp_tile_2_lte_',WORKER_ID,'_',SUB_WORKER_ID,'`;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
	ELSE
			INSERT INTO gt_gw_main.SP_LOG VALUES(@global_db,'SP_Generate_Global_Statistic_Sub_Tile_Reg_Map',CONCAT('tmp_tile_0_',WORKER_ID,'_',SUB_WORKER_ID,' INSERT cost:',TIMESTAMPDIFF(SECOND,STEP_START_TIME,SYSDATE()),' sec.'), NOW());
			SET STEP_START_TIME := SYSDATE();
	END IF;
	SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS ',@global_db,'.`tmp_tile_3_lte_',WORKER_ID,'_',SUB_WORKER_ID,'_g',GROUP_ID,'`;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt; 
	SET @SqlCmd=CONCAT('UPDATE ',TBL_NAME,' A,',@global_db,'.`table_region_tile_g',GROUP_ID,'` B
				SET A.REG_1_ID=B.REG_1_ID,
					A.REG_2_ID=B.REG_2_ID,
					A.REG_3_ID=B.REG_3_ID
				WHERE A.`tile_id`=B.`tile_id` ;
		;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	INSERT INTO gt_gw_main.SP_LOG VALUES(@global_db,'SP_Generate_Global_Statistic_Sub_Tile_Reg_Map',CONCAT(TBL_NAME,' UPDATE reg_id cost:',TIMESTAMPDIFF(SECOND,STEP_START_TIME,SYSDATE()),' sec.'), NOW());
	SET STEP_START_TIME := SYSDATE();
	END IF;
	
	INSERT INTO gt_gw_main.SP_LOG VALUES(@global_db,'SP_Generate_Global_Statistic_Sub_Tile_Reg_Map',CONCAT(TBL_NAME,',',WORKER_ID,'_',SUB_WORKER_ID,' Done: ',TIMESTAMPDIFF(SECOND,START_TIME,SYSDATE()),' seconds.'), NOW());
END$$
DELIMITER ;
