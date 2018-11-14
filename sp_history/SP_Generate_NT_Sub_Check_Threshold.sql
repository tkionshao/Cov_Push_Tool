DELIMITER $$
USE `gt_gw_main`$$
DROP PROCEDURE IF EXISTS `SP_CreateDB_LTE`$$
CREATE DEFINER=`covmo`@`%` PROCEDURE `SP_Generate_NT_Sub_Check_Threshold`(IN GT_DB VARCHAR(100),IN GT_COVMO VARCHAR(100))
a_label:
BEGIN
	DECLARE START_TIME DATETIME DEFAULT SYSDATE();
	DECLARE v_cnt INT;
	
	DECLARE GT_DB_STR INT;
	DECLARE sys_threshold INT;
	SET GT_DB_STR = gt_strtok(GT_DB,3,'_');
	SET @GT_DB_DATE = STR_TO_DATE(GT_DB_STR,'%Y%m%d');
	SET @SUB_GT_DB_DATE = DATE_SUB(@GT_DB_DATE,INTERVAL 1 DAY);
	SET @LAST_NT_DATE =  DATE_FORMAT(@SUB_GT_DB_DATE, '%Y%m%d');
	SET @SqlCmd=CONCAT(' CREATE TABLE IF NOT EXISTS ',GT_DB,'.nt_count	
		( 
		`ENODEB_CNT` INT(10) DEFAULT NULL,
		`CELL_CNT` INT(10) DEFAULT NULL,
		`LAST_CELL_CNT` INT(10) DEFAULT NULL,
		`sys_threshold` INT(10) DEFAULT NULL,
		`threshold` DECIMAL(10,2) DEFAULT NULL,
		`message`  VARCHAR(100) DEFAULT NULL) 
		ENGINE=MYISAM DEFAULT CHARSET=utf8;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt; 
	SET @SqlCmd=CONCAT('SELECT count(SCHEMA_NAME)   into  @NT_DB_COUNT FROM `information_schema`.`SCHEMATA` WHERE SCHEMA_NAME LIKE ''%gt_nt%'' 
	;');
	PREPARE Stmt FROM @SqlCmd;		
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	IF  @NT_DB_COUNT = 1
	
	THEN 
	
		SET @SqlCmd=CONCAT('SELECT COUNT(DISTINCT ENODEB_ID,CELL_ID) FROM ',GT_DB,'.nt_cell_current_lte INTO @today_nt;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		
		
		
		SET @SqlCmd=CONCAT('SELECT COUNT(DISTINCT ENODEB_ID) FROM ',GT_DB,'.nt_cell_current_lte INTO @today_nt_enb;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		
		SET @today_nt_enb = IFNULL(@today_nt_enb,0);
		SET @today_nt = IFNULL(@today_nt,0);
		
		
		
		SELECT VALUE FROM gt_gw_main.`integration_param` WHERE gt_group='WINA' AND gt_name = 'threshold' INTO sys_threshold; 
		
		SET @SqlCmd=CONCAT('INSERT INTO ',GT_DB,'.nt_count
				SELECT  
				',@today_nt_enb,' AS ENB_CNT,
				',@today_nt,' AS CELL_CNT,
				''0'' AS LAST_CELL_CNT,
				',sys_threshold,' AS sys_threshold,
				''0'' AS threshold,
				''success'' AS message; ');
		
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
	
	ELSE 
	
		SET @SqlCmd=CONCAT('SELECT TABLE_SCHEMA   into  @LAST_NT_DB FROM `information_schema`.`TABLES` WHERE TABLE_SCHEMA LIKE ''%gt_nt%'' 
		AND TABLE_NAME = ''nt_cell_current_lte''   AND TABLE_SCHEMA  <> ''',GT_DB,'''    order by  TABLE_SCHEMA  desc limit 1
		;');
		PREPARE Stmt FROM @SqlCmd;		
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		
		
		
		SET @SqlCmd=CONCAT('SELECT COUNT(*) FROM `information_schema`.`TABLES`WHERE TABLE_SCHEMA = ''',@LAST_NT_DB,'''
		AND TABLE_NAME = ''nt_cell_current_lte'' INTO @last_exists 
		;');
		PREPARE Stmt FROM @SqlCmd;		
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		
		
		IF @last_exists > 0 
			THEN 
		 
			SET @SqlCmd=CONCAT('SELECT COUNT(DISTINCT ENODEB_ID,CELL_ID) FROM ',GT_DB,'.nt_cell_current_lte INTO @today_nt;');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
			
			
			SET @SqlCmd=CONCAT('SELECT COUNT(DISTINCT ENODEB_ID,CELL_ID) FROM ',@LAST_NT_DB,'.nt_cell_current_lte INTO @last_nt;');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
			SELECT @last_nt;
			
			SET @SqlCmd=CONCAT('SELECT COUNT(DISTINCT ENODEB_ID) FROM ',GT_DB,'.nt_cell_current_lte INTO @today_nt_enb;');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
			
			
			SET @SqlCmd=CONCAT('SELECT COUNT(DISTINCT ENODEB_ID) FROM ',@LAST_NT_DB,'.nt_cell_current_lte INTO @last_nt_enb;');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
			
			SET @last_nt_enb = IFNULL(@last_nt_enb,0);
			SET @today_nt_enb = IFNULL(@today_nt_enb,0);
			SET @last_nt = IFNULL(@last_nt,0);
			SET @today_nt = IFNULL(@today_nt,0);
			
			SELECT CAST(ABS((@today_nt-@last_nt)/@last_nt)*100 AS DECIMAL(10,2)) INTO @threshold;
			SET @threshold = IFNULL(@threshold,0);
			
			
			SELECT VALUE FROM gt_gw_main.`integration_param` WHERE gt_group='WINA' AND gt_name = 'threshold' INTO sys_threshold; 
			
			IF @threshold > sys_threshold
				THEN
				
				SET @SqlCmd=CONCAT('INSERT INTO ',GT_DB,'.nt_count(ENODEB_CNT,CELL_CNT,LAST_CELL_CNT,sys_threshold,threshold,message)
						SELECT  
						',@today_nt_enb,' AS ENB_CNT,
						',@today_nt,' AS CELL_CNT,
						',@last_nt,' AS LAST_CELL_CNT,
						',sys_threshold,' AS sys_threshold,
						',@threshold,' AS threshold,
						''failed'' AS message; ');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
	
				SET @SqlCmd=CONCAT('TRUNCATE TABLE ',GT_DB,'.antenna_info; ');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				
				
				SET @SqlCmd=CONCAT('TRUNCATE TABLE ',GT_DB,'.nt_antenna_current_lte; ');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				
				SET @SqlCmd=CONCAT('TRUNCATE TABLE ',GT_DB,'.nt_cell_current_lte; ');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				
					SET @SqlCmd=CONCAT('TRUNCATE TABLE ',GT_DB,'.nt_mme_current_lte; ');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				
				SET @SqlCmd=CONCAT('TRUNCATE TABLE ',GT_DB,'.nt_nbr_4_2_current_lte; ');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				
				SET @SqlCmd=CONCAT('TRUNCATE TABLE ',GT_DB,'.nt_nbr_4_3_current_lte; ');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				
				SET @SqlCmd=CONCAT('TRUNCATE TABLE ',GT_DB,'.nt_nbr_4_4_current_lte; ');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				
				SET @SqlCmd=CONCAT('TRUNCATE TABLE ',GT_DB,'.nt_neighbor_voronoi_lte; ');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				
				SET @SqlCmd=CONCAT('TRUNCATE TABLE ',GT_DB,'.nt_tac_cell_current_lte; ');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				
				
				
				SET @SqlCmd=CONCAT('INSERT INTO ',GT_DB,'.antenna_info
						SELECT  *
						FROM ',@LAST_NT_DB,'.antenna_info; ');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				
				
				
				SET @SqlCmd=CONCAT('INSERT INTO ',GT_DB,'.nt_antenna_current_lte
						SELECT  *
						FROM ',@LAST_NT_DB,'.nt_antenna_current_lte; ');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				
				
				
				
				SET @SqlCmd=CONCAT('INSERT INTO ',GT_DB,'.nt_cell_current_lte
						SELECT  *
						FROM ',@LAST_NT_DB,'.nt_cell_current_lte; ');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				
				
				
				SET @SqlCmd=CONCAT('INSERT INTO ',GT_DB,'.nt_mme_current_lte
						SELECT  *
						FROM ',@LAST_NT_DB,'.nt_mme_current_lte; ');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				
				SET @SqlCmd=CONCAT('INSERT INTO ',GT_DB,'.nt_nbr_4_2_current_lte
						SELECT  *
						FROM ',@LAST_NT_DB,'.nt_nbr_4_2_current_lte; ');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				
				SET @SqlCmd=CONCAT('INSERT INTO ',GT_DB,'.nt_nbr_4_3_current_lte
						SELECT  *
						FROM ',@LAST_NT_DB,'.nt_nbr_4_3_current_lte; ');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				
				SET @SqlCmd=CONCAT('INSERT INTO ',GT_DB,'.nt_nbr_4_4_current_lte
						SELECT  *
						FROM ',@LAST_NT_DB,'.nt_nbr_4_4_current_lte; ');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				
				SET @SqlCmd=CONCAT('INSERT INTO ',GT_DB,'.nt_tac_cell_current_lte
						SELECT  *
						FROM ',@LAST_NT_DB,'.nt_tac_cell_current_lte; ');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				
			ELSE 
			
				SET @SqlCmd=CONCAT('INSERT INTO ',GT_DB,'.nt_count
						SELECT  
						',@today_nt_enb,' AS ENB_CNT,
						',@today_nt,' AS CELL_CNT,
						',@last_nt,' AS LAST_CELL_CNT,
						',sys_threshold,' AS sys_threshold,
						',@threshold,' AS threshold,
						''success'' AS message; ');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				
				INSERT INTO gt_gw_main.SP_LOG VALUES(GT_DB,'SP_Generate_NT_Sub_Check_Threshold',CONCAT('Done: ',TIMESTAMPDIFF(SECOND,START_TIME,SYSDATE()),' seconds.'), NOW());
				
			END IF;
		
		ELSE
			SET @SqlCmd=CONCAT('SELECT COUNT(DISTINCT ENODEB_ID,CELL_ID) FROM ',GT_DB,'.nt_cell_current_lte INTO @today_nt;');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
			
			
			SET @SqlCmd=CONCAT('SELECT COUNT(DISTINCT ENODEB_ID) FROM ',GT_DB,'.nt_cell_current_lte INTO @today_nt_enb;');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
			
			
			SET @today_nt_enb = IFNULL(@today_nt_enb,0);
			SET @today_nt = IFNULL(@today_nt,0);
			SET @last_nt = IFNULL(@last_nt,0);
			SET @threshold = IFNULL(@threshold,0);
			
			SET @SqlCmd=CONCAT('INSERT INTO ',GT_DB,'.nt_count(ENODEB_CNT,CELL_CNT,LAST_CELL_CNT,sys_threshold,threshold,message)
					SELECT  
					''',@today_nt_enb,''' AS ENB_CNT,
					''',@today_nt,''' AS CELL_CNT,
					''',@last_nt,''' AS LAST_CELL_CNT,
					''',sys_threshold,''' AS sys_threshold,
					''',@threshold,''' AS threshold,
					CONCAT(''success'') AS message; ');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
		END IF;
	END IF;
END$$
DELIMITER ;
