DELIMITER $$
USE `gt_global_statistic`$$
DROP PROCEDURE IF EXISTS `SP_CreateDB_LTE`$$
CREATE DEFINER=`covmo`@`%` PROCEDURE `SP_Generate_Global_Statistic_Sub_failcause_Aggr`(IN TECH_MASK TINYINT(2),IN WORKER_ID VARCHAR(10),IN FLAG TINYINT(2),IN exDate VARCHAR(10),IN DS_FLAG TINYINT(2),PENDING_FLAG TINYINT(2))
BEGIN
	DECLARE START_TIME DATETIME DEFAULT SYSDATE();
	DECLARE STEP_START_TIME DATETIME DEFAULT SYSDATE();
	DECLARE SP_Process VARCHAR(100) DEFAULT NULL;
	DECLARE FIRSTDAY_OF_WEEK TINYINT(4) DEFAULT 6;
	DECLARE v_DATA_DATE VARCHAR(10) DEFAULT NULL;
	DECLARE v_DATA_HOUR TINYINT(2) DEFAULT NULL;
	DECLARE v_PU_ID MEDIUMINT(9) DEFAULT NULL;
	DECLARE v_TECH_MASK TINYINT(2) DEFAULT NULL;
	DECLARE v_i TINYINT(2) DEFAULT 1;
	DECLARE v_R_Max TINYINT(2) DEFAULT 0;
	DECLARE v_group_db_name VARCHAR(100) DEFAULT '';
	
	DECLARE FAILCAUSE_COLUMN_STR VARCHAR(1500) DEFAULT 
		'`FAILURE_CNT`';
		  
	DECLARE FAILCAUSE_COLUMN_IFNULL_STR VARCHAR(5000) DEFAULT 
		'IFNULL(FAILURE_CNT,0) AS FAILURE_CNT';
		
	DECLARE FAILCAUSE_COLUMN_SUM_STR VARCHAR(5000) DEFAULT 
		'SUM(IFNULL(FAILURE_CNT,0)) AS FAILURE_CNT';
	
	DECLARE FAILCAUSE_COLUMN_UPD_STR VARCHAR(10000) DEFAULT
		'RPT_TABLE_NAME.FAILURE_CNT=RPT_TABLE_NAME.FAILURE_CNT+VALUES(FAILURE_CNT)';
	
	DECLARE CONTINUE HANDLER FOR SQLEXCEPTION
	BEGIN
		SET @SqlCmd=CONCAT('CREATE TABLE IF NOT EXISTS `gt_global_statistic`.`nw_error_log`
				(
				  `TABLE_NAME` VARCHAR(50) NOT NULL,
				  `SP_NAME` VARCHAR(50) DEFAULT NULL,
				  `INSERT_CMD` VARCHAR(100) DEFAULT NULL,
				  `INSERT_TIME` DATETIME DEFAULT NULL
				) ENGINE=MYISAM;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
	
		INSERT INTO `gt_global_statistic`.`nw_error_log`(`TABLE_NAME`,`SP_NAME`,`INSERT_CMD`,`INSERT_TIME`)
		VALUES (@SOURCE_TABLE_NAME,
			'SP_Generate_Global_Statistic_Sub_failcause_Aggr',
			CONCAT('Step ',@ERROR_FLAG,' error'),
			NOW());
	
		SET @SqlCmd=CONCAT('REPAIR TABLE ',@REPAIR_TABLE_NAME,';');				
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
	END;	
	
  	CALL gt_gw_main.`SP_Sub_Set_Session_Param_LTE`('');
  	
	SET @global_db='gt_global_statistic';
			
	SET @FLAG_FAILCAUSE_UMTS_DY=0;
	SET @FLAG_FAILCAUSE_LTE_DY=0;
	INSERT INTO gt_gw_main.SP_LOG VALUES(@global_db,'SP_Generate_Global_Statistic_Sub_failcause_Aggr',CONCAT(TECH_MASK,',',WORKER_ID,' START'), START_TIME);
	SET STEP_START_TIME := SYSDATE();
	IF FLAG IN (1,0) THEN 
		SET @SqlCmd=CONCAT('SELECT GROUP_CONCAT(DISTINCT CONCAT(`DATA_DATE`,'','',`DATA_HOUR`,'','',`TECH_MASK`) SEPARATOR ''|'' ) INTO @PU_STR_TECH
					FROM ',@global_db,'.tmp_table_call_cnt_',WORKER_ID,' 
					WHERE TECH_MASK=',TECH_MASK,';');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		INSERT INTO gt_gw_main.SP_LOG VALUES(@global_db,'SP_Generate_Global_Statistic_Sub_failcause_Aggr',@SqlCmd, START_TIME);		
		
		
		SET @SqlCmd=CONCAT('SELECT GROUP_CONCAT(DISTINCT `group_id` ORDER BY `group_id` SEPARATOR ''|'' ) INTO @REG_GROUP FROM ',@global_db,'.`usr_polygon_reg_3`;');
	
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
			
		SET @v_reg_m=1;
		SET @v_reg_Max=gt_covmo_csv_count(@REG_GROUP,'|');
		WHILE @v_reg_m <= @v_reg_Max DO
		BEGIN
			SET v_group_db_name=CONCAT('gt_global_statistic_g',gt_strtok(@REG_GROUP, @v_reg_m, '|'));
		
			SET v_i=1;
			SET v_R_Max=gt_covmo_csv_count(@PU_STR_TECH,'|');
			
			WHILE v_i <= v_R_Max DO
			BEGIN	
				SET v_DATA_DATE:=gt_covmo_csv_get(gt_strtok(@PU_STR_TECH, v_i, '|'),1);
				SET v_DATA_HOUR:=gt_covmo_csv_get(gt_strtok(@PU_STR_TECH, v_i, '|'),2);
				SET v_TECH_MASK:=gt_covmo_csv_get(gt_strtok(@PU_STR_TECH, v_i, '|'),3);
				SELECT `DAY_OF_WEEK`(v_DATA_DATE) INTO @DAY_OF_WEEK;
				SET @FIRST_DAY=gt_strtok(@DAY_OF_WEEK, 1, '|');
				SET @END_DAY=gt_strtok(@DAY_OF_WEEK, 2, '|');
				SET @DATE_WK=CONCAT(DATE_FORMAT(@FIRST_DAY,'%Y%m%d'),'_',DATE_FORMAT(@END_DAY,'%Y%m%d'));
				SET @DATE_MN=DATE_FORMAT(v_DATA_DATE,'%Y%m');
				SET @DATE_DY=DATE_FORMAT(v_DATA_DATE,'%Y%m%d');
				SET @EX_DATE=CONCAT(DATE_FORMAT(v_DATA_DATE,'%Y%m%d_'),v_DATA_HOUR,'_',WORKER_ID);
				
				SET @table_failcause_umts_dy=CONCAT(v_group_db_name,'.table_failcause_umts_dy_',@DATE_DY);
				SET @table_failcause_lte_dy=CONCAT(v_group_db_name,'.table_failcause_lte_dy_',@DATE_DY);
				
				IF v_TECH_MASK=2 THEN 	
					SET @SOURCE_TABLE_NAME=@table_failcause_umts_dy;
					SET @REPAIR_TABLE_NAME=@table_failcause_umts_dy;
					SET @ERROR_FLAG='table_failcause_umts_dy';
					SET @SqlCmd=CONCAT('INSERT INTO ',@table_failcause_umts_dy,' 
							    (
								`DATA_DATE`,
								`TILE_ID`,
								`RNC_ID`,
								`SITE_ID`,
								`CELL_ID`,
								`IMSI`,
								`IMEI`,
								`EVENT_ID`,
								`FAILURE_EVENT_ID`,
								`FAILURE_EVENT_CAUSE`,
								',FAILCAUSE_COLUMN_STR,')
							SELECT
								`DATA_DATE`,
								`TILE_ID`,
								`RNC_ID`,
								`SITE_ID`,
								`CELL_ID`,
								`IMSI`,
								`IMEI`,
								`EVENT_ID`,
								`FAILURE_EVENT_ID`,
								`FAILURE_EVENT_CAUSE`,
								',FAILCAUSE_COLUMN_SUM_STR,' 
							FROM ',@global_db,'.tmp_failcause_umts_',@EX_DATE,'
							GROUP BY `DATA_DATE`,
								`TILE_ID`,
								`RNC_ID`,
								`SITE_ID`,
								`CELL_ID`,
								`IMSI`,
								`EVENT_ID`,
								`FAILURE_EVENT_ID`,
								`FAILURE_EVENT_CAUSE`
							ORDER BY NULL
							ON DUPLICATE KEY UPDATE
							',REPLACE(FAILCAUSE_COLUMN_UPD_STR,'RPT_TABLE_NAME',@table_failcause_umts_dy),'
							;');
					PREPARE Stmt FROM @SqlCmd;
					EXECUTE Stmt;
					DEALLOCATE PREPARE Stmt; 
					
					SET @FLAG_FAILCAUSE_UMTS_DY=1;
					INSERT INTO gt_gw_main.SP_LOG VALUES(@global_db,'SP_Generate_Global_Statistic_Sub_failcause_Aggr',CONCAT('INSERT INTO ',@table_failcause_umts_hr,', cost:',TIMESTAMPDIFF(SECOND,STEP_START_TIME,SYSDATE()),' sec.'), NOW());
					SET STEP_START_TIME := SYSDATE();
				END IF;
				IF v_TECH_MASK=4 THEN 		
					SET @SOURCE_TABLE_NAME=@table_failcause_lte_dy;
					SET @REPAIR_TABLE_NAME=@table_failcause_lte_dy;
					SET @ERROR_FLAG='table_failcause_lte_dy';
					SET @SqlCmd=CONCAT('INSERT INTO ',@table_failcause_lte_dy,' 
							    (
								`DATA_DATE`,
								`TILE_ID`,
								`PU_ID`,
								`ENODEB_ID`,
								`CELL_ID`,
								`IMSI`,
								`IMEI`,
								`EVENT_ID`,
								`FAILURE_EVENT_ID`,
								`FAILURE_EVENT_CAUSE`,
								',FAILCAUSE_COLUMN_STR,')
							SELECT
								`DATA_DATE`,
								`TILE_ID`,
								`PU_ID`,
								`ENODEB_ID`,
								`CELL_ID`,
								`IMSI`,
								`IMEI`,
								`EVENT_ID`,
								`FAILURE_EVENT_ID`,
								`FAILURE_EVENT_CAUSE`,
								',FAILCAUSE_COLUMN_SUM_STR,' 
							FROM ',@global_db,'.tmp_failcause_lte_',@EX_DATE,'
							GROUP BY `DATA_DATE`,
								`TILE_ID`,
								`PU_ID`,
								`ENODEB_ID`,
								`CELL_ID`,
								`IMSI`,
								`EVENT_ID`,
								`FAILURE_EVENT_ID`,
								`FAILURE_EVENT_CAUSE`
							ORDER BY NULL
							ON DUPLICATE KEY UPDATE
							',REPLACE(FAILCAUSE_COLUMN_UPD_STR,'RPT_TABLE_NAME',@table_failcause_lte_dy),'
							;');
					PREPARE Stmt FROM @SqlCmd;
					EXECUTE Stmt;
					DEALLOCATE PREPARE Stmt; 
					
					SET @FLAG_FAILCAUSE_LTE_DY=1;
					INSERT INTO gt_gw_main.SP_LOG VALUES(@global_db,'SP_Generate_Global_Statistic_Sub_failcause_Aggr',CONCAT('INSERT INTO ',@table_failcause_lte_hr,', cost:',TIMESTAMPDIFF(SECOND,STEP_START_TIME,SYSDATE()),' sec.'), NOW());
					SET STEP_START_TIME := SYSDATE();
				END IF;
				SET v_i=v_i+1;
				INSERT INTO gt_gw_main.SP_LOG VALUES(@global_db,'SP_Generate_Global_Statistic_Sub_failcause_Aggr',CONCAT(v_TECH_MASK,',',WORKER_ID,',',v_i), NOW());
			END;
			END WHILE;
			SET @v_reg_m=@v_reg_m+1;
		END;
		END WHILE;
		
		IF v_TECH_MASK=2 THEN 		
			SET @SqlCmd=CONCAT('DROP TABLE IF EXISTS ',@global_db,'.tmp_failcause_umts_',@EX_DATE,';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
			INSERT INTO gt_gw_main.SP_LOG VALUES(@global_db,'SP_Generate_Global_Statistic_Sub_failcause_Aggr',CONCAT('UMTS DROP ',@global_db,'.tmp_failcause_umts_',@EX_DATE,' ',WORKER_ID,' cost:',TIMESTAMPDIFF(SECOND,STEP_START_TIME,SYSDATE()),' sec.'), NOW());
			SET STEP_START_TIME := SYSDATE();
						
			SET @SqlCmd=CONCAT('UPDATE `gt_global_statistic`.tmp_table_call_cnt_',WORKER_ID,' A
				SET 
				  A.`FLAG_FAILCAUSE_HR` = ',@FLAG_FAILCAUSE_UMTS_DY,'
				WHERE A.`TECH_MASK`=2
				;');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
		END IF;
		
		IF v_TECH_MASK=4 THEN 		
			SET @SqlCmd=CONCAT('DROP TABLE IF EXISTS ',@global_db,'.tmp_failcause_lte_',@EX_DATE,';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
			INSERT INTO gt_gw_main.SP_LOG VALUES(@global_db,'SP_Generate_Global_Statistic_Sub_failcause_Aggr',CONCAT('LTE DROP ',@global_db,'.tmp_failcause_lte_',@EX_DATE,' ',WORKER_ID,' cost:',TIMESTAMPDIFF(SECOND,STEP_START_TIME,SYSDATE()),' sec.'), NOW());
			SET STEP_START_TIME := SYSDATE();
						
			SET @SqlCmd=CONCAT('UPDATE `gt_global_statistic`.tmp_table_call_cnt_',WORKER_ID,' A
				SET 
				  A.`FLAG_FAILCAUSE_HR` = ',@FLAG_FAILCAUSE_LTE_DY,'
				WHERE A.`TECH_MASK`=4
				;');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
		END IF;
		INSERT INTO gt_gw_main.SP_LOG VALUES(@global_db,'SP_Generate_Global_Statistic_Sub_failcause_Aggr',CONCAT(v_DATA_DATE,',',v_DATA_HOUR,',',v_TECH_MASK,',',WORKER_ID,' Done: ',TIMESTAMPDIFF(SECOND,START_TIME,SYSDATE()),' seconds.'), NOW());
	END IF;
END$$
DELIMITER ;
