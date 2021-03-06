DELIMITER $$
USE `gt_gw_main`$$
DROP PROCEDURE IF EXISTS `SP_CreateDB_LTE`$$
CREATE DEFINER=`covmo`@`%` PROCEDURE `SP_Polystar_Exchange_Partition2`(IN TECH_MASK TINYINT(2),IN GT_DB VARCHAR(50),IN gt_polystar_db VARCHAR (20),IN RPT_TYPE VARCHAR(20),IMSI_GID TINYINT (2))
BEGIN
	DECLARE START_TIME DATETIME DEFAULT SYSDATE();
	DECLARE RNC_ID INT;
	DECLARE DATA_DATE VARCHAR(8);
	DECLARE DATA_QRT VARCHAR(4);
	DECLARE DATA_HOUR VARCHAR(4);
	DECLARE P_DATE_HR VARCHAR(4);
	DECLARE P_DATE_DY VARCHAR(4);
	DECLARE FOLDER_PATH VARCHAR(20);
	DECLARE DAILY_DB VARCHAR(25);
	DECLARE SH_EH VARCHAR(9) DEFAULT RIGHT(GT_DB,9);
	DECLARE CURRENT_NT_DB VARCHAR(50) DEFAULT CONCAT('gt_nt_',gt_strtok(GT_DB,3,'_'));
	DECLARE v_group_table_name VARCHAR(100) DEFAULT '';
	DECLARE v_group_table_name_tmp VARCHAR(100) DEFAULT '';
	
	SET SESSION group_concat_max_len = 100000000;
		
 	SELECT gt_strtok(GT_DB,2,'_') INTO RNC_ID;
 	SELECT gt_strtok(GT_DB,3,'_') INTO DATA_DATE;
 	SELECT gt_strtok(GT_DB,4,'_') INTO DATA_QRT;
	SELECT LEFT(DATA_QRT,2) INTO DATA_HOUR;
	
	SELECT LPAD((TO_DAYS(DATA_DATE) MOD 60),2,0) INTO P_DATE_HR;
	SELECT LPAD((TO_DAYS(DATA_DATE) MOD 90),2,0) INTO P_DATE_DY;
	
  --	RPT_TYPE=1(DPI_CALL_QR),2(RPT_HR),3(RPT_DY),4(RPT_WK)
	INSERT INTO gt_gw_main.sp_log VALUES('gt_gw_main','SP_Polystar_Exchange_Partition2','START TO EXCHANGE PARTITION',NOW());	
	
	IF TECH_MASK=4 AND RPT_TYPE = '1' THEN 
	
		INSERT INTO gt_gw_main.sp_log VALUES('gt_gw_main','SP_Polystar_Exchange_Partition2',CONCAT('START TO EXCHANGE PARTITION TABLE_POS_POLYSTAR_LTE_',DATA_QRT,''),NOW());	
		
		SET @SqlCmd=CONCAT('SELECT COUNT(*) INTO @V_EXIST_TABLE FROM information_schema.`TABLES`
						WHERE TABLE_SCHEMA=''',gt_polystar_db,'''
						AND TABLE_NAME=''TABLE_POS_POLYSTAR_LTE_',DATA_QRT,'_',IMSI_GID,''';');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		IF @V_EXIST_TABLE>0 THEN 
	
			SET @SqlCmd=CONCAT('	ALTER  TABLE ',gt_polystar_db,'.TABLE_POS_POLYSTAR_LTE_',DATA_DATE,'
							TRUNCATE PARTITION q',DATA_QRT,'',IMSI_GID,';
								');							
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
				
			SET @SqlCmd=CONCAT('ALTER  TABLE ',gt_polystar_db,'.TABLE_POS_POLYSTAR_LTE_',DATA_DATE,'
						EXCHANGE PARTITION q',DATA_QRT,'',IMSI_GID,'
						WITH TABLE ',gt_polystar_db,'.TABLE_POS_POLYSTAR_LTE_',DATA_QRT,'_',IMSI_GID,'	
						WITHOUT VALIDATION;');					
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
		
			SET @SqlCmd=CONCAT('DROP TABLE IF EXISTS ',gt_polystar_db,'.TABLE_POS_POLYSTAR_LTE_',DATA_QRT,'_',IMSI_GID,';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
		END IF;
	
		INSERT INTO gt_gw_main.sp_log VALUES('gt_gw_main','SP_Polystar_Exchange_Partition2',CONCAT('FINISH TO EXCHANGE PARTITION TABLE_POS_POLYSTAR_LTE_',DATA_QRT,''),NOW());	
	
	END IF;
	IF TECH_MASK=4 AND RPT_TYPE = '2' THEN 
	
		INSERT INTO gt_gw_main.sp_log VALUES('gt_gw_main','SP_Polystar_Exchange_Partition2',CONCAT('START TO EXCHANGE PARTITION RPT_POLYSTAR_LTE_',DATA_HOUR,''),NOW());	
	
		SET @SqlCmd=CONCAT('SELECT GROUP_CONCAT(DISTINCT `TABLE_NAME` ORDER BY `TABLE_NAME` SEPARATOR ''|'') INTO @TABLE_GROUP_HR 
					FROM information_schema.`TABLES` 
					WHERE TABLE_SCHEMA=''',gt_polystar_db,''' AND TABLE_NAME LIKE ''%lte_hr'' AND TABLE_NAME LIKE ''%xdr%'';');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		
		SET @v_m=1;
		SET @v_reg_Max=gt_covmo_csv_count(@TABLE_GROUP_HR,'|');
		WHILE @v_m <= @v_reg_Max DO
		BEGIN
			SET v_group_table_name=gt_strtok(@TABLE_GROUP_HR, @v_m, '|');
	
	
			SET @SqlCmd=CONCAT('SELECT COUNT(*) INTO @V_EXIST_TABLE FROM information_schema.`TABLES`
						WHERE TABLE_SCHEMA=''',gt_polystar_db,'''
						AND TABLE_NAME=''',v_group_table_name,'_',DATA_DATE,'_',DATA_HOUR,''';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			IF @V_EXIST_TABLE>0 THEN 
	
			
				SET @SqlCmd=CONCAT('ALTER  TABLE ',gt_polystar_db,'.',v_group_table_name,'
							TRUNCATE PARTITION h',P_DATE_HR,'',DATA_HOUR,';');					
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
		 
				SET @SqlCmd=CONCAT('ALTER  TABLE ',gt_polystar_db,'.',v_group_table_name,'
							EXCHANGE PARTITION h',P_DATE_HR,'',DATA_HOUR,'
							WITH TABLE ',gt_polystar_db,'.',v_group_table_name,'_',DATA_DATE,'_',DATA_HOUR,'
							WITHOUT VALIDATION;');				
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
		
				SET @SqlCmd=CONCAT('DROP TABLE IF EXISTS ',gt_polystar_db,'.',v_group_table_name,'_',DATA_DATE,'_',DATA_HOUR,';');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
			END IF;
		END;
		SET @v_m=@v_m+1;
		END WHILE; 
	
		INSERT INTO gt_gw_main.sp_log VALUES('gt_gw_main','SP_Polystar_Exchange_Partition2',CONCAT('FINISH TO EXCHANGE PARTITION RPT_POLYSTAR_LTE_',DATA_HOUR,''),NOW());	
	END IF;
	
	IF TECH_MASK=4 AND RPT_TYPE = '3' THEN 
	
		INSERT INTO gt_gw_main.sp_log VALUES('gt_gw_main','SP_Polystar_Exchange_Partition2',CONCAT('START TO EXCHANGE PARTITION RPT_POLYSTAR_LTE_',DATA_DATE,''),NOW());	
	
		SET @SqlCmd=CONCAT('SELECT GROUP_CONCAT(DISTINCT `TABLE_NAME` ORDER BY `TABLE_NAME` SEPARATOR ''|'') INTO @TABLE_GROUP_DY 
					FROM information_schema.`TABLES` 
					WHERE TABLE_SCHEMA=''',gt_polystar_db,''' AND TABLE_NAME LIKE ''%lte_dy'' AND TABLE_NAME LIKE ''%xdr%'';');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		
		SET @v_m=1;
		SET @v_reg_Max=gt_covmo_csv_count(@TABLE_GROUP_DY,'|');
		WHILE @v_m <= @v_reg_Max DO
		BEGIN
			SET v_group_table_name=gt_strtok(@TABLE_GROUP_DY, @v_m, '|');
	
			SET @SqlCmd=CONCAT('SELECT COUNT(*) INTO @V_EXIST_TABLE FROM information_schema.`TABLES`
						WHERE TABLE_SCHEMA=''',gt_polystar_db,'''
						AND TABLE_NAME=''',v_group_table_name,'_',DATA_DATE,''';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			IF @V_EXIST_TABLE>0 THEN 
	
				SET @SqlCmd=CONCAT('ALTER  TABLE ',gt_polystar_db,'.',v_group_table_name,'
							TRUNCATE PARTITION d',P_DATE_DY,';');					
						
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
		 
				SET @SqlCmd=CONCAT('ALTER  TABLE ',gt_polystar_db,'.',v_group_table_name,'
							EXCHANGE PARTITION d',P_DATE_DY,'
							WITH TABLE ',gt_polystar_db,'.',v_group_table_name,'_',DATA_DATE,'
							WITHOUT VALIDATION;');					
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
		
				SET @SqlCmd=CONCAT('DROP TABLE IF EXISTS ',gt_polystar_db,'.',v_group_table_name,'_',DATA_DATE,';');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
			END IF;
		END;
		SET @v_m=@v_m+1;
		END WHILE; 
	
		INSERT INTO gt_gw_main.sp_log VALUES('gt_gw_main','SP_Polystar_Exchange_Partition2',CONCAT('FINISH TO EXCHANGE PARTITION RPT_POLYSTAR_LTE_',DATA_DATE,''),NOW());		
	END IF;
	
	IF TECH_MASK=2 AND RPT_TYPE = '1' THEN 
		INSERT INTO gt_gw_main.sp_log VALUES('gt_gw_main','SP_Polystar_Exchange_Partition2',CONCAT('START TO EXCHANGE PARTITION TABLE_POS_POLYSTAR_UMTS_',DATA_QRT,''),NOW());	
		
		SET @SqlCmd=CONCAT('SELECT COUNT(*) INTO @V_EXIST_TABLE FROM information_schema.`TABLES`
						WHERE TABLE_SCHEMA=''',gt_polystar_db,'''
						AND TABLE_NAME=''TABLE_POS_POLYSTAR_umts_',DATA_QRT,'_',IMSI_GID,''';');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		IF @V_EXIST_TABLE>0 THEN 
	
			SET @SqlCmd=CONCAT('	ALTER  TABLE ',gt_polystar_db,'.TABLE_POS_POLYSTAR_umts_',DATA_DATE,'
							TRUNCATE PARTITION q',DATA_QRT,'',IMSI_GID,';
								');					
						
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
		 
			SET @SqlCmd=CONCAT('ALTER  TABLE ',gt_polystar_db,'.TABLE_POS_POLYSTAR_umts_',DATA_DATE,'
						EXCHANGE PARTITION q',DATA_QRT,'',IMSI_GID,'
						WITH TABLE ',gt_polystar_db,'.TABLE_POS_POLYSTAR_umts_',DATA_QRT,'_',IMSI_GID,'	
						WITHOUT VALIDATION;');					
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
		
			SET @SqlCmd=CONCAT('DROP TABLE IF EXISTS ',gt_polystar_db,'.TABLE_POS_POLYSTAR_umts_',DATA_QRT,'_',IMSI_GID,';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
		END IF;
	
		INSERT INTO gt_gw_main.sp_log VALUES('gt_gw_main','SP_Polystar_Exchange_Partition2',CONCAT('FINISH TO EXCHANGE PARTITION TABLE_POS_POLYSTAR_UMTS_',DATA_QRT,''),NOW());	
	END IF;
	
	IF TECH_MASK=2 AND RPT_TYPE = '2' THEN 
	
		INSERT INTO gt_gw_main.sp_log VALUES('gt_gw_main','SP_Polystar_Exchange_Partition2',CONCAT('START TO EXCHANGE PARTITION RPT_POLYSTAR_UMTS_',DATA_HOUR,''),NOW());	
	
		SET @SqlCmd=CONCAT('SELECT GROUP_CONCAT(DISTINCT `TABLE_NAME` ORDER BY `TABLE_NAME` SEPARATOR ''|'') INTO @TABLE_GROUP_HR 
					FROM information_schema.`TABLES` 
					WHERE TABLE_SCHEMA=''',gt_polystar_db,''' AND TABLE_NAME LIKE ''%umts_hr'' AND TABLE_NAME LIKE ''%xdr%'';');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		
		SET @v_m=1;
		SET @v_reg_Max=gt_covmo_csv_count(@TABLE_GROUP_HR,'|');
		WHILE @v_m <= @v_reg_Max DO
		BEGIN
			SET v_group_table_name=gt_strtok(@TABLE_GROUP_HR, @v_m, '|');
			
			SET @SqlCmd=CONCAT('SELECT COUNT(*) INTO @V_EXIST_TABLE FROM information_schema.`TABLES`
						WHERE TABLE_SCHEMA=''',gt_polystar_db,'''
						AND TABLE_NAME=''',v_group_table_name,'_',DATA_DATE,'_',DATA_HOUR,''';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			IF @V_EXIST_TABLE>0 THEN 
	
				SET @SqlCmd=CONCAT('ALTER  TABLE ',gt_polystar_db,'.',v_group_table_name,'
							TRUNCATE PARTITION h',P_DATE_HR,'',DATA_HOUR,';');					
						
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
		 
				SET @SqlCmd=CONCAT('ALTER  TABLE ',gt_polystar_db,'.',v_group_table_name,'
							EXCHANGE PARTITION h',P_DATE_HR,'',DATA_HOUR,'
							WITH TABLE ',gt_polystar_db,'.',v_group_table_name,'_',DATA_DATE,'_',DATA_HOUR,'
							WITHOUT VALIDATION;');					
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
			
	
				SET @SqlCmd=CONCAT('DROP TABLE IF EXISTS ',gt_polystar_db,'.',v_group_table_name,'_',DATA_DATE,'_',DATA_HOUR,';');
		
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
			END IF;
			
		END;
		SET @v_m=@v_m+1;
		END WHILE; 
	
		INSERT INTO gt_gw_main.sp_log VALUES('gt_gw_main','SP_Polystar_Exchange_Partition2',CONCAT('FINISH TO EXCHANGE PARTITION RPT_POLYSTAR_UMTS_',DATA_HOUR,''),NOW());	
	END IF;
	
	IF TECH_MASK=2 AND RPT_TYPE = '3' THEN 
	
		INSERT INTO gt_gw_main.sp_log VALUES('gt_gw_main','SP_Polystar_Exchange_Partition2',CONCAT('START TO EXCHANGE PARTITION RPT_POLYSTAR_UMTS_',DATA_DATE,''),NOW());	
	
		SET @SqlCmd=CONCAT('SELECT GROUP_CONCAT(DISTINCT `TABLE_NAME` ORDER BY `TABLE_NAME` SEPARATOR ''|'') INTO @TABLE_GROUP_DY 
					FROM information_schema.`TABLES` 
					WHERE TABLE_SCHEMA=''',gt_polystar_db,''' AND TABLE_NAME LIKE ''%umts_dy'' AND TABLE_NAME LIKE ''%xdr%'';');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		
		SET @v_m=1;
		SET @v_reg_Max=gt_covmo_csv_count(@TABLE_GROUP_DY,'|');
		WHILE @v_m <= @v_reg_Max DO
		BEGIN
			SET v_group_table_name=gt_strtok(@TABLE_GROUP_DY, @v_m, '|');
		
			SET @SqlCmd=CONCAT('SELECT COUNT(*) INTO @V_EXIST_TABLE FROM information_schema.`TABLES`
						WHERE TABLE_SCHEMA=''',gt_polystar_db,'''
						AND TABLE_NAME=''',v_group_table_name,'_',DATA_DATE,''';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			IF @V_EXIST_TABLE>0 THEN 
	
				SET @SqlCmd=CONCAT('ALTER  TABLE ',gt_polystar_db,'.',v_group_table_name,'
							TRUNCATE PARTITION d',P_DATE_DY,';');					
						
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
		 
				SET @SqlCmd=CONCAT('ALTER  TABLE ',gt_polystar_db,'.',v_group_table_name,'
							EXCHANGE PARTITION d',P_DATE_DY,'
							WITH TABLE ',gt_polystar_db,'.',v_group_table_name,'_',DATA_DATE,'
							WITHOUT VALIDATION;');					
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
		
				SET @SqlCmd=CONCAT('DROP TABLE IF EXISTS ',gt_polystar_db,'.',v_group_table_name,'_',DATA_DATE,';');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
			END IF;
		END;
		SET @v_m=@v_m+1;
		END WHILE; 
	
		INSERT INTO gt_gw_main.sp_log VALUES('gt_gw_main','SP_Polystar_Exchange_Partition2',CONCAT('FINISH TO EXCHANGE PARTITION RPT_POLYSTAR_UMTS_',DATA_DATE,''),NOW());		
	END IF;
	
	INSERT INTO gt_gw_main.SP_LOG VALUES(GT_DB,'SP_Polystar_Exchange_Partition2',CONCAT('Done: ',TIMESTAMPDIFF(SECOND,START_TIME,SYSDATE()),' seconds.'), NOW());
END$$
DELIMITER ;
