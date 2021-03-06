DELIMITER $$
USE `gt_gw_main`$$
DROP PROCEDURE IF EXISTS `SP_CreateDB_LTE`$$
CREATE DEFINER=`covmo`@`%` PROCEDURE `SP_Generate_NT_Sub_Check`(IN GT_DB VARCHAR(100),IN TBL VARCHAR(50),IN COL VARCHAR(50)
							,IN RULE VARCHAR(50),IN DEFAULT_VAL VARCHAR(50),IN TECH CHAR(5))
BEGIN
	
	DECLARE WORKER_ID VARCHAR(10) DEFAULT CONNECTION_ID();
	DECLARE rule_range_check INT DEFAULT 0;
	DECLARE d_min DOUBLE;
	DECLARE d_max DOUBLE;
	
	
	DECLARE ID VARCHAR(10) DEFAULT CONNECTION_ID();
	IF TECH = 'UMTS' THEN
		SET ID = 'RNC_ID';
	ELSEIF TECH = 'LTE' THEN
		SET ID = 'ENODEB_ID';
	ELSEIF TECH = 'GSM' THEN
		SET ID = 'BSC_ID';
	END IF;
	
	SET @SqlCmd=CONCAT('SELECT COUNT(*) INTO @NULL_CHECK_CNT FROM ',GT_DB,'.',TBL,' WHERE (',COL,' IS NULL OR ',COL,' = '''') AND (',COL,' IS NULL OR ',COL,' != 0);');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt; 
	
	IF @NULL_CHECK_CNT > 0 THEN
		
		SET @SqlCmd=CONCAT('INSERT INTO ',GT_DB,'.nt_log (ID,CELL_ID,TBL_NAME,COL_NAME,LOG_TYPE,DUMP_LOG)
				SELECT
					',ID,'
					,CELL_ID
					,''',TBL,'''
					,''',COL,'''
					,''2'' # 2 IS MODIFY
					,''DATA IS NULL''
				FROM ',GT_DB,'.',TBL,'
				WHERE (',COL,' IS NULL OR ',COL,' = '''') AND (',COL,' IS NULL OR ',COL,' != 0);');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt; 
	
		
		SET @SqlCmd=CONCAT('UPDATE ',GT_DB,'.',TBL,'
				SET ',COL,' = ',DEFAULT_VAL,'
					,FLAG = FLAG + 1
				WHERE (',COL,' IS NULL OR ',COL,' = '''') AND (',COL,' IS NULL OR ',COL,' != 0);');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt; 
	END IF;
	
	
	IF RULE != '' THEN
		SET rule_range_check = gt_covmo_csv_count(RULE,'to');
		IF rule_range_check > 1 THEN
			SET d_min = gt_strtok(RULE,1,'to');
			SET d_max = gt_strtok(RULE,2,'to');
			SET @SqlCmd=CONCAT('SELECT COUNT(*) INTO @R1_CHECK_CNT FROM ',GT_DB,'.',TBL,' WHERE ',COL,' > ',d_max,' or ',COL,' < ',d_min,';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt; 
			
			IF @R1_CHECK_CNT > 0 THEN
				
				SET @SqlCmd=CONCAT('INSERT INTO ',GT_DB,'.nt_log (ID,CELL_ID,TBL_NAME,COL_NAME,LOG_TYPE,DUMP_LOG)
						SELECT
							',ID,'
							,CELL_ID
							,''',TBL,'''
							,''',COL,'''
							,''2'' # 2 IS Illegal
							,CONCAT(''DATA IS Illegal:'',',COL,')
						FROM ',GT_DB,'.',TBL,'
						WHERE ',COL,' > ',d_max,' or ',COL,' < ',d_min,';');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt; 
			
				
				SET @SqlCmd=CONCAT('UPDATE ',GT_DB,'.',TBL,'
						SET ',COL,' = ',DEFAULT_VAL,'
							,FLAG = FLAG + 1
						WHERE ',COL,' > ',d_max,' or ',COL,' < ',d_min,';');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt; 
			END IF;
		ELSE
	
			SET @SqlCmd=CONCAT('SELECT COUNT(*) INTO @R2_CHECK_CNT FROM ',GT_DB,'.',TBL,' WHERE ',COL,' ',RULE,';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt; 
	
			IF @R2_CHECK_CNT  > 0 THEN
				
				SET @SqlCmd=CONCAT('INSERT INTO ',GT_DB,'.nt_log (ID,CELL_ID,TBL_NAME,COL_NAME,LOG_TYPE,DUMP_LOG)
						SELECT
							',ID,'
							,CELL_ID
							,''',TBL,'''
							,''',COL,'''
							,''2'' # 2 IS MODIFY
							,CONCAT(''DATA IS Illegal:'',',COL,')
						FROM ',GT_DB,'.',TBL,'
						WHERE ',COL,' ',RULE,';');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt; 
			
				
				SET @SqlCmd=CONCAT('UPDATE ',GT_DB,'.',TBL,'
						SET ',COL,' = ',DEFAULT_VAL,'
							,FLAG = FLAG + 1
						WHERE ',COL,' ',RULE,';');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt; 
			END IF;
		END IF;
	END IF;
END$$
DELIMITER ;
