DELIMITER $$
USE `gt_gw_main`$$
DROP PROCEDURE IF EXISTS `SP_CreateDB_LTE`$$
CREATE DEFINER=`covmo`@`%` PROCEDURE `SP_Generate_NT_Sub_Check_Special`(IN GT_DB VARCHAR(100),IN TBL VARCHAR(50),IN COL VARCHAR(50)
							,IN RULE VARCHAR(50),IN TECH CHAR(5))
BEGIN
	
	DECLARE WORKER_ID VARCHAR(10) DEFAULT CONNECTION_ID();
	DECLARE JOIN_TBL VARCHAR(30);
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
	IF COL IN ('BEAMWIDTH_H','BEAM_WIDTH_HORIZONTAL') THEN
		
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
					SET ',COL,' = CASE WHEN ANTENNA_TYPE = 2 THEN 65 ELSE 360 END
						,FLAG = FLAG + 1
					WHERE (',COL,' IS NULL OR ',COL,' = '''') AND (',COL,' IS NULL OR ',COL,' != 0);;');
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
							SET ',COL,' = CASE WHEN ANTENNA_TYPE = 2 THEN 65 ELSE 360 END
								,FLAG = FLAG + 1
							WHERE ',COL,' > ',d_max,' or ',COL,' < ',d_min,';');
					PREPARE Stmt FROM @SqlCmd;
					EXECUTE Stmt;
					DEALLOCATE PREPARE Stmt; 
				END IF;
			END IF;
		END IF;
	ELSEIF COL = 'ANTENNA_TYPE' THEN	
		IF TECH = 'UMTS' THEN
			SET JOIN_TBL = 'nt_current';
		ELSEIF TECH = 'LTE' THEN
			SET JOIN_TBL = 'nt_cell_current_lte';
		ELSEIF TECH = 'GSM' THEN
			SET JOIN_TBL = 'nt_cell_current_gsm';
		END IF;
	
		
		SET @SqlCmd=CONCAT('SELECT COUNT(*) INTO @NULL_CHECK_CNT FROM ',GT_DB,'.',TBL,' WHERE (',COL,' IS NULL OR ',COL,' = '''') AND (',COL,' IS NULL OR ',COL,' != 0);');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt; 
		IF @NULL_CHECK_CNT > 0 THEN
			SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE  IF EXISTS ',GT_DB,'.tmp_table_nt_check_',WORKER_ID,';');
			PREPARE stmt FROM @sqlcmd;
			EXECUTE stmt;
			DEALLOCATE PREPARE stmt;
		
			SET @SqlCmd=CONCAT('CREATE TEMPORARY TABLE ',GT_DB,'.`tmp_table_nt_check_',WORKER_ID,'`
					SELECT ',ID,',CELL_ID,',COL,' FROM ',GT_DB,'.',TBL,' WHERE (',COL,' IS NULL OR ',COL,' = '''') AND (',COL,' IS NULL OR ',COL,' != 0);;');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt; 
			
			SET @SqlCmd=CONCAT('INSERT INTO ',GT_DB,'.nt_log (ID,CELL_ID,TBL_NAME,COL_NAME,LOG_TYPE,DUMP_LOG)
					SELECT
						',ID,'
						,CELL_ID
						,''',TBL,'''
						,''',COL,'''
						,''2'' # 2 IS MODIFY
						,''DATA IS NULL''
					FROM ',GT_DB,'.`tmp_table_nt_check_',WORKER_ID,'`;');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt; 
		
			
			SET @SqlCmd=CONCAT('UPDATE ',GT_DB,'.`tmp_table_nt_check_',WORKER_ID,'` T,',GT_DB,'.',TBL,' A, ',GT_DB,'.',JOIN_TBL,' B
					SET A.',COL,' = CASE WHEN B.INDOOR = 1 THEN 1 ELSE 2 END
						,A.FLAG = A.FLAG + 1
					WHERE 
					T.',ID,' = A.',ID,' AND T.CELL_ID = A.CELL_ID
					AND A.',ID,' = B.',ID,' AND A.CELL_ID = B.CELL_ID;');
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
					SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE  IF EXISTS ',GT_DB,'.tmp_table_nt_check_',WORKER_ID,';');
					PREPARE stmt FROM @sqlcmd;
					EXECUTE stmt;
					DEALLOCATE PREPARE stmt;
				
					SET @SqlCmd=CONCAT('CREATE TEMPORARY TABLE ',GT_DB,'.`tmp_table_nt_check_',WORKER_ID,'`
							SELECT ',ID,',CELL_ID,',COL,' FROM ',GT_DB,'.',TBL,' WHERE ',COL,' > ',d_max,' or ',COL,' < ',d_min,';');
					PREPARE Stmt FROM @SqlCmd;
					EXECUTE Stmt;
					DEALLOCATE PREPARE Stmt; 
					
					SET @SqlCmd=CONCAT('INSERT INTO ',GT_DB,'.nt_log (ID,CELL_ID,TBL_NAME,COL_NAME,LOG_TYPE,DUMP_LOG)
							SELECT
								',ID,'
								,CELL_ID
								,''',TBL,'''
								,''',COL,'''
								,''2'' # 2 IS Illegal
								,CONCAT(''DATA IS Illegal:'',',COL,')
							FROM ',GT_DB,'.`tmp_table_nt_check_',WORKER_ID,'`;');
					PREPARE Stmt FROM @SqlCmd;
					EXECUTE Stmt;
					DEALLOCATE PREPARE Stmt; 
				
					
					SET @SqlCmd=CONCAT('UPDATE ',GT_DB,'.`tmp_table_nt_check_',WORKER_ID,'` T,',GT_DB,'.',TBL,' A, ',GT_DB,'.',JOIN_TBL,' B
							SET A.',COL,' = CASE WHEN B.INDOOR = 1 THEN 1 ELSE 2 END
								,A.FLAG = A.FLAG + 1
							WHERE 
							T.',ID,' = A.',ID,' AND T.CELL_ID = A.CELL_ID
							AND A.',ID,' = B.',ID,' AND A.CELL_ID = B.CELL_ID;');
					PREPARE Stmt FROM @SqlCmd;
					EXECUTE Stmt;
					DEALLOCATE PREPARE Stmt; 
				END IF;
			END IF;
		END IF;
	
	ELSEIF COL IN ('HEIGHT','ANTENNA_HEIGHT') THEN
	
		IF TECH = 'UMTS' THEN
			
			SET @SqlCmd=CONCAT('UPDATE ',GT_DB,'.',TBL,' A 
			JOIN ',GT_DB,'.nt_current B
			ON A.RNC_ID = B.RNC_ID AND A.CELL_ID = B.CELL_ID
			SET A.HEIGHT = 
             CASE 
                WHEN B.INDOOR = 1 and A.HEIGHT is null THEN 0
                WHEN B.INDOOR = 1 THEN A.HEIGHT
                WHEN A.HEIGHT < 3 THEN 3
                WHEN A.HEIGHT > 60 THEN 60
                WHEN A.HEIGHT IS NULL THEN 30
                ELSE A.HEIGHT
            END
			,A.FLAG = A.FLAG + 1;');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
			
		ELSEIF TECH = 'LTE' THEN
			
			SET @SqlCmd=CONCAT('UPDATE ',GT_DB,'.',TBL,' A 
			JOIN ',GT_DB,'.nt_cell_current_lte B
			ON A.ENODEB_ID = B.ENODEB_ID AND A.CELL_ID = B.CELL_ID
			SET A.ANTENNA_HEIGHT = 
            CASE 
                WHEN B.INDOOR = 1 and A.ANTENNA_HEIGHT is null THEN 0
                WHEN B.INDOOR = 1 THEN A.ANTENNA_HEIGHT
                WHEN A.ANTENNA_HEIGHT < 3 THEN 3
                WHEN A.ANTENNA_HEIGHT > 60 THEN 60
                WHEN A.ANTENNA_HEIGHT IS NULL THEN 30
                ELSE A.ANTENNA_HEIGHT
            END
			,A.FLAG = A.FLAG + 1;');
	
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
			
			
		ELSEIF TECH = 'GSM' THEN
			
			SET @SqlCmd=CONCAT('UPDATE ',GT_DB,'.',TBL,' A 
			JOIN ',GT_DB,'.nt_cell_current_gsm B
			ON A.BSC_ID = B.BSC_ID AND A.CELL_ID = B.CELL_ID
			SET A.HEIGHT = 
            CASE 
                WHEN B.INDOOR = 1 and A.HEIGHT is null THEN 0
                WHEN B.INDOOR = 1 THEN A.HEIGHT
                WHEN A.HEIGHT < 3 THEN 3
                WHEN A.HEIGHT > 60 THEN 60
                WHEN A.HEIGHT IS NULL THEN 30
                ELSE A.HEIGHT
            END
			,A.FLAG = A.FLAG + 1;');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
			
			
		END IF;
	
	ELSEIF COL = 'ANTENNA_GAIN' THEN
		IF TECH = 'UMTS' THEN
			SET JOIN_TBL = 'nt_current';
		ELSEIF TECH = 'LTE' THEN
			SET JOIN_TBL = 'nt_cell_current_lte';
		ELSEIF TECH = 'GSM' THEN
			SET JOIN_TBL = 'nt_cell_current_gsm';
		END IF;
		
		SET @SqlCmd=CONCAT('SELECT COUNT(*) INTO @NULL_CHECK_CNT FROM ',GT_DB,'.',TBL,' WHERE (',COL,' IS NULL OR ',COL,' = '''') AND (',COL,' IS NULL OR ',COL,' != 0);');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt; 
		IF @NULL_CHECK_CNT > 0 THEN
			SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE  IF EXISTS ',GT_DB,'.tmp_table_nt_check_',WORKER_ID,';');
			PREPARE stmt FROM @sqlcmd;
			EXECUTE stmt;
			DEALLOCATE PREPARE stmt;
		
			SET @SqlCmd=CONCAT('CREATE TEMPORARY TABLE ',GT_DB,'.`tmp_table_nt_check_',WORKER_ID,'`
					SELECT ',ID,',CELL_ID,',COL,' FROM ',GT_DB,'.',TBL,' WHERE (',COL,' IS NULL OR ',COL,' = '''') AND (',COL,' IS NULL OR ',COL,' != 0);;');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt; 
			
			SET @SqlCmd=CONCAT('INSERT INTO ',GT_DB,'.nt_log (ID,CELL_ID,TBL_NAME,COL_NAME,LOG_TYPE,DUMP_LOG)
					SELECT
						',ID,'
						,CELL_ID
						,''',TBL,'''
						,''',COL,'''
						,''2'' # 2 IS MODIFY
						,''DATA IS NULL''
					FROM ',GT_DB,'.`tmp_table_nt_check_',WORKER_ID,'`;');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt; 
			
			SET @SqlCmd=CONCAT('UPDATE ',GT_DB,'.`tmp_table_nt_check_',WORKER_ID,'` T,',GT_DB,'.',TBL,' A, ',GT_DB,'.',JOIN_TBL,' B
					SET A.',COL,' = CASE WHEN B.INDOOR = 1 THEN 7 ELSE 16.75 END
						,A.FLAG = A.FLAG + 1
					WHERE 
					T.',ID,' = A.',ID,' AND T.CELL_ID = A.CELL_ID
					AND A.',ID,' = B.',ID,' AND A.CELL_ID = B.CELL_ID;');
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
					SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE  IF EXISTS ',GT_DB,'.tmp_table_nt_check_',WORKER_ID,';');
					PREPARE stmt FROM @sqlcmd;
					EXECUTE stmt;
					DEALLOCATE PREPARE stmt;
				
					SET @SqlCmd=CONCAT('CREATE TEMPORARY TABLE ',GT_DB,'.`tmp_table_nt_check_',WORKER_ID,'`
							SELECT ',ID,',CELL_ID,',COL,' FROM ',GT_DB,'.',TBL,' WHERE ',COL,' > ',d_max,' or ',COL,' < ',d_min,';');
					PREPARE Stmt FROM @SqlCmd;
					EXECUTE Stmt;
					DEALLOCATE PREPARE Stmt; 
					
					SET @SqlCmd=CONCAT('INSERT INTO ',GT_DB,'.nt_log (ID,CELL_ID,TBL_NAME,COL_NAME,LOG_TYPE,DUMP_LOG)
							SELECT
								',ID,'
								,CELL_ID
								,''',TBL,'''
								,''',COL,'''
								,''2'' # 2 IS Illegal
								,CONCAT(''DATA IS Illegal:'',',COL,')
							FROM ',GT_DB,'.`tmp_table_nt_check_',WORKER_ID,'`;');
					PREPARE Stmt FROM @SqlCmd;
					EXECUTE Stmt;
					DEALLOCATE PREPARE Stmt; 
				
					
					SET @SqlCmd=CONCAT('UPDATE ',GT_DB,'.`tmp_table_nt_check_',WORKER_ID,'` T,',GT_DB,'.',TBL,' A, ',GT_DB,'.',JOIN_TBL,' B
							SET A.',COL,' = CASE WHEN B.INDOOR = 1 THEN 7 ELSE 16.75 END
								,A.FLAG = A.FLAG + 1
							WHERE 
							T.',ID,' = A.',ID,' AND T.CELL_ID = A.CELL_ID
							AND A.',ID,' = B.',ID,' AND A.CELL_ID = B.CELL_ID;');
					PREPARE Stmt FROM @SqlCmd;
					EXECUTE Stmt;
					DEALLOCATE PREPARE Stmt; 
				END IF;
			END IF;
		END IF;	
	ELSEIF COL = 'REFERENCE_SIGNAL_POWER ' THEN
		IF TECH = 'UMTS' THEN
			SET JOIN_TBL = 'xxxx';
		ELSEIF TECH = 'LTE' THEN
			SET JOIN_TBL = 'nt_antenna_current_lte';
		ELSEIF TECH = 'GSM' THEN
			SET JOIN_TBL = 'xxxx';
		END IF;
		
		SET @SqlCmd=CONCAT('SELECT COUNT(*) INTO @NULL_CHECK_CNT FROM ',GT_DB,'.',TBL,' WHERE (',COL,' IS NULL OR ',COL,' = '''') AND (',COL,' IS NULL OR ',COL,' != 0);');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt; 
		IF @NULL_CHECK_CNT > 0 THEN
			SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE  IF EXISTS ',GT_DB,'.tmp_table_nt_check_',WORKER_ID,';');
			PREPARE stmt FROM @sqlcmd;
			EXECUTE stmt;
			DEALLOCATE PREPARE stmt;
		
			SET @SqlCmd=CONCAT('CREATE TEMPORARY TABLE ',GT_DB,'.`tmp_table_nt_check_',WORKER_ID,'`
					SELECT ',ID,',CELL_ID,',COL,' FROM ',GT_DB,'.',TBL,' WHERE (',COL,' IS NULL OR ',COL,' = '''') AND (',COL,' IS NULL OR ',COL,' != 0);;');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt; 
			
			SET @SqlCmd=CONCAT('INSERT INTO ',GT_DB,'.nt_log (ID,CELL_ID,TBL_NAME,COL_NAME,LOG_TYPE,DUMP_LOG)
					SELECT
						',ID,'
						,CELL_ID
						,''',TBL,'''
						,''',COL,'''
						,''2'' # 2 IS MODIFY
						,''DATA IS NULL''
					FROM ',GT_DB,'.`tmp_table_nt_check_',WORKER_ID,'`;');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt; 
			
			SET @SqlCmd=CONCAT('UPDATE ',GT_DB,'.`tmp_table_nt_check_',WORKER_ID,'` T,',GT_DB,'.',TBL,' A
					SET A.',COL,' = CASE WHEN A.INDOOR_TYPE = 1 THEN 7 WHEN A.INDOOR_TYPE = 0 THEN 20 END
						,A.FLAG = A.FLAG + 1
					WHERE 
					T.',ID,' = A.',ID,' AND T.CELL_ID = A.CELL_ID;');
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
					SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE  IF EXISTS ',GT_DB,'.tmp_table_nt_check_',WORKER_ID,';');
					PREPARE stmt FROM @sqlcmd;
					EXECUTE stmt;
					DEALLOCATE PREPARE stmt;
				
					SET @SqlCmd=CONCAT('CREATE TEMPORARY TABLE ',GT_DB,'.`tmp_table_nt_check_',WORKER_ID,'`
							SELECT ',ID,',CELL_ID,',COL,' FROM ',GT_DB,'.',TBL,' WHERE ',COL,' > ',d_max,' or ',COL,' < ',d_min,';');
					PREPARE Stmt FROM @SqlCmd;
					EXECUTE Stmt;
					DEALLOCATE PREPARE Stmt; 
					
					SET @SqlCmd=CONCAT('INSERT INTO ',GT_DB,'.nt_log (ID,CELL_ID,TBL_NAME,COL_NAME,LOG_TYPE,DUMP_LOG)
							SELECT
								',ID,'
								,CELL_ID
								,''',TBL,'''
								,''',COL,'''
								,''2'' # 2 IS Illegal
								,CONCAT(''DATA IS Illegal:'',',COL,')
							FROM ',GT_DB,'.`tmp_table_nt_check_',WORKER_ID,'`;');
					PREPARE Stmt FROM @SqlCmd;
					EXECUTE Stmt;
					DEALLOCATE PREPARE Stmt; 
				
					
					SET @SqlCmd=CONCAT('UPDATE ',GT_DB,'.`tmp_table_nt_check_',WORKER_ID,'` T,',GT_DB,'.',TBL,' A
							SET A.',COL,' = CASE WHEN A.INDOOR_TYPE = 1 THEN 7 WHEN A.INDOOR_TYPE = 0 THEN 20 END
								,A.FLAG = A.FLAG + 1
							WHERE 
							T.',ID,' = A.',ID,' AND T.CELL_ID = A.CELL_ID;');
					PREPARE Stmt FROM @SqlCmd;
					EXECUTE Stmt;
					DEALLOCATE PREPARE Stmt; 
				END IF;
			END IF;
		END IF;
	ELSEIF COL = 'INDOOR_TYPE ' THEN
		IF TECH = 'UMTS' THEN
			SET JOIN_TBL = 'xxxx';
		ELSEIF TECH = 'LTE' THEN
			SET JOIN_TBL = 'xxxx';
		ELSEIF TECH = 'GSM' THEN
			SET JOIN_TBL = 'nt_antenna_current_gsm';
		END IF;
	
		SET @SqlCmd=CONCAT('SELECT COUNT(*) INTO @NULL_CHECK_CNT FROM ',GT_DB,'.',TBL,' WHERE (',COL,' IS NULL OR ',COL,' = '''') AND (',COL,' IS NULL OR ',COL,' != 0);');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt; 
	
		IF @NULL_CHECK_CNT > 0 THEN
			SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE  IF EXISTS ',GT_DB,'.tmp_table_nt_check_',WORKER_ID,';');
			PREPARE stmt FROM @sqlcmd;
			EXECUTE stmt;
			DEALLOCATE PREPARE stmt;
		
			SET @SqlCmd=CONCAT('CREATE TEMPORARY TABLE ',GT_DB,'.`tmp_table_nt_check_',WORKER_ID,'`
					SELECT ',ID,',CELL_ID,',COL,' FROM ',GT_DB,'.',TBL,' WHERE (',COL,' IS NULL OR ',COL,' = '''') AND (',COL,' IS NULL OR ',COL,' != 0);;');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt; 
			
			SET @SqlCmd=CONCAT('INSERT INTO ',GT_DB,'.nt_log (ID,CELL_ID,TBL_NAME,COL_NAME,LOG_TYPE,DUMP_LOG)
					SELECT
						',ID,'
						,CELL_ID
						,''',TBL,'''
						,''',COL,'''
						,''2'' # 2 IS MODIFY
						,''DATA IS NULL''
					FROM ',GT_DB,'.`tmp_table_nt_check_',WORKER_ID,'`;');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt; 
			
			SET @SqlCmd=CONCAT('UPDATE ',GT_DB,'.',TBL,' A 
						JOIN ',GT_DB,'.nt_cell_current_gsm B
						ON A.BSC_ID = B.BSC_ID AND A.CELL_ID = B.CELL_ID
						SET A.INDOOR_TYPE = 
				    CASE 
					WHEN B.INDOOR IS NULL and A.INDOOR_TYPE is null THEN 0
					WHEN A.INDOOR_TYPE IS NULL THEN B.INDOOR
					ELSE A.INDOOR_TYPE
				    END
						,A.FLAG = A.FLAG + 1;');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt; 
		END IF;
	
	END IF;
	
END$$
DELIMITER ;
