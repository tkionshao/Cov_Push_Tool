DELIMITER $$
USE `gt_gw_main`$$
DROP PROCEDURE IF EXISTS `SP_CreateDB_LTE`$$
CREATE DEFINER=`covmo`@`%` PROCEDURE `SP_Generate_NT_Sub_Dump_Special`(IN GT_DB VARCHAR(100),IN SOURCE_TBL VARCHAR(50),IN DUMP_TBL VARCHAR(50),IN COL VARCHAR(50),IN TECH VARCHAR(5), IN SPE_TYPE VARCHAR(50))
BEGIN
	
	DECLARE WORKER_ID VARCHAR(10) DEFAULT CONNECTION_ID();
	DECLARE ID VARCHAR(10);
	DECLARE CELL VARCHAR(30);
	DECLARE ANTENNA VARCHAR(30);
	DECLARE NBR_ID VARCHAR(30);
	DECLARE ID_TBL VARCHAR(30);
	
	IF TECH = 'UMTS' THEN
		SET ID = 'RNC_ID';
		SET CELL = 'nt_current';
		SET ANTENNA = 'nt_antenna_current';
		SET NBR_ID = 'NBR_RNC_ID';
		SET ID_TBL = 'nt_rnc_current';
	ELSEIF TECH = 'LTE' THEN
		SET ID = 'ENODEB_ID';
		SET CELL = 'nt_cell_current_lte';
		SET ANTENNA = 'nt_antenna_current_lte';
		SET NBR_ID = 'NBR_ENODEB_ID';
		SET ID_TBL = 'x';
	ELSEIF TECH = 'GSM' THEN
		SET ID = 'BSC_ID';
		SET CELL = 'nt_cell_current_gsm';
		SET ANTENNA = 'nt_antenna_current_gsm';
		SET NBR_ID = 'NBR_BSC_ID';
		SET ID_TBL = 'nt_cell_current_gsm';
	END IF;
	
	IF SPE_TYPE = 'location' THEN
		SET @WHERE_STR = 'LONGITUDE > @max_long
				OR LONGITUDE < @min_long
				OR LATITUDE > @max_lat
				OR LATITUDE < @min_lat';
	
		SET @SqlCmd=CONCAT('SELECT COUNT(*) INTO @CHECK_CNT FROM ',GT_DB,'.',SOURCE_TBL,' 
			WHERE ',@WHERE_STR,';');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt; 
	ELSEIF SPE_TYPE = 'location2' THEN
		SET @WHERE_STR = 'LONGITUDE =0
				OR LATITUDE =0';
	
		SET @SqlCmd=CONCAT('SELECT COUNT(*) INTO @CHECK_CNT FROM ',GT_DB,'.',SOURCE_TBL,' 
			WHERE ',@WHERE_STR,';');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt; 
	
	ELSEIF SPE_TYPE = 'mapping_with_cell' THEN
		SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS ',GT_DB,'.tmp_nt_check','_',WORKER_ID,' ;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		
		SET @SqlCmd=CONCAT('CREATE TEMPORARY TABLE ',GT_DB,'.tmp_nt_check','_',WORKER_ID,'
			SELECT A.*
			FROM ',GT_DB,'.',SOURCE_TBL,' A FORCE INDEX (ID_KEY) LEFT JOIN ',GT_DB,'.',CELL,' B
			ON A.',ID,'=B.',ID,' AND A.CELL_ID = B.CELL_ID
			WHERE B.CELL_ID IS NULL
		;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;	
	
		SET @SqlCmd=CONCAT('SELECT COUNT(*) INTO @CHECK_CNT FROM ',GT_DB,'.tmp_nt_check','_',WORKER_ID,';');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt; 
	
	ELSEIF SPE_TYPE = 'mapping_with_antenna' THEN
		SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS ',GT_DB,'.tmp_nt_check','_',WORKER_ID,' ;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		
		SET @SqlCmd=CONCAT('CREATE TEMPORARY TABLE ',GT_DB,'.tmp_nt_check','_',WORKER_ID,'
			SELECT A.*
			FROM ',GT_DB,'.',SOURCE_TBL,' A LEFT JOIN ',GT_DB,'.',ANTENNA,' B
			ON A.',ID,'=B.',ID,' AND A.CELL_ID = B.CELL_ID
			WHERE B.CELL_ID IS NULL
		;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;	
	
		SET @SqlCmd=CONCAT('SELECT COUNT(*) INTO @CHECK_CNT FROM ',GT_DB,'.tmp_nt_check','_',WORKER_ID,';');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt; 
	
	ELSEIF SPE_TYPE = 'azimuth_with_outdoor' THEN
		SET @SqlCmd=CONCAT('SELECT COUNT(*) INTO @CHECK_CNT FROM ',GT_DB,'.',SOURCE_TBL,' 
			WHERE AZIMUTH IS NULL AND INDOOR_TYPE=0;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt; 
	
	ELSEIF SPE_TYPE = 'neighbor_source_check_with_cell' THEN
		SET @SqlCmd=CONCAT('CREATE INDEX del1 ON ',GT_DB,'.',SOURCE_TBL,' (',ID,' ',IF(TECH='GSM',',LAC',''),' ',IF(TECH='UMTS',',NBR_TYPE',''),');');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
	
		SET @CHECK_CNT=1;
	ELSEIF SPE_TYPE = 'neighbor_target_check_with_cell' THEN
		SET @SqlCmd=CONCAT('CREATE INDEX del2 ON ',GT_DB,'.',SOURCE_TBL,' (',NBR_ID,' ',IF(TECH='GSM',',NBR_CELL_LAC',''),' ',IF(TECH='UMTS',',NBR_TYPE',''),');');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		SET @CHECK_CNT=1;
	END IF;
	
	
	
	IF @CHECK_CNT > 0 THEN
		IF SPE_TYPE = 'location' THEN
			
			SET @SqlCmd=CONCAT('INSERT INTO ',GT_DB,'.nt_log (ID,CELL_ID,TBL_NAME,COL_NAME,LOG_TYPE,DUMP_LOG)
					SELECT
						',ID,'
						,CELL_ID
						,''',SOURCE_TBL,'''
						,''LONG/LAT''
						,''1'' # 1 IS DUMP
						,''LONG OR LAT OUT OF RANGE''
					FROM ',GT_DB,'.',SOURCE_TBL,'
					WHERE ',@WHERE_STR,';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt; 
		
			
			SET @SqlCmd=CONCAT('INSERT INTO ',GT_DB,'.',DUMP_TBL,'
					SELECT * FROM ',GT_DB,'.',SOURCE_TBL,' WHERE ',@WHERE_STR,';
					');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt; 
			
			SET @SqlCmd=CONCAT('UPDATE ',GT_DB,'.',DUMP_TBL,'
								SET FLAG = 1000
								WHERE ',@WHERE_STR,';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt; 	
			
			SET @SqlCmd=CONCAT('DELETE FROM ',GT_DB,'.',SOURCE_TBL,' WHERE ',@WHERE_STR,';
					');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt; 
	
		ELSEIF SPE_TYPE = 'location2' THEN
	
			SET @SqlCmd=CONCAT('INSERT INTO ',GT_DB,'.nt_log (ID,CELL_ID,TBL_NAME,COL_NAME,LOG_TYPE,DUMP_LOG)
					SELECT
						',ID,'
						,CELL_ID
						,''',SOURCE_TBL,'''
						,''LONG/LAT''
						,''1'' # 1 IS DUMP
						,''Data with Invalid Value''
					FROM ',GT_DB,'.',SOURCE_TBL,'
					WHERE ',@WHERE_STR,';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt; 
		
			
			SET @SqlCmd=CONCAT('INSERT INTO ',GT_DB,'.',DUMP_TBL,'
					SELECT * FROM ',GT_DB,'.',SOURCE_TBL,' WHERE ',@WHERE_STR,';
					');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt; 
			
			SET @SqlCmd=CONCAT('UPDATE ',GT_DB,'.',DUMP_TBL,'
								SET FLAG = 1000
								WHERE ',@WHERE_STR,';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt; 
		
			
			SET @SqlCmd=CONCAT('DELETE FROM ',GT_DB,'.',SOURCE_TBL,' WHERE ',@WHERE_STR,';
					');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt; 
	
		ELSEIF SPE_TYPE = 'azimuth_with_outdoor' THEN
			
			SET @SqlCmd=CONCAT('INSERT INTO ',GT_DB,'.nt_log (ID,CELL_ID,TBL_NAME,COL_NAME,LOG_TYPE,DUMP_LOG)
					SELECT
						',ID,'
						,CELL_ID
						,''',SOURCE_TBL,'''
						,''',COL,'''
						,''1'' # 1 IS DUMP
						,''AZIMUTH IS NULL AND OUTDOOR''
					FROM ',GT_DB,'.',SOURCE_TBL,'
					WHERE AZIMUTH IS NULL AND INDOOR_TYPE=0;');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt; 
		
			
			SET @SqlCmd=CONCAT('INSERT INTO ',GT_DB,'.',DUMP_TBL,'
					SELECT * FROM ',GT_DB,'.',SOURCE_TBL,' WHERE AZIMUTH IS NULL AND INDOOR_TYPE=0;
					');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt; 
			
			SET @SqlCmd=CONCAT('UPDATE ',GT_DB,'.',DUMP_TBL,'
								SET FLAG = 1000
								;');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt; 		
			
			SET @SqlCmd=CONCAT('DELETE FROM ',GT_DB,'.',SOURCE_TBL,' WHERE AZIMUTH IS NULL AND INDOOR_TYPE=0;
					');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt; 
		ELSEIF SPE_TYPE = 'mapping_with_cell' THEN
			
			SET @SqlCmd=CONCAT('INSERT INTO ',GT_DB,'.nt_log (ID,CELL_ID,TBL_NAME,COL_NAME,LOG_TYPE,DUMP_LOG)
					SELECT
						',ID,'
						,CELL_ID
						,''',SOURCE_TBL,'''
						,''',COL,'''
						,''1'' # 1 IS DUMP
						,''NO CELL CAN MAPPING''
					FROM ',GT_DB,'.tmp_nt_check','_',WORKER_ID,';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt; 
			
			SET @SqlCmd=CONCAT('UPDATE ',GT_DB,'.tmp_nt_check','_',WORKER_ID,'
					SET FLAG = 1000;');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;		
			
			SET @SqlCmd=CONCAT('INSERT INTO ',GT_DB,'.',DUMP_TBL,'
					SELECT * FROM ',GT_DB,'.tmp_nt_check','_',WORKER_ID,'
					');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;		
			IF TECH = 'GSM' THEN
				SET @SqlCmd=CONCAT('DELETE A FROM ',GT_DB,'.',SOURCE_TBL,' A, ',GT_DB,'.tmp_nt_check','_',WORKER_ID,' B
						WHERE A.',ID,'=B.',ID,' AND A.CELL_ID = B.CELL_ID AND A.LAC = B.LAC;
						');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt; 
			ELSE
				SET @SqlCmd=CONCAT('DELETE A FROM ',GT_DB,'.',SOURCE_TBL,' A, ',GT_DB,'.tmp_nt_check','_',WORKER_ID,' B
						WHERE A.',ID,'=B.',ID,' AND A.CELL_ID = B.CELL_ID;
						');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt; 
			END IF;
	
	
		ELSEIF SPE_TYPE = 'mapping_with_antenna' THEN			
			SET @SqlCmd=CONCAT('INSERT INTO ',GT_DB,'.nt_log (ID,CELL_ID,TBL_NAME,COL_NAME,LOG_TYPE,DUMP_LOG)
					SELECT
						',ID,'
						,CELL_ID
						,''',SOURCE_TBL,'''
						,''',COL,'''
						,''1'' # 1 IS DUMP
						,''NO ANTENNA CAN MAPPING''
					FROM ',GT_DB,'.tmp_nt_check','_',WORKER_ID,';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt; 		
			
			SET @SqlCmd=CONCAT('UPDATE ',GT_DB,'.tmp_nt_check','_',WORKER_ID,'
					SET FLAG = 1000;');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT('INSERT INTO ',GT_DB,'.',DUMP_TBL,'
					SELECT * FROM ',GT_DB,'.tmp_nt_check','_',WORKER_ID,'
					');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt; 		
			
			SET @SqlCmd=CONCAT('DELETE A FROM ',GT_DB,'.',SOURCE_TBL,' A, ',GT_DB,'.tmp_nt_check','_',WORKER_ID,' B
					WHERE A.',ID,'=B.',ID,' AND A.CELL_ID = B.CELL_ID;
					');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt; 	
		ELSEIF SPE_TYPE = 'neighbor_source_check_with_cell' THEN
			SET @SqlCmd=CONCAT('INSERT INTO ',GT_DB,'.',DUMP_TBL,'
					SELECT * FROM ',GT_DB,'.',SOURCE_TBL,'
					WHERE ',ID,' NOT IN (SELECT ',ID,' FROM ',GT_DB,'.',ID_TBL,')
					',IF(TECH='UMTS','AND NBR_TYPE < 3',''),'
					');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt; 
			
			SET @SqlCmd=CONCAT('INSERT INTO ',GT_DB,'.nt_log (ID,CELL_ID,TBL_NAME,COL_NAME,LOG_TYPE,DUMP_LOG)
					SELECT
						',ID,'
						,CELL_ID
						,''',SOURCE_TBL,'''
						,''',COL,'''
						,''1'' # 1 IS DUMP
						,''NBR SOURCE ',ID,' CAN NOT MAPPING''
					FROM ',GT_DB,'.',SOURCE_TBL,'
					WHERE ',ID,' NOT IN (SELECT ',ID,' FROM ',GT_DB,'.',ID_TBL,')
					',IF(TECH='UMTS','AND NBR_TYPE < 3',''),'
					');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt; 
	
			SET @SqlCmd=CONCAT('DELETE FROM ',GT_DB,'.',SOURCE_TBL,'
					WHERE ',ID,' NOT IN (SELECT ',ID,' FROM ',GT_DB,'.',ID_TBL,')
					',IF(TECH='UMTS','AND NBR_TYPE < 3',''),'
					');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt; 		
		ELSEIF SPE_TYPE = 'neighbor_target_check_with_cell' THEN
			SET @SqlCmd=CONCAT('INSERT INTO ',GT_DB,'.',DUMP_TBL,'
					SELECT * FROM ',GT_DB,'.',SOURCE_TBL,'
					WHERE ',NBR_ID,' NOT IN (SELECT ',ID,' FROM ',GT_DB,'.',ID_TBL,')
					',IF(TECH='UMTS','AND NBR_TYPE < 3',''),'
					');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt; 
			SET @SqlCmd=CONCAT('INSERT INTO ',GT_DB,'.nt_log (ID,CELL_ID,TBL_NAME,COL_NAME,LOG_TYPE,DUMP_LOG)
					SELECT
						',NBR_ID,'
						,CELL_ID
						,''',SOURCE_TBL,'''
						,''',COL,'''
						,''1'' # 1 IS DUMP
						,''NBR TARGER ',NBR_ID,' CAN NOT MAPPING''
					FROM ',GT_DB,'.',SOURCE_TBL,'
					WHERE ',NBR_ID,' NOT IN (SELECT ',ID,' FROM ',GT_DB,'.',ID_TBL,')
					',IF(TECH='UMTS','AND NBR_TYPE < 3',''),'
					');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt; 
			SET @SqlCmd=CONCAT('DELETE FROM ',GT_DB,'.',SOURCE_TBL,'
					WHERE ',NBR_ID,' NOT IN (SELECT ',ID,' FROM ',GT_DB,'.',ID_TBL,')
					',IF(TECH='UMTS','AND NBR_TYPE < 3',''),'
					');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt; 	
		END IF;
	END IF;
END$$
DELIMITER ;
