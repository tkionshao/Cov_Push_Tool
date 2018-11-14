DELIMITER $$
USE `gt_gw_main`$$
DROP PROCEDURE IF EXISTS `SP_CreateDB_LTE`$$
CREATE DEFINER=`covmo`@`%` PROCEDURE `SP_CreateDB_Schema_PM`(GT_DB VARCHAR(100),TECH_MASK TINYINT(4))
BEGIN
	DECLARE START_TIME DATETIME DEFAULT SYSDATE();	
	DECLARE VENDOR_ID INT(11); 
	DECLARE NT_DB VARCHAR(100) DEFAULT CONCAT('gt_nt_',gt_strtok(GT_DB,3,'_'));
	SELECT (VALUE*1) INTO VENDOR_ID FROM gt_gw_main.integration_param WHERE gt_group = 'sp' AND gt_name = 'VENDOR_ID' ;
	INSERT INTO gt_gw_main.sp_log VALUES('gt_gw_main','SP_CreateDB_Schema_PM','SP_CreateDB_Schema_PM Start ', NOW());
	SET SESSION group_concat_max_len=@@max_allowed_packet;
	IF TECH_MASK IN (0,2) THEN 
		IF (VENDOR_ID & 1)>0 THEN
			SET @SqlCmd=CONCAT('SELECT COUNT(*) INTO @V_EXIST_DIM_ERI_UMTS FROM information_schema.`TABLES`
						WHERE TABLE_SCHEMA=''',NT_DB,''' 
						AND TABLE_NAME=''dim_pm_ericsson_umts'';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
			
			IF @V_EXIST_DIM_ERI_UMTS >0 THEN 
				SET @SqlCmd=CONCAT('SELECT GROUP_CONCAT( ''`'',`PM_COUNTER_NAME`,''`'','' '',DATA_TYPE, '' DEFAULT NULL''  ORDER BY `INDEX` SEPARATOR '','') 
							INTO @all_column_eri_umts  FROM ',NT_DB,'.dim_pm_ericsson_umts ;');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				
				SET @SqlCmd=CONCAT('DROP TABLE IF EXISTS ',GT_DB,'.`table_pm_ericsson_umts`;');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				
				SET @SqlCmd=CONCAT('CREATE TABLE ',GT_DB,'.`table_pm_ericsson_umts` (					
					',@all_column_eri_umts,')',' 		
					ENGINE=MyIsam DEFAULT CHARSET=latin1;');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				
				SET @SqlCmd = CONCAT('CREATE TABLE ',GT_DB,'.table_pm_ericsson_umts_aggr like ',GT_DB,'.table_pm_ericsson_umts;');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				
				SET @SqlCmd=CONCAT('ALTER TABLE ',GT_DB,'.table_pm_ericsson_umts ADD INDEX `id_key`(DATA_DATE,DATA_HOUR,RNC_NAME,CELL_ID);');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				
				SET @SqlCmd=CONCAT('ALTER TABLE ',GT_DB,'.table_pm_ericsson_umts_aggr ADD INDEX `id_key`(DATA_DATE,DATA_HOUR,RNC_ID,CELL_ID);');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
			ELSE 
				SELECT 'No dim table for Ericsson pm counter UMTS!' AS Message;
			END IF;
		END IF;
		
		IF (VENDOR_ID & 2)>0 THEN
			SET @SqlCmd=CONCAT('SELECT COUNT(*) INTO @V_EXIST_DIM_NSN_UMTS FROM information_schema.`TABLES`
						WHERE TABLE_SCHEMA=''',NT_DB,''' 
						AND TABLE_NAME=''dim_pm_nokia_umts'';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
			
			IF @V_EXIST_DIM_NSN_UMTS >0 THEN 
				SET @SqlCmd=CONCAT('SELECT GROUP_CONCAT( ''`'',`PM_COUNTER_NAME`,''`'','' '',DATA_TYPE, '' DEFAULT NULL''  ORDER BY `INDEX` SEPARATOR '','') 
							INTO @all_column_nsn_umts  FROM ',NT_DB,'.dim_pm_nokia_umts ;');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				
				SET @SqlCmd=CONCAT('DROP TABLE IF EXISTS ',GT_DB,'.`table_pm_nokia_umts`;');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				
				SET @SqlCmd=CONCAT('CREATE TABLE ',GT_DB,'.`table_pm_nokia_umts` (					
					',@all_column_nsn_umts,')',' 		
					ENGINE=MyIsam DEFAULT CHARSET=latin1;');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				
				SET @SqlCmd = CONCAT('CREATE TABLE ',GT_DB,'.table_pm_nokia_umts_aggr like ',GT_DB,'.table_pm_nokia_umts;');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
			ELSE 
				SELECT 'No dim table for NSN pm counter UMTS!' AS Message;
			END IF;
		END IF;
		
		IF (VENDOR_ID & 4)>0 THEN
			SET @SqlCmd=CONCAT('SELECT COUNT(*) INTO @V_EXIST_DIM_HUA_UMTS FROM information_schema.`TABLES`
						WHERE TABLE_SCHEMA=''',NT_DB,''' 
						AND TABLE_NAME=''dim_pm_huawei_umts'';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
			
			IF @V_EXIST_DIM_HUA_UMTS >0 THEN 
				SET @SqlCmd=CONCAT('SELECT GROUP_CONCAT( ''`'',`PM_COUNTER_NAME`,''`'','' '',DATA_TYPE, '' DEFAULT NULL''  ORDER BY `INDEX` SEPARATOR '','') 
							INTO @all_column_hua_umts  FROM ',NT_DB,'.dim_pm_huawei_umts ;');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				
				SET @SqlCmd=CONCAT('DROP TABLE IF EXISTS ',GT_DB,'.`table_pm_huawei_umts`;');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				
				SET @SqlCmd=CONCAT('CREATE TABLE ',GT_DB,'.`table_pm_huawei_umts` (					
					',@all_column_hua_umts,')',' 		
					ENGINE=MyIsam DEFAULT CHARSET=latin1;');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				SET @SqlCmd = CONCAT('CREATE TABLE ',GT_DB,'.table_pm_huawei_umts_aggr like ',GT_DB,'.table_pm_huawei_umts;');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
			ELSE 
				SELECT 'No dim table for Huawei pm counter UMTS!' AS Message;
			END IF;
		END IF;	
	END IF;	
	
	IF TECH_MASK IN (0,4) THEN 
		IF (VENDOR_ID & 1)>0 THEN		
			SET @SqlCmd=CONCAT('SELECT COUNT(*) INTO @V_EXIST_DIM_ERI_LTE FROM information_schema.`TABLES`
						WHERE TABLE_SCHEMA=''',NT_DB,''' 
						AND TABLE_NAME=''dim_pm_ericsson_lte'';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
			
			IF @V_EXIST_DIM_ERI_LTE >0 THEN 
				SET @SqlCmd=CONCAT('SELECT GROUP_CONCAT( ''`'',`PM_COUNTER_NAME`,''`'','' '',DATA_TYPE, '' DEFAULT NULL''  ORDER BY `INDEX` SEPARATOR '','') 
							INTO @all_column_eri_lte  FROM ',NT_DB,'.dim_pm_ericsson_lte ;');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				
				SET @SqlCmd=CONCAT('DROP TABLE IF EXISTS ',GT_DB,'.`table_pm_ericsson_lte`;');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				
				SET @SqlCmd=CONCAT('CREATE TABLE ',GT_DB,'.`table_pm_ericsson_lte` (					
					',@all_column_eri_lte,')',' 		
					ENGINE=MyIsam DEFAULT CHARSET=latin1;');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				SET @SqlCmd=CONCAT('ALTER TABLE ',GT_DB,'.table_pm_ericsson_lte ADD INDEX `id_key`(DATA_DATE,DATA_HOUR,ENODEB_ID,CELL_ID);');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				SET @SqlCmd = CONCAT('CREATE TABLE ',GT_DB,'.table_pm_ericsson_lte_aggr like ',GT_DB,'.table_pm_ericsson_lte;');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
			ELSE 
				SELECT 'No dim table for Ericsson pm counter LTE!' AS Message;
			END IF;
		END IF;
		
		IF (VENDOR_ID & 4)>0 THEN		
			SET @SqlCmd=CONCAT('SELECT COUNT(*) INTO @V_EXIST_DIM_HUA_LTE FROM information_schema.`TABLES`
			WHERE TABLE_SCHEMA=''',NT_DB,''' 
			AND TABLE_NAME=''dim_pm_huawei_lte'';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
			IF @V_EXIST_DIM_HUA_LTE >0 THEN 
				SET @SqlCmd=CONCAT('SELECT GROUP_CONCAT( ''`'',`PM_COUNTER_NAME`,''`'','' '',DATA_TYPE, '' DEFAULT NULL''  ORDER BY `INDEX` SEPARATOR '','') 
							INTO @all_column_hua_lte  FROM ',NT_DB,'.dim_pm_huawei_lte ;');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				
				SET @SqlCmd=CONCAT('DROP TABLE IF EXISTS ',GT_DB,'.`table_pm_huawei_lte`;');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				
				SET @SqlCmd=CONCAT('CREATE TABLE ',GT_DB,'.`table_pm_huawei_lte` (					
				',@all_column_hua_lte,')',' 		
				ENGINE=MyIsam DEFAULT CHARSET=latin1;');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				SET @SqlCmd = CONCAT('CREATE TABLE ',GT_DB,'.table_pm_huawei_lte_aggr like ',GT_DB,'.table_pm_huawei_lte;');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
			ELSE 
				SELECT 'No dim table for Huawei pm counter LTE!' AS Message;
			END IF;
		END IF;
	END IF;
	INSERT INTO gt_gw_main.SP_LOG VALUES('gt_gw_main','SP_CreateDB_Schema_PM',CONCAT('Done: ',TIMESTAMPDIFF(SECOND,START_TIME,SYSDATE()),' seconds.'), NOW());
END$$
DELIMITER ;
