DELIMITER $$
USE `gt_gw_main`$$
DROP PROCEDURE IF EXISTS `SP_CreateDB_LTE`$$
CREATE DEFINER=`covmo`@`%` PROCEDURE `SP_DTAG_SOURCE_NT_SUB_MAPPING_NT_ANTENNA`(IN GT_DB VARCHAR(100))
a_label:
BEGIN
-- Give NT_ANTENNA FREQUENCY from NT_CELL by RNC_ID and SITE_ID.
INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'SP_DTAG_SOURCE_NT_SUB_MAPPING_NT_ANTENNA','Update NT_ANTENNA.FREQUENCY by NT_CELL.', NOW());
		SET @SqlCmd=CONCAT('ALTER TABLE ',GT_DB,'.NT_CELL ADD KEY `ID_KEY` (`RNC_ID`,`CELL_ID`);');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt; 

		SET @SqlCmd=CONCAT('DROP TABLE IF EXISTS ',GT_DB,'.3g_antenna_freq_and_mod_index;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt; 

		SET @SqlCmd=CONCAT('CREATE TABLE ',GT_DB,'.3g_antenna_freq_and_mod_index AS SELECT a.ANTENNA_MODEL,b.FREQUENCY FROM ',GT_DB,'.NT_ANTENNA a LEFT JOIN ',GT_DB,'.NT_CELL b ON a.RNC_ID = b.RNC_ID AND a.CELL_ID = b.CELL_ID WHERE a.ANTENNA_MODEL IS NOT NULL;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt; 
		
		SET @SqlCmd=CONCAT('ALTER TABLE ',GT_DB,'.NT_CELL DROP KEY `ID_KEY`;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt; 

INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'SP_DTAG_SOURCE_NT_SUB_MAPPING_NT_ANTENNA','Create table including CATEGORIZE, ANTENNA_MODEL and FREQUENCY.', NOW());
		SET @SqlCmd=CONCAT('ALTER TABLE ',GT_DB,'.3g_antenna_freq_and_mod_index ADD KEY `TMP_KEY` (`ANTENNA_MODEL`,`FREQUENCY`);');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt; 
		
		SET @SqlCmd=CONCAT('ALTER TABLE ',GT_DB,'.ANTENNA_INFO ADD KEY `TMP_KEY` (`CATEGORIZE`,`FREQUENCY`);');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt; 

		SET @SqlCmd=CONCAT('DROP TABLE IF EXISTS ',GT_DB,'.3g_antenna_cat_and_mod_index;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt; 

		SET @SqlCmd=CONCAT('CREATE TABLE ',GT_DB,'.3g_antenna_cat_and_mod_index AS SELECT a.FREQUENCY, a.ANTENNA_MODEL AS WRONG_MODEL,b.ANTENNA_MODEL,b.CATEGORIZE, b.HBW3 FROM ',GT_DB,'.3g_antenna_freq_and_mod_index a LEFT JOIN ',GT_DB,'.ANTENNA_INFO b ON a.FREQUENCY = b.FREQUENCY AND a.ANTENNA_MODEL = b.CATEGORIZE GROUP BY b.ANTENNA_MODEL;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt; 

		SET @SqlCmd=CONCAT('ALTER TABLE ',GT_DB,'.3g_antenna_cat_and_mod_index ADD KEY `TMP_KEY` (`CATEGORIZE`);');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt; 
		
-- Update antenna_model from categorize to real antenna model.
INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'SP_DTAG_SOURCE_NT_SUB_MAPPING_NT_ANTENNA','Correct antenna_model, CATEGORIZE changes to ANTENNA_MODEL.', NOW());
		SET @SqlCmd=CONCAT('UPDATE ',GT_DB,'.NT_ANTENNA a, ',GT_DB,'.3g_antenna_cat_and_mod_index b 
								SET a.ANTENNA_MODEL = b.ANTENNA_MODEL WHERE a.ANTENNA_MODEL = b.CATEGORIZE;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt; 

-- Start to update the BEAMWIDTH_H,ANTENNA_TYPE AND ANTENNA_GAIN .
INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'SP_DTAG_SOURCE_NT_SUB_MAPPING_NT_ANTENNA','Update BEAMWIDTH_H.', NOW());
		SET @SqlCmd=CONCAT('UPDATE ',GT_DB,'.NT_ANTENNA a, ',GT_DB,'.ANTENNA_INFO b 
								SET a.BEAMWIDTH_H = b.HBW3 WHERE a.ANTENNA_MODEL = b.ANTENNA_MODEL;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt; 
INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'SP_DTAG_SOURCE_NT_SUB_MAPPING_NT_ANTENNA','Update ANTENNA_TYPE.', NOW());
		SET @SqlCmd=CONCAT('UPDATE ',GT_DB,'.NT_ANTENNA a, ',GT_DB,'.ANTENNA_INFO b 
								SET a.ANTENNA_TYPE = b.TYPE WHERE a.ANTENNA_MODEL = b.ANTENNA_MODEL;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt; 
INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'SP_DTAG_SOURCE_NT_SUB_MAPPING_NT_ANTENNA','Update ANTENNA_GAIN.', NOW());
		SET @SqlCmd=CONCAT('UPDATE ',GT_DB,'.NT_ANTENNA a, ',GT_DB,'.ANTENNA_INFO b 
								SET a.ANTENNA_GAIN = b.`GAIN(dBi)` WHERE a.ANTENNA_MODEL = b.ANTENNA_MODEL;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt; 
END$$
DELIMITER ;
