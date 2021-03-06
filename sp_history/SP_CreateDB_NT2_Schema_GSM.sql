DELIMITER $$
USE `gt_gw_main`$$
DROP PROCEDURE IF EXISTS `SP_CreateDB_LTE`$$
CREATE DEFINER=`covmo`@`%` PROCEDURE `SP_CreateDB_NT2_Schema_GSM`(IN GT_DB VARCHAR(100))
BEGIN
	SET @SqlCmd=CONCAT('CREATE TABLE ',GT_DB,'.`NT2_ANTENNA_GSM` (
				`BSC_ID` MEDIUMINT(9),
				`CELL_ID` MEDIUMINT(9),
				`LAC` MEDIUMINT(9),
				`ANTENNA_ID` TINYINT(4),
				`CELL_NAME` VARCHAR(50) CHARACTER SET utf8,
				`LONGITUDE` FLOAT(9,6),
				`LATITUDE` FLOAT(9,6),
				`AZIMUTH` SMALLINT(6),
				`ANTENNA_HEIGHT` FLOAT(7,2),
				`BCCH_POWER` FLOAT(7,2),
				`FEEDER_LOSS` FLOAT(7,2),
				`EIRP` FLOAT(7,2),
				`ANTENNA_MODEL` VARCHAR(50) CHARACTER SET utf8,
				`ANTENNA_TYPE` TINYINT,
				`ANTENNA_GAIN` FLOAT(7,2),
				`BEAMWIDTH_H` SMALLINT(6),
				`BEAMWIDTH_V` SMALLINT(6),
				`DOWNTILT_EL` SMALLINT(6),
				`DOWNTILT_MEC` SMALLINT(6),
				`ELEVATION` FLOAT(7,2),
				`FIELD_TYPE` TINYINT,
				`PATHLOSS_DISTANCE` FLOAT(7,2),
				`ANTENNA_RADIUS` FLOAT(7,2),
				`CLOSED_RADIUS` FLOAT(7,2),
				`REPEATER_IND` TINYINT(4) DEFAULT 0,
				`REPEATER_TYPE` VARCHAR(20),
				`REPEATER_DELAY` SMALLINT(6),
				`FLAG` INT(11),
				`PATHLOSS_360` TEXT,
				`ANTENNA_RADIUS_360`  TEXT,
				KEY `PRIMARY KEY` (`BSC_ID`,`CELL_ID`,`LAC`,`ANTENNA_ID`)
				)ENGINE=MYISAM DEFAULT CHARSET=latin1 DELAY_KEY_WRITE=1;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('CREATE TABLE ',GT_DB,'.`NT2_ANTENNA_GSM_DUMP` LIKE ',GT_DB,'.NT2_ANTENNA_GSM;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
    SET @SqlCmd=CONCAT('CREATE TABLE ',GT_DB,'.`NT2_BSC` (
				`BSC_ID` MEDIUMINT(9),
				`BSC_NAME` VARCHAR(50) CHARACTER SET utf8,
				`MCC` VARCHAR(3),
				`MNC` VARCHAR(3),
				`VENDOR` VARCHAR(50) CHARACTER SET utf8,
				`BSC_MODEL` VARCHAR(30),
				`SW_VERSION` VARCHAR(30),
				`BTS_CNT` SMALLINT(6),
				`CELL_CNT` SMALLINT(6),
				`NBR_DISTANCE_VORONOI` FLOAT(7,2),
				`NBR_DISTANCE_CM` FLOAT(7,2)
				)ENGINE=MYISAM DEFAULT CHARSET=latin1 DELAY_KEY_WRITE=1;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('CREATE TABLE ',GT_DB,'.`NT2_CELL_GSM` (
				`BSC_ID` MEDIUMINT(9),
				`SITE_ID` VARCHAR(50) CHARACTER SET utf8,
				`LAC` MEDIUMINT(9),
				`RAC` VARCHAR(10) CHARACTER SET utf8,
				`CELL_ID` MEDIUMINT(9),
				`CELL_NAME` VARCHAR(50) CHARACTER SET utf8,
				`SECTOR_ID` TINYINT(4),
				`FREQUENCY` SMALLINT(6) UNSIGNED,
				`BCCH_ARFCN` SMALLINT(6) UNSIGNED,
				`TCH_ARFCN` VARCHAR(120) CHARACTER SET utf8,
				`BSIC` TINYINT(4) UNSIGNED,
				`CGI` VARCHAR(50) CHARACTER SET utf8,
				`LONGITUDE` FLOAT(9,6),
				`LATITUDE` FLOAT(9,6),
				`INDOOR` TINYINT(4),
				`BTS_TYPE` TINYINT(4),
				`BTS_CAPACITY` varchar(20) CHARACTER SET utf8,
				`CELL_RADIUS` FLOAT(9,2),
				`CLUSTER` VARCHAR(100) CHARACTER SET utf8,
				`REGION` VARCHAR(100) CHARACTER SET utf8,
				`SUB_REGION` VARCHAR(100) CHARACTER SET utf8,
				`ZONE` VARCHAR(100) CHARACTER SET utf8,
				`MORPHOLOGY` VARCHAR(100) CHARACTER SET utf8,
				`VENDOR_SITE_INDEX` VARCHAR(50) CHARACTER SET utf8,
				`VENDOR_CELL_INDEX` VARCHAR(50) CHARACTER SET utf8,
				`CLUSTER_ID` SMALLINT(6),
				`GSM_NBCNT` SMALLINT(6) UNSIGNED,
				`NBR_DISTANCE_CM` FLOAT(9,2),
				`NBR_DISTANCE_CM_MAX` FLOAT(9,2),
				`NBR_DISTANCE_VORONOI` FLOAT(9,2),
				`NBR_DISTANCE_VORONOI_MAX` FLOAT(9,2),
				`SITE_DENSITY_TYPE` TINYINT(4),
				`FLAG` INT(11),
				`CM_ADMIN_STATE` TINYINT(4) DEFAULT 0,
				`CM_OPERATION_STATE` TINYINT(4) DEFAULT 1,
				`ACITVE_STATE` VARCHAR(20) DEFAULT ''1'',
				`SERVING_TYPE` TINYINT(4) DEFAULT 0,
				`BA_LIST` VARCHAR(280) DEFAULT ''\N'',
				`SITE_NAME` VARCHAR(50),
				KEY `NT_CELL_GSM_ID` (BSC_ID, LAC, CELL_ID)
				)ENGINE=MYISAM DEFAULT CHARSET=latin1 DELAY_KEY_WRITE=1;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('CREATE TABLE ',GT_DB,'.`NT2_CELL_GSM_DUMP` LIKE ',GT_DB,'.NT2_CELL_GSM;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('CREATE TABLE ',GT_DB,'.`NT2_NBR_2_2_GSM` (
				`BSC_ID` MEDIUMINT(9),
				`CELL_ID` MEDIUMINT(9),
				`LAC` MEDIUMINT(9),
				`BCCH_ARFCN` SMALLINT(6) UNSIGNED,
				`BSIC` TINYINT(4) UNSIGNED,
				`NBR_BSC_ID` MEDIUMINT(9),
				`NBR_CELL_ID` MEDIUMINT(9),
				`NBR_CELL_LAC` MEDIUMINT(9),
				`NBR_BCCH_ARFCN` SMALLINT(6) UNSIGNED,
				`NBR_BSIC` SMALLINT(6),
				`NBR_CGI` varchar(50) CHARACTER SET utf8,
				`PRIORITY` tinyint,
				`NBR_DISTANCE` FLOAT(9,2),
				`NBR_ANGLE` SMALLINT(6),
				`NBR_AZIMUTH_ANGLE` SMALLINT(6),
				KEY `PRIMARY KEY` (`BSC_ID`,`CELL_ID`,`LAC`,`NBR_BSC_ID`,`NBR_CELL_ID`,`NBR_CELL_LAC`)
				)ENGINE=MYISAM DEFAULT CHARSET=latin1 DELAY_KEY_WRITE=1;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('CREATE TABLE ',GT_DB,'.`NT2_NBR_2_2_GSM_DUMP` LIKE ',GT_DB,'.NT2_NBR_2_2_GSM;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;	
	
	SET @SqlCmd=CONCAT('CREATE TABLE ',GT_DB,'.`NT2_NBR_VORONOI_GSM` (
				`BSC_ID` MEDIUMINT(9),
				`LAC` MEDIUMINT(9),
				`SITE_ID` VARCHAR(50) CHARACTER SET utf8,
				`NBR_BSC_ID` MEDIUMINT(9),
				`NBR_LAC` MEDIUMINT(9),
				`NBR_SITE_ID` VARCHAR(50) CHARACTER SET utf8,
				`ANGLE` SMALLINT(6),
				`DISTANCE` FLOAT(9,2),
				`INDOOR_TYPE` TINYINT(4),
				`SITEDENSITY_RANGE` MEDIUMINT(9),
				`REFINE_DISTANCE` FLOAT(7,2),
				KEY `PRIMARY KEY` (`BSC_ID`,`SITE_ID`,`NBR_BSC_ID`,`NBR_SITE_ID`,`ANGLE`)
				)ENGINE=MYISAM DEFAULT CHARSET=latin1 DELAY_KEY_WRITE=1;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('CREATE TABLE IF NOT EXISTS ',GT_DB,'.`NT2_LOG_GSM` (
				`ID` MEDIUMINT(9) DEFAULT NULL,
				`CELL_ID` MEDIUMINT(9) DEFAULT NULL,
				`TBL_NAME` VARCHAR(50) DEFAULT NULL,
				`COL_NAME` VARCHAR(50) DEFAULT NULL,
				`LOG_TYPE` TINYINT(4) DEFAULT NULL,
				`DUMP_LOG` VARCHAR(100) DEFAULT NULL
				) ENGINE=MYISAM DEFAULT CHARSET=utf8 DELAY_KEY_WRITE=1;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt; 
	
	SET @SqlCmd=CONCAT('CREATE TABLE IF NOT EXISTS ',GT_DB,'.`NT2_ANTENNA_INFO` (
				`ANTENNA_MODEL` Varchar(100) CHARACTER SET utf8,
				`ANTENNA_TYPE` TINYINT(4),
				`GAIN_DBI` FLOAT,
				`DEVIATION` SMALLINT(6) Default 0,
				`HBW3` SMALLINT(6),
				`HBW3_CL` SMALLINT(6),
				`HBW3_CW` SMALLINT(6),
				`VBW3` SMALLINT(6),
				`VBW3_CL` SMALLINT(6),
				`VBW3_CW` SMALLINT(6),
				`HORIZONTAL_360` TEXT,
				`VERTICAL_360` TEXT,
				`ANTENNA_GROUP` varchar(50) CHARACTER SET utf8,
				`ELECTRICAL_TILT` FLOAT,
				`MECHANICAL_TILT` FLOAT,
				`CATEGORIZE` VarChar(50) CHARACTER SET utf8,
				`MANUFACTURER` VarChar(50) CHARACTER SET utf8,
				`TILT_TYPE` VarChar(20) CHARACTER SET utf8,
				`FREQUENCY` VARCHAR(20) CHARACTER SET utf8,
				`COMMENTS` Varchar(100) CHARACTER SET utf8
				)ENGINE=MYISAM DEFAULT CHARSET=latin1 DELAY_KEY_WRITE=1;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	CALL gt_gw_main.SP_Sub_Generate_Sys_Config(GT_DB,'gt_covmo','gsm');
	CALL gt_gw_main.SP_Sub_Generate_Dim_Imsi_Group(GT_DB,'gt_covmo','gsm');
	
END$$
DELIMITER ;
