CREATE DEFINER=`covmo`@`%` PROCEDURE `SP_CreateDB_NT_Schema_Sub`(IN GT_DB VARCHAR(100))
BEGIN
	
	SET @SqlCmd=CONCAT('CREATE TABLE IF NOT EXISTS ',GT_DB,'.`antenna_info` (
				  `ANTENNA_MODEL` VARCHAR(100) NOT NULL DEFAULT '''',
				  `TYPE` VARCHAR(60) DEFAULT NULL,
				  `GAIN(dBi)` DOUBLE DEFAULT NULL,
				  `HBW3` SMALLINT(6) DEFAULT NULL,
				  `HBW3_CL` SMALLINT(6) DEFAULT NULL,
				  `HBW3_CW` SMALLINT(6) DEFAULT NULL,
				  `HBW6` SMALLINT(6) DEFAULT NULL,
				  `HBW6_CL` SMALLINT(6) DEFAULT NULL,
				  `HBW6_CW` SMALLINT(6) DEFAULT NULL,
				  `VBW3` DOUBLE DEFAULT NULL,
				  `VBW3_CL` SMALLINT(6) DEFAULT NULL,
				  `VBW3_CW` SMALLINT(6) DEFAULT NULL,
				  `VBW6` DOUBLE DEFAULT NULL,
				  `VBW6_CL` SMALLINT(6) DEFAULT NULL,
				  `VBW6_CW` SMALLINT(6) DEFAULT NULL,
				  `HORIZONTAL_360` TEXT,
				  `VERTICAL_360` TEXT,
				  `CATEGORIZE` VARCHAR(50) DEFAULT NULL,
				  `MANUFACTURER` VARCHAR(50) DEFAULT NULL,
				  `TILT_TYPE` VARCHAR(100) DEFAULT NULL,
				  `ELECTRICAL_TILT` TINYINT(4) DEFAULT NULL,
				  `MECHANICAL_TILT` TINYINT(4) DEFAULT NULL,
				  `FREQUENCY` MEDIUMINT(9) DEFAULT NULL,
				  `COMMENTS` VARCHAR(100) DEFAULT NULL,
				  `ANTENNA_GROUP` VARCHAR(50) DEFAULT NULL,
				  PRIMARY KEY (`ANTENNA_MODEL`)
				) ENGINE=MYISAM DEFAULT CHARSET=utf8 DELAY_KEY_WRITE=1;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;  
	
		
	SET @SqlCmd=CONCAT('CREATE TABLE IF NOT EXISTS ',GT_DB,'.`nt_log` (
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
