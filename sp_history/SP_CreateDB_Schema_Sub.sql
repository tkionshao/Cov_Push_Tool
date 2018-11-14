DELIMITER $$
USE `gt_gw_main`$$
DROP PROCEDURE IF EXISTS `SP_CreateDB_LTE`$$
CREATE DEFINER=`covmo`@`%` PROCEDURE `SP_CreateDB_Schema_Sub`(IN GT_DB VARCHAR(100), IN TECH VARCHAR(10))
BEGIN
	DECLARE IMSI_IMEI_DIFF_FLAG VARCHAR(10);
	DECLARE PARTITION_STR VARCHAR(1500) DEFAULT 
		' PARTITION BY LIST (DATA_HOUR)
		(
		 PARTITION h0 VALUES IN (0) ENGINE = MYISAM,
		 PARTITION h1 VALUES IN (1) ENGINE = MYISAM,
		 PARTITION h2 VALUES IN (2) ENGINE = MYISAM,
		 PARTITION h3 VALUES IN (3) ENGINE = MYISAM,
		 PARTITION h4 VALUES IN (4) ENGINE = MYISAM,
		 PARTITION h5 VALUES IN (5) ENGINE = MYISAM,
		 PARTITION h6 VALUES IN (6) ENGINE = MYISAM,
		 PARTITION h7 VALUES IN (7) ENGINE = MYISAM,
		 PARTITION h8 VALUES IN (8) ENGINE = MYISAM,
		 PARTITION h9 VALUES IN (9) ENGINE = MYISAM,
		 PARTITION h10 VALUES IN (10) ENGINE = MYISAM,
		 PARTITION h11 VALUES IN (11) ENGINE = MYISAM,
		 PARTITION h12 VALUES IN (12) ENGINE = MYISAM,
		 PARTITION h13 VALUES IN (13) ENGINE = MYISAM,
		 PARTITION h14 VALUES IN (14) ENGINE = MYISAM,
		 PARTITION h15 VALUES IN (15) ENGINE = MYISAM,
		 PARTITION h16 VALUES IN (16) ENGINE = MYISAM,
		 PARTITION h17 VALUES IN (17) ENGINE = MYISAM,
		 PARTITION h18 VALUES IN (18) ENGINE = MYISAM,
		 PARTITION h19 VALUES IN (19) ENGINE = MYISAM,
		 PARTITION h20 VALUES IN (20) ENGINE = MYISAM,
		 PARTITION h21 VALUES IN (21) ENGINE = MYISAM,
		 PARTITION h22 VALUES IN (22) ENGINE = MYISAM,
		 PARTITION h23 VALUES IN (23) ENGINE = MYISAM	
		 );';
		 
	SELECT LOWER(`value`) INTO IMSI_IMEI_DIFF_FLAG  FROM gt_gw_main.integration_param WHERE gt_group = 'sp' AND gt_name = 'imsi_imei_diff' ;		 
	SET @SqlCmd=CONCAT('CREATE TABLE ',GT_DB,'.`table_call_imsi_',TECH,'`
				 (
				  `CALL_ID` BIGINT(20) DEFAULT NULL,
				  `START_TIME` DATETIME DEFAULT NULL,
				  `END_TIME` DATETIME DEFAULT NULL,
				  `DURATION` INT(11) DEFAULT NULL,
				  `CALL_SETUP_TIME` MEDIUMINT(9) DEFAULT NULL,
				  `IMSI` CHAR(16) DEFAULT NULL,
				  `IMEI` CHAR(16) DEFAULT NULL,
				  `MSISDN` VARCHAR(20) DEFAULT NULL,
				  `MAKE_ID` SMALLINT(6) DEFAULT NULL,
				  `MODEL_ID` MEDIUMINT(9) DEFAULT NULL,
				  `TECH_MASK` TINYINT(4) DEFAULT NULL,
				  `CALL_TYPE` TINYINT(4) DEFAULT NULL,
				  `APN` VARCHAR(100) DEFAULT NULL,
				  `CALL_STATUS` TINYINT(4) DEFAULT NULL,
				  `RELEASE_CAUSE` MEDIUMINT(9) DEFAULT NULL,
				  `START_CELL` CHAR(19) DEFAULT NULL,
				  `POS_FIRST_LOC` BIGINT(20) DEFAULT NULL,
				  `START_RXLEV_RSCP_RSRP_dBn` FLOAT DEFAULT NULL,
				  `START_RXQUAL_ECN0_RSRQ_dB` FLOAT DEFAULT NULL,
				  `END_CELL` CHAR(19) DEFAULT NULL,
				  `POS_LAST_LOC` BIGINT(20) DEFAULT NULL,
				  `END_RXLEV_RSCP_RSRP_dBm` FLOAT DEFAULT NULL,
				  `END_RXQUAL_ECN0_RSRQ_dB` FLOAT DEFAULT NULL,
				  `DL_TRAFFIC_VOLUME_MB` FLOAT DEFAULT NULL,
				  `DL_THROUGHPUT_Kbps` FLOAT DEFAULT NULL,
				  `DL_THROUGHPUT_MAX_Kbps` FLOAT DEFAULT NULL,
				  `UL_TRAFFIC_VOLUME_MB` FLOAT DEFAULT NULL,
				  `UL_THROUGHPUT_KBPS` FLOAT DEFAULT NULL,
				  `UL_THROUGHPUT_MAX_KBPS` FLOAT DEFAULT NULL,
				  `INTRA_FREQ_HO_ATTEMPT` MEDIUMINT(9) DEFAULT NULL,
				  `INTRA_FREQ_HO_FAILURE` MEDIUMINT(9) DEFAULT NULL,
				  `INTER_FREQ_HO_ATTEMPT` MEDIUMINT(9) DEFAULT NULL,
				  `INTER_FREQ_HO_FAILURE` SMALLINT(6) DEFAULT NULL,
				  `IRAT_HO_ATTEMPT` MEDIUMINT(9) DEFAULT NULL,
				  `IRAT_HO_FAILURE` INT(11) DEFAULT NULL,
				  `INDOOR` TINYINT(4) DEFAULT NULL,
				  `MOVING` TINYINT(4) DEFAULT NULL,
				  `MOVING_TYPE` TINYINT(4) DEFAULT NULL,
				  `B_PARTY_NUMBER` VARCHAR(15) DEFAULT NULL,
				  `DATA_DATE` DATE DEFAULT NULL,
				  `DATA_HOUR` TINYINT(4) DEFAULT NULL,
				  `BATCH` SMALLINT(6) DEFAULT NULL,
				  KEY `ID_KEY` (`IMSI`),
				  KEY `BATCH` (`BATCH`),
				  KEY `MSISDN` (`MSISDN`)
				) ENGINE=MYISAM DEFAULT CHARSET=utf8 DELAY_KEY_WRITE=1');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt; 
	
	IF IMSI_IMEI_DIFF_FLAG = 'true' THEN	
		SET @SqlCmd=CONCAT('CREATE TABLE ',GT_DB,'.`table_imsi_imei` (
					  `IMSI` CHAR(16) NOT NULL,
					  `IMEI` CHAR(16) NOT NULL,
					  PRIMARY KEY (`IMSI`,`IMEI`)
					) ENGINE=MYISAM DEFAULT CHARSET=utf8 DELAY_KEY_WRITE=1');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt; 
	ELSE
		SET @SqlCmd=CONCAT('CREATE TABLE ',GT_DB,'.`table_imsi`
					(
					  `IMSI` CHAR(16) NOT NULL,
					  PRIMARY KEY (`IMSI`)
					) ENGINE=MYISAM DEFAULT CHARSET=utf8 DELAY_KEY_WRITE=1');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt; 
	END IF;	
END$$
DELIMITER ;
