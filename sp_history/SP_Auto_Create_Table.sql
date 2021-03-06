DELIMITER $$
USE `gt_gw_main`$$
DROP PROCEDURE IF EXISTS `SP_CreateDB_LTE`$$
CREATE DEFINER=`covmo`@`%` PROCEDURE `SP_Auto_Create_Table`()
BEGIN
	DECLARE l_RNC INT;
	DECLARE l_GW_URI VARCHAR(512);
	DECLARE l_GW_USER VARCHAR(32);
	DECLARE l_GW_PASSWORD VARCHAR(32);
	DECLARE l_AP_URI VARCHAR(512);
	DECLARE l_AP_USER VARCHAR(32);
	DECLARE l_AP_PASSWORD VARCHAR(32);
	DECLARE l_VENDOR_ID INT;
	DECLARE S_IP VARCHAR(20);
	DECLARE S_PORT VARCHAR(10);
	DECLARE S_ACCOUNT VARCHAR(30);
	DECLARE S_PWD VARCHAR(30);
	DECLARE UNION_STR VARCHAR(1000);
	DECLARE S_TBL_NAME VARCHAR(100);
	DECLARE T_TBL_NAME VARCHAR(100);
	DECLARE DB VARCHAR(50) DEFAULT 'gt_gw_main';
	DECLARE no_more_maps INT;
	
	DECLARE csr CURSOR FOR
	SELECT RNC,GW_URI,GW_USER,GW_PASSWORD,AP_URI,AP_USER,AP_PASSWORD,VENDOR_ID FROM gt_gw_main.rnc_information
	WHERE RNC > 0;
	
	DECLARE CONTINUE HANDLER FOR NOT FOUND 
	BEGIN
		SET no_more_maps = 1;
		SELECT 1 INTO @handler_invoked FROM (SELECT 1) AS t;
	END;
	
	SET no_more_maps = 0;
	OPEN csr;
	dept_loop:REPEAT
                FETCH csr INTO l_RNC,l_GW_URI,l_GW_USER,l_GW_PASSWORD,l_AP_URI,l_AP_USER,l_AP_PASSWORD,l_VENDOR_ID;
                IF no_more_maps = 0 THEN
                        IF no_more_maps THEN
				LEAVE dept_loop;
			END IF;
			
			SET S_IP = REPLACE(gt_strtok(l_GW_URI,3,':'),'/','');
			SET S_PORT = REPLACE(gt_strtok(l_GW_URI,4,':'),'/','');
			SET S_ACCOUNT = l_GW_USER;
			SET S_PWD = l_GW_PASSWORD;			
			SET S_TBL_NAME = CONCAT('session_information_',S_IP,'_',S_PORT);
			SET T_TBL_NAME = CONCAT('table_call_cnt_',S_IP,'_',S_PORT);
	
			SET @SqlCmd=CONCAT('DROP TABLE IF EXISTS ',DB,'.`',S_TBL_NAME,'` ;');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT('CREATE TABLE ',DB,'.`',S_TBL_NAME,'`
						(					
						  `SESSION_ID` BIGINT(20) DEFAULT NULL,
						  `SESSION_DB` VARCHAR(100) NOT NULL,
						  `RNC` VARCHAR(100) DEFAULT NULL,
						  `FILE_STARTTIME` DATETIME DEFAULT NULL,
						  `FILE_ENDTIME` DATETIME DEFAULT NULL,
						  `STATUS` TINYINT(4) DEFAULT NULL COMMENT ''0-DEFAULT,1-RPT SP DONE'',
						  `IMPORT_TIME` DATETIME DEFAULT NULL,
						  `SESSION_START` DATETIME DEFAULT NULL,
						  `SESSION_END` DATETIME DEFAULT NULL,
						  `POSITION_VERSION` VARCHAR(50) DEFAULT NULL,
						  `POSITION_START` DATETIME DEFAULT NULL,
						  `POSITION_END` DATETIME DEFAULT NULL,
						  `SP_VERSION` VARCHAR(50) DEFAULT NULL,
						  `SP_STARTTIME` DATETIME DEFAULT NULL,
						  `SP_ENDTIME` DATETIME DEFAULT NULL,
						  `ORG_DB` VARCHAR(100) DEFAULT NULL,
						  `ORG_SESSION_NAME` VARCHAR(100) DEFAULT NULL,
						  `ORG_SESSION_IP` VARCHAR(15) DEFAULT NULL,
						  `ORG_SESSION_PORT` SMALLINT(6) DEFAULT NULL,
						  `REAL_SESSION_NAME` VARCHAR(100) DEFAULT NULL,
						  `DATA_VENDOR` TINYINT(4) UNSIGNED DEFAULT ''1'',
						  `DELETABLE` TINYINT(1) DEFAULT ''1'',
						  `GW_IP` VARCHAR(50) DEFAULT NULL,
						  `RNC_VERSION` VARCHAR(10) DEFAULT NULL,
						  `SESSION_TYPE` VARCHAR(10) DEFAULT ''DAY'',
						  `TECHNOLOGY` VARCHAR(10) NOT NULL,
						  `GW_URI` VARCHAR(512) DEFAULT NULL,
						  `AP_URI` VARCHAR(512) DEFAULT NULL,
					          `DS_AP_URI` VARCHAR(512) DEFAULT NULL,
						  PRIMARY KEY (`SESSION_DB`,`TECHNOLOGY`)
						) ENGINE=FEDERATED DEFAULT CHARSET=latin1 CONNECTION=''mysql://',S_ACCOUNT,':',S_PWD,'@',S_IP,':',S_PORT,'/gt_gw_main/session_information''');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt; 
	
			SET @SqlCmd=CONCAT('DROP TABLE IF EXISTS ',DB,'.`',T_TBL_NAME,'` ;');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT('CREATE TABLE ',DB,'.`',T_TBL_NAME,'`
						(					
						  `DATA_DATE` DATE NOT NULL,
						  `DATA_HOUR` TINYINT(4) NOT NULL,
						  `PU_ID` MEDIUMINT(9) NOT NULL,
						  `SERVICETYPE` VARCHAR(10) NOT NULL,
						  `TOT_CALL_CNT` BIGINT(9) DEFAULT NULL,
						  `TECH_MASK` TINYINT(4) NOT NULL DEFAULT ''2'',
						  `NOTE` VARCHAR(100),
						  PRIMARY KEY (`DATA_DATE`,`DATA_HOUR`,`PU_ID`,`SERVICETYPE`,`TECH_MASK`)
						) ENGINE=FEDERATED DEFAULT CHARSET=latin1 CONNECTION=''mysql://',S_ACCOUNT,':',S_PWD,'@',S_IP,':',S_PORT,'/gt_gw_main/table_call_cnt''');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt; 
                END IF;
        UNTIL no_more_maps
        END REPEAT dept_loop;
 
        CLOSE csr;
        SET no_more_maps=0;
END$$
DELIMITER ;
