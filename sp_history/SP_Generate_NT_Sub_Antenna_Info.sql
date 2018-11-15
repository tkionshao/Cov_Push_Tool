DELIMITER $$
USE `gt_gw_main`$$
DROP PROCEDURE IF EXISTS `SP_CreateDB_LTE`$$
CREATE DEFINER=`covmo`@`%` PROCEDURE `SP_Generate_NT_Sub_Antenna_Info`(IN GT_DB VARCHAR(100))
BEGIN
	
	DECLARE START_TIME DATETIME DEFAULT SYSDATE();
	DECLARE WORKER_ID VARCHAR(10) DEFAULT CONNECTION_ID();
	SELECT `value` INTO @enabledGSM FROM gt_gw_main.integration_param WHERE gt_group = 'ntparser' AND gt_name = 'enabledGSM';
	SELECT `value` INTO @enabledLTE FROM gt_gw_main.integration_param WHERE gt_group = 'ntparser' AND gt_name = 'enabledLTE';
	SELECT `value` INTO @enabledUMTS FROM gt_gw_main.integration_param WHERE gt_group = 'ntparser' AND gt_name = 'enabledUMTS';
	
	INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'SP_Generate_NT_Sub_Antenna_Info','Start', NOW());
	INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'SP_Generate_NT_Sub_Antenna_Info','Insert the antenna from gt_gw_main.antenna_info', NOW());
	
	SET @SqlCmd=CONCAT(' DELETE FROM ',GT_DB,'.`antenna_info` WHERE antenna_group IS NULL OR antenna_group = '''';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	SET @SqlCmd=CONCAT('DROP TABLE IF EXISTS ',GT_DB,'.tmp_antenna_model','_',WORKER_ID,' ;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;	
	SET @SqlCmd=CONCAT('CREATE TABLE ',GT_DB,'.tmp_antenna_model','_',WORKER_ID,'(
			  `ANTENNA_MODEL` VARCHAR(100) NOT NULL DEFAULT '''',
			  PRIMARY KEY (`ANTENNA_MODEL`)
			) ENGINE=MYISAM DEFAULT CHARSET=utf8;
			');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	IF @enabledGSM = 'true' THEN
		SET @SqlCmd=CONCAT('INSERT IGNORE INTO ',GT_DB,'.tmp_antenna_model','_',WORKER_ID,'
				SELECT DISTINCT ANTENNA_MODEL FROM ',GT_DB,'.nt_antenna_current_gsm;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
	END IF;
	IF @enabledLTE = 'true' THEN
		SET @SqlCmd=CONCAT('INSERT IGNORE INTO ',GT_DB,'.tmp_antenna_model','_',WORKER_ID,'
				SELECT DISTINCT ANTENNA_MODEL FROM ',GT_DB,'.nt_antenna_current_lte;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
	END IF;
	IF @enabledUMTS = 'true' THEN
		SET @SqlCmd=CONCAT('INSERT IGNORE INTO ',GT_DB,'.tmp_antenna_model','_',WORKER_ID,'
				SELECT DISTINCT ANTENNA_MODEL FROM ',GT_DB,'.nt_antenna_current;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
	END IF;
	SET @SqlCmd=CONCAT('
			INSERT IGNORE INTO ',GT_DB,'.antenna_info
			SELECT C.*
			FROM gt_gw_main.antenna_info C RIGHT JOIN
			(
				SELECT DISTINCT ANTENNA_GROUP 
					FROM gt_gw_main.antenna_info A,
					(SELECT DISTINCT ANTENNA_MODEL FROM ',GT_DB,'.tmp_antenna_model','_',WORKER_ID,') B
				WHERE A.ANTENNA_MODEL = B.ANTENNA_MODEL
			) D
			ON C.ANTENNA_GROUP = D.ANTENNA_GROUP;
			');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'SP_Generate_NT_Sub_Antenna_Info','Insert DEFAULT VALUE for antenna_info by nt db', NOW());
	SET @SqlCmd=CONCAT('SELECT COUNT(*) INTO @D_SECTOR FROM ',GT_DB,'.`antenna_info` WHERE ANTENNA_MODEL = ''DEFAULT_SECTOR'';');	
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('SELECT COUNT(*) INTO @D_SECTOR_MAPPING  FROM ',GT_DB,'.tmp_antenna_model','_',WORKER_ID,' A, ',GT_DB,'.`antenna_info` B WHERE A.antenna_model = B.antenna_model AND B.TYPE = 2 AND B.HBW3 < 70;');	
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	IF @D_SECTOR = 0 THEN 
		IF @D_SECTOR_MAPPING = 0 THEN 
			SET @SqlCmd=CONCAT('INSERT INTO ',GT_DB,'.`antenna_info` 
					    (`ANTENNA_MODEL`,`TYPE`,  `GAIN_DBI`,  `HBW3`,  `HBW3_CL`,  `HBW3_CW`,  `HBW6`,  `HBW6_CL`,  `HBW6_CW`,  `VBW3`,  `VBW3_CL`,  `VBW3_CW`,  `VBW6`,  `VBW6_CL`,  `VBW6_CW`,  `HORIZONTAL_360`,  `VERTICAL_360`,  `CATEGORIZE`,  `MANUFACTURER`,  `TILT_TYPE`,  `ELECTRICAL_TILT`,  `MECHANICAL_TILT`,  `FREQUENCY`, `ANTENNA_GROUP`,`DEVIATION`)
					    SELECT ''DEFAULT_SECTOR'' AS `ANTENNA_MODEL`,`TYPE`,  `GAIN(dBi)` as GAIN_DBI,  `HBW3`,  `HBW3_CL`,  `HBW3_CW`,  `HBW6`,  `HBW6_CL`,  `HBW6_CW`,  `VBW3`,  `VBW3_CL`,  `VBW3_CW`,  `VBW6`,  `VBW6_CL`,  `VBW6_CW`,  `HORIZONTAL_360`,  `VERTICAL_360`,  `CATEGORIZE`,  `MANUFACTURER`,  `TILT_TYPE`,  `ELECTRICAL_TILT`,  `MECHANICAL_TILT`,  `FREQUENCY`, `ANTENNA_GROUP`, `DEVIATION`			    FROM gt_gw_main.`antenna_info`	
					    WHERE `TYPE`=2 AND HBW3 < 70 LIMIT 1;');	
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
		ELSE
			SET @SqlCmd=CONCAT('INSERT INTO ',GT_DB,'.`antenna_info` 
					    (`ANTENNA_MODEL`,`TYPE`,  `GAIN_DBI`,  `HBW3`,  `HBW3_CL`,  `HBW3_CW`,  `HBW6`,  `HBW6_CL`,  `HBW6_CW`,  `VBW3`,  `VBW3_CL`,  `VBW3_CW`,  `VBW6`,  `VBW6_CL`,  `VBW6_CW`,  `HORIZONTAL_360`,  `VERTICAL_360`,  `CATEGORIZE`,  `MANUFACTURER`,  `TILT_TYPE`,  `ELECTRICAL_TILT`,  `MECHANICAL_TILT`,  `FREQUENCY`, `ANTENNA_GROUP`,`DEVIATION`)
					    SELECT ''DEFAULT_SECTOR'' AS `ANTENNA_MODEL`,`TYPE`,  `GAIN_dBi`,  `HBW3`,  `HBW3_CL`,  `HBW3_CW`,  `HBW6`,  `HBW6_CL`,  `HBW6_CW`,  `VBW3`,  `VBW3_CL`,  `VBW3_CW`,  `VBW6`,  `VBW6_CL`,  `VBW6_CW`,  `HORIZONTAL_360`,  `VERTICAL_360`,  `CATEGORIZE`,  `MANUFACTURER`,  `TILT_TYPE`,  `ELECTRICAL_TILT`,  `MECHANICAL_TILT`,  `FREQUENCY`, `ANTENNA_GROUP`,`DEVIATION`			    FROM ',GT_DB,'.`antenna_info`	
					    WHERE ANTENNA_MODEL IN (SELECT `ANTENNA_MODEL` 
								      FROM 
									(
										SELECT  A.`ANTENNA_MODEL`, COUNT(*) AS ANTENNA_CNT
										FROM ',GT_DB,'.tmp_antenna_model','_',WORKER_ID,' A , ',GT_DB,'.`antenna_info` B
										WHERE A.`ANTENNA_MODEL`=B.`ANTENNA_MODEL`
										AND B.`TYPE`=2
										GROUP BY A.`ANTENNA_MODEL`
										ORDER BY ANTENNA_CNT DESC
										LIMIT 1
									) AA 
								     );	');	
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
		END IF;
	END IF;
	
	SET @SqlCmd=CONCAT('SELECT COUNT(*) INTO @D_OMNI FROM ',GT_DB,'.`antenna_info` WHERE ANTENNA_MODEL = ''DEFAULT_OMNI'';');	
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('SELECT COUNT(*) INTO @D_OMNI_MAPPING  FROM ',GT_DB,'.tmp_antenna_model','_',WORKER_ID,' A, ',GT_DB,'.`antenna_info` B WHERE A.antenna_model = B.antenna_model AND B.TYPE = 1 AND B.HBW3 = 360;');	
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	IF @D_OMNI = 0 THEN 
		IF @D_OMNI_MAPPING = 0 THEN 
			SET @SqlCmd=CONCAT('INSERT INTO ',GT_DB,'.`antenna_info` 
					    (`ANTENNA_MODEL`,`TYPE`,  `GAIN_DBI`,  `HBW3`,  `HBW3_CL`,  `HBW3_CW`,  `HBW6`,  `HBW6_CL`,  `HBW6_CW`,  `VBW3`,  `VBW3_CL`,  `VBW3_CW`,  `VBW6`,  `VBW6_CL`,  `VBW6_CW`,  `HORIZONTAL_360`,  `VERTICAL_360`,  `CATEGORIZE`,  `MANUFACTURER`,  `TILT_TYPE`,  `ELECTRICAL_TILT`,  `MECHANICAL_TILT`,  `FREQUENCY`, `ANTENNA_GROUP`,`DEVIATION`)
					    SELECT ''DEFAULT_OMNI'' AS `ANTENNA_MODEL`,`TYPE`,  `GAIN(dBi)` as GAIN_DBI,  `HBW3`,  `HBW3_CL`,  `HBW3_CW`,  `HBW6`,  `HBW6_CL`,  `HBW6_CW`,  `VBW3`,  `VBW3_CL`,  `VBW3_CW`,  `VBW6`,  `VBW6_CL`,  `VBW6_CW`,  `HORIZONTAL_360`,  `VERTICAL_360`,  `CATEGORIZE`,  `MANUFACTURER`,  `TILT_TYPE`,  `ELECTRICAL_TILT`,  `MECHANICAL_TILT`,  `FREQUENCY`, `ANTENNA_GROUP`,`DEVIATION`
					    FROM gt_gw_main.`antenna_info`	
					    WHERE `TYPE`= 1 AND HBW3 = 360 LIMIT 1;');	
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
		ELSE
			SET @SqlCmd=CONCAT('INSERT INTO ',GT_DB,'.`antenna_info` 
					    (`ANTENNA_MODEL`,`TYPE`,  `GAIN_DBI`,  `HBW3`,  `HBW3_CL`,  `HBW3_CW`,  `HBW6`,  `HBW6_CL`,  `HBW6_CW`,  `VBW3`,  `VBW3_CL`,  `VBW3_CW`,  `VBW6`,  `VBW6_CL`,  `VBW6_CW`,  `HORIZONTAL_360`,  `VERTICAL_360`,  `CATEGORIZE`,  `MANUFACTURER`,  `TILT_TYPE`,  `ELECTRICAL_TILT`,  `MECHANICAL_TILT`,  `FREQUENCY`, `ANTENNA_GROUP`,`DEVIATION`)
					    SELECT ''DEFAULT_OMNI'' AS `ANTENNA_MODEL`,`TYPE`,  `GAIN_dBi`,  `HBW3`,  `HBW3_CL`,  `HBW3_CW`,  `HBW6`,  `HBW6_CL`,  `HBW6_CW`,  `VBW3`,  `VBW3_CL`,  `VBW3_CW`,  `VBW6`,  `VBW6_CL`,  `VBW6_CW`,  `HORIZONTAL_360`,  `VERTICAL_360`,  `CATEGORIZE`,  `MANUFACTURER`,  `TILT_TYPE`,  `ELECTRICAL_TILT`,  `MECHANICAL_TILT`,  `FREQUENCY`, `ANTENNA_GROUP`,`DEVIATION`
					    FROM ',GT_DB,'.`antenna_info`	
					    WHERE ANTENNA_MODEL IN (SELECT `ANTENNA_MODEL` 
								      FROM 
									(
										SELECT  A.`ANTENNA_MODEL`, COUNT(*) AS ANTENNA_CNT
										FROM ',GT_DB,'.tmp_antenna_model','_',WORKER_ID,' A , ',GT_DB,'.`antenna_info` B
										WHERE A.`ANTENNA_MODEL`=B.`ANTENNA_MODEL`
										AND B.`TYPE`=1
										GROUP BY A.`ANTENNA_MODEL`
										ORDER BY ANTENNA_CNT DESC
										LIMIT 1
									) AA 
								     );	');	
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
		END IF;
	END IF;
	INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'SP_Generate_NT_Sub_Antenna_Info','Dump', NOW());
	
	SET @SqlCmd=CONCAT('SELECT COUNT(*) INTO @CHECK_CNT FROM ',GT_DB,'.antenna_info 
				WHERE
				gt_covmo_csv_count(HORIZONTAL_360,''|'') < 360 
				OR gt_covmo_csv_count(VERTICAL_360,''|'') < 360;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt; 
	
	IF @CHECK_CNT > 0 THEN
		
		SET @SqlCmd=CONCAT('INSERT INTO ',GT_DB,'.nt_log (TBL_NAME,LOG_TYPE,DUMP_LOG)
				SELECT
					''antenna_info''
					,''1'' # 1 IS DUMP
					,''360 ERROR''
				FROM ',GT_DB,'.antenna_info
				WHERE
				gt_covmo_csv_count(HORIZONTAL_360,''|'') < 360 
				OR gt_covmo_csv_count(VERTICAL_360,''|'') < 360;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt; 
		SET @SqlCmd=CONCAT('CREATE TABLE IF NOT EXISTS ',GT_DB,'.antenna_info_dump LIKE ',GT_DB,'.antenna_info;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt; 
		
		SET @SqlCmd=CONCAT('DELETE A FROM ',GT_DB,'.antenna_info_dump A
				, ',GT_DB,'.antenna_info B
				WHERE A.antenna_model = B.antenna_model AND(
				gt_covmo_csv_count(B.HORIZONTAL_360,''|'') < 360 
				OR gt_covmo_csv_count(B.VERTICAL_360,''|'') < 360);');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt; 
		SET @SqlCmd=CONCAT('INSERT INTO ',GT_DB,'.antenna_info_dump
				SELECT * FROM ',GT_DB,'.antenna_info
				WHERE
				gt_covmo_csv_count(HORIZONTAL_360,''|'') < 360 
				OR gt_covmo_csv_count(VERTICAL_360,''|'') < 360;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt; 
	
		
		SET @SqlCmd=CONCAT('DELETE FROM ',GT_DB,'.antenna_info
				WHERE
				gt_covmo_csv_count(HORIZONTAL_360,''|'') < 360 
				OR gt_covmo_csv_count(VERTICAL_360,''|'') < 360;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt; 
	END IF;
	
	INSERT INTO gt_gw_main.SP_LOG VALUES(GT_DB,'SP_Generate_NT_Sub_Antenna_Info',CONCAT('Done: ',TIMESTAMPDIFF(SECOND,START_TIME,SYSDATE()),' seconds.'), NOW());
END$$
DELIMITER ;
