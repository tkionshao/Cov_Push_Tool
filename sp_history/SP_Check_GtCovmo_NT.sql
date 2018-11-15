DELIMITER $$
USE `gt_gw_main`$$
DROP PROCEDURE IF EXISTS `SP_CreateDB_LTE`$$
CREATE DEFINER=`covmo`@`%` PROCEDURE `SP_Check_GtCovmo_NT`(IN STEP VARCHAR(50),IN ORG_NT_DATE VARCHAR(20),IN GT_COVMO VARCHAR(100))
BEGIN
	DECLARE START_TIME DATETIME DEFAULT SYSDATE();
	DECLARE NT_DATE VARCHAR(20) DEFAULT CONCAT(SUBSTRING(ORG_NT_DATE,1,4),'-',SUBSTRING(ORG_NT_DATE,5,2),'-',SUBSTRING(ORG_NT_DATE,7,2));
	IF STEP = 'Check_NT_ALL_and_tmp_NT_CELL' THEN
		INSERT INTO gt_gw_main.sp_log VALUES(NT_DATE,'SP_Check_GtCovmo_NT','Check_NT_ALL_and_tmp_NT_CELL', NOW());
	
		SET @SqlCmd=CONCAT('SELECT COUNT(*) INTO @NT_ALL_CNT FROM ',GT_COVMO,'.NT_ALL WHERE `NT_DATE` = ''',NT_DATE,'''; ');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		
		IF @NT_ALL_CNT > 0 THEN
			SELECT FALSE;
		ELSE
			SELECT TRUE;
		END IF;	
	ELSEIF STEP = 'Check_NT_tmp_table' THEN
		INSERT INTO gt_gw_main.sp_log VALUES(NT_DATE,'SP_Check_GtCovmo_NT','Check_NT_tmp_table', NOW());
		SELECT COUNT(*) INTO @T_CNT1 FROM `information_schema`.`TABLES` WHERE `TABLE_SCHEMA` = 'gt_gw_main' AND `TABLE_NAME` = 'nt_antenna';
		SELECT COUNT(*) INTO @T_CNT2 FROM `information_schema`.`TABLES` WHERE `TABLE_SCHEMA` = 'gt_gw_main' AND `TABLE_NAME` = 'nt_cell';
		SELECT COUNT(*) INTO @T_CNT3 FROM `information_schema`.`TABLES` WHERE `TABLE_SCHEMA` = 'gt_gw_main' AND `TABLE_NAME` = 'nt_neighbor';
		SELECT COUNT(*) INTO @T_CNT4 FROM `information_schema`.`TABLES` WHERE `TABLE_SCHEMA` = 'gt_gw_main' AND `TABLE_NAME` = 'nt_rnc';
		SELECT COUNT(*) INTO @T_CNT5 FROM `information_schema`.`TABLES` WHERE `TABLE_SCHEMA` = 'gt_gw_main' AND `TABLE_NAME` = 'nt_cell_attribute';
		SELECT COUNT(*) INTO @T_CNT6 FROM `information_schema`.`TABLES` WHERE `TABLE_SCHEMA` = 'gt_gw_main' AND `TABLE_NAME` = 'tmp_antenna_info';
		SELECT COUNT(*) INTO @T_CNT7 FROM `information_schema`.`TABLES` WHERE `TABLE_SCHEMA` = 'gt_gw_main' AND `TABLE_NAME` = 'nt_cell_gsm';
		SELECT COUNT(*) INTO @T_CNT8 FROM `information_schema`.`TABLES` WHERE `TABLE_SCHEMA` = 'gt_gw_main' AND `TABLE_NAME` = 'nt_sc_code';
	
		IF @T_CNT1 != 1 AND @T_CNT2 != 1 AND @T_CNT3 != 1 AND @T_CNT4 != 1 AND @T_CNT5 != 1 AND @T_CNT6 != 1 AND @T_CNT7 != 1 AND @T_CNT8 != 1 THEN
			SELECT TRUE;
		ELSE
			SELECT COUNT(*) INTO @TMP_NT_CNT FROM 
			(
				SELECT NT_DATE FROM gt_gw_main.nt_antenna UNION
				SELECT NT_DATE FROM gt_gw_main.nt_cell UNION
				SELECT NT_DATE FROM gt_gw_main.nt_neighbor UNION
				SELECT NT_DATE FROM gt_gw_main.nt_rnc UNION
				SELECT 'NT_DATE' FROM gt_gw_main.tmp_antenna_info UNION
				SELECT NT_DATE FROM gt_gw_main.nt_cell_gsm
			) A;
			IF @TMP_NT_CNT > 0 THEN
				SELECT FALSE;
			ELSE
				SELECT TRUE;
			END IF;
		END IF;
	ELSEIF STEP = 'Drop_tmp_table' THEN
		INSERT INTO gt_gw_main.sp_log VALUES(NT_DATE,'SP_Check_GtCovmo_NT','Drop_tmp_table', NOW());
		DROP TABLE IF EXISTS gt_gw_main.`nt_antenna`;
		DROP TABLE IF EXISTS gt_gw_main.`nt_cell`;
		DROP TABLE IF EXISTS gt_gw_main.`nt_neighbor`;
		DROP TABLE IF EXISTS gt_gw_main.`nt_rnc`;
		DROP TABLE IF EXISTS gt_gw_main.`nt_cell_attribute`;
		DROP TABLE IF EXISTS gt_gw_main.`tmp_antenna_info`;
		DROP TABLE IF EXISTS gt_gw_main.`nt_cell_gsm`;
		DROP TABLE IF EXISTS gt_gw_main.`nt_sc_code`;
	END IF;
	INSERT INTO gt_gw_main.SP_LOG VALUES(NT_DATE,'SP_Check_GtCovmo_NT',CONCAT('Done: ',TIMESTAMPDIFF(SECOND,START_TIME,SYSDATE()),' seconds.'), NOW());
END$$
DELIMITER ;
