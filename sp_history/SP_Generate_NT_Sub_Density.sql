DELIMITER $$
USE `gt_gw_main`$$
DROP PROCEDURE IF EXISTS `SP_CreateDB_LTE`$$
CREATE DEFINER=`covmo`@`%` PROCEDURE `SP_Generate_NT_Sub_Density`(IN GT_DB VARCHAR(100),IN GT_COVMO VARCHAR(100),IN TECH VARCHAR(10))
a_label:
BEGIN
	DECLARE START_TIME DATETIME DEFAULT SYSDATE();
	DECLARE TBL VARCHAR(20);
	DECLARE COL VARCHAR(30);
	DECLARE DEN1 SMALLINT(6) DEFAULT 601;
	DECLARE DEN2 SMALLINT(6) DEFAULT 1201;
	DECLARE DEN3 SMALLINT(6) DEFAULT 2401;
	DECLARE DEN4 SMALLINT(6) DEFAULT 3601;
	
  	INSERT INTO gt_gw_main.SP_LOG VALUES(GT_DB,'SP_Generate_NT_Sub_Density','nt_cell_current_lte', NOW());
	SET @SqlCmd=CONCAT('SELECT att_value INTO @d1 FROM gt_covmo.sys_config WHERE group_name = ''NT'' AND att_name = ''Density_type_1'';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt; 
	
	SET @SqlCmd=CONCAT('SELECT att_value INTO @d2 FROM gt_covmo.sys_config WHERE group_name = ''NT'' AND att_name = ''Density_type_2'';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt; 
	
	SET @SqlCmd=CONCAT('SELECT att_value INTO @d3 FROM gt_covmo.sys_config WHERE group_name = ''NT'' AND att_name = ''Density_type_3'';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt; 
	
	SET @SqlCmd=CONCAT('SELECT att_value INTO @d4 FROM gt_covmo.sys_config WHERE group_name = ''NT'' AND att_name = ''Density_type_4'';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt; 
	IF @d1 IS NOT NULL THEN SET DEN1 = 600;	END IF;
	IF @d2 IS NOT NULL THEN SET DEN2 = 1200; END IF;
	IF @d3 IS NOT NULL THEN SET DEN3 = 2400; END IF;
	IF @d4 IS NOT NULL THEN SET DEN4 = 3600; END IF;
	IF TECH = 'lte' THEN
		SET TBL = 'nt_cell_current_lte';
		SET COL = 'NBR_DISTANCE_4G_VORONOI';
	ELSEIF TECH = 'umts' THEN
		SET TBL = 'nt_current';
		SET COL = 'NB_AVG_DISTANCE_VORONOI';
	ELSEIF TECH = 'gsm' THEN
		SET TBL = 'nt_cell_current_gsm';
		SET COL = 'NBR_AVG_DISTANCE_VORONOI';
	END IF;
	SET @SqlCmd=CONCAT('UPDATE ',GT_DB,'.',TBL,'
			SET SITE_DENSITY_TYPE = CASE WHEN ',COL,' <= ',DEN1,' THEN 1
					WHEN ',DEN1,' < ',COL,' and ',COL,' <= ',DEN2,' THEN 2
					WHEN ',DEN2,' < ',COL,' and ',COL,' <= ',DEN3,' THEN 3
					WHEN ',DEN3,' < ',COL,' and ',COL,' <= ',DEN4,' THEN 4
					WHEN ',DEN4,' < ',COL,' THEN 5
				END
			WHERE ',COL,' IS NOT NULL;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	SET @SqlCmd=CONCAT('UPDATE ',GT_DB,'.',TBL,'
			SET SITE_DENSITY_TYPE = 2
			WHERE ',COL,' IS NULL;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	INSERT INTO gt_gw_main.SP_LOG VALUES(GT_DB,'SP_Generate_NT_Sub_Density',CONCAT('Done: ',TIMESTAMPDIFF(SECOND,START_TIME,SYSDATE()),' seconds.'), NOW());
	
	
END$$
DELIMITER ;
