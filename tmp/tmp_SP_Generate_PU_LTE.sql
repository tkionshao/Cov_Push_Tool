CREATE DEFINER=`covmo`@`%` PROCEDURE `SP_Generate_PU_LTE`(IN GT_DB VARCHAR(100),IN GT_COVMO VARCHAR(100))
BEGIN
	DECLARE O_GT_DB VARCHAR(100) DEFAULT GT_DB;
	DECLARE START_TIME DATETIME DEFAULT SYSDATE();
	DECLARE WORKER_ID VARCHAR(10) DEFAULT CONNECTION_ID();
	
	
	INSERT INTO gt_gw_main.sp_log VALUES(O_GT_DB,'SP_Generate_PU_LTE','DTAG START', NOW());
	
	SET @SqlCmd=CONCAT('DROP   TABLE IF EXISTS `',GT_DB,'`.`cur_pu_enodeb_mapping_',WORKER_ID,'`;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt; 
	
	SET @SqlCmd=CONCAT('CREATE  TABLE ',GT_DB,'.`cur_pu_enodeb_mapping_',WORKER_ID,'`( 
				`ENODEB_ID` INT(11) DEFAULT NULL,
				`ENODEB_NAME` VARCHAR(50) DEFAULT NULL,
				`CLUSTER` INT(11) DEFAULT NULL,
				`CLUSTER_NAME_SUB_REGION` VARCHAR(50) DEFAULT NULL,
				`PU_ID` INT(11) DEFAULT NULL,
				`VENDOR` VARCHAR(50) DEFAULT NULL,
				PRIMARY KEY(ENODEB_ID))
	ENGINE=MYISAM DEFAULT CHARSET=latin1 DELAY_KEY_WRITE=1;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('INSERT INTO `',GT_DB,'`.`cur_pu_enodeb_mapping_',WORKER_ID,'`(ENODEB_ID,ENODEB_NAME,CLUSTER,CLUSTER_NAME_SUB_REGION,PU_ID,VENDOR)
	SELECT ENODEB_ID,ENODEB_NAME,CLUSTER,CLUSTER_NAME_SUB_REGION,PU_ID,VENDOR
	from gt_gw_main.pu_enodeb_mapping
	;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt; 
	
	INSERT INTO gt_gw_main.sp_log VALUES(O_GT_DB,'SP_Generate_PU_LTE','UPDATE table gt_gw_main.pu_enodeb_mapping PU_ID by gt_covmo.usr_pu_region', NOW());
	
	SET @SqlCmd=CONCAT('UPDATE ',GT_DB,'.`cur_pu_enodeb_mapping_',WORKER_ID,'`  A, gt_covmo.usr_pu_region B
				SET  A.PU_ID = B.PU_ID
				WHERE A.CLUSTER_NAME_SUB_REGION = B.region_name;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('SELECT COUNT(*)  into @cell_map_exists FROM ',GT_DB,'.cell_pu_mapping;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	IF @cell_map_exists > 0
	
	THEN 
		
		INSERT INTO gt_gw_main.sp_log VALUES(O_GT_DB,'SP_Generate_PU_LTE','Have NTDB.cell_pu_mapping (data from /opt/covmo/gen_nt_db_lte.sh get NW table_cell_lte_hr) ', NOW());
	
		SET @SqlCmd=CONCAT('DROP  TABLE IF EXISTS `',GT_DB,'`.`cur_cell_pu_mapping_',WORKER_ID,'`;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt; 
		
		SET @SqlCmd=CONCAT('CREATE  TABLE ',GT_DB,'.`cur_cell_pu_mapping_',WORKER_ID,'`( 
					`DATA_DATE` DATE NOT NULL,
					`PU_ID` MEDIUMINT(9) NOT NULL,
					`ENODEB_ID` MEDIUMINT(9) NOT NULL,
					`ENODEB_NAME`  VARCHAR(150) DEFAULT NULL,
					`CELL_ID` MEDIUMINT(9) NOT NULL,
					`VENDOR` VARCHAR(50) DEFAULT NULL,
					PRIMARY KEY (`CELL_ID`,`ENODEB_ID`)) 
		ENGINE=MYISAM DEFAULT CHARSET=latin1 DELAY_KEY_WRITE=1;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		
		SET @SqlCmd=CONCAT('INSERT ignore INTO `',GT_DB,'`.`cur_cell_pu_mapping_',WORKER_ID,'`(DATA_DATE,PU_ID,ENODEB_ID,ENODEB_NAME,CELL_ID,VENDOR)
		SELECT DATA_DATE,a.PU_ID,a.ENODEB_ID,b.ENODEB_NAME,a.CELL_ID,b.ENODEB_VENDOR
		from ',GT_DB,'.cell_pu_mapping A LEFT JOIN ',GT_DB,'.nt_cell_lte B
		ON A.ENODEB_ID = B.ENODEB_ID 
		WHERE b.ENODEB_NAME is NOT NULL
		order by DATA_HOUR desc
		;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt; 
		
-- 		SET @SqlCmd=CONCAT('INSERT ignore INTO `',GT_DB,'`.`cur_cell_pu_mapping_',WORKER_ID,'`(DATA_DATE,PU_ID,ENODEB_ID,ENODEB_NAME,CELL_ID,VENDOR)
-- 		SELECT DATA_DATE,a.PU_ID,a.ENODEB_ID,b.ENODEB_NAME,a.CELL_ID,b.ENODEB_VENDOR
-- 		from ',GT_DB,'.cell_pu_mapping A LEFT JOIN ',GT_DB,'.nt_cell_current_lte_dump B
-- 		ON A.ENODEB_ID = B.ENODEB_ID 
-- 		WHERE b.ENODEB_NAME is NOT NULL
-- 		order by DATA_HOUR desc
-- 		;');
-- 		PREPARE Stmt FROM @SqlCmd;
-- 		EXECUTE Stmt;
-- 		DEALLOCATE PREPARE Stmt; 
		
		INSERT INTO gt_gw_main.sp_log VALUES(O_GT_DB,'SP_Generate_PU_LTE','Update gt_gw_main.pu_enodeb_mapping when NW have new ENODEB', NOW());
		SET @SqlCmd=CONCAT('INSERT ignore INTO `',GT_DB,'`.`cur_pu_enodeb_mapping_',WORKER_ID,'`(PU_ID,ENODEB_ID,ENODEB_NAME,VENDOR)
		SELECT PU_ID,ENODEB_ID,ENODEB_NAME,VENDOR
		FROM `',GT_DB,'`.`cur_cell_pu_mapping_',WORKER_ID,'`
		;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt; 
	
		INSERT INTO gt_gw_main.sp_log VALUES(O_GT_DB,'SP_Generate_PU_LTE','use NTDB.cell_pu_mapping update PU_ID ', NOW());
	
		SET @SqlCmd=CONCAT('UPDATE ',GT_DB,'.`nt_cell_lte` A, ',GT_DB,'.`cell_pu_mapping` B
					SET 
						A.PU_ID = B.PU_ID,
						A.SUB_REGION_ID =B.PU_ID						
					WHERE A.ENODEB_ID = B.ENODEB_ID;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		
	ELSE
		
		SELECT 'no cell_pu_mapping';
	 
	END IF;
	
	INSERT INTO gt_gw_main.sp_log VALUES(O_GT_DB,'SP_Generate_PU_LTE','UPDATE table gt_gw_main.pu_enodeb_mapping CLUSTER_NAME_SUB_REGION by gt_covmo.usr_pu_region', NOW());
	SET @SqlCmd=CONCAT('UPDATE ',GT_DB,'.`cur_pu_enodeb_mapping_',WORKER_ID,'`  A, gt_covmo.usr_pu_region B
				SET  A.CLUSTER_NAME_SUB_REGION = B.region_name
				WHERE A.PU_ID = B.PU_ID AND A.CLUSTER_NAME_SUB_REGION IS NULL;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	
	INSERT INTO gt_gw_main.sp_log VALUES(O_GT_DB,'SP_Generate_PU_LTE','UPDATE nt_cell_current_lte by gt_gw_main.pu_enodeb_mapping', NOW());
	SET @SqlCmd=CONCAT('UPDATE ',GT_DB,'.`nt_cell_lte` A, ',GT_DB,'.`cur_pu_enodeb_mapping_',WORKER_ID,'` B
				SET 
					A.PU_ID = B.PU_ID,
					A.SUB_REGION_ID =B.PU_ID,
					A.CLUSTER_NAME_SUB_REGION =B.CLUSTER_NAME_SUB_REGION
				WHERE A.ENODEB_ID = B.ENODEB_ID ;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('REPLACE INTO  
	gt_gw_main.pu_enodeb_mapping(ENODEB_ID,ENODEB_NAME,CLUSTER,CLUSTER_NAME_SUB_REGION,PU_ID,VENDOR)
	SELECT ENODEB_ID,ENODEB_NAME,CLUSTER,CLUSTER_NAME_SUB_REGION,PU_ID,VENDOR
	from 
	`',GT_DB,'`.`cur_pu_enodeb_mapping_',WORKER_ID,'`
	where PU_ID <> 0 AND PU_ID IS NOT NULL
	;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt; 

	INSERT INTO gt_gw_main.sp_log VALUES(O_GT_DB,'SP_Generate_PU_LTE','Update NTDB.cell_pu_mapping PU_ID #26965(source data issue GTGW duplicate)', NOW());
	
	SET @SqlCmd=CONCAT('CREATE TABLE ',GT_DB,'.tmp_cell_pu_mapping AS (SELECT * FROM ',GT_DB,'.cell_pu_mapping);');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt; 
	
	SET @SqlCmd=CONCAT('UPDATE ',GT_DB,'.tmp_cell_pu_mapping A,`',GT_DB,'`.`cur_pu_enodeb_mapping_',WORKER_ID,'` B 
		SET 
			A.PU_ID = B.PU_ID
		WHERE A.ENODEB_ID = B.ENODEB_ID;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt; 
	
	SET @SqlCmd=CONCAT('REPLACE INTO ',GT_DB,'.cell_pu_mapping SELECT * FROM ',GT_DB,'.tmp_cell_pu_mapping;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt; 
	
	SET @SqlCmd=CONCAT('DROP TABLE IF EXISTS ',GT_DB,'.tmp_cell_pu_mapping;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt; 
	
	INSERT INTO gt_gw_main.sp_log VALUES(O_GT_DB,'SP_Generate_PU_LTE','INSERT PU (NTDB.nt_pu_current)', NOW());
	
	SET @SqlCmd=CONCAT('TRUNCATE TABLE ',GT_DB,'.`nt_pu_current`; ');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt; 
	
	SET @SqlCmd=CONCAT('	INSERT INTO ',GT_DB,'.`nt_pu_current`
				(PU_ID,PU_NAME,TECH_MASK,VEDNOR,SITE_CNT,CELL_CNT)
				SELECT  DISTINCT A.PU_ID,RNC_NAME AS PU_NAME,4 AS TECH_MASK ,VENDOR_NAME,
				COUNT(DISTINCT ENODEB_ID) SITE_CNT ,COUNT(DISTINCT CONCAT (ENODEB_ID,''@'',CELL_ID)) CELL_CNT
 				 FROM ',GT_DB,'.nt_cell_lte a
 				LEFT JOIN gt_gw_main.rnc_information b
				ON A.PU_ID=B.RNC 
			LEFT JOIN ',GT_COVMO,'.dim_vendor c
				ON b.vendor_id=c.vendor_id 
 				GROUP BY A.PU_ID						
 				; ');
	PREPARE Stmt FROM @SqlCmd;
 	EXECUTE Stmt;
 	DEALLOCATE PREPARE Stmt; 
 
	SET @SqlCmd=CONCAT('DROP  TABLE IF EXISTS `',GT_DB,'`.`cur_cell_pu_mapping_',WORKER_ID,'`;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt; 
	
	SET @SqlCmd=CONCAT('DROP  TABLE IF EXISTS `',GT_DB,'`.`cur_pu_enodeb_mapping_',WORKER_ID,'`;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt; 
	
	SET @SqlCmd=CONCAT('SELECT SUBSTRING_INDEX(''',GT_DB,''',''_'',-1) INTO @NTDBDate;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt; 
	
	INSERT INTO gt_gw_main.sp_log VALUES(O_GT_DB,'SP_Generate_PU_LTE',CONCAT('Done: ',TIMESTAMPDIFF(SECOND,START_TIME,SYSDATE()),' seconds.'), NOW());
		
