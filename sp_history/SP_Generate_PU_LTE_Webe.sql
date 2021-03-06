DELIMITER $$
USE `gt_gw_main`$$
DROP PROCEDURE IF EXISTS `SP_CreateDB_LTE`$$
CREATE DEFINER=`covmo`@`%` PROCEDURE `SP_Generate_PU_LTE_Webe`(IN GT_DB VARCHAR(100),IN GT_COVMO VARCHAR(100))
BEGIN
	DECLARE O_GT_DB VARCHAR(100) DEFAULT GT_DB;
	DECLARE START_TIME DATETIME DEFAULT SYSDATE();
	DECLARE WORKER_ID VARCHAR(10) DEFAULT CONNECTION_ID();
	
	
	INSERT INTO gt_gw_main.sp_log VALUES(O_GT_DB,'SP_Generate_PU_LTE','START', NOW());
	
	INSERT INTO gt_gw_main.sp_log VALUES(O_GT_DB,'SP_Generate_PU_LTE','Update pu_enodeb_mapping', NOW());
	
	SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS `',GT_DB,'`.`pu_enodeb_mapping_',WORKER_ID,'`;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt; 
	
	SET @SqlCmd=CONCAT('CREATE TEMPORARY TABLE ',GT_DB,'.`pu_enodeb_mapping_',WORKER_ID,'` 
				LIKE gt_gw_main.pu_enodeb_mapping;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('INSERT INTO ',GT_DB,'.`pu_enodeb_mapping_',WORKER_ID,'` 
				(ENODEB_ID,ENODEB_NAME,CLUSTER,CLUSTER_NAME_SUB_REGION,VENDOR)
				SELECT DISTINCT
				  ENODEB_ID AS ENODEB_ID,
				  ENODEB_NAME AS ENODEB_NAME,
				  CLUSTER_NAME_REGION AS CLUSTER,
				  CLUSTER_NAME_SUB_REGION AS CLUSTER_NAME_SUB_REGION,
				  ENODEB_VENDOR AS VENDOR
				FROM ',GT_DB,'.nt_cell_current_lte;
				;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('UPDATE ',GT_DB,'.`pu_enodeb_mapping_',WORKER_ID,'` A, gt_gw_main.pu_cluster_mapping B
				SET A.PU_ID = B.PU_ID
				WHERE A.CLUSTER = B.CLUSTER AND A.VENDOR = B.VENDOR;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('TRUNCATE TABLE gt_gw_main.`pu_enodeb_mapping`;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('INSERT INTO gt_gw_main.`pu_enodeb_mapping`
			SELECT * FROM ',GT_DB,'.`pu_enodeb_mapping_',WORKER_ID,'`;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	INSERT INTO gt_gw_main.sp_log VALUES(O_GT_DB,'SP_Generate_PU_LTE','Update dim_sub_region_mapping', NOW());
	
	SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS `',GT_DB,'`.`dim_sub_region_mapping_',WORKER_ID,'`;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt; 
	
	SET @SqlCmd=CONCAT('CREATE TEMPORARY TABLE ',GT_DB,'.`dim_sub_region_mapping_',WORKER_ID,'` 
				SELECT * FROM ',GT_COVMO,'.dim_sub_region_mapping;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS `',GT_DB,'`.`tmp_dim_sub_region_mapping_',WORKER_ID,'`;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt; 
	SET @SqlCmd=CONCAT('CREATE TEMPORARY TABLE ',GT_DB,'.`tmp_dim_sub_region_mapping_',WORKER_ID,'`
			SELECT  B.PU_ID,B.CLUSTER_NAME_SUB_REGION,B.VENDOR FROM ',GT_DB,'.`dim_sub_region_mapping_',WORKER_ID,'` A RIGHT JOIN (
				SELECT DISTINCT CLUSTER_NAME_SUB_REGION,PU_ID,VENDOR FROM gt_gw_main.pu_enodeb_mapping WHERE CLUSTER_NAME_SUB_REGION <> '''') B 
			ON A.CLUSTER_NAME_SUB_REGION = B.CLUSTER_NAME_SUB_REGION AND A.VENDOR = B.VENDOR
			WHERE A.PU_ID IS NULL AND B.PU_ID IS NOT NULL;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('INSERT INTO ',GT_COVMO,'.dim_sub_region_mapping
			(PU_ID,CLUSTER_NAME_SUB_REGION,VENDOR)
			SELECT  
				PU_ID,CLUSTER_NAME_SUB_REGION,VENDOR 
			FROM ',GT_DB,'.`tmp_dim_sub_region_mapping_',WORKER_ID,'`;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	INSERT INTO gt_gw_main.sp_log VALUES(O_GT_DB,'SP_Generate_PU_LTE','Update PU_ID & SUB_REGION_ID', NOW());
	
	SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS `',GT_DB,'`.`cur_dim_sub_region_mapping_',WORKER_ID,'`;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt; 
	
	SET @SqlCmd=CONCAT('CREATE TEMPORARY TABLE ',GT_DB,'.`cur_dim_sub_region_mapping_',WORKER_ID,'` 
				SELECT * FROM ',GT_COVMO,'.dim_sub_region_mapping;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;	
	
	SET @SqlCmd=CONCAT('UPDATE ',GT_DB,'.`nt_cell_current_lte` A, ',GT_DB,'.`cur_dim_sub_region_mapping_',WORKER_ID,'` B
				SET 
					A.PU_ID = B.PU_ID,
					A.SUB_REGION_ID = B.SUB_REGION_ID
				WHERE A.CLUSTER_NAME_SUB_REGION = B.CLUSTER_NAME_SUB_REGION;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	INSERT INTO gt_gw_main.sp_log VALUES(O_GT_DB,'SP_Generate_PU_LTE','Insert PU', NOW());
	
	SET @SqlCmd=CONCAT('TRUNCATE TABLE ',GT_DB,'.`nt_pu_current`; ');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt; 
	
	SET @SqlCmd=CONCAT('	INSERT INTO ',GT_DB,'.`nt_pu_current`
				(PU_ID,PU_NAME,TECH_MASK,VEDNOR,SITE_CNT,CELL_CNT)
				SELECT  DISTINCT A.PU_ID,RNC_NAME AS PU_NAME,4 AS TECH_MASK ,VENDOR_NAME,
				COUNT(DISTINCT ENODEB_ID) SITE_CNT ,COUNT(DISTINCT CONCAT (ENODEB_ID,''@'',CELL_ID)) CELL_CNT
 				 FROM ',GT_DB,'.nt_cell_current_lte a
 				LEFT JOIN gt_gw_main.rnc_information b
				ON A.PU_ID=B.RNC 
			LEFT JOIN ',GT_COVMO,'.dim_vendor c
				ON b.vendor_id=c.vendor_id 
 				GROUP BY A.PU_ID						
 				; ');
	PREPARE Stmt FROM @SqlCmd;
 	EXECUTE Stmt;
 	DEALLOCATE PREPARE Stmt; 
	
	INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'SP_Generate_PU_LTE',CONCAT('Done: ',TIMESTAMPDIFF(SECOND,START_TIME,SYSDATE()),' seconds.'), NOW());
			
END$$
DELIMITER ;
