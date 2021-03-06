DELIMITER $$
USE `gt_gw_main`$$
DROP PROCEDURE IF EXISTS `SP_CreateDB_LTE`$$
CREATE DEFINER=`covmo`@`%` PROCEDURE `Import_WINA_miss_DATA_GSM`(IN GT_DB VARCHAR(100))
BEGIN
INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'Import_WINA_miss_DATA_GSM','Update nt_antenna_gsm BCCH_POWER #24393#note-46', NOW());	
	SET @SqlCmd=CONCAT('UPDATE ',GT_DB,'.nt_antenna_gsm a,gt_gw_main.nt_antenna_gsm_bcch_power b,',GT_DB,'.2g_cell_oss_node_id c SET a.BCCH_POWER=b.BCCH_POWER WHERE a.BSC_ID=c.BSC_ID AND a.CELL_ID=c.CELL_ID AND a.LAC=c.LAC AND b.cell_oss_node_id=c.cell_oss_node_id;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt; 
   

 INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'Import_WINA_miss_DATA_GSM','update nt_antenna_gsm bcch_power #27392', NOW());	
	SET @SqlCmd=CONCAT('UPDATE ',GT_DB,'.nt_antenna_gsm a,gt_gw_main.nt_antenna_gsm_bcch_power_2G b,',GT_DB,'.2g_cell_oss_node_id c SET a.BCCH_POWER=b.BCCH_POWER WHERE a.BSC_ID=c.BSC_ID AND a.CELL_ID=c.CELL_ID AND a.LAC=c.LAC AND b.cell_oss_node_id=c.cell_oss_node_id;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	

 INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'Import_WINA_miss_DATA_GSM','cteate temp table 2G_nt_daily_node_id_with_structure', NOW());	
 	SET @SqlCmd=CONCAT('DROP TABLE IF EXISTS ',GT_DB,'.2G_nt_daily_node_id_with_structure;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;  

 	SET @SqlCmd=CONCAT('CREATE TABLE ',GT_DB,'.2G_nt_daily_node_id_with_structure AS SELECT a.BSC_ID, a.CELL_ID,a.LAC,a.CELL_OSS_NODE_ID ,CELL_STRUCTURE, CASE WHEN CELL_STRUCTURE = ''Closed'' THEN ''1'' ELSE ''0'' END AS INDOOR_TYPE FROM `',GT_DB,'`.`2g_cell_oss_node_id` a LEFT JOIN  `gt_gw_main`.`2g_cell_structure` b ON a.CELL_OSS_NODE_ID = b.NODE_ID WHERE b.CELL_STRUCTURE IS NOT NULL;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;  


 INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'Import_WINA_miss_DATA_GSM','Update nt_antenna_gsm INDOOR_TYPE by 2G_nt_daily_node_id_with_structure', NOW());	
 	SET @SqlCmd=CONCAT('UPDATE ',GT_DB,'.nt_antenna_gsm a, ',GT_DB,'.2G_nt_daily_node_id_with_structure b SET a.indoor_type  = b.indoor_type WHERE a.BSC_ID =b.BSC_ID AND a.CELL_ID = b.CELL_ID AND a.LAC = b.LAC;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;  
	

 INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'Import_WINA_miss_DATA_GSM','Update nt_antenna_gsm INDOOR_TYPE by AZIMUTH and ANTENNA_MODEL', NOW());	
 	SET @SqlCmd=CONCAT('UPDATE ',GT_DB,'.nt_antenna_gsm SET INDOOR_TYPE = 
      CASE  WHEN   ANTENNA_MODEL  LIKE ''%Halle%'' THEN 1
      WHEN   ANTENNA_MODEL  = ''Tunnel''  THEN ''1''
      WHEN   ANTENNA_MODEL  = ''FODAS''  THEN ''1''
      WHEN   ANTENNA_MODEL  <> ''Halle''  THEN ''0'' 
      WHEN   ANTENNA_MODEL  = ''NoName''  THEN INDOOR_TYPE
      ELSE  INDOOR_TYPE END;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;

INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'Import_WINA_miss_DATA_GSM','Update nt_cell_gsm INDOOR_TYPE by nt_antenna_gsm', NOW());	
 	SET @SqlCmd=CONCAT('UPDATE ',GT_DB,'.nt_cell_gsm a, ',GT_DB,'.nt_antenna_gsm b SET a.indoor = b.indoor_type WHERE a.BSC_ID =b.BSC_ID AND a.CELL_ID = b.CELL_ID AND a.LAC = b.LAC;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;  

 






















	SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS 2g_huawei_id_mapped_node_id;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;  
	
	SET @SqlCmd=CONCAT('CREATE TEMPORARY TABLE 2g_huawei_id_mapped_node_id AS SELECT a.BSC_ID, a.CELL_ID,a.LAC,a.CELL_OSS_NODE_ID, b.ID_IN_OMC FROM ',GT_DB,'.2G_nt_daily_node_id_with_structure a LEFT JOIN gt_gw_main.2g_huawei_id b ON b.node_id = a.CELL_OSS_NODE_ID WHERE b.ID_IN_OMC IS NOT NULL;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;  
	
		 SET @SqlCmd=CONCAT('ALTER TABLE 2g_huawei_id_mapped_node_id ADD KEY `id` (`BSC_ID`,`CELL_ID`,`LAC`);');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;  
	
		 SET @SqlCmd=CONCAT('UPDATE ',GT_DB,'.nt_cell_gsm a, 2g_huawei_id_mapped_node_id b SET a.local_cell_index=b.ID_IN_OMC WHERE a.BSC_ID=b.BSC_ID AND a.CELL_ID=b.CELL_ID AND a.LAC=b.LAC;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;  
	

INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'Import_WINA_miss_DATA_GSM','update nt_cell_gsm OPERSTATE_ENABLE=1,ACITVE_STATE=1 (default value) #27540', NOW());
	SET @SqlCmd=CONCAT('UPDATE ',GT_DB,'.nt_cell_gsm a SET a.OPERSTATE_ENABLE=1,a.ACITVE_STATE=1;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
END$$
DELIMITER ;
