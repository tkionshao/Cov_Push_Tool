DELIMITER $$
USE `gt_gw_main`$$
DROP PROCEDURE IF EXISTS `SP_CreateDB_LTE`$$
CREATE DEFINER=`covmo`@`%` PROCEDURE `Import_WINA_miss_DATA_UMTS`(IN GT_DB VARCHAR(100))
BEGIN
	INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'Import_WINA_miss_DATA_UMTS','Update nt_antenna CPICH_POWER #24393#note-46', NOW());	
	SET @SqlCmd=CONCAT('UPDATE ',GT_DB,'.nt_antenna a,gt_gw_main.nt_antenna_cpich_power b,',GT_DB,'.3g_cell_oss_node_id c SET a.CPICH_POWER=b.CPICH_POWER WHERE a.RNC_ID=c.RNC_ID AND a.CELL_ID=c.CELL_ID AND b.cell_oss_node_id=c.cell_oss_node_id;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt; 


INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'Import_WINA_miss_DATA_UMTS','create table 3G_nt_daily_node_id_with_structure for indoor type', NOW());	
 	SET @SqlCmd=CONCAT('DROP TABLE IF EXISTS ',GT_DB,'.3G_nt_daily_node_id_with_structure;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt; 
	
	SET @SqlCmd=CONCAT('CREATE TABLE ',GT_DB,'.3G_nt_daily_node_id_with_structure SELECT RNC_ID,CELL_ID,CELL_STRUCTURE,CASE b.CELL_STRUCTURE WHEN ''Closed'' THEN ''1'' ELSE ''0'' END AS INDOOR_TYPE FROM ',GT_DB,'.3g_cell_oss_node_id a LEFT JOIN gt_gw_main.3g_cell_structure b ON a.CELL_OSS_NODE_ID = b.node_id WHERE b.cell_structure IS NOT NULL;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
 	SET @SqlCmd=CONCAT('ALTER TABLE ',GT_DB,'.3G_nt_daily_node_id_with_structure ADD INDEX ID (RNC_ID, CELL_ID);');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;  
	
INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'Import_WINA_miss_DATA_UMTS','update nt_cell indoor #27198#note-20', NOW());	
 	SET @SqlCmd=CONCAT('UPDATE ',GT_DB,'.nt_cell a, ',GT_DB,'.3G_nt_daily_node_id_with_structure b SET a.indoor = b.indoor_type WHERE a.RNC_ID = b.RNC_ID AND a.CELL_ID = b.CELL_ID;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt; 


 INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'Import_WINA_miss_DATA_UMTS','update nt_antenna CPICH_POWER #27392', NOW());	
	SET @SqlCmd=CONCAT('UPDATE ',GT_DB,'.nt_antenna a,gt_gw_main.nt_antenna_CPICH_POWER_3G b,',GT_DB,'.3g_cell_oss_node_id c SET a.CPICH_POWER=b.CPICH_POWER WHERE a.RNC_ID=c.RNC_ID AND a.CELL_ID=c.CELL_ID AND b.cell_oss_node_id=c.cell_oss_node_id;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	

 INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'Import_WINA_miss_DATA_UMTS','Update NT_ANTENNA.INDOOR_TYPE by external Excel file.', NOW());
	SET @SqlCmd=CONCAT('UPDATE ',GT_DB,'.NT_ANTENNA a ,',GT_DB,'.3g_nt_daily_node_id_with_structure b SET a.INDOOR_TYPE = b.INDOOR_TYPE
	WHERE a.RNC_ID = b.RNC_ID AND a.CELL_ID = b.CELL_ID;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;	
	

 INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'Import_WINA_miss_DATA_UMTS','update nt_antenna INDOOR_TYPE by rule of azimuth and antenna type #27257', NOW());
 	SET @SqlCmd=CONCAT('UPDATE ',GT_DB,'.nt_antenna SET INDOOR_TYPE = 
				  CASE  WHEN   ANTENNA_MODEL  LIKE ''%Halle%'' THEN ''1''
				  WHEN   ANTENNA_MODEL  = ''Tunnel'' THEN 1
				  WHEN   ANTENNA_MODEL  = ''FODAS''  THEN 1
				  WHEN   ANTENNA_MODEL  <> ''Halle''  THEN 0  
				  WHEN   ANTENNA_MODEL  = ''NoName''  THEN INDOOR_TYPE
				  ELSE  INDOOR_TYPE END;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;  










 INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'Import_WINA_miss_DATA_UMTS','Update NT_CELL.INDOOR by NT_ANTENNA.INDOOR_TYPE.', NOW());
	SET @SqlCmd=CONCAT('UPDATE ',GT_DB,'.NT_CELL a, ',GT_DB,'.NT_ANTENNA b SET a.INDOOR = b.INDOOR_TYPE WHERE a.RNC_ID = b.RNC_ID AND a.CELL_ID = b.CELL_ID;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;

END$$
DELIMITER ;
