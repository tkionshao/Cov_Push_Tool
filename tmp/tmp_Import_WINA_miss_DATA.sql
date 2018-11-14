CREATE DEFINER=`covmo`@`%` PROCEDURE `Import_WINA_miss_DATA`(IN GT_DB VARCHAR(100))
BEGIN
	INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'Import_WINA_miss_DATA','Update nt_antenna CPICH_POWER #24393#note-46', NOW());	
	SET @SqlCmd=CONCAT('UPDATE ',GT_DB,'.nt_antenna a,gt_gw_main.nt_antenna_cpich_power b,',GT_DB,'.3g_cell_oss_node_id c SET a.CPICH_POWER=b.CPICH_POWER WHERE a.RNC_ID=c.RNC_ID AND a.CELL_ID=c.CELL_ID AND b.cell_oss_node_id=c.cell_oss_node_id;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt; 
	INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'Import_WINA_miss_DATA','Update nt_antenna_gsm BCCH_POWER #24393#note-46', NOW());	
	SET @SqlCmd=CONCAT('UPDATE ',GT_DB,'.nt_antenna_gsm a,gt_gw_main.nt_antenna_gsm_bcch_power b,',GT_DB,'.2g_cell_oss_node_id c SET a.BCCH_POWER=b.BCCH_POWER WHERE a.BSC_ID=c.BSC_ID AND a.CELL_ID=c.CELL_ID AND a.LAC=c.LAC AND b.cell_oss_node_id=c.cell_oss_node_id;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt; 
   ##27198#note-20
 # nt_cell_gsm local_cell_index 
 INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'Import_WINA_miss_DATA','update nt_cell_gsm local_cell_index #27198#note-20', NOW());
	SET @SqlCmd=CONCAT('CREATE TABLE ',GT_DB,'.2g_cell_oss_node_id_haveBSC_cell AS SELECT b.BSC_ID,b.CELL_ID,b.LAC,a.cell_oss_node_id FROM ',GT_DB,'.2g_cell_oss_node_id a INNER JOIN ',GT_DB,'.nt_cell_gsm b ON b.BSC_ID=a.BSC_ID AND b.CELL_ID=a.CELL_ID AND b.LAC=a.LAC;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;  
	
	SET @SqlCmd=CONCAT('ALTER TABLE ',GT_DB,'.2g_cell_oss_node_id_haveBSC_cell ADD INDEX ID (BSC_ID, CELL_ID, LAC);');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt; 
	

	
	SET @SqlCmd=CONCAT('UPDATE ',GT_DB,'.nt_cell_gsm a,gt_gw_main.2G_Huawei_ID b,',GT_DB,'.2g_cell_oss_node_id_haveBSC_cell c SET a.local_cell_index=b.ID_IN_OMC 
			WHERE a.BSC_ID=c.BSC_ID AND a.CELL_ID=c.CELL_ID AND a.LAC= c.LAC AND c.cell_oss_node_id=b.node_id;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;  
#nt_cell_gsm indoor
 INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'Import_WINA_miss_DATA','update nt_cell_gsm indoor #27198#note-20', NOW());
	SET @SqlCmd=CONCAT('UPDATE ',GT_DB,'.nt_cell_gsm a,gt_gw_main.2G_cell_structure b,',GT_DB,'.2g_cell_oss_node_id_haveBSC_cell c SET a.indoor=CASE b.CELL_STRUCTURE WHEN ''Closed'' THEN ''1'' ELSE ''0'' END WHERE a.BSC_ID=c.BSC_ID AND a.CELL_ID=c.CELL_ID AND a.LAC= c.LAC AND c.cell_oss_node_id=b.node_id;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;  
#nt_cell indoor	
INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'Import_WINA_miss_DATA','create temp table 3G_nt_daily_node_id_with_structure for indoor type', NOW());	
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
	
INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'Import_WINA_miss_DATA','update nt_cell indoor #27198#note-20', NOW());	
 	SET @SqlCmd=CONCAT('UPDATE ',GT_DB,'.nt_cell a, ',GT_DB,'.3G_nt_daily_node_id_with_structure b SET a.indoor = b.indoor_type WHERE a.RNC_ID = b.RNC_ID AND a.CELL_ID = b.CELL_ID;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt; 
 
## nt_cell_gsm BA_list #27392
 INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'Import_WINA_miss_DATA','update nt_cell_gsm BA_list #27392', NOW());	
	SET @SqlCmd=CONCAT('UPDATE ',GT_DB,'.nt_cell_gsm a,gt_gw_main.BA_list_final b,',GT_DB,'.2g_cell_oss_node_id c
		SET a.ba_list=b.ba_list WHERE a.bsc_id=c.bsc_id AND a.cell_id=c.cell_id AND a.LOCAL_CELL_INDEX=b.cell_id AND b.cell_oss_node_id=c.cell_oss_node_id
		;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt; 
## 27392 nt_antenna CPICH_POWER	
 INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'Import_WINA_miss_DATA','update nt_antenna CPICH_POWER #27392', NOW());	
	SET @SqlCmd=CONCAT('UPDATE ',GT_DB,'.nt_antenna a,gt_gw_main.nt_antenna_CPICH_POWER_3G b,',GT_DB,'.3g_cell_oss_node_id c SET a.CPICH_POWER=b.CPICH_POWER WHERE a.RNC_ID=c.RNC_ID AND a.CELL_ID=c.CELL_ID AND b.cell_oss_node_id=c.cell_oss_node_id;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
--  27193 nt_antenna INDOOR_TYPE
 INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'Import_WINA_miss_DATA','update nt_antenna INDOOR_TYPE by rule of azimuth and antenna type #27257', NOW());
 	SET @SqlCmd=CONCAT('UPDATE ',GT_DB,'.nt_antenna SET INDOOR_TYPE = 
				  CASE  WHEN   ANTENNA_MODEL  = ''Halle''  AND AZIMUTH = ''0''  THEN ''1''
				  WHEN   ANTENNA_MODEL  = ''Tunnel'' THEN 1
				  WHEN   ANTENNA_MODEL  = ''FODAS''  THEN 1
				  WHEN   ANTENNA_MODEL  <> ''Halle''  THEN 0  
				  WHEN   ANTENNA_MODEL  = ''NoName''  THEN INDOOR_TYPE
				  ELSE  INDOOR_TYPE END;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;  

 INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'Import_WINA_miss_DATA','update nt_antenna INDOOR_TYPE same as nt_cell_gsm INDOOR #27193', NOW());
	SET @SqlCmd=CONCAT('UPDATE ',GT_DB,'.nt_antenna a ,',GT_DB,'.3g_nt_daily_node_id_with_structure b SET a.indoor_type = b.indoor_type
	WHERE a.rnc_id = b.rnc_id AND a.cell_id = b.cell_id;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;

##27392 nt_antenna_gsm bcch_power	
 INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'Import_WINA_miss_DATA','update nt_antenna_gsm bcch_power #27392', NOW());	
	SET @SqlCmd=CONCAT('UPDATE ',GT_DB,'.nt_antenna_gsm a,gt_gw_main.nt_antenna_gsm_bcch_power_2G b,',GT_DB,'.2g_cell_oss_node_id c SET a.BCCH_POWER=b.BCCH_POWER WHERE a.BSC_ID=c.BSC_ID AND a.CELL_ID=c.CELL_ID AND a.LAC=c.LAC AND b.cell_oss_node_id=c.cell_oss_node_id;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
##27543 nt_antenna_gsm INDOOR_TYPE 
 INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'Import_WINA_miss_DATA','update nt_antenna_gsm INDOOR_TYPE same as nt_cell_gsm INDOOR #27543', NOW());
	SET @SqlCmd=CONCAT('UPDATE ',GT_DB,'.nt_antenna_gsm a,',GT_DB,'.nt_cell_gsm b SET a.INDOOR_TYPE=b.INDOOR WHERE a.BSC_ID=b.BSC_ID AND a.CELL_ID=b.CELL_ID AND a.LAC= b.LAC;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
##27540 default value issue (because load data into table by jar)
INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'Import_WINA_miss_DATA','update nt_cell_gsm OPERSTATE_ENABLE=1,ACITVE_STATE=1 (default value) #27540', NOW());
	SET @SqlCmd=CONCAT('UPDATE ',GT_DB,'.nt_cell_gsm a SET a.OPERSTATE_ENABLE=1,a.ACITVE_STATE=1;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
 
