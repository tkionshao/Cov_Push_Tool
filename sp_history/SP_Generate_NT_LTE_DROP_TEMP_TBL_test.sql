DELIMITER $$
USE `gt_gw_main`$$
DROP PROCEDURE IF EXISTS `SP_CreateDB_LTE`$$
CREATE DEFINER=`covmo`@`%` PROCEDURE `SP_Generate_NT_LTE_DROP_TEMP_TBL_test`(IN GT_DB VARCHAR(100),IN GT_COVMO VARCHAR(100))
BEGIN

  INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'SP_Generate_NT_LTE_DROP_TEMP_TBL','Drop Temp tables LTE', NOW());
 
--   SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS ',GT_DB,'.`tmp_nt_cell_lte_',WORKER_ID,'`;');
--   PREPARE Stmt FROM @SqlCmd;
--   EXECUTE Stmt;
--   DEALLOCATE PREPARE Stmt; 

  INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'SP_Generate_NT_LTE_DROP_TEMP_TBL','Drop table tmp_nt_current_nbr_distance_LTE_fixDataType', NOW());
  SET @SqlCmd=CONCAT('Drop TABLE IF EXISTS ',GT_DB,'.tmp_nt_current_nbr_distance_LTE_fixDataType;');
  PREPARE Stmt FROM @SqlCmd;
  EXECUTE Stmt;
  DEALLOCATE PREPARE Stmt;
  
  INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'SP_Generate_NT_LTE_DROP_TEMP_TBL','Drop table nt_antenna_lte', NOW());
  SET @SqlCmd=CONCAT('DROP TABLE IF EXISTS ',GT_DB,'.nt_antenna_lte;');
  PREPARE Stmt FROM @SqlCmd;
  EXECUTE Stmt;
  DEALLOCATE PREPARE Stmt;
  
  INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'SP_Generate_NT_LTE_DROP_TEMP_TBL','Drop table nt_cell_lte', NOW());
  SET @SqlCmd=CONCAT('DROP TABLE IF EXISTS ',GT_DB,'.nt_cell_lte;');
  PREPARE Stmt FROM @SqlCmd;
  EXECUTE Stmt;
  DEALLOCATE PREPARE Stmt;
  
  INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'SP_Generate_NT_LTE_DROP_TEMP_TBL','Drop table tmp_nt_cell_u', NOW());
  SET @SqlCmd=CONCAT('DROP TABLE IF EXISTS ',GT_DB,'.tmp_nt_cell_u;');
  PREPARE Stmt FROM @SqlCmd;
  EXECUTE Stmt;
  DEALLOCATE PREPARE Stmt;
  
  INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'SP_Generate_NT_LTE_DROP_TEMP_TBL','Drop table nt_mme_lte', NOW());
  SET @SqlCmd=CONCAT('DROP TABLE IF EXISTS ',GT_DB,'.nt_mme_lte;');
  PREPARE Stmt FROM @SqlCmd;
  EXECUTE Stmt;
  DEALLOCATE PREPARE Stmt;
  
  INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'SP_Generate_NT_LTE_DROP_TEMP_TBL','Drop table nt_nbr_4_2_lte', NOW());
  SET @SqlCmd=CONCAT(' DROP TABLE IF EXISTS ',GT_DB,'.nt_nbr_4_2_lte;');
  PREPARE Stmt FROM @SqlCmd;
  EXECUTE Stmt;
  DEALLOCATE PREPARE Stmt;
  
  INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'SP_Generate_NT_LTE_DROP_TEMP_TBL','Drop table nt_nbr_4_3_lte', NOW());
  SET @SqlCmd=CONCAT(' DROP TABLE IF EXISTS ',GT_DB,'.nt_nbr_4_3_lte;');
  PREPARE Stmt FROM @SqlCmd;
  EXECUTE Stmt;
  DEALLOCATE PREPARE Stmt;
  
  INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'SP_Generate_NT_LTE_DROP_TEMP_TBL','Drop table nt_nbr_4_4_lte', NOW());
  SET @SqlCmd=CONCAT(' DROP TABLE IF EXISTS ',GT_DB,'.nt_nbr_4_4_lte;');
  PREPARE Stmt FROM @SqlCmd;
  EXECUTE Stmt;
  DEALLOCATE PREPARE Stmt;
  
  INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'SP_Generate_NT_LTE_DROP_TEMP_TBL','Drop table nt_tac_cell_lte', NOW());
  SET @SqlCmd=CONCAT(' DROP TABLE IF EXISTS ',GT_DB,'.nt_tac_cell_lte;');
  PREPARE Stmt FROM @SqlCmd;
  EXECUTE Stmt;
  DEALLOCATE PREPARE Stmt;
  
  INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'SP_Generate_NT_LTE_DROP_TEMP_TBL','Drop table tmp_avg_voronoi_distance', NOW());
  SET @SqlCmd=CONCAT('DROP TABLE IF EXISTS ',GT_DB,'.tmp_avg_voronoi_distance;');
  PREPARE Stmt FROM @SqlCmd;
  EXECUTE Stmt;
  DEALLOCATE PREPARE Stmt;
  
  INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'SP_Generate_NT_LTE_DROP_TEMP_TBL','Drop table tmp_dtag_nt_antenna_frg_mapping_lte', NOW());
  SET @SqlCmd=CONCAT('DROP TABLE IF EXISTS ',GT_DB,'.tmp_dtag_nt_antenna_frg_mapping_lte;');
  PREPARE Stmt FROM @SqlCmd;
  EXECUTE Stmt;
  DEALLOCATE PREPARE Stmt;
  
  INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'SP_Generate_NT_LTE_DROP_TEMP_TBL','Drop table tmp_nt_antenna_default', NOW());
  SET @SqlCmd=CONCAT('DROP TABLE IF EXISTS ',GT_DB,'.tmp_nt_antenna_default;');
  PREPARE Stmt FROM @SqlCmd;
  EXECUTE Stmt;
  DEALLOCATE PREPARE Stmt;
  
  INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'SP_Generate_NT_LTE_DROP_TEMP_TBL','Drop table tmp_nt_nbr_cell_blacklist_pci', NOW());
  SET @SqlCmd=CONCAT('DROP TABLE IF EXISTS ',GT_DB,'.tmp_nt_nbr_cell_blacklist_pci ;');
  PREPARE Stmt FROM @SqlCmd;
  EXECUTE Stmt;
  DEALLOCATE PREPARE Stmt; 

  INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'SP_Generate_NT_LTE_DROP_TEMP_TBL','Drop Temp tables LTE Done!', NOW());


END$$
DELIMITER ;
