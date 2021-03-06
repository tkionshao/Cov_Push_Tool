DELIMITER $$
USE `gt_gw_main`$$
DROP PROCEDURE IF EXISTS `SP_CreateDB_LTE`$$
CREATE DEFINER=`covmo`@`%` PROCEDURE `SP_Generate_PU_LTE_Oman`(IN GT_DB VARCHAR(100),IN GT_COVMO VARCHAR(100))
BEGIN
	DECLARE O_GT_DB VARCHAR(100) DEFAULT GT_DB;
	DECLARE START_TIME DATETIME DEFAULT SYSDATE();
	DECLARE WORKER_ID VARCHAR(10) DEFAULT CONNECTION_ID();
	
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
END$$
DELIMITER ;
