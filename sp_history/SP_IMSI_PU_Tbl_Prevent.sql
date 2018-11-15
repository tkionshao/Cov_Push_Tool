DELIMITER $$
USE `gt_gw_main`$$
DROP PROCEDURE IF EXISTS `SP_CreateDB_LTE`$$
CREATE DEFINER=`covmo`@`%` PROCEDURE `SP_IMSI_PU_Tbl_Prevent`()
BEGIN	
	DECLARE START_TIME DATETIME DEFAULT SYSDATE();
	DECLARE IMSI_IMEI_DIFF_FLAG VARCHAR(10) default 'false';
	DECLARE STEP_START_TIME DATETIME DEFAULT SYSDATE();

	SELECT LOWER(`value`) INTO IMSI_IMEI_DIFF_FLAG  FROM gt_gw_main.integration_param WHERE gt_group = 'sp' AND gt_name = 'imsi_imei_diff' ;
	IF IMSI_IMEI_DIFF_FLAG = 'true' THEN
		INSERT INTO gt_gw_main.SP_LOG VALUES('gt_global_imsi','SP_IMSI_PU_Tbl_Prevent','Check dim_imsi_imei start', START_TIME);
		SET STEP_START_TIME := SYSDATE();

		SET @SqlCmd=CONCAT('SELECT RUN INTO @TAG_IMEI FROM gt_global_imsi.tbl_prevent WHERE TABLE_NAME=''dim_imsi_imei'';');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		
		IF @TAG_IMEI =0 THEN 
			SET @SqlCmd=CONCAT('SELECT table_comment INTO @crash FROM information_schema.TABLES WHERE table_schema=''gt_global_imsi'' AND table_name=''dim_imsi_imei'';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
			
			IF @crash<>'' THEN
				SET @SqlCmd=CONCAT('UPDATE gt_global_imsi.tbl_prevent SET RUN=''1'' WHERE TABLE_NAME=''dim_imsi_imei'';');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				
				INSERT INTO gt_gw_main.SP_LOG VALUES('gt_global_imsi','SP_IMSI_PU_Tbl_Prevent',CONCAT('UPDATE gt_global_imsi.tbl_prevent 0 cost:',TIMESTAMPDIFF(SECOND,STEP_START_TIME,SYSDATE()),' sec.'), NOW());
				SET STEP_START_TIME := SYSDATE();
				
				SET @SqlCmd=CONCAT('REPAIR TABLE gt_global_imsi.dim_imsi_imei;');	
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				
				INSERT INTO gt_gw_main.SP_LOG VALUES('gt_global_imsi','SP_IMSI_PU_Tbl_Prevent',CONCAT('To repair dim_imsi_imei cost :',TIMESTAMPDIFF(SECOND,STEP_START_TIME,SYSDATE()),' sec.'), NOW());
				SET STEP_START_TIME := SYSDATE();
			
				SET @SqlCmd=CONCAT('UPDATE gt_global_imsi.tbl_prevent SET RUN=''0'' WHERE TABLE_NAME=''dim_imsi_imei'';');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;	
				INSERT INTO gt_gw_main.SP_LOG VALUES('gt_global_imsi','SP_IMSI_PU_Tbl_Prevent',CONCAT('Repair dim_imsi_imei SUCCESSFULLY. Done cost:',TIMESTAMPDIFF(SECOND,START_TIME,SYSDATE()),' sec.'), NOW());
			ELSE 
				INSERT INTO gt_gw_main.SP_LOG VALUES('gt_global_imsi','SP_IMSI_PU_Tbl_Prevent',CONCAT('dim_imsi_imei IS NORMALLY. Done cost:',TIMESTAMPDIFF(SECOND,START_TIME,SYSDATE()),' sec.'), NOW());
			END IF;
		END IF;
	ELSE
		SET @SqlCmd=CONCAT('SELECT RUN INTO @TAG FROM gt_global_imsi.tbl_prevent WHERE TABLE_NAME=''table_imsi_pu'';');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		
		IF @TAG =0 THEN 
			SET @SqlCmd=CONCAT('SELECT table_comment INTO @crash FROM information_schema.TABLES WHERE table_schema=''gt_global_imsi'' AND table_name=''table_imsi_pu'';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
		
			IF @crash<>'' THEN
				SET @SqlCmd=CONCAT('UPDATE gt_global_imsi.tbl_prevent SET RUN=''1'' WHERE TABLE_NAME=''table_imsi_pu'';');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				
				INSERT INTO gt_gw_main.SP_LOG VALUES('gt_global_imsi','SP_IMSI_PU_Tbl_Prevent',CONCAT('UPDATE gt_global_imsi.tbl_prevent 1 cost:',TIMESTAMPDIFF(SECOND,STEP_START_TIME,SYSDATE()),' sec.'), NOW());
				SET STEP_START_TIME := SYSDATE();
				
				SET @SqlCmd=CONCAT('REPAIR TABLE gt_global_imsi.table_imsi_pu;');				
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				
				INSERT INTO gt_gw_main.SP_LOG VALUES('gt_global_imsi','SP_IMSI_PU_Tbl_Prevent',CONCAT('To repair table_imsi_pu cost :',TIMESTAMPDIFF(SECOND,STEP_START_TIME,SYSDATE()),' sec.'), NOW());
							
				SET @SqlCmd=CONCAT('UPDATE gt_global_imsi.tbl_prevent SET RUN=''0'' WHERE TABLE_NAME=''table_imsi_pu'';');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				INSERT INTO gt_gw_main.SP_LOG VALUES('gt_global_imsi','SP_IMSI_PU_Tbl_Prevent',CONCAT('Repair table_imsi_pu SUCCESSFULLY. Done cost:',TIMESTAMPDIFF(SECOND,START_TIME,SYSDATE()),' sec.'), NOW());
			ELSE 
				INSERT INTO gt_gw_main.SP_LOG VALUES('gt_global_imsi','SP_IMSI_PU_Tbl_Prevent',CONCAT('table_imsi_pu IS NORMALLY. Done cost:',TIMESTAMPDIFF(SECOND,START_TIME,SYSDATE()),' sec.'), NOW());
			END IF;
		END IF;
	END IF;
END$$
DELIMITER ;
