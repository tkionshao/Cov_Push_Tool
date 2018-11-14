CREATE DEFINER=`covmo`@`%` PROCEDURE `SP_IMSI_PU_Tbl_Prevent`()
BEGIN	
	DECLARE START_TIME DATETIME DEFAULT SYSDATE();
/*	
	DECLARE EXIT HANDLER FOR 1034
	BEGIN
		SET @SqlCmd=CONCAT('UPDATE gt_global_imsi.tbl_prevent
					SET RUN=''1'' WHERE TABLE_NAME=''table_imsi_pu''
				;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
	
		INSERT INTO gt_gw_main.SP_LOG VALUES('gt_global_imsi','SP_IMSI_PU_Tbl_Prevent','START REPAIR TABLE_IMSI_PU', START_TIME);
		
		SET @SqlCmd=CONCAT('REPAIR TABLE gt_global_imsi.table_imsi_pu;
				');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		
		
		SET @SqlCmd=CONCAT('UPDATE gt_global_imsi.tbl_prevent
					SET RUN=''0'' WHERE TABLE_NAME=''table_imsi_pu''
				;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		
		INSERT INTO gt_gw_main.SP_LOG VALUES('gt_global_imsi','SP_IMSI_PU_Tbl_Prevent',CONCAT('REPAIR TABLE_IMSI_PU Done: ',TIMESTAMPDIFF(SECOND,START_TIME,SYSDATE()),' seconds.'), NOW());
				
	END;
*/	
	SET @SqlCmd=CONCAT('SELECT RUN INTO @TAG FROM gt_global_imsi.tbl_prevent
				WHERE TABLE_NAME=''table_imsi_pu''
				;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	IF @TAG =0 THEN 
	
		SET @SqlCmd=CONCAT('SELECT table_comment INTO @crash FROM information_schema.TABLES WHERE table_schema=''gt_global_imsi'' AND table_name=''table_imsi_pu'';
					');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
	
		IF @crash<>'' THEN
	
			SET @SqlCmd=CONCAT('UPDATE gt_global_imsi.tbl_prevent
							SET RUN=''1'' WHERE TABLE_NAME=''table_imsi_pu''
						;');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
			INSERT INTO gt_gw_main.SP_LOG VALUES('gt_global_imsi','SP_IMSI_PU_Tbl_Prevent','START REPAIR TABLE_IMSI_PU', START_TIME);
			
			SET @SqlCmd=CONCAT('REPAIR TABLE gt_global_imsi.table_imsi_pu;');				
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
			
			
			SET @SqlCmd=CONCAT('UPDATE gt_global_imsi.tbl_prevent
						SET RUN=''0'' WHERE TABLE_NAME=''table_imsi_pu''
					;');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
			INSERT INTO gt_gw_main.SP_LOG VALUES('gt_global_imsi','SP_IMSI_PU_Tbl_Prevent','REPAIR TABLE_IMSI_PU SUCCESSFULLY', START_TIME);
			
		ELSE 
	
			INSERT INTO gt_gw_main.SP_LOG VALUES('gt_global_imsi','SP_IMSI_PU_Tbl_Prevent','TABLE_IMSI_PU IS NORMALLY', NOW());
	END IF;
	
	
	END IF;
	
