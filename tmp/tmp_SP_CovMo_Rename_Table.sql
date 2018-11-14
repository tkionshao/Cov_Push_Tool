CREATE DEFINER=`covmo`@`%` PROCEDURE `SP_CovMo_Rename_Table`(IN org_db VARCHAR(100),IN org_table VARCHAR(100),IN new_table VARCHAR(100))
BEGIN
	SELECT COUNT(table_name) INTO @CNT FROM information_schema.TABLES WHERE table_schema=org_db AND table_name=org_table;
	IF @CNT>0 THEN   
		SET @SqlCmd =CONCAT('RENAME TABLE ',org_db,'.',org_table,' TO ',org_db,'.',new_table,';');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
	END IF;
	INSERT INTO gt_gw_main.sp_log VALUES(org_db,'SP_CovMo_Rename_Table',@SqlCmd, NOW());
