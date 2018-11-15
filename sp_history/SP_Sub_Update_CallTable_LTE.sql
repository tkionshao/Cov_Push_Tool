DELIMITER $$
USE `gt_gw_main`$$
DROP PROCEDURE IF EXISTS `SP_CreateDB_LTE`$$
CREATE DEFINER=`covmo`@`%` PROCEDURE `SP_Sub_Update_CallTable_LTE`(IN GT_DB VARCHAR(100),IN GT_COVMO VARCHAR(100))
BEGIN
	DECLARE START_TIME DATETIME DEFAULT SYSDATE();
	DECLARE HUA_EXPORT_FLAG VARCHAR(10);
	DECLARE SINGTEL_EXPORT_FLAG VARCHAR(10);
	SELECT LOWER(`value`) INTO HUA_EXPORT_FLAG  FROM gt_gw_main.integration_param WHERE gt_group = 'sp' AND gt_name = 'hua_export' ;
	SELECT LOWER(`value`) INTO SINGTEL_EXPORT_FLAG  FROM gt_gw_main.integration_param WHERE gt_group = 'sp' AND gt_name = 'singtel_export' ;
	
	INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'SP_Sub_Update_CallTable_LTE','Start', NOW());
	
	SET @str = 'table_call_lte_update';
	CALL gt_gw_main.SP_Create_MergeTable_LTE(GT_DB,CONCAT('',@str,''));
	SET @str = 'table_call_lte';
	CALL gt_gw_main.SP_Create_MergeTable_LTE(GT_DB,CONCAT('',@str,''));
	SET @str = 'table_position_convert_ho_lte';
	CALL gt_gw_main.SP_Create_MergeTable_LTE(GT_DB,CONCAT('',@str,''));
	SET @str = 'table_position_convert_mr_lte';
	CALL gt_gw_main.SP_Create_MergeTable_LTE(GT_DB,CONCAT('',@str,''));
	SET @str = 'table_position_convert_serving_lte';
	CALL gt_gw_main.SP_Create_MergeTable_LTE(GT_DB,CONCAT('',@str,''));
	SET @str = 'table_cell_lte';
	CALL gt_gw_main.SP_Create_MergeTable_LTE(GT_DB,CONCAT('',@str,''));
	SET @str = 'table_erab_volte_lte';
	CALL gt_gw_main.SP_Create_MergeTable_LTE(GT_DB,CONCAT('',@str,''));
	
	SET @SqlCmd=CONCAT('CREATE TABLE ',GT_DB,'.table_call_nopos_lte LIKE ',GT_DB,'.table_call_nopos_lte_1;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @str = 'table_call_nopos_lte';
	CALL gt_gw_main.SP_Create_MergeTable_LTE(GT_DB,CONCAT('',@str,''));	
	
	SET @SqlCmd=CONCAT('UPDATE ',GT_DB,'.table_call_lte_update A,',GT_DB,'.table_lte_imsi_imei_collection B #for TWN IMSI/IMEI mapping
			    SET A.IMEI=B.IMEI,
				A.IMSI=B.IMSI
			    WHERE A.INITIAL_MME_UE_S1AP_ID=B.INITIAL_MME_UE_S1AP_ID
				AND A.INITIAL_ENB_UE_S1AP_ID=B.INITIAL_ENB_UE_S1AP_ID
			    ;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	IF HUA_EXPORT_FLAG = 'true' THEN
		CALL gt_gw_main.`SP_Sub_Table_Call_Export`(GT_DB,4,GT_COVMO);
	END IF;
	
	IF SINGTEL_EXPORT_FLAG = 'true' THEN
		CALL gt_gw_main.`SP_NDC_IMSI_export`(GT_DB,'//data//Mysql_Export//',4,GT_COVMO);
	END IF;
	
	INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'SP_Sub_Update_CallTable_LTE',CONCAT('Done: ',TIMESTAMPDIFF(SECOND,START_TIME,SYSDATE()),' seconds.'), NOW());
	
END$$
DELIMITER ;
