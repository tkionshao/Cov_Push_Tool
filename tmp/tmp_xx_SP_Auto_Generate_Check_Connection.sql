CREATE DEFINER=`covmo`@`%` PROCEDURE `xx_SP_Auto_Generate_Check_Connection`( IN TBL VARCHAR(100))
BEGIN
	DECLARE DB VARCHAR(50) DEFAULT 'gt_gw_main';
	SET @SqlCmd=CONCAT(' SELECT * FROM ',DB,'.`',TBL,'` LIMIT 1;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt; 
	
