DELIMITER $$
USE `gt_gw_main`$$
DROP PROCEDURE IF EXISTS `SP_CreateDB_LTE`$$
CREATE DEFINER=`covmo`@`%` PROCEDURE `xx_SP_Check_Data`(IN GT_DB VARCHAR(100))
BEGIN
	DECLARE CURRENT_NT_DB VARCHAR(50) DEFAULT CONCAT('gt_nt_',gt_strtok(GT_DB,3,'_'));
	DECLARE RNC_ID VARCHAR(50) DEFAULT gt_strtok(GT_DB, 2, "_");
	
	set @check_1 = null;
	SET @check_2 = NULL;
	
	SET @SqlCmd=CONCAT('SELECT COUNT(*) into @check_1
			    FROM (
				SELECT A.C_ID FROM ',GT_DB,'.table_nbap_rlsr A,',CURRENT_NT_DB,'.nt_current B
				WHERE A.C_ID=B.CELL_ID AND A.RNC_ID = B.RNC_ID
				AND A.C_ID IS NOT NULL
				LIMIT 1	) AA');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt; 
	
	SET @SqlCmd=CONCAT('SELECT COUNT(*) into @check_2
			    FROM (
					SELECT RNC_ID
					FROM ',CURRENT_NT_DB,'.nt_current
					WHERE RNC_ID = ',RNC_ID,' LIMIT 1
				LIMIT 1	) AA');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt; 
	select @check_1 * @check_2;
	
END$$
DELIMITER ;
