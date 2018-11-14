DELIMITER $$
USE `gt_gw_main`$$
DROP PROCEDURE IF EXISTS `SP_CreateDB_LTE`$$
CREATE DEFINER=`covmo`@`%` PROCEDURE `SP_Check_Data_LTE`(IN GT_DB VARCHAR(100))
BEGIN
	DECLARE CURRENT_NT_DB VARCHAR(50) DEFAULT CONCAT('gt_nt_',gt_strtok(GT_DB,3,'_'));
	DECLARE CHK SMALLINT(5) DEFAULT 0;
	
	SET @SqlCmd=CONCAT('SELECT COUNT(*) INTO @S1
		    FROM (
			SELECT A.serving_cell_id FROM ',GT_DB,'.table_event_lte_1 A,',CURRENT_NT_DB,'.nt_cell_current_lte B
			WHERE A.serving_enodeb_id =B.enodeb_id AND A.serving_cell_id=B.CELL_ID
			AND A.serving_cell_id IS NOT NULL
			LIMIT 1	) AA');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('SELECT COUNT(*) INTO @S2
		    FROM (
			SELECT A.serving_cell_id FROM ',GT_DB,'.table_event_lte_2 A,',CURRENT_NT_DB,'.nt_cell_current_lte B
			WHERE A.serving_enodeb_id =B.enodeb_id AND A.serving_cell_id=B.CELL_ID
			AND A.serving_cell_id IS NOT NULL
			LIMIT 1	) AA');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('SELECT COUNT(*) INTO @S3
		    FROM (
			SELECT A.serving_cell_id FROM ',GT_DB,'.table_event_lte_3 A,',CURRENT_NT_DB,'.nt_cell_current_lte B
			WHERE A.serving_enodeb_id =B.enodeb_id AND A.serving_cell_id=B.CELL_ID
			AND A.serving_cell_id IS NOT NULL
			LIMIT 1	) AA');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('SELECT COUNT(*) INTO @S4
		    FROM (
			SELECT A.serving_cell_id FROM ',GT_DB,'.table_event_lte_4 A,',CURRENT_NT_DB,'.nt_cell_current_lte B
			WHERE A.serving_enodeb_id =B.enodeb_id AND A.serving_cell_id=B.CELL_ID
			AND A.serving_cell_id IS NOT NULL
			LIMIT 1	) AA');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	SET @SqlCmd=CONCAT('SELECT COUNT(*) INTO @S5
		    FROM (
			SELECT A.serving_cell_id FROM ',GT_DB,'.table_event_lte_5 A,',CURRENT_NT_DB,'.nt_cell_current_lte B
			WHERE A.serving_enodeb_id =B.enodeb_id AND A.serving_cell_id=B.CELL_ID
			AND A.serving_cell_id IS NOT NULL
			LIMIT 1	) AA');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	SET @SqlCmd=CONCAT('SELECT COUNT(*) INTO @S6
		    FROM (
			SELECT A.serving_cell_id FROM ',GT_DB,'.table_event_lte_6 A,',CURRENT_NT_DB,'.nt_cell_current_lte B
			WHERE A.serving_enodeb_id =B.enodeb_id AND A.serving_cell_id=B.CELL_ID
			AND A.serving_cell_id IS NOT NULL
			LIMIT 1	) AA');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('SELECT COUNT(*) INTO @S7
		    FROM (
			SELECT A.serving_cell_id FROM ',GT_DB,'.table_event_lte_7 A,',CURRENT_NT_DB,'.nt_cell_current_lte B
			WHERE A.serving_enodeb_id =B.enodeb_id AND A.serving_cell_id=B.CELL_ID
			AND A.serving_cell_id IS NOT NULL
			LIMIT 1	) AA');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('SELECT COUNT(*) INTO @S7
		    FROM (
			SELECT A.serving_cell_id FROM ',GT_DB,'.table_event_lte_8 A,',CURRENT_NT_DB,'.nt_cell_current_lte B
			WHERE A.serving_enodeb_id =B.enodeb_id AND A.serving_cell_id=B.CELL_ID
			AND A.serving_cell_id IS NOT NULL
			LIMIT 1	) AA');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	IF (@S1 + @S2 + @S3 + @S4 + @S5 + @S6 + @S7) > 0 THEN
		SELECT 1;
	ELSE
		SELECT 0;
	END IF;
	
END$$
DELIMITER ;
