DELIMITER $$
USE `gt_gw_main`$$
DROP PROCEDURE IF EXISTS `SP_CreateDB_LTE`$$
CREATE DEFINER=`covmo`@`%` PROCEDURE `SP_Create_MergeTable_LTE`(IN GT_DB VARCHAR(100),IN MRG_TBL VARCHAR(100))
BEGIN
	DECLARE START_TIME DATETIME DEFAULT SYSDATE();
	DECLARE no_more_maps INT;
	DECLARE T_NAME VARCHAR(100);
	DECLARE UNION_STR VARCHAR(6000);
	
	DECLARE csr CURSOR FOR
	SELECT table_name FROM information_schema.TABLES 
	WHERE table_schema = GT_DB AND table_name LIKE CONCAT('',@str,'__') AND table_name != @str AND table_name != 'rpt_cell_tile_position_dy';
	
	DECLARE CONTINUE HANDLER FOR NOT FOUND 
	BEGIN
		SET no_more_maps = 1;
		SELECT '{tech:”LTE”, name:”SP-Report”, status:”2”,message_id: “null”, message: “SP_Create_MergeTable_LTE Failed LEAVE dept_loop”, log_path: “”}' AS message;
	END;
	SET no_more_maps = 0;
	OPEN csr;
	dept_loop:REPEAT
                FETCH csr INTO T_NAME;
                IF no_more_maps = 0 THEN
                        IF no_more_maps THEN
				LEAVE dept_loop;
			END IF;
	
			IF UNION_STR IS NULL THEN
	
				SET @SqlCmd=CONCAT('DROP TABLE IF EXISTS ',GT_DB,'.',MRG_TBL,';');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				SET @SqlCmd=CONCAT('CREATE TABLE ',GT_DB,'.',MRG_TBL,' LIKE ',GT_DB,'.',T_NAME,';');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
	
				SET UNION_STR = CONCAT(GT_DB,'.',T_NAME,'');
			ELSE
				SET UNION_STR = CONCAT(UNION_STR,' , ',GT_DB,'.',T_NAME,'');
			END IF;	
                END IF;
        UNTIL no_more_maps
        END REPEAT dept_loop;
	CLOSE csr;
	
	SET @SqlCmd=CONCAT('ALTER TABLE ',GT_DB,'.',MRG_TBL,' ENGINE = MRG_MYISAM UNION=(',UNION_STR,');');
	PREPARE Stmt FROM @SqlCmd;
  	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
END$$
DELIMITER ;
