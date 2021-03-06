DELIMITER $$
USE `gt_gw_main`$$
DROP PROCEDURE IF EXISTS `SP_CreateDB_LTE`$$
CREATE DEFINER=`covmo`@`%` PROCEDURE `SP_Flush_Database`(IN GT_DB VARCHAR(100))
BEGIN
	
	
	DECLARE tbl_name VARCHAR(100);
	DECLARE no_more_maps INT;
	
	DECLARE csr CURSOR FOR
	SELECT table_name FROM information_schema.TABLES WHERE table_schema = GT_DB;
	
	DECLARE CONTINUE HANDLER FOR NOT FOUND 
	BEGIN
		SET no_more_maps = 1;
		SELECT '{tech:�ALL �, name:�SP-Report�, status:�2�,message_id: �null�, message: �SP_Flush_Database Failed LEAVE dept_loop�, log_path: ��}' AS message;
	END;
	SET no_more_maps = 0;
	
	OPEN csr;
	
	dept_loop:REPEAT
                FETCH csr INTO tbl_name;
                IF no_more_maps = 0 THEN
                        IF no_more_maps THEN
				LEAVE dept_loop;
			END IF;
			SET @SqlCmd=CONCAT('FLUSH TABLE ',GT_DB,'.`',tbl_name,'` ;');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
			
	
                END IF;
        UNTIL no_more_maps
        END REPEAT dept_loop;
 
        CLOSE csr;
        SET no_more_maps=0;
END$$
DELIMITER ;
