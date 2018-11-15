DELIMITER $$
USE `gt_gw_main`$$
DROP PROCEDURE IF EXISTS `SP_CreateDB_LTE`$$
CREATE DEFINER=`covmo`@`%` PROCEDURE `SP_Copy_Database`(IN FROM_GT_DB VARCHAR(100), IN TO_GT_DB VARCHAR(100))
BEGIN
	DECLARE tbl_name VARCHAR(64);
	DECLARE chk INT DEFAULT 0;
	DECLARE done INT DEFAULT 0;
	DECLARE cur CURSOR FOR SELECT table_name FROM information_schema.TABLES WHERE table_schema = FROM_GT_DB AND table_name NOT LIKE 'table_imsi_diff_%';
	DECLARE CONTINUE HANDLER FOR NOT FOUND 
	BEGIN
		SET done = 1;
		SELECT '{tech:”UMTS”, name:”SP-Report”, status:”2”,message_id: “null”, message: “SP_Copy_Database Failed LEAVE read_loop;”, log_path: “”}' AS message;
	END;
		
	SELECT INSTR(TO_GT_DB, 'tmp') INTO chk;
	IF chk > 0 THEN
		SET @SqlCmd =CONCAT('DROP DATABASE IF EXISTS ', TO_GT_DB);
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
	END IF;
	
	SET @SqlCmd =CONCAT('CREATE DATABASE ', TO_GT_DB);
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	OPEN cur;
	read_loop: LOOP
    	FETCH cur INTO tbl_name;
		IF done THEN
			LEAVE read_loop;
		END IF;
	
		SET @SqlCmd=CONCAT('SELECT COUNT(*) INTO @CNT FROM information_schema.TABLES WHERE table_schema = ''',FROM_GT_DB,''' AND table_name = ''',tbl_name,''';');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt; 
	
		IF @CNT = 0 THEN
			SELECT SLEEP(2);
		END IF;
	
		SET @SqlCmd=CONCAT('CREATE TABLE ',TO_GT_DB,'.',tbl_name,' LIKE ',FROM_GT_DB,'.',tbl_name,';');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt; 	
	END LOOP;
	CLOSE cur;
END$$
DELIMITER ;
