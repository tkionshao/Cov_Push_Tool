DELIMITER $$
USE `gt_gw_main`$$
DROP PROCEDURE IF EXISTS `SP_CreateDB_LTE`$$
CREATE DEFINER=`covmo`@`%` PROCEDURE `SP_Drop_LinkTable`(IN DB_name VARCHAR(100),IN TBL_name VARCHAR(400))
BEGIN
  DECLARE LinkTable VARCHAR(1000);
  DECLARE i INT DEFAULT 0;
  
  SET @@session.group_concat_max_len = @@global.max_allowed_packet;
  SET @SqlCmd=CONCAT('SELECT GROUP_CONCAT(SUBSTRING_INDEX(table_name,''_'',-2) SEPARATOR ''|'') INTO @ALLIPPort FROM information_schema.TABLES WHERE ENGINE = ''FEDERATED'' AND table_name LIKE ''',TBL_name,'%'' AND table_schema LIKE ''',DB_name,''';');
  PREPARE Stmt FROM @SqlCmd;
  EXECUTE Stmt;
  DEALLOCATE PREPARE Stmt;
  
  SET @col_cnt=(LENGTH(@ALLIPPort) - LENGTH(REPLACE(@ALLIPPort, '|', '')))+1;
  SET i=1;
  WHILE i <= @col_cnt DO
  BEGIN 
    SET LinkTable = CONCAT('',TBL_name,'_',gt_strtok(@ALLIPPort, i, '|'),'');
    
    SELECT LinkTable;
    SET @SqlCmd=CONCAT('DROP TABLE IF EXISTS ',DB_name,'.`',LinkTable,'`;');
    SELECT @SqlCmd;
    PREPARE Stmt FROM @SqlCmd;
    EXECUTE Stmt;
    DEALLOCATE PREPARE Stmt;
    
    SET i = i + 1;
  END;
  END WHILE; 
  
END$$
DELIMITER ;
