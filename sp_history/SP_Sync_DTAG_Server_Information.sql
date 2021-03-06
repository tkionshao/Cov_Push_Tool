DELIMITER $$
USE `gt_gw_main`$$
DROP PROCEDURE IF EXISTS `SP_CreateDB_LTE`$$
CREATE DEFINER=`covmo`@`%` PROCEDURE `SP_Sync_DTAG_Server_Information`(IN V_SQL VARCHAR(4000))
BEGIN
  DECLARE l_GW_USER VARCHAR(32);
  DECLARE l_GW_PASSWORD VARCHAR(32);
  DECLARE l_DB_URI VARCHAR(512);
  DECLARE l_DB_USER VARCHAR(32);
  DECLARE l_DB_PASSWORD VARCHAR(32);
  
  DECLARE S_IP VARCHAR(20);
  DECLARE S_PORT VARCHAR(10);
  DECLARE S_ACCOUNT VARCHAR(30);
  DECLARE S_PWD VARCHAR(30);
    
  DECLARE S_TBL_NAME VARCHAR(100);
  DECLARE DB VARCHAR(50) DEFAULT 'gt_gw_main';
  DECLARE NEW_V_SQL VARCHAR(1000);
  DECLARE i INT DEFAULT 0;
  
  SET @ALLIPPort='';
  SELECT IFNULL(VALUE,'') INTO @ALLIPPort FROM `gt_gw_main`.`integration_param` WHERE gt_name='external_server_information';  
  
  SET @col_cnt=(LENGTH(@ALLIPPort) - LENGTH(REPLACE(@ALLIPPort, '|', '')))+1;
  
  SET i=1;
  
  IF @ALLIPPort<>'' THEN
    WHILE i <= @col_cnt DO
    BEGIN 
      SET S_IP = gt_strtok(gt_strtok(@ALLIPPort, i, '|'),1,':');
      SET S_PORT = gt_strtok(gt_strtok(@ALLIPPort, i, '|'),2,':');
      SET S_TBL_NAME = CONCAT('`external_server_information_',S_IP,'_',S_PORT,'`');
      
      
      IF S_IP='localhost' THEN
        SET i = i + 1;
      ELSE
        SET NEW_V_SQL=REPLACE(V_SQL,'external_server_information',S_TBL_NAME);
        SET @SqlCmd=NEW_V_SQL;
        PREPARE Stmt FROM @SqlCmd;
        EXECUTE Stmt;
        DEALLOCATE PREPARE Stmt;
        
        SET i = i + 1;
      END IF;
    END;
    END WHILE; 
  END IF;
END$$
DELIMITER ;
