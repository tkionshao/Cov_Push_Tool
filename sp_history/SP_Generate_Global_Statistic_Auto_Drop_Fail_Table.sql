DELIMITER $$
USE `gt_global_statistic`$$
DROP PROCEDURE IF EXISTS `SP_CreateDB_LTE`$$
CREATE DEFINER=`covmo`@`%` PROCEDURE `SP_Generate_Global_Statistic_Auto_Drop_Fail_Table`( IN EXISTS_MINUTE SMALLINT(6))
BEGIN
        DECLARE START_TIME DATETIME DEFAULT SYSDATE();        
        
        SET @@session.group_concat_max_len = @@global.max_allowed_packet;
        INSERT INTO gt_gw_main.sp_log VALUES('gt_gw_main','SP_Generate_Global_Statistic_Auto_Drop_Fail_Table','SP_Auto_Drop_Fail_Table Start ', NOW());
        
        SET @SqlCmd=CONCAT('SELECT GROUP_CONCAT(table_name) into @table_str 
                                FROM information_schema.tables 
                                WHERE table_schema=''gt_aggregate_db'' AND (`CREATE_TIME` < DATE_ADD(NOW(), INTERVAL - ',EXISTS_MINUTE,' MINUTE) OR `CREATE_TIME` IS NULL)
                                AND TABLE_NAME <> ''tmp_dim_handset'';');
        PREPARE Stmt FROM @SqlCmd;
        EXECUTE Stmt;
        DEALLOCATE PREPARE Stmt;
        
        IF @table_str IS NOT NULL THEN  
                SET @v_i=1;
                SET @v_R_Max=(CHAR_LENGTH(@table_str) - CHAR_LENGTH(REPLACE(@table_str,',','')))/(CHAR_LENGTH(','))+1;    
                WHILE @v_i <= @v_R_Max DO
                        BEGIN
                                SET @SqlCmd=CONCAT('DROP TABLE IF EXISTS gt_aggregate_db.',SPLIT_STR(@table_str,',',@v_i),';');
                                PREPARE Stmt FROM @SqlCmd;
                                EXECUTE Stmt;
                                DEALLOCATE PREPARE Stmt;          
                                SET @v_i=@v_i+1; 
                        END;
                END WHILE;    
                INSERT INTO gt_gw_main.SP_LOG VALUES('gt_gw_main','SP_Generate_Global_Statistic_Auto_Drop_Fail_Table',CONCAT('',@v_R_Max,' Tables has dropped',TIMESTAMPDIFF(SECOND,START_TIME,SYSDATE()),' seconds.'), NOW());
        ELSE
                INSERT INTO gt_gw_main.SP_LOG VALUES('gt_gw_main','SP_Generate_Global_Statistic_Auto_Drop_Fail_Table',CONCAT('No Tables dropped : ',TIMESTAMPDIFF(SECOND,START_TIME,SYSDATE()),' seconds.'), NOW());
        END IF;
        
        INSERT INTO gt_gw_main.SP_LOG VALUES('gt_gw_main','SP_Generate_Global_Statistic_Auto_Drop_Fail_Table',CONCAT('Done: ',TIMESTAMPDIFF(SECOND,START_TIME,SYSDATE()),' seconds.'), NOW());
END$$
DELIMITER ;
