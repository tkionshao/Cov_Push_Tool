DELIMITER $$
USE `gt_global_statistic`$$
DROP PROCEDURE IF EXISTS `SP_CreateDB_LTE`$$
CREATE DEFINER=`covmo`@`%` PROCEDURE `SP_Generate_Global_Statistic_DS_Purge_Data`(IN TECH_MASK TINYINT(2),IN exDate VARCHAR(10),IN KIND TINYINT(2))
BEGIN
	
	DECLARE START_TIME DATETIME DEFAULT SYSDATE();
	DECLARE STEP_START_TIME DATETIME DEFAULT SYSDATE();
	DECLARE v_DATA_DATE VARCHAR(10) DEFAULT NULL;
			
	SET v_DATA_DATE:=DATE_FORMAT(exDate,'%Y%m%d');
	
	SET @global_db='gt_global_statistic';
	INSERT INTO gt_gw_main.SP_LOG VALUES('gt_global_statistic','SP_Generate_Global_Statistic_DS_Purge_Data','START', START_TIME);
	SET STEP_START_TIME := SYSDATE();
	SET @SqlCmd=CONCAT('SELECT GROUP_CONCAT(tbl_name SEPARATOR ''|'' ) INTO @PRG_STR 
				FROM ',@global_db,'.`table_created_history`
				WHERE `START_DATE`<''',exDate,'''
				AND `TECH_MASK`=0
				AND `KIND`=',KIND,'
				;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @v_i=1;
	SET @v_R_Max=gt_covmo_csv_count(@PRG_STR,'|');
	WHILE @v_i <= @v_R_Max DO
	BEGIN
		SET @table_name:=gt_strtok(@PRG_STR, @v_i, '|');
		SET @SqlCmd=CONCAT('DROP TABLE IF EXISTS ',@global_db,'.',@table_name,';');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		SET @v_i=@v_i+1; 
	END;
	END WHILE;
	IF @v_R_Max>0 THEN 
		SET @SqlCmd=CONCAT('DELETE FROM ',@global_db,'.`table_created_history`
					WHERE `START_DATE`<''',exDate,'''
					AND `TECH_MASK`=0
					AND `KIND`=',KIND,'
					;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		
		SET @SqlCmd=CONCAT('DELETE FROM ',@global_db,'.`ds_information`
					WHERE `DATA_DATE`<''',exDate,'''
					;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
	END IF;
	INSERT INTO gt_gw_main.SP_LOG VALUES('gt_global_statistic','SP_Generate_Global_Statistic_DS_Purge_Data',CONCAT(v_DATA_DATE,',',TECH_MASK,',',KIND,' Done: ',TIMESTAMPDIFF(SECOND,START_TIME,SYSDATE()),' seconds.'), NOW());
END$$
DELIMITER ;
