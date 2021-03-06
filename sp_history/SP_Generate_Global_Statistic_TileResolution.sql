DELIMITER $$
USE `gt_global_statistic`$$
DROP PROCEDURE IF EXISTS `SP_CreateDB_LTE`$$
CREATE DEFINER=`covmo`@`%` PROCEDURE `SP_Generate_Global_Statistic_TileResolution`(IN DB_NAME VARCHAR(100),IN TBL_NAME VARCHAR(100))
BEGIN
	DECLARE START_TIME DATETIME DEFAULT SYSDATE();
	DECLARE tile_count SMALLINT(6) DEFAULT 0;
	DECLARE v_j SMALLINT(6);
	DECLARE REP_NAME VARCHAR(100) DEFAULT 'tile_19';
	DECLARE TARGET_TBL_NAME VARCHAR(100);
	DECLARE ZOOM_TBL_NAME VARCHAR(100);
	
	INSERT INTO gt_gw_main.SP_LOG VALUES(DB_NAME,'SP_Generate_Global_Statistic_TileResolution','Start', START_TIME);
	
	SET @TileResolution = NULL;
	
	SELECT att_value INTO @TileResolution FROM `gt_covmo`.`sys_config` WHERE group_name = 'system' AND att_name = 'TileResolution';
	SET tile_count = gt_covmo_csv_count(@TileResolution,',');
	SET v_j=1;
	WHILE v_j <= tile_count DO
	BEGIN	
		SET @zoomlevel = gt_covmo_csv_get(@TileResolution,v_j);
		SET TARGET_TBL_NAME=CONCAT('tile_',@zoomlevel);
		SET ZOOM_TBL_NAME=REPLACE(TBL_NAME,REP_NAME,TARGET_TBL_NAME);
		SET @CNT=NULL;
		
		SET @SqlCmd=CONCAT('SELECT COUNT(*) INTO @CNT FROM information_schema.`TABLES`
					WHERE TABLE_SCHEMA=''',DB_NAME,''' 
					AND TABLE_NAME=''',ZOOM_TBL_NAME,''';');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		
		IF @CNT=0 THEN 
			SET @SqlCmd=CONCAT('CREATE TABLE ',DB_NAME,'.',ZOOM_TBL_NAME,' LIKE ',DB_NAME,'.',TBL_NAME,';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
		END IF;	
		SET v_j=v_j+1;
	END;
	END WHILE;
	INSERT INTO gt_gw_main.SP_LOG VALUES(DB_NAME,'SP_Generate_Global_Statistic_TileResolution',CONCAT('Done: ',TIMESTAMPDIFF(SECOND,START_TIME,SYSDATE()),' seconds.'), NOW());
	
END$$
DELIMITER ;
