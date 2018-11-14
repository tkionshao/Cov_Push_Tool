DELIMITER $$
USE `gt_gw_main`$$
DROP PROCEDURE IF EXISTS `SP_CreateDB_LTE`$$
CREATE DEFINER=`covmo`@`%` PROCEDURE `SP_rerun_nation`(IN exDate VARCHAR(10),IN FLAG VARCHAR(10))
BEGIN
SET @data_date_v = exDate;
SET @drop_date_v = DATE_FORMAT(exDate,'%Y%m%d');
SET @drop_month_v = DATE_FORMAT(exDate,'%Y%m');
	IF FLAG = 1 THEN 
		SELECT CONCAT('DROP TABLE IF EXISTS ',table_name,';') AS str FROM information_schema.TABLES 
		WHERE table_schema='gt_global_statistic' AND table_name LIKE CONCAT('%',@drop_date_v,'%') AND table_name<>'table_call_cnt_history' 
		AND table_name<>'table_region_tile'  AND table_name<>'table_region_tile_copy' AND table_name<>'table_created_history' ;
		DELETE FROM   `gt_global_statistic`.`table_call_cnt_history`
		WHERE DATA_DATE  = @data_date_v;
		DELETE FROM   `gt_global_statistic`.`table_created_history`
		WHERE START_DATE  = @data_date_v;
		TRUNCATE TABLE gt_global_statistic.`tmp_running_task`;
		SELECT 
		DISTINCT CONCAT('CALL gt_gw_main.`SP_Generate_Global_Statistic_ONE`(4,''',DATA_DATE,''',',DATA_HOUR,',1,0,5,0);') AS str
		FROM `gt_covmo`.`table_call_cnt` a
		WHERE NOT EXISTS
		(
		SELECT NULL 
		FROM `gt_global_statistic`.`table_call_cnt_history` b
		WHERE a.`DATA_DATE`=b.`DATA_DATE` AND a.`DATA_HOUR`=b.`DATA_HOUR` AND a.`PU_ID`=b.`PU_ID`
		)
		AND `DATA_DATE`= @data_date_v
		GROUP BY a.`DATA_DATE`,a.`DATA_HOUR`,a.`PU_ID`;
		
	ELSEIF FLAG = 2 THEN 
		SELECT CONCAT('DROP TABLE IF EXISTS ',table_name,';') AS str FROM information_schema.TABLES 
		WHERE table_schema='gt_global_statistic' AND table_name LIKE CONCAT('%',@drop_month_v,'%') AND table_name<>'table_call_cnt_history' 
		AND table_name<>'table_region_tile'  AND table_name<>'table_region_tile_copy' AND table_name<>'table_created_history' ;
		DELETE FROM   `gt_global_statistic`.`table_call_cnt_history`
		WHERE DATA_DATE  >= @data_date_v;
		DELETE FROM   `gt_global_statistic`.`table_created_history`
		WHERE START_DATE  >= @data_date_v;
		TRUNCATE TABLE gt_global_statistic.`tmp_running_task`;
		SELECT 
		DISTINCT CONCAT('CALL gt_gw_main.`SP_Generate_Global_Statistic_ONE`(4,''',DATA_DATE,''',',DATA_HOUR,',0,0,5,0);') AS str
		FROM `gt_covmo`.`table_call_cnt` a
		WHERE NOT EXISTS
		(
		SELECT NULL 
		FROM `gt_global_statistic`.`table_call_cnt_history` b
		WHERE a.`DATA_DATE`=b.`DATA_DATE` AND a.`DATA_HOUR`=b.`DATA_HOUR` AND a.`PU_ID`=b.`PU_ID`
		)
		AND `DATA_DATE` >= @data_date_v
		GROUP BY a.`DATA_DATE`,a.`DATA_HOUR`,a.`PU_ID`;
	END IF;
	
END$$
DELIMITER ;
