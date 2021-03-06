DELIMITER $$
USE `gt_gw_main`$$
DROP PROCEDURE IF EXISTS `SP_CreateDB_LTE`$$
CREATE DEFINER=`covmo`@`%` PROCEDURE `SP_Session_Status`()
BEGIN
	
	SET @SqlCmd=CONCAT('
	SELECT @@hostname AS host_name, 
	gt_strtok(REPLACE(REPLACE(SUBSTR(rncstr,2,LENGTH(rncstr)-2),''","'',''|''),''"'',''''),4,''|'') AS rnc_name,
	gt_strtok(REPLACE(REPLACE(SUBSTR(rncstr,2,LENGTH(rncstr)-2),''","'',''|''),''"'',''''),5,''|'') AS start_time,
	gt_strtok(REPLACE(REPLACE(SUBSTR(rncstr,2,LENGTH(rncstr)-2),''","'',''|''),''"'',''''),6,''|'') AS end_time,
	gt_strtok(REPLACE(REPLACE(SUBSTR(rncstr,2,LENGTH(rncstr)-2),''","'',''|''),''"'',''''),7,''|'') AS vendor,
	STATUS,
	task_group,
	gt_strtok(REPLACE(SUBSTR(jsonstr,2,LENGTH(jsonstr)-2),''"'',''''),1,'','') AS positionCount,
	gt_strtok(REPLACE(SUBSTR(jsonstr,2,LENGTH(jsonstr)-2),''"'',''''),2,'','') AS positionCallCount,
	gt_strtok(REPLACE(SUBSTR(jsonstr,2,LENGTH(jsonstr)-2),''"'',''''),3,'','') AS sessionCallCount,
	message
	FROM queue   
	WHERE STATUS IN (''Complete'' ,''Failed'',''Canceled'')
	ORDER BY lastModified DESC LIMIT 1
	
	;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
END$$
DELIMITER ;
