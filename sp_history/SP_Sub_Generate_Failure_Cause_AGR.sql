DELIMITER $$
USE `gt_gw_main`$$
DROP PROCEDURE IF EXISTS `SP_CreateDB_LTE`$$
CREATE DEFINER=`covmo`@`%` PROCEDURE `SP_Sub_Generate_Failure_Cause_AGR`(IN TMP_GT_DB VARCHAR(100), IN GT_DB VARCHAR(100))
BEGIN
       	DECLARE RNC_ID INT;
	DECLARE SH_EH VARCHAR(9) DEFAULT RIGHT(GT_DB,9);
	DECLARE START_TIME DATETIME DEFAULT SYSDATE();
	
	SELECT gt_strtok(GT_DB,2,'_') INTO RNC_ID;		
	SELECT REPLACE(GT_DB,SH_EH,'0000_0000') INTO GT_DB;
	INSERT INTO gt_gw_main.SP_LOG VALUES(TMP_GT_DB,'SP_Sub_Generate_Failure_Cause_AGR','Start', START_TIME);
	
	SET @SqlCmd=CONCAT('INSERT INTO ',GT_DB,'.table_tile_failure_cause
				(DATA_DATE,
				DATA_HOUR,
				TILE_ID,
				RNC_ID,
				SITE_ID,
				CELL_ID,
				IMSI,
				IMEI_NEW,
				EVENT_ID,
				FAILURE_EVENT_ID,
				FAILURE_EVENT_CAUSE,
				FAILURE_CNT
			)
		 	SELECT
				DATA_DATE,
				DATA_HOUR,
				TILE_ID,
				RNC_ID,
				SITE_ID,
				CELL_ID,
				IMSI,
				IMEI_NEW,
				EVENT_ID,
				FAILURE_EVENT_ID,
				FAILURE_EVENT_CAUSE,
				SUM(FAILURE_CNT) AS FAILURE_CNT
			FROM ',TMP_GT_DB,'.table_tile_failure_cause_update
		;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	INSERT INTO gt_gw_main.SP_LOG VALUES(TMP_GT_DB,'SP_Sub_Generate_Failure_Cause_AGR',CONCAT('Done: ',TIMESTAMPDIFF(SECOND,START_TIME,SYSDATE()),' seconds.'), NOW());	
	
END$$
DELIMITER ;
