DELIMITER $$
USE `gt_gw_main`$$
DROP PROCEDURE IF EXISTS `SP_CreateDB_LTE`$$
CREATE DEFINER=`covmo`@`%` PROCEDURE `SP_Delete_Special_Data`()
BEGIN
	DECLARE START_TIME DATETIME DEFAULT SYSDATE();
	DECLARE GT_DB VARCHAR(18) DEFAULT 'gt_special_report';
	SET @@session.group_concat_max_len = @@global.max_allowed_packet;
	INSERT INTO gt_gw_main.SP_LOG VALUES(GT_DB,'SP_Delete_Special_Data','Start: ', NOW());	
	SET @CUR_DATE=NOW();
	
	SET @SqlCmd=CONCAT('SELECT  GROUP_CONCAT(CONCAT(''(DATA_DATE NOT BETWEEN '''''',start_date,'''''' AND '''''',end_date,'''''')'') SEPARATOR '' AND '' )  INTO @DURATION
						FROM gt_covmo.cus_special_calendar  WHERE ENABLED = 1
						GROUP BY ENABLED;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;	
	
	SET @SqlCmd=CONCAT('DELETE FROM `gt_special_report`.`special_hourly_report`
						WHERE ',@DURATION,' AND DATA_DATE<''',DATE_ADD(DATE(@CUR_DATE), INTERVAL -14 DAY),'''; ');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('DELETE FROM `gt_special_report`.`special_quarter_report`
						WHERE ',@DURATION,' AND DATA_DATE<''',DATE_ADD(DATE(@CUR_DATE), INTERVAL -14 DAY),'''; ');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	INSERT INTO gt_gw_main.SP_LOG VALUES(GT_DB,'SP_Delete_Special_Data',CONCAT('Done: ',TIMESTAMPDIFF(SECOND,START_TIME,SYSDATE()),CONCAT('seconds.','-',@h_cnt,',',@q_cnt)), NOW());
END$$
DELIMITER ;
