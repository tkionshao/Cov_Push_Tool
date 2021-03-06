DELIMITER $$
USE `gt_gw_main`$$
DROP PROCEDURE IF EXISTS `SP_CreateDB_LTE`$$
CREATE DEFINER=`covmo`@`%` PROCEDURE `SP_Generate_VIP_NEW`(IN GT_COVMO VARCHAR(100))
BEGIN
	
	INSERT INTO gt_gw_main.sp_log VALUES('VIP Import','SP_Generate_NT','Insert Data to dim_imsi_group', NOW());
	
	INSERT INTO gt_gw_main.dim_imsi_group
	(GROUP_ID,GROUP_NAME)
	SELECT  @rownum := @rownum + 1 AS GROUP_ID, t.GROUP_NAME 
	FROM 	(SELECT DISTINCT GROUP_NAME FROM dim_imsi_raw WHERE GROUP_NAME NOT IN  (SELECT GROUP_NAME FROM dim_imsi_group)) t,
		(SELECT @rownum := IFNULL(MAX(group_id),0) FROM dim_imsi_group) r;
	
	UPDATE  gt_gw_main.dim_imsi_group SET IMPORT_TIME=NOW();	
	
	INSERT INTO gt_gw_main.sp_log VALUES('VIP Import','SP_Generate_NT','Insert Data to dim_imsi', NOW());
	
	TRUNCATE TABLE gt_gw_main.dim_imsi; 
	
	INSERT INTO gt_gw_main.dim_imsi
	SELECT IMPORT_TIME, NICKNAME,IMSI,GROUP_ID,A.GROUP_NAME
	FROM gt_gw_main.dim_imsi_raw A,
	     gt_gw_main.dim_imsi_group B 
	WHERE A.GROUP_NAME=B.GROUP_NAME; 
	
	
	
	
	DELETE FROM  gt_gw_main.`dim_imsi_history`
	WHERE IMPORT_TIME=(SELECT IMPORT_TIME FROM dim_imsi_group LIMIT 1)	;
	
	
	INSERT INTO gt_gw_main.sp_log VALUES('VIP Import','SP_Generate_NT','Insert Data to dim_imsi_history', NOW());
	
	INSERT INTO gt_gw_main.`dim_imsi_history`
	SELECT A.* FROM  gt_gw_main.dim_imsi A
		       ,gt_gw_main.dim_imsi_group B
	WHERE A.GROUP_NAME=B.GROUP_NAME; 
	
	
	
	
	SET @SqlCmd=CONCAT('DELETE FROM ',GT_COVMO,'.`dim_imsi`;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('INSERT INTO ',GT_COVMO,'.`dim_imsi` SELECT *  FROM gt_gw_main.`dim_imsi` ;	');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	INSERT INTO gt_gw_main.sp_log VALUES('VIP Import','SP_Generate_NT','Done', NOW());
	
	
END$$
DELIMITER ;
