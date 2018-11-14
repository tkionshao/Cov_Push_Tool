DELIMITER $$
USE `gt_gw_main`$$
DROP PROCEDURE IF EXISTS `SP_CreateDB_LTE`$$
CREATE DEFINER=`covmo`@`%` PROCEDURE `SP_Create_Merge_Rename_Table_GSM`(IN GT_DB VARCHAR(100),IN FLAG CHAR(2),IN EVENT_NUM TINYINT(2),IN POS_NUM TINYINT(2))
BEGIN
	DECLARE START_TIME DATETIME DEFAULT SYSDATE();
	DECLARE CUSTOMER_USER_FLAG VARCHAR(50);
	DECLARE imsig_count SMALLINT(6) DEFAULT 0;
	DECLARE v_j SMALLINT(6);
	
	SELECT LOWER(`value`) INTO CUSTOMER_USER_FLAG  FROM gt_gw_main.integration_param WHERE gt_group = 'sp' AND gt_name = 'special_imsi' ;
	SELECT GROUP_CONCAT(DISTINCT (ABS(`GROUP_ID`))) INTO @imsig_value FROM `gt_covmo`.`dim_imsi_group` WHERE GROUP_ID < 0;
	INSERT INTO gt_gw_main.SP_LOG VALUES(GT_DB,'SP_Create_Merge_Rename_Table','Start', START_TIME);
	
	IF FLAG='DY' THEN 
	
		IF CUSTOMER_USER_FLAG = 'true' THEN
			SET imsig_count = gt_covmo_csv_count(@imsig_value,',');
			SET v_j=1;
			WHILE v_j <= imsig_count DO
			BEGIN	
				SET @group_id = gt_covmo_csv_get(@imsig_value,v_j);
				SET @group_bottom_str = CONCAT('_imsig',@group_id);
				
				CALL gt_gw_main.SP_Create_Merge_Rename_24QI(GT_DB,CONCAT('table_call_gsm',@group_bottom_str),@group_bottom_str,'table_call_gsm_imsig');
				CALL gt_gw_main.SP_Create_Merge_Rename_24QI(GT_DB,CONCAT('table_position_gsm',@group_bottom_str),@group_bottom_str,'table_position_gsm_imsig');
	
				SET v_j=v_j+1;
			END;
			END WHILE;
		END IF;	
	
        
	
	END IF;
	
	INSERT INTO gt_gw_main.SP_LOG VALUES(GT_DB,'SP_Create_Merge_Rename_Table',CONCAT('Done: ',TIMESTAMPDIFF(SECOND,START_TIME,SYSDATE()),' seconds.'), NOW());
	
END$$
DELIMITER ;
