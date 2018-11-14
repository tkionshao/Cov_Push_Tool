CREATE DEFINER=`covmo`@`%` PROCEDURE `SP_Grant`(IN GT_DB VARCHAR(50), IN TECH_MASK VARCHAR(10))
BEGIN
	DECLARE C_USER VARCHAR(1000);
	DECLARE GRANT_TABLE VARCHAR(1000);
	DECLARE NT_FLAG VARCHAR(10);
	DECLARE CURRENT_NT_DB VARCHAR(50) DEFAULT CONCAT('gt_nt_',gt_strtok(GT_DB,3,'_'));
	DECLARE v_i INT;
	DECLARE v_j INT;
	DECLARE user_count VARCHAR(10);
	DECLARE table_count VARCHAR(10);
	
	SELECT `value` INTO C_USER  FROM gt_gw_main.integration_param WHERE gt_group = 'sp' AND gt_name = 'grant_user' ;
	SELECT `value` INTO NT_FLAG  FROM gt_gw_main.integration_param WHERE gt_group = 'sp' AND gt_name = 'grant_nt' ;
	
	IF TECH_MASK = 1 THEN#GSM
		SELECT `value` INTO GRANT_TABLE  FROM gt_gw_main.integration_param WHERE gt_group = 'sp' AND gt_name = 'grant_table_gsm' ;
	ELSEIF TECH_MASK = 2 THEN#UMTS
		SELECT `value` INTO GRANT_TABLE  FROM gt_gw_main.integration_param WHERE gt_group = 'sp' AND gt_name = 'grant_table' ;
	ELSEIF TECH_MASK = 7 THEN#LTE
		SELECT `value` INTO GRANT_TABLE  FROM gt_gw_main.integration_param WHERE gt_group = 'sp' AND gt_name = 'grant_table_lte' ;
	END IF;
	
	
	
	INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'SP_Grant','Grant start', NOW());
	
	SET user_count = gt_covmo_csv_count(C_USER,',');
	SET table_count = gt_covmo_csv_count(GRANT_TABLE,',');
	SET v_i=1;
	WHILE v_i <= user_count DO
	BEGIN
		SET @user1 = gt_covmo_csv_get(C_USER,v_i);
		
		SET v_j=1;
		WHILE v_j <= table_count DO
		BEGIN
			SET @table1 = gt_covmo_csv_get(GRANT_TABLE,v_j);
			SET @SqlCmd =CONCAT('GRANT SELECT ON ',GT_DB,'.',@table1,' TO ',@user1,'@''%'' ;');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
		 	DEALLOCATE PREPARE Stmt;
	
	
			SET v_j=v_j+1;
		END;
		END WHILE;
		SET v_i=v_i+1;
	END;
	END WHILE;
	
	IF NT_FLAG = 'true' THEN
		SET @SqlCmd =CONCAT('GRANT SELECT ON ',CURRENT_NT_DB,'.* TO ',C_USER,'@''%'' ;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
	END IF;
	
	FLUSH PRIVILEGES;
	INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'SP_Grant','Grant end', NOW());
