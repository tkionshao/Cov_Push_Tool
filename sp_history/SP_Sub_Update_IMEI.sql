DELIMITER $$
USE `gt_gw_main`$$
DROP PROCEDURE IF EXISTS `SP_CreateDB_LTE`$$
CREATE DEFINER=`covmo`@`%` PROCEDURE `SP_Sub_Update_IMEI`(IN GT_DB VARCHAR(100),IN GT_COVMO VARCHAR(100),IN TECH_MASK TINYINT(4))
BEGIN
	DECLARE GT_SESSION_ID INT;
	DECLARE START_TIME DATETIME DEFAULT SYSDATE();
	DECLARE STEP_START_TIME DATETIME DEFAULT SYSDATE();
	
	DECLARE WORKER_ID VARCHAR(10) DEFAULT CONNECTION_ID();	
	
	SELECT SESSION_ID INTO GT_Session_ID FROM gt_gw_main.session_information WHERE SESSION_DB=GT_DB;
	INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'SP_Sub_Update_IMEI','Start', NOW());
	SET STEP_START_TIME := SYSDATE();
	
	IF TECH_MASK=1 THEN 
		SET @table_call='table_call_gsm';
	ELSEIF TECH_MASK=2 THEN 
		SET @table_call='table_call';	
	ELSEIF TECH_MASK=4 THEN 		
		SET @table_call='table_call_lte';
	ELSE 
		SET @table_call='table_call';
	END IF;
	
	SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS ',GT_DB,'.tmp_dim_imsi_imei','_',WORKER_ID,';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS ',GT_DB,'.tmp_dim_imsi_imei','_null_',WORKER_ID,';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;		
	
	SET @SqlCmd=CONCAT('CREATE TEMPORARY TABLE ',GT_DB,'.tmp_dim_imsi_imei','_',WORKER_ID,' 
				(
				`SESSION_ID` bigint(20) DEFAULT NULL,
				  `DATA_TIME` datetime DEFAULT NULL,
				  `IMSI` varchar(20),
				  `IMEI` varchar(20),
				  PRIMARY KEY (IMSI,IMEI)
				) ENGINE=MyISAM DEFAULT CHARSET=latin1;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('CREATE TEMPORARY TABLE ',GT_DB,'.tmp_dim_imsi_imei','_null_',WORKER_ID,' 
				(
					`IMSI` varchar(20),
					`IMEI` varchar(20),
					KEY IX_IMSI (IMSI,IMEI)
				) ENGINE=MyISAM DEFAULT CHARSET=latin1;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('INSERT INTO ',GT_DB,'.tmp_dim_imsi_imei','_',WORKER_ID,'
				SELECT SESSION_ID,DATA_TIME,IMSI,
					CASE WHEN LENGTH(AA.IMEI)=12 THEN CONCAT(''00'',AA.IMEI)
						 WHEN LENGTH(AA.IMEI)=13 THEN CONCAT(''0'',AA.IMEI)
						 WHEN LENGTH(AA.IMEI)>=14 THEN AA.IMEI
					    END AS IMEI 
				FROM 
				(
					SELECT ', GT_Session_ID,' AS SESSION_ID,t.START_TIME AS DATA_TIME,t.IMSI,t.IMEI,
						@num := IF(@IMSI=t.IMSI , @num + 1, 1) AS `RANK`,
						@IMSI := IMSI AS dummy
					FROM ',GT_DB,'.',@table_call,' t
					,(SELECT @num := 0) r,(SELECT @IMSI:='''') s 
					WHERE t.IMSI IS NOT NULL AND TRIM(t.IMSI) <> '''' 
					AND t.IMEI IS NOT NULL AND TRIM(t.IMEI) <> ''''
					AND t.IMEI NOT IN (''FFFFFFFFFFFFFFF'',''000000000000000'',''0'')
					ORDER BY t.IMSI,t.START_TIME DESC,t.START_TIME_MS DESC
				) AA
				WHERE `RANK`=1;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'SP_Sub_Update_IMEI',CONCAT('INSERT DATA TO tmp_dim_imsi_imei IMEI NOT NULL cost:',TIMESTAMPDIFF(SECOND,STEP_START_TIME,SYSDATE()),' sec.'), NOW());
	SET STEP_START_TIME := SYSDATE();
	
	SET @SqlCmd=CONCAT('INSERT INTO ',GT_DB,'.tmp_dim_imsi_imei','_null_',WORKER_ID,'
				SELECT DISTINCT IMSI,IMEI
				FROM ',GT_DB,'.',@table_call,' 
				WHERE IMSI IS NOT NULL AND TRIM(IMSI) <> '''' 
				AND IMEI IS NULL OR TRIM(IMEI) IN ('''',''FFFFFFFFFFFFFFF'',''000000000000000'',''0'');');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'SP_Sub_Update_IMEI',CONCAT('INSERT DATA TO tmp_dim_imsi_imei_null IMEI IS NULL cost:',TIMESTAMPDIFF(SECOND,STEP_START_TIME,SYSDATE()),' sec.'), NOW());
	SET STEP_START_TIME := SYSDATE();
	
	SET @SqlCmd=CONCAT('REPLACE INTO gt_gw_main.dim_imsi_imei
			    SELECT B.SESSION_ID,B.DATA_TIME,B.IMSI,B.IMEI FROM ',GT_DB,'.tmp_dim_imsi_imei','_',WORKER_ID,' B;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
		
	
	INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'SP_Sub_Update_IMEI',CONCAT('REPLACE INTO gt_gw_main.dim_imsi_imei cost:',TIMESTAMPDIFF(SECOND,STEP_START_TIME,SYSDATE()),' sec.'), NOW());
	SET STEP_START_TIME := SYSDATE();
	
	SET @SqlCmd=CONCAT('UPDATE ',GT_DB,'.tmp_dim_imsi_imei','_null_',WORKER_ID,' A ,gt_gw_main.dim_imsi_imei B
			    SET A.IMEI=B.IMEI
			    WHERE A.IMSI=B.IMSI
			    ;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('INSERT IGNORE INTO ',GT_DB,'.tmp_dim_imsi_imei','_null_',WORKER_ID,'
			    SELECT B.IMSI,B.IMEI FROM ',GT_DB,'.tmp_dim_imsi_imei','_',WORKER_ID,' B;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'SP_Sub_Update_IMEI',CONCAT('UPDATE gt_gw_main.dim_imsi_imei cost:',TIMESTAMPDIFF(SECOND,STEP_START_TIME,SYSDATE()),' sec.'), NOW());
	SET STEP_START_TIME := SYSDATE();
	
	SET @SqlCmd=CONCAT('UPDATE ',GT_DB,'.',@table_call,' A FORCE INDEX(IMSI),',GT_DB,'.tmp_dim_imsi_imei','_null_',WORKER_ID,' B
			    SET ',CASE WHEN TECH_MASK=2 THEN 'A.IMEI_NEW' ELSE 'A.IMEI' END ,'=B.IMEI
			    WHERE A.IMSI=B.IMSI
			    ;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;	
	INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'SP_Sub_Update_IMEI',CONCAT('UPDATE ',@table_call,' null cost:',TIMESTAMPDIFF(SECOND,STEP_START_TIME,SYSDATE()),' sec.'), NOW());
	SET STEP_START_TIME := SYSDATE();
	
	SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS ',GT_DB,'.tmp_dim_imsi_imei','_',WORKER_ID,';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;		
	
	SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS ',GT_DB,'.tmp_dim_imsi_imei','_null_',WORKER_ID,';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	IF TECH_MASK=2 THEN 
		CALL gt_gw_main.`SP_Sub_Update_APN`(GT_DB,GT_COVMO);
	END IF;
	
	INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'SP_Sub_Update_IMEI',CONCAT(' Done: ',TIMESTAMPDIFF(SECOND,START_TIME,SYSDATE()),' seconds.'), NOW());
	
END$$
DELIMITER ;
