CREATE DEFINER=`covmo`@`%` PROCEDURE `SP_Polystar_Merge_Top_User`(IN GT_DB VARCHAR(50),IN gt_polystar_db VARCHAR (20),IN TIME_TYPE VARCHAR(20))
BEGIN
	DECLARE START_TIME DATETIME DEFAULT SYSDATE();
	DECLARE RNC_ID INT;
	DECLARE DATA_DATE VARCHAR(8);
	DECLARE DATA_QRT VARCHAR(4);
	DECLARE DATA_HOUR VARCHAR(4);
	DECLARE FOLDER_PATH VARCHAR(20);
	DECLARE DAILY_DB VARCHAR(25);
	DECLARE SH_EH VARCHAR(9) DEFAULT RIGHT(GT_DB,9);
	DECLARE CURRENT_NT_DB VARCHAR(50) DEFAULT CONCAT('gt_nt_',gt_strtok(GT_DB,3,'_'));
	DECLARE DATA_DATE_END VARCHAR(50);
	
	
 	SELECT gt_strtok(GT_DB,2,'_') INTO RNC_ID;
 	SELECT gt_strtok(GT_DB,3,'_') INTO DATA_DATE;
 	SELECT gt_strtok(GT_DB,4,'_') INTO DATA_QRT;
	SELECT LEFT (DATA_QRT,2) INTO DATA_HOUR;
	
	SET DATA_DATE_END= CONCAT(DATE(DATA_DATE),' 23:59:59');
	SELECT `DAY_OF_WEEK`(DATA_DATE) INTO @DAY_OF_WEEK;
	SET @FIRST_DAY=gt_strtok(@DAY_OF_WEEK, 1, '|');
	SET @END_DAY=gt_strtok(@DAY_OF_WEEK, 2, '|');
	SET @DATE_WK=CONCAT(DATE_FORMAT(@FIRST_DAY,'%Y%m%d'),'_',DATE_FORMAT(@END_DAY,'%Y%m%d'));
	SET @FIRST_DAY_MN=DATE_SUB(DATA_DATE,INTERVAL DAY(DATA_DATE)-1 DAY);
	SET @END_DAY_MN=LAST_DAY(DATA_DATE);
 
	INSERT INTO gt_gw_main.sp_log VALUES('gt_gw_main','SP_Polystar_Merge_Top_User','START TO INSERT Merge top User',NOW());	
	
	IF TIME_TYPE =2 THEN 
		INSERT INTO gt_gw_main.sp_log VALUES('gt_gw_main','SP_Polystar_Merge_Top_User',CONCAT('START TO INSERT rpt_xdr_top_sub_global_hr:',DATA_DATE,'_',DATA_HOUR,''),NOW());	
		SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS ',gt_polystar_db,'.`polystar_merge_user_',DATA_DATE,'_',DATA_HOUR,'` ;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
	
		SET @SqlCmd=CONCAT('CREATE TEMPORARY TABLE IF NOT EXISTS ',gt_polystar_db,'.`polystar_merge_user_',DATA_DATE,'_',DATA_HOUR,'` (
					`DATA_DATE` DATE NOT NULL,
					`DATA_HOUR` TINYINT(4) NOT NULL,				
					`IMSI` VARCHAR(16) NOT NULL,
					`IMEI` VARCHAR(16) NOT NULL,
					`ul_numberOfFlow_umts` INT(11) DEFAULT ''0'',
					`ul_numberOfPackets_umts` INT(11) DEFAULT ''0'',
					`ul_numberOfBytes_umts` INT(11) DEFAULT ''0'',
					`dl_numberOfFlow_umts` INT(11) DEFAULT ''0'',
					`dl_numberOfPackets_umts` INT(11) DEFAULT ''0'',
					`dl_numberOfBytes_umts` INT(11) DEFAULT ''0'',
					`ul_numberOfFlow_lte` INT(11) DEFAULT ''0'',
					`ul_numberOfPackets_lte` INT(11) DEFAULT ''0'',
					`ul_numberOfBytes_lte` INT(11) DEFAULT ''0'',
					`dl_numberOfFlow_lte` INT(11) DEFAULT ''0'',
					`dl_numberOfPackets_lte` INT(11) DEFAULT ''0'',
					`dl_numberOfBytes_lte`INT(11) DEFAULT ''0''
			,KEY `IX_TIME_IMSI` (`DATA_DATE`,`DATA_HOUR`,`IMSI`,`IMEI`)
			) ENGINE=MYISAM DEFAULT CHARSET=latin1;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		
		SET @SqlCmd=CONCAT(' INSERT INTO  ',gt_polystar_db,'.`polystar_merge_user_',DATA_DATE,'_',DATA_HOUR,'`
				(`DATA_DATE`,`DATA_HOUR`,`IMSI`,`IMEI`,
					`ul_numberOfFlow_umts`,
					`ul_numberOfPackets_umts`,
					`ul_numberOfBytes_umts`,
					`dl_numberOfFlow_umts`,
					`dl_numberOfPackets_umts`,
					`dl_numberOfBytes_umts`
				)
				SELECT DATA_DATE,DATA_HOUR,IMSI,IMEI,
					`ul_numberOfFlow`,
					`ul_numberOfPackets`,
					`ul_numberOfBytes`,
					`dl_numberOfFlow`,
					`dl_numberOfPackets`,
					`dl_numberOfBytes`
					FROM ',gt_polystar_db,'.`rpt_xdr_top_sub_umts_hr`
					WHERE DATA_DATE=',DATA_DATE,' AND DATA_HOUR=',DATA_HOUR,'
				;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
	
		SET @SqlCmd=CONCAT(' INSERT INTO  ',gt_polystar_db,'.`polystar_merge_user_',DATA_DATE,'_',DATA_HOUR,'`
				(`DATA_DATE`,`DATA_HOUR`,`IMSI`,`IMEI`,
					`ul_numberOfFlow_lte`,
					`ul_numberOfPackets_lte`,
					`ul_numberOfBytes_lte`,
					`dl_numberOfFlow_lte`,
					`dl_numberOfPackets_lte`,
					`dl_numberOfBytes_lte`
				)
				SELECT DATA_DATE,DATA_HOUR,IMSI,IMEI,
					`ul_numberOfFlow`,
					`ul_numberOfPackets`,
					`ul_numberOfBytes`,
					`dl_numberOfFlow`,
					`dl_numberOfPackets`,
					`dl_numberOfBytes`
					FROM ',gt_polystar_db,'.`rpt_xdr_top_sub_lte_hr`
					WHERE DATA_DATE=',DATA_DATE,' AND DATA_HOUR=',DATA_HOUR,'
				;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
	
		SET @SqlCmd=CONCAT(' DELETE FROM  ',gt_polystar_db,'.`rpt_xdr_top_sub_global_hr`
					WHERE DATA_DATE=',DATA_DATE,' AND DATA_HOUR=',DATA_HOUR,'
					
				;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
	
		SET @SqlCmd=CONCAT(' INSERT INTO  ',gt_polystar_db,'.`rpt_xdr_top_sub_global_hr`
				(`DATA_DATE`,`DATA_HOUR`,`IMSI`,`IMEI`,
					`ul_numberOfFlow_umts`,
					`ul_numberOfPackets_umts`,
					`ul_numberOfBytes_umts`,
					`dl_numberOfFlow_umts`,
					`dl_numberOfPackets_umts`,
					`dl_numberOfBytes_umts`,
					`ul_numberOfFlow_lte`,
					`ul_numberOfPackets_lte`,
					`ul_numberOfBytes_lte`,
					`dl_numberOfFlow_lte`,
					`dl_numberOfPackets_lte`,
					`dl_numberOfBytes_lte`
				)
				SELECT DATA_DATE,DATA_HOUR,IMSI,IMEI,
					SUM(IFNULL(`ul_numberOfFlow_umts`,0)) AS `ul_numberOfFlow_umts`,
					SUM(IFNULL(`ul_numberOfPackets_umts`,0)) AS `ul_numberOfPackets_umts`,
					SUM(IFNULL(`ul_numberOfBytes_umts`,0)) AS `ul_numberOfBytes_umts`,
					SUM(IFNULL(`dl_numberOfFlow_umts`,0)) AS `dl_numberOfFlow_umts`, 
					SUM(IFNULL(`dl_numberOfPackets_umts`,0)) AS `dl_numberOfPackets_umts`,
					SUM(IFNULL(`dl_numberOfBytes_umts`,0)) AS `dl_numberOfBytes_umts`,
					SUM(IFNULL(`ul_numberOfFlow_lte`,0)) AS `ul_numberOfFlow_lte`,
					SUM(IFNULL(`ul_numberOfPackets_lte`,0)) AS `ul_numberOfPackets_lte`,
					SUM(IFNULL(`ul_numberOfBytes_lte`,0)) AS `ul_numberOfBytes_lte`,
					SUM(IFNULL(`dl_numberOfFlow_lte`,0)) AS `dl_numberOfFlow_lte`,
					SUM(IFNULL(`dl_numberOfPackets_lte`,0)) AS `dl_numberOfPackets_lte`,
					SUM(IFNULL(`dl_numberOfBytes_lte`,0)) AS `dl_numberOfBytes_lte`
				FROM ',gt_polystar_db,'.`polystar_merge_user_',DATA_DATE,'_',DATA_HOUR,'`
				GROUP BY DATA_DATE,DATA_HOUR,IMSI,IMEI
		--		ORDER BY SUM(ul_numberOfBytes_umts+dl_numberOfBytes_umts+ul_numberOfBytes_lte+dl_numberOfBytes_lte) DESC
		--		LIMIT 1000
					
				;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
	
		SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS ',gt_polystar_db,'.`polystar_merge_user_',DATA_DATE,'_',DATA_HOUR,'` ;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		
		INSERT INTO gt_gw_main.sp_log VALUES('gt_gw_main','SP_Polystar_Merge_Top_User',CONCAT('Done INSERT rpt_xdr_top_sub_global_hr:',DATA_DATE,'_',DATA_HOUR,''),NOW());	
	
	END IF;
	
	IF TIME_TYPE =3 THEN 
		INSERT INTO gt_gw_main.sp_log VALUES('gt_gw_main','SP_Polystar_Merge_Top_User',CONCAT('START TO INSERT rpt_xdr_top_sub_global_dy:',DATA_DATE,''),NOW());	
		SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS ',gt_polystar_db,'.`polystar_merge_user_',DATA_DATE,'` ;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
	
		SET @SqlCmd=CONCAT('CREATE TEMPORARY TABLE IF NOT EXISTS ',gt_polystar_db,'.`polystar_merge_user_',DATA_DATE,'` (
					`DATA_DATE` DATE NOT NULL,								
					`IMSI` VARCHAR(16) NOT NULL,
					`IMEI` VARCHAR(16) NOT NULL,
					`ul_numberOfFlow_umts` INT(11) DEFAULT ''0'',
					`ul_numberOfPackets_umts` INT(11) DEFAULT ''0'',
					`ul_numberOfBytes_umts` INT(11) DEFAULT ''0'',
					`dl_numberOfFlow_umts` INT(11) DEFAULT ''0'',
					`dl_numberOfPackets_umts` INT(11) DEFAULT ''0'',
					`dl_numberOfBytes_umts` INT(11) DEFAULT ''0'',
					`ul_numberOfFlow_lte` INT(11) DEFAULT ''0'',
					`ul_numberOfPackets_lte` INT(11) DEFAULT ''0'',
					`ul_numberOfBytes_lte` INT(11) DEFAULT ''0'',
					`dl_numberOfFlow_lte` INT(11) DEFAULT ''0'',
					`dl_numberOfPackets_lte` INT(11) DEFAULT ''0'',
					`dl_numberOfBytes_lte`INT(11) DEFAULT ''0''
			,KEY `IX_TIME_IMSI` (`DATA_DATE`,`IMSI`,`IMEI`)
			) ENGINE=MYISAM DEFAULT CHARSET=latin1;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		
		SET @SqlCmd=CONCAT(' INSERT INTO  ',gt_polystar_db,'.`polystar_merge_user_',DATA_DATE,'`
				(`DATA_DATE`,`IMSI`,`IMEI`,
					`ul_numberOfFlow_umts`,
					`ul_numberOfPackets_umts`,
					`ul_numberOfBytes_umts`,
					`dl_numberOfFlow_umts`,
					`dl_numberOfPackets_umts`,
					`dl_numberOfBytes_umts`
				)
				SELECT DATA_DATE,IMSI,IMEI,
					`ul_numberOfFlow`,
					`ul_numberOfPackets`,
					`ul_numberOfBytes`,
					`dl_numberOfFlow`,
					`dl_numberOfPackets`,
					`dl_numberOfBytes`
					FROM ',gt_polystar_db,'.`rpt_xdr_top_sub_umts_dy`
					WHERE DATA_DATE=',DATA_DATE,' 
				;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
	
		SET @SqlCmd=CONCAT(' INSERT INTO  ',gt_polystar_db,'.`polystar_merge_user_',DATA_DATE,'`
				(`DATA_DATE`,`IMSI`,`IMEI`,
					`ul_numberOfFlow_lte`,
					`ul_numberOfPackets_lte`,
					`ul_numberOfBytes_lte`,
					`dl_numberOfFlow_lte`,
					`dl_numberOfPackets_lte`,
					`dl_numberOfBytes_lte`
				)
				SELECT DATA_DATE,IMSI,IMEI,
					`ul_numberOfFlow`,
					`ul_numberOfPackets`,
					`ul_numberOfBytes`,
					`dl_numberOfFlow`,
					`dl_numberOfPackets`,
					`dl_numberOfBytes`
					FROM ',gt_polystar_db,'.`rpt_xdr_top_sub_lte_dy`
					WHERE DATA_DATE=',DATA_DATE,' 
				;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
	
		SET @SqlCmd=CONCAT(' DELETE FROM  ',gt_polystar_db,'.`rpt_xdr_top_sub_global_dy`
					WHERE DATA_DATE=',DATA_DATE,' 
				;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
	
		SET @SqlCmd=CONCAT(' INSERT INTO  ',gt_polystar_db,'.`rpt_xdr_top_sub_global_dy`
				(`DATA_DATE`,`IMSI`,`IMEI`,
					`ul_numberOfFlow_umts`,
					`ul_numberOfPackets_umts`,
					`ul_numberOfBytes_umts`,
					`dl_numberOfFlow_umts`,
					`dl_numberOfPackets_umts`,
					`dl_numberOfBytes_umts`,
					`ul_numberOfFlow_lte`,
					`ul_numberOfPackets_lte`,
					`ul_numberOfBytes_lte`,
					`dl_numberOfFlow_lte`,
					`dl_numberOfPackets_lte`,
					`dl_numberOfBytes_lte`
				)
				SELECT DATA_DATE,IMSI,IMEI,
					SUM(IFNULL(`ul_numberOfFlow_umts`,0)) AS `ul_numberOfFlow_umts`,
					SUM(IFNULL(`ul_numberOfPackets_umts`,0)) AS `ul_numberOfPackets_umts`,
					SUM(IFNULL(`ul_numberOfBytes_umts`,0)) AS `ul_numberOfBytes_umts`,
					SUM(IFNULL(`dl_numberOfFlow_umts`,0)) AS `dl_numberOfFlow_umts`, 
					SUM(IFNULL(`dl_numberOfPackets_umts`,0)) AS `dl_numberOfPackets_umts`,
					SUM(IFNULL(`dl_numberOfBytes_umts`,0)) AS `dl_numberOfBytes_umts`,
					SUM(IFNULL(`ul_numberOfFlow_lte`,0)) AS `ul_numberOfFlow_lte`,
					SUM(IFNULL(`ul_numberOfPackets_lte`,0)) AS `ul_numberOfPackets_lte`,
					SUM(IFNULL(`ul_numberOfBytes_lte`,0)) AS `ul_numberOfBytes_lte`,
					SUM(IFNULL(`dl_numberOfFlow_lte`,0)) AS `dl_numberOfFlow_lte`,
					SUM(IFNULL(`dl_numberOfPackets_lte`,0)) AS `dl_numberOfPackets_lte`,
					SUM(IFNULL(`dl_numberOfBytes_lte`,0)) AS `dl_numberOfBytes_lte`
				FROM ',gt_polystar_db,'.`polystar_merge_user_',DATA_DATE,'`
				GROUP BY DATA_DATE,IMSI,IMEI
		--		ORDER BY SUM(ul_numberOfBytes_umts+dl_numberOfBytes_umts+ul_numberOfBytes_lte+dl_numberOfBytes_lte) DESC
		--		LIMIT 1000
					
				;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
	
		SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS ',gt_polystar_db,'.`polystar_merge_user_',DATA_DATE,'` ;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		INSERT INTO gt_gw_main.sp_log VALUES('gt_gw_main','SP_Polystar_Merge_Top_User',CONCAT('Done TO INSERT rpt_xdr_top_sub_global_dy:',DATA_DATE,''),NOW());	
		
	END IF;
	
	INSERT INTO gt_gw_main.SP_LOG VALUES(GT_DB,'SP_Polystar_Merge_Top_User',CONCAT('Done: ',TIMESTAMPDIFF(SECOND,START_TIME,SYSDATE()),' seconds.'), NOW());
