CREATE DEFINER=`covmo`@`%` PROCEDURE `SP_Insert_Into_Daily_DB`(IN GT_DB VARCHAR(100),IN VENDER_ID INT,IN GW_IP VARCHAR(50),IN RUN_STATUS VARCHAR(10),IN GT_COVMO VARCHAR(100))
BEGIN
	DECLARE SH_EH VARCHAR(9) DEFAULT RIGHT(GT_DB,9);		
	DECLARE START_TIME DATETIME DEFAULT SYSDATE();
	DECLARE RNC_ID INT; 
	DECLARE FROM_GT_DB VARCHAR(100) DEFAULT GT_DB;
	DECLARE TO_GT_DB_TODAY VARCHAR(100);
	DECLARE TO_GT_DB_NEXT_DAY VARCHAR(100);
	
	DECLARE PARTITION_ID INT DEFAULT SUBSTRING(RIGHT(GT_DB,18),10,2) ;
	DECLARE GT_DB_START_HOUR INT DEFAULT SUBSTRING(RIGHT(GT_DB,18),10,4);
	
	INSERT INTO gt_gw_main.sp_log VALUES(FROM_GT_DB,'SP_Insert_Into_Daily_DB','START', NOW());
	SELECT gt_strtok(GT_DB,2,'_') INTO RNC_ID;
	SELECT REPLACE(GT_DB,SH_EH,'0000_0000') INTO GT_DB;
	SELECT REPLACE(GT_DB,SH_EH,'0000_0000') INTO TO_GT_DB_TODAY;
        SET @SqlCmd=CONCAT('SELECT COUNT(*) INTO @TABLE_CALL_CNT FROM ',FROM_GT_DB,'.table_call_update;');
        PREPARE Stmt FROM @SqlCmd;
        EXECUTE Stmt;
        DEALLOCATE PREPARE Stmt;
        IF @TABLE_CALL_CNT = 0 THEN
                SIGNAL SQLSTATE '10001' SET MESSAGE_TEXT='table_call_update NO records';
        END IF;
	
	SET @OLD_WORD = CONCAT(SUBSTRING(gt_strtok(GT_DB,3,'_'),1,4),SUBSTRING(gt_strtok(GT_DB,3,'_'),5,2),SUBSTRING(gt_strtok(GT_DB,3,'_'),7,2));
	SET @NEW_WORD = REPLACE(DATE_ADD(CONCAT(SUBSTRING(gt_strtok(GT_DB,3,'_'),1,4),'-',SUBSTRING(gt_strtok(GT_DB,3,'_'),5,2),'-',SUBSTRING(gt_strtok(GT_DB,3,'_'),7,2)), INTERVAL 1 DAY),'-','');
	SELECT 	REPLACE(GT_DB, @OLD_WORD, @NEW_WORD) INTO TO_GT_DB_NEXT_DAY;
	INSERT INTO gt_gw_main.sp_log VALUES(FROM_GT_DB,'SP_Insert_Into_Daily_DB','UPDATE CALLTABLE START', NOW());
	CALL SP_Sub_Update_CallTable(FROM_GT_DB,GT_COVMO);
	
	CALL SP_Insert_Into_Daily_DB_Process(FROM_GT_DB,TO_GT_DB_TODAY,VENDER_ID,GW_IP,RUN_STATUS);
	
	
	IF RUN_STATUS = 'rerun' THEN
		INSERT INTO gt_gw_main.sp_log VALUES(FROM_GT_DB,'SP_Insert_Into_Daily_DB','DELETE opt_inter_ifho', NOW());
	
		SET @SqlCmd=CONCAT('ALTER TABLE ',GT_DB,'.opt_inter_ifho TRUNCATE PARTITION b',GT_DB_START_HOUR,';');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;		
	END IF;	
	
	CALL SP_Sub_Set_Session_Param(GT_DB);
	
	INSERT INTO gt_gw_main.sp_log VALUES(FROM_GT_DB,'SP_Insert_Into_Daily_DB','INSERT opt_inter_ifho', NOW());
	
	SET @SqlCmd=CONCAT('UPDATE ',FROM_GT_DB,'.opt_inter_ifho SET BATCH=',GT_DB_START_HOUR,';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('INSERT INTO   ',GT_DB,'.opt_inter_ifho SELECT * FROM ',FROM_GT_DB,'.opt_inter_ifho;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	IF RUN_STATUS = 'rerun' THEN
		INSERT INTO gt_gw_main.sp_log VALUES(FROM_GT_DB,'SP_Insert_Into_Daily_DB','DELETE opt_inter_irat', NOW());
	
		SET @SqlCmd=CONCAT('ALTER TABLE ',GT_DB,'.opt_inter_irat TRUNCATE PARTITION b',GT_DB_START_HOUR,';');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;		
	END IF;	
	
	INSERT INTO gt_gw_main.sp_log VALUES(FROM_GT_DB,'SP_Insert_Into_Daily_DB','INSERT opt_inter_irat', NOW());
	
	SET @SqlCmd=CONCAT('UPDATE ',FROM_GT_DB,'.opt_inter_irat SET BATCH=',GT_DB_START_HOUR,';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('INSERT INTO   ',GT_DB,'.opt_inter_irat SELECT * FROM ',FROM_GT_DB,'.opt_inter_irat;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	IF RUN_STATUS = 'rerun' THEN
		INSERT INTO gt_gw_main.sp_log VALUES(FROM_GT_DB,'SP_Insert_Into_Daily_DB','DELETE opt_nbr_result', NOW());
	
		SET @SqlCmd=CONCAT('ALTER TABLE ',GT_DB,'.opt_nbr_result TRUNCATE PARTITION b',GT_DB_START_HOUR,';');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;		
	END IF;	
	
	INSERT INTO gt_gw_main.sp_log VALUES(FROM_GT_DB,'SP_Insert_Into_Daily_DB','INSERT opt_nbr_result', NOW());
	
	SET @SqlCmd=CONCAT('UPDATE ',FROM_GT_DB,'.opt_nbr_result SET BATCH=',GT_DB_START_HOUR,';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('INSERT INTO   ',GT_DB,'.opt_nbr_result SELECT * FROM ',FROM_GT_DB,'.opt_nbr_result;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	
	IF RUN_STATUS = 'rerun' THEN
		INSERT INTO gt_gw_main.sp_log VALUES(FROM_GT_DB,'SP_Insert_Into_Daily_DB','DELETE table_ranap_rab', NOW());
	
		SET @SqlCmd=CONCAT('ALTER TABLE ',GT_DB,'.table_ranap_rab TRUNCATE PARTITION b',GT_DB_START_HOUR,';');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;		
	END IF;	
	
	INSERT INTO gt_gw_main.sp_log VALUES(FROM_GT_DB,'SP_Insert_Into_Daily_DB','INSERT table_ranap_rab', NOW());
	
	SET @SqlCmd=CONCAT('UPDATE ',FROM_GT_DB,'.table_ranap_rab SET BATCH=',GT_DB_START_HOUR,';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('INSERT INTO   ',GT_DB,'.table_ranap_rab SELECT * FROM ',FROM_GT_DB,'.table_ranap_rab;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	INSERT INTO gt_gw_main.sp_log VALUES(FROM_GT_DB,'SP_Insert_Into_Daily_DB','INSERT table_pmnoloadsharingrrcconn', NOW());
	SET @SqlCmd=CONCAT('INSERT INTO   ',GT_DB,'.table_pmnoloadsharingrrcconn SELECT * FROM ',FROM_GT_DB,'.table_pmnoloadsharingrrcconn;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	INSERT INTO gt_gw_main.sp_log VALUES(FROM_GT_DB,'SP_Insert_Into_Daily_DB','INSERT table_pmnotimesrldelfractset', NOW());
	SET @SqlCmd=CONCAT('INSERT INTO   ',GT_DB,'.table_pmnotimesrldelfractset SELECT * FROM ',FROM_GT_DB,'.table_pmnotimesrldelfractset;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	INSERT INTO gt_gw_main.sp_log VALUES(FROM_GT_DB,'SP_Insert_Into_Daily_DB','INSERT table_pmnoattoutirathomulti', NOW());
	SET @SqlCmd=CONCAT('INSERT INTO   ',GT_DB,'.table_pmnoattoutirathomulti SELECT * FROM ',FROM_GT_DB,'.table_pmnoattoutirathomulti;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	INSERT INTO gt_gw_main.sp_log VALUES(FROM_GT_DB,'SP_Insert_Into_Daily_DB','INSERT table_pmcmattdlhls', NOW());
	SET @SqlCmd=CONCAT('INSERT INTO   ',GT_DB,'.table_pmcmattdlhls SELECT * FROM ',FROM_GT_DB,'.table_pmcmattdlhls;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
			
	SET @SqlCmd=CONCAT('INSERT INTO gt_gw_main.sp_log VALUES(''',FROM_GT_DB,''',''SP_Insert_Into_Daily_DB'',''INSERT ',GT_DB,'.`session_information`'', ''',NOW(),''');');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @FILE_STARTTIME=CONCAT(SUBSTRING(gt_strtok(FROM_GT_DB,3,'_'),1,4),'-',SUBSTRING(gt_strtok(FROM_GT_DB,3,'_'),5,2),'-',SUBSTRING(gt_strtok(FROM_GT_DB,3,'_'),7,2),' ',SUBSTRING(gt_strtok(FROM_GT_DB,4,'_'),1,2),':',SUBSTRING(gt_strtok(FROM_GT_DB,4,'_'),3,2),':00');
	SET @FILE_ENDTIME=CONCAT(SUBSTRING(gt_strtok(FROM_GT_DB,3,'_'),1,4),'-',SUBSTRING(gt_strtok(FROM_GT_DB,3,'_'),5,2),'-',SUBSTRING(gt_strtok(FROM_GT_DB,3,'_'),7,2),' ',SUBSTRING(gt_strtok(FROM_GT_DB,5,'_'),1,2),':',SUBSTRING(gt_strtok(FROM_GT_DB,5,'_'),3,2),':00');
	SET @SqlCmd=CONCAT('INSERT INTO ',GT_DB,'.session_information 
					    (`SESSION_ID`,
					     `SESSION_DB`,
					     `RNC`,
					     `FILE_STARTTIME`,
					     `FILE_ENDTIME`,
					     `IMPORT_TIME`,
					     `SESSION_TYPE`)
				VALUES (',CONNECTION_ID(),',
					''',FROM_GT_DB,''',
					''',RNC_ID,''',
					''',@FILE_STARTTIME,''',
					''',@FILE_ENDTIME,''',
					''',NOW(),''',
					''TEMP'');');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	CALL gt_gw_main.SP_Check_SysConfig('STEP2','',GT_DB,FROM_GT_DB);
	
	INSERT INTO gt_gw_main.SP_LOG VALUES(FROM_GT_DB,'SP_Insert_Into_Daily_DB',CONCAT('Done: ',TIMESTAMPDIFF(SECOND,START_TIME,SYSDATE()),' seconds.'), NOW());
	
