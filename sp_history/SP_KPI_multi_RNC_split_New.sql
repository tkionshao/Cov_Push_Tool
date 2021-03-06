DELIMITER $$
USE `gt_gw_main`$$
DROP PROCEDURE IF EXISTS `SP_CreateDB_LTE`$$
CREATE DEFINER=`covmo`@`%` PROCEDURE `SP_KPI_multi_RNC_split_New`(IN KPI_ID INT(11) ,IN PU VARCHAR(3000),IN START_DATE DATE,IN END_DATE DATE,IN START_HOUR TINYINT(2),IN END_HOUR TINYINT(2), IN SOURCE_TYPE TINYINT(2), IN SERVICE TINYINT(2)
							,IN DATA_QUARTER VARCHAR(10),IN CELL_ID VARCHAR(100),IN TILE_ID VARCHAR(100)
							,IN IMSI VARCHAR(4096),IN CLUSTER_ID VARCHAR(50),IN CALL_TYPE VARCHAR(50),IN CALL_STATUS VARCHAR(10),IN Mobility VARCHAR(10)
							,IN CELL_INDOOR VARCHAR(10),IN FREQUENCY VARCHAR(100) ,IN UARFCN VARCHAR(300),IN CELL_LON VARCHAR(50),IN CELL_LAT VARCHAR(50)
							,IN MSISDN VARCHAR(1024),IN IMEI_NEW VARCHAR(5000),IN APN VARCHAR(1024)
							,IN FILTER VARCHAR(1024),IN LIMITS VARCHAR(10),IN PID INT(11),IN SORT_STR VARCHAR(100),IN POS_KIND VARCHAR(10)
							,IN HAVING_STR VARCHAR(100),IN HIDE_IMEI_CNT TINYINT(2),IN SITE_ID VARCHAR(100)
							,IN MAKE_ID VARCHAR(1024),IN MODEL_ID VARCHAR(1024),IN POLYGON_STR VARCHAR(250),IN TECH_MASK TINYINT(4),IN WITHDUMP TINYINT(2),IN GT_COVMO VARCHAR(20),IN IMSI_GID SMALLINT(6),IN SPECIAL_IMSI TINYINT(2) ,IN SUB_REGION_ID VARCHAR(100),IN ENODEB_ID VARCHAR(100),IN CELL_GID INT(11))
a_label:
BEGIN	
	DECLARE UNION_STR TEXT DEFAULT '';
	DECLARE GT_DB VARCHAR(100) DEFAULT 'gt_temp_cache';
	DECLARE CCQ_TABLE VARCHAR(100) DEFAULT 'rpt_ccq';
	DECLARE WORKER_ID VARCHAR(10) DEFAULT CONNECTION_ID();
	DECLARE NT_DB VARCHAR(100);
	DECLARE v_i SMALLINT DEFAULT 1;	
	DECLARE SESSION_NAME VARCHAR(100);
	DECLARE v_R_Max SMALLINT;
	DECLARE SP_Process VARCHAR(100);
	DECLARE SP_Para_STR MEDIUMTEXT;
	DECLARE FILTER_STR VARCHAR(10000);
	DECLARE TARGET_TABLE VARCHAR(100) DEFAULT '';
	DECLARE SP_ERROR CONDITION FOR SQLSTATE '99996';
	DECLARE PU_ALL VARCHAR(65535) DEFAULT '';
	DECLARE DU_DATE VARCHAR(25);
	DECLARE IMSI_STR MEDIUMTEXT DEFAULT '';
	DECLARE IMSI_CONCAT MEDIUMTEXT DEFAULT '';
	DECLARE IMSI_PU SMALLINT DEFAULT NULL;
	DECLARE DS_AP_IP VARCHAR(20);
	DECLARE DS_AP_PORT VARCHAR(5);
	
	DECLARE EXIT HANDLER FOR 12701
	BEGIN
		SELECT 'Database server is currently busy, try again later.' AS IsSuccess;
	END;
	
	-- Incorrect key file for table
	DECLARE CONTINUE HANDLER FOR 1034
	BEGIN
		IF IMSI_PU IS NULL THEN
			SET IMSI_PU = -1;
			SET @table_imsi_pu='table_imsi_pu_ap2';
		ELSEIF IMSI_PU = -1 THEN
			SET @PU_CNT= 0;
		END IF;
--		SELECT '{tech:�ALL �, name:�SP-Report�, status:�2�,message_id: �null�, message: �SP_KPI_multi_RNC_split Failed Incorrect key file for table; try to repair it�, log_path: ��}' AS message;
	END;
	
	-- Table './xxxx/table name' is marked as crashed 
	DECLARE CONTINUE HANDLER FOR 145
	BEGIN
		IF IMSI_PU IS NULL THEN
			SET IMSI_PU = -1;
			SET @table_imsi_pu='table_imsi_pu_ap2';
		ELSEIF IMSI_PU = -1 THEN
			SET @PU_CNT= 0;
		END IF;
--		SELECT '{tech:�ALL �, name:�SP-Report�, status:�2�,message_id: �null�, message: �SP_KPI_multi_RNC_split Table was marked as crashed and should be repaired; try to repair it�, log_path: ��}' AS message;
	END;
	
	-- Table 'my table name' is marked as crashed 
	DECLARE CONTINUE HANDLER FOR 144
	BEGIN
		IF IMSI_PU IS NULL THEN
			SET IMSI_PU = -1;
			SET @table_imsi_pu='table_imsi_pu_ap2';
		ELSEIF IMSI_PU = -1 THEN
			SET @PU_CNT= 0;
		END IF;
--		SELECT '{tech:�ALL �, name:�SP-Report�, status:�2�,message_id: �null�, message: �SP_KPI_multi_RNC_split Table is crashed and last repair failed; try to repair it�, log_path: ��}' AS message;
	END;
		
	SET SESSION group_concat_max_len=@@max_allowed_packet;
	
	SELECT CONCAT('gt_nt_',DATE_FORMAT(END_DATE,'%Y%m%d')) INTO NT_DB;
	SET @table_imsi_pu='table_imsi_pu_ap1';
	INSERT INTO gt_gw_main.sp_log VALUES('gt_gw_main','SP_KPI_multi_RNC_split',CONCAT(KPI_ID,' Start'), NOW());
	SET @report_id=NULL;
	SET @column_crt_str=NULL;
	SET @column_col_str=NULL;
	SET @column_mrg_str=NULL;
	SET @sp_name=NULL;
	SET @SqlCmd=CONCAT('SELECT DISTINCT report_id,column_crt_str,column_col_str,column_mrg_str,sp_name,where_col,group_col
				INTO @report_id,@column_crt_str,@column_col_str,@column_mrg_str,@sp_name,@where_col,@group_col
				FROM gt_gw_main.tbl_rpt_other
				WHERE report_id=',KPI_ID,' AND service=',SERVICE,CASE WHEN KPI_ID IN (110006,110022) THEN CONCAT(' AND tech_mask=',TECH_MASK) ELSE '' END,';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	IF KPI_ID IN (110019) THEN
			SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS ',GT_DB,'.tmp_nt_cell_lte;');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
		
			SET @SqlCmd=CONCAT('CREATE TEMPORARY TABLE ',GT_DB,'.tmp_nt_cell_lte 
				SELECT    `ENODEB_ID`,
					  `CELL_ID`,
					  `CELL_NAME`,
					  `PCI`
					   FROM `',NT_DB,'`.`nt_cell_current_lte`;');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
			
			SET @SqlCmd=CONCAT('CREATE INDEX idx_cell ON ',GT_DB,'.tmp_nt_cell_lte(`ENODEB_ID`,`CELL_ID`);');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	END IF;	
	
	SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS ',GT_DB,'.tmp_imsi_pu_',WORKER_ID,';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('CREATE TEMPORARY TABLE ',GT_DB,'.tmp_imsi_pu_',WORKER_ID,' 
				(
				  `PU_DATE` varchar(50) NOT NULL,
				  `DATA_DATE` date NOT NULL,
				  `PU_ID` mediumint(9) NOT NULL DEFAULT ''0'',
				  `TECH_MASK` tinyint(2) NOT NULL,
				  `TECHNOLOGY` varchar(10) NOT NULL,
				   KEY (`PU_DATE`,`DATA_DATE`,`PU_ID`,`TECH_MASK`,`TECHNOLOGY`)
				)ENGINE=MYISAM;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;	
						
	SET @SqlCmd=CONCAT('SELECT REPLACE(gt_strtok(`value`,3,'':''),''/'','''') ,REPLACE(gt_strtok(`value`,4,'':''),''/'','''') INTO @AP_IP,@AP_PORT FROM `gt_gw_main`.`integration_param` WHERE gt_group=''system'' AND gt_name=''apDbUri'';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt; 
	
	SET @SqlCmd=CONCAT('SELECT `value` INTO @AP_USER FROM `gt_gw_main`.`integration_param` WHERE gt_group=''system'' AND gt_name=''apDbUser'';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt; 
	
	SET @SqlCmd=CONCAT('SELECT `value` INTO @AP_PSWD FROM `gt_gw_main`.`integration_param` WHERE gt_group=''system'' AND gt_name=''apDbPass'';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	IF IMSI_GID>0 OR IMSI<>'' THEN
		IF IMSI_GID>0 THEN
			SET @SqlCmd=CONCAT('SELECT GROUP_CONCAT(`IMSI`) into @IMSI_STR FROM `',GT_COVMO,'`.`dim_imsi` WHERE `GROUP_ID`=',IMSI_GID,';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
			SET IMSI_STR=@IMSI_STR;
		ELSE 
			SET IMSI_STR='';
		END IF;
		
		IF IMSI<>'' AND IMSI_STR<>'' THEN 
			SET IMSI_CONCAT=CONCAT('(',IMSI,',',IMSI_STR,')');
		ELSEIF IMSI_STR<>'' THEN 
			SET IMSI_CONCAT=CONCAT('(',IMSI_STR,')');
		ELSEIF IMSI<>'' THEN 
			SET IMSI_CONCAT=CONCAT('(',IMSI,')');
		END IF;	
		
		SET @IMSI_CNT=0;
		SET @IMSI_CNT=(CHAR_LENGTH(IMSI_CONCAT) - CHAR_LENGTH(REPLACE(IMSI_CONCAT,',','')))/(CHAR_LENGTH(','))+1;
		IF @IMSI_CNT>1000 THEN 
			SELECT 'IMSI list is over limitation!' AS NoSessionAvailable;
			LEAVE a_label;
		END IF;
	END IF;
	
	IF PU='' THEN 		
		IF KPI_ID IN (110001,110005,110020,110021,110026) THEN
			SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS ',GT_DB,'.tmp_dim_handset;');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
		
			SET @SqlCmd=CONCAT('CREATE TEMPORARY TABLE ',GT_DB,'.tmp_dim_handset AS
				SELECT A.`make_id`,B.`model_id`,A.`manufacturer`,B.`model` FROM  `',GT_COVMO,'`.`dim_handset_id` A ,`',GT_COVMO,'`.`dim_handset_m_id` B
				WHERE A.`make_id`=B.`make_id`;');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
			
			SET @SqlCmd=CONCAT('CREATE INDEX idx_make ON ',GT_DB,'.tmp_dim_handset(`make_id`,`model_id`,`manufacturer`,`model`);');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
		END IF;	
		IF IMSI_GID>0 OR IMSI<>'' THEN 	
			IF  IMSI_CONCAT <>'' THEN 
				SET @SqlCmd=CONCAT('INSERT INTO ',GT_DB,'.tmp_imsi_pu_',WORKER_ID,' SELECT DISTINCT CONCAT(''gt_'',`PU_ID`,''_'',DATE_FORMAT(`DATA_DATE`,''%Y%m%d''),''_0000_0000''),`DATA_DATE`,`PU_ID`,`TECH_MASK`,CASE `TECH_MASK` WHEN 1 THEN ''GSM'' WHEN 2 THEN ''UMTS'' WHEN 4 THEN ''LTE'' ELSE NULL END AS `TECHNOLOGY` FROM `gt_global_imsi`.',@table_imsi_pu,' WHERE `IMSI` IN ',IN_QUOTE(IMSI_CONCAT),' AND `DATA_DATE`>=''',START_DATE,''' AND `DATA_DATE`<''',DATE_ADD(END_DATE,INTERVAL 1 DAY),''' '
							,CASE TECH_MASK WHEN 1 THEN ' AND `TECH_MASK`=1' WHEN 2 THEN ' AND `TECH_MASK`=2' WHEN 3 THEN ' AND `TECH_MASK` IN (1,2)' 
									WHEN 4 THEN ' AND `TECH_MASK`=4' WHEN 5 THEN ' AND `TECH_MASK` IN (1,4)' WHEN 6 THEN ' AND `TECH_MASK` IN (2,4)' ELSE '' END ,';');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
			END IF;	
			IF IMSI_PU IS NULL THEN	
				SET @SqlCmd=CONCAT('SELECT COUNT(*) INTO @PU_CNT FROM ',GT_DB,'.tmp_imsi_pu_',WORKER_ID,';');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;	
			ELSEIF IMSI_PU = -1 THEN
				SET @PU_CNT =-1;
			END IF;	
			
			IF @PU_CNT>0 THEN 
				SET @SqlCmd=CONCAT('SELECT GROUP_CONCAT(DISTINCT CONCAT(A.`SESSION_DB`,''|'',B.`TECHNOLOGY`,''|'',REPLACE(gt_strtok(IFNULL(A.`DS_AP_URI`,B.`DS_AP_URI`),3,'':''),''/'',''''),''|'',REPLACE(gt_strtok(IFNULL(A.`DS_AP_URI`,B.`DS_AP_URI`),4,'':''),''/'','''')) SEPARATOR '','' ) INTO @PU_GC
							FROM `',GT_COVMO,'`.`session_information` A,`',GT_COVMO,'`.`rnc_information` B
							WHERE `SESSION_START`>=''',START_DATE,''' AND `SESSION_END` <''',DATE_ADD(END_DATE,INTERVAL 1 DAY),'''
							AND A.`RNC`=B.`RNC`',CASE TECH_MASK WHEN 1 THEN ' AND B.`TECHNOLOGY`=''GSM''' WHEN 2 THEN ' AND B.`TECHNOLOGY`=''UMTS''' WHEN 3 THEN ' AND B.`TECHNOLOGY` IN (''GSM'',''UMTS'')' 
							WHEN 4 THEN ' AND B.`TECHNOLOGY`=''LTE''' WHEN 5 THEN ' AND B.`TECHNOLOGY` IN (''GSM'',''LTE'')' WHEN 6 THEN ' AND B.`TECHNOLOGY` IN (''UMTS'',''LTE'')' ELSE '' END,'
							AND A.`TECHNOLOGY`=B.`TECHNOLOGY`
							AND EXISTS
							(SELECT NULL
							FROM ',GT_DB,'.TMP_IMSI_PU_',WORKER_ID,' C
							WHERE C.PU_DATE=A.`SESSION_DB` AND C.TECHNOLOGY=B.TECHNOLOGY);');
			ELSEIF @PU_CNT<0 THEN 
				SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS ',GT_DB,'.tmp_imsi_pu_bkp_',WORKER_ID,' ;');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
		
				SET @SqlCmd=CONCAT('CREATE TEMPORARY TABLE ',GT_DB,'.tmp_imsi_pu_bkp_',WORKER_ID,' 
							(
							  `PU_DATE` varchar(50) NOT NULL,
							  `DATA_DATE` date NOT NULL,
							  `PU_ID` mediumint(9) NOT NULL DEFAULT ''0'',
							  `TECH_MASK` tinyint(2) NOT NULL,
							  `TECHNOLOGY` varchar(10) NOT NULL,
							   KEY (`PU_DATE`,`DATA_DATE`,`PU_ID`,`TECH_MASK`,`TECHNOLOGY`)
							)ENGINE=MYISAM;');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
		
				SET @SqlCmd=CONCAT('INSERT INTO ',GT_DB,'.tmp_imsi_pu_bkp_',WORKER_ID,' SELECT DISTINCT CONCAT(''gt_'',`PU_ID`,''_'',DATE_FORMAT(`DATA_DATE`,''%Y%m%d''),''_0000_0000''),`DATA_DATE`,`PU_ID`,`TECH_MASK`,CASE `TECH_MASK` WHEN 1 THEN ''GSM'' WHEN 2 THEN ''UMTS'' WHEN 4 THEN ''LTE'' ELSE NULL END AS `TECHNOLOGY` FROM `gt_global_imsi`.',@table_imsi_pu,' WHERE `IMSI` IN ',IN_QUOTE(IMSI_CONCAT),' AND `DATA_DATE`>=''',START_DATE,''' AND `DATA_DATE`<''',DATE_ADD(END_DATE,INTERVAL 1 DAY),''' '
							,CASE TECH_MASK WHEN 1 THEN ' AND `TECH_MASK`=1' WHEN 2 THEN ' AND `TECH_MASK`=2' WHEN 3 THEN ' AND `TECH_MASK` IN (1,2)' 
								WHEN 4 THEN ' AND `TECH_MASK`=4' WHEN 5 THEN ' AND `TECH_MASK` IN (1,4)' WHEN 6 THEN ' AND `TECH_MASK` IN (2,4)' ELSE '' END  ,';');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
		
				IF @PU_CNT = -1 THEN 
					SET @SqlCmd=CONCAT('SELECT COUNT(*) INTO @PU_CNT2 FROM ',GT_DB,'.tmp_imsi_pu_bkp_',WORKER_ID,';');
					PREPARE Stmt FROM @SqlCmd;
					EXECUTE Stmt;
					DEALLOCATE PREPARE Stmt;
					
					IF @PU_CNT2>0 THEN	
						SET @SqlCmd=CONCAT('SELECT GROUP_CONCAT(DISTINCT CONCAT(A.`SESSION_DB`,''|'',B.`TECHNOLOGY`,''|'',REPLACE(gt_strtok(IFNULL(A.`DS_AP_URI`,B.`DS_AP_URI`),3,'':''),''/'',''''),''|'',REPLACE(gt_strtok(IFNULL(A.`DS_AP_URI`,B.`DS_AP_URI`),4,'':''),''/'','''')) SEPARATOR '','' ) INTO @PU_GC 
									FROM `',GT_COVMO,'`.`session_information` A,`',GT_COVMO,'`.`rnc_information` B
									WHERE `SESSION_START`>=''',START_DATE,''' AND `SESSION_END` <''',DATE_ADD(END_DATE,INTERVAL 1 DAY),'''
									AND A.`RNC`=B.`RNC`',CASE TECH_MASK WHEN 1 THEN ' AND B.`TECHNOLOGY`=''GSM''' WHEN 2 THEN ' AND B.`TECHNOLOGY`=''UMTS''' WHEN 3 THEN ' AND B.`TECHNOLOGY` IN (''GSM'',''UMTS'')' 
									WHEN 4 THEN ' AND B.`TECHNOLOGY`=''LTE''' WHEN 5 THEN ' AND B.`TECHNOLOGY` IN (''GSM'',''LTE'')' WHEN 6 THEN ' AND B.`TECHNOLOGY` IN (''UMTS'',''LTE'')' ELSE '' END,'
									AND A.`TECHNOLOGY`=B.`TECHNOLOGY`
									AND EXISTS
										(SELECT NULL
										FROM ',GT_DB,'.tmp_imsi_pu_bkp_',WORKER_ID,' C
										WHERE C.PU_DATE=A.`SESSION_DB` AND C.TECHNOLOGY=B.TECHNOLOGY);');
					ELSEIF @PU_CNT2=0 THEN
						SELECT 'IMSI can not be found!' AS NoSessionAvailable;
						LEAVE a_label;
					END IF;
				ELSEIF @PU_CNT = 0 THEN	
					SELECT 'IMSI can not be found!' AS NoSessionAvailable;
					LEAVE a_label;
				END IF;
			ELSE 	
				SELECT 'IMSI can not be found!' AS NoSessionAvailable;
				LEAVE a_label;
			END IF;
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
		ELSE 
			SET @SqlCmd=CONCAT('SELECT GROUP_CONCAT(DISTINCT CONCAT(A.`SESSION_DB`,''|'',B.`TECHNOLOGY`,''|'',REPLACE(gt_strtok(IFNULL(A.`DS_AP_URI`,B.`DS_AP_URI`),3,'':''),''/'',''''),''|'',REPLACE(gt_strtok(IFNULL(A.`DS_AP_URI`,B.`DS_AP_URI`),4,'':''),''/'','''')) SEPARATOR '','' ) INTO @PU_GC 
						FROM `',GT_COVMO,'`.`session_information` A,`',GT_COVMO,'`.`rnc_information` B
						WHERE `SESSION_START`>=''',START_DATE,''' AND `SESSION_END` <''',DATE_ADD(END_DATE,INTERVAL 1 DAY),'''
						AND A.`RNC`=B.`RNC`',CASE TECH_MASK WHEN 1 THEN ' AND B.`TECHNOLOGY`=''GSM''' WHEN 2 THEN ' AND B.`TECHNOLOGY`=''UMTS''' WHEN 3 THEN ' AND B.`TECHNOLOGY` IN (''GSM'',''UMTS'')' 
							WHEN 4 THEN ' AND B.`TECHNOLOGY`=''LTE''' WHEN 5 THEN ' AND B.`TECHNOLOGY` IN (''GSM'',''LTE'')' WHEN 6 THEN ' AND B.`TECHNOLOGY` IN (''UMTS'',''LTE'')' ELSE '' END,'
						AND A.`TECHNOLOGY`=B.`TECHNOLOGY`;');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
		END IF;		
	ELSE  
		IF IMSI_GID>0 OR IMSI<>'' THEN
			IF IMSI_CONCAT <>'' THEN 
				SET @SqlCmd=CONCAT('INSERT INTO ',GT_DB,'.tmp_imsi_pu_',WORKER_ID,' SELECT DISTINCT CONCAT(''gt_'',`PU_ID`,''_'',DATE_FORMAT(`DATA_DATE`,''%Y%m%d''),''_0000_0000''),`DATA_DATE`,`PU_ID`,`TECH_MASK`,CASE `TECH_MASK` WHEN 1 THEN ''GSM'' WHEN 2 THEN ''UMTS'' WHEN 4 THEN ''LTE'' ELSE NULL END AS `TECHNOLOGY` FROM `gt_global_imsi`.',@table_imsi_pu,' WHERE `IMSI` IN ',IN_QUOTE(IMSI_CONCAT),' AND `DATA_DATE`>=''',START_DATE,''' AND `DATA_DATE`<''',DATE_ADD(END_DATE,INTERVAL 1 DAY),''' '
							,CASE TECH_MASK WHEN 1 THEN ' AND `TECH_MASK`=1' WHEN 2 THEN ' AND `TECH_MASK`=2' WHEN 3 THEN ' AND `TECH_MASK` IN (1,2)' 
								WHEN 4 THEN ' AND `TECH_MASK`=4' WHEN 5 THEN ' AND `TECH_MASK` IN (1,4)' WHEN 6 THEN ' AND `TECH_MASK` IN (2,4)' ELSE '' END ,';');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
			END IF;	
		
			IF IMSI_PU IS NULL THEN	
				SET @SqlCmd=CONCAT('SELECT COUNT(*) INTO @PU_CNT FROM ',GT_DB,'.tmp_imsi_pu_',WORKER_ID,';');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;	
			ELSEIF IMSI_PU = -1 THEN
				SET @PU_CNT =-1;
			END IF;	
			
			IF @PU_CNT>0 THEN 
				SET @SqlCmd=CONCAT('SELECT GROUP_CONCAT(DISTINCT CONCAT(A.`SESSION_DB`,''|'',B.`TECHNOLOGY`,''|'',REPLACE(gt_strtok(IFNULL(A.`DS_AP_URI`,B.`DS_AP_URI`),3,'':''),''/'',''''),''|'',REPLACE(gt_strtok(IFNULL(A.`DS_AP_URI`,B.`DS_AP_URI`),4,'':''),''/'','''')) SEPARATOR '','' ) INTO @PU_GC 
							FROM `',GT_COVMO,'`.`session_information` A,`',GT_COVMO,'`.`rnc_information` B
							WHERE `SESSION_START`>=''',START_DATE,''' AND `SESSION_END` <''',DATE_ADD(END_DATE,INTERVAL 1 DAY),'''
							AND A.`RNC`',CASE WHEN gt_covmo_csv_count(PU,',')>1 THEN CONCAT(' IN (',PU,')') ELSE CONCAT('=',PU) END,'
							AND A.`RNC`=B.`RNC`',CASE TECH_MASK WHEN 1 THEN ' AND B.`TECHNOLOGY`=''GSM''' WHEN 2 THEN ' AND B.`TECHNOLOGY`=''UMTS''' WHEN 3 THEN ' AND B.`TECHNOLOGY` IN (''GSM'',''UMTS'')' 
							WHEN 4 THEN ' AND B.`TECHNOLOGY`=''LTE''' WHEN 5 THEN ' AND B.`TECHNOLOGY` IN (''GSM'',''LTE'')' WHEN 6 THEN ' AND B.`TECHNOLOGY` IN (''UMTS'',''LTE'')' ELSE '' END,' 
							AND A.`TECHNOLOGY`=B.`TECHNOLOGY`
							AND EXISTS
								(SELECT NULL
								FROM ',GT_DB,'.tmp_imsi_pu_',WORKER_ID,' C
								WHERE C.PU_DATE=A.`SESSION_DB` AND C.TECHNOLOGY=B.TECHNOLOGY);');
			ELSEIF @PU_CNT<0 THEN 
				SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS ',GT_DB,'.tmp_imsi_pu_bkp_',WORKER_ID,' ;');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
		
				SET @SqlCmd=CONCAT('CREATE TEMPORARY TABLE ',GT_DB,'.tmp_imsi_pu_bkp_',WORKER_ID,' 
							(
							  `PU_DATE` varchar(50) NOT NULL,
							  `DATA_DATE` date NOT NULL,
							  `PU_ID` mediumint(9) NOT NULL DEFAULT ''0'',
							  `TECH_MASK` tinyint(2) NOT NULL,
							  `TECHNOLOGY` varchar(10) NOT NULL,
							   KEY (`PU_DATE`,`DATA_DATE`,`PU_ID`,`TECH_MASK`,`TECHNOLOGY`)
							)ENGINE=MYISAM;');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				
				SET @SqlCmd=CONCAT('INSERT INTO ',GT_DB,'.tmp_imsi_pu_bkp_',WORKER_ID,' SELECT DISTINCT CONCAT(''gt_'',`PU_ID`,''_'',DATE_FORMAT(`DATA_DATE`,''%Y%m%d''),''_0000_0000''),`DATA_DATE`,`PU_ID`,`TECH_MASK`,CASE `TECH_MASK` WHEN 1 THEN ''GSM'' WHEN 2 THEN ''UMTS'' WHEN 4 THEN ''LTE'' ELSE NULL END AS `TECHNOLOGY` FROM `gt_global_imsi`.',@table_imsi_pu,' WHERE `IMSI` IN ',IN_QUOTE(IMSI_CONCAT),' AND `DATA_DATE`>=''',START_DATE,''' AND `DATA_DATE`<''',DATE_ADD(END_DATE,INTERVAL 1 DAY),''' '
							,CASE TECH_MASK WHEN 1 THEN ' AND `TECH_MASK`=1' WHEN 2 THEN ' AND `TECH_MASK`=2' WHEN 3 THEN ' AND `TECH_MASK` IN (1,2)' 
								WHEN 4 THEN ' AND `TECH_MASK`=4' WHEN 5 THEN ' AND `TECH_MASK` IN (1,4)' WHEN 6 THEN ' AND `TECH_MASK` IN (2,4)' ELSE '' END  ,';');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
		
				IF @PU_CNT = -1 THEN 
					SET @SqlCmd=CONCAT('SELECT COUNT(*) INTO @PU_CNT2 FROM ',GT_DB,'.tmp_imsi_pu_bkp_',WORKER_ID,';');
					PREPARE Stmt FROM @SqlCmd;
					EXECUTE Stmt;
					DEALLOCATE PREPARE Stmt;
					
					IF @PU_CNT2>0 THEN	
						SET @SqlCmd=CONCAT('SELECT GROUP_CONCAT(DISTINCT CONCAT(A.`SESSION_DB`,''|'',B.`TECHNOLOGY`,''|'',REPLACE(gt_strtok(IFNULL(A.`DS_AP_URI`,B.`DS_AP_URI`),3,'':''),''/'',''''),''|'',REPLACE(gt_strtok(IFNULL(A.`DS_AP_URI`,B.`DS_AP_URI`),4,'':''),''/'','''')) SEPARATOR '','' ) INTO @PU_GC 
									FROM `',GT_COVMO,'`.`session_information` A,`',GT_COVMO,'`.`rnc_information` B
									WHERE `SESSION_START`>=''',START_DATE,''' AND `SESSION_END` <''',DATE_ADD(END_DATE,INTERVAL 1 DAY),'''
									AND A.`RNC`',CASE WHEN gt_covmo_csv_count(PU,',')>1 THEN CONCAT(' IN (',PU,')') ELSE CONCAT('=',PU) END,'
									AND A.`RNC`=B.`RNC`',CASE TECH_MASK WHEN 1 THEN ' AND B.`TECHNOLOGY`=''GSM''' WHEN 2 THEN ' AND B.`TECHNOLOGY`=''UMTS''' WHEN 3 THEN ' AND B.`TECHNOLOGY` IN (''GSM'',''UMTS'')' 
							WHEN 4 THEN ' AND B.`TECHNOLOGY`=''LTE''' WHEN 5 THEN ' AND B.`TECHNOLOGY` IN (''GSM'',''LTE'')' WHEN 6 THEN ' AND B.`TECHNOLOGY` IN (''UMTS'',''LTE'')' ELSE '' END,' 
									AND A.`TECHNOLOGY`=B.`TECHNOLOGY`
									AND EXISTS
										(SELECT NULL
										FROM ',GT_DB,'.tmp_imsi_pu_bkp_',WORKER_ID,' C
										WHERE C.PU_DATE=A.`SESSION_DB` AND C.TECHNOLOGY=B.TECHNOLOGY);');
					ELSEIF @PU_CNT2=0 THEN
						SELECT 'IMSI can not be found!' AS NoSessionAvailable;
						LEAVE a_label;	
					END IF;
				ELSEIF @PU_CNT = 0 THEN	
					SELECT 'IMSI can not be found!' AS NoSessionAvailable;
					LEAVE a_label;	
				END IF;
			ELSE 	
				SELECT 'IMSI can not be found!' AS NoSessionAvailable;
				LEAVE a_label;
			END IF;	
		ELSE 
			SET @SqlCmd=CONCAT('SELECT GROUP_CONCAT(DISTINCT CONCAT(A.`SESSION_DB`,''|'',B.`TECHNOLOGY`,''|'',REPLACE(gt_strtok(IFNULL(A.`DS_AP_URI`,B.`DS_AP_URI`),3,'':''),''/'',''''),''|'',REPLACE(gt_strtok(IFNULL(A.`DS_AP_URI`,B.`DS_AP_URI`),4,'':''),''/'','''')) SEPARATOR '','' ) INTO @PU_GC 
					FROM `',GT_COVMO,'`.`session_information` A,`',GT_COVMO,'`.`rnc_information` B
					WHERE `SESSION_START`>=''',START_DATE,''' AND `SESSION_END` <''',DATE_ADD(END_DATE,INTERVAL 1 DAY),'''
					AND A.`RNC`=B.`RNC`',CASE TECH_MASK WHEN 1 THEN ' AND B.`TECHNOLOGY`=''GSM''' WHEN 2 THEN ' AND B.`TECHNOLOGY`=''UMTS''' WHEN 3 THEN ' AND B.`TECHNOLOGY` IN (''GSM'',''UMTS'')' 
							WHEN 4 THEN ' AND B.`TECHNOLOGY`=''LTE''' WHEN 5 THEN ' AND B.`TECHNOLOGY` IN (''GSM'',''LTE'')' WHEN 6 THEN ' AND B.`TECHNOLOGY` IN (''UMTS'',''LTE'')' ELSE '' END,' 
					AND A.`RNC`',CASE WHEN gt_covmo_csv_count(PU,',')>1 THEN CONCAT(' IN (',PU,')') ELSE CONCAT('=',PU) END,'
					AND A.`TECHNOLOGY`=B.`TECHNOLOGY`;');
		END IF;
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
				
	END IF;
	SET PU_ALL=IFNULL(@PU_GC,'');
	
	IF PU_ALL='' THEN 
		SELECT 'No Data available!' AS NoSessionAvailable;
		LEAVE a_label;
	ELSE 
		SET SP_Para_STR = CONCAT(SOURCE_TYPE,',',SERVICE,',',WORKER_ID,','
					,CASE WHEN DATA_QUARTER='' THEN '''''' ELSE DATA_QUARTER END,','
					,CASE WHEN CELL_ID='' THEN '''''' ELSE CONCAT('''',CELL_ID,'''') END,','
					,CASE WHEN TILE_ID='' THEN '''''' ELSE CONCAT('''',TILE_ID,'''') END,','
					,CASE WHEN IMSI='' AND IMSI_CONCAT='' THEN '''''' ELSE CONCAT('''',IMSI_CONCAT,'''') END,','
					,CASE WHEN CLUSTER_ID='' THEN '''''' ELSE CLUSTER_ID END,','
					,CASE WHEN CALL_TYPE='' THEN '''''' ELSE CONCAT('''',CALL_TYPE,'''') END,','
					,CASE WHEN CALL_STATUS='' THEN '''''' ELSE CONCAT('''',CALL_STATUS,'''') END,','
					,IFNULL(@TMP_INDOOR,''''''),','
					,IFNULL(@TMP_MOVING,''''''),','
					,CASE WHEN CELL_INDOOR='' THEN '''''' ELSE CELL_INDOOR END,','
					,CASE WHEN FREQUENCY='' THEN '''''' ELSE CONCAT('''',FREQUENCY,'''') END,','
					,CASE WHEN UARFCN='' THEN '''''' ELSE CONCAT('''',UARFCN,'''') END,','
					,CASE WHEN CELL_LON='' THEN '''''' ELSE CELL_LON END,','
					,CASE WHEN CELL_LAT='' THEN '''''' ELSE CELL_LAT END,','
					,CASE WHEN MSISDN='' THEN '''''' ELSE CONCAT('''',MSISDN,'''') END,','
					,CASE WHEN IMEI_NEW='' THEN '''''' ELSE CONCAT('''',IMEI_NEW,'''') END,','
					,CASE WHEN APN='' THEN '''''' ELSE CONCAT('''',APN,'''') END,','
					,CASE WHEN FILTER='' THEN '''''' ELSE CONCAT('''',REPLACE(FILTER,'''',''''''''''),'''') END,','
					,CASE WHEN PID='' THEN '''''' ELSE PID END,','
					,CASE WHEN POS_KIND='' THEN '''''' ELSE CONCAT('''',POS_KIND,'''') END,','
					,CASE WHEN SITE_ID='' THEN '''''' ELSE CONCAT('''',SITE_ID,'''') END,','
					,CASE WHEN MAKE_ID='' THEN '''''' ELSE CONCAT('''',MAKE_ID,'''') END,','
					,CASE WHEN MODEL_ID='' THEN '''''' ELSE CONCAT('''',MODEL_ID,'''') END,','
					,CASE WHEN POLYGON_STR='' THEN '''''' ELSE CONCAT('''',POLYGON_STR,'''') END,','
					,CASE WHEN WITHDUMP='' THEN '''0''' ELSE WITHDUMP END,','
					,CASE WHEN SPECIAL_IMSI='' THEN '''0''' ELSE SPECIAL_IMSI END,','
					,CASE WHEN SUB_REGION_ID='' THEN '''''' ELSE CONCAT('''',SUB_REGION_ID,'''') END,','
					,CASE WHEN ENODEB_ID='' THEN '''''' ELSE CONCAT('''',ENODEB_ID,'''') END,','
					,CASE WHEN CELL_GID='' THEN '''''' ELSE CONCAT('''',CELL_GID,'''') END);
		SET CCQ_TABLE:=CONCAT(CCQ_TABLE,'_',WORKER_ID);
		SET @SqlCmd=CONCAT('CREATE DATABASE IF NOT EXISTS ',GT_DB);    
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;  
	
		CALL gt_schedule.sp_job_create('SP_KPI_multi_RNC',GT_DB);
		SET @V_Multi_PU = @JOB_ID;	
		SET @v_i=1;
		SET @Quotient_v=1;
		SET @v_R_Max=(CHAR_LENGTH(@PU_GC) - CHAR_LENGTH(REPLACE(@PU_GC,',','')))/(CHAR_LENGTH(','))+1;
		SET @v_START_HOUR=START_HOUR;
		SET @v_END_HOUR=END_HOUR;	
		WHILE @v_i <= @v_R_Max DO
		BEGIN
			SET SESSION_NAME = gt_strtok(SPLIT_STR(PU_ALL,',',@v_i),1,'|');
			SET TARGET_TABLE=CONCAT(GT_DB,'.',CCQ_TABLE,'_',REPLACE(SESSION_NAME,'_0000_0000',''),'_',gt_strtok(SPLIT_STR(PU_ALL,',',@v_i),2,'|'));
			SET DS_AP_IP= gt_strtok(SPLIT_STR(PU_ALL,',',@v_i),3,'|');
			SET DS_AP_PORT= gt_strtok(SPLIT_STR(PU_ALL,',',@v_i),4,'|');
			
			SET @SqlCmd=CONCAT('DROP TABLE IF EXISTS ',TARGET_TABLE,';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
			
			SET @SqlCmd=CONCAT('CREATE TABLE ',TARGET_TABLE,'(',@column_crt_str,')',' ENGINE=MyIsam DEFAULT CHARSET=utf8;');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
			
			CALL gt_schedule.sp_job_add_task_upd(CONCAT('CALL gt_gw_main.SP_KPI_multi_split(',KPI_ID,',''',SESSION_NAME,''',',@v_START_HOUR,',',@v_END_HOUR,','
										,SP_Para_STR 
										,',''',GT_COVMO,''',''',gt_strtok(SPLIT_STR(PU_ALL,',',@v_i),2,'|'),''',''',GT_DB,''',''',TARGET_TABLE,''',''',@column_crt_str,''',''',@column_col_str,''',''',@sp_name,''',''',IMSI_GID,''',''',DS_AP_IP,''',''',DS_AP_PORT,''',''',@AP_USER,''',''',@AP_PSWD,''');'),@V_Multi_PU);
			IF (@v_i=1) THEN 
				SET UNION_STR:=CONCAT(TARGET_TABLE,',');
			ELSE 
				SET UNION_STR:=CONCAT(UNION_STR,TARGET_TABLE,',');
			END IF; 
			SET @v_i=@v_i+@Quotient_v;
		END;
		END WHILE;
		SET UNION_STR =LEFT(UNION_STR,LENGTH(UNION_STR)-1);
 		CALL gt_schedule.sp_job_upd(@V_Multi_PU);
		CALL gt_schedule.sp_job_start(@V_Multi_PU);
		CALL gt_schedule.sp_job_enable_event_200();
		CALL gt_schedule.sp_job_wait(@V_Multi_PU);
		CALL gt_schedule.sp_job_disable_event_200();
	 
		SET @SqlCmd=CONCAT('SELECT `STATUS` INTO @JOB_STATUS FROM `gt_schedule`.`job_history` WHERE ID=',@V_Multi_PU,';');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		
		IF @JOB_STATUS='FINISHED' THEN 
			SET @SqlCmd=CONCAT('DROP TABLE IF EXISTS ',GT_DB,'.',CCQ_TABLE,';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
			
			SET @SqlCmd=CONCAT('CREATE TABLE ',GT_DB,'.',CCQ_TABLE,'(',@column_crt_str,') ENGINE=MRG_MYISAM DEFAULT CHARSET=utf8 INSERT_METHOD=FIRST UNION=(',UNION_STR,')');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt; 
			SET @column_mrg_str=REPLACE(@column_mrg_str,'HIDE_IMEI_CNT',HIDE_IMEI_CNT);
			IF @group_col IS NOT NULL AND KPI_ID<>110020 THEN 
				SET DU_DATE=CASE WHEN START_DATE=END_DATE THEN CONCAT('''',START_DATE,'''') ELSE CONCAT('''',START_DATE,'~',END_DATE,'''') END;	
				SET @column_mrg_str=REPLACE(@column_mrg_str,'DU_DATE',DU_DATE);
			ELSE 
				SET @column_mrg_str=REPLACE(@column_mrg_str,'DU_DATE','DS_DATE');
			END IF;
				
			SET @SqlCmd=CONCAT('SELECT SQL_CALC_FOUND_ROWS ',@column_mrg_str,' FROM ',GT_DB,'.',CCQ_TABLE
						,CASE WHEN KPI_ID IN (110005,110020,110021,110026) THEN CONCAT(' A LEFT JOIN ',GT_DB,'.tmp_dim_handset B ON gt_strtok(A.HANDSET,1,''|'')=B.MAKE_ID AND gt_strtok(A.HANDSET,2,''|'')=B.MODEL_ID ') 
							WHEN KPI_ID IN (110001) THEN CONCAT(' A LEFT JOIN ',GT_DB,'.tmp_dim_handset B ON A.MAKE_ID=B.MAKE_ID AND A.MODEL_ID=B.MODEL_ID ') ELSE '' END
						,CASE WHEN KPI_ID IN (110019) THEN CONCAT(' A LEFT JOIN ',GT_DB,'.tmp_nt_cell_lte B ON A.ENODEB_ID=B.ENODEB_ID AND A.CELL_ID=B.CELL_ID ') ELSE '' END
  						,CASE WHEN @where_col IS NULL THEN '' ELSE CONCAT(' WHERE ',@where_col) END
  						,CASE WHEN @group_col IS NULL THEN '' ELSE CONCAT(' GROUP BY ',@group_col) END
						,CASE WHEN SORT_STR='' THEN '' ELSE CONCAT(' ORDER BY ',SORT_STR) END
						,CASE WHEN LIMITS='' THEN '' ELSE CONCAT(' LIMIT ',LIMITS) END,';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
		
			SELECT FOUND_ROWS() INTO @V_CNT;
			
			SET @SqlCmd=CONCAT('REPLACE INTO `',GT_COVMO,'`.`tbl_qry_totalcount`(`WORK_ID`,`QRY_TIME`,`TOTAL_CNT`)
						SELECT ',PID,' AS `WORK_ID`,''',NOW(),''' AS `QRY_TIME`,',@V_CNT,' AS `TOTAL_CNT`;');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
			
			SET @SqlCmd=CONCAT('DROP TABLE IF EXISTS ',GT_DB,'.',CCQ_TABLE,';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
			
			SET @v_i=1;	
				
			WHILE @v_i <= @v_R_Max DO
				BEGIN
					SET SESSION_NAME = gt_strtok(SPLIT_STR(PU_ALL,',',@v_i),1,'|');
					SET TARGET_TABLE=CONCAT(GT_DB,'.',CCQ_TABLE,'_',REPLACE(SESSION_NAME,'_0000_0000',''),'_',gt_strtok(SPLIT_STR(PU_ALL,',',@v_i),2,'|'));
			
					SET @SqlCmd=CONCAT('DROP TABLE IF EXISTS ',TARGET_TABLE,';');
					PREPARE Stmt FROM @SqlCmd;
					EXECUTE Stmt;
					DEALLOCATE PREPARE Stmt;
					SET @v_i=@v_i+@Quotient_v;
				END;
			END WHILE;
		ELSE
			SET @v_i=1;	
			WHILE @v_i <= @v_R_Max DO
				BEGIN
					SET SESSION_NAME = gt_strtok(SPLIT_STR(PU_ALL,',',@v_i),1,'|');
					SET TARGET_TABLE=CONCAT(GT_DB,'.',CCQ_TABLE,'_',REPLACE(SESSION_NAME,'_0000_0000',''),'_',gt_strtok(SPLIT_STR(PU_ALL,',',@v_i),2,'|'));
					SET @SqlCmd=CONCAT('DROP TABLE IF EXISTS ',TARGET_TABLE,';');
					PREPARE Stmt FROM @SqlCmd;
					EXECUTE Stmt;
					DEALLOCATE PREPARE Stmt;
					SET @v_i=@v_i+@Quotient_v;
				END;
			END WHILE;
			
			INSERT INTO `gt_gw_main`.`tbl_rpt_error` (`PID`,`CreateTime`,`error_str`) VALUES (PID,NOW(),CONCAT(PID,' Main Parallel Jobs Fail - SP_CovMo_Cross_Query'));
			SIGNAL SP_ERROR
				SET MESSAGE_TEXT = 'Main Parallel Jobs Fail - SP_KPI_multi_RNC_split';
		END IF;
	END IF;	
	
	INSERT INTO gt_gw_main.sp_log VALUES('gt_gw_main','SP_KPI_multi_RNC_split',CONCAT(KPI_ID,' Done'), NOW());
END$$
DELIMITER ;
