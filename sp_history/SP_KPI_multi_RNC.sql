DELIMITER $$
USE `gt_gw_main`$$
DROP PROCEDURE IF EXISTS `SP_CreateDB_LTE`$$
CREATE DEFINER=`covmo`@`%` PROCEDURE `SP_KPI_multi_RNC`(IN KPI_ID INT(11) ,IN PU VARCHAR(100),IN START_DATE DATE,IN END_DATE DATE,IN START_HOUR TINYINT(2),IN END_HOUR TINYINT(2), IN SOURCE_TYPE TINYINT(2), IN SERVICE TINYINT(2)
							,IN DATA_QUARTER VARCHAR(10),IN CELL_ID VARCHAR(100),IN TILE_ID VARCHAR(100)
							,IN IMSI VARCHAR(4096),IN CLUSTER_ID VARCHAR(50),IN CALL_TYPE VARCHAR(30),IN CALL_STATUS VARCHAR(10),IN Mobility VARCHAR(10)
							,IN CELL_INDOOR VARCHAR(10),IN FREQUENCY VARCHAR(100) ,IN UARFCN VARCHAR(100),IN CELL_LON VARCHAR(50),IN CELL_LAT VARCHAR(50)
							,IN MSISDN VARCHAR(1024),IN IMEI_NEW VARCHAR(5000),IN APN VARCHAR(1024)
							,IN FILTER VARCHAR(1024),IN LIMITS VARCHAR(10),IN PID INT(11),IN SORT_STR VARCHAR(100),IN POS_KIND VARCHAR(10)
							,IN HAVING_STR VARCHAR(100),IN HIDE_IMEI_CNT TINYINT(2),IN SITE_ID VARCHAR(100)
							,IN MAKE_ID VARCHAR(1024),IN MODEL_ID VARCHAR(1024),IN POLYGON_STR VARCHAR(250),IN TECH_MASK TINYINT(4),IN WITHDUMP TINYINT(2),IN GT_COVMO VARCHAR(20) )
a_label:
BEGIN	
	DECLARE done INT DEFAULT 0;  
	DECLARE UNION_STR VARCHAR(20000) DEFAULT '';
	DECLARE GT_DB VARCHAR(100) DEFAULT 'gt_temp_cache';
	DECLARE V_SESSIONDB VARCHAR(100);
	DECLARE CCQ_TABLE VARCHAR(100) DEFAULT 'rpt_ccq';
	DECLARE WORKER_ID VARCHAR(10) DEFAULT CONNECTION_ID();
	DECLARE i INT DEFAULT 0;
	DECLARE v_1 INT DEFAULT 0;
	
	DECLARE v_i SMALLINT DEFAULT 1;	
	DECLARE j_i SMALLINT DEFAULT 1;	
	DECLARE SESSION_NAME VARCHAR(100);
	DECLARE v_R_Max SMALLINT;	
	DECLARE v_T_Max SMALLINT;	
 	DECLARE START_DATE_J DATETIME;
	DECLARE V_Multi_PU INT;
	DECLARE v_START_HOUR TINYINT(2) DEFAULT 0;
	DECLARE v_END_HOUR TINYINT(2) DEFAULT 0;
	DECLARE SP_Process VARCHAR(100);
	DECLARE SP_Para_STR VARCHAR(10000);
	DECLARE HASH_STR VARCHAR(32) DEFAULT '';
	DECLARE FILTER_STR VARCHAR(10000);
	DECLARE START_TIME DATETIME DEFAULT SYSDATE();
	DECLARE TARGET_TABLE VARCHAR(100) DEFAULT '';
	DECLARE SP_ERROR CONDITION FOR SQLSTATE '99996';
	DECLARE KPI_ERROR CONDITION FOR SQLSTATE '99999';
	DECLARE PU_ALL VARCHAR(1000) DEFAULT '';
	DECLARE ALLPU TINYINT(2) DEFAULT 0;
	DECLARE COLUMN_CRT_STR VARCHAR(1500) DEFAULT 
		' (CALL_ID BIGINT(20) DEFAULT NULL,
		RAB_SEQ_ID TINYINT(2) DEFAULT NULL,
		START_TIME DATETIME DEFAULT NULL,
		END_TIME DATETIME DEFAULT NULL,
		DURATION INT(11) DEFAULT NULL,
		IMSI CHAR(16) DEFAULT NULL,
		MSISDN VARCHAR(20) DEFAULT NULL,
		B_PARTY_NUMBER VARCHAR(15) DEFAULT NULL,
		POS_FIRST_CELL VARCHAR(20) DEFAULT NULL,
		POS_FIRST_LOC BIGINT(20) DEFAULT NULL,
		POS_LAST_CELL VARCHAR(20) DEFAULT NULL,
		POS_LAST_LOC BIGINT(20) DEFAULT NULL,
		START_CELL_ID VARCHAR(50) DEFAULT NULL,
		END_CELL_ID VARCHAR(50) DEFAULT NULL,
		CALL_TYPE VARCHAR(15) DEFAULT NULL,
		CALL_STATUS VARCHAR(11) DEFAULT NULL,
		RELEASE_CAUSE_STR VARCHAR(100) DEFAULT NULL,
		UL_VOLUME DOUBLE DEFAULT NULL,
		DL_VOLUME DOUBLE DEFAULT NULL,
		UL_THROUPUT DOUBLE DEFAULT NULL,
		DL_THROUPUT DOUBLE DEFAULT NULL,
		MANUFACTURER VARCHAR(32) DEFAULT NULL,
		MODEL VARCHAR(200) DEFAULT NULL,
		DS_DATE DATE DEFAULT NULL,
		PU SMALLINT(6) DEFAULT NULL,
		TECH_MASK TINYINT(4) DEFAULT NULL,
		POS_CONFIDENCE tinyint(4) DEFAULT NULL) ';
	
	DECLARE COLUMN_COL_STR VARCHAR(500) DEFAULT 
		'CALL_ID,
		RAB_SEQ_ID,
		START_TIME,
		END_TIME,
		DURATION,
		IMSI,
		MSISDN,
		B_PARTY_NUMBER,
		POS_FIRST_CELL,
		POS_FIRST_LOC,
		POS_LAST_CELL,
		POS_LAST_LOC,
		START_CELL_ID,
		END_CELL_ID,
		CALL_TYPE,
		CALL_STATUS,
		RELEASE_CAUSE_STR,
		UL_VOLUME,
		DL_VOLUME,
		UL_THROUPUT,
		DL_THROUPUT,
		MANUFACTURER,
		MODEL,
		DS_DATE,
		PU,
		TECH_MASK,
		POS_CONFIDENCE';
	DECLARE COLUMN_MRG_STR VARCHAR(1000) DEFAULT CONCAT('CALL_ID AS col_1,RAB_SEQ_ID AS col_2,START_TIME AS col_3,END_TIME AS col_4,DURATION AS col_5,IF(LENGTH(IMSI) > 0,CONCAT(LEFT(IMSI, LENGTH(IMSI) - ',IFNULL(HIDE_IMEI_CNT,0),'),REPEAT(''*'', ',IFNULL(HIDE_IMEI_CNT,0),')),'''') AS col_6,MSISDN AS col_7,B_PARTY_NUMBER AS col_8,POS_FIRST_CELL AS col_9,gt_covmo_proj_geohash_to_lat(POS_FIRST_LOC) AS col_10,gt_covmo_proj_geohash_to_lng(POS_FIRST_LOC) AS col_11,POS_LAST_CELL AS col_12,gt_covmo_proj_geohash_to_lat(POS_LAST_LOC) AS col_13,gt_covmo_proj_geohash_to_lng(POS_LAST_LOC) AS col_14,START_CELL_ID AS col_15,END_CELL_ID AS col_16,call_type AS col_17,call_status AS col_18,RELEASE_CAUSE_STR AS col_19,UL_VOLUME AS col_20,DL_VOLUME AS col_21,UL_THROUPUT AS col_22,DL_THROUPUT AS col_23,MANUFACTURER AS col_24,MODEL AS col_25,CALL_ID AS id,gt_covmo_proj_geohash_to_lng(POS_LAST_LOC) AS longitude,gt_covmo_proj_geohash_to_lat(POS_LAST_LOC) AS latitude,1000 AS height,pu AS pu,ds_date AS ds_date,TECH_MASK AS TECH_MASK,POS_CONFIDENCE AS POS_CONFIDENCE ');
	
	DECLARE EXIT HANDLER FOR 1064 
	BEGIN 	
		SET @v_i=1;	
		SET @j_i=1;
			
		WHILE @v_i <= @v_R_Max DO
			BEGIN
				WHILE @j_i <= @v_T_Max DO
					BEGIN
						SET SESSION_NAME = CONCAT('gt_',gt_strtok(gt_covmo_csv_get(PU_ALL,@v_i),1,'|'),'_',DATE_FORMAT(@START_DATE_J,'%Y%m%d'),'_0000_0000');
						SET @START_DATE_J=DATE_ADD(@START_DATE_J,INTERVAL 1 DAY);
						
						SET @SqlCmd=CONCAT('SELECT COUNT(*) INTO @SESSION_CNT FROM `gt_covmo`.`session_information` WHERE `SESSION_DB` = ''', SESSION_NAME,''' AND `TECHNOLOGY`=''',gt_strtok(gt_covmo_csv_get(PU_ALL,@v_i),2,'|'),''';');
						PREPARE Stmt FROM @SqlCmd;
						EXECUTE Stmt;
						DEALLOCATE PREPARE Stmt;
						
						IF @SESSION_CNT>0 THEN 
							SET TARGET_TABLE=CONCAT(GT_DB,'.',CCQ_TABLE,'_',REPLACE(SESSION_NAME,'_0000_0000',''),'_',gt_strtok(gt_covmo_csv_get(PU_ALL,@v_i),2,'|'));
							SET @SqlCmd=CONCAT('DROP TABLE IF EXISTS ',TARGET_TABLE,';');
							PREPARE Stmt FROM @SqlCmd;
							EXECUTE Stmt;
							DEALLOCATE PREPARE Stmt;
						END IF;
						SET @j_i=@j_i+@Quotient_j;
					END;
				END WHILE;			
				SET @START_DATE_J=START_DATE;
				SET @Quotient_j=1;
				SET @j_i=1;
				SET @v_i=@v_i+@Quotient_v;
			END;
		END WHILE;
		SELECT 'Error' AS IsSuccess; 
	END;	
	INSERT INTO gt_gw_main.sp_log VALUES('gt_gw_main','SP_KPI_multi_PU',CONCAT(KPI_ID,' Start'), NOW());
					
	IF PU='' THEN 		
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
		
		SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS tmp_imsi_pu_',WORKER_ID,' ;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
	
		SET @SqlCmd=CONCAT('CREATE TEMPORARY TABLE tmp_imsi_pu_',WORKER_ID,' 
					(
					  `DATA_DATE` date NOT NULL,
					  `PU_ID` mediumint(9) NOT NULL DEFAULT ''0'',
					  `TECH_MASK` tinyint(2) NOT NULL,
					  `TECHNOLOGY` varchar(10) NOT NULL,
					   KEY (`DATA_DATE`,`PU_ID`,`TECH_MASK`,`TECHNOLOGY`)
					)ENGINE=MYISAM;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		
		SET @SqlCmd=CONCAT('SELECT spider_bg_direct_sql(CONCAT('' SELECT DISTINCT `DATA_DATE`,`PU_ID`,`TECH_MASK`,CASE `TECH_MASK` WHEN 1 THEN ''''GSM'''' WHEN 2 THEN ''''UMTS'''' WHEN 4 THEN ''''LTE'''' ELSE NULL END AS `TECHNOLOGY` FROM `gt_global_imsi`.`table_imsi_pu` WHERE `IMSI` IN ',REPLACE(IN_QUOTE(IMSI),"'","''"),' AND `DATA_DATE`>=''''',START_DATE,''''' AND `DATA_DATE`<''''',DATE_ADD(END_DATE,INTERVAL 1 DAY),''''' '
					,CASE TECH_MASK WHEN 0 THEN '' ELSE CONCAT(' AND `TECH_MASK`=',TECH_MASK) END ,';'') 
					, ''tmp_imsi_pu_',WORKER_ID,'''
					, CONCAT(''HOST ''''',@AP_IP,''''', PORT ''''',@AP_PORT,''''',USER ''''',@AP_USER,''''', PASSWORD ''''',@AP_PSWD,''''''')
					) INTO @bb
					;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		SET @SqlCmd=CONCAT('SELECT COUNT(*) INTO @PU_CNT FROM tmp_imsi_pu_',WORKER_ID,';');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		IF @PU_CNT>0 THEN 
			SET @SqlCmd=CONCAT('SELECT GROUP_CONCAT(DISTINCT CONCAT(A.RNC,''|'',B.`TECHNOLOGY`) SEPARATOR '','' ) INTO @PU_GC 
						FROM `',GT_COVMO,'`.`session_information` A,`',GT_COVMO,'`.`rnc_information` B
						WHERE `SESSION_START`>=''',START_DATE,''' AND `SESSION_END` <''',DATE_ADD(END_DATE,INTERVAL 1 DAY),'''
						AND A.`RNC`=B.`RNC`',CASE TECH_MASK WHEN 1 THEN ' AND B.`TECHNOLOGY`=''GSM''' WHEN 2 THEN ' AND B.`TECHNOLOGY`=''UMTS''' WHEN 4 THEN ' AND B.`TECHNOLOGY`=''LTE''' ELSE '' END  ,'
						AND EXISTS
						(SELECT NULL
						FROM TMP_IMSI_PU_',WORKER_ID,' C
						WHERE C.PU_ID=B.RNC AND C.TECHNOLOGY=B.TECHNOLOGY);');
		ELSE 	
			SET @SqlCmd=CONCAT('SELECT GROUP_CONCAT(DISTINCT CONCAT(A.RNC,''|'',B.`TECHNOLOGY`) SEPARATOR '','' ) INTO @PU_GC 
						FROM `',GT_COVMO,'`.`session_information` A,`',GT_COVMO,'`.`rnc_information` B
						WHERE `SESSION_START`>=''',START_DATE,''' AND `SESSION_END` <''',DATE_ADD(END_DATE,INTERVAL 1 DAY),'''
						AND A.`RNC`=B.`RNC`',CASE TECH_MASK WHEN 1 THEN ' AND B.`TECHNOLOGY`=''GSM''' WHEN 2 THEN ' AND B.`TECHNOLOGY`=''UMTS''' WHEN 4 THEN ' AND B.`TECHNOLOGY`=''LTE''' ELSE '' END  ,';');
		END IF;
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		
		SET PU_ALL=IFNULL(@PU_GC,'');
		SET ALLPU=1;
	ELSE  
		SET @SqlCmd=CONCAT('SELECT GROUP_CONCAT(DISTINCT CONCAT(A.RNC,''|'',B.`TECHNOLOGY`) SEPARATOR '','' ) INTO @PU_GC 
					FROM `gt_covmo`.`session_information` A,`gt_covmo`.`rnc_information` B
					WHERE `SESSION_START`>=''',START_DATE,''' AND `SESSION_END` <''',DATE_ADD(END_DATE,INTERVAL 1 DAY),'''
					AND A.`RNC`=B.`RNC`',CASE TECH_MASK WHEN 1 THEN ' AND B.`TECHNOLOGY`=''GSM''' WHEN 2 THEN ' AND B.`TECHNOLOGY`=''UMTS''' WHEN 4 THEN ' AND B.`TECHNOLOGY`=''LTE''' ELSE '' END ,' 
					AND A.`RNC`',CASE WHEN gt_covmo_csv_count(PU,',')>1 THEN CONCAT(' IN (',PU,')') ELSE CONCAT('=',PU) END,';');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		SET PU_ALL=IFNULL(@PU_GC,'');
		SET ALLPU=0;
	END IF;
	IF PU_ALL='' THEN 
		SELECT 'No Data available!' AS Result;
		LEAVE a_label;
	ELSE 
		SET SP_Para_STR = CONCAT(SOURCE_TYPE,',',SERVICE,',',WORKER_ID,','
					,CASE WHEN DATA_QUARTER='' THEN '''''' ELSE DATA_QUARTER END,','
					,CASE WHEN CELL_ID='' THEN '''''' ELSE CONCAT('''',CELL_ID,'''') END,','
					,CASE WHEN TILE_ID='' THEN '''''' ELSE CONCAT('''',TILE_ID,'''') END,','
					,CASE WHEN IMSI='' THEN '''''' ELSE CONCAT('''',IMSI,'''') END,','
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
					,CASE WHEN FILTER='' THEN '''''' ELSE FILTER END,','
					,CASE WHEN PID='' THEN '''''' ELSE PID END,','
					,CASE WHEN POS_KIND='' THEN '''''' ELSE CONCAT('''',POS_KIND,'''') END,','
					,CASE WHEN SITE_ID='' THEN '''''' ELSE CONCAT('''',SITE_ID,'''') END,','
					,CASE WHEN MAKE_ID='' THEN '''''' ELSE CONCAT('''',MAKE_ID,'''') END,','
					,CASE WHEN MODEL_ID='' THEN '''''' ELSE CONCAT('''',MODEL_ID,'''') END,','
					,CASE WHEN POLYGON_STR='' THEN '''''' ELSE CONCAT('''',POLYGON_STR,'''') END,','
					,CASE WHEN WITHDUMP='' THEN '''0''' ELSE WITHDUMP END,','
					,ALLPU);
		SET CCQ_TABLE:=CONCAT(CCQ_TABLE,'_',WORKER_ID);
		SET @SqlCmd=CONCAT('CREATE DATABASE IF NOT EXISTS ',GT_DB);    
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;  
		CALL gt_schedule.sp_job_create('SP_KPI_multi_RNC',GT_DB);
		SET @V_Multi_PU = @JOB_ID;	
		SET @v_i=1;
		SET @j_i=1;  
		SET @Quotient_v=1;
		SET @Quotient_j=1;	
		SET @START_DATE_J=START_DATE;
		SET @v_R_Max=gt_covmo_csv_count(PU_ALL,',');
		SET @v_T_Max=DATEDIFF(END_DATE,START_DATE)+1;
		
		SET @v_START_HOUR=START_HOUR;
		SET @v_END_HOUR=END_HOUR;	
		WHILE @v_i <= @v_R_Max DO
		BEGIN
			WHILE @j_i <= @v_T_Max DO
				BEGIN
					SET SESSION_NAME = CONCAT('gt_',gt_strtok(gt_covmo_csv_get(PU_ALL,@v_i),1,'|'),'_',DATE_FORMAT(@START_DATE_J,'%Y%m%d'),'_0000_0000');
					SET @START_DATE_J=DATE_ADD(@START_DATE_J,INTERVAL 1 DAY)  ;
				
					SET @SqlCmd=CONCAT('SELECT COUNT(*) INTO @SESSION_CNT FROM `gt_covmo`.`session_information` WHERE `SESSION_DB` = ''', SESSION_NAME,''' AND `TECHNOLOGY`=''',gt_strtok(gt_covmo_csv_get(PU_ALL,@v_i),2,'|'),''';');
					PREPARE Stmt FROM @SqlCmd;
					EXECUTE Stmt;
					DEALLOCATE PREPARE Stmt;
					
					IF @SESSION_CNT>0 THEN 
						SET TARGET_TABLE=CONCAT(GT_DB,'.',CCQ_TABLE,'_',REPLACE(SESSION_NAME,'_0000_0000',''),'_',gt_strtok(gt_covmo_csv_get(PU_ALL,@v_i),2,'|'));
						SET @SqlCmd=CONCAT('DROP TABLE IF EXISTS ',TARGET_TABLE,';');
						PREPARE Stmt FROM @SqlCmd;
						EXECUTE Stmt;
						DEALLOCATE PREPARE Stmt;
						 
						SET @SqlCmd=CONCAT('CREATE TABLE ',TARGET_TABLE,COLUMN_CRT_STR,' ENGINE=MyIsam DEFAULT CHARSET=utf8;');
						PREPARE Stmt FROM @SqlCmd;
						EXECUTE Stmt;
						DEALLOCATE PREPARE Stmt;
						CALL gt_schedule.sp_job_add_task(CONCAT('CALL gt_gw_main.SP_KPI_multi_IMSI_TRACE(',KPI_ID,',''',SESSION_NAME,''',',@v_START_HOUR,',',@v_END_HOUR,','
													,SP_Para_STR 
													,',''',GT_COVMO,''',''',gt_strtok(gt_covmo_csv_get(PU_ALL,@v_i),2,'|'),''',''',GT_DB,''',''',TARGET_TABLE,''');'),@V_Multi_PU);
						IF (@v_i=1 AND @j_i=1) THEN 
							SET UNION_STR:=CONCAT(TARGET_TABLE,',');
						ELSE 
							SET UNION_STR:=CONCAT(UNION_STR,TARGET_TABLE,',');
						END IF; 
					END IF;
					SET @j_i=@j_i+@Quotient_j;
				END;
			END WHILE;
			SET @START_DATE_J=START_DATE;
			SET @Quotient_j=1;
			SET @j_i=1;
			SET @v_i=@v_i+@Quotient_v;
		END;
		END WHILE;
		
		SET UNION_STR =LEFT(UNION_STR,LENGTH(UNION_STR)-1);
		CALL gt_schedule.sp_job_enable_event();
		CALL gt_schedule.sp_job_start(@V_Multi_PU);
		CALL gt_schedule.sp_job_wait(@V_Multi_PU);
		CALL gt_schedule.sp_job_disable_event();
	 
		SET @SqlCmd=CONCAT('SELECT `STATUS` INTO @JOB_STATUS FROM `gt_schedule`.`job_history` WHERE ID=',@V_Multi_PU,';');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		
		IF @JOB_STATUS='FINISHED' THEN 
			SET @SqlCmd=CONCAT('DROP TABLE IF EXISTS ',GT_DB,'.',CCQ_TABLE,';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
			
			SET @SqlCmd=CONCAT('CREATE TABLE ',GT_DB,'.',CCQ_TABLE,COLUMN_CRT_STR,' ENGINE=MRG_MYISAM DEFAULT CHARSET=utf8 INSERT_METHOD=FIRST UNION=(',UNION_STR,')');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt; 
			
			SET @SqlCmd=CONCAT('SELECT SQL_CALC_FOUND_ROWS ',COLUMN_MRG_STR,' FROM ',GT_DB,'.',CCQ_TABLE,' 
						WHERE CALL_ID IS NOT NULL '
						,CASE WHEN SORT_STR<>'' THEN CONCAT(' ORDER BY ',SORT_STR) ELSE '' END							
						,CASE WHEN LIMITS='' THEN '' ELSE CONCAT(' LIMIT ',LIMITS) END,';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
			
			SELECT FOUND_ROWS() INTO @V_CNT;
			
			SET @SqlCmd=CONCAT('REPLACE INTO `gt_covmo`.`tbl_qry_totalcount`(`WORK_ID`,`QRY_TIME`,`TOTAL_CNT`)
						SELECT ',PID,' AS `WORK_ID`,''',NOW(),''' AS `QRY_TIME`,',@V_CNT,' AS `TOTAL_CNT`;');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
			
			SET @SqlCmd=CONCAT('DROP TABLE IF EXISTS ',GT_DB,'.',CCQ_TABLE,';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
			
			SET @v_i=1;	
			SET @j_i=1;
				
			WHILE @v_i <= @v_R_Max DO
				BEGIN
					WHILE @j_i <= @v_T_Max DO
						BEGIN
							SET SESSION_NAME = CONCAT('gt_',gt_strtok(gt_covmo_csv_get(PU_ALL,@v_i),1,'|'),'_',DATE_FORMAT(@START_DATE_J,'%Y%m%d'),'_0000_0000');
							SET @START_DATE_J=DATE_ADD(@START_DATE_J,INTERVAL 1 DAY);
							SET @SqlCmd=CONCAT('SELECT COUNT(*) INTO @SESSION_CNT FROM `gt_covmo`.`session_information` WHERE `SESSION_DB` = ''', SESSION_NAME,''' AND `TECHNOLOGY`=''',gt_strtok(gt_covmo_csv_get(PU_ALL,@v_i),2,'|'),''';');
							PREPARE Stmt FROM @SqlCmd;
							EXECUTE Stmt;
							DEALLOCATE PREPARE Stmt;
							
							IF @SESSION_CNT>0 THEN 
								SET TARGET_TABLE=CONCAT(GT_DB,'.',CCQ_TABLE,'_',REPLACE(SESSION_NAME,'_0000_0000',''),'_',gt_strtok(gt_covmo_csv_get(PU_ALL,@v_i),2,'|'));
						
								SET @SqlCmd=CONCAT('DROP TABLE IF EXISTS ',TARGET_TABLE,';');
								PREPARE Stmt FROM @SqlCmd;
								EXECUTE Stmt;
								DEALLOCATE PREPARE Stmt;
							END IF;
							SET @j_i=@j_i+@Quotient_j;
						END;
					END WHILE;			
					SET @START_DATE_J=START_DATE;
					SET @Quotient_j=1;
					SET @j_i=1;
					SET @v_i=@v_i+@Quotient_v;
				END;
			END WHILE;
		ELSE
			SET @v_i=1;	
			SET @j_i=1;
				
			WHILE @v_i <= @v_R_Max DO
				BEGIN
					WHILE @j_i <= @v_T_Max DO
						BEGIN
							SET SESSION_NAME = CONCAT('gt_',gt_strtok(gt_covmo_csv_get(PU_ALL,@v_i),1,'|'),'_',DATE_FORMAT(@START_DATE_J,'%Y%m%d'),'_0000_0000');
							SET @START_DATE_J=DATE_ADD(@START_DATE_J,INTERVAL 1 DAY);
							
							SET @SqlCmd=CONCAT('SELECT COUNT(*) INTO @SESSION_CNT FROM `gt_covmo`.`session_information` WHERE `SESSION_DB` = ''', SESSION_NAME,''' AND `TECHNOLOGY`=''',gt_strtok(gt_covmo_csv_get(PU_ALL,@v_i),2,'|'),''';');
							PREPARE Stmt FROM @SqlCmd;
							EXECUTE Stmt;
							DEALLOCATE PREPARE Stmt;
							
							IF @SESSION_CNT>0 THEN 
								SET TARGET_TABLE=CONCAT(GT_DB,'.',CCQ_TABLE,'_',REPLACE(SESSION_NAME,'_0000_0000',''),'_',gt_strtok(gt_covmo_csv_get(PU_ALL,@v_i),2,'|'));
								SET @SqlCmd=CONCAT('DROP TABLE IF EXISTS ',TARGET_TABLE,';');
								PREPARE Stmt FROM @SqlCmd;
								EXECUTE Stmt;
								DEALLOCATE PREPARE Stmt;
							END IF;
							SET @j_i=@j_i+@Quotient_j;
						END;
					END WHILE;			
					SET @START_DATE_J=START_DATE;
					SET @Quotient_j=1;
					SET @j_i=1;
					SET @v_i=@v_i+@Quotient_v;
				END;
			END WHILE;
			
			INSERT INTO `gt_gw_main`.`tbl_rpt_error` (`PID`,`CreateTime`,`error_str`) VALUES (PID,NOW(),CONCAT(PID,' Main Parallel Jobs Fail - SP_CovMo_Cross_Query'));
			SIGNAL SP_ERROR
				SET MESSAGE_TEXT = 'Main Parallel Jobs Fail - SP_CovMo_Cross_Query';
		END IF;
	END IF;				
END$$
DELIMITER ;
