DELIMITER $$
USE `gt_gw_main`$$
DROP PROCEDURE IF EXISTS `SP_CreateDB_LTE`$$
CREATE DEFINER=`covmo`@`%` PROCEDURE `SP_Sub_Table_Call_Export`(IN GT_DB VARCHAR(100),IN TECH_MASK TINYINT(4),IN GT_COVMO VARCHAR(100))
BEGIN
	DECLARE START_TIME DATETIME DEFAULT SYSDATE();
	DECLARE RNC_ID INT;
	DECLARE DATA_DATE VARCHAR(8);
	DECLARE DATA_QRT VARCHAR(4);
	DECLARE FOLDER_PATH VARCHAR(20);
	DECLARE DAILY_DB VARCHAR(30);
	DECLARE SH_EH VARCHAR(9) DEFAULT RIGHT(GT_DB,9);
	DECLARE CURRENT_NT_DB VARCHAR(50) DEFAULT CONCAT('gt_nt_',gt_strtok(GT_DB,3,'_'));
	
	SET FOLDER_PATH='//data//Mysql_Export//';	
 	SELECT gt_strtok(GT_DB,2,'_') INTO RNC_ID;
 	SELECT gt_strtok(GT_DB,3,'_') INTO DATA_DATE;
 	SELECT gt_strtok(GT_DB,4,'_') INTO DATA_QRT;
	
	SELECT REPLACE(GT_DB,SH_EH,'0000_0000') INTO DAILY_DB;
	
	INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'SP_Sub_Table_Call_Export','Start', NOW());
	IF TECH_MASK=1 THEN 
		SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS ',GT_DB,'.TMP_TABLE_CALL_EXPORT_GSM;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
	
		SET @SqlCmd=CONCAT('CREATE TEMPORARY TABLE ',GT_DB,'.TMP_TABLE_CALL_EXPORT_GSM
				SELECT 
					''GSM'',
					A.POS_FIRST_BSC AS `Start_BSC_ID`,
					A.POS_LAST_BSC AS `End_BSC_ID`,
					A.`IMSI`,
					B.`MSISDN`,
					A.`IMEI`,
					A.`CALL_ID`,
					(CASE WHEN call_type=10 THEN ''Voice'' 				
						WHEN call_type=16 THEN ''SMS'' 
						WHEN call_type=20 THEN ''GPRS Data'' 
						WHEN call_type=99 THEN ''Others'' 
						ELSE ''Unkonwn'' END) AS `CALL_TYPE`,
					(CASE WHEN call_status=1 THEN ''Normal'' 
						WHEN call_status=2 THEN ''Drop'' 
						WHEN call_status=3 THEN ''Block'' 
						WHEN call_status=6 THEN ''Setup Failure'' 
						ELSE ''Unspecified'' END) AS `CALL_Status`,
					NULL AS `RRC_REQUEST_TYPE`,
					A.`START_TIME`, A.`END_TIME`,
					A.POS_FIRST_LON AS `Start_Longitude`,
					A.POS_FIRST_LAT AS `Start_Latitude`,
					A.POS_LAST_LON AS `End_Longitude`,
					A.POS_LAST_LAT AS `End_Latitude`,
					A.CALL_SETUP_TIME/1000 AS `CALL_SETUP_TIME`,
					NULL AS `DL_Throughput`,
					NULL AS `UL_Throughput`,
					
					A.POS_FIRST_RxLev_FULL_UPLINK AS `Start_RxLev_UL`,
					A.POS_FIRST_RxLev_FULL_DOWNLINK AS `Start_RxLev_DL`,
					A.POS_FIRST_RxQual_FULL_UPLINK AS `Start_RxQual_UL`,
					A.POS_FIRST_RxQual_FULL_DOWNLINK AS `Start_RxQual_DL`,	
					
					A.POS_LAST_RxLev_FULL_UPLINK AS `End_RxLev_UL`,
					A.POS_LAST_RxLev_FULL_DOWNLINK AS `End_RxLev_DL`,
					A.POS_LAST_RxQual_FULL_UPLINK AS `End_RxQual_UL`,
					A.POS_LAST_RxQual_FULL_DOWNLINK AS `End_RxQual_DL`,
					A.DURATION/1000/3600 AS `CS_Erlang`,
					NULL AS `DL_Data_Volume`,
					NULL AS `UL_Data_Volume`,
					A.POS_FIRST_CELL AS `StartCell`,
					A.POS_LAST_CELL AS `EndCell`,
					A.POS_FIRST_LAC AS `StartLAC`,
					A.POS_LAST_LAC AS `EndLAC`,
				
					A.MAKE_ID AS `MAKE_ID`,
					A.MODEL_ID AS `MODEL_ID`,
					A.MNC AS `MNC`,
					A.MCC AS `MCC`,
					A.APN AS `APN`,
					A.POS_FIRST_CONFIDENCE AS `POS_FIRST_CONFIDENCE`,
					A.POS_LAST_CONFIDENCE AS `POS_LAST_CONFIDENCE`
					
					FROM ',GT_DB,'.`table_call_gsm_update` A
					LEFT JOIN ',GT_COVMO,'.`dim_msisdn` B
					ON A.IMSI=B.IMSI
					;
				');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
	
		SET @SqlCmd=CONCAT('ALTER TABLE ',GT_DB,'.TMP_TABLE_CALL_EXPORT_GSM
					ADD COLUMN `MAKE_STRING` varchar(50) NULL,
					ADD COLUMN `MODEL_STRING` varchar(200) NULL ;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
	
		SET @SqlCmd=CONCAT('CREATE INDEX IX_make_id ON ',GT_DB,'.TMP_TABLE_CALL_EXPORT_GSM(`make_id`);');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
	
		SET @SqlCmd=CONCAT('CREATE INDEX IX_model_id ON ',GT_DB,'.TMP_TABLE_CALL_EXPORT_GSM(`model_id`);');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
	
		SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS ',GT_DB,'.TMP_dim_handset_id;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
	
		SET @SqlCmd=CONCAT('CREATE TEMPORARY TABLE ',GT_DB,'.TMP_dim_handset_id
				
					SELECT make_id,manufacturer
					FROM ',gt_covmo,'.`dim_handset_id` 					
				');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
	
		SET @SqlCmd=CONCAT('CREATE INDEX IX_make_id ON ',GT_DB,'.TMP_dim_handset_id(`make_id`);');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
	
		SET @SqlCmd=CONCAT(' UPDATE ',GT_DB,'.TMP_TABLE_CALL_EXPORT_GSM A,',GT_DB,'.TMP_dim_handset_id B					
					SET A.MAKE_STRING=B.manufacturer
					WHERE A.MAKE_ID=B.MAKE_ID
					;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt; 
	
		SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS ',GT_DB,'.TMP_dim_handset_m_id;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
	
		SET @SqlCmd=CONCAT('CREATE TEMPORARY TABLE ',GT_DB,'.TMP_dim_handset_m_id
				
					SELECT make_id,model_id,model
					FROM ',gt_covmo,'.`dim_handset_m_id` 					
				');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
	
		SET @SqlCmd=CONCAT('CREATE INDEX IX_model_id ON ',GT_DB,'.TMP_dim_handset_m_id(`make_id`,`model_id`);');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
	
		SET @SqlCmd=CONCAT(' UPDATE ',GT_DB,'.TMP_TABLE_CALL_EXPORT_GSM A, ',GT_DB,'.TMP_dim_handset_m_id B					
					SET A.MODEL_STRING=B.model					
					WHERE A.MAKE_ID=B.MAKE_ID
					AND A.MODEL_ID=B.MODEL_ID
					
					;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt; 
		SET @SqlCmd=CONCAT('
					SELECT 
					''TECH'' AS TECH,
					''imsi'' AS IMSI,
					''startTime'' AS START_TIME,
					''endTime '' AS END_TIME,
					''imei'' AS IMEI,
					''make'' AS MAKE_STRING,
					''model'' AS MODEL_STRING,
	
					''Call_Type '' AS CALL_TYPE,
					''Call_Status '' as CALL_Status,
	
					
	
					''MNC'' AS MNC,
					''MCC'' AS MCC,
					''APN'' AS APN,
	
					''Start_Cell_ID '' AS  StartCell,
					''Start_LAC'' AS StartLAC,
					''Start_BSC_ID'' AS Start_BSC_ID,
					''Start_Latitude'' AS Start_Latitude,
					''Start_Longitude'' AS Start_Longitude,
					''Start_RxLev_UL'' AS `Start_RxLev_UL`,
					''Start_RxLev_DL'' AS `Start_RxLev_DL`,
					''Start_RxQual_UL'' AS `Start_RxQual_UL`,
					''Start_RxQual_DL'' AS `Start_RxQual_DL`,	
	
					''End_Cell_ID'' AS  EndCell,
					''End_LAC'' AS EndLAC,					
					''End_BSC_ID'' AS End_BSC_ID,
					''End_Latitude'' AS End_Latitude,
					''End_Longitude'' AS End_Longitude,					
					''End_RxLev_UL'' AS `End_RxLev_UL`,
					''End_RxLev_DL'' AS `End_RxLev_DL`,
					''End_RxQual_UL'' AS `End_RxQual_UL`,
					''End_RxQual_DL'' AS `End_RxQual_DL`,
	
					''UL_Traffic_Volume'' AS UL_Data_Volume,
					''UL_Throughput_Max'' AS UL_Throughput,	
					''DL_Traffic_Volume'' AS DL_Data_Volume,
					''DL_Throughput_Max'' AS DL_Throughput ,
	
					''MSISDN'' AS MSISDN,					
					''CALL_ID'' AS CALL_ID,
					''Call_Setup_Time(Second)'' AS Call_Setup_Time,		
					''CS_Erlang'' AS CS_Erlang,
	
					''RRC_REQUEST_TYPE'' AS RRC_REQUEST_TYPE,
					''START_CONFIDENCE'' AS POS_FIRST_CONFIDENCE,
					''END_CONFIDENCE'' AS POS_LAST_CONFIDENCE
					
	
					UNION
					SELECT 
						''GSM'',						
						
					IMSI,
					START_TIME,
					END_TIME,
					IMEI,
					MAKE_STRING,
					MODEL_STRING,
	
					CALL_TYPE,
					CALL_Status,
	
					
	
					MNC,
					MCC,
					APN,
	
					StartCell,
					StartLAC,
					Start_BSC_ID,
					Start_Latitude,
					Start_Longitude,
					`Start_RxLev_UL`,
					`Start_RxLev_DL`,
					`Start_RxQual_UL`,
					`Start_RxQual_DL`,	
	
					EndCell,
					EndLAC,					
					End_BSC_ID,
					End_Latitude,
					End_Longitude,					
					`End_RxLev_UL`,
					`End_RxLev_DL`,
					`End_RxQual_UL`,
					`End_RxQual_DL`,
	
					UL_Data_Volume,
					UL_Throughput,	
					DL_Data_Volume,
					DL_Throughput ,
	
					MSISDN,					
					CALL_ID,
					Call_Setup_Time,		
					CS_Erlang,
					RRC_REQUEST_TYPE,
					POS_FIRST_CONFIDENCE,
					POS_LAST_CONFIDENCE
					FROM ',GT_DB,'.TMP_TABLE_CALL_EXPORT_GSM					
					INTO OUTFILE ''',FOLDER_PATH,'/GSM',RNC_ID,'_',DATA_DATE,'_',DATA_QRT,'.csv''
					FIELDS TERMINATED BY ''\t''
					OPTIONALLY ENCLOSED BY ''''
					LINES TERMINATED BY ''\n'';');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		
		SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS ',GT_DB,'.TMP_TABLE_CALL_EXPORT_GSM;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
	
		SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS ',GT_DB,'.TMP_dim_handset_id;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
	
		SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS ',GT_DB,'.TMP_dim_handset_m_id;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
	
	END IF;
	IF TECH_MASK=2 THEN 
		
		SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS ',GT_DB,'.TMP_TABLE_CALL_EXPORT_UMTS;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
	
		SET @SqlCmd=CONCAT('CREATE TEMPORARY TABLE ',GT_DB,'.TMP_TABLE_CALL_EXPORT_UMTS
				SELECT ''UMTS'',',RNC_ID,',A.`IMSI`, B.`MSISDN`,A.`IMEI_NEW`,A.`CALL_ID`,
						(CASE WHEN A.call_type =10 THEN ''Voice'' 
						WHEN call_type=11 THEN ''Video'' 
						WHEN call_type=12 THEN ''PS 99'' 
						WHEN A.call_type=13 THEN ''PS HSPA'' 
						WHEN call_type=14 THEN ''Multi-RAB''  
						WHEN A.call_type=15 THEN ''Signal'' 
						WHEN call_type=16 THEN ''UMTS SMS'' 
						WHEN A.call_type IN (18,19) THEN ''Unspecified Call'' 
						ELSE ''Unkonwn'' END) AS `CALL_TYPE`,
						(CASE WHEN call_status=1 THEN ''Normal'' 
						WHEN call_status=2 THEN ''Drop''
						WHEN call_status=3 THEN ''Block'' 
						WHEN call_status=6 THEN ''Setup Failure'' 
						ELSE ''Unspecified'' END) AS `CALL_Status`,
					
					(CASE   WHEN A.RRC_REQUEST_TYPE =0 THEN ''Originating Conversational Call'' 
						WHEN a.RRC_REQUEST_TYPE=1 THEN ''Originating Streaming Call'' 
						WHEN a.RRC_REQUEST_TYPE=2 THEN ''Originating Interactive Call'' 
						WHEN A.RRC_REQUEST_TYPE=3 THEN ''Originating Background Call'' 
						WHEN a.RRC_REQUEST_TYPE=4 THEN ''Originating Subscribed Traffic Call''  
						WHEN A.RRC_REQUEST_TYPE=5 THEN ''Terminating Conversational Call'' 
						WHEN a.RRC_REQUEST_TYPE=6 THEN ''Terminating Streaming Call'' 
						WHEN a.RRC_REQUEST_TYPE=7 THEN ''Terminating Interactive Call'' 
						WHEN a.RRC_REQUEST_TYPE=8 THEN ''Terminating Background Call'' 
						WHEN a.RRC_REQUEST_TYPE=9 THEN ''Emergency Call'' 
						WHEN a.RRC_REQUEST_TYPE=10 THEN ''InterRAT - Cell Reselection'' 
						WHEN a.RRC_REQUEST_TYPE=11 THEN ''InterRAT - Cell Change Order'' 
						WHEN a.RRC_REQUEST_TYPE=12 THEN ''Registration'' 
						WHEN a.RRC_REQUEST_TYPE=13 THEN ''Detach'' 
						WHEN a.RRC_REQUEST_TYPE=14 THEN ''Originating High Priority Signalling'' 
						WHEN a.RRC_REQUEST_TYPE=15 THEN ''Originating LowPriority Signalling (SMS)'' 
						WHEN a.RRC_REQUEST_TYPE=16 THEN ''Call Re-establishment'' 
						WHEN a.RRC_REQUEST_TYPE=17 THEN ''Terminating High Priority Signalling'' 
						WHEN a.RRC_REQUEST_TYPE=18 THEN ''Terminating Low Priority Signalling (SMS)'' 
						WHEN a.RRC_REQUEST_TYPE=19 THEN ''TerminatingCauseUnknown'' 
						WHEN a.RRC_REQUEST_TYPE=20 THEN ''MBMS-Reception''
						WHEN a.RRC_REQUEST_TYPE=21 THEN ''MBMS-PTP-RB-Request''
						ELSE ''Unkonwn'' END) AS `RRC_REQUEST_TYPE`,
					A.`START_TIME`, A.`END_TIME`,
					A.POS_FIRST_LON AS `Start_Longitude`,
					A.POS_FIRST_LAT AS `Start_Latitude`,
					A.POS_LAST_LON AS `End_Longitude`,
					A.POS_LAST_LAT AS `End_Latitude`,
					A.CALL_SETUP_TIME/1000 as `CALL_SETUP_TIME`,
					A.DL_THROUGHPUT_MAX AS `DL_Throughput`,
					A.UL_THROUGHPUT_MAX AS `UL_Throughput`,
					A.POS_FIRST_RSCP AS `Start_RSCP`,
					A.POS_FIRST_ECN0 AS `Start_ECN0`,
					A.POS_LAST_RSCP AS `End_RSCP`,
					A.POS_LAST_ECN0 AS `End_ECN0`,
					A.CS_CALL_DURA/1000/3600 AS `CS_Erlang`,
					A.DL_TRAFFIC_VOLUME AS `DL_Data_Volume`,
					A.UL_TRAFFIC_VOLUME AS `UL_Data_Volume`,
					A.POS_FIRST_CELL AS `StartCell`,
					A.POS_LAST_CELL AS `EndCell`,
	
					A.POS_FIRST_RNC AS `Start_RNC_ID`,
					A.POS_LAST_RNC AS `End_RNC_ID`,
					
					A.MAKE_ID AS `MAKE_ID`,
					A.MODEL_ID AS `MODEL_ID`,
					A.MNC AS `MNC`,
					A.MCC AS `MCC`,
					A.APN AS `APN`,
					A.POS_FIRST_CONFIDENCE AS `POS_FIRST_CONFIDENCE`,
					A.POS_LAST_CONFIDENCE AS `POS_LAST_CONFIDENCE`
					
					FROM ',DAILY_DB,'.table_call_',DATA_QRT,' A
					LEFT JOIN ',GT_COVMO,'.`dim_msisdn` B
					ON A.IMSI=B.IMSI
					;
				');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
	
		SET @SQLCMD=CONCAT ('CREATE INDEX  Callid on ',GT_DB,'.TMP_TABLE_CALL_EXPORT_UMTS(CALL_ID) ');
		
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
	
		SET @SQLCMD=CONCAT ('CREATE INDEX  Calltype on ',GT_DB,'.TMP_TABLE_CALL_EXPORT_UMTS(call_type) ');
		
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
	
		SET @SQLCMD=CONCAT ('CREATE INDEX  RRC_REQUEST_TYPE on ',GT_DB,'.TMP_TABLE_CALL_EXPORT_UMTS(RRC_REQUEST_TYPE) ');
	
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
	
		SET @SqlCmd=CONCAT('CREATE TEMPORARY TABLE ',GT_DB,'.TMP_TABLE_CALL_EXPORT_UMTS_RRC
				
					SELECT RRC_REQUEST_TYPE,CALL_ID,call_type
					FROM ',GT_DB,'.TMP_TABLE_CALL_EXPORT_UMTS
					WHERE call_type=''Multi-RAB''
				
				');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		
		SET @SQLCMD=CONCAT ('CREATE INDEX  Callid on ',GT_DB,'.TMP_TABLE_CALL_EXPORT_UMTS_RRC(CALL_ID) ');
		
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
	
		SET @SQLCMD=CONCAT ('CREATE INDEX  Calltype on ',GT_DB,'.TMP_TABLE_CALL_EXPORT_UMTS_RRC(call_type) ');
		
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
	
		SET @SQLCMD=CONCAT ('CREATE INDEX  RRC_REQUEST_TYPE on ',GT_DB,'.TMP_TABLE_CALL_EXPORT_UMTS_RRC(RRC_REQUEST_TYPE) ');
	
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;		
	
		SET @SqlCmd=CONCAT('UPDATE ',GT_DB,'.TMP_TABLE_CALL_EXPORT_UMTS A , ',GT_DB,'.`TMP_TABLE_CALL_EXPORT_UMTS_RRC` B
					SET A.RRC_REQUEST_TYPE = B.RRC_REQUEST_TYPE
					WHERE A.CALL_ID=B.CALL_ID
					AND A.call_type= ''Voice''
					AND A.RRC_REQUEST_TYPE =''Unkonwn''
					;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		SET @SqlCmd=CONCAT('ALTER TABLE ',GT_DB,'.TMP_TABLE_CALL_EXPORT_UMTS
					ADD COLUMN `StartLAC` varchar(20) NULL,
					ADD COLUMN `EndLAC` varchar(20) NULL ;');
		
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		
		SET @SQLCMD=CONCAT ('CREATE INDEX  StartCell on ',GT_DB,'.TMP_TABLE_CALL_EXPORT_UMTS(StartCell) ');
		
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
	
		SET @SQLCMD=CONCAT ('CREATE INDEX  EndCell on ',GT_DB,'.TMP_TABLE_CALL_EXPORT_UMTS(EndCell) ');
	
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		
		SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS ',GT_DB,'.TMP_nt_current;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
	
		SET @SqlCmd=CONCAT('CREATE TEMPORARY TABLE ',GT_DB,'.TMP_nt_current
				
					SELECT LAC,CELL_ID
					FROM ',CURRENT_NT_DB,'.`nt_current` A
				
				');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
	
		SET @SQLCMD=CONCAT ('CREATE INDEX  CELL_ID on ',GT_DB,'.TMP_nt_current(CELL_ID) ');
		
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
	
	
		SET @SqlCmd=CONCAT('UPDATE ',GT_DB,'.TMP_TABLE_CALL_EXPORT_UMTS A,',CURRENT_NT_DB,'.`nt_current` B
					SET A.StartLAC=B.LAC
					WHERE A.Start_RNC_ID=B.RNC_ID
					AND A.StartCell=B.CELL_ID;');
	
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
	
		SET @SqlCmd=CONCAT('UPDATE ',GT_DB,'.TMP_TABLE_CALL_EXPORT_UMTS A,',CURRENT_NT_DB,'.`nt_current` B
					SET A.EndLac=B.LAC
					WHERE A.End_RNC_ID=B.RNC_ID
					AND A.EndCell=B.CELL_ID;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
	
	
		SET @SqlCmd=CONCAT('ALTER TABLE ',GT_DB,'.TMP_TABLE_CALL_EXPORT_UMTS
					ADD COLUMN `MAKE_STRING` varchar(50) NULL,
					ADD COLUMN `MODEL_STRING` varchar(200) NULL ;');
		
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
	
		SET @SqlCmd=CONCAT('CREATE INDEX IX_make_id ON ',GT_DB,'.TMP_TABLE_CALL_EXPORT_UMTS(`make_id`);');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
	
		SET @SqlCmd=CONCAT('CREATE INDEX IX_model_id ON ',GT_DB,'.TMP_TABLE_CALL_EXPORT_UMTS(`model_id`);');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
	
		SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS ',GT_DB,'.TMP_dim_handset_id;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
	
		SET @SqlCmd=CONCAT('CREATE TEMPORARY TABLE ',GT_DB,'.TMP_dim_handset_id
				
					SELECT make_id,manufacturer
					FROM ',gt_covmo,'.`dim_handset_id` 					
				');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
	
		SET @SqlCmd=CONCAT('CREATE INDEX IX_make_id ON ',GT_DB,'.TMP_dim_handset_id(`make_id`);');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
	
		SET @SqlCmd=CONCAT(' UPDATE ',GT_DB,'.TMP_TABLE_CALL_EXPORT_UMTS A,',GT_DB,'.TMP_dim_handset_id B					
					SET A.MAKE_STRING=B.manufacturer
					WHERE A.MAKE_ID=B.MAKE_ID
					;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt; 
	
		SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS ',GT_DB,'.TMP_dim_handset_m_id;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
	
		SET @SqlCmd=CONCAT('CREATE TEMPORARY TABLE ',GT_DB,'.TMP_dim_handset_m_id
				
					SELECT make_id,model_id,model
					FROM ',gt_covmo,'.`dim_handset_m_id` 					
				');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
	
		SET @SqlCmd=CONCAT('CREATE INDEX IX_model_id ON ',GT_DB,'.TMP_dim_handset_m_id(`make_id`,`model_id`);');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
	
		SET @SqlCmd=CONCAT(' UPDATE ',GT_DB,'.TMP_TABLE_CALL_EXPORT_UMTS A, ',GT_DB,'.TMP_dim_handset_m_id B					
					SET A.MODEL_STRING=B.model					
					WHERE A.MAKE_ID=B.MAKE_ID
					AND A.MODEL_ID=B.MODEL_ID
					;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt; 
	
		SET @SqlCmd=CONCAT('SELECT ''TECH'' AS TECH,
					''imsi'' AS IMSI,
					''startTime'' AS START_TIME,
					''endTime'' AS END_TIME,
					''imei'' AS IMEI_NEW,
					''make'' AS MAKE_STRING,
					''model'' AS MODEL_STRING,
					''Call_Type '' AS CALL_TYPE,
					''Call_Status '' as CALL_Status,
					
					''MNC'' AS MNC,
					''MCC'' AS MCC,
					''APN'' AS APN,
					''Start_Cell_ID '' AS  StartCell,
					''Start_LAC'' AS StartLAC,
					''Start_RNC_ID'' AS `Start_RNC_ID`,
					''Start_Latitude'' AS Start_Latitude,
					''Start_Longitude'' AS Start_Longitude,
					''Start_RSCP'' AS Start_RSCP,
					''Start_EcN0'' AS Start_ECN0,
	
					''End_Cell_ID'' AS `EndCell`,
					''End_LAC'' AS EndLAC,
					''End_RNC_ID'' AS End_RNC_ID,
					''End_Latitude'' AS End_Latitude,
					''End_Longitude'' AS End_Longitude,				
					''End_RSCP'' AS End_RSCP,
					''End_EcN0'' AS End_ECN0,
					''UL_Traffic_Volume'' AS UL_Data_Volume,
					''UL_Throughput_Max'' AS UL_Throughput,
					''DL_Traffic_Volume'' AS DL_Data_Volume,
					''DL_Throughput_Max'' AS DL_Throughput ,
	
					''MSISDN'' AS MSISDN,					
					''CALL_ID'' AS CALL_ID,					
					''Call_Setup_Time(Second)'' AS Call_Setup_Time,
					''CS_Erlang'' AS CS_Erlang,
				
					''RRC_REQUEST_TYPE'' as RRC_REQUEST_TYPE,
					''START_CONFIDENCE'' AS POS_FIRST_CONFIDENCE, 
					''END_CONFIDENCE'' AS POS_LAST_CONFIDENCE
					
					UNION
					SELECT ''UMTS'',
					IMSI,
					START_TIME,
					END_TIME,
					IMEI_NEW,
					MAKE_STRING,
					MODEL_STRING,
					CALL_TYPE,
					CALL_Status,
					
					MNC,
					MCC,
					APN,
					StartCell,
					StartLAC,
					`Start_RNC_ID`,
					Start_Latitude,
					Start_Longitude,
					Start_RSCP,
					Start_ECN0,
	
					EndCell,
					EndLAC,
					End_RNC_ID,
					End_Latitude,
					End_Longitude,				
					End_RSCP,
					End_ECN0,
					UL_Data_Volume,
					UL_Throughput,
					DL_Data_Volume,
					DL_Throughput ,
	
					MSISDN,					
					CALL_ID,						
					Call_Setup_Time,
					CS_Erlang,
					
					RRC_REQUEST_TYPE,
					POS_FIRST_CONFIDENCE,
					POS_LAST_CONFIDENCE
	
					FROM ',GT_DB,'.TMP_TABLE_CALL_EXPORT_UMTS
					INTO OUTFILE ''',FOLDER_PATH,'/UMTS',RNC_ID,'_',DATA_DATE,'_',DATA_QRT,'.csv''
					FIELDS TERMINATED BY ''\t''
					OPTIONALLY ENCLOSED BY ''''
					LINES TERMINATED BY ''\n'';');
		
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS ',GT_DB,'.TMP_TABLE_CALL_EXPORT_UMTS;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
	
		SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS ',GT_DB,'.TMP_nt_current;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
	
		SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS ',GT_DB,'.TMP_dim_handset_id;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
	
		SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS ',GT_DB,'.TMP_dim_handset_m_id;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
	END IF;
	IF TECH_MASK=4 THEN 
		SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS ',GT_DB,'.TMP_TABLE_CALL_EXPORT_LTE;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
	
		SET @SqlCmd=CONCAT('CREATE TEMPORARY TABLE ',GT_DB,'.TMP_TABLE_CALL_EXPORT_LTE
				SELECT ''LTE'' AS `LTE`,
				',RNC_ID,' AS `',RNC_ID,'`,
				A.`IMSI` AS `IMSI`,
				B.`MSISDN` AS `MSISDN`,
				A.`IMEI` AS `IMEI`,
				A.`CALL_ID` AS `CALL_ID`,
				(CASE WHEN A.CALL_TYPE=21 THEN ''LTE-TRAFFIC'' WHEN A.CALL_TYPE=22 THEN ''LTE-SIGNALING'' WHEN A.CALL_TYPE=24 THEN ''LTE-SMS'' WHEN A.CALL_TYPE=29 THEN ''LTE-Unspecified'' ELSE ''Unkonwn'' END) AS `CALL_TYPE`,
				(CASE WHEN A.CALL_STATUS=1 THEN ''Normal'' WHEN A.CALL_STATUS=2 THEN ''Drop'' WHEN A.CALL_STATUS=3 THEN ''Block'' WHEN A.CALL_STATUS=4 THEN ''Unspecified Call'' WHEN A.CALL_STATUS=5 THEN ''CS-Fallback'' WHEN A.CALL_STATUS=6 THEN ''Setup Failure'' ELSE ''Unspecified'' END) AS `CALL_Status`,
				
	
				(CASE WHEN A.RRC_REQUEST_TYPE=1 THEN ''Mo-Signalling'' WHEN A.RRC_REQUEST_TYPE=2 THEN ''Mo-Access'' WHEN A.RRC_REQUEST_TYPE=3 THEN ''Mo-Data'' WHEN A.RRC_REQUEST_TYPE=4 THEN ''Mt-Access'' 
				      WHEN A.RRC_REQUEST_TYPE=5 THEN ''Emergency'' WHEN A.RRC_REQUEST_TYPE=6 THEN ''HighPriorityAccess'' WHEN A.RRC_REQUEST_TYPE=7 THEN ''LTE unknown ''  WHEN A.RRC_REQUEST_TYPE=8 THEN ''LTE handover signalling'' 
				ELSE ''Unkonwn'' END) AS `RRC_REQUEST_TYPE`,
		
				A.`START_TIME`, A.`END_TIME`,
				gt_covmo_proj_geohash_to_lng(A.POS_FIRST_LOC) AS `Start_Longitude`,
				gt_covmo_proj_geohash_to_lat(A.POS_FIRST_LOC) AS `Start_Latitude`,
				gt_covmo_proj_geohash_to_lng(A.POS_LAST_LOC) AS `End_Longitude`,
				gt_covmo_proj_geohash_to_lat(A.POS_LAST_LOC) AS `End_Latitude`,
				A.CALL_SETUP_TIME/1000 AS `CALL_SETUP_TIME`,
				A.DL_THROUPUT_MAX AS `DL_Throughput`,
				A.UL_THROUPUT_MAX AS `UL_Throughput`,
				A.POS_FIRST_S_RSRP AS `Start_RSRP`,
				A.POS_FIRST_S_RSRQ AS `Start_RSRQ`,
				A.POS_LAST_S_RSRP AS `End_RSRP`,
				A.POS_LAST_S_RSRQ AS `End_RSRQ`,
				A.DL_VOLUME AS `DL_Data_Volume`,
				A.UL_VOLUME AS `UL_Data_Volume`,
				A.POS_FIRST_S_CELL AS `StartCell`,
				A.POS_LAST_S_CELL AS `EndCell`,
				(CASE WHEN A.POS_FIRST_S_ENODEB =-1 AND A.POS_FIRST_S_CELL =-1 THEN '''' ELSE CONCAT(A.POS_FIRST_S_ENODEB,A.POS_FIRST_S_CELL) END) AS `StartEnodebCell`,
				(CASE WHEN A.POS_LAST_S_ENODEB =-1 AND A.POS_LAST_S_CELL =-1 THEN '''' ELSE CONCAT(A.POS_LAST_S_ENODEB,A.POS_LAST_S_CELL) END) AS `EndEnodebCell`,
				(CASE WHEN A.POS_FIRST_S_ENODEB =-1 THEN '''' ELSE A.POS_FIRST_S_ENODEB END)  AS `StartEnodeb`,
				(CASE WHEN A.POS_LAST_S_ENODEB =-1 THEN '''' ELSE A.POS_LAST_S_ENODEB END) AS `EndEnodeb`,
			
				A.MAKE_ID AS `MAKE_ID`,
				A.MODEL_ID AS `MODEL_ID`,
				A.MNC AS `MNC`,
				A.MCC AS `MCC`,
				NULL AS `APN`
				FROM ',DAILY_DB,'.table_call_lte_',DATA_QRT,' A
				LEFT JOIN ',GT_COVMO,'.`dim_msisdn` B
				ON A.IMSI=B.IMSI
				WHERE CALL_TYPE <> 23
				;
				');	
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
	
		SET @SqlCmd=CONCAT('ALTER TABLE ',GT_DB,'.TMP_TABLE_CALL_EXPORT_LTE
					ADD COLUMN `CS_Erlang` INT(11) NULL AFTER `End_RSRQ`;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
	
		
		SET @SQLCMD=CONCAT ('CREATE INDEX  CallidINDEX on ',GT_DB,'.TMP_TABLE_CALL_EXPORT_LTE(CALL_ID) ');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
	
		SET @SqlCmd=CONCAT('	INSERT INTO ',GT_DB,'.TMP_TABLE_CALL_EXPORT_LTE
					(
					
					LTE,
					`',RNC_ID,'`,
					IMSI,
					MSISDN,
					IMEI,
					CALL_ID,
					CALL_TYPE,
					CALL_Status,
					START_TIME, 
					END_TIME,
					Start_Longitude,
					Start_Latitude,
					End_Longitude,
					End_Latitude,
					CALL_SETUP_TIME,
					DL_Throughput,
					UL_Throughput,
					Start_RSRP,
					Start_RSRQ,
					End_RSRP,
					End_RSRQ,
					CS_Erlang,
					DL_Data_Volume,
					UL_Data_Volume,
					StartCell,
					EndCell,
					StartEnodebCell,
					EndEnodebCell,
					StartEnodeb,
					EndEnodeb,
				
					MAKE_ID,
					MODEL_ID,
					MNC,
					MCC,
					APN
	
					)
					SELECT ''LTE'',''',RNC_ID,''',A.`IMSI`, B.`MSISDN`,A.`IMEI`,A.`CALL_ID`,
						''LTE-VoLTE''AS `CALL_TYPE`,
						(CASE WHEN A.ERAB_STATUS=1 THEN ''Normal'' WHEN A.ERAB_STATUS=2 THEN ''Drop'' WHEN A.ERAB_STATUS=3 THEN ''Block'' WHEN A.ERAB_STATUS=4 THEN ''Unspecified Call'' WHEN A.ERAB_STATUS=5 THEN ''CS-Fallback'' WHEN A.ERAB_STATUS=6 THEN ''Setup Failure'' ELSE ''Unspecified'' END) AS `CALL_Status`,
					A.`ERAB_START_TIME`,
					A.`ERAB_END_TIME`,
						gt_covmo_proj_geohash_to_lng(A.ERAB_START_LOC) AS `Start_Longitude`,
						gt_covmo_proj_geohash_to_lat(A.ERAB_START_LOC) AS `Start_Latitude`,
						gt_covmo_proj_geohash_to_lng(A.ERAB_END_LOC) AS `End_Longitude`,
						gt_covmo_proj_geohash_to_lat(A.ERAB_END_LOC) AS `End_Latitude`,
					A.`ERAB_ACCESS_DELAY`/1000 AS `CALL_SETUP_TIME`,
					NULL AS `DL_Throughput`,
					NULL AS `UL_Throughput`,
					A.ERAB_START_SERVING_RSRP AS `Start_RSRP`,
					A.ERAB_START_SERVING_RSRQ AS `Start_RSRQ`,
					A.ERAB_END_SERVING_RSRP AS `End_RSRP`,
					A.ERAB_END_SERVING_RSRQ AS `End_RSRQ`,
					A.`DURATION`/1000 AS `CS_Erlang`,
					NULL AS `DL_Data_Volume`,
					NULL AS `UL_Data_Volume`,
					A.ERAB_START_SERVING_CELL AS `StartCell`,
					A.ERAB_END_SERVING_CELL AS `EndCell`,
					(CASE WHEN A.ERAB_START_SERVING_ENODEB =-1 AND A.ERAB_START_SERVING_CELL =-1 THEN '''' ELSE CONCAT(A.ERAB_START_SERVING_ENODEB,A.ERAB_START_SERVING_CELL) END) AS `StartEnodebCell`,
					(CASE WHEN A.ERAB_END_SERVING_ENODEB =-1 AND A.ERAB_END_SERVING_CELL =-1 THEN '''' ELSE CONCAT(A.ERAB_END_SERVING_ENODEB,A.ERAB_END_SERVING_CELL) END) AS `EndEnodebCell`,
					(CASE WHEN A.ERAB_START_SERVING_ENODEB =-1 THEN '''' ELSE A.ERAB_START_SERVING_ENODEB END)  AS `StartEnodeb`,
					(CASE WHEN A.ERAB_END_SERVING_ENODEB =-1 THEN '''' ELSE A.ERAB_END_SERVING_ENODEB END) AS `EndEnodeb`,
				
					A.MAKE_ID AS `MAKE_ID`,
					A.MODEL_ID AS `MODEL_ID`,
					A.MNC AS `MNC`,
					A.MCC AS `MCC`,
					NULL AS `APN`
					FROM ',DAILY_DB,'.table_erab_volte_lte_',DATA_QRT,' A
					LEFT JOIN ',GT_COVMO,'.`dim_msisdn` B
					ON A.IMSI=B.IMSI
					WHERE A.IMSI IS NOT NULL
				;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
	
		SET @SqlCmd=CONCAT('UPDATE ',GT_DB,'.TMP_TABLE_CALL_EXPORT_LTE A , ',DAILY_DB,'.table_call_lte_',DATA_QRT,' B
					SET A.RRC_REQUEST_TYPE = B.RRC_REQUEST_TYPE
					WHERE A.CALL_ID=B.CALL_ID
					AND A.RRC_REQUEST_TYPE IS NULL;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
	
		SET @SqlCmd=CONCAT('UPDATE ',GT_DB,'.TMP_TABLE_CALL_EXPORT_LTE A , ',GT_COVMO,'.dim_rrc_request_type_lte B
					SET A.RRC_REQUEST_TYPE = B.cuase_value
					WHERE A.RRC_REQUEST_TYPE=B.numeric_value;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
	
		SET @SqlCmd=CONCAT('ALTER TABLE ',GT_DB,'.TMP_TABLE_CALL_EXPORT_LTE
					ADD COLUMN `StartTAC` varchar(20) NULL,
					ADD COLUMN `EndTAC` varchar(20) NULL ;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		
		SET @SQLCMD=CONCAT ('CREATE INDEX  StartINDEX on ',GT_DB,'.TMP_TABLE_CALL_EXPORT_LTE(StartCell,StartEnodeb) ');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
	
		SET @SQLCMD=CONCAT ('CREATE INDEX  EndINDEX on ',GT_DB,'.TMP_TABLE_CALL_EXPORT_LTE(EndCell,EndEnodeb) ');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
	
		SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS ',GT_DB,'.TMP_nt_tac_cell_current_lte;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
	
		SET @SqlCmd=CONCAT('CREATE TEMPORARY TABLE ',GT_DB,'.TMP_nt_tac_cell_current_lte
				
					SELECT TAC,ENODEB_ID,CELL_ID
					FROM ',CURRENT_NT_DB,'.`nt_tac_cell_current_lte` 					
				');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		
		SET @SQLCMD=CONCAT ('CREATE INDEX  EndoebCellINDEX on ',GT_DB,'.TMP_nt_tac_cell_current_lte(ENODEB_ID,CELL_ID) ');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
	
	
		SET @SqlCmd=CONCAT('UPDATE ',GT_DB,'.TMP_TABLE_CALL_EXPORT_LTE A,',GT_DB,'.`TMP_nt_tac_cell_current_lte` B
					SET A.StartTAC=B.TAC
					WHERE A.StartCell=B.CELL_ID
					AND A.StartEnodeb=B.ENODEB_ID;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
	
		SET @SqlCmd=CONCAT('UPDATE ',GT_DB,'.TMP_TABLE_CALL_EXPORT_LTE A,',GT_DB,'.`TMP_nt_tac_cell_current_lte` B
					SET A.EndTAC=B.TAC
					WHERE A.EndCell=B.CELL_ID
					AND A.EndEnodeb=B.ENODEB_ID;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
	
		SET @SqlCmd=CONCAT('ALTER TABLE ',GT_DB,'.TMP_TABLE_CALL_EXPORT_LTE
					ADD COLUMN `MAKE_STRING` varchar(50) NULL,
					ADD COLUMN `MODEL_STRING` varchar(200) NULL ;');
		
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
	
		SET @SqlCmd=CONCAT('CREATE INDEX IX_make_id ON ',GT_DB,'.TMP_TABLE_CALL_EXPORT_LTE(`make_id`);');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
	
		SET @SqlCmd=CONCAT('CREATE INDEX IX_model_id ON ',GT_DB,'.TMP_TABLE_CALL_EXPORT_LTE(`model_id`);');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
	
		SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS ',GT_DB,'.TMP_dim_handset_id;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
	
		SET @SqlCmd=CONCAT('CREATE TEMPORARY TABLE ',GT_DB,'.TMP_dim_handset_id
				
					SELECT make_id,manufacturer
					FROM ',gt_covmo,'.`dim_handset_id` 					
				');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
	
		SET @SqlCmd=CONCAT('CREATE INDEX IX_make_id ON ',GT_DB,'.TMP_dim_handset_id(`make_id`);');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
	
		SET @SqlCmd=CONCAT(' UPDATE ',GT_DB,'.TMP_TABLE_CALL_EXPORT_LTE A,',GT_DB,'.TMP_dim_handset_id B					
					SET A.MAKE_STRING=B.manufacturer
					WHERE A.MAKE_ID=B.MAKE_ID
					;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt; 
	
		SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS ',GT_DB,'.TMP_dim_handset_m_id;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
	
		SET @SqlCmd=CONCAT('CREATE TEMPORARY TABLE ',GT_DB,'.TMP_dim_handset_m_id
				
					SELECT make_id,model_id,model
					FROM ',gt_covmo,'.`dim_handset_m_id` 					
				');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
	
		SET @SqlCmd=CONCAT('CREATE INDEX IX_model_id ON ',GT_DB,'.TMP_dim_handset_m_id(`make_id`,`model_id`);');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
	
		
	
		SET @SqlCmd=CONCAT(' UPDATE ',GT_DB,'.TMP_TABLE_CALL_EXPORT_LTE A, ',GT_DB,'.TMP_dim_handset_m_id B					
					SET A.MODEL_STRING=B.model					
					WHERE A.MAKE_ID=B.MAKE_ID
					AND A.MODEL_ID=B.MODEL_ID
					;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt; 
		SET @SqlCmd=CONCAT('
					SELECT ''TECH'' AS TECH,
					''imsi'' AS IMSI,
					''startTime'' AS START_TIME,
					''endTime'' AS END_TIME,
					''imei'' AS IMEI,
					''make'' AS MAKE_STRING,
					''model'' AS MODEL_STRING,
					''Call_Type '' AS CALL_TYPE,
					''Call_Status '' as CALL_Status,
					
					''MNC'' AS MNC,
					''MCC'' AS MCC,
					''APN'' AS APN,
		
					''Start_Cell_ID '' AS  StartEnodebCell,
					''Start_TAC'' AS StartTAC,
					''Start_ENODEB_ID'' AS StartEnodeb,
					''Start_Latitude'' AS Start_Latitude,
	 				''Start_Longitude'' AS Start_Longitude,
					''Start_RSRP'' AS Start_RSRP,
					''Start_RSRQ'' AS Start_RSRQ,
	
					''End_Cell_ID'' AS  EndEnodebCell,
					''End_TAC'' AS EndTAC,
					''End_ENODEB_ID'' AS EndEnodeb,
					''End_Latitude'' AS End_Latitude,
					''End_Longitude'' AS End_Longitude,
					''End_RSRP'' AS End_RSRP,
					''End_RSRQ'' AS End_RSRQ,
					''UL_Traffic_Volume'' AS UL_Data_Volume,
					''UL_Throughput_Max'' AS UL_Throughput,		
					''DL_Traffic_Volume'' AS DL_Data_Volume,
					''DL_Throughput_Max'' AS DL_Throughput ,
	
					''MSISDN'' AS MSISDN,					
					''CALL_ID'' AS CALL_ID,					
					''Call_Setup_Time(Second)'' AS Call_Setup_Time,
					''CS_Erlang'' AS CS_Erlang,
	
					''RRC_REQUEST_TYPE'' as RRC_REQUEST_TYPE 
					
				UNION
					SELECT ''LTE'',
					IMSI,
					START_TIME,
					END_TIME,
					IMEI,
					MAKE_STRING,
					MODEL_STRING,
					CALL_TYPE,
					CALL_Status,
					
					MNC,
					MCC,
					APN,
		
					StartEnodebCell,
					StartTAC,
					StartEnodeb,
					Start_Latitude,
	 				Start_Longitude,
					Start_RSRP,
					Start_RSRQ,
	
					EndEnodebCell,
					EndTAC,
					EndEnodeb,
					End_Latitude,
					End_Longitude,
					End_RSRP,
					End_RSRQ,
					UL_Data_Volume,
					UL_Throughput,		
					DL_Data_Volume,
					DL_Throughput ,
					MSISDN,					
					CALL_ID,					
					Call_Setup_Time,
					CS_Erlang,
	
					RRC_REQUEST_TYPE 
					
					
					FROM ',GT_DB,'.TMP_TABLE_CALL_EXPORT_LTE 		
					INTO OUTFILE ''',FOLDER_PATH,'/LTE',RNC_ID,'_',DATA_DATE,'_',DATA_QRT,'.csv''
					FIELDS TERMINATED BY ''\t''
					OPTIONALLY ENCLOSED BY ''''
					LINES TERMINATED BY ''\n'';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	END IF;
	
	SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS ',GT_DB,'.TMP_TABLE_CALL_EXPORT_LTE;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS ',GT_DB,'.TMP_nt_tac_cell_current_lte;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS ',GT_DB,'.TMP_dim_handset_id;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS ',GT_DB,'.TMP_dim_handset_m_id;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	INSERT INTO gt_gw_main.sp_log VALUES('gt_gw_main','SP_Sub_Table_Call_Export',CONCAT(CONNECTION_ID(),' Done: ',TIMESTAMPDIFF(SECOND,START_TIME,SYSDATE()),' seconds.'), NOW());
END$$
DELIMITER ;
