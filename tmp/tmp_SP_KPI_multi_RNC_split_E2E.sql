CREATE DEFINER=`covmo`@`%` PROCEDURE `SP_KPI_multi_RNC_split_E2E`(IN KPI_ID INT(11) ,IN PU VARCHAR(3000),IN START_TIME VARCHAR(50),IN END_TIME VARCHAR(50)
							,IN IMSI VARCHAR(4096),IN TECH_MASK TINYINT(4),IN GT_COVMO VARCHAR(20))
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
	DECLARE PU_ALL VARCHAR(10240) DEFAULT '';
	DECLARE DU_DATE VARCHAR(25);
	DECLARE IMSI_STR MEDIUMTEXT DEFAULT '';
	DECLARE IMSI_CONCAT MEDIUMTEXT DEFAULT '';
	DECLARE IMSI_PU SMALLINT DEFAULT NULL;
	DECLARE DS_AP_IP VARCHAR(20);
	DECLARE DS_AP_PORT VARCHAR(5);
	
	DECLARE START_DATE DATE DEFAULT DATE(START_TIME);
	DECLARE END_DATE DATE DEFAULT DATE(END_TIME);
	DECLARE EXIT HANDLER FOR 12701
	
	BEGIN
		SELECT 'Database server is currently busy, try again later.' AS IsSuccess;	
		
	END;
	
	DECLARE CONTINUE HANDLER FOR 1034
	BEGIN
		IF IMSI_PU IS NULL THEN
			SET IMSI_PU = -1;
		ELSEIF IMSI_PU = -1 THEN
			SET @PU_CNT= 0;
		END IF;
		SELECT '{tech:”ALL ”, name:”SP-Report”, status:”2”,message_id: “null”, message: “SP_KPI_multi_RNC_split_E2E Failed Incorrect key file for table; try to repair it”, log_path: “”}' AS message;
	END;
		
	SET SESSION group_concat_max_len=@@max_allowed_packet;
	
	SELECT CONCAT('gt_nt_',DATE_FORMAT(END_DATE,'%Y%m%d')) INTO NT_DB;
	
	INSERT INTO gt_gw_main.sp_log VALUES('gt_gw_main','SP_KPI_multi_RNC_split_E2E',CONCAT(KPI_ID,' Start'), NOW());
	SET @report_id=NULL;
	SET @column_crt_str=NULL;
	SET @column_col_str=NULL;
	SET @column_mrg_str=NULL;
	SET @sp_name=NULL;
	SET @SqlCmd=CONCAT('SELECT DISTINCT report_id,column_crt_str,column_col_str,column_mrg_str,sp_name,where_col,group_col
				INTO @report_id,@column_crt_str,@column_col_str,@column_mrg_str,@sp_name,@where_col,@group_col
				FROM gt_gw_main.tbl_rpt_other
				WHERE report_id=',KPI_ID,CASE WHEN KPI_ID IN (110006,110022) THEN CONCAT(' AND tech_mask=',TECH_MASK) ELSE '' END,';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	IF PU='' THEN 	
		SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS tmp_imsi_pu_',WORKER_ID,' ;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
	
		SET @SqlCmd=CONCAT('CREATE TEMPORARY TABLE tmp_imsi_pu_',WORKER_ID,' 
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
		
		IF KPI_ID IN (110001,110005,110020) THEN
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
	
		IF  IMSI_CONCAT <>'' THEN 
			SET @SqlCmd=CONCAT('SELECT GROUP_CONCAT(`rnc`) into @vendor_rnc_id FROM `gt_covmo`.`rnc_information` WHERE vendor_id =2;');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
 			SET @SqlCmd=CONCAT('SELECT spider_bg_direct_sql(CONCAT('' SELECT DISTINCT CONCAT(''''gt_'''',`PU_ID`,''''_'''',DATE_FORMAT(`DATA_DATE`,''''%Y%m%d''''),''''_0000_0000''''),`DATA_DATE`,`PU_ID`,`TECH_MASK`,CASE `TECH_MASK` WHEN 1 THEN ''''GSM'''' WHEN 2 THEN ''''UMTS'''' WHEN 4 THEN ''''LTE'''' ELSE NULL END AS `TECHNOLOGY` FROM `gt_global_imsi`.`table_imsi_pu` WHERE `IMSI` IN ',REPLACE(IN_QUOTE(IMSI_CONCAT),"'","''"),' AND `DATA_DATE`>=''''',START_DATE,''''' AND `DATA_DATE`<''''',DATE_ADD(END_DATE,INTERVAL 1 DAY),''''' '
						,CASE TECH_MASK WHEN 1 THEN ' AND `TECH_MASK`=1' WHEN 2 THEN ' AND `TECH_MASK`=2' WHEN 3 THEN ' AND `TECH_MASK` IN (1,2)' 
								WHEN 4 THEN ' AND `TECH_MASK`=4' WHEN 5 THEN ' AND `TECH_MASK` IN (1,4)' WHEN 6 THEN ' AND `TECH_MASK` IN (2,4)' ELSE '' END ,' 
						AND PU_ID NOT IN (',@vendor_rnc_id,')
						AND IMSI IN (',IMSI_CONCAT,')
						;'') 
						, ''tmp_imsi_pu_',WORKER_ID,'''
						, CONCAT(''HOST ''''',@AP_IP,''''', PORT ''''',@AP_PORT,''''',USER ''''',@AP_USER,''''', PASSWORD ''''',@AP_PSWD,''''''')
						) INTO @bb
						;');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
		END IF;	
	
		IF IMSI_PU IS NULL THEN
	
			SET @SqlCmd=CONCAT('SELECT COUNT(*) INTO @PU_CNT FROM tmp_imsi_pu_',WORKER_ID,';');
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
						FROM TMP_IMSI_PU_',WORKER_ID,' C
						WHERE C.PU_DATE=A.`SESSION_DB` AND C.TECHNOLOGY=B.TECHNOLOGY);');
		ELSEIF @PU_CNT<0 THEN 
			SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS tmp_imsi_pu_bkp_',WORKER_ID,' ;');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT('CREATE TEMPORARY TABLE tmp_imsi_pu_bkp_',WORKER_ID,' 
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
	
			SET @SqlCmd=CONCAT('SELECT GROUP_CONCAT(`rnc`) into @vendor_rnc_id FROM `gt_covmo`.`rnc_information` WHERE vendor_id =2;');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT('SELECT spider_bg_direct_sql(CONCAT('' SELECT DISTINCT CONCAT(''''gt_'''',`PU_ID`,''''_'''',DATE_FORMAT(`DATA_DATE`,''''%Y%m%d''''),''''_0000_0000''''),`DATA_DATE`,`PU_ID`,`TECH_MASK`,CASE `TECH_MASK` WHEN 1 THEN ''''GSM'''' WHEN 2 THEN ''''UMTS'''' WHEN 4 THEN ''''LTE'''' ELSE NULL END AS `TECHNOLOGY` FROM `gt_global_imsi`.`table_imsi_pu_bkp` WHERE `IMSI` IN ',REPLACE(IN_QUOTE(IMSI_CONCAT),"'","''"),' AND `DATA_DATE`>=''''',START_DATE,''''' AND `DATA_DATE`<''''',DATE_ADD(END_DATE,INTERVAL 1 DAY),''''' '
						,CASE TECH_MASK WHEN 1 THEN ' AND `TECH_MASK`=1' WHEN 2 THEN ' AND `TECH_MASK`=2' WHEN 3 THEN ' AND `TECH_MASK` IN (1,2)' 
								WHEN 4 THEN ' AND `TECH_MASK`=4' WHEN 5 THEN ' AND `TECH_MASK` IN (1,4)' WHEN 6 THEN ' AND `TECH_MASK` IN (2,4)' ELSE '' END  ,'
								AND PU_ID NOT IN (',@vendor_rnc_id,')
								AND IMSI IN (',IMSI_CONCAT,')
						;'') 
						, ''tmp_imsi_pu_bkp_',WORKER_ID,'''
						, CONCAT(''HOST ''''',@AP_IP,''''', PORT ''''',@AP_PORT,''''',USER ''''',@AP_USER,''''', PASSWORD ''''',@AP_PSWD,''''''')
						) INTO @bb
						;');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			IF @PU_CNT = -1 THEN 
				
				SET @SqlCmd=CONCAT('SELECT COUNT(*) INTO @PU_CNT2 FROM tmp_imsi_pu_bkp_',WORKER_ID,';');
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
									FROM tmp_imsi_pu_bkp_',WORKER_ID,' C
									WHERE C.PU_DATE=A.`SESSION_DB` AND C.TECHNOLOGY=B.TECHNOLOGY);');
				END IF;
			END IF;
		ELSE
			SELECT 'No Data available!' AS NoSessionAvailable;
			LEAVE a_label;
		END IF;	
		
	
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
				
		SET PU_ALL=IFNULL(@PU_GC,'');
	
	END IF;
	
	IF PU_ALL='' THEN 
		SELECT 'No Data available!' AS NoSessionAvailable;
		LEAVE a_label;
	ELSE 
		SET SP_Para_STR = CONCAT(WORKER_ID,','
					
					,CASE WHEN IMSI='' AND IMSI_CONCAT='' THEN '''''' ELSE CONCAT('''',IMSI_CONCAT,'''') END
			
					);
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
		SET @v_START_TIME=START_TIME;
		SET @v_END_TIME=END_TIME;	
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
					CALL gt_schedule.sp_job_add_task_upd(CONCAT('CALL gt_gw_main.SP_KPI_multi_split_E2E(',KPI_ID,',''',SESSION_NAME,''',''',START_TIME,''',''',END_TIME,''','
												,SP_Para_STR 
												,',''',GT_COVMO,''',''',gt_strtok(SPLIT_STR(PU_ALL,',',@v_i),2,'|'),''',''',GT_DB,''',''',TARGET_TABLE,''',''',@column_crt_str,''',''',@column_col_str,''',''',@sp_name,''',''',DS_AP_IP,''',''',DS_AP_PORT,''',''',@AP_USER,''',''',@AP_PSWD,''');'),@V_Multi_PU);
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
		CALL gt_schedule.sp_job_enable_event();
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
			
			SET @SqlCmd=CONCAT('CREATE TABLE ',GT_DB,'.',CCQ_TABLE,'(',@column_crt_str,') ENGINE=MRG_MYISAM DEFAULT CHARSET=utf8 INSERT_METHOD=FIRST UNION=(',UNION_STR,')');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt; 
		
			IF @group_col IS NOT NULL AND KPI_ID<>110020 THEN 
				SET DU_DATE=CASE WHEN START_DATE=END_DATE THEN CONCAT('''',START_DATE,'''') ELSE CONCAT('''',START_DATE,'~',END_DATE,'''') END;	
				SET @column_mrg_str=REPLACE(@column_mrg_str,'DU_DATE',DU_DATE);
			ELSE 
				SET @column_mrg_str=REPLACE(@column_mrg_str,'DU_DATE','DS_DATE');
			END IF;
				
			SET @SqlCmd=CONCAT('SELECT SQL_CALC_FOUND_ROWS ',@column_mrg_str,' FROM ',GT_DB,'.',CCQ_TABLE
						,CASE WHEN KPI_ID IN (110005,110020) THEN CONCAT(' A LEFT JOIN ',GT_DB,'.tmp_dim_handset B ON gt_strtok(A.HANDSET,1,''|'')=B.MAKE_ID AND gt_strtok(A.HANDSET,2,''|'')=B.MODEL_ID ') 
							WHEN KPI_ID IN (110001) THEN CONCAT(' A LEFT JOIN ',GT_DB,'.tmp_dim_handset B ON A.MAKE_ID=B.MAKE_ID AND A.MODEL_ID=B.MODEL_ID ') ELSE '' END
						,CASE WHEN KPI_ID IN (110019) THEN CONCAT(' A LEFT JOIN ',GT_DB,'.tmp_nt_cell_lte B ON A.ENODEB_ID=B.ENODEB_ID AND A.CELL_ID=B.CELL_ID ') ELSE '' END
  						,CASE WHEN @where_col IS NULL THEN '' ELSE CONCAT(' WHERE ',@where_col) END
  						,CASE WHEN @group_col IS NULL THEN '' ELSE CONCAT(' GROUP BY ',@group_col) END
					
					
						,';');	
			
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
		
		
			SELECT FOUND_ROWS() INTO @V_CNT;
			
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
			
			INSERT INTO `gt_gw_main`.`tbl_rpt_error` (`PID`,`CreateTime`,`error_str`) VALUES (WORKER_ID,NOW(),CONCAT(WORKER_ID,' Main Parallel Jobs Fail - SP_CovMo_Cross_Query'));
			SIGNAL SP_ERROR
				SET MESSAGE_TEXT = 'Main Parallel Jobs Fail - SP_KPI_multi_RNC_split';
		END IF;
	END IF;	
