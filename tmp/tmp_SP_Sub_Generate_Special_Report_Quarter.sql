CREATE DEFINER=`covmo`@`%` PROCEDURE `SP_Sub_Generate_Special_Report_Quarter`(IN GT_DB VARCHAR(100),IN PATH VARCHAR(100),IN VENDOR_ID TINYINT(4))
BEGIN
       	DECLARE RNC_ID INT;
	DECLARE START_TIME DATETIME DEFAULT SYSDATE();
	DECLARE START_DATE VARCHAR(10) DEFAULT CONCAT(LEFT(gt_strtok(GT_DB,3,'_'),4),'-',SUBSTRING(gt_strtok(GT_DB,3,'_'),5,2),'-',SUBSTRING(gt_strtok(GT_DB,3,'_'),7,2));
	DECLARE START_HH VARCHAR(2) DEFAULT LEFT(gt_strtok(GT_DB,4,'_'),2);
	DECLARE START_MM VARCHAR(2) DEFAULT RIGHT(gt_strtok(GT_DB,4,'_'),2);
	DECLARE WORKER_ID VARCHAR(10) DEFAULT CONNECTION_ID();
	DECLARE GT_DB_START_MIN VARCHAR(10) DEFAULT SUBSTRING(RIGHT(GT_DB,18),12,2);
	DECLARE SH_EH VARCHAR(9) DEFAULT RIGHT(GT_DB,9);
	DECLARE SH VARCHAR(4) DEFAULT gt_strtok(SH_EH,1,'_');
	DECLARE DY_GT_DB VARCHAR(100);
	#DECLARE pilot_rscp SMALLINT;
	#DECLARE pilot_ecn0 SMALLINT;
	#DECLARE pilot_rscp_delta SMALLINT;
	#DECLARE pilot_pollution_trigger SMALLINT;
	DECLARE interfere_rscp SMALLINT;
	DECLARE interfere_ecn0 SMALLINT;
	DECLARE CURRENT_NT_DB VARCHAR(50) DEFAULT CONCAT('gt_nt_',DATE_FORMAT(START_DATE,'%Y%m%d'));
	
	SELECT REPLACE(GT_DB,SH_EH,'0000_0000') INTO DY_GT_DB;
		
        SET SESSION max_heap_table_size = 1024*1024*1024*4; 
        SET SESSION tmp_table_size = 1024*1024*1024*6; 
        SET SESSION join_buffer_size = 1024*1024*1024*1; 
        SET SESSION sort_buffer_size = 1024*1024*1024*1; 
        SET SESSION read_buffer_size = 1024*1024*1024*1; 
	
        /*SELECT att_value INTO pilot_rscp FROM CURRENT_NT_DB.`sys_config` WHERE `group_name`='pilot' AND att_name = 'pilot_rscp';
	SELECT att_value INTO pilot_ecn0 FROM CURRENT_NT_DB.`sys_config` WHERE `group_name`='pilot' AND att_name ='pilot_ecn0';
	SELECT att_value INTO pilot_rscp_delta FROM CURRENT_NT_DB.`sys_config` WHERE `group_name`='pilot' AND att_name ='pilot_rscp_delta';
	SELECT att_value INTO pilot_pollution_trigger FROM CURRENT_NT_DB.`sys_config` WHERE `group_name`='pilot' AND att_name ='pilot_pollution_trigger';*/
	
	SET @SqlCmd=CONCAT('SELECT att_value INTO @pilot_rscp FROM ',CURRENT_NT_DB,'.`sys_config` WHERE `group_name`=''pilot'' AND att_name = ''pilot_rscp'';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	SET @SqlCmd=CONCAT('SELECT att_value INTO @pilot_ecn0 FROM ',CURRENT_NT_DB,'.`sys_config` WHERE `group_name`=''pilot'' AND att_name =''pilot_ecn0'';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	SET @SqlCmd=CONCAT('SELECT att_value INTO @pilot_rscp_delta FROM ',CURRENT_NT_DB,'.`sys_config` WHERE `group_name`=''pilot'' AND att_name =''pilot_rscp_delta'';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	SET @SqlCmd=CONCAT('SELECT att_value INTO @pilot_pollution_trigger FROM ',CURRENT_NT_DB,'.`sys_config` WHERE `group_name`=''pilot'' AND att_name =''pilot_pollution_trigger'';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
 	SELECT SUBSTRING(SUBSTRING(GT_DB, 4,LENGTH(GT_DB)-4),1, LOCATE('_', SUBSTRING(GT_DB, 4,LENGTH(GT_DB)-4))-1) INTO RNC_ID;
 
	INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'SP_Sub_Generate_Special_Report_Quarter','Start', NOW());
		
	SET @DATA_QUARTER=
		CASE WHEN GT_DB_START_MIN = 00 THEN 0
		     WHEN GT_DB_START_MIN = 15 THEN 1
		     WHEN GT_DB_START_MIN = 30 THEN 2
		     WHEN GT_DB_START_MIN = 45 THEN 3 END;
	
	
	INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'SP_Sub_Generate_Special_Report_Quarter','step 1', NOW());
	
	SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS  ',DY_GT_DB,'.`tmp_sh_table_first','_',WORKER_ID,'` ;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;	
	
	SET @SqlCmd=CONCAT('CREATE TEMPORARY TABLE ',DY_GT_DB,'.`tmp_sh_table_first','_',WORKER_ID,'` ENGINE=MYISAM
				SELECT POS_FIRST_RNC RNC_ID 
				,POS_FIRST_CELL CELL_ID 
				,COUNT(POS_FIRST_CELL) AS init_call_count
				,AVG(POS_FIRST_RSCP) AS init_rscp
				,AVG(POS_FIRST_ECN0) AS init_ecn0
				,SUM(DL_TRAFFIC_VOLUME)/1024 AS dl_vol
				,SUM(UL_TRAFFIC_VOLUME)/1024 AS ul_vol
				,SUM(CASE WHEN CALL_TYPE IN (12,13,14,18) THEN 1 ELSE 0 END) AS ps_call_count
				,MAX(DL_THROUGHPUT_AVG) AS max_dl_thro
				,MAX(UL_THROUGHPUT_AVG) AS max_ul_thro
				,SUM(CASE CALL_STATUS WHEN 3 THEN 1 ELSE 0 END) AS block
				,AVG(CASE `SIMULATED` WHEN 0 THEN POS_FIRST_RSCP ELSE NULL END) AS init_pilot_rscp
				,AVG(CASE `SIMULATED` WHEN 0 THEN POS_FIRST_ECN0 ELSE NULL END) AS init_pilot_ecn0
				,SUM(CASE WHEN CALL_STATUS IN (1) THEN 1 ELSE 0 END) AS NORMAL_CALL_COUNT
				,SUM(CASE WHEN CALL_STATUS IN (2) THEN 1 ELSE 0 END) AS DROP_CALL_COUNT
				,SUM(CASE WHEN CALL_STATUS IN (3) THEN 1 ELSE 0 END) AS BLOCK_CALL_COUNT
				FROM ',DY_GT_DB,'.`table_call_',SH,'`
				WHERE POS_FIRST_CELL IS NOT NULL AND POS_FIRST_RSCP IS NOT NULL AND DATA_HOUR=',START_HH,'
				GROUP BY POS_FIRST_RNC,POS_FIRST_CELL;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	
	INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'SP_Sub_Generate_Special_Report_Quarter','step 3', NOW());
	
	SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS  ',DY_GT_DB,'.`tmp_sh_table_start','_',WORKER_ID,'` ;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;	
	
	SET @SqlCmd=CONCAT('CREATE TEMPORARY TABLE ',DY_GT_DB,'.`tmp_sh_table_start','_',WORKER_ID,'` ENGINE=MYISAM
				SELECT 
					RNC_ID,CELL_ID
					#,SUM(IF((CALL_TYPE=13 OR CALL_TYPE=12 OR CALL_TYPE=14 OR CALL_TYPE=18),DL_DATA_THRU*CALL_CNT, 0))/SUM(IF((CALL_TYPE=13 OR CALL_TYPE=12 OR CALL_TYPE=14 OR CALL_TYPE=18),CALL_CNT, 0)) AS dl_thro
					#,SUM(IF((CALL_TYPE=13 OR CALL_TYPE=12 OR CALL_TYPE=14 OR CALL_TYPE=18),UL_DATA_THRU*CALL_CNT, 0))/SUM(IF((CALL_TYPE=13 OR CALL_TYPE=12 OR CALL_TYPE=14 OR CALL_TYPE=18),CALL_CNT, 0)) AS ul_thro
					,SUM(IF((CALL_TYPE=13 OR CALL_TYPE=12 OR CALL_TYPE=14 OR CALL_TYPE=18),DL_DATA_THRU, 0)) AS dl_thro
					,SUM(IF((CALL_TYPE=13 OR CALL_TYPE=12 OR CALL_TYPE=14 OR CALL_TYPE=18),UL_DATA_THRU, 0)) AS ul_thro
				FROM (
				    SELECT
						 DATA_DATE
						, DATA_HOUR
						, POS_FIRST_FREQUENCY AS FREQUENCY
						, POS_FIRST_UARFCN AS UARFCN
						, INDOOR
						, MOVING
						, gt_covmo_proj_geohash_to_hex_geohash(POS_FIRST_LOC)  AS TILE_ID
						, POS_FIRST_RNC AS RNC_ID
						, POS_FIRST_CELL_INDOOR AS CELL_INDOOR
						, POS_FIRST_CLUSTER AS CLUSTER_ID
						, POS_FIRST_SITE AS SITE_ID
						, POS_FIRST_CELL AS CELL_ID
						, CALL_TYPE 
						, CALL_STATUS
						, COUNT(POS_FIRST_CELL) AS CALL_CNT
						, SUM(RRC_CONNECT_DURATION/1000)/3600 AS ERLANG
						, SUM(DL_TRAFFIC_VOLUME)*8/3600 AS DL_DATA_THRU
						, SUM(UL_TRAFFIC_VOLUME)*8/3600 AS UL_DATA_THRU
					FROM ',DY_GT_DB,'.`table_call_',SH,'`
					WHERE POS_FIRST_RNC =',RNC_ID,'
					AND POS_FIRST_RSCP IS NOT NULL
					AND DATA_HOUR=',START_HH,'
					GROUP BY  DATA_DATE
						, DATA_HOUR
						, POS_FIRST_FREQUENCY 
						, POS_FIRST_UARFCN 
						, INDOOR
						, MOVING
						, gt_covmo_proj_geohash_to_hex_geohash(POS_FIRST_LOC)
						, POS_FIRST_RNC
						, POS_FIRST_CELL_INDOOR
						, POS_FIRST_CLUSTER
						, POS_FIRST_SITE
						, POS_FIRST_CELL
						, CALL_TYPE 
						, CALL_STATUS
				) A
				GROUP BY RNC_ID,CELL_ID HAVING 1
				;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'SP_Sub_Generate_Special_Report_Quarter','step 4', NOW());
	
	SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS  ',DY_GT_DB,'.`tmp_sh_table_last','_',WORKER_ID,'` ;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('CREATE TEMPORARY TABLE ',DY_GT_DB,'.`tmp_sh_table_last','_',WORKER_ID,'` ENGINE=MYISAM
				SELECT POS_LAST_RNC AS RNC_ID 
					,POS_LAST_CELL AS CELL_ID 
					,COUNT(POS_LAST_CELL) AS end_call_count
					,SUM(IRAT_HHO_ATTEMPT) AS irat_att
					,SUM(IRAT_HHO_SUCCESS) AS irat_success
				FROM ',DY_GT_DB,'.`table_call_',SH,'`
				WHERE POS_LAST_RNC = ',RNC_ID,' AND DATA_HOUR=',START_HH,'
				GROUP BY POS_LAST_RNC,POS_LAST_CELL;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS  ',DY_GT_DB,'.`tmp_sh_table_last2','_',WORKER_ID,'` ;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	SET @SqlCmd=CONCAT('CREATE TEMPORARY TABLE ',DY_GT_DB,'.`tmp_sh_table_last2','_',WORKER_ID,'` ENGINE=MYISAM
			SELECT 
				RNC_ID,CELL_ID
				,SUM(BEST_RSCP_1*CALL_CNT)/SUM(CALL_CNT) AS end_pilot_rscp
				,SUM(BEST_ECN0_1*CALL_CNT)/SUM(CALL_CNT) AS end_pilot_ecn0
				FROM (
			SELECT
				 DATA_DATE
				, DATA_HOUR
				, INDOOR
				, MOVING
				, gt_covmo_proj_geohash_to_hex_geohash(POS_LAST_LOC)  AS TILE_ID
				, POS_LAST_RNC AS RNC_ID
				, POS_LAST_CELL_INDOOR AS CELL_INDOOR
				, POS_LAST_CLUSTER AS CLUSTER_ID
				, POS_LAST_SITE AS SITE_ID
				, POS_LAST_FREQUENCY AS FREQUENCY 
				, POS_LAST_UARFCN AS UARFCN 
				, POS_LAST_CELL AS CELL_ID
				, CALL_TYPE 
				, CALL_STATUS
				, COUNT(POS_LAST_CELL) AS CALL_CNT
				, SUM(IF(IU_RELEASE_CAUSE = 14,1,0)) AS CAUSE_14_CNT
				, SUM(IF(IU_RELEASE_CAUSE = 15,1,0)) AS CAUSE_15_CNT
				, SUM(IF(IU_RELEASE_CAUSE = 46,1,0)) AS CAUSE_46_CNT
				, SUM(IF(IU_RELEASE_CAUSE = 115,1,0)) AS CAUSE_115_CNT
				, SUM(IF(IU_RELEASE_CAUSE NOT IN (14,15,46,115) AND CALL_STATUS =2,1,0)) AS CAUSE_OTHERS_CNT
				, AVG(POS_LAST_RSCP) AS BEST_RSCP_1
				, AVG(POS_LAST_ECN0) AS BEST_ECN0_1
				, median(POS_LAST_RSCP) AS BEST_RSCP_1_MED
				, median(POS_LAST_ECN0) AS BEST_ECN0_1_MED
				-- , SUM(DURATION/1000)/3600 AS ERLANG
				, SUM(IRAT_HHO_ATTEMPT) AS IRAT_HHO_ATTEMPT
				, SUM(IRAT_HHO_SUCCESS) AS IRAT_HHO_SUCCESS
				FROM ',DY_GT_DB,'.`table_call_',SH,'`
				WHERE POS_LAST_RNC=',RNC_ID,'
				AND DATA_HOUR=',START_HH,'
				GROUP BY 
						DATA_DATE
						, DATA_HOUR
						, INDOOR
						, MOVING
						, gt_covmo_proj_geohash_to_hex_geohash(POS_LAST_LOC)
						, POS_LAST_RNC
						, POS_LAST_CELL_INDOOR
						, POS_LAST_CLUSTER
						, POS_LAST_SITE
						, POS_LAST_FREQUENCY
						, POS_LAST_UARFCN
						, POS_LAST_CELL
						, CALL_TYPE 
						, CALL_STATUS
				ORDER BY NULL
			) A
			WHERE BEST_RSCP_1 IS NOT NULL
			GROUP BY RNC_ID,CELL_ID
			;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;	
	
	
	INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'SP_Sub_Generate_Special_Report_Quarter','step 5', NOW());
	
	SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS  ',DY_GT_DB,'.`tmp_sh_table_fp','_',WORKER_ID,'` ;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('CREATE TEMPORARY TABLE ',DY_GT_DB,'.`tmp_sh_table_fp','_',WORKER_ID,'` ENGINE=MYISAM
				SELECT  gt_covmo_csv_get(RNC_ID,1) RNC_ID
					, gt_covmo_csv_get(CELL_ID,1) CELL_ID
					, COUNT(*) AS mr_count
					, AVG(gt_covmo_csv_get(RSCP,1)) AS fp_pilot_rscp
					, AVG(gt_covmo_csv_get(ECN0,1)) AS fp_pilot_ecn0
					, AVG(gt_covmo_csv_count(ACTIVE_SET_CELL_ID, '','')) AS as_count
					, SUM(IF(gt_covmo_polluter(RSCP, ECN0,',@pilot_rscp,',',@pilot_ecn0,',',@pilot_rscp_delta,')>',@pilot_pollution_trigger,',gt_covmo_polluter(RSCP, ECN0,',@pilot_rscp,',',@pilot_ecn0,',',@pilot_rscp_delta,')-',@pilot_pollution_trigger,',0)) AS POLLUTER
					, SUM(IF(gt_covmo_polluter(RSCP, ECN0,',@pilot_rscp,',',@pilot_ecn0,',',@pilot_rscp_delta,')>',@pilot_pollution_trigger,',1,0)) AS polluted_mr	
				FROM ',DY_GT_DB,'.`table_call_',SH,'` A  FORCE INDEX (CALL_ID), ',DY_GT_DB,'.table_position B FORCE INDEX (CALL_ID)
				WHERE A.CALL_ID=B.CALL_ID
				AND gt_covmo_csv_getnum(B.RNC_ID,1)=',RNC_ID,'
				AND RSCP IS NOT NULL 
				AND A.DATA_HOUR=',START_HH,' AND B.DATA_HOUR=',START_HH,'
				AND EVENT_ID NOT IN (''20'',''21'',''22'',''23'',''24'',''25'',''26'',''27'',''28'')
				GROUP BY gt_covmo_csv_get(RNC_ID,1),gt_covmo_csv_get(CELL_ID,1)
				ORDER BY NULL;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'SP_Sub_Generate_Special_Report_Quarter','step 6', NOW());
	
	
	SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS  ',DY_GT_DB,'.`tmp_sh_table_status2','_',WORKER_ID,'` ;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('CREATE TEMPORARY TABLE ',DY_GT_DB,'.`tmp_sh_table_status2','_',WORKER_ID,'` ENGINE=MYISAM
				SELECT  POS_LAST_RNC RNC_ID
					,POS_LAST_CELL CELL_ID
					,SUM(CASE WHEN CALL_STATUS IN (1) THEN 1 ELSE 0 END) AS NORMAL_CALL_COUNT
					,SUM(CASE WHEN CALL_STATUS IN (2) THEN 1 ELSE 0 END) AS DROP_CALL_COUNT
				FROM ',DY_GT_DB,'.`table_call_',SH,'`
				WHERE POS_LAST_RNC IS NOT NULL AND POS_LAST_CELL IS NOT NULL AND DATA_HOUR=',START_HH,'
				GROUP BY POS_LAST_RNC,POS_LAST_CELL
				ORDER BY NULL;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'SP_Sub_Generate_Special_Report_Quarter','step 7', NOW());
	
	SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS  ',DY_GT_DB,'.`tmp_special_quarter_report_csv','_',WORKER_ID,'` ;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('CREATE TEMPORARY TABLE ',DY_GT_DB,'.`tmp_special_quarter_report_csv','_',WORKER_ID,'`
				(
				  `DATA_DATE` DATE DEFAULT NULL,
				  `DATA_HOUR` TINYINT(4) DEFAULT NULL,
				  `DATA_QUARTER` TINYINT(4) DEFAULT NULL,
				  `RNC_ID` VARCHAR(50) DEFAULT NULL,
				  `CELL_ID` VARCHAR(50) DEFAULT NULL,
				  `INIT_CALL_COUNT` INT(11) DEFAULT NULL,
				  `INIT_RSCP` DOUBLE DEFAULT NULL,
				  `INIT_PILOT_RSCP` DOUBLE DEFAULT NULL,
				  `INIT_ECN0` DOUBLE DEFAULT NULL,
				  `INIT_PILOT_ECN0` DOUBLE DEFAULT NULL,
				  `END_CALL_COUNT` INT(11) DEFAULT NULL,
				  `FP_PILOT_RSCP` DOUBLE DEFAULT NULL,
				  `FP_PILOT_ECN0` DOUBLE DEFAULT NULL,
				  `END_PILOT_RSCP` DOUBLE DEFAULT NULL,
				  `END_PILOT_ECN0` DOUBLE DEFAULT NULL,
				  `DL_VOL` DOUBLE DEFAULT NULL,
				  `UL_VOL` DOUBLE DEFAULT NULL,
				  `PS_CALL_COUNT` INT(11) DEFAULT NULL,
				  `DL_THRO` DOUBLE DEFAULT NULL,
				  `UL_THRO` DOUBLE DEFAULT NULL,
				  `MAX_DL_THRO` DOUBLE DEFAULT NULL,
				  `MAX_UL_THRO` DOUBLE DEFAULT NULL,
				  `BLOCK_RATE` DOUBLE DEFAULT NULL,
				  `DROP_RATE` DOUBLE DEFAULT NULL,
				  `BLOCK_CALL_COUNT` INT(11) DEFAULT NULL,
				  `DROP_CALL_COUNT` INT(11) DEFAULT NULL,
				  `NORMAL_CALL_COUNT` INT(11) DEFAULT NULL,
				  `IFHO_COUNT` INT(11) DEFAULT NULL,
				  `IFHO_RATE` DOUBLE DEFAULT NULL,
				  `IRAT_ATT` INT(11) DEFAULT NULL,
				  `IRAT_SUCCESS_RATE` DOUBLE DEFAULT NULL,
				  `MR_COUNT` INT(11) DEFAULT NULL,
				  `POLLUTED_MR` DOUBLE DEFAULT NULL,
				  `POLLUTER` DOUBLE DEFAULT NULL,
				  `AS_COUNT` DOUBLE DEFAULT NULL				
				) ENGINE=MYISAM DEFAULT CHARSET=LATIN1;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'SP_Sub_Generate_Special_Report_Quarter','step 8', NOW());
	
	SET @SqlCmd=CONCAT('INSERT INTO ',DY_GT_DB,'.`tmp_special_quarter_report_csv','_',WORKER_ID,'` ( RNC_ID,CELL_ID)
				SELECT RNC_ID,CELL_ID
				FROM 
				(
					SELECT RNC_ID,CELL_ID FROM ',DY_GT_DB,'.`tmp_sh_table_first','_',WORKER_ID,'`
					UNION 
					SELECT RNC_ID,CELL_ID FROM ',DY_GT_DB,'.`tmp_sh_table_last','_',WORKER_ID,'`
					UNION 
					SELECT RNC_ID,CELL_ID FROM ',DY_GT_DB,'.`tmp_sh_table_fp','_',WORKER_ID,'`
					UNION 
					SELECT RNC_ID,CELL_ID FROM ',DY_GT_DB,'.`tmp_sh_table_status2','_',WORKER_ID,'`
					UNION 
					SELECT RNC_ID,CELL_ID FROM ',DY_GT_DB,'.`tmp_sh_table_start','_',WORKER_ID,'`
				) AA;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'SP_Sub_Generate_Special_Report_Quarter','step 9', NOW());
	
	SET @SqlCmd=CONCAT('UPDATE ',DY_GT_DB,'.`tmp_special_quarter_report_csv','_',WORKER_ID,'` a,',DY_GT_DB,'.`tmp_sh_table_first','_',WORKER_ID,'` B
				SET 
					A.INIT_CALL_COUNT=B.INIT_CALL_COUNT
					,A.INIT_RSCP=B.INIT_RSCP
					,A.INIT_ECN0=B.INIT_ECN0
					,A.DL_VOL=B.DL_VOL
					,A.UL_VOL=B.UL_VOL
					,A.PS_CALL_COUNT=B.PS_CALL_COUNT
					,A.MAX_DL_THRO=B.MAX_DL_THRO
					,A.MAX_UL_THRO=B.MAX_UL_THRO
					,A.BLOCK_RATE=B.BLOCK/B.INIT_CALL_COUNT
					,A.INIT_PILOT_RSCP=B.INIT_PILOT_RSCP
					,A.INIT_PILOT_ECN0=B.INIT_PILOT_ECN0
					,A.BLOCK_CALL_COUNT=B.BLOCK_CALL_COUNT
					,A.NORMAL_CALL_COUNT=B.NORMAL_CALL_COUNT		
					,A.BLOCK_RATE=B.BLOCK_CALL_COUNT/(B.NORMAL_CALL_COUNT + B.BLOCK_CALL_COUNT + B.DROP_CALL_COUNT)
				WHERE A.RNC_ID=B.RNC_ID AND A.CELL_ID=B.CELL_ID;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	
	SET @SqlCmd=CONCAT('UPDATE ',DY_GT_DB,'.`tmp_special_quarter_report_csv','_',WORKER_ID,'`
				SET 
					IFHO_RATE=IFHO_COUNT/INIT_CALL_COUNT
				WHERE IFHO_COUNT IS NOT NULL AND INIT_CALL_COUNT IS NOT NULL;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('UPDATE ',DY_GT_DB,'.`tmp_special_quarter_report_csv','_',WORKER_ID,'` a,',DY_GT_DB,'.`tmp_sh_table_start','_',WORKER_ID,'` B
				SET 
					A.DL_THRO=B.DL_THRO
					,A.UL_THRO=B.UL_THRO
				WHERE A.RNC_ID=B.RNC_ID AND A.CELL_ID=B.CELL_ID;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('UPDATE ',DY_GT_DB,'.`tmp_special_quarter_report_csv','_',WORKER_ID,'` a,',DY_GT_DB,'.`tmp_sh_table_status2','_',WORKER_ID,'` B
				SET 
					A.DROP_RATE=B.DROP_CALL_COUNT/(B.NORMAL_CALL_COUNT+B.DROP_CALL_COUNT)
					,A.DROP_CALL_COUNT=B.DROP_CALL_COUNT
				WHERE A.RNC_ID=B.RNC_ID AND A.CELL_ID=B.CELL_ID;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('UPDATE ',DY_GT_DB,'.`tmp_special_quarter_report_csv','_',WORKER_ID,'` a,',DY_GT_DB,'.`tmp_sh_table_last','_',WORKER_ID,'` B
				SET A.END_CALL_COUNT=B.END_CALL_COUNT
					,A.IRAT_ATT=B.IRAT_ATT
					,A.IRAT_SUCCESS_RATE=(B.IRAT_SUCCESS/B.IRAT_ATT)
				WHERE A.RNC_ID=B.RNC_ID AND A.CELL_ID=B.CELL_ID ;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('UPDATE ',DY_GT_DB,'.`tmp_special_quarter_report_csv','_',WORKER_ID,'` a,',DY_GT_DB,'.`tmp_sh_table_last2','_',WORKER_ID,'` B
				SET 
					A.END_PILOT_RSCP=B.END_PILOT_RSCP
					,A.END_PILOT_ECN0=B.END_PILOT_ECN0
				WHERE A.RNC_ID=B.RNC_ID AND A.CELL_ID=B.CELL_ID ;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('UPDATE ',DY_GT_DB,'.`tmp_special_quarter_report_csv','_',WORKER_ID,'` a,',DY_GT_DB,'.`tmp_sh_table_fp','_',WORKER_ID,'` B
				SET A.MR_COUNT=B.MR_COUNT
					,A.FP_PILOT_RSCP=B.FP_PILOT_RSCP
					,A.FP_PILOT_ECN0=B.FP_PILOT_ECN0
					,A.AS_COUNT=B.AS_COUNT
					,A.POLLUTER=B.POLLUTER
					,A.POLLUTED_MR=B.POLLUTED_MR
				WHERE A.RNC_ID=B.RNC_ID AND A.CELL_ID=B.CELL_ID ;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'SP_Sub_Generate_Special_Report_Quarter','step 10', NOW());
	SET @SqlCmd=CONCAT('SELECT ''DATA_DATE'',''DATA_HOUR'',''DATA_QUARTER'',''RNC_ID'',''CELL_ID'',
				''INIT_CALL_COUNT'',''INIT_RSCP'',''INIT_PILOT_RSCP'',''INIT_ECN0'',''INIT_PILOT_ECN0'',
				''END_CALL_COUNT'',''FP_PILOT_RSCP'',''FP_PILOT_ECN0'',''END_PILOT_RSCP'',''END_PILOT_ECN0'',
				''DL_VOL'',''UL_VOL'',''PS_CALL_COUNT'',''DL_THRO'',''UL_THRO'',
				''MAX_DL_THRO'',''MAX_UL_THRO'',''BLOCK_RATE'',
				''DROP_RATE'',''BLOCK_CALL_COUNT'',''DROP_CALL_COUNT'',''NORMAL_CALL_COUNT'',
				''IFHO_COUNT'',''IFHO_RATE'',
				''IRAT_ATT'',''IRAT_SUCCESS_RATE'',
				''MR_COUNT'',''POLLUTED_MR'',''POLLUTER'',''AS_COUNT''
			    UNION 
			    SELECT 
				''',START_DATE,''',
				''',START_HH,''',
				''',@DATA_QUARTER,''',
				`RNC_ID`,
				`CELL_ID`,
				COALESCE(`INIT_CALL_COUNT`),
				COALESCE(`INIT_RSCP`),
				COALESCE(`INIT_PILOT_RSCP`),
				COALESCE(`INIT_ECN0`),
				COALESCE(`INIT_PILOT_ECN0`),
				COALESCE(`END_CALL_COUNT`),
				COALESCE(`FP_PILOT_RSCP`),
				COALESCE(`FP_PILOT_ECN0`),
				COALESCE(`END_PILOT_RSCP`),
				COALESCE(`END_PILOT_ECN0`),
				COALESCE(`DL_VOL`),
				COALESCE(`UL_VOL`),
				COALESCE(`PS_CALL_COUNT`),
				COALESCE(`DL_THRO`),
				COALESCE(`UL_THRO`),
				COALESCE(`MAX_DL_THRO`),
				COALESCE(`MAX_UL_THRO`),
				COALESCE(`BLOCK_RATE`),
				COALESCE(`DROP_RATE`),
				COALESCE(`BLOCK_CALL_COUNT`),
				COALESCE(`DROP_CALL_COUNT`),
				COALESCE(`NORMAL_CALL_COUNT`),
				COALESCE(`IFHO_COUNT`),
				COALESCE(`IFHO_RATE`),
				COALESCE(`IRAT_ATT`),
				COALESCE(`IRAT_SUCCESS_RATE`),
				COALESCE(`MR_COUNT`),
				COALESCE(`POLLUTED_MR`),
				COALESCE(`POLLUTER`),
				COALESCE(`AS_COUNT`)
			    FROM ',DY_GT_DB,'.`tmp_special_quarter_report_csv','_',WORKER_ID,'`
			    WHERE `RNC_ID`=',RNC_ID,' 
			    INTO OUTFILE ''',PATH,'/',CASE VENDOR_ID 
					WHEN 1 THEN 'Ericsson'
					WHEN 2 THEN 'Huawei'
					WHEN 3 THEN 'Huawei'
					WHEN 7 THEN 'NSN'
				 END 
				,'-',RNC_ID,'-',START_DATE,'-',START_HH,'-'
				,CASE START_MM 
					WHEN '00' THEN '1'
					WHEN '15' THEN '2'
					WHEN '30' THEN '3'
					WHEN '45' THEN '4'
				 END 
				,'_SPECIAL_QUARTER.csv''
			    FIELDS TERMINATED BY '',''
			    LINES TERMINATED BY ''\n''
			    ;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE ',DY_GT_DB,'.`tmp_sh_table_first','_',WORKER_ID,'` ;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE ',DY_GT_DB,'.`tmp_sh_table_last','_',WORKER_ID,'` ;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;	
	
	SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE ',DY_GT_DB,'.`tmp_sh_table_fp','_',WORKER_ID,'` ;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;		
	
	SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE ',DY_GT_DB,'.`tmp_special_quarter_report_csv','_',WORKER_ID,'` ;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;	
	
	SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE ',DY_GT_DB,'.`tmp_sh_table_status2','_',WORKER_ID,'` ;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;	
	
	INSERT INTO gt_gw_main.SP_LOG VALUES(GT_DB,'SP_Sub_Generate_Special_Report_Quarter',CONCAT('Done: ',TIMESTAMPDIFF(SECOND,START_TIME,SYSDATE()),' seconds.'), NOW());	
	
        SET SESSION max_heap_table_size = 1024*1024*128; 
        SET SESSION tmp_table_size = 1024*1024*128; 
        SET SESSION join_buffer_size = 1024*1024*128; 
        SET SESSION sort_buffer_size = 1024*1024*128; 
        SET SESSION read_buffer_size = 1024*1024*128; 
	
