DELIMITER $$
USE `gt_gw_main`$$
DROP PROCEDURE IF EXISTS `SP_CreateDB_LTE`$$
CREATE DEFINER=`covmo`@`%` PROCEDURE `SP_KPI_Zerotraffic_Single`(IN KPI_ID INT(11) ,IN PU VARCHAR(100),IN START_DATE DATE,IN END_DATE DATE,IN START_HOUR TINYINT(2),IN END_HOUR TINYINT(2), IN SOURCE_TYPE TINYINT(2), IN SERVICE TINYINT(2)
							,IN DATA_QUARTER VARCHAR(10),IN CELL_ID VARCHAR(100),IN TILE_ID VARCHAR(100)
							,IN IMSI VARCHAR(4096),IN CLUSTER_ID VARCHAR(50),IN CALL_TYPE VARCHAR(30),IN CALL_STATUS VARCHAR(10),IN Mobility VARCHAR(10)
							,IN CELL_INDOOR VARCHAR(10),IN FREQUENCY VARCHAR(100) ,IN UARFCN VARCHAR(100),IN CELL_LON VARCHAR(50),IN CELL_LAT VARCHAR(50)
							,IN MSISDN VARCHAR(1024),IN IMEI_NEW VARCHAR(5000),IN APN VARCHAR(1024)
							,IN FILTER VARCHAR(1024),IN LIMITS VARCHAR(10),IN PID INT(11),IN SORT_STR VARCHAR(100),IN POS_KIND VARCHAR(10)
							,IN HAVING_STR VARCHAR(100),IN HIDE_IMEI_CNT TINYINT(2),IN SITE_ID VARCHAR(100)
							,IN MAKE_ID VARCHAR(1024),IN MODEL_ID VARCHAR(1024),IN POLYGON_STR VARCHAR(250),IN TECH_MASK TINYINT(4),IN WITHDUMP TINYINT(2),IN GT_COVMO VARCHAR(20),IN IMSI_GID SMALLINT(6),IN SPECIAL_IMSI TINYINT(2) ,IN SUB_REGION_ID VARCHAR(100),IN ENODEB_ID VARCHAR(100),IN CELL_GID INT(11))
a_label:
BEGIN	
	DECLARE WORKER_ID VARCHAR(10) DEFAULT CONNECTION_ID();
	DECLARE PU_ID INT;
	DECLARE NT_DB VARCHAR(100);
	DECLARE GT_DB VARCHAR(100);
	DECLARE FILTER_STR VARCHAR(10000);
	DECLARE POS_KIND_LOC VARCHAR(10) DEFAULT '';
	DECLARE DY_FLAG TINYINT DEFAULT '0';
	DECLARE PU_ALL VARCHAR(100);
	DECLARE IMSI_STR TEXT DEFAULT '';
	
	INSERT INTO gt_gw_main.sp_log VALUES('gt_gw_main','SP_KPI_Zerotraffic_Single',CONCAT(KPI_ID,' Start'), NOW());
	SET @SqlCmd=CONCAT('SELECT GROUP_CONCAT(DISTINCT CONCAT(A.SESSION_DB,''|'',B.`TECHNOLOGY`) SEPARATOR '','' ) INTO @PU_GC 
					FROM `gt_covmo`.`session_information` A,`gt_covmo`.`rnc_information` B
					WHERE `SESSION_START`>=''',START_DATE,''' AND `SESSION_END` <''',DATE_ADD(END_DATE,INTERVAL 1 DAY),'''
					AND A.`RNC`=B.`RNC`',CASE TECH_MASK WHEN 1 THEN ' AND B.`TECHNOLOGY`=''GSM''' WHEN 2 THEN ' AND B.`TECHNOLOGY`=''UMTS''' WHEN 4 THEN ' AND B.`TECHNOLOGY`=''LTE''' ELSE '' END ,' 
					AND A.`RNC`',CASE WHEN gt_covmo_csv_count(PU,',')>1 THEN CONCAT(' IN (',PU,')') ELSE CONCAT('=',PU) END,';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET PU_ALL=IFNULL(@PU_GC,'');
	
	IF PU_ALL='' THEN 
		SELECT 'No Data available!' AS NoSessionAvailable;
	LEAVE a_label;
	
	END IF;
	
	IF IMSI_GID>0 THEN
		SET @SqlCmd=CONCAT('SELECT GROUP_CONCAT(`IMSI`) into @IMSI_STR FROM `gt_covmo`.`dim_imsi` WHERE `GROUP_ID`=',IMSI_GID,';');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		SET IMSI_STR=@IMSI_STR;
	ELSE 
		SET IMSI_STR='';
	END IF;
	IF START_HOUR=0 AND END_HOUR=23 THEN 
		SET DY_FLAG=1;
		
			SET FILTER_STR=CONCAT(' 1 ',IN_STR('DATA_QUARTER',DATA_QUARTER)
					,CASE WHEN POS_KIND='' THEN IN_STR('A.CELL_ID',CELL_ID) ELSE IN_STR(CONCAT('POS_',POS_KIND,'_CELL'),CELL_ID) END
					,CASE WHEN POS_KIND='' THEN IN_STR('TILE_ID',TILE_ID) ELSE IN_STR(CONCAT('gt_covmo_proj_geohash_to_hex_geohash(POS_',POS_KIND_LOC,'_LOC)'),TILE_ID) END
					,CASE WHEN (IMSI<>'' AND IMSI_STR<> '') THEN IN_STR('IMSI',IN_QUOTE(CONCAT(IMSI,',',IMSI_STR))) 
					      WHEN (IMSI<>'') THEN IN_STR('IMSI',IN_QUOTE(IMSI))  
					      WHEN (IMSI_STR<>'') THEN IN_STR('IMSI',IN_QUOTE(IMSI_STR)) 
					      ELSE ''
					 END
					,IN_STR('CLUSTER_ID',CLUSTER_ID),IN_STR('CALL_TYPE',IN_QUOTE(CALL_TYPE)),IN_STR('CALL_STATUS',IN_QUOTE(CALL_STATUS))
					,IN_STR('A.INDOOR',@TMP_INDOOR),IN_STR('A.MOVING',@TMP_MOVING),IN_STR('CELL_INDOOR',CELL_INDOOR)
					,IN_STR('A.FREQUENCY',FREQUENCY),IN_STR('A.UARFCN',UARFCN),IN_STR('CELL_LON',CELL_LON),IN_STR('CELL_LAT',CELL_LAT)
					,IN_STR('IMEI_NEW',IMEI_NEW),IN_STR('APN',APN)
					,IN_STR('SITE_ID',IN_QUOTE(SITE_ID)),IN_STR('MAKE_ID',MAKE_ID),IN_STR('MODEL_ID',MODEL_ID),IN_STR('POLYGON_STR',POLYGON_STR)
					,CASE WHEN FILTER='' THEN '' ELSE CONCAT(' AND ',FILTER) END);			
	ELSE 	
	 
			SET FILTER_STR=CONCAT(CONCAT('A.DATA_HOUR >=',START_HOUR,' AND A.DATA_HOUR<=',END_HOUR),IN_STR('DATA_QUARTER',DATA_QUARTER)
					,CASE WHEN POS_KIND='' THEN IN_STR('CELL_ID',CELL_ID) ELSE IN_STR(CONCAT('POS_',POS_KIND,'_CELL'),CELL_ID) END
					,CASE WHEN POS_KIND='' THEN IN_STR('TILE_ID',TILE_ID) ELSE IN_STR(CONCAT('gt_covmo_proj_geohash_to_hex_geohash(POS_',POS_KIND_LOC,'_LOC)'),TILE_ID) END
					,CASE WHEN (IMSI<>'' AND IMSI_STR<> '') THEN IN_STR('IMSI',IN_QUOTE(CONCAT(IMSI,',',IMSI_STR))) 
					      WHEN (IMSI<>'') THEN IN_STR('IMSI',IN_QUOTE(IMSI))  
					      WHEN (IMSI_STR<>'') THEN IN_STR('IMSI',IN_QUOTE(IMSI_STR)) 
					      ELSE ''
					 END
					,IN_STR('CLUSTER_ID',CLUSTER_ID),IN_STR('CALL_TYPE',IN_QUOTE(CALL_TYPE)),IN_STR('CALL_STATUS',IN_QUOTE(CALL_STATUS))
					,IN_STR('INDOOR',@TMP_INDOOR),IN_STR('MOVING',@TMP_MOVING),IN_STR('CELL_INDOOR',CELL_INDOOR)
					,IN_STR('FREQUENCY',FREQUENCY),IN_STR('UARFCN',UARFCN),IN_STR('CELL_LON',CELL_LON),IN_STR('CELL_LAT',CELL_LAT)
					,IN_STR('IMEI_NEW',IMEI_NEW),IN_STR('APN',APN)
					,IN_STR('SITE_ID',IN_QUOTE(SITE_ID)),IN_STR('MAKE_ID',MAKE_ID),IN_STR('MODEL_ID',MODEL_ID),IN_STR('POLYGON_STR',POLYGON_STR)
					,CASE WHEN FILTER='' THEN '' ELSE CONCAT(' AND ',FILTER) END);
		
	END IF;
	
	SET @v_i=1;
	SET @Quotient_v=1;
	SET @v_R_Max=(CHAR_LENGTH(@PU_GC) - CHAR_LENGTH(REPLACE(@PU_GC,',','')))/(CHAR_LENGTH(','))+1;	
	WHILE @v_i <= @v_R_Max DO
	BEGIN
		SET GT_DB = gt_strtok(SPLIT_STR(PU_ALL,',',@v_i),1,'|');
		SET @TECHNOLOGY = gt_strtok(SPLIT_STR(PU_ALL,',',@v_i),2,'|');
		SELECT CONCAT('gt_nt_',gt_strtok(GT_DB,3,'_')) INTO NT_DB;
		SELECT gt_strtok(GT_DB,2,'_') INTO PU_ID;
	
			IF @TECHNOLOGY='UMTS' AND KPI_ID='110002' THEN 
			SET @SqlCmd=CONCAT(' Drop TEMPORARY table if exists ',GT_DB,'.tmp_table_zerotraffic_umts_',WORKER_ID,';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
				
			SET @SqlCmd=CONCAT(' CREATE TEMPORARY TABLE  ',GT_DB,'.tmp_table_zerotraffic_umts_',WORKER_ID,' ENGINE=MYISAM AS 
						SELECT 
						 nt_tile.cell_id AS col_1,
						 nt_tile.CELL_NAME AS col_2,
						 nt_tile.rnc_id AS col_3,
						 nt_tile.ds_date AS col_4,
						 IFNULL(nt_tile.CS_CALL_DURA,0) AS col_5,
						 IFNULL(nt_tile.DL_DATA_THRU,0) AS col_6,
						 IFNULL(nt_tile.UL_DATA_THRU,0) AS col_7,
						 1 AS col_8,
						 nt_tile.ACTIVE_STATUS AS col_9,
						 nt_tile.Administrative_state AS col_10,
						 nt_tile.OPERSTATE_ENABLE AS col_11,	
						 pm.Downtimeman AS col_12,
						 pm.Downtimeauto AS col_13,
						 
						nt_tile.cell_id AS id,
						NULL AS longitude,
						NULL AS latitude,
						1000 AS height,
						 nt_tile.ds_date AS DS_DATE,
						 ',PU_ID,' AS pu,
						 2 AS TECH_MASK 
						FROM 
					(	SELECT 
						  nt.rnc_id AS RNC_ID,
						  nt.cell_id AS CELL_ID,
						  nt.CELL_NAME AS CELL_NAME,
						  nt.ACTIVE_STATUS AS ACTIVE_STATUS,
						  IFNULL(dat.cs,0) AS CS_CALL_DURA,
						  IFNULL(dat.dl,0) AS DL_DATA_THRU,
						  IFNULL(dat.ul,0) AS UL_DATA_THRU,
						(CASE WHEN nt.ADMINSTATE_LOCKED=1 THEN ''Unlocked''
						WHEN nt.ADMINSTATE_LOCKED=0 THEN ''Locked''
						ELSE '''' END) AS Administrative_state,
						(CASE WHEN nt.OPERSTATE_ENABLE=1 THEN ''Unlocked''
						WHEN nt.OPERSTATE_ENABLE=0 THEN ''Locked''
						ELSE '''' END) AS OPERSTATE_ENABLE,	
												
						ds_date,
						 ',PU_ID,' AS pu,
						2 AS TECH_MASK 
						FROM ',NT_DB,'.nt_current nt
						
						LEFT JOIN
						(
							SELECT cell_id,rnc_id,
							DATA_DATE AS ds_date,
								IFNULL(SUM(IF(CALL_TYPE IN (10,11),erlang,0)),0) AS cs,
								IFNULL(SUM(IF(CALL_TYPE IN (12,13,14,18),DL_TRAFFIC,0)),0) AS dl,
								IFNULL(SUM(IF(CALL_TYPE IN (12,13,14,18),UL_TRAFFIC,0)),0) AS ul
							FROM ',GT_DB,'.',CASE WHEN DY_FLAG=1 THEN 'table_tile_start_dy_c A' ELSE 'table_tile_start_c A' END,' 
							WHERE ',CASE WHEN FILTER_STR='' OR FILTER_STR IS NULL THEN '' ELSE CONCAT(FILTER_STR,' AND ') END,' A.rnc_id=',PU_ID,' 
							GROUP BY a.cell_id,a.rnc_id
							HAVING cs=0 AND (dl=0 OR ul=0)
						) dat 
						ON dat.cell_id = nt.cell_id AND dat.rnc_id = nt.rnc_id 
						WHERE nt.rnc_id=',PU_ID,' 
						AND (cs=0)
		--  						AND (cs=0 or cs IS NULL) AND (dl=0 OR dl IS NULL OR ul=0 or ul IS NULL) 
						) nt_tile
						LEFT JOIN 
						(
							SELECT cell_id,rnc_id,Downtimeman,Downtimeauto
							FROM ',GT_DB,'.table_pm_counter_umts A 
							WHERE A.rnc_id=',PU_ID,' 
							GROUP BY a.cell_id,a.rnc_id
						
						) pm
						ON pm.cell_id = nt_tile.cell_id AND pm.rnc_id = nt_tile.rnc_id 
						WHERE nt_tile.rnc_id=',PU_ID,' ;');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt; 
			
			SET @SqlCmd=CONCAT(' SELECT * FROM  ',GT_DB,'.tmp_table_zerotraffic_umts_',WORKER_ID,';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT(' Drop TEMPORARY table if exists ',GT_DB,'.tmp_table_zerotraffic_umts_',WORKER_ID,';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
			
			INSERT INTO gt_gw_main.SP_LOG VALUES(GT_DB,'SP_KPI_Zerotraffic_Single','SELECT zero_traffic_single_umts', NOW());
	
			END IF;
			IF @TECHNOLOGY='LTE' AND KPI_ID='110015' THEN 
			
			SET @SqlCmd=CONCAT(' Drop TEMPORARY table if exists ',GT_DB,'.tmp_table_zerotraffic_lte_',WORKER_ID,';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
			
			SET @SqlCmd=CONCAT(' CREATE TEMPORARY TABLE  ',GT_DB,'.tmp_table_zerotraffic_lte_',WORKER_ID,' ENGINE=MYISAM AS 
						SELECT 	 
						  nt.cell_id AS col_1,
						  nt.cell_name AS col_2,
						  nt.ENODEB_ID AS col_3,
						  IFNULL(dat.dl,0) AS col_4,
						  IFNULL(dat.ul,0) AS col_5,
						  nt.ACT_STATE AS col_6,
						  nt.cell_id AS id,
						  NULL AS longitude,
						  NULL AS latitude,
						  ds_date AS DS_DATE,
						  ',PU_ID,' AS PU,
						  4 AS TECH_MASK 
						FROM ',NT_DB,'.nt_cell_current_lte nt
						LEFT JOIN
						(
							SELECT cell_id,ENODEB_ID,
							DATA_DATE AS ds_date,
								IFNULL(SUM(DL_VOLUME_SUM)/1024,0) AS dl,
								IFNULL(SUM(UL_VOLUME_SUM)/1024,0) AS ul
							FROM ',GT_DB,'.',CASE WHEN DY_FLAG=1 THEN ' rpt_cell_position_dy_def A' ELSE 'rpt_cell_position_def A' END,' 
							WHERE ',CASE WHEN FILTER_STR='' OR FILTER_STR IS NULL THEN '' ELSE CONCAT(FILTER_STR,'') END,' 
							GROUP BY a.cell_id,a.ENODEB_id
							HAVING dl=0 OR ul=0
						) dat 
						ON dat.cell_id = nt.cell_id AND dat.ENODEB_id = nt.ENODEB_id 
						WHERE nt.PU_ID=',PU_ID,' 
						AND dl=0 or ul=0
					;');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt; 
			
			SET @SqlCmd=CONCAT(' SELECT * FROM  ',GT_DB,'.tmp_table_zerotraffic_lte_',WORKER_ID,';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT(' Drop TEMPORARY table if exists ',GT_DB,'.tmp_table_zerotraffic_lte_',WORKER_ID,';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
			
			INSERT INTO gt_gw_main.SP_LOG VALUES(GT_DB,'SP_KPI_Zerotraffic_Single','SELECT zero_traffic_single_lte', NOW());
	
			END IF;
			
			IF @TECHNOLOGY='GSM' AND KPI_ID='110017' THEN 
			
			SET @SqlCmd=CONCAT(' Drop TEMPORARY table if exists ',GT_DB,'.tmp_table_zerotraffic_gsm_',WORKER_ID,';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
					
			SET @SqlCmd=CONCAT(' CREATE TEMPORARY TABLE  ',GT_DB,'.tmp_table_zerotraffic_gsm_',WORKER_ID,' ENGINE=MYISAM AS 
						SELECT 
						 
						  nt.cell_id AS col_1,
						  nt.cell_name AS col_2,
						  nt.BSC_ID AS col_3,
						  IFNULL(dat.cs,0) AS col_4,
						  NULL AS col_5,
						  NULL AS col_6,
						  NULL AS col_7,
						  nt.cell_id AS id,  
						  NULL AS longitude,
						  NULL AS latitude,
						  ds_date AS DS_DATE,
						  ',PU_ID,' AS PU,
						  1 AS TECH_MASK 
						FROM ',NT_DB,'.nt_cell_current_gsm nt
						LEFT JOIN
						(
							SELECT cell_id,BSC_ID,
							DATA_DATE AS ds_date,
								IFNULL(SUM(IF(CALL_TYPE IN (10),DURATION/3600,0)),0) AS cs
								
							FROM ',GT_DB,'.',CASE WHEN DY_FLAG=1 THEN ' table_tile_fp_gsm_dy_c A' ELSE 'table_tile_fp_gsm_c A' END,' 
							WHERE ',CASE WHEN FILTER_STR='' OR FILTER_STR IS NULL THEN '' ELSE CONCAT(FILTER_STR,' AND ') END,' A.BSC_ID=',PU_ID,' 
							GROUP BY a.cell_id,a.BSC_ID
							HAVING (SUM(IF(A.CALL_TYPE=10,A.DURATION/3600,0)))=0
						) dat 
						ON dat.cell_id = nt.cell_id AND dat.BSC_id = nt.BSC_id 
						WHERE nt.BSC_id=',PU_ID,' 
						AND cs=0
						;');	
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
			
			SET @SqlCmd=CONCAT(' SELECT * FROM  ',GT_DB,'.tmp_table_zerotraffic_gsm_',WORKER_ID,';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT(' Drop TEMPORARY table if exists ',GT_DB,'.tmp_table_zerotraffic_gsm_',WORKER_ID,';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			INSERT INTO gt_gw_main.SP_LOG VALUES(GT_DB,'SP_KPI_Zerotraffic_Single','SELECT zero_traffic_single_gsm', NOW());
			END IF;	
	
		SET @v_i=@v_i+@Quotient_v;
	END;
	END WHILE;	
				
	INSERT INTO `gt_gw_main`.`tbl_rpt_qrystr`
		(`KPI_ID`,`RNC`,`START_DATE`,`END_DATE`,`START_HOUR`,`END_HOUR`,`SOURCE_TYPE`,`SERVICE`,`PID`,`QryStr`,`ID`,`SP_NAME`,`CreateTime`)
	VALUES (KPI_ID,
			PU_ID,
			gt_strtok(GT_DB,3,'_'),
			gt_strtok(GT_DB,3,'_'),
			START_HOUR,
			END_HOUR,
			SOURCE_TYPE,
			SERVICE,
			WORKER_ID,
			@SqlCmd,
			0,
			'SP_KPI_Zerotraffic',
			NOW());
	
	INSERT INTO gt_gw_main.SP_LOG VALUES(GT_DB,'SP_KPI_Zerotraffic_Single','End', NOW());
	
END$$
DELIMITER ;
