DELIMITER $$
USE `gt_gw_main`$$
DROP PROCEDURE IF EXISTS `SP_CreateDB_LTE`$$
CREATE DEFINER=`covmo`@`%` PROCEDURE `SP_Sub_Generate_UE_GSM`(IN GT_DB VARCHAR(100), IN KIND VARCHAR(20), IN VENDOR_SOURCE VARCHAR(20),IN GT_COVMO VARCHAR(100))
BEGIN
	DECLARE BSC_ID INT;
	DECLARE O_GT_DB VARCHAR(100) DEFAULT GT_DB;
	DECLARE START_TIME DATETIME DEFAULT SYSDATE();
	DECLARE PARTITION_ID INT DEFAULT SUBSTRING(RIGHT(GT_DB,18),10,2) ;
	DECLARE SH_EH VARCHAR(9) DEFAULT RIGHT(GT_DB,9);
	DECLARE GT_DATE VARCHAR(18) DEFAULT RIGHT(GT_DB,18);
	DECLARE STARTHOUR VARCHAR(2) DEFAULT SUBSTRING(RIGHT(GT_DB,18),10,2);
	DECLARE ENDHOUR VARCHAR(2) DEFAULT IF(SUBSTRING(RIGHT(GT_DB,18),15,2)='00','24',SUBSTRING(RIGHT(GT_DB,18),15,2));
	DECLARE RUN VARCHAR(20);
	DECLARE WORKER_ID VARCHAR(10) DEFAULT CONNECTION_ID();
	DECLARE CURRENT_NT_DB VARCHAR(50) DEFAULT CONCAT('gt_nt_',gt_strtok(GT_DB,3,'_'));
	SET SESSION group_concat_max_len = 100000;
	
	SELECT gt_strtok(GT_DB,2,'_') INTO BSC_ID;
	
	SELECT REPLACE(GT_DB,SH_EH,'0000_0000') INTO GT_DB;
	
	IF VENDOR_SOURCE = 'GW' THEN
		IF KIND = 'DAILY' THEN
			SET RUN = '_tmp';
		ELSEIF KIND = 'RERUN' THEN
			SET RUN = '_rerun';
		END IF;
	ELSEIF VENDOR_SOURCE = 'AP' THEN
		SET RUN = '';
	END IF;
	
	SET @SqlCmd=CONCAT('SELECT att_value INTO @SYS_CONFIG_TILE FROM ',CURRENT_NT_DB,'.`sys_config` WHERE `group_name`=''system'' AND att_name = ''MapResolution'';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
		
	IF gt_covmo_csv_count(@SYS_CONFIG_TILE,',') =3 THEN
		
		SET @SqlCmd=CONCAT('SELECT gt_covmo_csv_get(att_value,3) INTO @ZOOM_LEVEL FROM ',CURRENT_NT_DB,'.`sys_config` WHERE `group_name`=''system'' AND att_name = ''MapResolution'';');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
	ELSE 
		SET @SqlCmd=CONCAT('SELECT att_value INTO @ZOOM_LEVEL FROM ',CURRENT_NT_DB,'.`sys_config` WHERE `group_name`=''system'' AND att_name = ''MapResolution'';');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
	END IF;
	
	SET @SqlCmd=CONCAT('ALTER TABLE ',GT_DB,RUN,'.table_tile_ue_gsm TRUNCATE PARTITION h',PARTITION_ID,';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('ALTER TABLE ',GT_DB,RUN,'.table_tile_ue_gsm_t TRUNCATE PARTITION h',PARTITION_ID,';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('ALTER TABLE ',GT_DB,RUN,'.table_tile_ue_gsm_c TRUNCATE PARTITION h',PARTITION_ID,';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('ALTER TABLE ',GT_DB,RUN,'.table_tile_ue_gsm_t_def TRUNCATE PARTITION h',PARTITION_ID,';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('ALTER TABLE ',GT_DB,RUN,'.table_tile_ue_gsm_c_def TRUNCATE PARTITION h',PARTITION_ID,';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	INSERT INTO gt_gw_main.sp_log VALUES(O_GT_DB,'SP_Sub_Generate_UE_GSM','Create temp table tmp_table_tile_ue_gsm ', NOW());
	
	SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE  IF EXISTS ',GT_DB,RUN,'.tmp_table_tile_ue_gsm_',WORKER_ID,';');
	PREPARE stmt FROM @sqlcmd;
	EXECUTE stmt;
	DEALLOCATE PREPARE stmt;
	
	SET @SqlCmd=CONCAT('CREATE TEMPORARY TABLE ',GT_DB,RUN,'.tmp_table_tile_ue_gsm_',WORKER_ID,' 
			SELECT * FROM ',GT_DB,RUN,'.table_tile_ue_gsm WHERE 1<>1;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	INSERT INTO gt_gw_main.sp_log VALUES(O_GT_DB,'SP_Sub_Generate_UE_GSM','Insert into tmp_table_tile_ue_gsm ', NOW());
	
	SET @SqlCmd=CONCAT('INSERT INTO  ',GT_DB,RUN,'.tmp_table_tile_ue_gsm_',WORKER_ID,'
				(
				DATA_DATE,
				DATA_HOUR,
				INDOOR,
				MOVING,
				TILE_ID,
				BSC_ID,
				CELL_ID,
				CALL_TYPE,
				CALL_STATUS,
				BANDINDEX,
				CELL_INDOOR,
				CLUSTER_ID,
				LAC,
				SITE_ID,
				CELL_LON,
				CELL_LAT,
				BCCH_ARFCN,
				CELL_NAME,
				UE_CNT
				)
			SELECT
				DATA_DATE
				,DATA_HOUR
				,INDOOR
				,MOVING
				,gt_covmo_proj_geohash_to_hex_geohash(POS_FIRST_LOC, ',@ZOOM_LEVEL,') AS TILE_ID
				,POS_FIRST_BSC
				,POS_FIRST_CELL
				,CALL_TYPE
				,CALL_STATUS
				,POS_FIRST_BANDINDEX
				,POS_FIRST_CELL_INDOOR
				,NULL AS CLUSTER_ID
				,POS_FIRST_LAC
				,POS_FIRST_SITE
				,NULL AS CELL_LON
				,NULL AS CELL_LAT
				,POS_FIRST_BCCH_ARFCN
				,NULL AS CELL_NAME
				,COUNT(*) AS UE_CNT
			FROM ',GT_DB,RUN,'.table_call_gsm
			WHERE POS_FIRST_BSC =',BSC_ID,'
			AND POS_FIRST_RXLEV_FULL_DOWNLINK IS NOT NULL
			AND DATA_HOUR >= ',STARTHOUR,' AND DATA_HOUR < ',ENDHOUR,'
			GROUP BY  
				DATA_DATE
				,DATA_HOUR
				,CALL_TYPE
				,CALL_STATUS
				,MOVING
				,INDOOR
				,POS_FIRST_BSC
				,POS_FIRST_CELL
				,gt_covmo_proj_geohash_to_hex_geohash(POS_FIRST_LOC, ',@ZOOM_LEVEL,')
			ORDER BY NULL');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	INSERT INTO gt_gw_main.sp_log VALUES(O_GT_DB,'SP_Sub_Generate_UE_GSM','Update CELL_LON, CELL_LAT, CELL_NAME in table_tile_ue_gsm ', NOW());
	SET @SqlCmd=CONCAT(' UPDATE ',GT_DB,RUN,'.tmp_table_tile_ue_gsm_',WORKER_ID,' A, 
				    ',CURRENT_NT_DB,'.nt_cell_current_gsm B
			     SET A.CELL_LON=B.LONGITUDE
				,A.CELL_LAT=B.LATITUDE
				,A.CELL_NAME=B.CELL_NAME
				,A.CLUSTER_ID=B.CLUSTER_ID
				WHERE A.BSC_ID=B.BSC_ID
				AND A.CELL_ID=B.CELL_ID
				AND A.DATA_HOUR >= ',STARTHOUR,' AND A.DATA_HOUR < ',ENDHOUR,';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
		
	INSERT INTO gt_gw_main.sp_log VALUES(O_GT_DB,'SP_Sub_Generate_UE_GSM','Insert into table_tile_ue_gsm ', NOW());
	
	SET @SqlCmd=CONCAT('INSERT INTO  ',GT_DB,RUN,'.table_tile_ue_gsm
				(
				DATA_DATE,
				DATA_HOUR,
				INDOOR,
				MOVING,
				TILE_ID,
				BSC_ID,
				CELL_ID,
				CALL_TYPE,
				CALL_STATUS,
				BANDINDEX,
				CELL_INDOOR,
				CLUSTER_ID,
				LAC,
				SITE_ID,
				CELL_LON,
				CELL_LAT,
				BCCH_ARFCN,
				CELL_NAME,
				UE_CNT
				)
			SELECT 
				DATA_DATE,
				DATA_HOUR,
				INDOOR,
				MOVING,
				TILE_ID,
				BSC_ID,
				CELL_ID,
				CALL_TYPE,
				CALL_STATUS,
				BANDINDEX,
				CELL_INDOOR,
				CLUSTER_ID,
				LAC,
				SITE_ID,
				CELL_LON,
				CELL_LAT,
				BCCH_ARFCN,
				CELL_NAME,
				UE_CNT
			FROM ',GT_DB,RUN,'.tmp_table_tile_ue_gsm_',WORKER_ID,';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	INSERT INTO gt_gw_main.sp_log VALUES(O_GT_DB,'SP_Sub_Generate_UE_GSM','Insert into table_tile_ue_gsm_t ', NOW());
	SET @SqlCmd=CONCAT('INSERT INTO  ',GT_DB,RUN,'.table_tile_ue_gsm_t
				(
				DATA_DATE,
				DATA_HOUR,
				INDOOR,
				MOVING,
				TILE_ID,
				CALL_TYPE,
				CALL_STATUS,
				UE_CNT
				)
			    SELECT
				DATA_DATE
				,DATA_HOUR
				,INDOOR
				,MOVING
				,TILE_ID
				,CALL_TYPE
				,CALL_STATUS
				,SUM(UE_CNT) AS UE_CNT
			FROM ',GT_DB,RUN,'.tmp_table_tile_ue_gsm_',WORKER_ID,'
			GROUP BY   
				DATA_DATE
				,DATA_HOUR
				,INDOOR
				,MOVING
				,TILE_ID
				,CALL_TYPE
				,CALL_STATUS
			ORDER BY NULL;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	INSERT INTO gt_gw_main.sp_log VALUES(O_GT_DB,'SP_Sub_Generate_UE_GSM','Insert into table_tile_ue_gsm_c ', NOW());
	SET @SqlCmd=CONCAT('INSERT INTO  ',GT_DB,RUN,'.table_tile_ue_gsm_c
				(
				DATA_DATE,
				DATA_HOUR,
				INDOOR,
				MOVING,
				BSC_ID,
				CELL_ID,
				CALL_TYPE,
				CALL_STATUS,
				BANDINDEX,
				CELL_INDOOR,
				CLUSTER_ID,
				LAC,
				SITE_ID,
				CELL_LON,
				CELL_LAT,
				BCCH_ARFCN,
				CELL_NAME,
				UE_CNT
				)
			    SELECT
				DATA_DATE
				,DATA_HOUR
				,INDOOR
				,MOVING
				,BSC_ID
				,CELL_ID
				,CALL_TYPE
				,CALL_STATUS
				,BANDINDEX
				,CELL_INDOOR
				,CLUSTER_ID
				,LAC
				,SITE_ID
				,CELL_LON
				,CELL_LAT
				,BCCH_ARFCN
				,CELL_NAME
				,SUM(UE_CNT) AS UE_CNT
			FROM ',GT_DB,RUN,'.tmp_table_tile_ue_gsm_',WORKER_ID,'
			GROUP BY   
				DATA_DATE
				,DATA_HOUR
				,INDOOR
				,MOVING
				,BSC_ID
				,CELL_ID
				,CALL_TYPE
				,CALL_STATUS
			ORDER BY NULL;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	INSERT INTO gt_gw_main.sp_log VALUES(O_GT_DB,'SP_Sub_Generate_UE_GSM','Insert into table_tile_ue_gsm_t_def ', NOW());
	SET @SqlCmd=CONCAT('INSERT INTO  ',GT_DB,RUN,'.table_tile_ue_gsm_t_def
				(
				DATA_DATE,
				DATA_HOUR,
				TILE_ID,
				UE_CNT
				)
			    SELECT
				DATA_DATE
				,DATA_HOUR
				,TILE_ID
				,SUM(UE_CNT) AS UE_CNT
			FROM ',GT_DB,RUN,'.tmp_table_tile_ue_gsm_',WORKER_ID,'
			GROUP BY   
				DATA_DATE
				,DATA_HOUR
				,TILE_ID
			ORDER BY NULL;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	INSERT INTO gt_gw_main.sp_log VALUES(O_GT_DB,'SP_Sub_Generate_UE_GSM','Insert into table_tile_ue_gsm_c_def ', NOW());
	SET @SqlCmd=CONCAT('INSERT INTO  ',GT_DB,RUN,'.table_tile_ue_gsm_c_def
				(
				DATA_DATE,
				DATA_HOUR,
				BSC_ID,
				CELL_ID,
				CELL_LON,
				CELL_LAT,
				CELL_NAME,
				UE_CNT
				)
			    SELECT
				DATA_DATE
				,DATA_HOUR
				,BSC_ID
				,CELL_ID
				,CELL_LON
				,CELL_LAT
				,CELL_NAME
				,SUM(UE_CNT) AS UE_CNT
			FROM ',GT_DB,RUN,'.tmp_table_tile_ue_gsm_',WORKER_ID,'
			GROUP BY   
				DATA_DATE
				,DATA_HOUR
				,BSC_ID
				,CELL_ID
			ORDER BY NULL;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	INSERT INTO gt_gw_main.sp_log VALUES(O_GT_DB,'SP_Sub_Generate_UE_GSM','Insert into table_tile_ue_gsm_dy ', NOW());
	SET @SqlCmd=CONCAT('REPLACE INTO ',GT_DB,'.table_tile_ue_gsm_dy
				(
			DATA_DATE,
			INDOOR,
			MOVING,
			TILE_ID,
			BSC_ID,
			CELL_ID,
			CALL_TYPE,
			CALL_STATUS,
			BANDINDEX,
			CELL_INDOOR,
			CLUSTER_ID,
			LAC,
			SITE_ID,
			CELL_LON,
			CELL_LAT,
			BCCH_ARFCN,
			CELL_NAME,
			UE_CNT
				)
		SELECT
			B.DATA_DATE
			,B.INDOOR
			,B.MOVING
			,B.TILE_ID
			,B.BSC_ID
			,B.CELL_ID
			,B.CALL_TYPE
			,B.CALL_STATUS
			,B.BANDINDEX
			,B.CELL_INDOOR
			,B.CLUSTER_ID
			,B.LAC
			,B.SITE_ID
			,B.CELL_LON
			,B.CELL_LAT
			,B.BCCH_ARFCN
			,B.CELL_NAME
			,CASE WHEN A.UE_CNT IS NULL AND B.UE_CNT IS NULL THEN NULL ELSE IFNULL(A.UE_CNT,0) + IFNULL(B.UE_CNT,0) END AS UE_CNT
		FROM ',GT_DB,'.table_tile_ue_gsm_dy a RIGHT JOIN 
		(
		 	SELECT
				DATA_DATE
				,INDOOR
				,MOVING
				,TILE_ID
				,BSC_ID
				,CELL_ID
				,CALL_TYPE
				,CALL_STATUS
				,BANDINDEX
				,CELL_INDOOR
				,CLUSTER_ID
				,LAC
				,SITE_ID
				,CELL_LON
				,CELL_LAT
				,BCCH_ARFCN
				,CELL_NAME
				,SUM(UE_CNT) AS UE_CNT
			FROM ',GT_DB,RUN,'.tmp_table_tile_ue_gsm_',WORKER_ID,'
			GROUP BY 
				DATA_DATE
				,INDOOR
				,MOVING
				,TILE_ID
				,BSC_ID
				,CELL_ID
				,CALL_TYPE
				,CALL_STATUS
			ORDER BY NULL
		) B
		ON  A.DATA_DATE=B.DATA_DATE
		AND A.CALL_TYPE=B.CALL_TYPE
		AND A.CALL_STATUS=B.CALL_STATUS
		AND A.MOVING=B.MOVING
		AND A.INDOOR=B.INDOOR
		AND A.BSC_ID=B.BSC_ID
		AND A.CELL_ID=B.CELL_ID
		AND A.TILE_ID=B.TILE_ID
		;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	INSERT INTO gt_gw_main.sp_log VALUES(O_GT_DB,'SP_Sub_Generate_UE_GSM','Insert into table_tile_ue_gsm_dy_t ', NOW());
	SET @SqlCmd=CONCAT('REPLACE INTO ',GT_DB,'.table_tile_ue_gsm_dy_t
				(
			DATA_DATE,
			INDOOR,
			MOVING,
			TILE_ID,
			CALL_TYPE,
			CALL_STATUS,
			UE_CNT
				)
		SELECT
			B.DATA_DATE
			,B.INDOOR
			,B.MOVING
			,B.TILE_ID
			,B.CALL_TYPE
			,B.CALL_STATUS
			,CASE WHEN A.UE_CNT IS NULL AND B.UE_CNT IS NULL THEN NULL ELSE IFNULL(A.UE_CNT,0) + IFNULL(B.UE_CNT,0) END AS UE_CNT
		FROM ',GT_DB,'.table_tile_ue_gsm_dy a RIGHT JOIN 
		(
		 	SELECT
				DATA_DATE
				,INDOOR
				,MOVING
				,TILE_ID
				,CALL_TYPE
				,CALL_STATUS
				,SUM(UE_CNT) AS UE_CNT
			FROM ',GT_DB,RUN,'.tmp_table_tile_ue_gsm_',WORKER_ID,'
			GROUP BY 
				DATA_DATE
				,INDOOR
				,MOVING
				,TILE_ID
				,CALL_TYPE
				,CALL_STATUS
			ORDER BY NULL
		) B
		ON  A.DATA_DATE=B.DATA_DATE
		AND A.INDOOR=B.INDOOR
		AND A.MOVING=B.MOVING
		AND A.TILE_ID=B.TILE_ID
		AND A.CALL_TYPE=B.CALL_TYPE
		AND A.CALL_STATUS=B.CALL_STATUS
		;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	INSERT INTO gt_gw_main.sp_log VALUES(O_GT_DB,'SP_Sub_Generate_UE_GSM','Insert into table_tile_ue_gsm_dy_c ', NOW());
	SET @SqlCmd=CONCAT('REPLACE INTO ',GT_DB,'.table_tile_ue_gsm_dy_c
		(
			DATA_DATE,
			INDOOR,
			MOVING,
			BSC_ID,
			CELL_ID,
			CALL_TYPE,
			CALL_STATUS,
			BANDINDEX,
			CELL_INDOOR,
			CLUSTER_ID,
			LAC,
			SITE_ID,
			CELL_LON,
			CELL_LAT,
			BCCH_ARFCN,
			CELL_NAME,
			UE_CNT
		)
		SELECT
			B.DATA_DATE
			,B.INDOOR
			,B.MOVING
			,B.BSC_ID
			,B.CELL_ID
			,B.CALL_TYPE
			,B.CALL_STATUS
			,B.BANDINDEX
			,B.CELL_INDOOR
			,B.CLUSTER_ID
			,B.LAC
			,B.SITE_ID
			,B.CELL_LON
			,B.CELL_LAT
			,B.BCCH_ARFCN
			,B.CELL_NAME
			,CASE WHEN A.UE_CNT IS NULL AND B.UE_CNT IS NULL THEN NULL ELSE IFNULL(A.UE_CNT,0) + IFNULL(B.UE_CNT,0) END AS UE_CNT
		FROM ',GT_DB,'.table_tile_ue_gsm_dy_c a RIGHT JOIN 
		(
		 	SELECT
				DATA_DATE
				,INDOOR
				,MOVING
				,BSC_ID
				,CELL_ID
				,CALL_TYPE
				,CALL_STATUS
				,BANDINDEX
				,CELL_INDOOR
				,CLUSTER_ID
				,LAC
				,SITE_ID
				,CELL_LON
				,CELL_LAT
				,BCCH_ARFCN
				,CELL_NAME
				,SUM(UE_CNT) AS UE_CNT
			FROM ',GT_DB,RUN,'.tmp_table_tile_ue_gsm_',WORKER_ID,'
			GROUP BY
				DATA_DATE
				,INDOOR
				,MOVING
				,BSC_ID
				,CELL_ID
				,CALL_TYPE
				,CALL_STATUS
			ORDER BY NULL
		) B
		ON  A.DATA_DATE=B.DATA_DATE
		AND A.INDOOR=B.INDOOR
		AND A.MOVING=B.MOVING
		AND A.BSC_ID=B.BSC_ID
		AND A.CELL_ID=B.CELL_ID
		AND A.CALL_TYPE=B.CALL_TYPE
		AND A.CALL_STATUS=B.CALL_STATUS
		;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	INSERT INTO gt_gw_main.sp_log VALUES(O_GT_DB,'SP_Sub_Generate_UE_GSM','Insert into table_tile_ue_gsm_dy_t_def ', NOW());
	SET @SqlCmd=CONCAT('REPLACE INTO ',GT_DB,'.table_tile_ue_gsm_dy_t_def
		(
			DATA_DATE,
			TILE_ID,
			UE_CNT
		)
		SELECT
			B.DATA_DATE
			,B.TILE_ID	
			,CASE WHEN A.UE_CNT IS NULL AND B.UE_CNT IS NULL THEN NULL ELSE IFNULL(A.UE_CNT,0) + IFNULL(B.UE_CNT,0) END AS UE_CNT
		FROM ',GT_DB,'.table_tile_ue_gsm_dy_t_def a RIGHT JOIN 
		(
		 	SELECT
				DATA_DATE
				,TILE_ID
				,SUM(UE_CNT) AS UE_CNT
			FROM ',GT_DB,RUN,'.tmp_table_tile_ue_gsm_',WORKER_ID,'
			GROUP BY 
				DATA_DATE
				, TILE_ID
		) B
		ON  A.DATA_DATE=B.DATA_DATE
		AND A.TILE_ID=B.TILE_ID
		;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	INSERT INTO gt_gw_main.sp_log VALUES(O_GT_DB,'SP_Sub_Generate_UE_GSM','Insert into table_tile_ue_gsm_dy_c_def ', NOW());
	SET @SqlCmd=CONCAT('REPLACE INTO ',GT_DB,'.table_tile_ue_gsm_dy_c_def
		(
			DATA_DATE,
			BSC_ID,
			CELL_ID,
			CELL_LON,
			CELL_LAT,
			CELL_NAME,
			UE_CNT
		)
		SELECT
			B.DATA_DATE
			,B.BSC_ID
			,B.CELL_ID
			,B.CELL_LON
			,B.CELL_LAT
			,B.CELL_NAME
			,CASE WHEN A.UE_CNT IS NULL AND B.UE_CNT IS NULL THEN NULL ELSE IFNULL(A.UE_CNT,0) + IFNULL(B.UE_CNT,0) END AS UE_CNT
		FROM ',GT_DB,'.table_tile_ue_gsm_dy_c_def a RIGHT JOIN 
		(
		 	SELECT
				DATA_DATE
				,BSC_ID
				,CELL_ID
				,CELL_LON
				,CELL_LAT
				,SUM(UE_CNT) AS UE_CNT
			FROM ',GT_DB,RUN,'.tmp_table_tile_ue_gsm_',WORKER_ID,'
			GROUP BY 
				DATA_DATE
				,BSC_ID
				,CELL_ID
			ORDER BY NULL
		) B
		ON  A.DATA_DATE=B.DATA_DATE
		AND A.BSC_ID=B.BSC_ID
		AND A.CELL_ID=B.CELL_ID
		;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	INSERT INTO gt_gw_main.sp_log VALUES(O_GT_DB,'SP_Sub_Generate_UE_GSM',CONCAT('Done: ',TIMESTAMPDIFF(SECOND,START_TIME,SYSDATE()),' seconds.'), NOW());
	
		
END$$
DELIMITER ;
