DELIMITER $$
USE `gt_gw_main`$$
DROP PROCEDURE IF EXISTS `SP_CreateDB_LTE`$$
CREATE DEFINER=`covmo`@`%` PROCEDURE `Sp_Sub_Generate_Interface_Serverity`(IN gt_db VARCHAR(100), IN KIND VARCHAR(20), IN VENDOR_SOURCE VARCHAR(20),IN GT_COVMO VARCHAR(100))
BEGIN
	DECLARE O_GT_DB VARCHAR(100) DEFAULT GT_DB;
	DECLARE PARTITION_ID INT DEFAULT SUBSTRING(RIGHT(GT_DB,18),10,2);
	DECLARE STARTHOUR VARCHAR(2) DEFAULT SUBSTRING(RIGHT(GT_DB,18),10,2);
	DECLARE ENDHOUR VARCHAR(2) DEFAULT IF(SUBSTRING(RIGHT(GT_DB,18),15,2)='00','24',SUBSTRING(RIGHT(GT_DB,18),15,2));
	
	DECLARE SH_EH VARCHAR(9) DEFAULT RIGHT(GT_DB,9);
	DECLARE START_TIME DATETIME DEFAULT SYSDATE();	
	DECLARE RUN VARCHAR(20);
	DECLARE CURRENT_NT_DB VARCHAR(50) DEFAULT CONCAT('gt_nt_',gt_strtok(GT_DB,3,'_'));
	CALL SP_Sub_Set_Session_Param(GT_DB);	        	
	SELECT REPLACE(GT_DB,SH_EH,'0000_0000')INTO GT_DB;
	
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
	
	
	INSERT INTO gt_gw_main.SP_LOG VALUES(O_GT_DB,'Sp_Sub_Generate_Interface_Serverity','Start', START_TIME);
	
	SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS  ',GT_DB,RUN,'.tmp_table_interference_severity_detail;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS  ',GT_DB,RUN,'.tmp_detail_tile;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('ALTER TABLE ',GT_DB,RUN,'.table_interference_severity TRUNCATE PARTITION h',PARTITION_ID);	
	PREPARE stmt FROM @sqlcmd;
	EXECUTE stmt;
	DEALLOCATE PREPARE stmt;
	SET @SqlCmd=CONCAT('ALTER TABLE ',GT_DB,RUN,'.table_interference_severity_detail TRUNCATE PARTITION h',PARTITION_ID);	
	PREPARE stmt FROM @sqlcmd;
	EXECUTE stmt;
	DEALLOCATE PREPARE stmt;		
	
	INSERT INTO gt_gw_main.sp_log VALUES(O_GT_DB,'Sp_Sub_Generate_Interface_Serverity','Insert into tmp_table_interference_severity_detail ', NOW());	
	SET @SqlCmd=CONCAT('CREATE TEMPORARY TABLE ',GT_DB,RUN,'.tmp_table_interference_severity_detail AS 
		SELECT *
		FROM ',GT_DB,RUN,'.`table_interference_severity_detail` WHERE 1<>1;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
		
	SET @sqlcmd =CONCAT('INSERT INTO ',GT_DB,RUN,'.tmp_table_interference_severity_detail
				(DATA_DATE,DATA_HOUR,FREQUENCY,UARFCN,INDOOR,MOVING,TILE_ID,TILE_LON,TILE_LAT,RNC_ID,CELL_INDOOR
				,CLUSTER_ID,SITE_ID,CELL_ID,CALL_TYPE,CALL_STATUS,IMSI,CALL_CNT,RSCP,ECN0, RSCP_MED,ECN0_MED)	
			     SELECT
					 DATA_DATE
					, DATA_HOUR
					, POS_AS1_FREQUENCY AS FREQUENCY
					, POS_AS1_UARFCN AS UARFCN
					, INDOOR
					, MOVING
					, gt_covmo_proj_geohash_to_hex_geohash(POS_AS_LOC, ',@ZOOM_LEVEL,') AS TILE_ID
					, gt_covmo_proj_geohash_to_lng(POS_AS_LOC) as TILE_LON
					, gt_covmo_proj_geohash_to_lat(POS_AS_LOC) as TILE_LAT
					, POS_AS1_RNC AS RNC_ID
					, POS_AS1_CELL_INDOOR AS CELL_INDOOR
					, POS_AS1_CLUSTER AS CLUSTER_ID
					, POS_AS1_SITE AS SITE_ID
					, POS_AS1_CELL AS CELL_ID
					, CALL_TYPE 
					, CALL_STATUS
					, IMSI
					, COUNT(POS_AS1_CELL) AS IMSI_CALL_CNT
					, AVG(POS_AS1_RSCP) AS RSCP
					, AVG(POS_AS1_ECN0) AS ECN0
					, MEDIAN(POS_AS1_RSCP) RSCP_MED
					, MEDIAN(POS_AS1_ECN0) ECN0_MED
				FROM ',GT_DB ,RUN,'.table_call 
				WHERE DATA_HOUR >= ',STARTHOUR,' AND DATA_HOUR < ',ENDHOUR,' 
				AND POS_AS1_RSCP IS NOT NULL
				GROUP BY  DATA_DATE
					, DATA_HOUR
-- 					, POS_AS1_FREQUENCY
-- 					, POS_AS1_UARFCN
					, INDOOR
					, MOVING
					, gt_covmo_proj_geohash_to_hex_geohash(POS_AS_LOC, ',@ZOOM_LEVEL,')
					, POS_AS1_RNC
-- 					, POS_AS1_CELL_INDOOR
-- 					, POS_AS1_CLUSTER
-- 					, POS_AS1_SITE
					, POS_AS1_CELL
					, CALL_TYPE 
					, CALL_STATUS
					, IMSI 
				ORDER BY NULL; ');
       	
       	PREPARE stmt FROM @sqlcmd;
	EXECUTE stmt;
	DEALLOCATE PREPARE stmt;
	
	SET @SqlCmd=CONCAT('CREATE TEMPORARY TABLE ',GT_DB,RUN,'.tmp_detail_tile AS 
				SELECT DATA_DATE,DATA_HOUR,RNC_ID,CELL_ID,TILE_ID
				FROM ',GT_DB,RUN,'.`tmp_table_interference_severity_detail`;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('CREATE INDEX `ix_tmp_tile_interference_severity_detail` ON ',GT_DB,RUN,'.tmp_table_interference_severity_detail (`RNC_ID`,`CELL_ID`);');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('CREATE INDEX `ix_tmp_detail_tile` ON ',GT_DB,RUN,'.tmp_detail_tile (`RNC_ID`,`CELL_ID`);');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
        SET @sqlcmd=CONCAT(' UPDATE ',GT_DB,RUN,'.tmp_table_interference_severity_detail A, 
				    ',CURRENT_NT_DB,'.nt_current B
			     SET A.CELL_LON=B.LONGITUDE
				,A.CELL_LAT=B.LATITUDE
				WHERE A.RNC_ID=B.RNC_ID	AND A.CELL_ID=B.CELL_ID 
-- 				AND DATA_HOUR >= ',STARTHOUR,' AND DATA_HOUR < ',ENDHOUR,'
				;');	
	PREPARE stmt FROM @sqlcmd;
	EXECUTE stmt;
	DEALLOCATE PREPARE stmt;	
	
	INSERT INTO gt_gw_main.sp_log VALUES(O_GT_DB,'Sp_Sub_Generate_Interface_Serverity','insert into table_interference_severity ', NOW());
	
			
	SET @sqlcmd =CONCAT('INSERT INTO ',GT_DB,RUN,'.table_interference_severity 
				SELECT DATA_DATE,DATA_HOUR,RNC_ID,CELL_ID,FREQUENCY,UARFCN,INDOOR,MOVING,CELL_INDOOR,CLUSTER_ID,SITE_ID,CELL_LON,CELL_LAT,
					(
						SELECT COUNT(DISTINCT TILE_ID) TILE_CNT 
						FROM ',GT_DB,RUN,'.tmp_detail_tile A
						WHERE A.DATA_DATE=TMP.DATA_DATE 
							AND DATA_HOUR=TMP.DATA_HOUR 
							AND A.RNC_ID=TMP.RNC_ID
							AND A.CELL_ID=TMP.CELL_ID 
					) TILE_CNT ,IMSI_CNT,CALL_CNT, WEIGHTED_RSCP , WEIGHTED_ECN0
					,6371 * ACOS( 
							COS( RADIANS(CELL_LAT)) 
							* COS( RADIANS(WEIGHTED_TILE_LAT))
							* COS( RADIANS(CELL_LON) - RADIANS(WEIGHTED_TILE_LON))
							+ SIN( RADIANS(CELL_LAT) 
						    ) 
					      * SIN( RADIANS(WEIGHTED_TILE_LAT))) * 1000 AS WEIGHTED_DISTANCE 	
				FROM 
					(
						SELECT DATA_DATE,DATA_HOUR,RNC_ID,CELL_ID,MAX(FREQUENCY) FREQUENCY,
							MAX(UARFCN) UARFCN, INDOOR, MOVING,
							MAX(CELL_INDOOR) CELL_INDOOR,MAX(CLUSTER_ID) CLUSTER_ID,MAX(SITE_ID) SITE_ID,
							MAX(CELL_LON) CELL_LON,MAX(CELL_LAT) CELL_LAT,		
							COUNT(DISTINCT IMSI) AS IMSI_CNT,
							SUM(CALL_CNT) CALL_CNT,
							AVG(WEIGHTED_RSCP) WEIGHTED_RSCP,
							AVG(WEIGHTED_ECN0) WEIGHTED_ECN0,
							AVG(WEIGHTED_TILE_LON) WEIGHTED_TILE_LON,
							AVG(WEIGHTED_TILE_LAT) WEIGHTED_TILE_LAT
						FROM 
							(
								SELECT DATA_DATE,DATA_HOUR,RNC_ID,CELL_ID,MAX(FREQUENCY) FREQUENCY,
									MAX(UARFCN) UARFCN, INDOOR, MOVING,
									MAX(CELL_INDOOR) CELL_INDOOR,MAX(CLUSTER_ID) CLUSTER_ID,MAX(SITE_ID) SITE_ID,
									MAX(CELL_LON) CELL_LON,MAX(CELL_LAT) CELL_LAT,			
									IMSI,
									SUM(CALL_CNT) CALL_CNT,
									AVG(RSCP) WEIGHTED_RSCP,
									AVG(ECN0) WEIGHTED_ECN0,
									AVG(TILE_LON) WEIGHTED_TILE_LON,
									AVG(TILE_LAT) WEIGHTED_TILE_LAT
								FROM ',GT_DB,RUN,'.tmp_table_interference_severity_detail
-- 								WHERE DATA_HOUR >= ',STARTHOUR,' AND DATA_HOUR < ',ENDHOUR,'
								GROUP BY DATA_DATE,DATA_HOUR,RNC_ID,CELL_ID,IMSI,INDOOR,MOVING	
							) TMP 
						GROUP BY DATA_DATE,DATA_HOUR,RNC_ID,CELL_ID,INDOOR,MOVING
					) TMP	
			'); 		
	PREPARE stmt FROM @sqlcmd;
	EXECUTE stmt;
	DEALLOCATE PREPARE stmt;
	
	SET @SqlCmd=CONCAT(' INSERT INTO ',GT_DB,RUN,'.table_interference_severity_detail
				SELECT * FROM ',GT_DB,RUN,'.tmp_table_interference_severity_detail;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS  ',GT_DB,RUN,'.tmp_table_interference_severity_detail;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS  ',GT_DB,RUN,'.tmp_detail_tile;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	INSERT INTO gt_gw_main.SP_LOG VALUES(O_GT_DB,'Sp_Sub_Generate_Interface_Serverity',CONCAT('Done: ',TIMESTAMPDIFF(SECOND,START_TIME,SYSDATE()),' seconds.'), NOW());	
END$$
DELIMITER ;
