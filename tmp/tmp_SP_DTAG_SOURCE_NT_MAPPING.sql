CREATE DEFINER=`covmo`@`%` PROCEDURE `SP_DTAG_SOURCE_NT_MAPPING`(IN GT_DB VARCHAR(100),IN NT_DATE VARCHAR(100))
a_label:
BEGIN
  DECLARE START_TIME DATETIME DEFAULT SYSDATE();
  
  DECLARE ENABLE_GSM_FLAG VARCHAR(10);
  DECLARE ENABLE_UMTS_FLAG VARCHAR(10);
  DECLARE WORKER_ID VARCHAR(10) DEFAULT CONNECTION_ID();
  
  
  SELECT (`value`) INTO ENABLE_GSM_FLAG  FROM gt_gw_main.integration_param WHERE gt_group = 'ntparser' AND gt_name = 'enabledGSM' ;
  SELECT (`value`) INTO ENABLE_UMTS_FLAG  FROM gt_gw_main.integration_param WHERE gt_group = 'ntparser' AND gt_name = 'enabledUMTS' ;
  SET @@session.group_concat_max_len = @@global.max_allowed_packet;
 
 
INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'Import_WINA_miss_DATA_GSM','Start to update', NOW());	
    CALL gt_gw_main.Import_WINA_miss_DATA_GSM(GT_DB);
INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'Import_WINA_miss_DATA_UMTS','Start to update', NOW());	
    CALL gt_gw_main.Import_WINA_miss_DATA_UMTS(GT_DB);  

    INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'SP_DTAG_SOURCE_NT_MAPPING','Update RNC_ID', NOW());
    SET @SqlCmd=CONCAT(' UPDATE  ',GT_DB,'.nt_nbr_4_3_lte
        SET NBR_RNC_ID = (NBR_RNC_ID-51000000)
        ;');
    PREPARE Stmt FROM @SqlCmd;
    EXECUTE Stmt;
    DEALLOCATE PREPARE Stmt; 
    
    SET @SqlCmd=CONCAT(' UPDATE  ',GT_DB,'.nt_cell
        SET RNC_ID = (RNC_ID-51000000)
        ;');
    PREPARE Stmt FROM @SqlCmd;
    EXECUTE Stmt;
    DEALLOCATE PREPARE Stmt; 
    
    SET @SqlCmd=CONCAT(' UPDATE  ',GT_DB,'.nt_antenna
        SET RNC_ID = (RNC_ID-51000000)
        ;');
    PREPARE Stmt FROM @SqlCmd;
    EXECUTE Stmt;
    DEALLOCATE PREPARE Stmt; 
    
    SET @SqlCmd=CONCAT(' UPDATE  ',GT_DB,'.nt_neighbor
        SET RNC_ID = (RNC_ID-51000000)
        ;');
    PREPARE Stmt FROM @SqlCmd;
    EXECUTE Stmt;
    DEALLOCATE PREPARE Stmt; 
 
    SET @SqlCmd=CONCAT(' UPDATE  ',GT_DB,'.nt_neighbor
        SET NBR_RNC_ID = (NBR_RNC_ID-51000000) WHERE NBR_RNC_ID LIKE ''51%''
        ;');
    PREPARE Stmt FROM @SqlCmd;
    EXECUTE Stmt;
    DEALLOCATE PREPARE Stmt; 
 
    SET @SqlCmd=CONCAT(' UPDATE  ',GT_DB,'.nt_neighbor
        SET NBR_RNC_ID = (NBR_RNC_ID-41000000) WHERE NBR_RNC_ID LIKE ''41%''
        ;');
    PREPARE Stmt FROM @SqlCmd;
    EXECUTE Stmt;
    DEALLOCATE PREPARE Stmt;
    
    SET @SqlCmd=CONCAT(' UPDATE  ',GT_DB,'.nt_rnc
        SET RNC_ID = (RNC_ID-51000000)
        ;');
    PREPARE Stmt FROM @SqlCmd;
    EXECUTE Stmt;
    DEALLOCATE PREPARE Stmt; 
    
    SET @SqlCmd=CONCAT(' UPDATE  ',GT_DB,'.nt_cell_attribute
        SET RNC_ID = (RNC_ID-51000000)
        ;');
    PREPARE Stmt FROM @SqlCmd;
    EXECUTE Stmt;
    DEALLOCATE PREPARE Stmt; 
  
    SET @SqlCmd=CONCAT(' UPDATE  ',GT_DB,'.nt_sc_code
        SET RNC_ID = (RNC_ID-51000000)
        ;');
    PREPARE Stmt FROM @SqlCmd;
    EXECUTE Stmt;
    DEALLOCATE PREPARE Stmt;
  
       
    INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'SP_DTAG_SOURCE_NT_MAPPING','Update BSC_ID', NOW());
        SET @SqlCmd=CONCAT(' UPDATE  ',GT_DB,'.nt_nbr_4_2_lte
        SET NBR_BSC_ID = (NBR_BSC_ID-41000000)
        ;');
    PREPARE Stmt FROM @SqlCmd;
    EXECUTE Stmt;
    DEALLOCATE PREPARE Stmt; 
    
    SET @SqlCmd=CONCAT(' UPDATE  ',GT_DB,'.nt_cell_gsm
        SET BSC_ID = (BSC_ID-41000000)
        ;');
    PREPARE Stmt FROM @SqlCmd;
    EXECUTE Stmt;
    DEALLOCATE PREPARE Stmt; 
    
    SET @SqlCmd=CONCAT(' UPDATE  ',GT_DB,'.nt_antenna_gsm
        SET BSC_ID = (BSC_ID-41000000)
        ;');
    PREPARE Stmt FROM @SqlCmd;
    EXECUTE Stmt;
    DEALLOCATE PREPARE Stmt; 
    
    SET @SqlCmd=CONCAT(' UPDATE  ',GT_DB,'.nt_neighbor_gsm
        SET BSC_ID = (BSC_ID-41000000),NBR_BSC_ID = (NBR_BSC_ID-41000000)
        ;');
    PREPARE Stmt FROM @SqlCmd;
    EXECUTE Stmt;
    DEALLOCATE PREPARE Stmt; 
    
    SET @SqlCmd=CONCAT(' UPDATE  ',GT_DB,'.nt_bsc
        SET BSC_ID = (BSC_ID-41000000)
        ;');
    PREPARE Stmt FROM @SqlCmd;
    EXECUTE Stmt;
    DEALLOCATE PREPARE Stmt; 
  
  
    SET @SqlCmd=CONCAT('UPDATE ',GT_DB,'.nt_cell_lte
    SET ACT_START=(STR_TO_DATE(REPLACE(ACT_START,'':00.0'','':00''),''%Y-%m-%d %T''))                           
    ;');
    PREPARE Stmt FROM @SqlCmd;
    EXECUTE Stmt;
    DEALLOCATE PREPARE Stmt;
    
    SET @SqlCmd=CONCAT('UPDATE ',GT_DB,'.nt_cell_lte
    SET ACT_END=(STR_TO_DATE(REPLACE(ACT_END,'':00.0'','':00''),''%Y-%m-%d %T''))                           
    ;');
    PREPARE Stmt FROM @SqlCmd;
    EXECUTE Stmt;
    DEALLOCATE PREPARE Stmt;
    
    SET @SqlCmd=CONCAT('UPDATE ',GT_DB,'.nt_cell_lte
    SET PLAN_START=(STR_TO_DATE(REPLACE(PLAN_START,'':00.0'','':00''),''%Y-%m-%d %T''))                           
    ;');
    PREPARE Stmt FROM @SqlCmd;
    EXECUTE Stmt;
    DEALLOCATE PREPARE Stmt;
    
    SET @SqlCmd=CONCAT('UPDATE ',GT_DB,'.nt_cell_lte
    SET PLAN_END=(STR_TO_DATE(REPLACE(PLAN_END,'':00.0'','':00''),''%Y-%m-%d %T''))                           
    ;');
    PREPARE Stmt FROM @SqlCmd;
    EXECUTE Stmt;
    DEALLOCATE PREPARE Stmt;
    
    
    SET @SqlCmd=CONCAT('UPDATE ',GT_DB,'.nt_cell_lte
    SET CELL_NAME=(IF(CELL_NAME IS NULL,CONCAT(ENODEB_ID,''_'',CELL_ID),CONCAT(CELL_NAME,''_'',CELL_NE_ID)))                            
    ;');
    PREPARE Stmt FROM @SqlCmd;
    EXECUTE Stmt;
    DEALLOCATE PREPARE Stmt;
    
    
      SET @SqlCmd=CONCAT('UPDATE ',GT_DB,'.nt_cell_lte
            SET REGION_ID=(
            CASE CLUSTER_NAME_REGION
            WHEN ''BY'' THEN 1
            WHEN ''DO'' THEN 2
            WHEN ''FY'' THEN 3
            WHEN ''HH'' THEN 4
            WHEN ''HY'' THEN 5
            WHEN ''KY'' THEN 6
            WHEN ''LY'' THEN 7
            WHEN ''MY'' THEN 8
            WHEN ''NY'' THEN 9
            WHEN ''SY'' THEN 10 
            ELSE 99 
            END);');
    
    
    SET @SqlCmd=CONCAT('UPDATE ',GT_DB,'.nt_cell_lte
    SET EUTRABAND=(
    CASE
    WHEN DL_EARFCN >=0 AND DL_EARFCN<=599 THEN  1
    WHEN DL_EARFCN >=600 AND DL_EARFCN<=1199 THEN 2
    WHEN DL_EARFCN >=1200 AND DL_EARFCN<=1949 THEN 3
    WHEN DL_EARFCN >=1950 AND DL_EARFCN<=2399 THEN 4
    WHEN DL_EARFCN >=2400 AND DL_EARFCN<=2649 THEN 5
    WHEN DL_EARFCN >=2650 AND DL_EARFCN<=2749 THEN 6
    WHEN DL_EARFCN >=2750 AND DL_EARFCN<=3449 THEN 7
    WHEN DL_EARFCN >=3450 AND DL_EARFCN<=3799 THEN 8
    WHEN DL_EARFCN >=3800 AND DL_EARFCN<=4149 THEN 9
    WHEN DL_EARFCN >=4150 AND DL_EARFCN<=4749 THEN 10
    WHEN DL_EARFCN >=4750 AND DL_EARFCN<=4949 THEN 11
    WHEN DL_EARFCN >=5010 AND DL_EARFCN<=5179 THEN 12
    WHEN DL_EARFCN >=5180 AND DL_EARFCN<=5279 THEN 13
    WHEN DL_EARFCN >=5280 AND DL_EARFCN<=5379 THEN 14
    WHEN DL_EARFCN >=5730 AND DL_EARFCN<=5849 THEN 17
    WHEN DL_EARFCN >=5850 AND DL_EARFCN<=5999 THEN 18
    WHEN DL_EARFCN >=6000 AND DL_EARFCN<=6149 THEN 19
    WHEN DL_EARFCN >=6150 AND DL_EARFCN<=6449 THEN 20
    WHEN DL_EARFCN >=6450 AND DL_EARFCN<=6599 THEN 21
    WHEN DL_EARFCN >=6600 AND DL_EARFCN<=7399 THEN 22
    WHEN DL_EARFCN >=7500 AND DL_EARFCN<=7699 THEN 23
    WHEN DL_EARFCN >=7700 AND DL_EARFCN<=8039 THEN 24
    WHEN DL_EARFCN >=8040 AND DL_EARFCN<=8689 THEN 25
    WHEN DL_EARFCN >=8690 AND DL_EARFCN<=9039 THEN 26
    WHEN DL_EARFCN >=9040 AND DL_EARFCN<=9209 THEN 27
    WHEN DL_EARFCN >=9210 AND DL_EARFCN<=9659 THEN 28
    WHEN DL_EARFCN >=9660 AND DL_EARFCN<=9769 THEN 29
    WHEN DL_EARFCN >=9920 AND DL_EARFCN<=10359 THEN 32
    WHEN DL_EARFCN >=36000 AND DL_EARFCN<=36199 THEN 33
    WHEN DL_EARFCN >=36200 AND DL_EARFCN<=36349 THEN 34
    WHEN DL_EARFCN >=36350 AND DL_EARFCN<=36949 THEN 35
    WHEN DL_EARFCN >=36950 AND DL_EARFCN<=37549 THEN 36
    WHEN DL_EARFCN >=37550 AND DL_EARFCN<=37749 THEN 37
    WHEN DL_EARFCN >=37750 AND DL_EARFCN<=38249 THEN 38
    WHEN DL_EARFCN >=38250 AND DL_EARFCN<=38649 THEN 39
    WHEN DL_EARFCN >=38650 AND DL_EARFCN<=39649 THEN 40
    WHEN DL_EARFCN >=39650 AND DL_EARFCN<=41589 THEN 41
    WHEN DL_EARFCN >=41590 AND DL_EARFCN<=43589 THEN 42
    WHEN DL_EARFCN >=43590 AND DL_EARFCN<=45589 THEN 43
    WHEN DL_EARFCN >=45590 AND DL_EARFCN<=46589 THEN 44
    WHEN DL_EARFCN >=46590 AND DL_EARFCN<=46789 THEN 45
    WHEN DL_EARFCN >=46790 AND DL_EARFCN<=54539 THEN 46
    WHEN DL_EARFCN >=54540 AND DL_EARFCN<=55239 THEN 47
    WHEN DL_EARFCN >=55240 AND DL_EARFCN<=56739 THEN 48
    WHEN DL_EARFCN >=65536 AND DL_EARFCN<=66435 THEN 65
    WHEN DL_EARFCN >=66436 AND DL_EARFCN<=67335 THEN 66
    WHEN DL_EARFCN >=67336 AND DL_EARFCN<=67535 THEN 67
    WHEN DL_EARFCN >=67536 AND DL_EARFCN<=67835 THEN 68
    WHEN DL_EARFCN >=67836 AND DL_EARFCN<=68335 THEN 69
    WHEN DL_EARFCN >=68336 AND DL_EARFCN<=68585 THEN 70
    ELSE NULL
    END )                             
    
    ;');
    PREPARE Stmt FROM @SqlCmd;
    EXECUTE Stmt;
    DEALLOCATE PREPARE Stmt;
 
    
    # PU_ID
    SET @LAST_NT_DB = NULL ;
    SET @SqlCmd=CONCAT('SELECT TABLE_SCHEMA   into  @LAST_NT_DB FROM `information_schema`.`TABLES` WHERE TABLE_SCHEMA LIKE ''gt_nt%'' 
    AND TABLE_NAME = ''nt_cell_current_lte''   AND TABLE_SCHEMA  <> ''',GT_DB,'''    
    AND DATE(gt_strtok(TABLE_SCHEMA,3,''_''))   <  DATE(gt_strtok(''',GT_DB,''' ,3,''_''))  
    order by  TABLE_SCHEMA  desc limit 1
    ;');
    PREPARE Stmt FROM @SqlCmd;    
    EXECUTE Stmt;
    DEALLOCATE PREPARE Stmt;
  
    IF @LAST_NT_DB IS NOT NULL
      THEN 
		INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'SP_DTAG_SOURCE_NT_MAPPING','UPDATE nt_cell_lte PU use LAST nt_cell_current_lte', NOW());
	      SET @SqlCmd=CONCAT('UPDATE ',GT_DB,'.`nt_cell_lte` A, ',@LAST_NT_DB,'.`nt_cell_current_lte` B
		    SET 
		      A.PU_ID = B.PU_ID,
		      A.SUB_REGION_ID = B.PU_ID,
		      A.CLUSTER_NAME_SUB_REGION =B.CLUSTER_NAME_SUB_REGION
		    WHERE A.ENODEB_ID = B.ENODEB_ID
		    AND a.CELL_ID = b.CELL_ID;');
	      PREPARE Stmt FROM @SqlCmd;
	      EXECUTE Stmt;
	      DEALLOCATE PREPARE Stmt;
      
      ELSE
      
	SELECT 'no previous NT DB exists';
  
    END IF;
    CALL gt_gw_main.SP_Generate_PU_LTE(GT_DB,'GT_COVMO');
    INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'SP_DTAG_SOURCE_NT_MAPPING','Export pu_enodeb_mapping.csv(History/output)', NOW());
	SET @SqlCmd=CONCAT('
	      SELECT
	      ''ENODEB_ID'',
	      ''ENODEB_NAME'',
		''CLUSTER'',
		''CLUSTER_NAME_SUB_REGION'',
		''PU_ID'',
		''VENDOR''
	       UNION ALL
	       SELECT
	      `ENODEB_ID`,
		ENODEB_NAME,
		CLUSTER,
		CLUSTER_NAME_SUB_REGION,
		PU_ID,
		VENDOR
	    FROM gt_gw_main.`pu_enodeb_mapping`
	    INTO OUTFILE ''/',NT_DATE,'/pu_enodeb_mapping.csv''
	    FIELDS TERMINATED BY '',''
	    LINES TERMINATED BY ''\n'';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
 
#SLAVIP  
    SET @SqlCmd=CONCAT('UPDATE ',GT_DB,'.`nt_cell_lte` A, ',GT_DB,'.`NT_CELL_LTE_SLAVIP` B
          SET 
            A.SLA = B.SLA,
            A.VIP = B.VIP
          WHERE A.ENODEB_ID = B.ENODEB_ID 
              AND A.CELL_ID = B.CELL_ID;');
    PREPARE Stmt FROM @SqlCmd;
    EXECUTE Stmt;
    DEALLOCATE PREPARE Stmt;
  
    SET @SqlCmd=CONCAT('DROP TABLE IF EXISTS `',GT_DB,'`.`NT_CELL_LTE_SLAVIP`;');
    PREPARE Stmt FROM @SqlCmd;
    EXECUTE Stmt;
    DEALLOCATE PREPARE Stmt;
 
 
	INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'SP_DTAG_SOURCE_NT_MAPPING','Update nt_cell FREQUENCY', NOW());
	SET @SqlCmd=CONCAT('UPDATE ',GT_DB,'.`nt_cell` 
		SET FREQUENCY=(
			CASE WHEN DL_UARFCN/5 BETWEEN 900 AND 999  THEN 900
			WHEN DL_UARFCN/5 BETWEEN 1800 AND 1899  THEN 1800
			WHEN DL_UARFCN/5 BETWEEN 1900 AND 1999  THEN 1900
			WHEN DL_UARFCN/5 BETWEEN 2100 AND 2199  THEN 2100
			ELSE 2100 END);');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;  
	
   ###update nt_antenna_lte REPEATER rule (REPEATER_TYPE,REPEATER_TA_DELAY) #24599
 INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'SP_DTAG_SOURCE_NT_MAPPING','update nt_antenna_lte REPEATER rule #24599', NOW());
 	SET @SqlCmd=CONCAT('DROP TABLE IF EXISTS ',GT_DB,'.tmp_nt_antenna_lte;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt; 
	
	SET @SqlCmd=CONCAT('CREATE TABLE IF NOT EXISTS ',GT_DB,'.tmp_nt_antenna_lte AS 
        (
SELECT
d.ENODEB_ID,
d.CELL_ID,
d.LONGITUDE,
d.LATITUDE,
d.AZIMUTH,
d.ANTENNA_TYPE,
d.REPEATER,
b.REPEATER_TYPE,b.REPEATER_TA_DELAY,
d.ANTENNA_MODEL,
d.ANTENNA_HEIGHT,
d.ANTENNA_GAIN,
d.BEAM_WIDTH_HORIZONTAL,
d.BEAM_WIDTH_VERTICAL,
d.DOWN_TILT_MECHANICAL,
d.DOWN_TILT_ELECTRICAL,
d.FEEDER_ATTEN,
d.REFERENCE_SIGNAL_POWER,
d.ELEVATION,
d.INDOOR_TYPE,
d.ANTENNA_ID,
d.PATHLOSS_DISTANCE,
d.ANTENNA_RADIUS,
d.CLOSED_RADIUS,
d.FLAG
FROM ',GT_DB,'.repeater_ref_cell a INNER JOIN ',GT_DB,'.repeater b ON a.REPEATER_OSS_NODE_ID=b.REPEATER_OSS_NODE_ID LEFT JOIN ',GT_DB,'.`nt_cell_lte` c ON a.cell_OSS_NODE_ID=c.cell_OSS_NODE_ID
          INNER JOIN ',GT_DB,'.`nt_antenna_lte` d 
           ON c.enodeb_id = d.enodeb_id AND c.cell_id=d.cell_id);');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt; 
	
	SET @SqlCmd=CONCAT('UPDATE ',GT_DB,'.tmp_nt_antenna_lte SET REPEATER=1;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;  
	
	SET @SqlCmd=CONCAT('DELETE FROM ',GT_DB,'.nt_antenna_lte WHERE CONCAT (ENODEB_ID,cell_id) IN (SELECT CONCAT (ENODEB_ID,cell_id) FROM ',GT_DB,'.tmp_nt_antenna_lte);');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;  
	
	SET @SqlCmd=CONCAT('INSERT INTO ',GT_DB,'.tmp_nt_antenna_lte (
		SELECT
		d.ENODEB_ID,
		d.CELL_ID,
		d.LONGITUDE,
		d.LATITUDE,
		d.AZIMUTH,
		d.ANTENNA_TYPE,
		0 AS REPEATER,NULL AS REPEATER_TYPE,
		NULL AS REPEATER_TA_DELAY,
		d.ANTENNA_MODEL,
		d.ANTENNA_HEIGHT,
		d.ANTENNA_GAIN,
		d.BEAM_WIDTH_HORIZONTAL,
		d.BEAM_WIDTH_VERTICAL,
		d.DOWN_TILT_MECHANICAL,
		d.DOWN_TILT_ELECTRICAL,
		d.FEEDER_ATTEN,
		d.REFERENCE_SIGNAL_POWER,
		d.ELEVATION,
		d.INDOOR_TYPE,
		d.ANTENNA_ID,
		d.PATHLOSS_DISTANCE,
		d.ANTENNA_RADIUS,
		d.CLOSED_RADIUS,
		d.FLAG
		FROM ',GT_DB,'.tmp_nt_antenna_lte d GROUP BY d.ENODEB_ID,d.CELL_ID);');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;  
	
	SET @SqlCmd=CONCAT('INSERT INTO ',GT_DB,'.nt_antenna_lte 
      (ENODEB_ID,CELL_ID,LONGITUDE,LATITUDE,AZIMUTH,ANTENNA_TYPE,REPEATER,REPEATER_TYPE,REPEATER_TA_DELAY,ANTENNA_MODEL,ANTENNA_HEIGHT,ANTENNA_GAIN,
      BEAM_WIDTH_HORIZONTAL,BEAM_WIDTH_VERTICAL,DOWN_TILT_MECHANICAL,DOWN_TILT_ELECTRICAL,FEEDER_ATTEN,REFERENCE_SIGNAL_POWER,ELEVATION,INDOOR_TYPE,ANTENNA_ID,PATHLOSS_DISTANCE,ANTENNA_RADIUS,CLOSED_RADIUS,FLAG)
    SELECT  
	ENODEB_ID,
	CELL_ID,
	LONGITUDE,
	LATITUDE, 
	AZIMUTH,
	ANTENNA_TYPE,
	REPEATER,
	REPEATER_TYPE,
	REPEATER_TA_DELAY,
	ANTENNA_MODEL,
	ANTENNA_HEIGHT,
	ANTENNA_GAIN,
	BEAM_WIDTH_HORIZONTAL,
	BEAM_WIDTH_VERTICAL,
	DOWN_TILT_MECHANICAL,
	DOWN_TILT_ELECTRICAL,
	FEEDER_ATTEN ,
	REFERENCE_SIGNAL_POWER,
	ELEVATION,
	INDOOR_TYPE,
	RANK AS ANTENNA_ID,
	PATHLOSS_DISTANCE,
	ANTENNA_RADIUS,
	CLOSED_RADIUS,
	FLAG
      FROM 
      (SELECT 
      IF(@ENODEB_ID=A.ENODEB_ID AND @CELL_ID=A.CELL_ID,@ROWID:=@ROWID+1,@ROWID:=1) RANK,  
      A.*, @ENODEB_ID:=A.ENODEB_ID,@CELL_ID :=A.CELL_ID 
      FROM 
	(SELECT A.* FROM ',GT_DB,'.tmp_nt_antenna_lte A  
	ORDER BY A.ENODEB_ID,A.CELL_ID,REPEATER,REPEATER_TYPE,REPEATER_TA_DELAY,LONGITUDE DESC,LATITUDE DESC,AZIMUTH DESC) A
      ) AA
      WHERE 
      RANK  IS NOT NULL
      ;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;  
	
 	SET @SqlCmd=CONCAT('DROP TABLE IF EXISTS ',GT_DB,'.tmp_nt_antenna_lte;');
 	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;  
  
 INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'SP_DTAG_SOURCE_NT_MAPPING','update nt_antenna_lte REPEATER done', NOW());
 ###update nt_antenna_lte REPEATER done
 
 ##update GSM BA_LIST
/* INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'SP_DTAG_SOURCE_NT_MAPPING','update nt_cell_gsm BA_LIST', NOW());
	SET @SqlCmd=CONCAT('DROP TABLE IF EXISTS ',GT_DB,'.tmp_balist;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;  
	
	SET @SqlCmd=CONCAT('CREATE TABLE ',GT_DB,'.tmp_balist SELECT  bsc_id, cell_id,lac,GROUP_CONCAT(nbr_bcch_arfcn SEPARATOR ''|'') AS ba_list FROM ',GT_DB,'.nt_neighbor_gsm a GROUP BY bsc_id, cell_id,lac;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;  
	
	SET @SqlCmd=CONCAT('UPDATE ',GT_DB,'.nt_cell_gsm a,',GT_DB,'.tmp_balist b
		SET a.ba_list=b.ba_list WHERE a.bsc_id=b.bsc_id AND a.cell_id=b.cell_id AND a.lac=b.lac;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt; 
	
	SET @SqlCmd=CONCAT('DROP TABLE IF EXISTS ',GT_DB,'.tmp_balist;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt; 
	   
	INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'SP_DTAG_SOURCE_NT_MAPPING','update nt_cell_gsm BA_LIST done', NOW());
*/ 
 ###Keep NT_CELL_GSM,NT_CELL unique
 ##GSM
	INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'SP_DTAG_SOURCE_NT_MAPPING','keep NT_CELL_GSM,NT_CELL unique (#21746#26607)', NOW());
	
	SET @SqlCmd=CONCAT('DROP TABLE IF EXISTS ',GT_DB,'.tmp_NT_CELL_GSM;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;  
	
	SET @SqlCmd=CONCAT('CREATE TABLE ',GT_DB,'.tmp_NT_CELL_GSM LIKE ',GT_DB,'.NT_CELL_GSM;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('INSERT INTO ',GT_DB,'.tmp_NT_CELL_GSM SELECT * FROM ',GT_DB,'.NT_CELL_GSM GROUP BY BSC_ID,CELL_ID,LAC;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('TRUNCATE TABLE ',GT_DB,'.NT_CELL_GSM;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('INSERT INTO ',GT_DB,'.NT_CELL_GSM SELECT * FROM ',GT_DB,'.tmp_NT_CELL_GSM;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('DROP TABLE IF EXISTS ',GT_DB,'.tmp_NT_CELL_GSM;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
 ##UMTS
	SET @SqlCmd=CONCAT('DROP TABLE IF EXISTS ',GT_DB,'.tmp_NT_CELL;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;  
	
	SET @SqlCmd=CONCAT('CREATE TABLE ',GT_DB,'.tmp_NT_CELL LIKE ',GT_DB,'.NT_CELL;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('INSERT INTO ',GT_DB,'.tmp_NT_CELL SELECT * FROM ',GT_DB,'.NT_CELL GROUP BY RNC_ID,CELL_ID;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('TRUNCATE TABLE ',GT_DB,'.NT_CELL;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('INSERT INTO ',GT_DB,'.NT_CELL SELECT * FROM ',GT_DB,'.tmp_NT_CELL;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('DROP TABLE IF EXISTS ',GT_DB,'.tmp_NT_CELL;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
 ####
 
 ###Keep nt_antenna_gsm,nt_antenna unique
 ##GSM
	INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'SP_DTAG_SOURCE_NT_MAPPING','keep nt_antenna_gsm,nt_antenna unique (#21746#26607)', NOW());
	
	SET @SqlCmd=CONCAT('DROP TABLE IF EXISTS ',GT_DB,'.tmp_nt_antenna_gsm;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;  
	
	SET @SqlCmd=CONCAT('CREATE TABLE ',GT_DB,'.tmp_nt_antenna_gsm LIKE ',GT_DB,'.nt_antenna_gsm;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('INSERT INTO ',GT_DB,'.tmp_nt_antenna_gsm SELECT * FROM ',GT_DB,'.nt_antenna_gsm GROUP BY BSC_ID,CELL_ID,LAC;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('TRUNCATE TABLE ',GT_DB,'.nt_antenna_gsm;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('INSERT INTO ',GT_DB,'.nt_antenna_gsm SELECT * FROM ',GT_DB,'.tmp_nt_antenna_gsm;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('DROP TABLE IF EXISTS ',GT_DB,'.tmp_nt_antenna_gsm;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
 ##UMTS
	SET @SqlCmd=CONCAT('DROP TABLE IF EXISTS ',GT_DB,'.tmp_nt_antenna;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;  
	
	SET @SqlCmd=CONCAT('CREATE TABLE ',GT_DB,'.tmp_nt_antenna LIKE ',GT_DB,'.nt_antenna;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('INSERT INTO ',GT_DB,'.tmp_nt_antenna SELECT * FROM ',GT_DB,'.nt_antenna GROUP BY RNC_ID,CELL_ID;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('TRUNCATE TABLE ',GT_DB,'.nt_antenna;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('INSERT INTO ',GT_DB,'.nt_antenna SELECT * FROM ',GT_DB,'.tmp_nt_antenna;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('DROP TABLE IF EXISTS ',GT_DB,'.tmp_nt_antenna;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
 ####
 
  
	
	SET @SqlCmd=CONCAT('SELECT GROUP_CONCAT(CONCAT("''",COLUMN_NAME,"''")),TABLE_NAME INTO @Header , @table_name
		FROM INFORMATION_SCHEMA.COLUMNS 
		WHERE TABLE_NAME = ''nt_cell_lte''
		AND TABLE_SCHEMA = ''',GT_DB,'''
		ORDER BY ORDINAL_POSITION;');
	SELECT @SqlCmd;
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt; 
    
	IF @table_name='nt_cell_lte' THEN
		SET @SqlCmd=CONCAT('SELECT
		   ',@Header,'
		   UNION ALL
		   SELECT *
		FROM ',GT_DB,'.`nt_cell_lte`
		INTO OUTFILE ''/',NT_DATE,'/NT_CELL_LTE.csv''
		FIELDS TERMINATED BY '',''
		LINES TERMINATED BY ''\n'';');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
	ELSE
		SELECT 'table nt_cell_lte do not exists';
	END IF;	
    
    
    SET @SqlCmd=CONCAT('UPDATE ',GT_DB,'.nt_antenna_lte
    SET LONGITUDE=(SUBSTR(LPAD(LONGITUDE,6,''0''),1,2) + SUBSTR(LPAD(LONGITUDE,6,''0''),3,2)/60 ++ SUBSTR(LPAD(LONGITUDE,6,''0''),5,2)/3600)                            
    ;');
    PREPARE Stmt FROM @SqlCmd;
    EXECUTE Stmt;
    DEALLOCATE PREPARE Stmt;  
    
    SET @SqlCmd=CONCAT('UPDATE ',GT_DB,'.nt_antenna_lte
    SET LATITUDE=(SUBSTR(LPAD(LATITUDE,6,''0''),1,2) + SUBSTR(LPAD(LATITUDE,6,''0''),3,2)/60 ++ SUBSTR(LPAD(LATITUDE,6,''0''),5,2)/3600)                            
    ;');
    PREPARE Stmt FROM @SqlCmd;
    EXECUTE Stmt;
    DEALLOCATE PREPARE Stmt;
    
    
    
    SET @SqlCmd=CONCAT('UPDATE ',GT_DB,'.nt_antenna_lte
    SET REFERENCE_SIGNAL_POWER=(IFNULL(REFERENCE_SIGNAL_POWER/10,15.22))                            
    ;');
    PREPARE Stmt FROM @SqlCmd;
    EXECUTE Stmt;
    DEALLOCATE PREPARE Stmt;
    
    
    SET @SqlCmd=CONCAT('drop TABLE if exists ',GT_DB,'.tmp_nt_antenna_lte');
    PREPARE Stmt FROM @SqlCmd;
    EXECUTE Stmt;
    DEALLOCATE PREPARE Stmt; 
    
    
    SET @SqlCmd=CONCAT('CREATE TABLE ',GT_DB,'.tmp_nt_antenna_lte LIKE ',GT_DB,'.nt_antenna_lte');
    PREPARE Stmt FROM @SqlCmd;
    EXECUTE Stmt;
    DEALLOCATE PREPARE Stmt; 
    
    
    SET @SqlCmd=CONCAT('insert into  ',GT_DB,'.tmp_nt_antenna_lte
    select * from ',GT_DB,'.nt_antenna_lte
    ');
    PREPARE Stmt FROM @SqlCmd;
    EXECUTE Stmt;
    DEALLOCATE PREPARE Stmt;
    
    SET @SqlCmd=CONCAT('truncate table ',GT_DB,'.nt_antenna_lte
    ');
    PREPARE Stmt FROM @SqlCmd;
    EXECUTE Stmt;
    DEALLOCATE PREPARE Stmt;
    
    
    SET @SqlCmd=CONCAT('INSERT INTO ',GT_DB,'.nt_antenna_lte 
      (ENODEB_ID,CELL_ID,LONGITUDE,LATITUDE,AZIMUTH,ANTENNA_TYPE,REPEATER,REPEATER_TYPE,REPEATER_TA_DELAY,ANTENNA_MODEL,ANTENNA_HEIGHT,ANTENNA_GAIN,
      BEAM_WIDTH_HORIZONTAL,BEAM_WIDTH_VERTICAL,DOWN_TILT_MECHANICAL,DOWN_TILT_ELECTRICAL,FEEDER_ATTEN,REFERENCE_SIGNAL_POWER,ELEVATION,ANTENNA_ID,INDOOR_TYPE)
    SELECT  
      ENODEB_ID,
      CELL_ID,
      LONGITUDE,
      LATITUDE, 
      AZIMUTH,
      IFNULL(ANTENNA_TYPE,IF(INDOOR=1,1,2)) ANTENNA_TYPE ,REPEATER,REPEATER_TYPE,REPEATER_TA_DELAY,
      ANTENNA_MODEL,ANTENNA_HEIGHT,ANTENNA_GAIN,
      BEAM_WIDTH_HORIZONTAL,IFNULL(BEAM_WIDTH_VERTICAL,7) BEAM_WIDTH_VERTICAL ,IFNULL(DOWN_TILT_MECHANICAL,0) DOWN_TILT_MECHANICAL ,
      IFNULL(DOWN_TILT_ELECTRICAL,0) DOWN_TILT_ELECTRICAL ,
      IFNULL(FEEDER_ATTEN,3) FEEDER_ATTEN ,
      REFERENCE_SIGNAL_POWER,
      ELEVATION,
      RANK AS ANTENNA_ID,
      CASE  WHEN   ANTENNA_MODEL  LIKE ''Halle%'' THEN 1
      WHEN   ANTENNA_MODEL  = ''Tunnel''  THEN 1 -- #27426Indoor_Type.pptx
      WHEN   ANTENNA_MODEL  = ''FODAS''  THEN 1
      -- WHEN   ANTENNA_MODEL  <> ''Halle''  THEN 0  
      WHEN   ANTENNA_MODEL  = ''NoName''  THEN INDOOR
      ELSE  INDOOR END AS INDOOR_TYPE
      FROM 
      (SELECT 
      IF(@ENODEB_ID=A.ENODEB_ID AND @CELL_ID=A.CELL_ID,@ROWID:=@ROWID+1,@ROWID:=1) RANK,  
      A.*, @ENODEB_ID:=A.ENODEB_ID,@CELL_ID :=A.CELL_ID 
      FROM 
      (
        SELECT A.* ,C.INDOOR,C.DL_EARFCN
        FROM ',GT_DB,'.tmp_nt_antenna_lte A
        INNER JOIN ',GT_DB,'.NT_CELL_LTE C
        ON A.ENODEB_ID=C.ENODEB_ID 
        AND A.CELL_ID=C.CELL_ID
        
      ) A,( SELECT @ENODEB_ID:=NULL,@CELL_ID:=NULL ,@ROWID :=0) D
      ORDER BY A.ENODEB_ID,A.CELL_ID,REPEATER,REPEATER_TA_DELAY,LONGITUDE DESC,LATITUDE DESC,AZIMUTH DESC
      ) AA
      WHERE 
      RANK  IS NOT NULL 
      ;');
    PREPARE Stmt FROM @SqlCmd;
    EXECUTE Stmt;
    DEALLOCATE PREPARE Stmt; 
  
    SET @SqlCmd=CONCAT('UPDATE ',GT_DB,'.nt_antenna_lte SET ANTENNA_GAIN=16.75 WHERE INDOOR_TYPE=1 AND ANTENNA_GAIN IS NULL;');
    PREPARE Stmt FROM @SqlCmd;
    EXECUTE Stmt;
    DEALLOCATE PREPARE Stmt; 
   
    SET @SqlCmd=CONCAT('UPDATE ',GT_DB,'.nt_antenna_lte SET ANTENNA_GAIN=7 WHERE INDOOR_TYPE=0 AND ANTENNA_GAIN IS NULL;');
    PREPARE Stmt FROM @SqlCmd;
    EXECUTE Stmt;
    DEALLOCATE PREPARE Stmt;
 
    ## Update ANTENNA_MODEL ##
 ## Update nt_antenna_lte ANTENNA_MODEL ##
 INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'SP_DTAG_SOURCE_NT_MAPPING','Update nt_antenna_lte ANTENNA_MODEL', NOW());
  SET @SqlCmd =CONCAT('CREATE INDEX CATEGORIZE_IDX ON ',GT_DB,'.ANTENNA_INFO(CATEGORIZE,ELECTRICAL_TILT,FREQUENCY);') ;
  PREPARE Stmt FROM @SqlCmd;
  EXECUTE Stmt;
  DEALLOCATE PREPARE Stmt;
 
  SET @SqlCmd=CONCAT('TRUNCATE TABLE ',GT_DB,'.antenna_info;');
  PREPARE Stmt FROM @SqlCmd;
  EXECUTE Stmt;
  DEALLOCATE PREPARE Stmt; 
  
  SET @SqlCmd=CONCAT('INSERT INTO ',GT_DB,'.antenna_info
           SELECT * FROM  gt_gw_main.antenna_info;');
  PREPARE Stmt FROM @SqlCmd;
  EXECUTE Stmt;
  DEALLOCATE PREPARE Stmt; 
 
 SET @SqlCmd=CONCAT('ALTER IGNORE TABLE ',GT_DB,'.nt_antenna_lte ADD UNIQUE(ENODEB_ID,CELL_ID,ANTENNA_ID);');
 PREPARE Stmt FROM @SqlCmd;
 EXECUTE Stmt;
 DEALLOCATE PREPARE Stmt;
  
  SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS ',GT_DB,'.`tmp_nt_cell_lte_',WORKER_ID,'`;');
  PREPARE Stmt FROM @SqlCmd;
  EXECUTE Stmt;
  DEALLOCATE PREPARE Stmt; 
  
  SET @SqlCmd=CONCAT('CREATE TEMPORARY  TABLE ',GT_DB,'.`tmp_nt_cell_lte_',WORKER_ID,'`(
          `ENODEB_ID` MEDIUMINT(9) DEFAULT NULL,
          `CELL_ID` MEDIUMINT(9) DEFAULT NULL,
          `EUTRABAND` MEDIUMINT(9) DEFAULT NULL,
          `FREQUENCY` MEDIUMINT(9) DEFAULT NULL,
          KEY `FREQUENCY` (`FREQUENCY`)
        ) ENGINE=MYISAM DEFAULT CHARSET=latin1 DELAY_KEY_WRITE=1;');
  PREPARE Stmt FROM @SqlCmd;
  EXECUTE Stmt;
  DEALLOCATE PREPARE Stmt;  
 
  SET @SqlCmd=CONCAT(' INSERT INTO ',GT_DB,'.`tmp_nt_cell_lte_',WORKER_ID,'`
           SELECT ENODEB_ID,CELL_ID,EUTRABAND,
           CASE WHEN EUTRABAND = 3 THEN 1800 
		WHEN EUTRABAND = 7 THEN 2600 
		WHEN EUTRABAND = 20 THEN 800 
		WHEN EUTRABAND = 8 THEN 900 
		WHEN EUTRABAND = 32 THEN 1500 
		WHEN EUTRABAND = 42 THEN 3500 
		END AS FREQUENCY
           FROM ',GT_DB,'.nt_cell_lte;');
  PREPARE Stmt FROM @SqlCmd;
  EXECUTE Stmt;
  DEALLOCATE PREPARE Stmt; 
 
  SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS ',GT_DB,'.`temp_antenna`;');
  PREPARE Stmt FROM @SqlCmd;
  EXECUTE Stmt;
  DEALLOCATE PREPARE Stmt; 
  SET @SqlCmd=CONCAT('CREATE TEMPORARY TABLE IF NOT EXISTS ',GT_DB,'.`temp_antenna` AS 
        (
          SELECT DISTINCT a.enodeb_id, a.CELL_ID, a.ANTENNA_ID, a.ANTENNA_MODEL, 
            a.DOWN_TILT_ELECTRICAL, c.FREQUENCY 
          FROM ',GT_DB,'.`nt_antenna_lte` a
          INNER JOIN ',GT_DB,'.`tmp_nt_cell_lte_',WORKER_ID,'` c 
          WHERE a.enodeb_id = c.enodeb_id AND a.cell_id=c.cell_id
        );');
  PREPARE Stmt FROM @SqlCmd;
  EXECUTE Stmt;
  DEALLOCATE PREPARE Stmt; 
 
  SET @SqlCmd=CONCAT('ALTER TABLE ',GT_DB,'.`temp_antenna` ADD INDEX (ANTENNA_MODEL,FREQUENCY);');
  PREPARE Stmt FROM @SqlCmd;
  EXECUTE Stmt;
  DEALLOCATE PREPARE Stmt;
  
  
  SET @SqlCmd=CONCAT('UPDATE ',GT_DB,'.`temp_antenna`  a 
      LEFT JOIN ',GT_DB,'.antenna_info b ON a.ANTENNA_MODEL = b.CATEGORIZE AND a.frequency = b.FREQUENCY 
      SET a.DOWN_TILT_ELECTRICAL = b.ELECTRICAL_TILT 
      WHERE a.DOWN_TILT_ELECTRICAL != b.ELECTRICAL_TILT;');
  PREPARE Stmt FROM @SqlCmd;
  EXECUTE Stmt;
  DEALLOCATE PREPARE Stmt; 
  
  SET @SqlCmd=CONCAT('ALTER TABLE ',GT_DB,'.`temp_antenna` ADD INDEX (enodeb_id, CELL_ID, ANTENNA_ID, FREQUENCY);');
  PREPARE Stmt FROM @SqlCmd;
  EXECUTE Stmt;
  DEALLOCATE PREPARE Stmt;
 
  /*SET @SqlCmd =CONCAT('INSERT INTO ',GT_DB,'.nt_log SELECT target.enodeb_id,target.cell_id,''nt_antenna_lte'',target.ANTENNA_MODEL,''2'',''NO antenna_info CAN MAP'' FROM ',GT_DB,'.nt_antenna_lte target LEFT JOIN ',GT_DB,'.temp_antenna a ON target.enodeb_id = a.enodeb_id AND target.cell_id=a.cell_id
        LEFT JOIN ',GT_DB,'.antenna_info b ON a.ANTENNA_MODEL = b.CATEGORIZE AND a.frequency = b.FREQUENCY AND a.DOWN_TILT_ELECTRICAL = b.ELECTRICAL_TILT
        WHERE b.`ANTENNA_MODEL` IS NULL OR b.`ANTENNA_MODEL`=''''') ;
  PREPARE Stmt FROM @SqlCmd;
  EXECUTE Stmt;
  DEALLOCATE PREPARE Stmt;*/
  
  SET @SqlCmd=CONCAT('UPDATE ',GT_DB,'.nt_antenna_lte target
        LEFT JOIN ',GT_DB,'.temp_antenna a ON target.enodeb_id = a.enodeb_id AND target.cell_id=a.cell_id
        LEFT JOIN ',GT_DB,'.antenna_info b ON a.ANTENNA_MODEL = b.CATEGORIZE and a.frequency = b.FREQUENCY and a.DOWN_TILT_ELECTRICAL = b.ELECTRICAL_TILT
        SET target.ANTENNA_MODEL=IF(b.`ANTENNA_MODEL` IS NULL OR b.`ANTENNA_MODEL`='''',''NoName'',b.`ANTENNA_MODEL`);');
  PREPARE Stmt FROM @SqlCmd;
  EXECUTE Stmt;
  DEALLOCATE PREPARE Stmt;
 
-- update nt_antenna_lte ANTENNA_TYPE #27615 
	INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'SP_DTAG_SOURCE_NT_MAPPING','#27615 Update nt_antenna_lte ANTENNA_TYPE', NOW());
 	SET @SqlCmd=CONCAT('UPDATE ',GT_DB,'.nt_antenna_lte target
		      INNER JOIN gt_gw_main.antenna_info a ON target.ANTENNA_MODEL=a.ANTENNA_MODEL
		      SET target.ANTENNA_TYPE=a.TYPE;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt; 
    
	SET @SqlCmd=CONCAT('SELECT GROUP_CONCAT(CONCAT("''",COLUMN_NAME,"''")),TABLE_NAME INTO @Header , @table_name
		FROM INFORMATION_SCHEMA.COLUMNS 
		WHERE TABLE_NAME = ''nt_antenna_lte''
		AND TABLE_SCHEMA = ''',GT_DB,'''
		ORDER BY ORDINAL_POSITION;');
	SELECT @SqlCmd;
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt; 
	
	IF @table_name='nt_antenna_lte' THEN
	    SET @SqlCmd=CONCAT('
		SELECT
		  ',@Header,'
		 UNION ALL
		   SELECT *
	    FROM ',GT_DB,'.`nt_antenna_lte`
	    INTO OUTFILE ''/',NT_DATE,'/NT_ANTENNA_LTE.csv''
	    FIELDS TERMINATED BY '',''
	    LINES TERMINATED BY ''\n'';');
	    PREPARE Stmt FROM @SqlCmd;
	    EXECUTE Stmt;
	    DEALLOCATE PREPARE Stmt;
	ELSE
		SELECT 'table nt_antenna_lte do not exists';
	END IF;
    
 
	SET @SqlCmd=CONCAT('SELECT GROUP_CONCAT(CONCAT("''",COLUMN_NAME,"''")),TABLE_NAME INTO @Header , @table_name
		FROM INFORMATION_SCHEMA.COLUMNS 
		WHERE TABLE_NAME = ''nt_mme_lte''
		AND TABLE_SCHEMA = ''',GT_DB,'''
		ORDER BY ORDINAL_POSITION;');
	SELECT @SqlCmd;
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt; 
	
	IF @table_name='nt_mme_lte' THEN
	    SET @SqlCmd=CONCAT('SELECT
		  ',@Header,'
		 UNION ALL SELECT *  from ',GT_DB,'.`nt_mme_lte`
	    INTO OUTFILE ''/',NT_DATE,'/NT_MME_LTE.csv''
	    FIELDS TERMINATED BY '',''
	    LINES TERMINATED BY ''\n'';');
	    PREPARE Stmt FROM @SqlCmd;
	    EXECUTE Stmt;
	    DEALLOCATE PREPARE Stmt;
	ELSE
		SELECT 'table nt_mme_lte do not exists';
	END IF;
 
	SET @SqlCmd=CONCAT('SELECT GROUP_CONCAT(CONCAT("''",COLUMN_NAME,"''")),TABLE_NAME INTO @Header , @table_name
		FROM INFORMATION_SCHEMA.COLUMNS 
		WHERE TABLE_NAME = ''nt_nbr_4_2_lte''
		AND TABLE_SCHEMA = ''',GT_DB,'''
		ORDER BY ORDINAL_POSITION;');
	SELECT @SqlCmd;
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt; 
	
      	IF @table_name='nt_nbr_4_2_lte' THEN
	
	    SET @SqlCmd=CONCAT('SELECT ',@Header,'
	    UNION ALL
	    select * from ',GT_DB,'.`nt_nbr_4_2_lte`
	    INTO OUTFILE ''/',NT_DATE,'/NT_NBR_4_2_LTE.csv''
	    FIELDS TERMINATED BY '',''
	    LINES TERMINATED BY ''\n'';');
	    PREPARE Stmt FROM @SqlCmd;
	    EXECUTE Stmt;
	    DEALLOCATE PREPARE Stmt;  
	ELSE
		SELECT 'table nt_nbr_4_2_lte do not exists';
	END IF;
	
	
	SET @SqlCmd=CONCAT('SELECT GROUP_CONCAT(CONCAT("''",COLUMN_NAME,"''")),TABLE_NAME INTO @Header , @table_name
		FROM INFORMATION_SCHEMA.COLUMNS 
		WHERE TABLE_NAME = ''nt_nbr_4_3_lte''
		AND TABLE_SCHEMA = ''',GT_DB,'''
		ORDER BY ORDINAL_POSITION;');
	SELECT @SqlCmd;
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	IF @table_name='nt_nbr_4_3_lte' THEN
    
	    SET @SqlCmd=CONCAT('SELECT ',@Header,'
	    UNION ALL
	    SELECT *  from ',GT_DB,'.`nt_nbr_4_3_lte`
	    INTO OUTFILE ''/',NT_DATE,'/NT_NBR_4_3_LTE.csv''
	    FIELDS TERMINATED BY '',''
	    LINES TERMINATED BY ''\n'';');
	    PREPARE Stmt FROM @SqlCmd;
	    EXECUTE Stmt;
	    DEALLOCATE PREPARE Stmt;  
	ELSE
		SELECT 'table nt_nbr_4_3_lte do not exists';
	END IF;
    
    SET @SqlCmd=CONCAT('UPDATE ',GT_DB,'.nt_nbr_4_4_lte
    SET NBR_TYPE=( CASE TRIM(NBR_TYPE) WHEN ''INTER'' THEN 1 WHEN ''INTRA'' THEN 2 ELSE NBR_TYPE END )                            
    ;');
    PREPARE Stmt FROM @SqlCmd;
    EXECUTE Stmt;
    DEALLOCATE PREPARE Stmt;
    
	SET @SqlCmd=CONCAT('SELECT GROUP_CONCAT(CONCAT("''",COLUMN_NAME,"''")),TABLE_NAME INTO @Header , @table_name
		FROM INFORMATION_SCHEMA.COLUMNS 
		WHERE TABLE_NAME = ''nt_nbr_4_4_lte''
		AND TABLE_SCHEMA = ''',GT_DB,'''
		ORDER BY ORDINAL_POSITION;');
	SELECT @SqlCmd;
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt; 
	IF @table_name='nt_nbr_4_4_lte' THEN
    	    SET @SqlCmd=CONCAT('SELECT ',@Header,'
	    UNION ALL
	    SELECT * FROM ',GT_DB,'.`nt_nbr_4_4_lte`
	    INTO OUTFILE ''/',NT_DATE,'/NT_NBR_4_4_LTE.csv''
	    FIELDS TERMINATED BY '',''
	    LINES TERMINATED BY ''\n'';');
	    PREPARE Stmt FROM @SqlCmd;
	    EXECUTE Stmt;
	    DEALLOCATE PREPARE Stmt;  
	ELSE
		SELECT 'table nt_nbr_4_4_lte do not exists';
	END IF;
	
	SET @SqlCmd=CONCAT('SELECT GROUP_CONCAT(CONCAT("''",COLUMN_NAME,"''")),TABLE_NAME INTO @Header , @table_name
		FROM INFORMATION_SCHEMA.COLUMNS 
		WHERE TABLE_NAME = ''nt_tac_cell_lte''
		AND TABLE_SCHEMA = ''',GT_DB,'''
		ORDER BY ORDINAL_POSITION;');
	SELECT @SqlCmd;
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt; 
	
	IF @table_name='nt_tac_cell_lte' THEN
    
	    SET @SqlCmd=CONCAT('SELECT ',@Header,' FROM ',GT_DB,'.`nt_tac_cell_lte`
	    INTO OUTFILE ''/',NT_DATE,'/NT_TAC_CELL_LTE.csv''
	    FIELDS TERMINATED BY '',''
	    LINES TERMINATED BY ''\n'';');
	    PREPARE Stmt FROM @SqlCmd;
	    EXECUTE Stmt;
	    DEALLOCATE PREPARE Stmt;
		
	ELSE
		SELECT 'table nt_cell_lte do not exists';
	END IF;
    
    
    
    SET @SqlCmd=CONCAT('truncate table ',GT_DB,'.rule_nt_nbr_enodeb_lte;');
    PREPARE Stmt FROM @SqlCmd;
    EXECUTE Stmt;
    DEALLOCATE PREPARE Stmt; 
    
    SET @SqlCmd=CONCAT('INSERT INTO ',GT_DB,'.rule_nt_nbr_enodeb_lte 
    SELECT RIGHT(ENODEB_OSS_NODE_ID_SOURCE,6),MCC_TARGET,MNC_TARGET,ENODEBID_TARGET,
    CASE WHEN X2LIST_TYPE LIKE ''%Blacklist%'' THEN 1 ELSE 2 END AS BL_TYPE 
    FROM ',GT_DB,'.NT_NBR_LTE_BLACKLIST ;');
    PREPARE Stmt FROM @SqlCmd;
    EXECUTE Stmt;
    DEALLOCATE PREPARE Stmt; 
    
    
    SET @SqlCmd=CONCAT('truncate table ',GT_DB,'.rule_nt_nbr_cell_pci_lte;');
    PREPARE Stmt FROM @SqlCmd;
    EXECUTE Stmt;
    DEALLOCATE PREPARE Stmt; 
    
    SET @SqlCmd=CONCAT('INSERT INTO ',GT_DB,'.rule_nt_nbr_cell_pci_lte 
    SELECT ENODEB_ID,CELL_ID,ifnull(NBCB_FREQUENZ,-1),NBCB_PCI_STARTWERT  FROM ',GT_DB,'.NT_NBR_CELL_BLACKLIST_PCI ;');
    PREPARE Stmt FROM @SqlCmd;
    EXECUTE Stmt;
    DEALLOCATE PREPARE Stmt; 
    SET @SqlCmd=CONCAT('select max(NBCB_PCI_BEREICH)+1 into @pci_max  from ',GT_DB,'.NT_NBR_CELL_BLACKLIST_PCI;');
    PREPARE Stmt FROM @SqlCmd;
    EXECUTE Stmt;
    DEALLOCATE PREPARE Stmt;
        
    
    IF @pci_max  > 0
          
    THEN
        
    SET @v_i=1;
    SET @SqlCmd=CONCAT('select max(NBCB_PCI_BEREICH) into @v_R_Max  from ',GT_DB,'.NT_NBR_CELL_BLACKLIST_PCI;');
    PREPARE Stmt FROM @SqlCmd;
    EXECUTE Stmt;
    DEALLOCATE PREPARE Stmt;
    
    SET @SqlCmd=CONCAT('drop table if exists ',GT_DB,'.tmp_nt_nbr_cell_blacklist_pci ;');
    PREPARE Stmt FROM @SqlCmd;
    EXECUTE Stmt;
    DEALLOCATE PREPARE Stmt; 
    
    SET @SqlCmd=CONCAT('create table ',GT_DB,'.`tmp_nt_nbr_cell_blacklist_pci` (
    `ENODEB_ID` MEDIUMINT(9) DEFAULT NULL,
    `CELL_ID` MEDIUMINT(9) DEFAULT NULL,
    `NBCB_FREQUENZ` MEDIUMINT(9) DEFAULT NULL,
    `NBCB_PCI_STARTWERT` SMALLINT(6) DEFAULT NULL,
    `NBCB_PCI_BEREICH` SMALLINT(6) DEFAULT NULL,
    KEY `NT_NBR_CELL_BLACKLIST_PCI_IDX1` (`ENODEB_ID`,`CELL_ID`)
    ) ENGINE=MYISAM DEFAULT CHARSET=utf8 DELAY_KEY_WRITE=1;');
    PREPARE Stmt FROM @SqlCmd;
    EXECUTE Stmt;
    DEALLOCATE PREPARE Stmt;
    
    
    SET @SqlCmd=CONCAT('INSERT INTO ',GT_DB,'.tmp_nt_nbr_cell_blacklist_pci 
    SELECT ENODEB_ID,CELL_ID,NBCB_FREQUENZ,NBCB_PCI_STARTWERT,NBCB_PCI_BEREICH  FROM ',GT_DB,'.NT_NBR_CELL_BLACKLIST_PCI  
    where NBCB_PCI_BEREICH >0;');
    PREPARE Stmt FROM @SqlCmd;
    EXECUTE Stmt;
    DEALLOCATE PREPARE Stmt; 
    
    WHILE @v_i <= @v_R_Max DO
    BEGIN
    
    
    
    SET @SqlCmd=CONCAT('UPDATE ',GT_DB,'.tmp_nt_nbr_cell_blacklist_pci 
      SET NBCB_PCI_STARTWERT=NBCB_PCI_STARTWERT+1,NBCB_PCI_BEREICH = NBCB_PCI_BEREICH -1
      WHERE NBCB_PCI_BEREICH >=0;');
    PREPARE Stmt FROM @SqlCmd;
    EXECUTE Stmt;
    DEALLOCATE PREPARE Stmt;
    
    SET @SqlCmd=CONCAT('INSERT INTO ',GT_DB,'.rule_nt_nbr_cell_pci_lte 
    SELECT ENODEB_ID,CELL_ID,NBCB_FREQUENZ,NBCB_PCI_STARTWERT  FROM ',GT_DB,'.tmp_nt_nbr_cell_blacklist_pci  
    where NBCB_PCI_BEREICH >=0;');
    PREPARE Stmt FROM @SqlCmd;
    EXECUTE Stmt;
    DEALLOCATE PREPARE Stmt; 
    
    SET @SqlCmd=CONCAT('delete from  ',GT_DB,'.tmp_nt_nbr_cell_blacklist_pci 
    where NBCB_PCI_BEREICH  =0;');
    PREPARE Stmt FROM @SqlCmd;
    EXECUTE Stmt;
    DEALLOCATE PREPARE Stmt; 
    SET @v_i=@v_i+1; 
          
    END;
    END WHILE;
      
    END IF;
    
    SET @SqlCmd=CONCAT('drop table if exists ',GT_DB,'.tmp_nt_nbr_cell_blacklist_pci ;');
    PREPARE Stmt FROM @SqlCmd;
    EXECUTE Stmt;
    DEALLOCATE PREPARE Stmt; 
    
    
    
    SET @SqlCmd=CONCAT('select ENODEB_ID,CELL_ID,NBCB_FREQUENZ,NBCB_PCI_STARTWERT,NBCB_PCI_BEREICH from ',GT_DB,'.`NT_NBR_CELL_BLACKLIST_PCI`
    INTO OUTFILE ''/',NT_DATE,'/NT_NBR_CELL_BLACKLIST_PCI.csv''
    FIELDS TERMINATED BY '',''
    LINES TERMINATED BY ''\n'';');
    PREPARE Stmt FROM @SqlCmd;
    EXECUTE Stmt;
    DEALLOCATE PREPARE Stmt;
    
    
    SET @SqlCmd=CONCAT('select ENODEB_OSS_NODE_ID_SOURCE,MCC_TARGET,MNC_TARGET,ENODEBID_TARGET,X2LIST_TYPE from ',GT_DB,'.`NT_NBR_LTE_BLACKLIST`
    INTO OUTFILE ''/',NT_DATE,'/NT_NBR_LTE_BLACKLIST.csv''
    FIELDS TERMINATED BY '',''
    LINES TERMINATED BY ''\n'';');
    PREPARE Stmt FROM @SqlCmd;
    EXECUTE Stmt;
    DEALLOCATE PREPARE Stmt;
    
    
    
    SET @SqlCmd=CONCAT('
    select ''ENODEB_ID'',''CELL_ID'',''BL_FREQ_EARFCN'',''BL_PCI'' UNION ALL
    select ENODEB_ID,CELL_ID,BL_FREQ_EARFCN,BL_PCI from ',GT_DB,'.`RULE_NT_NBR_CELL_PCI_LTE`
    INTO OUTFILE ''/',NT_DATE,'/RULE_NT_NBR_CELL_PCI_LTE.csv''
    FIELDS TERMINATED BY '',''
    LINES TERMINATED BY ''\n'';');
    PREPARE Stmt FROM @SqlCmd;
    EXECUTE Stmt;
    DEALLOCATE PREPARE Stmt;
    
    
    SET @SqlCmd=CONCAT('
    SELECT ''ENODEB_ID'',''BL_MCC_TRAGET'',''BL_MNC_TARGET'',''BL_ENODEB_ID'',''BL_TYPE'' UNION ALL
    select ENODEB_ID,BL_MCC_TRAGET,BL_MNC_TARGET,BL_ENODEB_ID,BL_TYPE from ',GT_DB,'.`RULE_NT_NBR_ENODEB_LTE`
    INTO OUTFILE ''/',NT_DATE,'/RULE_NT_NBR_ENODEB_LTE.csv''
    FIELDS TERMINATED BY '',''
    LINES TERMINATED BY ''\n'';');
    PREPARE Stmt FROM @SqlCmd;
    EXECUTE Stmt;
    DEALLOCATE PREPARE Stmt;
  
  
  
  IF ENABLE_GSM_FLAG = 'true' THEN
  
    
    SET @SqlCmd=CONCAT('UPDATE ',GT_DB,'.nt_antenna_gsm
    SET LONGITUDE=(SUBSTR(LPAD(LONGITUDE,6,''0''),1,2) + SUBSTR(LPAD(LONGITUDE,6,''0''),3,2)/60 ++ SUBSTR(LPAD(LONGITUDE,6,''0''),5,2)/3600)                            
    ;');
    PREPARE Stmt FROM @SqlCmd;
    EXECUTE Stmt;
    DEALLOCATE PREPARE Stmt;  
    
    SET @SqlCmd=CONCAT('UPDATE ',GT_DB,'.nt_antenna_gsm
    SET LATITUDE=(SUBSTR(LPAD(LATITUDE,6,''0''),1,2) + SUBSTR(LPAD(LATITUDE,6,''0''),3,2)/60 ++ SUBSTR(LPAD(LATITUDE,6,''0''),5,2)/3600)                            
    ;');
    PREPARE Stmt FROM @SqlCmd;
    EXECUTE Stmt;
    DEALLOCATE PREPARE Stmt;
  
    
    SET @SqlCmd=CONCAT('UPDATE ',GT_DB,'.nt_cell_gsm
    SET LONGITUDE=(SUBSTR(LPAD(LONGITUDE,6,''0''),1,2) + SUBSTR(LPAD(LONGITUDE,6,''0''),3,2)/60 ++ SUBSTR(LPAD(LONGITUDE,6,''0''),5,2)/3600)                            
    ;');
    PREPARE Stmt FROM @SqlCmd;
    EXECUTE Stmt;
    DEALLOCATE PREPARE Stmt;  
    
    SET @SqlCmd=CONCAT('UPDATE ',GT_DB,'.nt_cell_gsm
    SET LATITUDE=(SUBSTR(LPAD(LATITUDE,6,''0''),1,2) + SUBSTR(LPAD(LATITUDE,6,''0''),3,2)/60 ++ SUBSTR(LPAD(LATITUDE,6,''0''),5,2)/3600)                            
    ;');
    PREPARE Stmt FROM @SqlCmd;
    EXECUTE Stmt;
    DEALLOCATE PREPARE Stmt;
  
    SET @SqlCmd=CONCAT('UPDATE ',GT_DB,'.nt_antenna_gsm
    SET ANTENNA_ID= 1                       
    ;');
    PREPARE Stmt FROM @SqlCmd;
    EXECUTE Stmt;
    DEALLOCATE PREPARE Stmt;
 
	SET @SqlCmd=CONCAT('SELECT GROUP_CONCAT(CONCAT("''",COLUMN_NAME,"''")),TABLE_NAME INTO @Header , @table_name
		FROM INFORMATION_SCHEMA.COLUMNS 
		WHERE TABLE_NAME = ''nt_antenna_gsm''
		AND TABLE_SCHEMA = ''',GT_DB,'''
		ORDER BY ORDINAL_POSITION;');
	SELECT @SqlCmd;
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt; 
	
	IF @table_name='nt_antenna_gsm' THEN
	    
	    SET @SqlCmd=CONCAT('SELECT ',@Header,'
	    UNION ALL
	    SELECT *
	    FROM ',GT_DB,'.`nt_antenna_gsm`
	    INTO OUTFILE ''/',NT_DATE,'/NT_ANTENNA_GSM.csv''
	    FIELDS TERMINATED BY '',''
	    LINES TERMINATED BY ''\n'';');
	    PREPARE Stmt FROM @SqlCmd;
	    EXECUTE Stmt;
	    DEALLOCATE PREPARE Stmt;
	 
	ELSE
		SELECT 'table nt_antenna_gsm do not exists';
	END IF;
	
	SET @SqlCmd=CONCAT('SELECT GROUP_CONCAT(CONCAT("''",COLUMN_NAME,"''")),TABLE_NAME INTO @Header , @table_name
		FROM INFORMATION_SCHEMA.COLUMNS 
		WHERE TABLE_NAME = ''nt_bsc''
		AND TABLE_SCHEMA = ''',GT_DB,'''
		ORDER BY ORDINAL_POSITION;');
	SELECT @SqlCmd;
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;   
	
	IF @table_name='nt_bsc' THEN  
 
	    SET @SqlCmd=CONCAT('SELECT ',@Header,'
	    UNION ALL
	    SELECT *
	    FROM ',GT_DB,'.`nt_bsc`
	    INTO OUTFILE ''/',NT_DATE,'/NT_BSC.csv''
	    FIELDS TERMINATED BY '',''
	    LINES TERMINATED BY ''\n'';');
	    PREPARE Stmt FROM @SqlCmd;
	    EXECUTE Stmt;
	    DEALLOCATE PREPARE Stmt;
	ELSE
		SELECT 'table nt_bsc do not exists';
	END IF;
	
	SET @SqlCmd=CONCAT('SELECT GROUP_CONCAT(CONCAT("''",COLUMN_NAME,"''")),TABLE_NAME INTO @Header , @table_name
		FROM INFORMATION_SCHEMA.COLUMNS 
		WHERE TABLE_NAME = ''nt_cell_gsm''
		AND TABLE_SCHEMA = ''',GT_DB,'''
		ORDER BY ORDINAL_POSITION;');
	SELECT @SqlCmd;
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt; 
	
	IF @table_name='nt_cell_gsm' THEN
    
	    SET @SqlCmd=CONCAT('SELECT ',@Header,'
	      UNION ALL
	      SELECT *		
	    FROM ',GT_DB,'.`nt_cell_gsm`
	    INTO OUTFILE ''/',NT_DATE,'/NT_CELL_GSM.csv''
	    FIELDS TERMINATED BY '',''
	    LINES TERMINATED BY ''\n'';');
	    PREPARE Stmt FROM @SqlCmd;
	    EXECUTE Stmt;
	    DEALLOCATE PREPARE Stmt;
	ELSE
		SELECT 'table nt_cell_gsm do not exists';
	END IF;  
	
	SET @SqlCmd=CONCAT('SELECT GROUP_CONCAT(CONCAT("''",COLUMN_NAME,"''")),TABLE_NAME INTO @Header , @table_name
		FROM INFORMATION_SCHEMA.COLUMNS 
		WHERE TABLE_NAME = ''NT_NEIGHBOR_GSM''
		AND TABLE_SCHEMA = ''',GT_DB,'''
		ORDER BY ORDINAL_POSITION;');
	SELECT @SqlCmd;
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;  
	
	IF @table_name='NT_NEIGHBOR_GSM' THEN
 
	    SET @SqlCmd=CONCAT('SELECT ',@Header,'
	      UNION ALL
	      SELECT *
	    FROM ',GT_DB,'.`NT_NEIGHBOR_GSM`
	    INTO OUTFILE ''/',NT_DATE,'/NT_NEIGHBOR_GSM.csv''
	    FIELDS TERMINATED BY '',''
	    LINES TERMINATED BY ''\n'';');
	    PREPARE Stmt FROM @SqlCmd;
	    EXECUTE Stmt;
	    DEALLOCATE PREPARE Stmt;
 
	ELSE
		SELECT 'table NT_NEIGHBOR_GSM do not exists';
	END IF;
  
  
  END IF;
  
  
    
  IF ENABLE_UMTS_FLAG = 'true' THEN
    
    
    SET @SqlCmd=CONCAT('UPDATE ',GT_DB,'.nt_antenna
    SET LONGITUDE=(SUBSTR(LPAD(LONGITUDE,6,''0''),1,2) + SUBSTR(LPAD(LONGITUDE,6,''0''),3,2)/60 ++ SUBSTR(LPAD(LONGITUDE,6,''0''),5,2)/3600)                            
    ;');
    PREPARE Stmt FROM @SqlCmd;
    EXECUTE Stmt;
    DEALLOCATE PREPARE Stmt;  
    
    SET @SqlCmd=CONCAT('UPDATE ',GT_DB,'.nt_antenna
    SET LATITUDE=(SUBSTR(LPAD(LATITUDE,6,''0''),1,2) + SUBSTR(LPAD(LATITUDE,6,''0''),3,2)/60 ++ SUBSTR(LPAD(LATITUDE,6,''0''),5,2)/3600)                            
    ;');
    PREPARE Stmt FROM @SqlCmd;
    EXECUTE Stmt;
    DEALLOCATE PREPARE Stmt;
  
  
    SET @SqlCmd=CONCAT('UPDATE ',GT_DB,'.nt_antenna
    SET ANTENNA_ID= 1                       
    ;');
    PREPARE Stmt FROM @SqlCmd;
    EXECUTE Stmt;
    DEALLOCATE PREPARE Stmt;
  
  
    SET @SqlCmd=CONCAT('UPDATE ',GT_DB,'.nt_cell
    SET LONGITUDE=(SUBSTR(LPAD(LONGITUDE,6,''0''),1,2) + SUBSTR(LPAD(LONGITUDE,6,''0''),3,2)/60 ++ SUBSTR(LPAD(LONGITUDE,6,''0''),5,2)/3600)                            
    ;');
    PREPARE Stmt FROM @SqlCmd;
    EXECUTE Stmt;
    DEALLOCATE PREPARE Stmt;  
    
    SET @SqlCmd=CONCAT('UPDATE ',GT_DB,'.nt_cell
    SET LATITUDE=(SUBSTR(LPAD(LATITUDE,6,''0''),1,2) + SUBSTR(LPAD(LATITUDE,6,''0''),3,2)/60 ++ SUBSTR(LPAD(LATITUDE,6,''0''),5,2)/3600)                            
    ;');
    PREPARE Stmt FROM @SqlCmd;
    EXECUTE Stmt;
    DEALLOCATE PREPARE Stmt;
 
--     SET @SqlCmd=CONCAT('UPDATE ',GT_DB,'.nt_antenna
-- 	SET INDOOR_TYPE=0,ANTENNA_ID=1                          
--     ;');
--     PREPARE Stmt FROM @SqlCmd;
--     EXECUTE Stmt;
--     DEALLOCATE PREPARE Stmt;  
    
    SET @SqlCmd=CONCAT('UPDATE ',GT_DB,'.nt_antenna
	SET ANTENNA_ID=1                          
    ;');
    PREPARE Stmt FROM @SqlCmd;
    EXECUTE Stmt;
    DEALLOCATE PREPARE Stmt;  
 
	## Update nt_antenna ANTENNA_MODEL ##
  INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'SP_DTAG_SOURCE_NT_MAPPING','Update nt_antenna ANTENNA_MODEL', NOW());
 
  SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS ',GT_DB,'.`temp_antenna`;');
  PREPARE Stmt FROM @SqlCmd;
  EXECUTE Stmt;
  DEALLOCATE PREPARE Stmt; 
 
  SET @SqlCmd =CONCAT('CREATE TEMPORARY TABLE IF NOT EXISTS ',GT_DB,'.`temp_antenna` AS 
        (
          SELECT DISTINCT a.rnc_id, a.CELL_ID, a.ANTENNA_ID, a.ANTENNA_MODEL, 
            a.DOWNTILT_EL, c.FREQUENCY 
          FROM ',GT_DB,'.`nt_antenna` a
          INNER JOIN ',GT_DB,'.`nt_cell` c 
          WHERE a.rnc_id = c.rnc_id AND a.cell_id=c.cell_id
        );') ;
  PREPARE Stmt FROM @SqlCmd;
  EXECUTE Stmt;
  DEALLOCATE PREPARE Stmt;
 
  SET @SqlCmd =CONCAT('ALTER TABLE ',GT_DB,'.`temp_antenna` ADD INDEX (ANTENNA_MODEL,FREQUENCY);') ;
  PREPARE Stmt FROM @SqlCmd;
  EXECUTE Stmt;
  DEALLOCATE PREPARE Stmt;
 
  SET @SqlCmd =CONCAT('UPDATE ',GT_DB,'.`temp_antenna`  a 
      LEFT JOIN ',GT_DB,'.antenna_info b ON a.ANTENNA_MODEL = b.CATEGORIZE AND a.frequency = b.FREQUENCY 
      SET a.DOWNTILT_EL = b.ELECTRICAL_TILT 
      WHERE a.DOWNTILT_EL != b.ELECTRICAL_TILT;') ;
  PREPARE Stmt FROM @SqlCmd;
  EXECUTE Stmt;
  DEALLOCATE PREPARE Stmt;
 
  SET @SqlCmd =CONCAT('ALTER TABLE ',GT_DB,'.`temp_antenna` ADD INDEX (rnc_id, CELL_ID, ANTENNA_ID, FREQUENCY);') ;
  PREPARE Stmt FROM @SqlCmd;
  EXECUTE Stmt;
  DEALLOCATE PREPARE Stmt;
 
  /*SET @SqlCmd =CONCAT('INSERT INTO ',GT_DB,'.nt_log SELECT target.rnc_id,target.cell_id,''nt_antenna'',target.ANTENNA_MODEL,''2'',''NO antenna_info CAN MAP'' FROM ',GT_DB,'.nt_antenna target LEFT JOIN ',GT_DB,'.temp_antenna a ON target.rnc_id = a.rnc_id AND target.cell_id=a.cell_id
        LEFT JOIN ',GT_DB,'.antenna_info b ON a.ANTENNA_MODEL = b.CATEGORIZE AND a.frequency = b.FREQUENCY AND a.DOWNTILT_EL = b.ELECTRICAL_TILT
        WHERE b.`ANTENNA_MODEL` IS NULL OR b.`ANTENNA_MODEL`=''''') ;
  PREPARE Stmt FROM @SqlCmd;
  EXECUTE Stmt;
  DEALLOCATE PREPARE Stmt;*/
 
  SET @SqlCmd =CONCAT('UPDATE ',GT_DB,'.nt_antenna target
        LEFT JOIN ',GT_DB,'.temp_antenna a ON target.rnc_id = a.rnc_id AND target.cell_id=a.cell_id
        LEFT JOIN ',GT_DB,'.antenna_info b ON a.ANTENNA_MODEL = b.CATEGORIZE AND a.frequency = b.FREQUENCY AND a.DOWNTILT_EL = b.ELECTRICAL_TILT
        SET target.ANTENNA_MODEL=IF(b.`ANTENNA_MODEL` IS NULL OR b.`ANTENNA_MODEL`='''',''NoName'',b.`ANTENNA_MODEL`);') ;
  PREPARE Stmt FROM @SqlCmd;
  EXECUTE Stmt;
  DEALLOCATE PREPARE Stmt;
 
  ## Update nt_antenna_gsm ANTENNA_MODEL ##
  INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'SP_DTAG_SOURCE_NT_MAPPING','Update nt_antenna_gsm ANTENNA_MODEL', NOW());
 
  SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS ',GT_DB,'.`temp_antenna`;');
  PREPARE Stmt FROM @SqlCmd;
  EXECUTE Stmt;
  DEALLOCATE PREPARE Stmt; 
 
  SET @SqlCmd =CONCAT('CREATE TEMPORARY TABLE IF NOT EXISTS ',GT_DB,'.`temp_antenna` AS 
        (
          SELECT DISTINCT a.bsc_id, a.CELL_ID, a.ANTENNA_ID, a.ANTENNA_MODEL, 
            a.DOWNTILT_EL, c.BANDINDEX 
          FROM ',GT_DB,'.`nt_antenna_gsm` a
          INNER JOIN ',GT_DB,'.`nt_cell_gsm` c 
          WHERE a.bsc_id = c.bsc_id AND a.cell_id=c.cell_id
        );') ;
  PREPARE Stmt FROM @SqlCmd;
  EXECUTE Stmt;
  DEALLOCATE PREPARE Stmt;
 
  SET @SqlCmd =CONCAT('ALTER TABLE ',GT_DB,'.`temp_antenna` ADD INDEX (ANTENNA_MODEL,BANDINDEX);') ;
  PREPARE Stmt FROM @SqlCmd;
  EXECUTE Stmt;
  DEALLOCATE PREPARE Stmt;
 
  SET @SqlCmd =CONCAT('UPDATE ',GT_DB,'.`temp_antenna`  a 
      LEFT JOIN ',GT_DB,'.antenna_info b ON a.ANTENNA_MODEL = b.CATEGORIZE AND a.BANDINDEX = b.FREQUENCY 
      SET a.DOWNTILT_EL = b.ELECTRICAL_TILT 
      WHERE a.DOWNTILT_EL != b.ELECTRICAL_TILT;') ;
  PREPARE Stmt FROM @SqlCmd;
  EXECUTE Stmt;
  DEALLOCATE PREPARE Stmt;
 
  SET @SqlCmd =CONCAT('ALTER TABLE ',GT_DB,'.`temp_antenna` ADD INDEX (bsc_id, CELL_ID, ANTENNA_ID, BANDINDEX);') ;
  PREPARE Stmt FROM @SqlCmd;
  EXECUTE Stmt;
  DEALLOCATE PREPARE Stmt;
 
  /*SET @SqlCmd =CONCAT('INSERT INTO ',GT_DB,'.nt_log SELECT target.bsc_id,target.cell_id,''nt_antenna_gsm'',target.ANTENNA_MODEL,''2'',''NO antenna_info CAN MAP'' FROM ',GT_DB,'.nt_antenna_gsm target LEFT JOIN ',GT_DB,'.temp_antenna a ON target.bsc_id = a.bsc_id AND target.cell_id=a.cell_id
        LEFT JOIN ',GT_DB,'.antenna_info b ON a.ANTENNA_MODEL = b.CATEGORIZE AND a.BANDINDEX = b.FREQUENCY AND a.DOWNTILT_EL = b.ELECTRICAL_TILT
        WHERE b.`ANTENNA_MODEL` IS NULL OR b.`ANTENNA_MODEL`=''''') ;
  PREPARE Stmt FROM @SqlCmd;
  EXECUTE Stmt;
  DEALLOCATE PREPARE Stmt;*/
 
  SET @SqlCmd =CONCAT('UPDATE ',GT_DB,'.nt_antenna_gsm target
        LEFT JOIN ',GT_DB,'.temp_antenna a ON target.bsc_id = a.bsc_id AND target.cell_id=a.cell_id
        LEFT JOIN ',GT_DB,'.antenna_info b ON a.ANTENNA_MODEL = b.CATEGORIZE AND a.BANDINDEX = b.FREQUENCY AND a.DOWNTILT_EL = b.ELECTRICAL_TILT
        SET target.ANTENNA_MODEL=IF(b.`ANTENNA_MODEL` IS NULL OR b.`ANTENNA_MODEL`='''',''NoName'',b.`ANTENNA_MODEL`);') ;
  PREPARE Stmt FROM @SqlCmd;
  EXECUTE Stmt;
  DEALLOCATE PREPARE Stmt;
   
  
  SET @SqlCmd =CONCAT('DROP INDEX CATEGORIZE_IDX ON ',gt_db,'.ANTENNA_INFO') ;
  PREPARE Stmt FROM @SqlCmd;
  EXECUTE Stmt;
  DEALLOCATE PREPARE Stmt;
 
  INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'SP_DTAG_SOURCE_NT_MAPPING','ANTENNA_MODEL done', NOW());
-- update ANTENNA_MODEL done
 
-- update beamwidth_h Start
INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'SP_DTAG_SOURCE_NT_MAPPING','SP_DTAG_SOURCE_NT_SUB_MAPPING_NT_ANTENNA start', NOW());

CALL gt_gw_main.SP_DTAG_SOURCE_NT_SUB_MAPPING_NT_ANTENNA(GT_DB);
    
INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'SP_DTAG_SOURCE_NT_MAPPING','SP_DTAG_SOURCE_NT_SUB_MAPPING_NT_ANTENNA done', NOW());
-- update beamwidth done!

 -- start to dump NT
  INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'SP_DTAG_SOURCE_NT_MAPPING','Start to dump NT', NOW());

	SET @SqlCmd=CONCAT('SELECT GROUP_CONCAT(CONCAT("''",COLUMN_NAME,"''")),TABLE_NAME INTO @Header , @table_name
		FROM INFORMATION_SCHEMA.COLUMNS 
		WHERE TABLE_NAME = ''nt_antenna''
		AND TABLE_SCHEMA = ''',GT_DB,'''
		ORDER BY ORDINAL_POSITION;');
	SELECT @SqlCmd;
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt; 
	
	IF @table_name='nt_antenna' THEN
	    SET @SqlCmd=CONCAT('
	    SELECT ',@Header,'
	    UNION ALL
	    SELECT *
	    FROM ',GT_DB,'.`nt_antenna`
	    INTO OUTFILE ''/',NT_DATE,'/NT_ANTENNA.csv''
	    FIELDS TERMINATED BY '',''
	    LINES TERMINATED BY ''\n'';');
	    PREPARE Stmt FROM @SqlCmd;
	    EXECUTE Stmt;
	    DEALLOCATE PREPARE Stmt;
	ELSE
		SELECT 'table nt_antenna do not exists';
	END IF;
	
	SET @SqlCmd=CONCAT('SELECT GROUP_CONCAT(CONCAT("''",COLUMN_NAME,"''")),TABLE_NAME INTO @Header , @table_name
		FROM INFORMATION_SCHEMA.COLUMNS 
		WHERE TABLE_NAME = ''nt_cell''
		AND TABLE_SCHEMA = ''',GT_DB,'''
		ORDER BY ORDINAL_POSITION;');
	SELECT @SqlCmd;
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt; 
	
	IF @table_name='nt_cell' THEN
	    SET @SqlCmd=CONCAT('
		SELECT ',@Header,'
	      UNION ALL
	      SELECT *
	    FROM ',GT_DB,'.`nt_cell`
	    INTO OUTFILE ''/',NT_DATE,'/NT_CELL.csv''
	    FIELDS TERMINATED BY '',''
	    LINES TERMINATED BY ''\n'';');
	    PREPARE Stmt FROM @SqlCmd;
	    EXECUTE Stmt;
	    DEALLOCATE PREPARE Stmt;
	ELSE
		SELECT 'table nt_cell do not exists';
	END IF;
	
	SET @SqlCmd=CONCAT('SELECT GROUP_CONCAT(CONCAT("''",COLUMN_NAME,"''")),TABLE_NAME INTO @Header , @table_name
		FROM INFORMATION_SCHEMA.COLUMNS 
		WHERE TABLE_NAME = ''nt_rnc''
		AND TABLE_SCHEMA = ''',GT_DB,'''
		ORDER BY ORDINAL_POSITION;');
	SELECT @SqlCmd;
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
    
	IF @table_name='nt_rnc' THEN
	    SET @SqlCmd=CONCAT('
	      SELECT ',@Header,'
	      UNION ALL
	      SELECT *
	    FROM ',GT_DB,'.`nt_rnc`
	    INTO OUTFILE ''/',NT_DATE,'/NT_RNC.csv''
	    FIELDS TERMINATED BY '',''
	    LINES TERMINATED BY ''\n'';');
	    PREPARE Stmt FROM @SqlCmd;
	    EXECUTE Stmt;
	    DEALLOCATE PREPARE Stmt;
	ELSE
		SELECT 'table nt_rnc do not exists';
	END IF;
	
	SET @SqlCmd=CONCAT('SELECT GROUP_CONCAT(CONCAT("''",COLUMN_NAME,"''")),TABLE_NAME INTO @Header , @table_name
		FROM INFORMATION_SCHEMA.COLUMNS 
		WHERE TABLE_NAME = ''nt_neighbor''
		AND TABLE_SCHEMA = ''',GT_DB,'''
		ORDER BY ORDINAL_POSITION;');
	SELECT @SqlCmd;
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt; 
   
	IF @table_name='nt_neighbor' THEN   
	    SET @SqlCmd=CONCAT('
	      SELECT ',@Header,'
	      UNION ALL
	      SELECT *
	    FROM ',GT_DB,'.`nt_neighbor`
	    INTO OUTFILE ''/',NT_DATE,'/NT_NEIGHBOR.csv''
	    FIELDS TERMINATED BY '',''
	    LINES TERMINATED BY ''\n'';');
	    PREPARE Stmt FROM @SqlCmd;
	    EXECUTE Stmt;
	    DEALLOCATE PREPARE Stmt;
	ELSE
		SELECT 'table nt_neighbor do not exists';
	END IF;
  
    SET @SqlCmd=CONCAT('UPDATE ',GT_DB,'.nt_sc_code
    SET LONGITUDE=(SUBSTR(LPAD(LONGITUDE,6,''0''),1,2) + SUBSTR(LPAD(LONGITUDE,6,''0''),3,2)/60 ++ SUBSTR(LPAD(LONGITUDE,6,''0''),5,2)/3600)                            
    ;');
    PREPARE Stmt FROM @SqlCmd;
    EXECUTE Stmt;
    DEALLOCATE PREPARE Stmt;  
    
    SET @SqlCmd=CONCAT('UPDATE ',GT_DB,'.nt_sc_code
    SET LATITUDE=(SUBSTR(LPAD(LATITUDE,6,''0''),1,2) + SUBSTR(LPAD(LATITUDE,6,''0''),3,2)/60 ++ SUBSTR(LPAD(LATITUDE,6,''0''),5,2)/3600)                            
    ;');
    PREPARE Stmt FROM @SqlCmd;
    EXECUTE Stmt;
    DEALLOCATE PREPARE Stmt;
 
	SET @SqlCmd=CONCAT('SELECT GROUP_CONCAT(CONCAT("''",COLUMN_NAME,"''")),TABLE_NAME INTO @Header , @table_name
		FROM INFORMATION_SCHEMA.COLUMNS 
		WHERE TABLE_NAME = ''nt_sc_code''
		AND TABLE_SCHEMA = ''',GT_DB,'''
		ORDER BY ORDINAL_POSITION;');
	SELECT @SqlCmd;
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt; 
	
  	IF @table_name='nt_sc_code' THEN
	    SET @SqlCmd=CONCAT('
	      SELECT ',@Header,'
	      UNION ALL
	      SELECT *
	    FROM ',GT_DB,'.`nt_sc_code`
	    INTO OUTFILE ''/',NT_DATE,'/NT_SC_CODE.csv''
	    FIELDS TERMINATED BY '',''
	    LINES TERMINATED BY ''\n'';');
	    PREPARE Stmt FROM @SqlCmd;
	    EXECUTE Stmt;
	    DEALLOCATE PREPARE Stmt;
	ELSE
		SELECT 'table nt_sc_code do not exists';
	END IF;
	
	SET @SqlCmd=CONCAT('SELECT GROUP_CONCAT(CONCAT("''",COLUMN_NAME,"''")),TABLE_NAME INTO @Header , @table_name
		FROM INFORMATION_SCHEMA.COLUMNS 
		WHERE TABLE_NAME = ''nt_pu_current''
		AND TABLE_SCHEMA = ''',GT_DB,'''
		ORDER BY ORDINAL_POSITION;');
	SELECT @SqlCmd;
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt; 
   
	IF @table_name='nt_pu_current' THEN   
	    SET @SqlCmd=CONCAT('
	      SELECT ',@Header,'
	      UNION ALL
	      SELECT *
	    FROM ',GT_DB,'.`nt_pu_current`
	    INTO OUTFILE ''/',NT_DATE,'/NT_PU_CURRENT.csv''
	    FIELDS TERMINATED BY '',''
	    LINES TERMINATED BY ''\n'';');
	    PREPARE Stmt FROM @SqlCmd;
	    EXECUTE Stmt;
	    DEALLOCATE PREPARE Stmt;
	ELSE
		SELECT 'table nt_pu_current do not exists';
	END IF;
	
  
  
    SET @SqlCmd=CONCAT('
      SELECT
      ''ENODEB_ID'',
      ''ENODEB_USERLABEL''
       UNION ALL
       SELECT
      `ENODEB_ID`,
      `ENODEB_USERLABEL`
    FROM ',GT_DB,'.`nt_cell_lte_userlabel`
    INTO OUTFILE ''/',NT_DATE,'/NT_CELL_LTE_USERLABEL.csv''
    FIELDS TERMINATED BY '',''
    LINES TERMINATED BY ''\n'';');
    PREPARE Stmt FROM @SqlCmd;
    EXECUTE Stmt;
    DEALLOCATE PREPARE Stmt;
    
    
  END IF;
  
  
  INSERT INTO gt_gw_main.SP_LOG VALUES(GT_DB,'SP_DTAG_SOURCE_NT_MAPPING',CONCAT('Done: ',TIMESTAMPDIFF(SECOND,START_TIME,SYSDATE()),' seconds.'), NOW());
  
  
