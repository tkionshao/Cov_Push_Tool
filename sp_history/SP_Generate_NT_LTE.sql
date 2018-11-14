DELIMITER $$
USE `gt_gw_main`$$
DROP PROCEDURE IF EXISTS `SP_CreateDB_LTE`$$
CREATE DEFINER=`covmo`@`%` PROCEDURE `SP_Generate_NT_LTE`(IN GT_DB VARCHAR(100),IN GT_COVMO VARCHAR(100))
a_label:
BEGIN
  DECLARE START_TIME DATETIME DEFAULT SYSDATE();
  DECLARE v_cnt INT;
  DECLARE WORKER_ID VARCHAR(10) DEFAULT CONNECTION_ID();
  DECLARE GT_DB_STR INT;
  SET GT_DB_STR = gt_strtok(GT_DB,3,'_');
  SET @GT_DB_DATE = STR_TO_DATE(GT_DB_STR,'%Y%m%d');
  SET @SUB_GT_DB_DATE = DATE_SUB(@GT_DB_DATE,INTERVAL 1 DAY);
  SET @LAST_NT_DATE =  DATE_FORMAT(@SUB_GT_DB_DATE, '%Y%m%d');
  SET @LAST_NT_DB =CONCAT('gt_nt_',@LAST_NT_DATE);
  
    INSERT INTO gt_gw_main.SP_LOG VALUES(GT_DB,'SP_Generate_NT_LTE','nt_cell_current_lte', NOW());
    CALL gt_gw_main.SP_Sub_Generate_Sys_Config(GT_DB,'gt_covmo','lte');
    
  SET @SqlCmd=CONCAT('CREATE TABLE ',GT_DB,'.nt_cell_current_lte_dump LIKE ',GT_DB,'.nt_cell_current_lte');
  PREPARE Stmt FROM @SqlCmd;
  EXECUTE Stmt;
  DEALLOCATE PREPARE Stmt; 
  
  INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'SP_Generate_NT_LTE','nt_cell_current_lte - dump', NOW()); 
  
  SET @SqlCmd=CONCAT('UPDATE ',GT_DB,'.nt_cell_lte SET ACT_STATE=''COMMERCIAL'' WHERE  ACT_STATE = 1 ;');
  PREPARE Stmt FROM @SqlCmd;
  EXECUTE Stmt;
  DEALLOCATE PREPARE Stmt;  
  
  CALL gt_gw_main.SP_Generate_NT_Sub_Dump(GT_DB,'nt_cell_lte','nt_cell_current_lte_dump','PU_ID','LTE');
  CALL gt_gw_main.SP_Generate_NT_Sub_Dump(GT_DB,'nt_cell_lte','nt_cell_current_lte_dump','ACT_STATE','LTE');
  CALL gt_gw_main.SP_Generate_NT_Sub_Dump(GT_DB,'nt_cell_lte','nt_cell_current_lte_dump','ENODEB_ID','LTE');
  CALL gt_gw_main.SP_Generate_NT_Sub_Dump(GT_DB,'nt_cell_lte','nt_cell_current_lte_dump','CELL_ID','LTE');
  CALL gt_gw_main.SP_Generate_NT_Sub_Dump(GT_DB,'nt_cell_lte','nt_cell_current_lte_dump','EUTRABAND','LTE');
  CALL gt_gw_main.SP_Generate_NT_Sub_Dump(GT_DB,'nt_cell_lte','nt_cell_current_lte_dump','PCI','LTE');
  CALL gt_gw_main.SP_Generate_NT_Sub_Dump(GT_DB,'nt_cell_lte','nt_cell_current_lte_dump','BWCHANNEL','LTE');
  CALL gt_gw_main.SP_Generate_NT_Sub_Dump(GT_DB,'nt_cell_lte','nt_cell_current_lte_dump','DL_EARFCN','LTE');
  CALL gt_gw_main.SP_Generate_NT_Sub_Dump(GT_DB,'nt_cell_lte','nt_cell_current_lte_dump','CLUSTER_NAME_SUB_REGION','LTE');
  
  INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'SP_Generate_NT_LTE','nt_cell_current_lte - CHECK', NOW());
  SET @SqlCmd=CONCAT('ALTER IGNORE TABLE ',GT_DB,'.nt_cell_lte ADD UNIQUE(ENODEB_ID,CELL_ID);');
  PREPARE Stmt FROM @SqlCmd;
  EXECUTE Stmt;
  DEALLOCATE PREPARE Stmt;
  
  SET @SqlCmd=CONCAT('INSERT INTO ',GT_DB,'.nt_cell_current_lte 
          SELECT *
        FROM ',GT_DB,'.nt_cell_lte;');
  PREPARE Stmt FROM @SqlCmd;
  EXECUTE Stmt;
  DEALLOCATE PREPARE Stmt;  
  
  CALL gt_gw_main.SP_Generate_NT_Sub_Check(GT_DB,'nt_cell_current_lte','ENODEB_NAME','','ENODEB_ID','LTE');
    CALL gt_gw_main.SP_Generate_NT_Sub_Check(GT_DB,'nt_cell_current_lte','CELL_NAME','','CELL_ID','LTE');
    CALL gt_gw_main.SP_Generate_NT_Sub_Check(GT_DB,'nt_cell_current_lte','ENODEB_TYPE','1to6','1','LTE');
    CALL gt_gw_main.SP_Generate_NT_Sub_Check(GT_DB,'nt_cell_current_lte','INDOOR','0to2','0','LTE');
  
  SET @SqlCmd=CONCAT('UPDATE ',GT_DB,'.nt_cell_current_lte SET ACT_STATE=''COMMERCIAL'';');
  PREPARE Stmt FROM @SqlCmd;
  EXECUTE Stmt;
  DEALLOCATE PREPARE Stmt;
  
  
  INSERT INTO gt_gw_main.SP_LOG VALUES(GT_DB,'SP_Generate_NT_LTE','nt_antenna_current_lte', NOW());
  
  SET @SqlCmd=CONCAT('DELETE FROM ',GT_DB,'.nt_antenna_lte WHERE LONGITUDE = 0 AND LATITUDE =0;' );
  PREPARE Stmt FROM @SqlCmd;
  EXECUTE Stmt;
  DEALLOCATE PREPARE Stmt;    
  
  SET @SqlCmd=CONCAT('CREATE TABLE ',GT_DB,'.nt_antenna_current_lte_dump LIKE ',GT_DB,'.nt_antenna_current_lte;');
  PREPARE Stmt FROM @SqlCmd;
  EXECUTE Stmt;
  DEALLOCATE PREPARE Stmt; 
  
  INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'SP_Generate_NT_LTE','nt_antenna_current_lte - dump', NOW());
  
  CALL gt_gw_main.SP_Generate_NT_Sub_Dump(GT_DB,'nt_antenna_lte','nt_antenna_current_lte_dump','ENODEB_ID','LTE');
  CALL gt_gw_main.SP_Generate_NT_Sub_Dump(GT_DB,'nt_antenna_lte','nt_antenna_current_lte_dump','CELL_ID','LTE');
  CALL gt_gw_main.SP_Generate_NT_Sub_Dump(GT_DB,'nt_antenna_lte','nt_antenna_current_lte_dump','LONGITUDE','LTE');
  CALL gt_gw_main.SP_Generate_NT_Sub_Dump(GT_DB,'nt_antenna_lte','nt_antenna_current_lte_dump','LATITUDE','LTE');
  CALL gt_gw_main.SP_Generate_NT_Sub_Dump_Special(GT_DB,'nt_antenna_lte','nt_antenna_current_lte_dump','','LTE','mapping_with_cell');
  CALL gt_gw_main.SP_Generate_NT_Sub_Dump_Special(GT_DB,'nt_antenna_lte','nt_antenna_current_lte_dump','AZIMUTH','LTE','azimuth_with_outdoor');#17838
  
    
  INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'SP_Generate_NT_LTE','nt_antenna_current_lte - CHECK', NOW());
  
  SET @SqlCmd=CONCAT('INSERT INTO ',GT_DB,'.nt_antenna_current_lte 
        SELECT
          `ENODEB_ID`,
          `CELL_ID`,
          `LONGITUDE`,
          `LATITUDE`,
          `AZIMUTH`,
          `ANTENNA_TYPE`,
          `REPEATER`,
          `REPEATER_TYPE`,
          IF(`REPEATER_TA_DELAY` IS NULL AND `REPEATER`=1,320,(299792458*REPEATER_TA_DELAY*0.000001/4.88)) AS `REPEATER_TA_DELAY`, 
          IF(`ANTENNA_MODEL` IS NULL OR `ANTENNA_MODEL`='''',''NoName'',`ANTENNA_MODEL`) AS `ANTENNA_MODEL`,
          `ANTENNA_HEIGHT`,
          `ANTENNA_GAIN`,
          `BEAM_WIDTH_HORIZONTAL`,
          `BEAM_WIDTH_VERTICAL`,
          `DOWN_TILT_MECHANICAL`,
          `DOWN_TILT_ELECTRICAL`,
          `FEEDER_ATTEN`,
          `REFERENCE_SIGNAL_POWER`,
          `ELEVATION`,
          `INDOOR_TYPE`,
          `ANTENNA_ID`,
          `PATHLOSS_DISTANCE`,
          `ANTENNA_RADIUS`,
          `CLOSED_RADIUS`,
          0 AS `FLAG`
        FROM ',GT_DB,'.nt_antenna_lte;');
  PREPARE Stmt FROM @SqlCmd;
  EXECUTE Stmt;
  DEALLOCATE PREPARE Stmt;  
  
  SET @SqlCmd=CONCAT('ALTER IGNORE TABLE ',GT_DB,'.nt_antenna_current_lte ADD UNIQUE(ENODEB_ID,CELL_ID,ANTENNA_ID);');
  PREPARE Stmt FROM @SqlCmd;
  EXECUTE Stmt;
  DEALLOCATE PREPARE Stmt;
  
  /*SET @SqlCmd =CONCAT('CREATE INDEX CATEGORIZE_IDX ON ',gt_db,'.ANTENNA_INFO(CATEGORIZE,ELECTRICAL_TILT,FREQUENCY);') ;
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
           CASE WHEN EUTRABAND = 3 THEN 1800 WHEN EUTRABAND = 7 THEN 2600 WHEN EUTRABAND = 20 THEN 800 WHEN EUTRABAND = 8 THEN 900 WHEN EUTRABAND = 32 THEN 1500 END AS FREQUENCY
           FROM ',GT_DB,'.nt_cell_current_lte;');
  PREPARE Stmt FROM @SqlCmd;
  EXECUTE Stmt;
  DEALLOCATE PREPARE Stmt; 
  
  -- ------------------------- task#18833 ---------------------------------------
  SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS ',GT_DB,'.`temp_antenna`;');
  PREPARE Stmt FROM @SqlCmd;
  EXECUTE Stmt;
  DEALLOCATE PREPARE Stmt; 
  SET @SqlCmd=CONCAT('CREATE TEMPORARY TABLE IF NOT EXISTS ',GT_DB,'.`temp_antenna` AS 
        (
          SELECT DISTINCT a.enodeb_id, a.CELL_ID, a.ANTENNA_ID, a.ANTENNA_MODEL, 
            a.DOWN_TILT_ELECTRICAL, c.FREQUENCY 
          FROM ',GT_DB,'.`nt_antenna_current_lte` a
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
  
  #When DOWN_TILT_ELECTRICAL doesn't exist in antenna_info to use first record be default 
  SET @SqlCmd=CONCAT('UPDATE ',GT_DB,'.`temp_antenna`  a 
      LEFT JOIN ',GT_DB,'.antenna_info b ON a.ANTENNA_MODEL = b.CATEGORIZE AND a.frequency = b.FREQUENCY 
      SET a.DOWN_TILT_ELECTRICAL = b.ELECTRICAL_TILT 
      WHERE a.DOWN_TILT_ELECTRICAL != b.ELECTRICAL_TILT');
  PREPARE Stmt FROM @SqlCmd;
  EXECUTE Stmt;
  DEALLOCATE PREPARE Stmt; 
  
  SET @SqlCmd=CONCAT('ALTER TABLE ',GT_DB,'.`temp_antenna` ADD INDEX (enodeb_id, CELL_ID, ANTENNA_ID, FREQUENCY);');
  PREPARE Stmt FROM @SqlCmd;
  EXECUTE Stmt;
  DEALLOCATE PREPARE Stmt; 
  
  SET @SqlCmd=CONCAT('UPDATE ',GT_DB,'.nt_antenna_current_lte target
        LEFT JOIN ',GT_DB,'.temp_antenna a ON target.enodeb_id = a.enodeb_id AND target.cell_id=a.cell_id
        LEFT JOIN ',GT_DB,'.antenna_info b ON a.ANTENNA_MODEL = b.CATEGORIZE and a.frequency = b.FREQUENCY and a.DOWN_TILT_ELECTRICAL = b.ELECTRICAL_TILT
        SET target.ANTENNA_MODEL=IF(b.`ANTENNA_MODEL` IS NULL OR b.`ANTENNA_MODEL`='''',''NoName'',b.`ANTENNA_MODEL`);');
  PREPARE Stmt FROM @SqlCmd;
  EXECUTE Stmt;
  DEALLOCATE PREPARE Stmt;
  
  SET @SqlCmd =CONCAT('DROP INDEX CATEGORIZE_IDX ON ',gt_db,'.ANTENNA_INFO') ;
  PREPARE Stmt FROM @SqlCmd;
  EXECUTE Stmt;
  DEALLOCATE PREPARE Stmt;*/
  -- ----------------------------------------------------------------------------------------
  
  SET @SqlCmd=CONCAT('UPDATE ',GT_DB,'.nt_antenna_current_lte 
        SET INDOOR_TYPE = 1 WHERE ANTENNA_MODEL=''Halle'' AND AZIMUTH=0;');
  PREPARE Stmt FROM @SqlCmd;
  EXECUTE Stmt;
  DEALLOCATE PREPARE Stmt;
 
SET @SqlCmd=CONCAT('UPDATE ',GT_DB,'.nt_antenna_current_lte target
      INNER JOIN ',GT_DB,'.antenna_info a ON target.ANTENNA_MODEL=a.ANTENNA_MODEL
      SET target.ANTENNA_TYPE=a.TYPE;');
PREPARE Stmt FROM @SqlCmd;
EXECUTE Stmt;
DEALLOCATE PREPARE Stmt;
  CALL gt_gw_main.SP_Generate_NT_Sub_Check(GT_DB,'nt_antenna_current_lte','AZIMUTH','','0','LTE');#17838
  CALL gt_gw_main.SP_Generate_NT_Sub_Check(GT_DB,'nt_antenna_current_lte','BEAM_WIDTH_VERTICAL','1to360','7','LTE');
  CALL gt_gw_main.SP_Generate_NT_Sub_Check(GT_DB,'nt_antenna_current_lte','DOWN_TILT_MECHANICAL','-90to90','0','LTE');
  CALL gt_gw_main.SP_Generate_NT_Sub_Check(GT_DB,'nt_antenna_current_lte','DOWN_TILT_ELECTRICAL','-90to90','0','LTE');
  CALL gt_gw_main.SP_Generate_NT_Sub_Check(GT_DB,'nt_antenna_current_lte','FEEDER_ATTEN','0to20','3','LTE');
  CALL gt_gw_main.SP_Generate_NT_Sub_Check_Special(GT_DB,'nt_antenna_current_lte','ANTENNA_TYPE','1to3','LTE');
  CALL gt_gw_main.SP_Generate_NT_Sub_Check_Special(GT_DB,'nt_antenna_current_lte','ANTENNA_HEIGHT','3to300','LTE');
  CALL gt_gw_main.SP_Generate_NT_Sub_Check_Special(GT_DB,'nt_antenna_current_lte','ANTENNA_GAIN','0to30','LTE');
  CALL gt_gw_main.SP_Generate_NT_Sub_Check_Special(GT_DB,'nt_antenna_current_lte','BEAM_WIDTH_HORIZONTAL','1to360','LTE');
  CALL gt_gw_main.SP_Generate_NT_Sub_Check_Special(GT_DB,'nt_antenna_current_lte','REFERENCE_SIGNAL_POWER','-60to50','LTE');
  
  INSERT INTO gt_gw_main.SP_LOG VALUES(GT_DB,'SP_Generate_NT_LTE','dump cell IF NOT IN antenna', NOW());
  CALL gt_gw_main.SP_Generate_NT_Sub_Dump_Special(GT_DB,'nt_cell_current_lte','nt_cell_current_lte_dump','','LTE','mapping_with_antenna');
  
  SET @SqlCmd=CONCAT('UPDATE ',GT_DB,'.nt_cell_current_lte
        SET NBR_DISTANCE_4G_CM = CASE WHEN NBR_DISTANCE_4G_CM < 100 OR NBR_DISTANCE_4G_CM IS NULL THEN 100 ELSE NBR_DISTANCE_4G_CM END,
        NBR_DISTANCE_4G_VORONOI = CASE WHEN NBR_DISTANCE_4G_VORONOI < 100 THEN 100 WHEN NBR_DISTANCE_4G_VORONOI IS NULL THEN 3000 ELSE NBR_DISTANCE_4G_VORONOI END;');
  PREPARE Stmt FROM @SqlCmd;
  EXECUTE Stmt;
  DEALLOCATE PREPARE Stmt; 
  
  INSERT INTO gt_gw_main.SP_LOG VALUES(GT_DB,'SP_Generate_NT_LTE','nt_tac_cell_current_lte', NOW());
  SET @SqlCmd=CONCAT(' INSERT INTO ',GT_DB,'.nt_tac_cell_current_lte(ENODEB_ID,CELL_ID,TAC)
        SELECT DISTINCT ENODEB_ID,CELL_ID,TAC FROM ',GT_DB,'.nt_tac_cell_lte; ');
  PREPARE Stmt FROM @SqlCmd;
  EXECUTE Stmt;
  DEALLOCATE PREPARE Stmt; 
  
  INSERT INTO gt_gw_main.SP_LOG VALUES(GT_DB,'SP_Generate_NT_LTE','nt_mme_current_lte', NOW());
  SET @SqlCmd=CONCAT('INSERT INTO ',GT_DB,'.nt_mme_current_lte
        (MME_ID,MME_NAME,MME_NE_ID,S_GW_ID,S_GW_NAME,USER_LABEL,MCC,MNC,VENDOR)
        SELECT MME_ID,MME_NAME,MME_NE_ID,S_GW_ID,S_GW_NAME,USER_LABEL,MCC,MNC,VENDOR 
        FROM ',GT_DB,'.nt_mme_lte; ');
  PREPARE Stmt FROM @SqlCmd;
  EXECUTE Stmt;
  DEALLOCATE PREPARE Stmt;
  
  INSERT INTO gt_gw_main.SP_LOG VALUES(GT_DB,'SP_Generate_NT_LTE','nt_nbr_4_2_current_lte', NOW());
  SET @SqlCmd=CONCAT('INSERT INTO ',GT_DB,'.nt_nbr_4_2_current_lte
        (ENODEB_ID,CELL_ID,NBR_BSC_ID,NBR_CELL_ID,PRIORITY,ARFCN,BCC,NCC,LAC)
        SELECT DISTINCT 
          ENODEB_ID,CELL_ID,NBR_BSC_ID,NBR_CELL_ID,PRIORITY,ARFCN,BCC,NCC,LAC 
        FROM ',GT_DB,'.nt_nbr_4_2_lte; ');
  PREPARE Stmt FROM @SqlCmd;
  EXECUTE Stmt;
  DEALLOCATE PREPARE Stmt; 
  
  INSERT INTO gt_gw_main.SP_LOG VALUES(GT_DB,'SP_Generate_NT_LTE','nt_nbr_4_3_current_lte', NOW());
  SET @SqlCmd=CONCAT('INSERT INTO ',GT_DB,'.nt_nbr_4_3_current_lte
        (ENODEB_ID,CELL_ID,NBR_RNC_ID,NBR_CELL_ID,PRIORITY,PSC)
        SELECT DISTINCT 
          ENODEB_ID,CELL_ID,NBR_RNC_ID,NBR_CELL_ID,PRIORITY,PSC 
        FROM ',GT_DB,'.nt_nbr_4_3_lte; ');
  PREPARE Stmt FROM @SqlCmd;
  EXECUTE Stmt;
  DEALLOCATE PREPARE Stmt; 
  
  INSERT INTO gt_gw_main.SP_LOG VALUES(GT_DB,'SP_Generate_NT_LTE','nt_nbr_4_4_current_lte', NOW());
  SET @SqlCmd=CONCAT('INSERT INTO ',GT_DB,'.nt_nbr_4_4_current_lte
        (ENODEB_ID,CELL_ID,NBR_ENODEB_ID,NBR_CELL_ID,PRIORITY,NBR_TYPE)
        SELECT  DISTINCT 
          ENODEB_ID,CELL_ID,NBR_ENODEB_ID,NBR_CELL_ID,PRIORITY, CASE TRIM(NBR_TYPE) WHEN ''INTER'' THEN 1 WHEN ''INTRA'' THEN 2 ELSE NBR_TYPE END NBR_TYPE 
        FROM ',GT_DB,'.nt_nbr_4_4_lte; ');
  PREPARE Stmt FROM @SqlCmd;
  EXECUTE Stmt;
  DEALLOCATE PREPARE Stmt;  
  
  INSERT INTO gt_gw_main.SP_LOG VALUES(GT_DB,'SP_Generate_NT_LTE','NT_CELL_CURRENT_LTE.NBR_DISTANCE_4G_CM', NOW());
  
  SET @SqlCmd=CONCAT('CREATE TABLE ',GT_DB,'.tmp_nt_current_nbr_distance_LTE_fixDataType(
    `enodeb_id` MEDIUMINT(9) DEFAULT NULL,
    `cell_id` TINYINT(4) DEFAULT NULL,
    `CELL_LON` DECIMAL (9, 6) DEFAULT NULL,
    `CELL_LAT` DECIMAL (9, 6) DEFAULT NULL,
    `NBR_LON` DECIMAL (9, 6) DEFAULT NULL,
    `NBR_LAT` DECIMAL (9, 6) DEFAULT NULL,
    KEY `enodeb_id` (`enodeb_id`),
    KEY `cell_id` (`cell_id`)
    ) ENGINE=MYISAM DEFAULT CHARSET=utf8;');
  PREPARE Stmt FROM @SqlCmd;
  EXECUTE Stmt;
  DEALLOCATE PREPARE Stmt;
    
    SET @SqlCmd=CONCAT('INSERT INTO ',GT_DB,'.tmp_nt_current_nbr_distance_LTE_fixDataType
        SELECT DISTINCT a.enodeb_id,a.cell_id,B.LONGITUDE CELL_LON,B.LATITUDE CELL_LAT ,C.LONGITUDE NBR_LON,C.LATITUDE NBR_LAT
        FROM ',GT_DB,'.nt_nbr_4_4_current_lte A
                INNER JOIN ',GT_DB,'.NT_ANTENNA_CURRENT_LTE  B
                ON a.enodeb_id=b.enodeb_id AND a.cell_id=b.cell_id 
                INNER JOIN ',GT_DB,'.NT_ANTENNA_CURRENT_LTE  C 
                ON A.nbr_enodeb_id=C.enodeb_id AND A.nbr_cell_id=C.cell_id
                WHERE EXISTS
                    (SELECT 1 FROM ',GT_DB,'.NT_CELL_CURRENT_LTE D
                        WHERE A.nbr_enodeb_id=D.enodeb_id
                        AND A.nbr_cell_id=D.cell_id
                        AND D.INDOOR=0
                    ); 
    ');
    PREPARE Stmt FROM @SqlCmd;
    EXECUTE Stmt;
    DEALLOCATE PREPARE Stmt;
    
    SET @SqlCmd=CONCAT('CREATE TABLE ',GT_DB,'.tmp_nt_current_nbr_distance_LTE
                SELECT DISTINCT B.enodeb_id,B.cell_id,GT_COVMO_DISTANCE(B.CELL_LON,B.CELL_LAT ,B.NBR_LON,B.NBR_LAT) AS NBR_DISTANCE_4G_CM
                FROM ',GT_DB,'.tmp_nt_current_nbr_distance_LTE_fixDataType  B
                ;');
    PREPARE Stmt FROM @SqlCmd;
    EXECUTE Stmt;
    DEALLOCATE PREPARE Stmt;
    
    SET @SqlCmd=CONCAT('CREATE TABLE ',GT_DB,'.tmp_update_data_LTE
                SELECT enodeb_id,cell_id,AVG(NBR_DISTANCE_4G_CM) AS NBR_AVG_DISTANCE_4G_CM
                FROM ',GT_DB,'.tmp_nt_current_nbr_distance_LTE
                WHERE NBR_DISTANCE_4G_CM >3
                GROUP BY enodeb_id,cell_id
                HAVING COUNT(*) > 1
    ;');
    PREPARE Stmt FROM @SqlCmd;
    EXECUTE Stmt;
    DEALLOCATE PREPARE Stmt;
    SET @SqlCmd=CONCAT('CREATE INDEX idx_tmp_update_data_LTE ON ',GT_DB,'.tmp_update_data_LTE (enodeb_id,cell_id);');
    PREPARE Stmt FROM @SqlCmd;
    EXECUTE Stmt;
    DEALLOCATE PREPARE Stmt;
    
    SET @SqlCmd=CONCAT(' UPDATE ',GT_DB,'.NT_CELL_CURRENT_LTE A
            JOIN 
            (
                SELECT ENODEB_ID,CELL_ID ,NBR_AVG_DISTANCE_4G_CM
                FROM (
                    SELECT a.enodeb_id,a.cell_id,b.NBR_AVG_DISTANCE_4G_CM
                     FROM  ',GT_DB,'.tmp_nt_current_nbr_distance_LTE a
                    JOIN  ',GT_DB,'.tmp_update_data_LTE  b
                    ON a.enodeb_id=b.enodeb_id AND a.cell_id=b.cell_id 
                    WHERE a.NBR_DISTANCE_4G_CM > 3
                ) AA
                GROUP BY ENODEB_ID,CELL_ID 
            ) B
            ON A.ENODEB_ID=B.ENODEB_ID
            AND A.CELL_ID=B.CELL_ID
            SET A.NBR_DISTANCE_4G_CM=B.NBR_AVG_DISTANCE_4G_CM ');
    PREPARE Stmt FROM @SqlCmd;
    EXECUTE Stmt;
    DEALLOCATE PREPARE Stmt; 
    
    SET @SqlCmd=CONCAT('DROP TABLE IF EXISTS ',GT_DB,'.tmp_nt_current_nbr_distance_LTE;');
    PREPARE Stmt FROM @SqlCmd;
    EXECUTE Stmt;
    DEALLOCATE PREPARE Stmt;
    
    SET @SqlCmd=CONCAT('DROP TABLE IF EXISTS ',GT_DB,'.tmp_update_data_LTE;');
    PREPARE Stmt FROM @SqlCmd;
    EXECUTE Stmt;
    DEALLOCATE PREPARE Stmt;
   /*SET @SqlCmd=CONCAT(' UPDATE ',GT_DB,'.NT_CELL_CURRENT_LTE A
      JOIN 
      (
      SELECT ENODEB_ID,CELL_ID ,AVG(GT_COVMO_DISTANCE(CELL_LON,CELL_LAT,NBR_LON,NBR_LAT)) AS NBR_DISTANCE_4G_CM 
      FROM (
        SELECT DISTINCT a.enodeb_id,a.cell_id,B.LONGITUDE CELL_LON,B.LATITUDE CELL_LAT ,C.LONGITUDE NBR_LON,C.LATITUDE NBR_LAT
        FROM  ',GT_DB,'.nt_nbr_4_4_current_lte A
        INNER JOIN  ',GT_DB,'.NT_ANTENNA_CURRENT_LTE  b
        ON a.enodeb_id=b.enodeb_id AND a.cell_id=b.cell_id 
        INNER JOIN ',GT_DB,'.NT_ANTENNA_CURRENT_LTE  c ON a.nbr_enodeb_id=C.enodeb_id
        AND a.nbr_cell_id=C.cell_id 
        WHERE EXISTS
          (SELECT 1 FROM ',GT_DB,'.NT_CELL_CURRENT_LTE  D
            WHERE a.nbr_enodeb_id=D.enodeb_id
            AND a.nbr_cell_id=D.cell_id
            AND D.INDOOR=0
          )
        ) AA
      GROUP BY ENODEB_ID,CELL_ID 
      ) B
      ON A.ENODEB_ID=B.ENODEB_ID
      AND A.CELL_ID=B.CELL_ID
      SET A.NBR_DISTANCE_4G_CM=B.NBR_DISTANCE_4G_CM;');
  PREPARE Stmt FROM @SqlCmd;
  EXECUTE Stmt;
  DEALLOCATE PREPARE Stmt;*/
  
  INSERT INTO gt_gw_main.SP_LOG VALUES(GT_DB,'SP_Generate_NT_LTE','Update NT_CELL_CURRENT_LTE.NBR_DISTANCE_4G_VORONOI', NOW());
  
  SET @SqlCmd=CONCAT('DROP TABLE IF EXISTS ',GT_DB,'.tmp_avg_voronoi_distance;');
  PREPARE Stmt FROM @SqlCmd;
  EXECUTE Stmt;
  DEALLOCATE PREPARE Stmt;
  
  SET @SqlCmd=CONCAT('CREATE TABLE ',GT_DB,'.tmp_avg_voronoi_distance ENGINE=MYISAM AS
        SELECT ENODEB_ID,AVG(REFINE_DISTANCE) AS DISTANCE_AVG 
        FROM ',GT_DB,'.`nt_neighbor_voronoi_lte` 
        GROUP BY ENODEB_ID;');
  PREPARE Stmt FROM @SqlCmd;
  EXECUTE Stmt;
  DEALLOCATE PREPARE Stmt;
  
  SET @SqlCmd=CONCAT('ALTER TABLE ',GT_DB,'.tmp_avg_voronoi_distance ADD INDEX `ix_enodeb_id`(ENODEB_ID);');
  PREPARE Stmt FROM @SqlCmd;
  EXECUTE Stmt;
  DEALLOCATE PREPARE Stmt;
  
  SET @SqlCmd=CONCAT('UPDATE ',GT_DB,'.NT_CELL_CURRENT_LTE A, ',GT_DB,'.tmp_avg_voronoi_distance B
        SET A.`NBR_DISTANCE_4G_VORONOI` = B.DISTANCE_AVG
        WHERE A.ENODEB_ID = B.ENODEB_ID;');
  PREPARE Stmt FROM @SqlCmd;
  EXECUTE Stmt;
  DEALLOCATE PREPARE Stmt;  
  
  INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'SP_Generate_NT_LTE','DISTANCE', NOW());
  SET @SqlCmd=CONCAT('Drop TABLE IF EXISTS ',GT_DB,'.tmp_nt_cell_u;');
  PREPARE Stmt FROM @SqlCmd;
  EXECUTE Stmt;
  DEALLOCATE PREPARE Stmt;  
  
  SET @SqlCmd=CONCAT('CREATE TABLE ',GT_DB,'.tmp_nt_cell_u ENGINE=MYISAM
                                SELECT A.ENODEB_ID,A.CELL_ID,B.LONGITUDE,B.LATITUDE,B.AZIMUTH
                                FROM ',GT_DB,'.nt_cell_current_lte A
                                JOIN ',GT_DB,'.nt_antenna_current_lte B
                                ON A.CELL_ID=B.CELL_ID AND A.ENODEB_ID=B.ENODEB_ID;');
        PREPARE Stmt FROM @SqlCmd;
        EXECUTE Stmt;
        DEALLOCATE PREPARE Stmt;
  
  SET @SqlCmd=CONCAT('CREATE INDEX idx_tmp_nt_cell_u ON ',GT_DB,'.tmp_nt_cell_u (ENODEB_ID,CELL_ID);');
  PREPARE Stmt FROM @SqlCmd;
  EXECUTE Stmt;
  DEALLOCATE PREPARE Stmt; 
  
  SET @SqlCmd=CONCAT('UPDATE ',GT_DB,'.`nt_nbr_4_4_current_lte` A
                JOIN ',GT_DB,'.tmp_nt_cell_u B ON A.CELL_ID=B.CELL_ID AND A.ENODEB_ID=B.ENODEB_ID 
                JOIN ',GT_DB,'.tmp_nt_cell_u C ON A.NBR_CELL_ID=C.CELL_ID AND A.NBR_ENODEB_ID=C.ENODEB_ID
 
                SET NBR_ANGLE=gt_covmo_angle(B.LONGITUDE,B.LATITUDE,C.LONGITUDE,C.LATITUDE)
                ,NBR_AZIMUTH_ANGLE=gt_covmo_azimuth_angle(B.LONGITUDE,B.LATITUDE,B.AZIMUTH,C.LONGITUDE,C.LATITUDE,C.AZIMUTH)
                ,NBR_DISTANCE=CASE
                WHEN B.LONGITUDE=C.LONGITUDE AND B.LATITUDE=C.LATITUDE THEN 0
                ELSE gt_covmo_distance(B.LONGITUDE,B.LATITUDE,C.LONGITUDE,C.LATITUDE)
                END
                WHERE NBR_TYPE<3;');
  PREPARE Stmt FROM @SqlCmd;
  EXECUTE Stmt;
  DEALLOCATE PREPARE Stmt; 
    
  CALL gt_gw_main.SP_Generate_NT_Sub_Density(GT_DB,'gt_covmo','lte');
  CALL gt_gw_main.SP_Generate_NT_Sub_Pathloss(GT_DB,'gt_covmo','lte');
  CALL SP_Generate_NT_Sub_Check_Threshold(GT_DB,'GT_COVMO');
  INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'SP_Generate_NT_LTE','Update NBR_TYPE by DL_EARFCN', NOW());
    
  SET @SqlCmd=CONCAT('UPDATE ',GT_DB,'.nt_nbr_4_4_current_lte a,  (
            SELECT a.*, b.DL_EARFCN AS S_EAR, c.DL_EARFCN AS T_EAR FROM (
                SELECT ENODEB_ID,CELL_ID,NBR_ENODEB_ID,NBR_CELL_ID,NBR_TYPE FROM ',GT_DB,'.nt_nbr_4_4_current_lte WHERE NBR_TYPE=0
            ) a, ',GT_DB,'.nt_cell_current_lte b, ',GT_DB,'.nt_cell_current_lte c
 
 
            WHERE a.ENODEB_ID=b.ENODEB_ID AND a.CELL_ID=b.CELL_ID
            AND a.NBR_ENODEB_ID=c.ENODEB_ID AND a.NBR_CELL_ID=c.CELL_ID
            )t
        SET a.NBR_TYPE = (CASE WHEN t.S_EAR=t.T_EAR THEN 2 ELSE 1 END)
        WHERE a.ENODEB_ID=t.ENODEB_ID AND a.CELL_ID=t.CELL_ID AND a.NBR_ENODEB_ID=t.NBR_ENODEB_ID AND a.NBR_CELL_ID=t.NBR_CELL_ID;');
  PREPARE Stmt FROM @SqlCmd;
  EXECUTE Stmt;
  DEALLOCATE PREPARE Stmt; 
  
  SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS ',GT_DB,'.`tmp_nt_cell_lte_',WORKER_ID,'`;');
  PREPARE Stmt FROM @SqlCmd;
  EXECUTE Stmt;
  DEALLOCATE PREPARE Stmt; 
  
--   SET @SqlCmd=CONCAT('Drop TABLE IF EXISTS ',GT_DB,'.tmp_nt_current_nbr_distance_LTE_fixDataType;');
--   PREPARE Stmt FROM @SqlCmd;
--   EXECUTE Stmt;
--   DEALLOCATE PREPARE Stmt;
--   
--   SET @SqlCmd=CONCAT(' DROP TABLE IF EXISTS ',GT_DB,'.nt_antenna_lte;');
--   PREPARE Stmt FROM @SqlCmd;
--   EXECUTE Stmt;
--   DEALLOCATE PREPARE Stmt;
--   
--   SET @SqlCmd=CONCAT(' DROP TABLE IF EXISTS ',GT_DB,'.nt_cell_lte;');
--   PREPARE Stmt FROM @SqlCmd;
--   EXECUTE Stmt;
--   DEALLOCATE PREPARE Stmt;
--   
--   SET @SqlCmd=CONCAT('DROP TABLE IF EXISTS ',GT_DB,'.tmp_nt_cell_u;');
--   PREPARE Stmt FROM @SqlCmd;
--   EXECUTE Stmt;
--   DEALLOCATE PREPARE Stmt;
--   
--   SET @SqlCmd=CONCAT(' DROP TABLE IF EXISTS ',GT_DB,'.nt_mme_lte;');
--   PREPARE Stmt FROM @SqlCmd;
--   EXECUTE Stmt;
--   DEALLOCATE PREPARE Stmt;
--   
--   SET @SqlCmd=CONCAT(' DROP TABLE IF EXISTS ',GT_DB,'.nt_nbr_4_2_lte;');
--   PREPARE Stmt FROM @SqlCmd;
--   EXECUTE Stmt;
--   DEALLOCATE PREPARE Stmt;
--   
--   SET @SqlCmd=CONCAT(' DROP TABLE IF EXISTS ',GT_DB,'.nt_nbr_4_3_lte;');
--   PREPARE Stmt FROM @SqlCmd;
--   EXECUTE Stmt;
--   DEALLOCATE PREPARE Stmt;
--   
--   SET @SqlCmd=CONCAT(' DROP TABLE IF EXISTS ',GT_DB,'.nt_nbr_4_4_lte;');
--   PREPARE Stmt FROM @SqlCmd;
--   EXECUTE Stmt;
--   DEALLOCATE PREPARE Stmt;
--   
--   SET @SqlCmd=CONCAT(' DROP TABLE IF EXISTS ',GT_DB,'.nt_tac_cell_lte;');
--   PREPARE Stmt FROM @SqlCmd;
--   EXECUTE Stmt;
--   DEALLOCATE PREPARE Stmt;
--   
--   SET @SqlCmd=CONCAT('DROP TABLE IF EXISTS ',GT_DB,'.tmp_avg_voronoi_distance;');
--   PREPARE Stmt FROM @SqlCmd;
--   EXECUTE Stmt;
--   DEALLOCATE PREPARE Stmt;
--   
--   SET @SqlCmd=CONCAT('DROP TABLE IF EXISTS ',GT_DB,'.tmp_dtag_nt_antenna_frg_mapping_lte;');
--   PREPARE Stmt FROM @SqlCmd;
--   EXECUTE Stmt;
--   DEALLOCATE PREPARE Stmt;
--   SET @SqlCmd=CONCAT('DROP TABLE IF EXISTS ',GT_DB,'.tmp_nt_antenna_default;');
--   PREPARE Stmt FROM @SqlCmd;
--   EXECUTE Stmt;
--   DEALLOCATE PREPARE Stmt;
--   
--   SET @SqlCmd=CONCAT('drop table if exists ',GT_DB,'.tmp_nt_nbr_cell_blacklist_pci ;');
--   PREPARE Stmt FROM @SqlCmd;
--   EXECUTE Stmt;
--   DEALLOCATE PREPARE Stmt; 
  INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'SP_Generate_NT_LTE','DIST rule', NOW());
  
  SET @SqlCmd=CONCAT('
  UPDATE ',GT_DB,'.nt_cell_current_lte
  SET 
    NBR_DISTANCE_4G_CM = CASE WHEN NBR_DISTANCE_4G_CM < 100 OR NBR_DISTANCE_4G_CM IS NULL THEN 100 ELSE NBR_DISTANCE_4G_CM END,
    NBR_DISTANCE_4G_VORONOI = CASE WHEN NBR_DISTANCE_4G_VORONOI < 100 THEN 100 WHEN NBR_DISTANCE_4G_VORONOI IS NULL THEN 3000 ELSE NBR_DISTANCE_4G_VORONOI END
  ; 
  ');
  PREPARE Stmt FROM @SqlCmd;
  EXECUTE Stmt;
  DEALLOCATE PREPARE Stmt; 
  
  INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'SP_Generate_NT_LTE','ANTENNA_RADIUS rule', NOW());
  SET @SqlCmd=CONCAT('UPDATE ',GT_DB,'.nt_antenna_current_lte A, ',GT_DB,'.nt_cell_current_lte B
        SET 
          ANTENNA_RADIUS = CASE WHEN indoor_type = 0 AND (NBR_DISTANCE_4G_VORONOI*1.5-SITE_DENSITY_TYPE/10) > PATHLOSS_DISTANCE 
            THEN (NBR_DISTANCE_4G_VORONOI*1.5-SITE_DENSITY_TYPE/10)
          WHEN indoor_type = 0 AND (NBR_DISTANCE_4G_VORONOI*1.5-SITE_DENSITY_TYPE/10) < PATHLOSS_DISTANCE 
            THEN PATHLOSS_DISTANCE
        ELSE 50 END 
        WHERE A.enodeb_id = B.enodeb_id and A.cell_id = B.cell_id;');
  PREPARE Stmt FROM @SqlCmd;
  EXECUTE Stmt;
  DEALLOCATE PREPARE Stmt; 
  
  INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'SP_Generate_NT_LTE','CLOSED_RADIUS rule', NOW());
  SET @SqlCmd=CONCAT('UPDATE ',GT_DB,'.nt_antenna_current_lte A, ',GT_DB,'.nt_cell_current_lte B
        SET 
          CLOSED_RADIUS = CASE WHEN indoor_type = 0 THEN ( 1+ FLOOR(ANTENNA_HEIGHT/ 50 )) * NBR_DISTANCE_4G_VORONOI / 5 
            ELSE 50 END
        WHERE A.enodeb_id = B.enodeb_id and A.cell_id = B.cell_id;');
  PREPARE Stmt FROM @SqlCmd;
  EXECUTE Stmt;
  DEALLOCATE PREPARE Stmt; 
 
  INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'SP_Generate_NT_LTE','#27589 NB IOT cell will not be dumped', NOW());
  SET @SqlCmd=CONCAT('INSERT INTO ',GT_DB,'.nt_cell_current_lte 
	SELECT * FROM ',GT_DB,'.nt_cell_current_lte_dump WHERE 
(enodeb_id=101497 AND cell_id=14) OR 
(enodeb_id=101534 AND cell_id=9) OR
(enodeb_id=101534 AND cell_id=10) OR
(enodeb_id=101534 AND cell_id=11) OR
(enodeb_id=101564 AND cell_id=14) OR
(enodeb_id=102749 AND cell_id=12) OR
(enodeb_id=102901 AND cell_id=13) OR
(enodeb_id=105427 AND cell_id=8) OR
(enodeb_id=106327 AND cell_id=10) OR
(enodeb_id=106328 AND cell_id=8) OR
(enodeb_id=106328 AND cell_id=9) OR
(enodeb_id=106444 AND cell_id=10) OR
(enodeb_id=106943 AND cell_id=9) OR
(enodeb_id=107015 AND cell_id=10) OR
(enodeb_id=108423 AND cell_id=10) OR
(enodeb_id=108423 AND cell_id=12) OR
(enodeb_id=108444 AND cell_id=11) OR
(enodeb_id=108444 AND cell_id=12) OR
(enodeb_id=116338 AND cell_id=9) OR
(enodeb_id=116915 AND cell_id=9) OR
(enodeb_id=120099 AND cell_id=7) OR
(enodeb_id=122793 AND cell_id=10) OR
(enodeb_id=141218 AND cell_id=6) OR
(enodeb_id=102749 AND cell_id=13) OR
(enodeb_id=102886 AND cell_id=10)	
	;');
  PREPARE Stmt FROM @SqlCmd;
  EXECUTE Stmt;
  DEALLOCATE PREPARE Stmt; 
 
  SET @SqlCmd=CONCAT('INSERT INTO ',GT_DB,'.nt_antenna_current_lte 
SELECT * FROM ',GT_DB,'.nt_antenna_current_lte_dump WHERE 
(enodeb_id=101497 AND cell_id=14) OR 
(enodeb_id=101534 AND cell_id=9) OR
(enodeb_id=101534 AND cell_id=10) OR
(enodeb_id=101534 AND cell_id=11) OR
(enodeb_id=101564 AND cell_id=14) OR
(enodeb_id=102749 AND cell_id=12) OR
(enodeb_id=102901 AND cell_id=13) OR
(enodeb_id=105427 AND cell_id=8) OR
(enodeb_id=106327 AND cell_id=10) OR
(enodeb_id=106328 AND cell_id=8) OR
(enodeb_id=106328 AND cell_id=9) OR
(enodeb_id=106444 AND cell_id=10) OR
(enodeb_id=106943 AND cell_id=9) OR
(enodeb_id=107015 AND cell_id=10) OR
(enodeb_id=108423 AND cell_id=10) OR
(enodeb_id=108423 AND cell_id=12) OR
(enodeb_id=108444 AND cell_id=11) OR
(enodeb_id=108444 AND cell_id=12) OR
(enodeb_id=116338 AND cell_id=9) OR
(enodeb_id=116915 AND cell_id=9) OR
(enodeb_id=120099 AND cell_id=7) OR
(enodeb_id=122793 AND cell_id=10) OR
(enodeb_id=141218 AND cell_id=6) OR
(enodeb_id=102749 AND cell_id=13) OR
(enodeb_id=102886 AND cell_id=10);');
  PREPARE Stmt FROM @SqlCmd;
  EXECUTE Stmt;
  DEALLOCATE PREPARE Stmt; 
  
  INSERT INTO gt_gw_main.SP_LOG VALUES(GT_DB,'SP_Generate_NT_LTE',CONCAT('Done: ',TIMESTAMPDIFF(SECOND,START_TIME,SYSDATE()),' seconds.'), NOW());
  
END$$
DELIMITER ;
