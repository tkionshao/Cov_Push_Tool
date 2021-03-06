DELIMITER $$
USE `gt_gw_main`$$
DROP PROCEDURE IF EXISTS `SP_CreateDB_LTE`$$
CREATE DEFINER=`covmo`@`%` PROCEDURE `SP_Generate_NT_Sub_Pathloss`(IN GT_DB VARCHAR(100),IN GT_COVMO VARCHAR(100),IN TECH VARCHAR(10))
a_label:
BEGIN
	DECLARE START_TIME DATETIME DEFAULT SYSDATE();
	DECLARE WORKER_ID VARCHAR(10) DEFAULT CONNECTION_ID();
	
  	INSERT INTO gt_gw_main.SP_LOG VALUES(GT_DB,'SP_Generate_NT_Sub_Pathloss','start', NOW());
 
	IF TECH = 'lte' THEN
		SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE  IF EXISTS ',GT_DB,'.tmp_lte_freq_',WORKER_ID,';');
		PREPARE stmt FROM @sqlcmd;
		EXECUTE stmt;
		DEALLOCATE PREPARE stmt;
	
		SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE  IF EXISTS ',GT_DB,'.tmp_lte_pathloss_',WORKER_ID,';');
		PREPARE stmt FROM @sqlcmd;
		EXECUTE stmt;
		DEALLOCATE PREPARE stmt;
		SET @SqlCmd=CONCAT('CREATE TEMPORARY TABLE ',GT_DB,'.tmp_lte_freq_',WORKER_ID,' (
				  `EUTRABAND` smallint(6) DEFAULT NULL,
				  `FREQ` float DEFAULT NULL,
				  `ADJ_FREQ` smallint(6) DEFAULT NULL
				) ENGINE=MyISAM DEFAULT CHARSET=latin1;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
	
		SET @SqlCmd=CONCAT('INSERT INTO ',GT_DB,'.tmp_lte_freq_',WORKER_ID,'  (`EUTRABAND`, `FREQ`, `ADJ_FREQ`) 
				VALUES
				  (1, 2140, 2100),
				  (2, 1960, 1800),
				  (3, 1842.5, 1800),
				  (4, 2132.5, 2100),
				  (5, 881.5, 900),
				  (6, 880, 900),
				  (7, 2655, 2700),
				  (8, 942.5, 900),
				  (9, 1862.4, 1800),
				  (10, 2140, 2100),
				  (11, 1485.9, 1500),
				  (12, 737.5, 900),
				  (13, 751, 900),
				  (14, 763, 900),
				  (17, 740, 900),
				  (18, 867.5, 900),
				  (19, 882.5, 900),
				  (20, 806, 900),
				  (21, 1503.4, 1500),
				  (22, 3550, 3600),
				  (23, 2190, 2100),
				  (24, 1542, 1500),
				  (25, 1962.5, 1800),
				  (26, 876.5, 900),
				  (27, 860.5, 900),
				  (28, 780.5, 900),
				  (33, 1910, 1800),
				  (34, 2017.5, 2100),
				  (35, 1880, 1800),
				  (36, 1960, 1800),
				  (37, 1920, 1800),
				  (38, 2595, 2700),
				  (39, 1900, 1800),
				  (40, 2350, 2100),
				  (41, 2593, 2700),
				  (42, 3500, 3600),
				  (43, 3700, 3600),
				  (44, 753, 900) ;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
	
		SET @SqlCmd=CONCAT('CREATE TEMPORARY TABLE ',GT_DB,'.tmp_lte_pathloss_',WORKER_ID,'
				SELECT 
				  enodeb_id,CELL_ID,
				 ## If this antenna is indoor cell antenna, then RF distance will be 100.
				 ## Other will follow COST_HATA_MODEL_AND_URBAN_AREAS
				  CASE
				    WHEN table1.INDOOR = 1 
				    THEN 100 
				    ELSE POW(
				      10,
				      (RR + 115- table1.FF + table1.EE - 3 * CC) / table1.BB
				    ) * 1000 * COS(RADIANS(DMEC)) 
				  END AS RF_Distance
				FROM
				  (SELECT 
				    a.enodeb_id AS enodeb_id,
				    a.cell_id AS CELL_ID,
				 
				 ##[FF]:bandindex of Transmission Factor
				    CASE
				      WHEN c.ADJ_FREQ > 1500 
				      THEN 46.3+33.9 * LOG10(c.ADJ_FREQ) - 13.82 * LOG10(b.ANTENNA_HEIGHT) 
				      ELSE 69.55+26.16 * LOG10(c.ADJ_FREQ) - 13.82 * LOG10(b.ANTENNA_HEIGHT) 
				    END AS FF,
				 
				 ##[EE]:Mobile station antenna height correction factor
				    CASE
				      WHEN a.site_density_type < 3 
				      THEN 3.2 * POW(LOG10(11.75 * 1.8), 2) - 4.97 
				      ELSE 0.8+1.8 * (1.1 * LOG10(c.ADJ_FREQ) - 0.7) - 1.56 * LOG10(c.ADJ_FREQ) 
				    END AS EE,
				 
				 ##[BB]:Base Station Height Correction
				    44.9-6.55 * LOG10(b.ANTENNA_HEIGHT) AS BB,
				 
				 ##[CC]:Site Density Correction Factor
				    CASE
				      WHEN a.site_density_type = 1 
				      THEN 1 
				      WHEN a.site_density_type = 2 
				      THEN 0.67 
				      WHEN a.site_density_type = 3 
				      THEN 0.33 
				      WHEN a.site_density_type = 4 
				      THEN 0.17 
				      WHEN a.site_density_type = 5 
				      THEN 0 
				    END AS CC,
				 
				 ##[RR]:Antenna EIRP (db)
				    (b.REFERENCE_SIGNAL_POWER + ANTENNA_GAIN - FEEDER_ATTEN) AS RR,
				 
				 ##[HH]:Antenna Height (meter)
				    b.ANTENNA_HEIGHT AS HH,
				 
				 ##[INDOOR]:Indoor Cell
				    a.indoor AS INDOOR,
				 
				 ##[SS]:Site Density Type
				    a.site_density_type AS SS,
				 
				 ##[DMEC]:Antenna Downtilt Mechanical (degree)
				    DOWN_TILT_MECHANICAL AS DMEC 
				  FROM
				    ',GT_DB,'.nt_cell_current_lte a,
				    ',GT_DB,'.nt_antenna_current_lte b,
				    ',GT_DB,'.tmp_lte_freq_',WORKER_ID,' c
				  WHERE a.enodeb_id = b.enodeb_id 
				    AND a.cell_id = b.cell_id
				    AND a.EUTRABAND = c.EUTRABAND) table1 ;
		');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		SET @SqlCmd=CONCAT('UPDATE ',GT_DB,'.nt_antenna_current_lte A, ',GT_DB,'.tmp_lte_pathloss_',WORKER_ID,' B
			SET A.PATHLOSS_DISTANCE = B.RF_Distance
			WHERE A.enodeb_id = B.enodeb_id AND A.CELL_ID = B.CELL_ID;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE  IF EXISTS ',GT_DB,'.tmp_lte_pathloss_',WORKER_ID,';');
		PREPARE stmt FROM @sqlcmd;
		EXECUTE stmt;
		DEALLOCATE PREPARE stmt;	
	
	ELSEIF TECH = 'umts' THEN
		SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE  IF EXISTS ',GT_DB,'.tmp_umts_pathloss_',WORKER_ID,';');
		PREPARE stmt FROM @sqlcmd;
		EXECUTE stmt;
		DEALLOCATE PREPARE stmt;
	
		SET @SqlCmd=CONCAT('CREATE TEMPORARY TABLE ',GT_DB,'.tmp_umts_pathloss_',WORKER_ID,'
				SELECT 
				  RNC_ID,CELL_ID,
				 ## If this antenna is indoor cell antenna, then RF distance will be 100.
				 ## Other will follow COST_HATA_MODEL_AND_URBAN_AREAS
				  CASE
				    WHEN table1.INDOOR = 1 
				    THEN 100 
				    ELSE POW(
				      10,
				      (RR + 100- table1.FF + table1.EE - 3 * CC) / table1.BB
				    ) * 1000 * COS(RADIANS(DMEC)) 
				  END AS RF_Distance
				FROM
				  (SELECT 
				    a.rnc_id AS RNC_ID,
				    a.cell_id AS CELL_ID,
				 
				 ##[FF]:Frequency of Transmission Factor
				    CASE
				      WHEN a.frequency > 1500 
				      THEN 46.3+33.9 * LOG10(a.frequency) - 13.82 * LOG10(b.height) 
				      ELSE 69.55+26.16 * LOG10(a.frequency) - 13.82 * LOG10(b.height) 
				    END AS FF,
				 
				 ##[EE]:Mobile station antenna height correction factor
				    CASE
				      WHEN a.site_density_type < 3 
				      THEN 3.2 * POW(LOG10(11.75 * 1.8), 2) - 4.97 
				      ELSE 0.8+1.8 * (1.1 * LOG10(a.frequency) - 0.7) - 1.56 * LOG10(a.frequency) 
				    END AS EE,
				 
				 ##[BB]:Base Station Height Correction
				    44.9-6.55 * LOG10(b.height) AS BB,
				 
				 ##[CC]:Site Density Correction Factor
				    CASE
				      WHEN a.site_density_type = 1 
				      THEN 1 
				      WHEN a.site_density_type = 2 
				      THEN 0.67 
				      WHEN a.site_density_type = 3 
				      THEN 0.33 
				      WHEN a.site_density_type = 4 
				      THEN 0.17 
				      WHEN a.site_density_type = 5 
				      THEN 0 
				    END AS CC,
				 
				 ##[RR]:Antenna EIRP (db)
				    b.EIRP AS RR,
				 
				 ##[HH]:Antenna Height (meter)
				    b.height AS HH,
				 
				 ##[INDOOR]:Indoor Cell
				    a.indoor AS INDOOR,
				 
				 ##[SS]:Site Density Type
				    a.site_density_type AS SS,
				 
				 ##[DMEC]:Antenna Downtilt Mechanical (degree)
				    DOWNTILT_MEC AS DMEC 
				  FROM
				    ',GT_DB,'.nt_current a,
				    ',GT_DB,'.nt_antenna_current b 
				  WHERE a.rnc_id = b.rnc_id 
				    AND a.cell_id = b.cell_id) table1 ;
		');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		SET @SqlCmd=CONCAT('UPDATE ',GT_DB,'.nt_antenna_current A, ',GT_DB,'.tmp_umts_pathloss_',WORKER_ID,' B
			SET A.PATHLOSS_DISTANCE = B.RF_Distance
			WHERE A.RNC_ID = B.RNC_ID AND A.CELL_ID = B.CELL_ID;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE  IF EXISTS ',GT_DB,'.tmp_umts_pathloss_',WORKER_ID,';');
		PREPARE stmt FROM @sqlcmd;
		EXECUTE stmt;
		DEALLOCATE PREPARE stmt;
	ELSEIF TECH = 'gsm' THEN
	
		SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE  IF EXISTS ',GT_DB,'.tmp_gsm_pathloss_',WORKER_ID,';');
		PREPARE stmt FROM @sqlcmd;
		EXECUTE stmt;
		DEALLOCATE PREPARE stmt;
	
		SET @SqlCmd=CONCAT('CREATE TEMPORARY TABLE ',GT_DB,'.tmp_gsm_pathloss_',WORKER_ID,'
				SELECT 
				  bsc_id,CELL_ID,LAC,
				 ## If this antenna is indoor cell antenna, then RF distance will be 100.
				 ## Other will follow COST_HATA_MODEL_AND_URBAN_AREAS
				  CASE
				    WHEN table1.INDOOR = 1 
				    THEN 100 
				    ELSE POW(
				      10,
				      (RR + 100- table1.FF + table1.EE - 3 * CC) / table1.BB
				    ) * 1000 * COS(RADIANS(DMEC)) 
				  END AS RF_Distance
				FROM
				  (SELECT 
				    a.bsc_id AS bsc_id,
				    a.cell_id AS CELL_ID,
				    a.lac AS LAC,
				 
				 ##[FF]:bandindex of Transmission Factor
				    CASE
				      WHEN a.bandindex > 1500 
				      THEN 46.3+33.9 * LOG10(a.bandindex) - 13.82 * LOG10(b.height) 
				      ELSE 69.55+26.16 * LOG10(a.bandindex) - 13.82 * LOG10(b.height) 
				    END AS FF,
				 
				 ##[EE]:Mobile station antenna height correction factor
				    CASE
				      WHEN a.site_density_type < 3 
				      THEN 3.2 * POW(LOG10(11.75 * 1.8), 2) - 4.97 
				      ELSE 0.8+1.8 * (1.1 * LOG10(a.bandindex) - 0.7) - 1.56 * LOG10(a.bandindex) 
				    END AS EE,
				 
				 ##[BB]:Base Station Height Correction
				    44.9-6.55 * LOG10(b.height) AS BB,
				 
				 ##[CC]:Site Density Correction Factor
				    CASE
				      WHEN a.site_density_type = 1 
				      THEN 1 
				      WHEN a.site_density_type = 2 
				      THEN 0.67 
				      WHEN a.site_density_type = 3 
				      THEN 0.33 
				      WHEN a.site_density_type = 4 
				      THEN 0.17 
				      WHEN a.site_density_type = 5 
				      THEN 0 
				    END AS CC,
				 
				 ##[RR]:Antenna EIRP (db)
				    b.EIRP AS RR,
				 
				 ##[HH]:Antenna Height (meter)
				    b.height AS HH,
				 
				 ##[INDOOR]:Indoor Cell
				    a.indoor AS INDOOR,
				 
				 ##[SS]:Site Density Type
				    a.site_density_type AS SS,
				 
				 ##[DMEC]:Antenna Downtilt Mechanical (degree)
				    DOWNTILT_MEC AS DMEC 
				  FROM
				    ',GT_DB,'.nt_cell_current_gsm a,
				    ',GT_DB,'.nt_antenna_current_gsm b 
				  WHERE a.bsc_id = b.bsc_id 
				    AND a.cell_id = b.cell_id
				    AND a.lac = b.lac) table1 ;
		');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		SET @SqlCmd=CONCAT('UPDATE ',GT_DB,'.nt_antenna_current_gsm A, ',GT_DB,'.tmp_gsm_pathloss_',WORKER_ID,' B
			SET A.PATHLOSS_DISTANCE = B.RF_Distance
			WHERE A.BSC_ID = B.BSC_ID AND A.CELL_ID = B.CELL_ID AND A.LAC = B.LAC;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE  IF EXISTS ',GT_DB,'.tmp_gsm_pathloss_',WORKER_ID,';');
		PREPARE stmt FROM @sqlcmd;
		EXECUTE stmt;
		DEALLOCATE PREPARE stmt;
	END IF;
	INSERT INTO gt_gw_main.SP_LOG VALUES(GT_DB,'SP_Generate_NT_Sub_Pathloss',CONCAT('Done: ',TIMESTAMPDIFF(SECOND,START_TIME,SYSDATE()),' seconds.'), NOW());
	
	
END$$
DELIMITER ;
