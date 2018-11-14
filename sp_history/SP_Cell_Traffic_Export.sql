DELIMITER $$
USE `operations_monitor`$$
DROP PROCEDURE IF EXISTS `SP_CreateDB_LTE`$$
CREATE DEFINER=`covmo`@`%` PROCEDURE `SP_Cell_Traffic_Export`(IN exDate VARCHAR(10),IN exHour TINYINT(2),IN flag TINYINT(2) )
BEGIN
	SET @v_DATA_DATE_FORMAT= DATE_FORMAT(exDate,'%Y%m%d');
	SET @NT_DB =CONCAT('gt_nt_',@v_DATA_DATE_FORMAT);
	
	SET @dy_csv_date = @v_DATA_DATE_FORMAT;
	
	
	IF exHour < 10 THEN 
	SET @bi_CSV_DATE = CONCAT(@v_DATA_DATE_FORMAT,'_0',exHour);
	
	ELSE 
	
	SET @bi_CSV_DATE = CONCAT(@v_DATA_DATE_FORMAT,'_',exHour);
	
	END IF;
	
	IF flag = '2'
	THEN 
	
	SET @SqlCmd=CONCAT('
	DELETE FROM cell_traffic
	WHERE DATA_DATE < DATE_SUB((''',exDate,'''),INTERVAL 7 DAY)
	');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	
	SET @SqlCmd=CONCAT('SELECT
			  ''DATA_DATE'',
			  ''PU_NAME'',
			  ''ENODEB_ID'',
			  ''CELL_ID'',
			  ''SUB_REGION_ID'',
			  ''ECI'',
			  ''CELL_NAME'',
			  ''CLUSTER_NAME_REGION'',
			  ''CLUSTER_NAME_SUB_REGION'',
			  ''CELL_OSS_NODE_ID'',
			  ''ENODEB_OSS_NODE_ID'',
			  ''ENODEB_VENDOR'',
			  ''ACT_STATE'',
			  ''GTCE'',
			  ''CELL_TRAFFIC_DATA_COVERAGE'',
			  ''GTCE_AVAILABILITY'',
			  ''CELL_TRAFFIC_1ST_TIME_IN_24HR'',
			  ''NO_CELL_TRAFFIC_IN_24HR'',
			  ''CELL_NEVER_GEN_TRAFFIC'',
			  ''TRAFFIC_H00'',
			  ''TRAFFIC_H01'',
			  ''TRAFFIC_H02'',
			  ''TRAFFIC_H03'',
			  ''TRAFFIC_H04'',
			  ''TRAFFIC_H05'',
			  ''TRAFFIC_H06'',
			  ''TRAFFIC_H07'',
			  ''TRAFFIC_H08'',
			  ''TRAFFIC_H09'',
			  ''TRAFFIC_H10'',
			  ''TRAFFIC_H11'',
			  ''TRAFFIC_H12'',
			  ''TRAFFIC_H13'',
			  ''TRAFFIC_H14'',
			  ''TRAFFIC_H15'',
			  ''TRAFFIC_H16'',
			  ''TRAFFIC_H17'',
			  ''TRAFFIC_H18'',
			  ''TRAFFIC_H19'',
			  ''TRAFFIC_H20'',
			  ''TRAFFIC_H21'',
			  ''TRAFFIC_H22'',
			  ''TRAFFIC_H23''
			UNION ALL
			SELECT
			  DATA_DATE,
			  PU_NAME,
			  ENODEB_ID,
			  CELL_ID,
			  SUB_REGION_ID,
			  ECI,
			  CELL_NAME,
			  CLUSTER_NAME_REGION,
			  CLUSTER_NAME_SUB_REGION,
			  CELL_OSS_NODE_ID,
			  ENODEB_OSS_NODE_ID,
			  ENODEB_VENDOR,
			  ACT_STATE,
			  GTCE,
			  CELL_TRAFFIC_DATA_COVERAGE,
			  GTCE_AVAILABILITY,
			  CELL_TRAFFIC_1ST_TIME_IN_24HR,
			  NO_CELL_TRAFFIC_IN_24HR,
			  CELL_NEVER_GEN_TRAFFIC,
			  TRAFFIC_H00,
			  TRAFFIC_H01,
			  TRAFFIC_H02,
			  TRAFFIC_H03,
			  TRAFFIC_H04,
			  TRAFFIC_H05,
			  TRAFFIC_H06,
			  TRAFFIC_H07,
			  TRAFFIC_H08,
			  TRAFFIC_H09,
			  TRAFFIC_H10,
			  TRAFFIC_H11,
			  TRAFFIC_H12,
			  TRAFFIC_H13,
			  TRAFFIC_H14,
			  TRAFFIC_H15,
			  TRAFFIC_H16,
			  TRAFFIC_H17,
			  TRAFFIC_H18,
			  TRAFFIC_H19,
			  TRAFFIC_H20,
			  TRAFFIC_H21,
			  TRAFFIC_H22,
			  TRAFFIC_H23
			FROM  `operations_monitor`.`cell_traffic`
			WHERE DATA_DATE = ''',exDate,'''
			INTO OUTFILE ''/data/dtag/Positivelist/',@dy_csv_date,'_cell_traffic.csv''
			CHARACTER SET UTF8
			FIELDS TERMINATED BY '',''
			LINES TERMINATED BY ''\n'';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	
	
	ELSEIF flag = 1
	THEN 
	
	
	
		SET @SqlCmd=CONCAT('SELECT
			  ''DATA_DATE'',
			  ''PU_NAME'',
			  ''ENODEB_ID'',
			  ''CELL_ID'',
			  ''SUB_REGION_ID'',
			  ''ECI'',
			  ''CELL_NAME'',
			  ''CLUSTER_NAME_REGION'',
			  ''CLUSTER_NAME_SUB_REGION'',
			  ''CELL_OSS_NODE_ID'',
			  ''ENODEB_OSS_NODE_ID'',
			  ''ENODEB_VENDOR'',
			  ''ACT_STATE'',
			  ''GTCE'',
			  ''CELL_TRAFFIC_DATA_COVERAGE'',
			  ''GTCE_AVAILABILITY'',
			  ''CELL_TRAFFIC_1ST_TIME_IN_24HR'',
			  ''NO_CELL_TRAFFIC_IN_24HR'',
			  ''CELL_NEVER_GEN_TRAFFIC'',
			  ''TRAFFIC_H00'',
			  ''TRAFFIC_H01'',
			  ''TRAFFIC_H02'',
			  ''TRAFFIC_H03'',
			  ''TRAFFIC_H04'',
			  ''TRAFFIC_H05'',
			  ''TRAFFIC_H06'',
			  ''TRAFFIC_H07'',
			  ''TRAFFIC_H08'',
			  ''TRAFFIC_H09'',
			  ''TRAFFIC_H10'',
			  ''TRAFFIC_H11'',
			  ''TRAFFIC_H12'',
			  ''TRAFFIC_H13'',
			  ''TRAFFIC_H14'',
			  ''TRAFFIC_H15'',
			  ''TRAFFIC_H16'',
			  ''TRAFFIC_H17'',
			  ''TRAFFIC_H18'',
			  ''TRAFFIC_H19'',
			  ''TRAFFIC_H20'',
			  ''TRAFFIC_H21'',
			  ''TRAFFIC_H22'',
			  ''TRAFFIC_H23''
			UNION ALL
			SELECT
			  DATA_DATE,
			  PU_NAME,
			  ENODEB_ID,
			  CELL_ID,
			  SUB_REGION_ID,
			  ECI,
			  CELL_NAME,
			  CLUSTER_NAME_REGION,
			  CLUSTER_NAME_SUB_REGION,
			  CELL_OSS_NODE_ID,
			  ENODEB_OSS_NODE_ID,
			  ENODEB_VENDOR,
			  ACT_STATE,
			  GTCE,
			  CELL_TRAFFIC_DATA_COVERAGE,
			  GTCE_AVAILABILITY,
			  CELL_TRAFFIC_1ST_TIME_IN_24HR,
			  NO_CELL_TRAFFIC_IN_24HR,
			  CELL_NEVER_GEN_TRAFFIC,
			  TRAFFIC_H00,
			  TRAFFIC_H01,
			  TRAFFIC_H02,
			  TRAFFIC_H03,
			  TRAFFIC_H04,
			  TRAFFIC_H05,
			  TRAFFIC_H06,
			  TRAFFIC_H07,
			  TRAFFIC_H08,
			  TRAFFIC_H09,
			  TRAFFIC_H10,
			  TRAFFIC_H11,
			  TRAFFIC_H12,
			  TRAFFIC_H13,
			  TRAFFIC_H14,
			  TRAFFIC_H15,
			  TRAFFIC_H16,
			  TRAFFIC_H17,
			  TRAFFIC_H18,
			  TRAFFIC_H19,
			  TRAFFIC_H20,
			  TRAFFIC_H21,
			  TRAFFIC_H22,
			  TRAFFIC_H23
			FROM  `operations_monitor`.`cell_traffic`
			WHERE DATA_DATE = ''',exDate,'''  
			INTO OUTFILE ''/data/dtag/Positivelist/',@bi_CSV_DATE,'_cell_traffic.csv''
			CHARACTER SET UTF8
			FIELDS TERMINATED BY '',''
			LINES TERMINATED BY ''\n'';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	END IF ; 
	
	
	
	
	
	
	
	
	
END$$
DELIMITER ;
