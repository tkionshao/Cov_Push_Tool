DELIMITER $$
USE `gt_gw_main`$$
DROP PROCEDURE IF EXISTS `SP_CreateDB_LTE`$$
CREATE DEFINER=`covmo`@`%` PROCEDURE `SP_Api_Imsi_Info`(IN IMSI CHAR(16),IN DATA_DATE CHAR(10),IN START_TIME CHAR(8),IN END_TIME CHAR(8))
BEGIN
	SET @START_TIME=CONCAT(DATA_DATE,' ',START_TIME);
	SET @END_TIME=CONCAT(DATA_DATE,' ',END_TIME);
	SET @GT_DB=CONCAT('gt_100_',REPLACE(DATA_DATE,'-',''),'_0000_0000');
			
	SET @SqlCmd=CONCAT('SELECT COUNT(*) from  gt_gw_main.session_information WHERE SESSION_DB =  ''',@GT_DB,''' INTO  @SESSION_CNT  ;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	IF @SESSION_CNT = 0 THEN 
		SELECT 'No data available' AS message;	
	ELSE	
		SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS tmp_table_call_lte;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		
		SET @SqlCmd=CONCAT('CREATE TEMPORARY TABLE tmp_table_call_lte AS 
					SELECT
						  POS_FIRST_S_ENODEB AS ENODEB_ID,
						  POS_FIRST_S_CELL AS CELL_ID,
						  POS_FIRST_S_CELL_NAME AS CELL_NAME,
						  POS_FIRST_S_EUTRABAND AS EUTRABAND,
						  1 AS CALL_COUNT,
						  `DURATION` AS CALL_DURATION,
						  CASE WHEN (`INDOOR`=1 OR MOVING<>1) THEN 1 ELSE 0 END AS `INDOOR`,
						  CASE WHEN (POS_FIRST_S_RSRP<-115 AND POS_FIRST_S_RSRQ<-20) THEN 1
								WHEN (POS_FIRST_S_RSRP<-105 AND POS_FIRST_S_RSRP>=-115 AND POS_FIRST_S_RSRQ<-20) THEN 1
								WHEN (POS_FIRST_S_RSRP<-95 AND POS_FIRST_S_RSRP>=-105 AND POS_FIRST_S_RSRQ<-20) THEN 1
								WHEN (POS_FIRST_S_RSRP<-115 AND POS_FIRST_S_RSRQ>=-20 AND POS_FIRST_S_RSRQ<-15) THEN 1
								WHEN (POS_FIRST_S_RSRP<-105 AND POS_FIRST_S_RSRP>=-115 AND POS_FIRST_S_RSRQ>=-20 AND POS_FIRST_S_RSRQ<-15) THEN 1			
								WHEN (POS_FIRST_S_RSRP IS NULL AND POS_FIRST_S_RSRQ<-20) THEN 1
								ELSE 0 END AS RF_1,
						  CASE WHEN (POS_FIRST_S_RSRP<-85 AND POS_FIRST_S_RSRP>=-95 AND POS_FIRST_S_RSRQ<-20) THEN 1
								WHEN (POS_FIRST_S_RSRP>=-85 AND POS_FIRST_S_RSRQ<-20) THEN 1
								WHEN (POS_FIRST_S_RSRP<-95 AND POS_FIRST_S_RSRP>=-105 AND POS_FIRST_S_RSRQ>=-20 AND POS_FIRST_S_RSRQ<-15) THEN 1
								WHEN (POS_FIRST_S_RSRP<-85 AND POS_FIRST_S_RSRP>=-95 AND POS_FIRST_S_RSRQ>=-20 AND POS_FIRST_S_RSRQ<-15) THEN 1
								WHEN (POS_FIRST_S_RSRP<-115 AND POS_FIRST_S_RSRQ>=-15 AND POS_FIRST_S_RSRQ<-10) THEN 1
								WHEN (POS_FIRST_S_RSRP IS NULL AND POS_FIRST_S_RSRQ>=-20 AND POS_FIRST_S_RSRQ<-15) THEN 1
								WHEN (POS_FIRST_S_RSRP<-115 AND POS_FIRST_S_RSRQ IS NULL) THEN 1
								ELSE 0 END AS RF_2,
						  CASE WHEN (POS_FIRST_S_RSRP>=-85 AND POS_FIRST_S_RSRQ>=-20 AND POS_FIRST_S_RSRQ<-15) THEN 1
								WHEN (POS_FIRST_S_RSRP<-105 AND POS_FIRST_S_RSRP>=-115 AND POS_FIRST_S_RSRQ>=-15 AND POS_FIRST_S_RSRQ<-10) THEN 1
								WHEN (POS_FIRST_S_RSRP<-95 AND POS_FIRST_S_RSRP>=-105 AND POS_FIRST_S_RSRQ>=-15 AND POS_FIRST_S_RSRQ<-10) THEN 1
								WHEN (POS_FIRST_S_RSRP<-85 AND POS_FIRST_S_RSRP>=-95 AND POS_FIRST_S_RSRQ>=-15 AND POS_FIRST_S_RSRQ<-10) THEN 1
								WHEN (POS_FIRST_S_RSRP<-115 AND POS_FIRST_S_RSRQ>=-10 AND POS_FIRST_S_RSRQ<-5) THEN 1
								WHEN (POS_FIRST_S_RSRP<-105 AND POS_FIRST_S_RSRP>=-115 AND POS_FIRST_S_RSRQ>=-10 AND POS_FIRST_S_RSRQ<-5) THEN 1
								WHEN (POS_FIRST_S_RSRP IS NULL AND POS_FIRST_S_RSRQ>=-15 AND POS_FIRST_S_RSRQ<-10) THEN 1
								WHEN (POS_FIRST_S_RSRP<-85 AND POS_FIRST_S_RSRP>=-115 AND POS_FIRST_S_RSRQ IS NULL) THEN 1
								ELSE 0 END AS RF_3,
						  CASE WHEN (POS_FIRST_S_RSRP>=-85 AND POS_FIRST_S_RSRQ>=-15 AND POS_FIRST_S_RSRQ<-10) THEN 1
								WHEN (POS_FIRST_S_RSRP<-95 AND POS_FIRST_S_RSRP>=-105 AND POS_FIRST_S_RSRQ>=-10 AND POS_FIRST_S_RSRQ<-5) THEN 1
								WHEN (POS_FIRST_S_RSRP<-115 AND POS_FIRST_S_RSRQ>=-5) THEN 1
								WHEN (POS_FIRST_S_RSRP<-105 AND POS_FIRST_S_RSRP>=-115 AND POS_FIRST_S_RSRQ>=-5) THEN 1
								WHEN (POS_FIRST_S_RSRP<-95 AND POS_FIRST_S_RSRQ>=-105 AND POS_FIRST_S_RSRQ>=-5) THEN 1
								WHEN (POS_FIRST_S_RSRP IS NULL AND POS_FIRST_S_RSRQ>=-10) THEN 1
								WHEN (POS_FIRST_S_RSRP>=-85 AND POS_FIRST_S_RSRQ IS NULL) THEN 1
								ELSE 0 END AS RF_4,
						  CASE WHEN (POS_FIRST_S_RSRP<-85 AND POS_FIRST_S_RSRP>=-95 AND POS_FIRST_S_RSRQ>=-10 AND POS_FIRST_S_RSRQ<-5) THEN 1
								WHEN (POS_FIRST_S_RSRP>=-85 AND POS_FIRST_S_RSRQ>=-10 AND POS_FIRST_S_RSRQ<-5) THEN 1
								WHEN (POS_FIRST_S_RSRP<-85 AND POS_FIRST_S_RSRP>=-95 AND POS_FIRST_S_RSRQ>=-5) THEN 1
								WHEN (POS_FIRST_S_RSRQ>=-85 AND POS_FIRST_S_RSRQ>=-5) THEN 1
								ELSE 0 END AS RF_5,
								POS_FIRST_S_RSRP,POS_FIRST_S_RSRQ
						FROM `',@GT_DB,'`.`table_call_lte`
						WHERE IMSI=''',IMSI,''' AND `START_TIME`>=''',@START_TIME,''' AND `START_TIME`<''',@END_TIME,''';');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		
		SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS tmp_table_call_lte_all;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		
		SET @SqlCmd=CONCAT('CREATE TEMPORARY TABLE tmp_table_call_lte_all AS 
					SELECT 
						CASE WHEN (AVG(POS_FIRST_S_RSRP)<-115 AND AVG(POS_FIRST_S_RSRQ)<-20) THEN 1
							WHEN (AVG(POS_FIRST_S_RSRP)<-105 AND AVG(POS_FIRST_S_RSRP)>=-115 AND AVG(POS_FIRST_S_RSRQ)<-20) THEN 1
							WHEN (AVG(POS_FIRST_S_RSRP)<-95 AND AVG(POS_FIRST_S_RSRP)>=-105 AND AVG(POS_FIRST_S_RSRQ)<-20) THEN 1
							WHEN (AVG(POS_FIRST_S_RSRP)<-115 AND AVG(POS_FIRST_S_RSRQ)>=-20 AND AVG(POS_FIRST_S_RSRQ)<-15) THEN 1
							WHEN (AVG(POS_FIRST_S_RSRP)<-105 AND AVG(POS_FIRST_S_RSRP)>=-115 AND AVG(POS_FIRST_S_RSRQ)>=-20 AND AVG(POS_FIRST_S_RSRQ)<-15) THEN 1
							WHEN (AVG(POS_FIRST_S_RSRP) IS NULL AND AVG(POS_FIRST_S_RSRQ)<-20) THEN 1
							WHEN (AVG(POS_FIRST_S_RSRP)<-85 AND AVG(POS_FIRST_S_RSRP)>=-95 AND AVG(POS_FIRST_S_RSRQ)<-20) THEN 2
							WHEN (AVG(POS_FIRST_S_RSRP)>=-85 AND AVG(POS_FIRST_S_RSRQ)<-20) THEN 2
							WHEN (AVG(POS_FIRST_S_RSRP)<-95 AND AVG(POS_FIRST_S_RSRP)>=-105 AND AVG(POS_FIRST_S_RSRQ)>=-20 AND AVG(POS_FIRST_S_RSRQ)<-15) THEN 2
							WHEN (AVG(POS_FIRST_S_RSRP)<-85 AND AVG(POS_FIRST_S_RSRP)>=-95 AND AVG(POS_FIRST_S_RSRQ)>=-20 AND AVG(POS_FIRST_S_RSRQ)<-15) THEN 2
							WHEN (AVG(POS_FIRST_S_RSRP)<-115 AND AVG(POS_FIRST_S_RSRQ)>=-15 AND AVG(POS_FIRST_S_RSRQ)<-10) THEN 2
							WHEN (AVG(POS_FIRST_S_RSRP) IS NULL AND AVG(POS_FIRST_S_RSRQ)>=-20 AND AVG(POS_FIRST_S_RSRQ)<-15) THEN 2
							WHEN (AVG(POS_FIRST_S_RSRP)<-115 AND AVG(POS_FIRST_S_RSRQ) IS NULL) THEN 2
							WHEN (AVG(POS_FIRST_S_RSRP)>=-85 AND AVG(POS_FIRST_S_RSRQ)>=-20 AND AVG(POS_FIRST_S_RSRQ)<-15) THEN 3
							WHEN (AVG(POS_FIRST_S_RSRP)<-105 AND AVG(POS_FIRST_S_RSRP)>=-115 AND AVG(POS_FIRST_S_RSRQ)>=-15 AND AVG(POS_FIRST_S_RSRQ)<-10) THEN 3
							WHEN (AVG(POS_FIRST_S_RSRP)<-95 AND AVG(POS_FIRST_S_RSRP)>=-105 AND AVG(POS_FIRST_S_RSRQ)>=-15 AND AVG(POS_FIRST_S_RSRQ)<-10) THEN 3
							WHEN (AVG(POS_FIRST_S_RSRP)<-85 AND AVG(POS_FIRST_S_RSRP)>=-95 AND AVG(POS_FIRST_S_RSRQ)>=-15 AND AVG(POS_FIRST_S_RSRQ)<-10) THEN 3
							WHEN (AVG(POS_FIRST_S_RSRP)<-115 AND AVG(POS_FIRST_S_RSRQ)>=-10 AND AVG(POS_FIRST_S_RSRQ)<-5) THEN 3
							WHEN (AVG(POS_FIRST_S_RSRP)<-105 AND AVG(POS_FIRST_S_RSRP)>=-115 AND AVG(POS_FIRST_S_RSRQ)>=-10 AND AVG(POS_FIRST_S_RSRQ)<-5) THEN 3
							WHEN (AVG(POS_FIRST_S_RSRP) IS NULL AND AVG(POS_FIRST_S_RSRQ)>=-15 AND AVG(POS_FIRST_S_RSRQ)<-10) THEN 3
							WHEN (AVG(POS_FIRST_S_RSRP)<-85 AND AVG(POS_FIRST_S_RSRP)>=-115 AND AVG(POS_FIRST_S_RSRQ) IS NULL) THEN 3
							WHEN (AVG(POS_FIRST_S_RSRP)>=-85 AND AVG(POS_FIRST_S_RSRQ)>=-15 AND AVG(POS_FIRST_S_RSRQ)<-10) THEN 4
							WHEN (AVG(POS_FIRST_S_RSRP)<-105 AND AVG(POS_FIRST_S_RSRP)>=-95 AND AVG(POS_FIRST_S_RSRQ)>=-10 AND AVG(POS_FIRST_S_RSRQ)<-5) THEN 4
							WHEN (AVG(POS_FIRST_S_RSRP)<-115 AND AVG(POS_FIRST_S_RSRQ)>=-5) THEN 4
							WHEN (AVG(POS_FIRST_S_RSRP)<-105 AND AVG(POS_FIRST_S_RSRP)>=-115 AND AVG(POS_FIRST_S_RSRQ)>=-5) THEN 4
							WHEN (AVG(POS_FIRST_S_RSRP)<-95 AND AVG(POS_FIRST_S_RSRQ)>=-105 AND AVG(POS_FIRST_S_RSRQ)>=-5) THEN 4
							WHEN (AVG(POS_FIRST_S_RSRP) IS NULL AND AVG(POS_FIRST_S_RSRQ)>=-10) THEN 4
							WHEN (AVG(POS_FIRST_S_RSRP)>=-85 AND AVG(POS_FIRST_S_RSRQ) IS NULL) THEN 4
							WHEN (AVG(POS_FIRST_S_RSRP)<-85 AND AVG(POS_FIRST_S_RSRP)>=-95 AND AVG(POS_FIRST_S_RSRQ)>=-10 AND AVG(POS_FIRST_S_RSRQ)<-5) THEN 5
							WHEN (AVG(POS_FIRST_S_RSRP)>=-85 AND AVG(POS_FIRST_S_RSRQ)>=-10 AND AVG(POS_FIRST_S_RSRQ)<-5) THEN 5
							WHEN (AVG(POS_FIRST_S_RSRP)<-85 AND AVG(POS_FIRST_S_RSRP)>=-95 AND AVG(POS_FIRST_S_RSRQ)>=-5) THEN 5
							WHEN (AVG(POS_FIRST_S_RSRQ)>=-85 AND AVG(POS_FIRST_S_RSRQ)>=-5) THEN 5
						ELSE NULL END AS RF_QUALITY_OVERALL
					FROM tmp_table_call_lte;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		
		SET @SqlCmd=CONCAT('SELECT ECI,
					CELL_NAME,
					RF_BAND,
					CELL_RF_QUALITY_OVERALL,
					RF_QUALITY_OVERALL,
					INDOOR_PERCENTAGE,
					CALL_COUNT,
					CALL_DURATION,
					RF_QUALITY_EXCELLENT,
					RF_QUALITY_VERYGOOD,
					RF_QUALITY_GOOD,
					RF_QUALITY_FAIR,
					RF_QUALITY_POOR
					FROM 
					(
						SELECT CONCAT(ENODEB_ID,CELL_ID) AS ECI,
						CELL_NAME,
						GROUP_CONCAT( DISTINCT EUTRABAND SEPARATOR '','' ) AS RF_BAND,
						SUM(CALL_COUNT) AS CALL_COUNT,
						SUM(CALL_DURATION)/1000 AS CALL_DURATION ,
						ROUND(SUM(INDOOR)/SUM(CALL_COUNT)*100) AS INDOOR_PERCENTAGE,
						SUM(RF_5) AS RF_QUALITY_EXCELLENT,
						SUM(RF_4) AS RF_QUALITY_VERYGOOD,
						SUM(RF_3) AS RF_QUALITY_GOOD,
						SUM(RF_2) AS RF_QUALITY_FAIR,
						SUM(RF_1) AS RF_QUALITY_POOR,
						CASE WHEN (AVG(POS_FIRST_S_RSRP)<-115 AND AVG(POS_FIRST_S_RSRQ)<-20) THEN 1
							WHEN (AVG(POS_FIRST_S_RSRP)<-105 AND AVG(POS_FIRST_S_RSRP)>=-115 AND AVG(POS_FIRST_S_RSRQ)<-20) THEN 1
							WHEN (AVG(POS_FIRST_S_RSRP)<-95 AND AVG(POS_FIRST_S_RSRP)>=-105 AND AVG(POS_FIRST_S_RSRQ)<-20) THEN 1
							WHEN (AVG(POS_FIRST_S_RSRP)<-115 AND AVG(POS_FIRST_S_RSRQ)>=-20 AND AVG(POS_FIRST_S_RSRQ)<-15) THEN 1
							WHEN (AVG(POS_FIRST_S_RSRP)<-105 AND AVG(POS_FIRST_S_RSRP)>=-115 AND AVG(POS_FIRST_S_RSRQ)>=-20 AND AVG(POS_FIRST_S_RSRQ)<-15) THEN 1
							WHEN (AVG(POS_FIRST_S_RSRP) IS NULL AND AVG(POS_FIRST_S_RSRQ)<-20) THEN 1
							WHEN (AVG(POS_FIRST_S_RSRP)<-85 AND AVG(POS_FIRST_S_RSRP)>=-95 AND AVG(POS_FIRST_S_RSRQ)<-20) THEN 2
							WHEN (AVG(POS_FIRST_S_RSRP)>=-85 AND AVG(POS_FIRST_S_RSRQ)<-20) THEN 2
							WHEN (AVG(POS_FIRST_S_RSRP)<-95 AND AVG(POS_FIRST_S_RSRP)>=-105 AND AVG(POS_FIRST_S_RSRQ)>=-20 AND AVG(POS_FIRST_S_RSRQ)<-15) THEN 2
							WHEN (AVG(POS_FIRST_S_RSRP)<-85 AND AVG(POS_FIRST_S_RSRP)>=-95 AND AVG(POS_FIRST_S_RSRQ)>=-20 AND AVG(POS_FIRST_S_RSRQ)<-15) THEN 2
							WHEN (AVG(POS_FIRST_S_RSRP)<-115 AND AVG(POS_FIRST_S_RSRQ)>=-15 AND AVG(POS_FIRST_S_RSRQ)<-10) THEN 2
							WHEN (AVG(POS_FIRST_S_RSRP) IS NULL AND AVG(POS_FIRST_S_RSRQ)>=-20 AND AVG(POS_FIRST_S_RSRQ)<-15) THEN 2
							WHEN (AVG(POS_FIRST_S_RSRP)<-115 AND AVG(POS_FIRST_S_RSRQ) IS NULL) THEN 2
							WHEN (AVG(POS_FIRST_S_RSRP)>=-85 AND AVG(POS_FIRST_S_RSRQ)>=-20 AND AVG(POS_FIRST_S_RSRQ)<-15) THEN 3
							WHEN (AVG(POS_FIRST_S_RSRP)<-105 AND AVG(POS_FIRST_S_RSRP)>=-115 AND AVG(POS_FIRST_S_RSRQ)>=-15 AND AVG(POS_FIRST_S_RSRQ)<-10) THEN 3
							WHEN (AVG(POS_FIRST_S_RSRP)<-95 AND AVG(POS_FIRST_S_RSRP)>=-105 AND AVG(POS_FIRST_S_RSRQ)>=-15 AND AVG(POS_FIRST_S_RSRQ)<-10) THEN 3
							WHEN (AVG(POS_FIRST_S_RSRP)<-85 AND AVG(POS_FIRST_S_RSRP)>=-95 AND AVG(POS_FIRST_S_RSRQ)>=-15 AND AVG(POS_FIRST_S_RSRQ)<-10) THEN 3
							WHEN (AVG(POS_FIRST_S_RSRP)<-115 AND AVG(POS_FIRST_S_RSRQ)>=-10 AND AVG(POS_FIRST_S_RSRQ)<-5) THEN 3
							WHEN (AVG(POS_FIRST_S_RSRP)<-105 AND AVG(POS_FIRST_S_RSRP)>=-115 AND AVG(POS_FIRST_S_RSRQ)>=-10 AND AVG(POS_FIRST_S_RSRQ)<-5) THEN 3
							WHEN (AVG(POS_FIRST_S_RSRP) IS NULL AND AVG(POS_FIRST_S_RSRQ)>=-15 AND AVG(POS_FIRST_S_RSRQ)<-10) THEN 3
							WHEN (AVG(POS_FIRST_S_RSRP)<-85 AND AVG(POS_FIRST_S_RSRP)>=-115 AND AVG(POS_FIRST_S_RSRQ) IS NULL) THEN 3
							WHEN (AVG(POS_FIRST_S_RSRP)>=-85 AND AVG(POS_FIRST_S_RSRQ)>=-15 AND AVG(POS_FIRST_S_RSRQ)<-10) THEN 4
							WHEN (AVG(POS_FIRST_S_RSRP)<-105 AND AVG(POS_FIRST_S_RSRP)>=-95 AND AVG(POS_FIRST_S_RSRQ)>=-10 AND AVG(POS_FIRST_S_RSRQ)<-5) THEN 4
							WHEN (AVG(POS_FIRST_S_RSRP)<-115 AND AVG(POS_FIRST_S_RSRQ)>=-5) THEN 4
							WHEN (AVG(POS_FIRST_S_RSRP)<-105 AND AVG(POS_FIRST_S_RSRP)>=-115 AND AVG(POS_FIRST_S_RSRQ)>=-5) THEN 4
							WHEN (AVG(POS_FIRST_S_RSRP)<-95 AND AVG(POS_FIRST_S_RSRQ)>=-105 AND AVG(POS_FIRST_S_RSRQ)>=-5) THEN 4
							WHEN (AVG(POS_FIRST_S_RSRP) IS NULL AND AVG(POS_FIRST_S_RSRQ)>=-10) THEN 4
							WHEN (AVG(POS_FIRST_S_RSRP)>=-85 AND AVG(POS_FIRST_S_RSRQ) IS NULL) THEN 4
							WHEN (AVG(POS_FIRST_S_RSRP)<-85 AND AVG(POS_FIRST_S_RSRP)>=-95 AND AVG(POS_FIRST_S_RSRQ)>=-10 AND AVG(POS_FIRST_S_RSRQ)<-5) THEN 5
							WHEN (AVG(POS_FIRST_S_RSRP)>=-85 AND AVG(POS_FIRST_S_RSRQ)>=-10 AND AVG(POS_FIRST_S_RSRQ)<-5) THEN 5
							WHEN (AVG(POS_FIRST_S_RSRP)<-85 AND AVG(POS_FIRST_S_RSRP)>=-95 AND AVG(POS_FIRST_S_RSRQ)>=-5) THEN 5
							WHEN (AVG(POS_FIRST_S_RSRQ)>=-85 AND AVG(POS_FIRST_S_RSRQ)>=-5) THEN 5
							ELSE NULL END AS CELL_RF_QUALITY_OVERALL
						FROM tmp_table_call_lte
						WHERE CELL_ID IS NOT NULL
						GROUP BY ENODEB_ID,CELL_ID
					) A, tmp_table_call_lte_all B;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
	END IF ;
	
END$$
DELIMITER ;
