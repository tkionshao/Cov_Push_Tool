CREATE DEFINER=`covmo`@`%` PROCEDURE `SP_Sub_Generate_ACO_Report_lte`(IN GT_DB VARCHAR(100) )
BEGIN	
	DECLARE O_GT_DB VARCHAR(100) DEFAULT GT_DB;
	DECLARE SH_EH VARCHAR(9) DEFAULT RIGHT(GT_DB,9);
	DECLARE START_TIME DATETIME DEFAULT SYSDATE();	
	DECLARE ACTIVE_CNT INT DEFAULT 0;
	DECLARE I INT DEFAULT 0 ;
	DECLARE WORKER_ID VARCHAR(10) DEFAULT CONNECTION_ID();
	DECLARE CURRENT_NT_DB VARCHAR(50) DEFAULT CONCAT('gt_nt_',gt_strtok(GT_DB,3,'_'));
	
	INSERT INTO gt_gw_main.SP_LOG VALUES(O_GT_DB,'Sp_Sub_Generate_ACO_Report_lte','Start', START_TIME);
	
	INSERT INTO gt_gw_main.sp_log VALUES(O_GT_DB,'Sp_Sub_Generate_ACO_Report_lte','Step1', NOW());
	SET @SqlCmd=CONCAT('DROP TABLE  IF EXISTS ',GT_DB,'.tmp_opt_aco_traffic_unbalance_lte;');
	PREPARE stmt FROM @sqlcmd;
	EXECUTE stmt;
	DEALLOCATE PREPARE stmt;
	
	SET @SqlCmd=CONCAT('CREATE TABLE ',GT_DB,'.`tmp_opt_aco_traffic_unbalance_lte` (
	`ENODEB_ID` mediumint(9) DEFAULT NULL,
	`CELL_ID` mediumint(9) DEFAULT NULL,
	`DATA_DATE` datetime DEFAULT NULL,
	`DATA_HOUR` tinyint(4) DEFAULT NULL,
	`DATA_CALL_COUNT` mediumint(9) DEFAULT ''0'',
	`VOLTE_CALL_COUNT` mediumint(9) DEFAULT ''0'',
	`MIN_THROUGHPUT` double DEFAULT NULL,
	`MAX_THROUGHPUT` double DEFAULT NULL,
	`AVG_THROUGHPUT` double DEFAULT NULL,
	`TRAFFIC_VOLUME_DIST` double DEFAULT NULL,
	`BLOCK_RATE` double DEFAULT NULL,
	`BLOCK_CALL_CNT` mediumint(9) DEFAULT ''0'',
	`BLOCK_WEIGHT` double DEFAULT NULL,
	`AVG_TRA_HIGH_THAN_NEB` double DEFAULT NULL,
	`NBR_CNT` mediumint(9) DEFAULT NULL,
	`AVG_TRA_DIFF_RATE` double DEFAULT NULL,
	`MAX_TRA_DIFF_RATE` double DEFAULT NULL,
	KEY `opt_aco_traffic_unbalance_idx1` (`CELL_ID`,`DATA_HOUR`)
	) ENGINE=MyISAM DEFAULT CHARSET=latin1;');
	PREPARE stmt FROM @sqlcmd;
	EXECUTE stmt;
	DEALLOCATE PREPARE stmt;	
		
	SET @SqlCmd=CONCAT('Insert into ',GT_DB,'.tmp_opt_aco_traffic_unbalance_lte
	SELECT
	ENODEB_ID,CELL_ID,DATA_DATE,DATA_HOUR
	,SUM(DATA_CALL_COUNT) AS DATA_CALL_COUNT
	,SUM(VOLTE_CALL_COUNT) AS VOLTE_CALL_COUNT
	,MIN(MIN_THROUGHPUT) AS MIN_THROUGHPUT
	,MAX(MAX_THROUGHPUT) AS MAX_THROUGHPUT
	,SUM(AVG_THROUGHPUT_NUM)/SUM(AVG_THROUGHPUT_DEN) AS AVG_THROUGHPUT
	,SUM(TRAFFIC_VOLUME_DIST) AS TRAFFIC_VOLUME_DIST
	,SUM(BLOCK_CALL_CNT)/SUM(BLOCK_DEN) AS BLOCK_RATE
	,SUM(BLOCK_CALL_CNT) AS BLOCK_CALL_CNT
	,POW(SUM(BLOCK_CALL_CNT),2)/SUM(BLOCK_DEN) AS BLOCK_WEIGHT
	,NULL AS AVG_TRA_HIGH_THAN_NEB
	,NULL AS NBR_CNT
	,NULL AS AVG_TRA_DIFF_RATE
	,NULL AS MAX_TRA_DIFF_RATE
	FROM ',GT_DB,'.opt_aco_traffic_lte
	GROUP BY ENODEB_ID,CELL_ID,DATA_DATE,DATA_HOUR;
	');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;	
	
	
	INSERT INTO gt_gw_main.sp_log VALUES(O_GT_DB,'Sp_Sub_Generate_ACO_Report_lte','Step3', NOW());
	
	SET @SqlCmd=CONCAT('DROP TABLE IF EXISTS ',GT_DB,'.tmp_aco_nt_lte;');
	PREPARE stmt FROM @sqlcmd;
	EXECUTE stmt;
	DEALLOCATE PREPARE stmt;
	
	SET @SqlCmd=CONCAT('CREATE TABLE ',GT_DB,'.tmp_aco_nt_lte as 
	SELECT A.* FROM  ',CURRENT_NT_DB,'.nt_cell_current_lte A
	INNER JOIN (SELECT DISTINCT ENODEB_ID,CELL_ID FROM ',GT_DB,'.tmp_opt_aco_traffic_unbalance_lte) B
	ON A.ENODEB_ID=B.ENODEB_ID AND A.CELL_ID=B.CELL_ID;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('CREATE INDEX tmp_aco_nt_idx1 ON ',GT_DB,'.tmp_aco_nt_lte(ENODEB_ID,cell_id)');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS ',GT_DB,'.tmp_aco_neighbor_',WORKER_ID,';');
	PREPARE stmt FROM @sqlcmd;
	EXECUTE stmt;
	DEALLOCATE PREPARE stmt;
	
	
	SET @SqlCmd=CONCAT('CREATE TEMPORARY TABLE ',GT_DB,'.tmp_aco_neighbor_',WORKER_ID,' as 		
	SELECT DISTINCT A.ENODEB_ID,A.CELL_ID,B.NBR_ENODEB_ID,B.NBR_CELL_ID FROM ',GT_DB,'.tmp_aco_nt_lte A 
	INNER JOIN  ',CURRENT_NT_DB,'.nt_nbr_4_4_current_lte B 
	ON A.ENODEB_ID=B.ENODEB_ID AND A.CELL_ID=B.CELL_ID
	WHERE B.NBR_TYPE =1		
		
	UNION		
	
	SELECT DISTINCT B.ENODEB_ID,B.CELL_ID,C.ENODEB_ID NBR_ENODEB_ID,C.CELL_ID NBR_CELL_ID FROM 
	',CURRENT_NT_DB,'.nt_neighbor_voronoi_lte A
	INNER JOIN ',GT_DB,'.tmp_aco_nt_lte B
	ON A.ENODEB_ID=B.ENODEB_ID 
	INNER JOIN ',GT_DB,'.tmp_aco_nt_lte C		
	ON A.NBR_ENODEB_ID=C.ENODEB_ID 
	WHERE B.DL_EARFCN=C.DL_EARFCN;
	
	');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('UPDATE ',GT_DB,'.tmp_opt_aco_traffic_unbalance_lte A ,
	( SELECT ENODEB_ID,CELL_ID ,COUNT(*) CNT 
	FROM ',GT_DB,'.tmp_aco_neighbor_',WORKER_ID,'
	GROUP BY ENODEB_ID,CELL_ID
	) B
	SET A.NBR_CNT=B.CNT 
	WHERE A.ENODEB_ID=B.ENODEB_ID AND A.CELL_ID=B.CELL_ID;
	');	
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	
	
	INSERT INTO gt_gw_main.sp_log VALUES(O_GT_DB,'Sp_Sub_Generate_ACO_Report_lte','Step4', NOW());
	
	SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE  IF EXISTS ',GT_DB,'.tmp_aco_avg_tra_high_web_',WORKER_ID,';');
	PREPARE stmt FROM @sqlcmd;
	EXECUTE stmt;
	DEALLOCATE PREPARE stmt;
	
	SET @SQLCMD=CONCAT ('CREATE TEMPORARY TABLE ',GT_DB,'.tmp_aco_avg_tra_high_web_',WORKER_ID,' as 
	SELECT AA.ENODEB_ID,AA.CELL_ID,AA.DATA_HOUR ,TRAFFIC_VOLUME_DIST,(SUM(CASE WHEN TRAFFIC_VOLUME_DIST>TRAFFIC_VOLUME_DIST_NBR*1.2 THEN 1 ELSE 0 END)/NBR_CNT)AS AVG_TRA_HIGH_THAN_NEB
	FROM ',GT_DB,'.tmp_opt_aco_traffic_unbalance_lte AA
	INNER JOIN 
	(
	SELECT A.DATA_HOUR,B.ENODEB_ID,B.CELL_ID,B.NBR_ENODEB_ID,B.NBR_CELL_ID,A.TRAFFIC_VOLUME_DIST AS TRAFFIC_VOLUME_DIST_NBR
	FROM ',GT_DB,'.tmp_opt_aco_traffic_unbalance_lte A
	INNER JOIN ',GT_DB,'.tmp_aco_neighbor_',WORKER_ID,' B
	ON A.ENODEB_ID=B.NBR_ENODEB_ID
	AND A.CELL_ID=B.NBR_CELL_ID
	) BB
	ON AA.ENODEB_ID=BB.ENODEB_ID AND AA.CELL_ID=BB.CELL_ID 
	AND AA.DATA_HOUR=BB.DATA_HOUR 
	GROUP BY ENODEB_ID,CELL_ID ,DATA_DATE,DATA_HOUR ;
	');	
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SQLCMD=CONCAT ('UPDATE ',GT_DB,'.tmp_opt_aco_traffic_unbalance_lte A,',GT_DB,'.tmp_aco_avg_tra_high_web_',WORKER_ID,' B	
	SET A.AVG_TRA_HIGH_THAN_NEB=B.AVG_TRA_HIGH_THAN_NEB
	WHERE A.ENODEB_ID=B.ENODEB_ID AND A.CELL_ID=B.CELL_ID AND A.DATA_HOUR=B.DATA_HOUR;
	');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	
	
	
	
	
	INSERT INTO gt_gw_main.sp_log VALUES(O_GT_DB,'Sp_Sub_Generate_ACO_Report','Step5', NOW());
	SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE  IF EXISTS ',GT_DB,'.tmp_aco_neighbor_traffic_',WORKER_ID,';');
	PREPARE stmt FROM @sqlcmd;
	EXECUTE stmt;
	DEALLOCATE PREPARE stmt;
	SET @SQLCMD=CONCAT ('CREATE TEMPORARY TABLE ',GT_DB,'.tmp_aco_neighbor_traffic_',WORKER_ID,' as 	
	SELECT A.DATA_DATE,A.DATA_HOUR,B.ENODEB_ID,B.CELL_ID,MIN(A.TRAFFIC_VOLUME_DIST) NBR_TRAFFIC_VOLUME_DIST FROM ',GT_DB,'.tmp_opt_aco_traffic_unbalance_lte A
	INNER JOIN  ',GT_DB,'.tmp_aco_neighbor_',WORKER_ID,' B 
	ON B.NBR_ENODEB_ID=A.ENODEB_ID AND B.NBR_CELL_ID=A.CELL_ID
	GROUP BY A.DATA_DATE,A.DATA_HOUR,B.ENODEB_ID,B.CELL_ID;
	');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SQLCMD=CONCAT ('CREATE INDEX  tmp_aco_neighbor_traffic_idx1 on ',GT_DB,'.tmp_aco_neighbor_traffic_',WORKER_ID,'(data_hour,cell_id) ');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SQLCMD=CONCAT ('UPDATE ',GT_DB,'.tmp_opt_aco_traffic_unbalance_lte A ,',GT_DB,'.tmp_aco_neighbor_traffic_',WORKER_ID,' b	
	SET MAX_TRA_DIFF_RATE=(A.TRAFFIC_VOLUME_DIST-B.NBR_TRAFFIC_VOLUME_DIST)/A.TRAFFIC_VOLUME_DIST	 
	WHERE A.DATA_HOUR=B.DATA_HOUR AND A.CELL_ID=B.CELL_ID AND A.DATA_DATE=B.DATA_DATE AND A.ENODEB_ID=B.ENODEB_ID;
	');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	
	INSERT INTO gt_gw_main.sp_log VALUES(O_GT_DB,'Sp_Sub_Generate_ACO_Report','Step6', NOW());
	SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE  IF EXISTS ',GT_DB,'.tmp_aco_neighbor_traffic_summary_',WORKER_ID,';');
	PREPARE stmt FROM @sqlcmd;
	EXECUTE stmt;
	DEALLOCATE PREPARE stmt;
	
	SET @SQLCMD=CONCAT ('CREATE TEMPORARY TABLE ',GT_DB,'.tmp_aco_neighbor_traffic_summary_',WORKER_ID,' as 
	SELECT A.DATA_DATE,A.DATA_HOUR,B.ENODEB_ID,B.CELL_ID,B.NBR_ENODEB_ID,B.NBR_CELL_ID
	,A.TRAFFIC_VOLUME_DIST NBR_TRAFFIC_VOLUME_DIST 
	FROM ',GT_DB,'.tmp_opt_aco_traffic_unbalance_lte A	
	INNER JOIN  ',GT_DB,'.tmp_aco_neighbor_',WORKER_ID,' B 
	ON B.NBR_ENODEB_ID=A.ENODEB_ID AND B.NBR_CELL_ID=A.CELL_ID;
	');	
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
		
	SET @SQLCMD=CONCAT ('CREATE INDEX  tmp_aco_neighbor_traffic_summary_idx1 on ',GT_DB,'.tmp_aco_neighbor_traffic_summary_',WORKER_ID,'(data_hour,cell_id) ');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SQLCMD=CONCAT ('UPDATE ',GT_DB,'.tmp_opt_aco_traffic_unbalance_lte AA ,
	(
	SELECT B.ENODEB_ID,B.CELL_ID,B.DATA_DATE,B.DATA_HOUR
	,SUM(A.TRAFFIC_VOLUME_DIST-B.NBR_TRAFFIC_VOLUME_DIST)/A.TRAFFIC_VOLUME_DIST/A.NBR_CNT AS AVG_TRA_DIFF_RATE
	FROM ',GT_DB,'.tmp_opt_aco_traffic_unbalance_lte A 
	INNER JOIN ',GT_DB,'.tmp_aco_neighbor_traffic_summary_',WORKER_ID,' b 
	ON A.DATA_HOUR=B.DATA_HOUR AND A.CELL_ID=B.CELL_ID AND A.DATA_DATE=B.DATA_DATE AND A.ENODEB_ID=B.ENODEB_ID
	WHERE  TRAFFIC_VOLUME_DIST>B.NBR_TRAFFIC_VOLUME_DIST
	GROUP BY DATA_DATE,DATA_HOUR,ENODEB_ID,CELL_ID
	) CC 
	SET  AA.AVG_TRA_DIFF_RATE=CC.AVG_TRA_DIFF_RATE
	WHERE AA.DATA_HOUR=CC.DATA_HOUR AND AA.CELL_ID=CC.CELL_ID AND AA.DATA_DATE=CC.DATA_DATE AND AA.ENODEB_ID=CC.ENODEB_ID;
	');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	
	INSERT INTO gt_gw_main.sp_log VALUES(O_GT_DB,'Sp_Sub_Generate_ACO_Report_lte','Step7', NOW());
	
	SET @SqlCmd=CONCAT('TRUNCATE TABLE ',GT_DB,'.opt_aco_traffic_unbalance_lte;');
	PREPARE stmt FROM @sqlcmd;
	EXECUTE stmt;
	DEALLOCATE PREPARE stmt;
	
	SET @SqlCmd=CONCAT('INSERT INTO ',GT_DB,'.`opt_aco_traffic_unbalance_lte`
	(`ENODEB_ID`,
	`CELL_ID`,
	`DATA_DATE`,
	`DATA_HOUR`,
	`DATA_CALL_COUNT`,
	`VOLTE_CALL_COUNT`,
	`MIN_THROUGHPUT`,
	`MAX_THROUGHPUT`,
	`AVG_THROUGHPUT`,
	`TRAFFIC_VOLUME_DIST`,
	`BLOCK_RATE`,
	`BLOCK_CALL_CNT`,
	`BLOCK_WEIGHT`,
	`AVG_TRA_HIGH_THAN_NEB`,
	`NBR_CNT`,
	`AVG_TRA_DIFF_RATE`,
	`MAX_TRA_DIFF_RATE`)
	SELECT `ENODEB_ID`,
	`CELL_ID`,
	`DATA_DATE`,
	`DATA_HOUR`,
	`DATA_CALL_COUNT`,
	`VOLTE_CALL_COUNT`,
	`MIN_THROUGHPUT`,
	`MAX_THROUGHPUT`,
	`AVG_THROUGHPUT`,
	`TRAFFIC_VOLUME_DIST`,
	`BLOCK_RATE`,
	`BLOCK_CALL_CNT`,
	`BLOCK_WEIGHT`,
	`AVG_TRA_HIGH_THAN_NEB`,
	`NBR_CNT`,
	`AVG_TRA_DIFF_RATE`,
	`MAX_TRA_DIFF_RATE` FROM ',GT_DB,'.tmp_opt_aco_traffic_unbalance_lte;');
	PREPARE stmt FROM @sqlcmd;
	EXECUTE stmt;
	DEALLOCATE PREPARE stmt;		
	
	SET @SqlCmd=CONCAT('DROP TABLE  IF EXISTS ',GT_DB,'.tmp_aco_nt;');
	PREPARE stmt FROM @sqlcmd;
	EXECUTE stmt;
	DEALLOCATE PREPARE stmt;
	
	SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE  IF EXISTS ',GT_DB,'.tmp_aco_neighbor_',WORKER_ID,';');
	PREPARE stmt FROM @sqlcmd;
	EXECUTE stmt;
	DEALLOCATE PREPARE stmt;
	
	SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE  IF EXISTS ',GT_DB,'.tmp_aco_avg_tra_high_web_',WORKER_ID,';');
	PREPARE stmt FROM @sqlcmd;
	EXECUTE stmt;
	DEALLOCATE PREPARE stmt;
	
	SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE  IF EXISTS ',GT_DB,'.tmp_aco_neighbor_traffic_',WORKER_ID,';');
	PREPARE stmt FROM @sqlcmd;
	EXECUTE stmt;
	DEALLOCATE PREPARE stmt;
	
	SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE  IF EXISTS ',GT_DB,'.tmp_aco_neighbor_traffic_summary_',WORKER_ID,';');
	PREPARE stmt FROM @sqlcmd;
	EXECUTE stmt;
	DEALLOCATE PREPARE stmt;
	
	
	
	INSERT INTO gt_gw_main.SP_LOG VALUES(O_GT_DB,'Sp_Sub_Generate_ACO_Report_lte',CONCAT('Done: ',TIMESTAMPDIFF(SECOND,START_TIME,SYSDATE()),' seconds.'), NOW());
