DELIMITER $$
USE `gt_gw_main`$$
DROP PROCEDURE IF EXISTS `SP_CreateDB_LTE`$$
CREATE DEFINER=`covmo`@`%` PROCEDURE `Sp_Sub_Generate_NBO_Report`(IN GT_DB VARCHAR(100))
BEGIN
	DECLARE RNC_ID INT;
	DECLARE START_TIME DATETIME DEFAULT SYSDATE();
	DECLARE CURRENT_NT_DB VARCHAR(50) DEFAULT CONCAT('gt_nt_',gt_strtok(GT_DB,3,'_'));
	
	SELECT gt_strtok(GT_DB,2,'_') INTO RNC_ID;
	
	INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'Sp_Sub_Generate_NBO_Report','Start ', NOW());
	
	INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'Sp_Sub_Generate_NBO_Report','CREATE TABLE-tmp_nbr_intra', NOW());
	
	SET @SqlCmd=CONCAT('TRUNCATE TABLE ',GT_DB,'.opt_nbr_result_inter;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('TRUNCATE TABLE ',GT_DB,'.opt_nbr_result_intra;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	SET @SqlCmd=CONCAT('DROP TABLE IF EXISTS ',GT_DB,'.tmp_nbr_intra;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('CREATE TABLE ',GT_DB,'.tmp_nbr_intra ENGINE=MYISAM
				SELECT 
					a.rnc_id, a.cell_id, a.nbr_rnc_id, a.nbr_cell_id, a.cm_nbr_state,  
					(a.MS_RSCP_AVG*a.MS_MEASURE_COUNT) AS rscp_A,(a.MS_MEASURE_COUNT) rscp_B
					, (a.MS_ECN0_AVG*a.MS_MEASURE_COUNT) AS ecn0_A,(a.MS_MEASURE_COUNT) ecn0_B
					, (a.DISTANCE_METER) AS distance,  (a.EVENT_COUNT) AS evt_cnt
					, (a.AS_TOTAL_COUNT) AS as_total_cnt
					, (PRI_PRIORITY) AS PRI_PRIORITY,  (PRI_DISTANCE) AS PRI_DISTANCE
					, (PRI_AS) AS PRI_AS,  (PRI_EVENT) AS PRI_EVENT,  (PRI_RSCP) AS PRI_RSCP
					, (PRI_ECNO) AS PRI_ECNO 
				FROM ',GT_DB,'.opt_nbr_result a 
				WHERE a.cm_nbr_state = 0 OR a.cm_nbr_state = 1  ;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'Sp_Sub_Generate_NBO_Report','CREATE TABLE-tmp_nt_neighbor', NOW());
	
	SET @SqlCmd=CONCAT('DROP TABLE IF EXISTS ',GT_DB,'.tmp_nt_neighbor;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('CREATE TABLE ',GT_DB,'.tmp_nt_neighbor ENGINE=MYISAM
				SELECT RNC_ID, CELL_ID, NBR_RNC_ID, NBR_CELL_ID 
				FROM ',CURRENT_NT_DB,'.nt_neighbor_current 
				WHERE NBR_TYPE=1 AND RNC_ID=',RNC_ID,';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'Sp_Sub_Generate_NBO_Report','CREATE INDEX ix_tmp_nt_neighbor ON tmp_nt_neighbor', NOW());
	
	SET @SqlCmd=CONCAT('CREATE INDEX ix_tmp_nt_neighbor ON ',GT_DB,'.tmp_nt_neighbor (`rnc_id`,`cell_id`,`nbr_rnc_id`,`nbr_cell_id`);');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;	
	
	INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'Sp_Sub_Generate_NBO_Report','CREATE TABLE-tmp_opt_nbr_result', NOW());
	
	SET @SqlCmd=CONCAT('DROP TABLE IF EXISTS ',GT_DB,'.tmp_opt_nbr_result;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('CREATE TABLE ',GT_DB,'.tmp_opt_nbr_result ENGINE=MYISAM
				SELECT RNC_ID, CELL_ID, NBR_RNC_ID, NBR_CELL_ID 
				FROM ',GT_DB,'.opt_nbr_result  
				WHERE RNC_ID=',RNC_ID,';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'Sp_Sub_Generate_NBO_Report','CREATE INDEX ix_tmp_opt_nbr_result ON tmp_opt_nbr_result', NOW());
	
	SET @SqlCmd=CONCAT('CREATE INDEX ix_tmp_opt_nbr_result ON ',GT_DB,'.tmp_opt_nbr_result (`rnc_id`,`cell_id`,`nbr_rnc_id`,`nbr_cell_id`);');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT(' INSERT INTO ',GT_DB,'.tmp_nbr_intra
				SELECT B.RNC_ID, B.CELL_ID, B.NBR_RNC_ID, B.NBR_CELL_ID,1 CM_NBR_STATE
					, NULL AS rscp_A, NULL AS rscp_B
					, NULL AS ecn0_A, NULL AS ecn0_B
					, NULL AS distance
					, 0 AS evt_cnt
					, 0 AS as_total_cnt
					, 0 AS PRI_PRIORITY, 0 AS PRI_DISTANCE
					, 0 AS PRI_AS, 0 AS PRI_EVENT, 0 AS PRI_RSCP, 0 AS PRI_ECNO 
				FROM ',GT_DB,'.tmp_nt_neighbor A FORCE INDEX (ix_tmp_nt_neighbor)
				LEFT JOIN ',GT_DB,'.tmp_opt_nbr_result B FORCE INDEX (ix_tmp_opt_nbr_result)
				ON A.RNC_ID=B.RNC_ID
				AND A.CELL_ID=B.CELL_ID
				AND A.NBR_RNC_ID=B.NBR_RNC_ID
				AND A.NBR_CELL_ID=B.NBR_CELL_ID
				WHERE B.RNC_ID IS NULL ;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('DROP TABLE IF EXISTS ',GT_DB,'.tmp_table_opt_nbr_result;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
 	
	SET @SqlCmd=CONCAT('CREATE TABLE ',GT_DB,'.tmp_table_opt_nbr_result ENGINE=MYISAM
				SELECT  result.rnc_id, result.cell_id, result.nbr_rnc_id, result.nbr_cell_id
					, result.cm_nbr_state, result.rscp, result.ecn0, result.distance, result.as_total_cnt, result.evt_cnt
					, ((2*result.PRI_PRIORITY)+(2*result.PRI_DISTANCE)+result.PRI_AS+result.PRI_EVENT+result.PRI_RSCP+result.PRI_ECNO) AS priority  
					, @curRank := IF(@RNC_ID=RNC_ID AND @CELL_ID = CELL_ID , @curRank + 1, 1) AS `RANK`
					, @RNC_ID := RNC_ID AS dummy1
					, @CELL_ID := CELL_ID AS dummy2
				FROM  (
					SELECT uni.rnc_id, uni.cell_id, uni.nbr_rnc_id, uni.nbr_cell_id, uni.cm_nbr_state,  
						SUM(rscp_A)/SUM(rscp_B) rscp
						, SUM(ecn0_A)/SUM(ecn0_B) ecn0
						, AVG(distance) AS distance,  SUM(evt_cnt) AS evt_cnt
						, SUM(as_total_cnt) AS as_total_cnt
						, AVG(PRI_PRIORITY) AS PRI_PRIORITY,  AVG(PRI_DISTANCE) AS PRI_DISTANCE
						, AVG(PRI_AS) AS PRI_AS,  AVG(PRI_EVENT) AS PRI_EVENT,  AVG(PRI_RSCP) AS PRI_RSCP
						, AVG(PRI_ECNO) AS PRI_ECNO  
					FROM ',GT_DB,'.tmp_nbr_intra uni
					GROUP BY rnc_id, cell_id, nbr_rnc_id, nbr_cell_id 
				) result ,(SELECT @curRank := 0) r,(SELECT @RNC_ID:='''') s,(SELECT @CELL_ID:='''') w 
				ORDER BY result.RNC_ID,result.CELL_ID ,priority;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'Sp_Sub_Generate_NBO_Report','CREATE INDEX ix_nbr_rnc_id ON tmp_table_opt_nbr_result', NOW());
	
	SET @SqlCmd=CONCAT('CREATE INDEX ix_nbr_rnc_id ON ',GT_DB,'.tmp_table_opt_nbr_result (`nbr_rnc_id`,`nbr_cell_id`);');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'Sp_Sub_Generate_NBO_Report','CREATE TABLE-opt_nbr_result_intra', NOW());
	
	SET @SqlCmd=CONCAT('INSERT INTO ',GT_DB,'.opt_nbr_result_intra 
				(`RNC_ID`,
				     `CELL_ID`,
				     `NBR_RNC_ID`,
				     `NBR_CELL_ID`,
				     `IS_CM_NBR`,
				     `RSCP`,
				     `ECN0`,
				     `DISTANCE`,
				     `AS_COUNT`,
				     `EVENT_COUNT`,
				     `RANK`,
				     `longitude`,
				     `latitude`,
				     `height`)
				SELECT result.rnc_id AS RNC_ID, 
					result.cell_id AS CELL_ID, 
					result.nbr_rnc_id AS NBR_RNC_ID, 
					result.nbr_cell_id AS NBR_CELL_ID, 
					IF(result.cm_nbr_state=1, ''yes'', ''no'') AS IS_CM_NBR, 
					result.rscp AS RSCP, 
					result.ecn0 AS ECN0, 
					result.distance AS DISTANCE, 
					result.as_total_cnt AS AS_COUNT, 
					result.evt_cnt AS EVENT_COUNT, 
					result.rank AS RANK, 
					nt.LONGITUDE AS longitude, nt.LATITUDE AS latitude, 1000 AS height  
				FROM ',GT_DB,'.tmp_table_opt_nbr_result result  
				LEFT JOIN ',CURRENT_NT_DB,'.nt_current nt 
				ON nt.rnc_id = result.nbr_rnc_id AND nt.cell_id = result.nbr_cell_id  
				ORDER BY RANK ;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
		
	INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'Sp_Sub_Generate_NBO_Report','CREATE TABLE-opt_nbr_result_inter', NOW());
	
	
	SET @SqlCmd=CONCAT('DROP TABLE IF EXISTS ',GT_DB,'.tmp_opt_nbr_result_inter;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	SET @SqlCmd=CONCAT('CREATE TABLE ',GT_DB,'.tmp_opt_nbr_result_inter ENGINE=MYISAM
				SELECT  rk.rnc_id AS RNC_ID, rk.cell_id AS CELL_ID,  
					rk.nbr_rnc_id AS NBR_RNC_ID, rk.nbr_cell_id AS NBR_CELL_ID
					, IF(rk.nbr_type = 2, ''CM Inter'', ''N/A'') AS NEIGHBOR_TYPE
					, rk.non_sec AS DETECT_COUNT, rk.co_sec AS CO_SECTOR_COUNT
					, rk.LONGITUDE AS longitude, rk.LATITUDE AS latitude, rk.height	
					, @curRank := IF(@RNC_ID=rk.RNC_ID AND @CELL_ID = rk.CELL_ID , @curRank + 1, 1) AS `RANK`
					, @RNC_ID := rk.RNC_ID AS dummy1
					, @CELL_ID := rk.CELL_ID AS dummy2  
				FROM 
				(
					SELECT opt.RNC_ID, opt.CELL_ID, opt.NBR_RNC_ID, opt.NBR_CELL_ID, opt.NBR_TYPE, opt.ifho
						, opt.non_sec, opt.co_sec
						, nt.LONGITUDE AS longitude, nt.LATITUDE AS latitude, 3000 AS height
					FROM
					(
						SELECT RNC_ID, CELL_ID, NBR_RNC_ID, NBR_CELL_ID, NBR_TYPE
							, SUM(ifho) AS ifho
							, SUM(non_sec) AS non_sec
							, SUM(co_sec) AS co_sec 
						FROM	(
								SELECT RNC_ID, CELL_ID, NBR_RNC_ID, NBR_CELL_ID, NBR_TYPE
									, (IFHO_CNT_NON_SEC+IFHO_CNT) AS ifho
									, (IFHO_CNT_NON_SEC) AS non_sec
									, (IFHO_CNT) AS co_sec 
								FROM ',GT_DB,'.opt_inter_ifho  
								WHERE nbr_type=0 OR nbr_type=2
								UNION ALL 
								SELECT B.RNC_ID, B.CELL_ID, B.NBR_RNC_ID, B.NBR_CELL_ID, B.NBR_TYPE
									, 0 AS ifho
									, 0 AS non_sec
									, 0 AS co_sec 
								FROM ',CURRENT_NT_DB,'.nt_neighbor_current B FORCE INDEX (IX_NBR_TYPE)
								LEFT JOIN ',GT_DB,'.opt_inter_ifho A 
								ON B.RNC_ID=A.RNC_ID 
								AND B.CELL_ID=A.CELL_ID 
								AND B.NBR_RNC_ID=A.NBR_RNC_ID 
								AND B.NBR_CELL_ID=A.NBR_CELL_ID 
								WHERE B.NBR_TYPE=2 AND B.RNC_ID=',RNC_ID,' AND A.RNC_ID IS NULL
						) AA
						GROUP BY RNC_ID, CELL_ID,NBR_RNC_ID, NBR_CELL_ID
					) opt, ',CURRENT_NT_DB,'.nt_current nt  
					WHERE opt.nbr_cell_id = nt.cell_id AND opt.nbr_rnc_id = nt.rnc_id  
				) rk,(SELECT @curRank := 0) r,(SELECT @RNC_ID:='''') s,(SELECT @CELL_ID:='''') w 
				ORDER BY rk.RNC_ID,rk.CELL_ID,rk.ifho DESC, rk.NBR_CELL_ID
				;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
		
	SET @SqlCmd=CONCAT('INSERT INTO ',GT_DB,'.opt_nbr_result_inter 
					(`RNC_ID`,
					     `CELL_ID`,
					     `NBR_RNC_ID`,
					     `NBR_CELL_ID`,
					     `NEIGHBOR_TYPE`,
					     `DETECT_COUNT`,
					     `CO_SECTOR_COUNT`,
					     `RANK`,
					     `longitude`,
					     `latitude`,
					     `height`)	
				SELECT result.rnc_id AS RNC_ID, result.cell_id AS CELL_ID,  
					result.nbr_rnc_id AS NBR_RNC_ID, result.nbr_cell_id AS NBR_CELL_ID
					, result.NEIGHBOR_TYPE , result.DETECT_COUNT AS DETECT_COUNT
					, result.CO_SECTOR_COUNT AS CO_SECTOR_COUNT,  result.rank AS RANK
					, result.LONGITUDE AS longitude, result.LATITUDE AS latitude, result.height AS height
				FROM ',GT_DB,'.tmp_opt_nbr_result_inter result ;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	
	SET @SqlCmd=CONCAT('DROP TABLE IF EXISTS ',GT_DB,'.tmp_nbr_intra;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('DROP TABLE IF EXISTS ',GT_DB,'.tmp_nt_neighbor;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('DROP TABLE IF EXISTS ',GT_DB,'.tmp_opt_nbr_result;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('DROP TABLE IF EXISTS ',GT_DB,'.tmp_table_opt_nbr_result;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('DROP TABLE IF EXISTS ',GT_DB,'.tmp_opt_nbr_result_inter;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'Sp_Sub_Generate_NBO_Report',CONCAT('Done: ',TIMESTAMPDIFF(SECOND,START_TIME,SYSDATE()),' seconds.'), NOW());
	
END$$
DELIMITER ;
