DELIMITER $$
USE `gt_gw_main`$$
DROP PROCEDURE IF EXISTS `SP_CreateDB_LTE`$$
CREATE DEFINER=`covmo`@`%` PROCEDURE `SP_Sub_Update_Position`(IN GT_DB VARCHAR(100),IN GT_COVMO VARCHAR(100))
BEGIN
	DECLARE GT_DB_START_HOUR VARCHAR(10) DEFAULT SUBSTRING(RIGHT(GT_DB,18),10,4);
	DECLARE START_TIME DATETIME DEFAULT SYSDATE();
	DECLARE CURRENT_NT_DB VARCHAR(50) DEFAULT CONCAT('gt_nt_',gt_strtok(GT_DB,3,'_'));
	
	SET SESSION max_heap_table_size = 1024*1024*1024*4; 
        SET SESSION tmp_table_size = 1024*1024*1024*4; 
        SET SESSION join_buffer_size = 1024*1024*1024; 
        SET SESSION sort_buffer_size = 1024*1024*1024; 
        SET SESSION read_buffer_size = 1024*1024*1024; 
	
	SET @SqlCmd=CONCAT('DROP TABLE IF EXISTS ',GT_DB,'.tmp_table_indoor_prob;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;	
	
	INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'SP_Sub_Update_Position_New','CREATE TABLE-tmp_table_indoor_prob', NOW());
	
	SET @SqlCmd=CONCAT('	CREATE TABLE ',GT_DB,'.`tmp_table_indoor_prob` 
					(
					  `RSCP` SMALLINT DEFAULT NULL,
					  `PG_DELAY` SMALLINT DEFAULT NULL,
					  `RSCP_CNT` INT(10) DEFAULT NULL,
					  `RSCP_SUM` INT(10) DEFAULT NULL,
					  `PG_AVG` DOUBLE DEFAULT NULL,
					  `PG_STDEV` DOUBLE DEFAULT NULL,
					  `INDOOR_POISSON` DOUBLE DEFAULT NULL,
					  `OUTDOOR_POISSON` DOUBLE DEFAULT NULL,
					  `INDOOR_PROB` SMALLINT DEFAULT NULL,
					  KEY `PG_DELAY` (`PG_Delay`,`RSCP`)
					) ENGINE=MYISAM ');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt; 
	
	INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'SP_Sub_Update_Position_New','CREATE TABLE-tmp_table_indoor_prob_pre', NOW());
	
	SET @SqlCmd=CONCAT('DROP TABLE IF EXISTS ',GT_DB,'.tmp_table_indoor_prob_pre;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;		
	
	SET @SqlCmd=CONCAT('	CREATE TABLE ',GT_DB,'.`tmp_table_indoor_prob_pre` ENGINE=MYISAM
				SELECT * FROM ',GT_DB,'.table_position FORCE INDEX(seq_id)
				WHERE SEQ_ID=1 AND MOVING=0; ');
				
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt; 
	
	SET @SqlCmd=CONCAT('CREATE INDEX IX_RNC_CELL_ID ON ',GT_DB,'.tmp_table_indoor_prob_pre (`RNC_ID`,`CELL_ID`);');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
			
	SET @SqlCmd =CONCAT('SELECT COUNT(*) INTO @INDEX_FLAG  FROM INFORMATION_SCHEMA.STATISTICS 
				WHERE table_schema =  ''',CURRENT_NT_DB, ''' 
				AND table_name = ''nt_current'' AND index_name = ''IX_INDOOR'''); 
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	IF @INDEX_FLAG=0 THEN 
		SET @SqlCmd=CONCAT('CREATE INDEX IX_INDOOR ON ',CURRENT_NT_DB,'.nt_current (`INDOOR`);');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
	END IF ;
	
	INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'SP_Sub_Update_Position_New','CREATE TABLE-tmp_nt_pre', NOW());
	
	SET @SqlCmd=CONCAT('DROP TABLE IF EXISTS ',GT_DB,'.tmp_nt_pre;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('CREATE TABLE  ',GT_DB,'.tmp_nt_pre
			    (
				`RNC_ID` MEDIUMINT(9) NOT NULL,
				`CELL_ID` MEDIUMINT(9) NOT NULL,
				PRIMARY KEY (`RNC_ID`,`CELL_ID`)
				) ENGINE=MYISAM ');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;	
	
	SET @SqlCmd=CONCAT('INSERT INTO ',GT_DB,'.tmp_nt_pre 
			    SELECT DISTINCT RNC_ID,CELL_ID FROM  ',CURRENT_NT_DB,'.NT_CURRENT  FORCE INDEX (IX_INDOOR) 	 	
			    WHERE INDOOR<>1;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;	
	
	INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'SP_Sub_Update_Position_New','CREATE TABLE-tmp_table_indoor_prob_pre_2', NOW());
	
	SET @SqlCmd=CONCAT('DROP TABLE IF EXISTS ',GT_DB,'.tmp_table_indoor_prob_pre_2;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;	
	
	SET @SqlCmd=CONCAT('CREATE TABLE ',GT_DB,'.tmp_table_indoor_prob_pre_2 ENGINE=MYISAM 
			    SELECT A.* FROM  ',GT_DB,'.tmp_table_indoor_prob_pre A FORCE INDEX (IX_RNC_CELL_ID),  ',GT_DB,'.tmp_nt_pre B 	 	
			    WHERE A.RNC_ID =B.RNC_ID AND A.CELL_ID =B.CELL_ID  ;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'SP_Sub_Update_Position_New','INSERT DATA TO tmp_table_indoor_prob', NOW());
	
	SET @SqlCmd=CONCAT('	INSERT INTO ',GT_DB,'.`tmp_table_indoor_prob` 
				(RSCP,PG_DELAY,RSCP_CNT ,RSCP_SUM )
				SELECT RSCP 
					,CASE WHEN PROPAGATION_DELAY <8 THEN PROPAGATION_DELAY
					      ELSE 8
					 END AS PG_DELAY
					,COUNT(*) AS RSCP_CNT 
					,SUM(RSCP) AS RSCP_SUM 
				FROM ',GT_DB,'.tmp_table_indoor_prob_pre_2
				GROUP BY RSCP,
					CASE WHEN PROPAGATION_DELAY <8 THEN PROPAGATION_DELAY
					      ELSE 8
					END  
				ORDER BY NULL;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt; 
	
	INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'SP_Sub_Update_Position_New','UPDATE PG_AVG of tmp_table_indoor_prob', NOW());
	
	SET @SqlCmd=CONCAT('	UPDATE ',GT_DB,'.`tmp_table_indoor_prob` a,  
				(SELECT PG_DELAY
					-- , SUM(RSCP_CNT) AS RSCPTOTCNT
					-- , SUM(RSCP_SUM) AS RSCPTOTSUM
					, SUM(RSCP_SUM)/SUM(RSCP_CNT) AS PG_AVG 
				FROM ',GT_DB,'.tmp_table_indoor_prob
				GROUP BY PG_DELAY
				) b
				SET a.PG_AVG = b.PG_AVG
				WHERE a.PG_DELAY = b.PG_DELAY;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt; 
	DEALLOCATE PREPARE Stmt; 
	
	INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'SP_Sub_Update_Position_New','UPDATE PG_STDEV of tmp_table_indoor_prob', NOW());
	
	SET @SqlCmd=CONCAT('UPDATE ',GT_DB,'.`tmp_table_indoor_prob` a,  	 
				(
				SELECT PG_DELAY
					-- , SUM((RSCP-PG_AVG)*(RSCP-PG_AVG)*RSCP_CNT) AS STDSUM 
					-- , SUM(RSCP_CNT) AS RSCPTOTCNT 
					, SQRT(SUM((RSCP-PG_AVG)*(RSCP-PG_AVG)*RSCP_CNT) / SUM(RSCP_CNT)) AS PGSTDEV
				FROM ',GT_DB,'.tmp_table_indoor_prob
				GROUP BY PG_DELAY
				) b
				SET a.PG_STDEV = b.PGSTDEV
				WHERE a.PG_DELAY = b.PG_DELAY;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt; 
	DEALLOCATE PREPARE Stmt; 
	
	INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'SP_Sub_Update_Position_New','UPDATE INDOOR_POISSON, OUTDOOR_POISSON of tmp_table_indoor_prob', NOW());
	
	SET @SqlCmd=CONCAT('UPDATE ',GT_DB,'.`tmp_table_indoor_prob` a,  	 
				(
				SELECT `RSCP`
					,`PG_Delay`
					 ,(1/(PG_STDEV*SQRT(2*PI())))*EXP(-0.5*((RSCP-(PG_AVG+10))/PG_STDEV)*((RSCP-(PG_AVG+10))/PG_STDEV)) AS OUTDOOR_POISSON
					 ,(1/(PG_STDEV*SQRT(2*PI())))*EXP(-0.5*((RSCP-(PG_AVG ))/PG_STDEV)*((RSCP-(PG_AVG))/PG_STDEV)) AS INDOOR_POISSON
				FROM  ',GT_DB,'.tmp_table_indoor_prob 
				) b
				SET a.INDOOR_POISSON = b.INDOOR_POISSON
				, a.OUTDOOR_POISSON = b.OUTDOOR_POISSON
				WHERE a.`RSCP` = b.`RSCP`
				AND a.`PG_Delay`=b.`PG_Delay`
				;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt; 
	DEALLOCATE PREPARE Stmt; 
	
	SET @SqlCmd=CONCAT('UPDATE ',GT_DB,'.`tmp_table_indoor_prob` a,  	 
				(
					SELECT RSCP,PG_Delay, INDOOR_POISSON*INDOOR_POISSON*INDOOR_POISSON/(INDOOR_POISSON*INDOOR_POISSON*INDOOR_POISSON+OUTDOOR_POISSON*OUTDOOR_POISSON*OUTDOOR_POISSON)*100 AS INDOOR_PROB 
					FROM ',GT_DB,'.tmp_table_indoor_prob
				) b
				SET a.INDOOR_PROB = b.INDOOR_PROB
				WHERE a.`RSCP` = b.`RSCP`
				AND a.`PG_Delay`=b.`PG_Delay`
				;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt; 
	DEALLOCATE PREPARE Stmt; 
	
	SET @SqlCmd=CONCAT('DROP TABLE IF EXISTS ',GT_DB,'.tmp_table_pos_list;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('CREATE TABLE ',GT_DB,'.`tmp_table_pos_list` ENGINE=MYISAM 
				SELECT DISTINCT CASE WHEN PROPAGATION_DELAY <8 THEN PROPAGATION_DELAY
					ELSE 8
				END AS PG_DELAY
				,CEIL(RSCP + SIGN(INDOOR_GAIN_ANGLE)*SQRT(ABS(INDOOR_GAIN_ANGLE))+SIGN(INDOOR_GAIN_DISTANCE)*SQRT(ABS(INDOOR_GAIN_DISTANCE))) AS NEW_RSCP
				FROM ',GT_DB,'.table_position
				WHERE SEQ_ID=1 AND MOVING=0
				UNION
				SELECT DISTINCT CASE WHEN PROPAGATION_DELAY <8 THEN PROPAGATION_DELAY
					ELSE 8
				END AS PG_DELAY
				,RSCP
				FROM ',GT_DB,'.table_position
				WHERE SEQ_ID=1 AND MOVING=0;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt; 
	DEALLOCATE PREPARE Stmt; 
	
	SET @SqlCmd=CONCAT('DROP TABLE IF EXISTS ',GT_DB,'.tmp_table_pos_ul_dl;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('	CREATE TABLE ',GT_DB,'.`tmp_table_pos_ul_dl` ENGINE=MYISAM 
				SELECT `PG_DELAY`,MIN(RSCP) AS UL_RSCP ,MAX(INDOOR_PROB) AS UL_PROB ,MAX(RSCP)AS DL_RSCP , MIN(INDOOR_PROB) AS DL_PROB 
				FROM ',GT_DB,'.tmp_table_indoor_prob
				GROUP BY `PG_DELAY`; ');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt; 
	DEALLOCATE PREPARE Stmt; 
	
	SET @SqlCmd=CONCAT('DROP TABLE IF EXISTS ',GT_DB,'.tmp_table_pos_all;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('	CREATE TABLE ',GT_DB,'.`tmp_table_pos_all` ENGINE=MYISAM 
				SELECT  @rownum := @rownum + 1 AS num, c. * 
				FROM (
					SELECT  A.*, B.INDOOR_PROB 
					FROM ',GT_DB,'.tmp_table_pos_list  A 
					LEFT JOIN  ',GT_DB,'.tmp_table_indoor_prob B
					ON A.PG_DELAY=B.PG_DELAY
					AND A.NEW_RSCP=B.RSCP
					ORDER BY PG_DELAY,NEW_RSCP
					)c,(SELECT @rownum := 0) d; ');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt; 
	DEALLOCATE PREPARE Stmt; 
	
	SET @SqlCmd=CONCAT('	UPDATE ',GT_DB,'.`tmp_table_pos_all` A, ',GT_DB,'. tmp_table_pos_ul_dl B
				SET A.INDOOR_PROB=B.UL_PROB
				WHERE A.`PG_DELAY`=B.`PG_DELAY`
				AND A.NEW_RSCP < UL_RSCP ; ');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt; 
	DEALLOCATE PREPARE Stmt; 
	
	SET @SqlCmd=CONCAT('	UPDATE ',GT_DB,'.`tmp_table_pos_all` A,  ',GT_DB,'. tmp_table_pos_ul_dl B
				SET A.INDOOR_PROB=B.DL_PROB
				WHERE A.`PG_DELAY`=B.`PG_DELAY`
				AND A.NEW_RSCP > DL_RSCP ; ');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt; 
	DEALLOCATE PREPARE Stmt; 		
	
	SET @SqlCmd=CONCAT('DROP TABLE IF EXISTS ',GT_DB,'.tmp_table_pos_makeup;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT(' CREATE TABLE ',GT_DB,'.tmp_table_pos_makeup ENGINE=MYISAM
				SELECT  
				target.num AS NUM,
				target.new_rscp AS RSCP,
				pre.new_rscp AS PRE_RSCP,
				pre.indoor_prob AS PRE_PROB,
				nex.new_rscp AS NEXT_RSCP,
				nex.indoor_prob AS NEXT_PROB,
				CASE (pre.new_rscp-nex.new_rscp) WHEN 0 THEN nex.indoor_prob ELSE pre.indoor_prob-((pre.indoor_prob - nex.indoor_prob)* (pre.new_rscp-target.new_rscp)/(pre.new_rscp-nex.new_rscp)) END AS NEW_INDOOR_PROB
				FROM 
					(SELECT a.num AS target, MAX(b.num) AS pre, MIN(c.num) AS nex 
					 FROM 
						',GT_DB,'.tmp_table_pos_all a,
						',GT_DB,'.tmp_table_pos_all b,
						',GT_DB,'.tmp_table_pos_all c
					WHERE a.INDOOR_PROB IS NULL
					AND b.INDOOR_PROB IS NOT NULL
					AND c.INDOOR_PROB IS NOT NULL
					AND b.num < a.num
					AND c.num > a.num
					GROUP BY a.num
					) tt
				LEFT JOIN ',GT_DB,'.tmp_table_pos_all target 
				ON target.num = tt.target
				LEFT JOIN ',GT_DB,'.tmp_table_pos_all pre 
				ON pre.num = tt.pre
				LEFT JOIN ',GT_DB,'.tmp_table_pos_all nex 
				ON nex.num = tt.nex; ');	
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt; 
	DEALLOCATE PREPARE Stmt; 
	
	SET @SqlCmd=CONCAT('	UPDATE ',GT_DB,'.`tmp_table_pos_all` A,  ',GT_DB,'.tmp_table_pos_makeup B
				SET A.INDOOR_PROB=B.NEW_INDOOR_PROB
				WHERE A.NUM=B.NUM ; ');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt; 
	DEALLOCATE PREPARE Stmt; 	
			
	SET @SqlCmd =CONCAT('SELECT COUNT(*) INTO @INDEX_FLAG2  FROM INFORMATION_SCHEMA.STATISTICS 
				WHERE table_schema =  ''',GT_DB, ''' 
				AND table_name = ''tmp_table_pos_all'' AND index_name = ''IX_RSCP_PG'''); 
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	IF @INDEX_FLAG2=0 THEN 
		SET @SqlCmd=CONCAT('CREATE INDEX IX_RSCP_PG ON ',GT_DB,'.tmp_table_pos_all (`PG_DELAY`,`NEW_RSCP`);');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
	END IF ;
		
	INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'SP_Sub_Update_Position_New','UPDATE INDOOR of table_position AS NULL', NOW());
	
	SET @SqlCmd=CONCAT('UPDATE  ',GT_DB,'.table_position
			    SET INDOOR=NULL;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'SP_Sub_Update_Position_New','UPDATE INDOOR of table_position', NOW());
	
	SET @SqlCmd=CONCAT('	UPDATE ',GT_DB,'.`table_position` A1 FORCE INDEX (SEQ_ID) , ',GT_DB,'.tmp_table_pos_all B1 FORCE INDEX (IX_RSCP_PG)
				SET A1.INDOOR=B1.INDOOR_PROB
				WHERE A1.SEQ_ID=1
				AND IF(A1.`PROPAGATION_DELAY`>8,8,A1.`PROPAGATION_DELAY` )=B1.`PG_DELAY`
				AND A1.`RSCP`=B1.`NEW_RSCP`	;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt; 
	DEALLOCATE PREPARE Stmt; 
	
	INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'SP_Sub_Update_Position_New','UPDATE BATCH', NOW());
	
	SET @SqlCmd=CONCAT('	UPDATE ',GT_DB,'.`table_position`
				SET BATCH=',GT_DB_START_HOUR,';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt; 
	DEALLOCATE PREPARE Stmt; 
	
	SET @SqlCmd=CONCAT('DROP TABLE IF EXISTS ',GT_DB,'.tmp_table_indoor_prob;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('DROP TABLE IF EXISTS ',GT_DB,'.tmp_table_indoor_prob_pre;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('DROP TABLE IF EXISTS ',GT_DB,'.tmp_nt_pre;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('DROP TABLE IF EXISTS ',GT_DB,'.tmp_table_indoor_prob_pre_2;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('DROP TABLE IF EXISTS ',GT_DB,'.tmp_table_pos_list;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('DROP TABLE IF EXISTS ',GT_DB,'.tmp_table_pos_ul_dl;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('DROP TABLE IF EXISTS ',GT_DB,'.tmp_table_pos_all;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('DROP TABLE IF EXISTS ',GT_DB,'.tmp_table_pos_makeup;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'SP_Sub_Update_Position_New',CONCAT('Done: ',TIMESTAMPDIFF(SECOND,START_TIME,SYSDATE()),' seconds.'), NOW());
	
	SET SESSION max_heap_table_size = 1024*1024*128; 
	SET SESSION tmp_table_size = 1024*1024*128; 
	SET SESSION join_buffer_size = 1024*1024*128; 
	SET SESSION sort_buffer_size = 1024*1024*128; 
	SET SESSION read_buffer_size = 1024*1024*128; 
	
END$$
DELIMITER ;
