DELIMITER $$
USE `gt_gw_main`$$
DROP PROCEDURE IF EXISTS `SP_CreateDB_LTE`$$
CREATE DEFINER=`covmo`@`%` PROCEDURE `SP_Generate_Report_Main_LTE`(IN GT_DB VARCHAR(100), IN KIND VARCHAR(20), IN VENDOR_SOURCE VARCHAR(20), IN note VARCHAR(500),IN GT_COVMO VARCHAR(100))
BEGIN
	
	DECLARE START_TIME DATETIME DEFAULT SYSDATE();
	DECLARE SH_EH VARCHAR(9) DEFAULT RIGHT(GT_DB,9);
	DECLARE O_GT_DB VARCHAR(100) DEFAULT GT_DB;
	DECLARE PU_ID INT;
	DECLARE CURRENT_NT_DB VARCHAR(50) DEFAULT CONCAT('gt_nt_',gt_strtok(GT_DB,3,'_'));
	DECLARE STARTHOUR VARCHAR(2) DEFAULT SUBSTRING(RIGHT(GT_DB,18),10,2);
	DECLARE START_HOUR VARCHAR(2) DEFAULT SUBSTRING(RIGHT(GT_DB,18),10,2);
	DECLARE ENDHOUR VARCHAR(2) DEFAULT IF(SUBSTRING(RIGHT(GT_DB,18),15,2)='00','24',SUBSTRING(RIGHT(GT_DB,18),15,2));
	
	
	DECLARE v_DATA_DATE VARCHAR(30);
	DECLARE WeekStart CHAR(20);
	DECLARE WeekEnd CHAR(20);
	DECLARE	WEEK_DB VARCHAR(50);
	DECLARE TAC_REPORT_FLAG VARCHAR(10);
	DECLARE PM_COUNTER_FLAG VARCHAR(10);
	
	INSERT INTO gt_gw_main.SP_LOG VALUES(O_GT_DB,'SP_Generate_Report_Main_LTE','Start', START_TIME);
	
	SELECT LOWER(`value`) INTO TAC_REPORT_FLAG  FROM gt_gw_main.integration_param WHERE gt_group = 'sp' AND gt_name = '01.tac.report' ;
	SELECT LOWER(`value`) INTO PM_COUNTER_FLAG FROM gt_gw_main.integration_param WHERE gt_group = 'sp' AND gt_name = 'pm_counter';
	SELECT gt_strtok(GT_DB,2,'_') INTO PU_ID;
	SELECT REPLACE(GT_DB,SH_EH,'0000_0000') INTO GT_DB;
	
	SET v_DATA_DATE = STR_TO_DATE(gt_strtok(GT_DB,3,'_'),'%Y%m%d');
	SET WeekStart = FIRST_DAY_OF_WEEK(v_DATA_DATE);
	SET WeekEnd = LAST_DAY_OF_WEEK(v_DATA_DATE);
	SET WEEK_DB=CONCAT('gt_',PU_ID,'_',DATE_FORMAT(WeekStart,'%Y%m%d'),'_',DATE_FORMAT(WeekEnd,'%Y%m%d'));
			
	IF KIND IN ('DAILY','RERUN') THEN 
		IF PM_COUNTER_FLAG = 'true' THEN
			CALL GT_GW_MAIN.SP_PM_COUNTER_AGGR(O_GT_DB, CURRENT_NT_DB, 4);
		END IF;
		
		IF TAC_REPORT_FLAG = 'true' THEN 
			CALL gt_gw_main.SP_Sub_Generate_TAC_End_LTE(O_GT_DB);
			CALL gt_gw_main.SP_Sub_Generate_TAC_Start_LTE(O_GT_DB);
		END IF;
		
			
		SET @SqlCmd=CONCAT(' DELETE FROM  ',GT_DB,'.table_call_cnt
				WHERE PU_ID = ',PU_ID,' AND 
				DATA_HOUR >= ',STARTHOUR,' AND DATA_HOUR < ',ENDHOUR,';
				');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		
		SET @SqlCmd=CONCAT(' INSERT INTO  ',GT_DB,'.table_call_cnt
				(DATA_DATE,DATA_HOUR,PU_ID,SERVICETYPE,TOT_CALL_CNT,TECH_MASK,NOTE)
				SELECT DATA_DATE,DATA_HOUR,',PU_ID, ' AS PU_ID ,
				CASE WHEN CALL_TYPE = ''21'' THEN ''PS'' WHEN CALL_TYPE = ''22'' THEN ''SG'' WHEN CALL_TYPE = ''23'' THEN ''VoLTE'' ELSE ''Unidentified'' END AS CALL_TYPE
				,SUM(SERVING_CNT) AS TOT_CALL_CNT
				,4 AS TECH_MASK
				,''',note,'''
				FROM  ',GT_DB,'.rpt_cell_end_',STARTHOUR,' A
				WHERE DATA_HOUR >= ',STARTHOUR,' AND DATA_HOUR < ',ENDHOUR,'
				GROUP BY  DATA_DATE,DATA_HOUR,CASE WHEN CALL_TYPE = ''21'' THEN ''PS'' WHEN CALL_TYPE = ''22'' THEN ''SG'' WHEN CALL_TYPE = ''23'' THEN ''VoLTE'' ELSE ''Unidentified'' END;
				');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		
		
		SET @SqlCmd=CONCAT(' REPLACE INTO  gt_gw_main.table_call_cnt
				(DATA_DATE,DATA_HOUR,PU_ID,SERVICETYPE,TOT_CALL_CNT,TECH_MASK,NOTE)
				SELECT DATA_DATE,DATA_HOUR,PU_ID,SERVICETYPE,TOT_CALL_CNT,TECH_MASK,NOTE
				FROM  ',GT_DB,'.table_call_cnt
				WHERE PU_ID = ',PU_ID,' AND 
				DATA_HOUR >= ',STARTHOUR,' AND DATA_HOUR < ',ENDHOUR,';
				');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		
		INSERT INTO gt_gw_main.sp_log VALUES(O_GT_DB,'SP_Generate_Report_Main_LTE','NBO NBR_TYPE', NOW());
	
-- 		SELECT VALUE INTO @nt2_flag FROM gt_gw_main.integration_param WHERE gt_group='sp' AND gt_name='nt2' ;
-- 		IF @nt2_flag='true' THEN
			
		SET @SqlCmd=CONCAT('UPDATE ',GT_DB,'.opt_nbr_inter_intra_lte_',START_HOUR,' a 
		LEFT JOIN ',CURRENT_NT_DB,'.nt2_cell_lte b ON a.enodeb_id = b.enodeb_id AND a.cell_id=b.cell_id 
		LEFT JOIN ',CURRENT_NT_DB,'.nt2_cell_lte c ON a.nbr_enodeb_id = c.enodeb_id AND a.nbr_cell_id=c.cell_id
		LEFT JOIN ',CURRENT_NT_DB,'.nt2_nbr_4_4_lte d ON a.cell_id = d.cell_id AND a.enodeb_id = d.enodeb_id AND a.nbr_enodeb_id = d.nbr_enodeb_id AND a.nbr_cell_id=d.nbr_cell_id 
		SET a.nbr_type = IF(b.dl_earfcn = c.dl_earfcn, IF(ISNULL(d.enodeb_id), 10,1), IF(ISNULL(d.cell_id), 20,2)); ');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
	
-- 		ELSE
-- 		
-- 			SET @SqlCmd=CONCAT('UPDATE ',GT_DB,'.opt_nbr_inter_intra_lte_',START_HOUR,' A, ',CURRENT_NT_DB,'.nt_cell_current_lte B,',CURRENT_NT_DB,'.nt_cell_current_lte C
-- 			SET A.NBR_TYPE = CASE WHEN B.DL_EARFCN = C.DL_EARFCN THEN 20 ELSE 10 END 
-- 			WHERE A.ENODEB_ID = B.ENODEB_ID AND A.CELL_ID = B.CELL_ID AND A.NBR_ENODEB_ID = C.ENODEB_ID AND A.NBR_CELL_ID = C.CELL_ID;');
-- 			PREPARE Stmt FROM @SqlCmd;
-- 			EXECUTE Stmt;
-- 			DEALLOCATE PREPARE Stmt;
-- 			
-- 			SET @SqlCmd=CONCAT('UPDATE ',GT_DB,'.opt_nbr_inter_intra_lte_',START_HOUR,' A, ',CURRENT_NT_DB,'.nt_nbr_4_4_current_lte B 
-- 			SET A.NBR_TYPE = B.NBR_TYPE
-- 			WHERE A.ENODEB_ID = B.ENODEB_ID AND A.CELL_ID = B.CELL_ID AND A.NBR_ENODEB_ID = B.NBR_ENODEB_ID AND A.NBR_CELL_ID = B.NBR_CELL_ID;
-- 			');
-- 			PREPARE Stmt FROM @SqlCmd;
-- 			EXECUTE Stmt;
-- 			DEALLOCATE PREPARE Stmt;
-- 	
-- 		END IF;
	
	END IF;
	
	IF KIND = 'WEEK' THEN 
	
		CALL gt_gw_main.SP_Sub_Generate_Overshooting_Severity_LTE(WEEK_DB,GT_COVMO,'WEEK');
	END IF ;
	
	INSERT INTO gt_gw_main.SP_LOG VALUES(O_GT_DB,'SP_Generate_Report_Main_LTE',CONCAT('Done: ',TIMESTAMPDIFF(SECOND,START_TIME,SYSDATE()),' seconds.'), NOW());
	
	
	
END$$
DELIMITER ;
