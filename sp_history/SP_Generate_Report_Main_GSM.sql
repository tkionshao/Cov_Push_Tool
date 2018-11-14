DELIMITER $$
USE `gt_gw_main`$$
DROP PROCEDURE IF EXISTS `SP_CreateDB_LTE`$$
CREATE DEFINER=`covmo`@`%` PROCEDURE `SP_Generate_Report_Main_GSM`(IN GT_DB VARCHAR(100), IN KIND VARCHAR(20), IN VENDOR_SOURCE VARCHAR(20), IN note VARCHAR(500),IN GT_COVMO VARCHAR(100))
BEGIN
	
	DECLARE START_TIME DATETIME DEFAULT SYSDATE();
	DECLARE SH_EH VARCHAR(9) DEFAULT RIGHT(GT_DB,9);
	DECLARE O_GT_DB VARCHAR(100) DEFAULT GT_DB;
	DECLARE PU_ID INT;
	DECLARE STARTHOUR VARCHAR(2) DEFAULT SUBSTRING(RIGHT(GT_DB,18),10,2);
	DECLARE ENDHOUR VARCHAR(2) DEFAULT IF(SUBSTRING(RIGHT(GT_DB,18),15,2)='00','24',SUBSTRING(RIGHT(GT_DB,18),15,2));
	
	INSERT INTO gt_gw_main.SP_LOG VALUES(O_GT_DB,'SP_Generate_Report_Main_GSM','Start', START_TIME);
	
	SELECT gt_strtok(GT_DB,2,'_') INTO PU_ID;
	SELECT REPLACE(GT_DB,SH_EH,'0000_0000') INTO GT_DB;
	
	
	CALL gt_gw_main.SP_Sub_Generate_Start_GSM(O_GT_DB,KIND,VENDOR_SOURCE,GT_COVMO);
	CALL gt_gw_main.SP_Sub_Generate_End_GSM(O_GT_DB,KIND,VENDOR_SOURCE,GT_COVMO);
	CALL gt_gw_main.SP_Sub_Generate_FP_GSM(O_GT_DB,KIND,VENDOR_SOURCE,GT_COVMO);
	CALL gt_gw_main.SP_Sub_Generate_Dominant_GSM(O_GT_DB,KIND,VENDOR_SOURCE,GT_COVMO);
	
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
    CASE 
    WHEN CALL_TYPE = ''10'' THEN ''VOICE'' 
    WHEN CALL_TYPE = ''11'' THEN ''CS'' 
    WHEN CALL_TYPE = ''13'' THEN ''PS'' 
    WHEN CALL_TYPE = ''14'' THEN ''Multi RAB'' 
    WHEN CALL_TYPE = ''15'' THEN ''Signalling'' 
    WHEN CALL_TYPE = ''16'' THEN ''SMS'' 
    WHEN CALL_TYPE = ''99'' THEN ''Other'' 
    ELSE ''Unidentified'' END AS CALL_TYPE
    ,COUNT(*) AS TOT_CALL_CNT
    ,1 AS TECH_MASK,''',note,'''
    FROM  ',GT_DB,'.table_call_gsm A
    WHERE DATA_HOUR >= ',STARTHOUR,' AND DATA_HOUR < ',ENDHOUR,'
    GROUP BY  DATA_DATE,DATA_HOUR,CALL_TYPE;
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
	
	INSERT INTO gt_gw_main.SP_LOG VALUES(O_GT_DB,'SP_Generate_Report_Main_GSM',CONCAT('Done: ',TIMESTAMPDIFF(SECOND,START_TIME,SYSDATE()),' seconds.'), NOW());
	
END$$
DELIMITER ;
