CREATE DEFINER=`covmo`@`%` PROCEDURE `SP_Sub_Generate_LU_Reject`(IN TMP_GT_DB VARCHAR(100),IN GT_DB VARCHAR(100))
BEGIN   	
	DECLARE START_TIME DATETIME DEFAULT SYSDATE();
	DECLARE SH_EH VARCHAR(9) DEFAULT RIGHT(TMP_GT_DB,9);
	DECLARE SH VARCHAR(4) DEFAULT gt_strtok(SH_EH,1,'_');
	SET @SqlCmd=CONCAT('TRUNCATE TABLE ',GT_DB,'.`table_lu_reject_',SH,'`;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	INSERT INTO gt_gw_main.sp_log VALUES(TMP_GT_DB,'SP_Sub_Generate_LU_Reject',CONCAT('Count LU_FAILURE_table_call_start'), START_TIME);
	SET @SqlCmd=CONCAT('INSERT INTO ',GT_DB,'.`table_lu_reject_',SH,'`(
				`IMSI`,
				`Total_Reject_Calls`,
				`LU_FAILURE_CS_CNT`,
				`LU_FAILURE_PS_CNT`,
				`LU_FAILURE_Multi_RAB_CNT`,
				`LU_FAILURE_Signalling_CNT`,
				`CAUSE_2_CNT`,
				`CAUSE_3_CNT`,
				`CAUSE_4_CNT`,
				`CAUSE_5_CNT`,
				`CAUSE_6_CNT`,
				`CAUSE_7_CNT`,
				`CAUSE_8_CNT`,
				`CAUSE_9_CNT`,
				`CAUSE_10_CNT`,
				`CAUSE_11_CNT`,
				`CAUSE_12_CNT`,
				`CAUSE_13_CNT`,
				`CAUSE_14_CNT`,
				`CAUSE_15_CNT` ,
				`CAUSE_16_CNT`,
				`CAUSE_17_CNT`,
				`CAUSE_20_CNT`,
				`CAUSE_21_CNT`,
				`CAUSE_22_CNT`
			)
		SELECT	
			 IMSI
			, SUM(1) AS `Total_Reject_Calls` 
			, SUM(IF(CALL_TYPE IN (10,11),1,0)) AS LU_FAILURE_CS_CNT
 			, SUM(IF(CALL_TYPE IN (12,13),1,0)) AS LU_FAILURE_PS_CNT
			, SUM(IF(CALL_TYPE IN (14),1,0)) AS LU_FAILURE_Multi_RAB_CNT
 			, SUM(IF(CALL_TYPE IN (15),1,0)) AS LU_FAILURE_Signalling_CNT
 			, SUM(IF(LU_FAILURE = 2,1,0)) AS CAUSE_2_CNT
			, SUM(IF(LU_FAILURE = 3,1,0)) AS CAUSE_3_CNT
 			, SUM(IF(LU_FAILURE = 4,1,0)) AS CAUSE_4_CNT
 			, SUM(IF(LU_FAILURE = 5,1,0)) AS CAUSE_5_CNT
 			, SUM(IF(LU_FAILURE = 6,1,0)) AS CAUSE_6_CNT
 			, SUM(IF(LU_FAILURE = 7,1,0)) AS CAUSE_7_CNT
 			, SUM(IF(LU_FAILURE = 8,1,0)) AS CAUSE_8_CNT
			, SUM(IF(LU_FAILURE = 9,1,0)) AS CAUSE_9_CNT
			, SUM(IF(LU_FAILURE = 10,1,0)) AS CAUSE_10_CNT
			, SUM(IF(LU_FAILURE = 11,1,0)) AS CAUSE_11_CNT
			, SUM(IF(LU_FAILURE = 12,1,0)) AS CAUSE_12_CNT
			, SUM(IF(LU_FAILURE = 13,1,0)) AS CAUSE_13_CNT
			, SUM(IF(LU_FAILURE = 14,1,0)) AS CAUSE_14_CNT
			, SUM(IF(LU_FAILURE = 15,1,0)) AS CAUSE_15_CNT
			, SUM(IF(LU_FAILURE = 16,1,0)) AS CAUSE_16_CNT
			, SUM(IF(LU_FAILURE = 17,1,0)) AS CAUSE_17_CNT
			, SUM(IF(LU_FAILURE = 20,1,0)) AS CAUSE_20_CNT
			, SUM(IF(LU_FAILURE = 21,1,0)) AS CAUSE_21_CNT
			, SUM(IF(LU_FAILURE = 22,1,0)) AS CAUSE_22_CNT
		FROM ',GT_DB,'.table_call_',SH,'
		WHERE LU_FAILURE IS NOT NULL 
		AND IMSI IS NOT NULL 
		AND CALL_TYPE IN (10, 11, 12, 13,14,15)
		GROUP BY  IMSI
-- 		ON DUPLICATE KEY UPDATE 			
-- 			',GT_DB,'.table_lu_reject.Total_Reject_Calls=',GT_DB,'.table_lu_reject.Total_Reject_Calls+VALUES(Total_Reject_Calls),
-- 			',GT_DB,'.table_lu_reject.LU_FAILURE_CS_CNT=',GT_DB,'.table_lu_reject.LU_FAILURE_CS_CNT+VALUES(LU_FAILURE_CS_CNT),
-- 			',GT_DB,'.table_lu_reject.LU_FAILURE_PS_CNT=',GT_DB,'.table_lu_reject.LU_FAILURE_PS_CNT+VALUES(LU_FAILURE_PS_CNT),
-- 			',GT_DB,'.table_lu_reject.LU_FAILURE_Multi_RAB_CNT=',GT_DB,'.table_lu_reject.LU_FAILURE_Multi_RAB_CNT+VALUES(LU_FAILURE_Multi_RAB_CNT),
-- 			',GT_DB,'.table_lu_reject.LU_FAILURE_Signalling_CNT=',GT_DB,'.table_lu_reject.LU_FAILURE_Signalling_CNT+VALUES(LU_FAILURE_Signalling_CNT),
-- 			',GT_DB,'.table_lu_reject.CAUSE_2_CNT=',GT_DB,'.table_lu_reject.CAUSE_2_CNT+VALUES(CAUSE_2_CNT),
-- 			',GT_DB,'.table_lu_reject.CAUSE_3_CNT=',GT_DB,'.table_lu_reject.CAUSE_3_CNT+VALUES(CAUSE_3_CNT),
-- 			',GT_DB,'.table_lu_reject.CAUSE_4_CNT=',GT_DB,'.table_lu_reject.CAUSE_4_CNT+VALUES(CAUSE_4_CNT),
-- 			',GT_DB,'.table_lu_reject.CAUSE_5_CNT=',GT_DB,'.table_lu_reject.CAUSE_5_CNT+VALUES(CAUSE_5_CNT),
-- 			',GT_DB,'.table_lu_reject.CAUSE_6_CNT=',GT_DB,'.table_lu_reject.CAUSE_6_CNT+VALUES(CAUSE_6_CNT),
-- 			',GT_DB,'.table_lu_reject.CAUSE_7_CNT=',GT_DB,'.table_lu_reject.CAUSE_7_CNT+VALUES(CAUSE_7_CNT),
-- 			',GT_DB,'.table_lu_reject.CAUSE_8_CNT=',GT_DB,'.table_lu_reject.CAUSE_8_CNT+VALUES(CAUSE_8_CNT),
-- 			',GT_DB,'.table_lu_reject.CAUSE_9_CNT=',GT_DB,'.table_lu_reject.CAUSE_9_CNT+VALUES(CAUSE_9_CNT),
-- 			',GT_DB,'.table_lu_reject.CAUSE_10_CNT=',GT_DB,'.table_lu_reject.CAUSE_10_CNT+VALUES(CAUSE_10_CNT),
-- 			',GT_DB,'.table_lu_reject.CAUSE_11_CNT=',GT_DB,'.table_lu_reject.CAUSE_11_CNT+VALUES(CAUSE_11_CNT),
-- 			',GT_DB,'.table_lu_reject.CAUSE_12_CNT=',GT_DB,'.table_lu_reject.CAUSE_12_CNT+VALUES(CAUSE_12_CNT),
-- 			',GT_DB,'.table_lu_reject.CAUSE_13_CNT=',GT_DB,'.table_lu_reject.CAUSE_13_CNT+VALUES(CAUSE_13_CNT),
-- 			',GT_DB,'.table_lu_reject.CAUSE_14_CNT=',GT_DB,'.table_lu_reject.CAUSE_14_CNT+VALUES(CAUSE_14_CNT),
-- 			',GT_DB,'.table_lu_reject.CAUSE_15_CNT=',GT_DB,'.table_lu_reject.CAUSE_15_CNT+VALUES(CAUSE_15_CNT),
-- 			',GT_DB,'.table_lu_reject.CAUSE_16_CNT=',GT_DB,'.table_lu_reject.CAUSE_16_CNT+VALUES(CAUSE_16_CNT),
-- 			',GT_DB,'.table_lu_reject.CAUSE_17_CNT=',GT_DB,'.table_lu_reject.CAUSE_17_CNT+VALUES(CAUSE_17_CNT),
-- 			',GT_DB,'.table_lu_reject.CAUSE_20_CNT=',GT_DB,'.table_lu_reject.CAUSE_20_CNT+VALUES(CAUSE_20_CNT),
-- 			',GT_DB,'.table_lu_reject.CAUSE_21_CNT=',GT_DB,'.table_lu_reject.CAUSE_21_CNT+VALUES(CAUSE_21_CNT),
-- 			',GT_DB,'.table_lu_reject.CAUSE_22_CNT=',GT_DB,'.table_lu_reject.CAUSE_22_CNT+VALUES(CAUSE_22_CNT)
			;');
		
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	
	
		
	INSERT INTO gt_gw_main.sp_log VALUES(TMP_GT_DB,'SP_Sub_Generate_LU_Reject',CONCAT('Count LU_reject Done cost:',TIMESTAMPDIFF(SECOND,START_TIME,SYSDATE()),' sec.'), NOW());		
