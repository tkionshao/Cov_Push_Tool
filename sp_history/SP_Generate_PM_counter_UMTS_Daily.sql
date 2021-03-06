DELIMITER $$
USE `gt_gw_main`$$
DROP PROCEDURE IF EXISTS `SP_CreateDB_LTE`$$
CREATE DEFINER=`covmo`@`%` PROCEDURE `SP_Generate_PM_counter_UMTS_Daily`(IN GT_DB VARCHAR(100))
a_label:
BEGIN
	DECLARE START_TIME DATETIME DEFAULT SYSDATE();
	DECLARE O_GT_DB VARCHAR(100) DEFAULT GT_DB;
	DECLARE STARTHOUR VARCHAR(2) DEFAULT SUBSTRING(RIGHT(GT_DB,18),10,2);
	DECLARE ENDHOUR VARCHAR(2) DEFAULT IF(SUBSTRING(RIGHT(GT_DB,18),15,2)='00','24',SUBSTRING(RIGHT(GT_DB,18),15,2));
	DECLARE SH_EH VARCHAR(9) DEFAULT RIGHT(GT_DB,9);
	DECLARE WORKER_ID VARCHAR(10) DEFAULT CONNECTION_ID();
	DECLARE PARTITION_ID INT DEFAULT SUBSTRING(RIGHT(GT_DB,18),10,2);
	DECLARE FILEDATE VARCHAR(10) DEFAULT gt_strtok(GT_DB,3,'_');
	DECLARE SESSION_DATE VARCHAR(18) DEFAULT CONCAT(LEFT(FILEDATE,4),'-',SUBSTRING(FILEDATE,5,2),'-',SUBSTRING(FILEDATE,7,2));
	DECLARE CURRENT_NT_DB VARCHAR(50) DEFAULT CONCAT('gt_nt_',gt_strtok(GT_DB,3,'_'));
	
	SELECT REPLACE(GT_DB,SH_EH,'0000_0000') INTO GT_DB;
	SET SESSION max_heap_table_size = 1024*1024*1024*4; 
	SET SESSION tmp_table_size = 1024*1024*1024*4; 
	SET SESSION join_buffer_size = 1024*1024*1024; 
	SET SESSION sort_buffer_size = 1024*1024*1024; 
	SET SESSION read_buffer_size = 1024*1024*1024; 
	
  	INSERT INTO gt_gw_main.SP_LOG VALUES(GT_DB,'SP_Generate_PM_counter_UMTS_Daily','Start', START_TIME);
 
	SET @SqlCmd=CONCAT('ALTER TABLE ',GT_DB,'.table_pm_counter_umts TRUNCATE PARTITION h',PARTITION_ID,';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt; 
	INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'SP_Generate_PM_counter_UMTS_Daily','tmp_table_pmnoloadsharingrrcconn', NOW());
	
	SET @SqlCmd=CONCAT(' Drop TEMPORARY table if exists ',GT_DB,'.tmp_table_pmnoloadsharingrrcconn_',WORKER_ID,';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('
	CREATE TEMPORARY TABLE ',GT_DB,'.tmp_table_pmnoloadsharingrrcconn_',WORKER_ID,' ENGINE=MYISAM AS 
	SELECT 
		neun
		,nedn
		,mts
		,moid
		,CONCAT(LEFT(LEFT(mts,10),4),''-'',SUBSTRING(LEFT(mts,10),5,2),''-'',SUBSTRING(LEFT(mts,10),7,2)) AS  DATA_DATE
		,CAST(RIGHT(LEFT(mts,10),2) AS SIGNED) AS DATA_HOUR
		,neun AS RNC_ID
		,REPLACE(moid,''ManagedElement=1,RncFunction=1,UtranCell='','''')  AS CELL_ID
		,pmTotNoRrcConnectReqSuccess
		,pmTotNoRrcConnectReq
		,pmTotNoRrcConnectReqCsSucc
		,pmTotNoRrcConnectReqCs
		,pmNoRabEstablishSuccessSpeech
		,pmNoRabEstablishSuccessCS64
		,pmNoRabEstablishSuccessCs57
		,pmNoRabEstablishAttemptSpeech
		,pmNoRabEstablishAttemptCS64
		,pmNoRabEstablishAttemptCs57
		,pmTotNoRrcConnectReqPsSucc
		,pmTotNoRrcConnectReqPs
		,pmNoRabEstablishSuccessPacketStream
		,pmNoRabEstablishSuccessPacketStream128
		,pmNoRabEstablishSuccessPacketInteractive
		,pmNoRabEstablishAttemptPacketStream
		,pmNoRabEstablishAttemptPacketStream128
		,pmNoRabEstablishAttemptPacketInteractive
		,pmNoRrcCsReqDeniedAdm
		,pmNoOfNonHoReqDeniedSpeech
		,pmNoOfNonHoReqDeniedCs
		,pmNoSystemRabReleaseSpeech
		,pmNoSystemRabReleaseCs64
		,pmNoSystemRabReleaseCsStream
		,pmNoNormalRabReleaseSpeech
		,pmNoNormalRabReleaseCs64
		,pmNoNormalRabReleaseCsStream
		,pmNoOfNonHoReqDeniedInteractive
		,pmNoOfNonHoReqDeniedHs
		,pmNoOfNonHoReqDeniedPsStreaming
		,pmNoOfNonHoReqDeniedPsStr128
		,pmNoSystemRabReleasePacketStream
		,pmNoSystemRabReleasePacketStream128
		,pmNoSystemRabReleasePsStreamHs
		,pmNoSystemRabReleasePacket
		,pmNoNormalRabReleasePacketStream
		,pmNoNormalRabReleasePacketStream128
		,pmNoNormalRabReleasePsStreamHs
		,pmNoNormalRabReleasePacket
		,pmCelldowntimeman
		,pmCelldowntimeauto
	
		,pmNoDirRetryAtt
		,pmNoNormalNasSignReleasePs
		,pmNoNormalRbReleaseHs
		,pmNoRabEstablishAttemptPacketInteractiveEul
		,pmNoRabEstablishAttemptPacketInteractiveHs
		,pmNoRabEstablishSuccessPacketInteractiveEul
		,pmNoRabEstablishSuccessPacketInteractiveHs
		,pmNoSuccRbReconfOrigPsIntDch
		,pmNoSystemNasSignReleasePs
		,pmNoSystemRbReleaseHs
		,pmPsIntHsToFachSucc
		,pmSamplesDchDlRlcUserPacketThp
		,pmSamplesDchUlRlcUserPacketThp
		,pmSumBestCs64RabEstablish
		,pmSumBestDchPsIntRabEstablish
		,pmSumBestPsEulRabEstablish
		,pmSumBestPsHsAdchRabEstablish
		
		,pmSumBestCs12Establish
		,pmSumDchDlRlcUserPacketThp
		,pmSumDchUlRlcUserPacketThp
	
		
				
		-- ,pmNoRabEstBlockRnBestPsStreamHs
	FROM ',GT_DB,'.table_pmnoloadsharingrrcconn
	WHERE CAST(RIGHT(LEFT(mts,10),2) AS SIGNED) >= ',STARTHOUR,' AND CAST(RIGHT(LEFT(mts,10),2) AS SIGNED) < ',ENDHOUR,'
		AND CONCAT(LEFT(LEFT(mts,10),4),''-'',SUBSTRING(LEFT(mts,10),5,2),''-'',SUBSTRING(LEFT(mts,10),7,2)) = ''',SESSION_DATE,'''
	;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('ALTER TABLE ',GT_DB,'.tmp_table_pmnoloadsharingrrcconn_',WORKER_ID,' ADD INDEX `ix_key`(neun,nedn,mts,moid);');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'SP_Generate_PM_counter_UMTS_Daily','tmp_table_pmNoTimesRlDelFrActSet', NOW());
	SET @SqlCmd=CONCAT('Drop TEMPORARY table if exists ',GT_DB,'.tmp_table_pmNoTimesRlDelFrActSet_',WORKER_ID,';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('
	CREATE TEMPORARY TABLE ',GT_DB,'.tmp_table_pmNoTimesRlDelFrActSet_',WORKER_ID,' ENGINE=MYISAM AS 
	SELECT 
		neun
		,nedn
		,mts
		,moid
		,CONCAT(LEFT(LEFT(mts,10),4),''-'',SUBSTRING(LEFT(mts,10),5,2),''-'',SUBSTRING(LEFT(mts,10),7,2)) AS  DATA_DATE
		,CAST(RIGHT(LEFT(mts,10),2) AS SIGNED) AS DATA_HOUR
		,neun AS RNC_ID
		,REPLACE(moid,''ManagedElement=1,RncFunction=1,UtranCell='','''')  AS CELL_ID
		,pmNoRrcConnReqBlockTnCs
		,pmNoRabEstBlockTnSpeechBest
		,pmNoRabEstBlockTnCs57Best
		,pmNoRabEstBlockTnCs64Best
		,pmNoRrcConnReqBlockTnPs
		,pmNoRabEstBlockTnPsIntNonHsBest
		,pmNoRabEstBlockTnPsStrNonHsBest
		,pmNoRabEstBlockTnPsIntHsBest
		,pmNoSystemRabReleasePacketUra
		,pmNoNormalRabReleasePacketUra
		,pmChSwitchSuccFachUra
		,pmNoRabEstBlockTnPsStreamHsBest
	
		,pmNoTimesCellFailAddToActSet
		,pmNoTimesRlAddToActSet
		,pmSamplesUlRssi
		,pmSumUlRssi
	FROM ',GT_DB,'.table_pmNoTimesRlDelFrActSet
	WHERE CAST(RIGHT(LEFT(mts,10),2) AS SIGNED) >= ',STARTHOUR,' AND CAST(RIGHT(LEFT(mts,10),2) AS SIGNED) < ',ENDHOUR,'
		AND CONCAT(LEFT(LEFT(mts,10),4),''-'',SUBSTRING(LEFT(mts,10),5,2),''-'',SUBSTRING(LEFT(mts,10),7,2)) = ''',SESSION_DATE,'''
	;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('ALTER TABLE ',GT_DB,'.tmp_table_pmNoTimesRlDelFrActSet_',WORKER_ID,' ADD INDEX `ix_key`(neun,nedn,mts,moid);');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT(' Drop TEMPORARY table if exists ',GT_DB,'.tmp_pm_umts_1_',WORKER_ID,';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('
	CREATE TEMPORARY TABLE ',GT_DB,'.tmp_pm_umts_1_',WORKER_ID,' ENGINE=MYISAM AS 
	SELECT A.DATA_DATE,
		A.DATA_HOUR,
		A.RNC_ID,
		A.CELL_ID,
		SUM(pmTotNoRrcConnectReqSuccess) AS NoRrcConnectReqSuccess,
		SUM(pmTotNoRrcConnectReq) AS NoRrcConnectReq,
		SUM(pmTotNoRrcConnectReqCsSucc) AS NoRrcConnectReqSuccessCs,
		SUM(pmTotNoRrcConnectReqCs) AS NoRrcConnectReqCs	,
		SUM(pmNoRabEstablishSuccessSpeech + pmNoRabEstablishSuccessCS64 + pmNoRabEstablishSuccessCs57) AS NoRabEstablishSuccessCS,
		SUM(pmNoRabEstablishAttemptSpeech + pmNoRabEstablishAttemptCS64 + pmNoRabEstablishAttemptCs57) AS NoRabEstablishAttemptCS,
		SUM(pmTotNoRrcConnectReqPsSucc) AS NoRrcConnectReqSuccessPs,
		SUM(pmTotNoRrcConnectReqPs) AS NoRrcConnectReqPs,
		SUM(pmNoRabEstablishSuccessPacketStream + pmNoRabEstablishSuccessPacketStream128 +  pmNoRabEstablishSuccessPacketInteractive) AS NoRabEstablishSuccessPS,
		SUM(pmNoRabEstablishAttemptPacketStream + pmNoRabEstablishAttemptPacketStream128 + pmNoRabEstablishAttemptPacketInteractive ) AS NoRabEstablishAttemptPS,
		SUM(pmNoRrcCsReqDeniedAdm + pmNoRrcConnReqBlockTnCs) AS NoRrcReqBlock,
		SUM(pmNoOfNonHoReqDeniedSpeech + pmNoRabEstBlockTnSpeechBest + pmNoOfNonHoReqDeniedCs + pmNoRabEstBlockTnCs57Best + pmNoRabEstBlockTnCs64Best) AS NoRabReqBlockCS,
		SUM(pmNoSystemRabReleaseSpeech + pmNoSystemRabReleaseCs64 + pmNoSystemRabReleaseCsStream) AS NoSystemRabReleaseCS,
		SUM(pmNoNormalRabReleaseSpeech + pmNoNormalRabReleaseCs64 + pmNoNormalRabReleaseCsStream) AS NoNormalRabReleaseCS,
		SUM(a.pmNoSystemRabReleasePacketStream + a.pmNoSystemRabReleasePacketStream128 + a.pmNoSystemRabReleasePsStreamHs + a.pmNoSystemRabReleasePacket- b.pmNoSystemRabReleasePacketUra) AS NoSystemRabReleasePS,
		SUM(a.pmNoNormalRabReleasePacketStream +  a.pmNoNormalRabReleasePacketStream128 + a.pmNoNormalRabReleasePsStreamHs + b.pmNoSystemRabReleasePacketUra + a.pmNoNormalRabReleasePacket - b.pmNoNormalRabReleasePacketUra + b.pmChSwitchSuccFachUra) AS NoNormalRabReleasePS,
		SUM(pmNoRrcConnReqBlockTnPs) AS NoRabReqBlockPS,
		SUM(	pmNoOfNonHoReqDeniedInteractive + 
			pmNoOfNonHoReqDeniedHs+ 
			pmNoRabEstBlockTnPsIntNonHsBest + 
			pmNoRabEstBlockTnPsIntHsBest + 
			pmNoOfNonHoReqDeniedPsStreaming + 
			pmNoOfNonHoReqDeniedPsStr128 + 
			pmNoRabEstBlockTnPsStrNonHsBest +
			--  pmNoRabEstBlockRnBestPsStreamHs +
			pmNoRabEstBlockTnPsStreamHsBest) AS NoOfCONGCALLS,
		SUM(pmCelldowntimeman) AS Downtimeman,
		SUM(pmCelldowntimeauto) AS Downtimeauto,
		SUM(IFNULL(A.pmNoDirRetryAtt,0)) AS pmNoDirRetryAtt,
		SUM(IFNULL(A.pmNoNormalNasSignReleasePs,0)) AS pmNoNormalNasSignReleasePs,
		SUM(IFNULL(A.pmNoNormalRabReleasePacket,0)) AS pmNoNormalRabReleasePacket,
		SUM(IFNULL(A.pmNoNormalRabReleaseSpeech,0)) AS pmNoNormalRabReleaseSpeech,
		SUM(IFNULL(A.pmNoNormalRbReleaseHs,0)) AS pmNoNormalRbReleaseHs,
		SUM(IFNULL(A.pmNoRabEstablishAttemptPacketInteractive,0)) AS pmNoRabEstablishAttemptPacketInteractive,
		SUM(IFNULL(A.pmNoRabEstablishAttemptPacketInteractiveEul,0)) AS pmNoRabEstablishAttemptPacketInteractiveEul,
		SUM(IFNULL(A.pmNoRabEstablishAttemptPacketInteractiveHs,0)) AS pmNoRabEstablishAttemptPacketInteractiveHs,
		SUM(IFNULL(A.pmNoRabEstablishAttemptSpeech,0)) AS pmNoRabEstablishAttemptSpeech,
		SUM(IFNULL(A.pmNoRabEstablishSuccessPacketInteractive,0)) AS pmNoRabEstablishSuccessPacketInteractive,
		SUM(IFNULL(A.pmNoRabEstablishSuccessPacketInteractiveEul,0)) AS pmNoRabEstablishSuccessPacketInteractiveEul,
		SUM(IFNULL(A.pmNoRabEstablishSuccessPacketInteractiveHs,0)) AS pmNoRabEstablishSuccessPacketInteractiveHs,
		SUM(IFNULL(A.pmNoRabEstablishSuccessSpeech,0)) AS pmNoRabEstablishSuccessSpeech,
		SUM(IFNULL(A.pmNoSuccRbReconfOrigPsIntDch,0)) AS pmNoSuccRbReconfOrigPsIntDch,
		SUM(IFNULL(A.pmNoSystemNasSignReleasePs,0)) AS pmNoSystemNasSignReleasePs,
		SUM(IFNULL(A.pmNoSystemRabReleasePacket,0)) AS pmNoSystemRabReleasePacket,
		SUM(IFNULL(A.pmNoSystemRabReleaseSpeech,0)) AS pmNoSystemRabReleaseSpeech,
		SUM(IFNULL(A.pmNoSystemRbReleaseHs,0)) AS pmNoSystemRbReleaseHs,
		SUM(IFNULL(A.pmPsIntHsToFachSucc,0)) AS pmPsIntHsToFachSucc,
		SUM(IFNULL(A.pmSamplesDchDlRlcUserPacketThp,0)) AS pmSamplesDchDlRlcUserPacketThp,
		SUM(IFNULL(A.pmSamplesDchUlRlcUserPacketThp,0)) AS pmSamplesDchUlRlcUserPacketThp,
		SUM(IFNULL(A.pmSumBestCs64RabEstablish,0)) AS pmSumBestCs64RabEstablish,
		SUM(IFNULL(A.pmSumBestDchPsIntRabEstablish,0)) AS pmSumBestDchPsIntRabEstablish,
		SUM(IFNULL(A.pmSumBestPsEulRabEstablish,0)) AS pmSumBestPsEulRabEstablish,
		SUM(IFNULL(A.pmSumBestPsHsAdchRabEstablish,0)) AS pmSumBestPsHsAdchRabEstablish,
		SUM(IFNULL(A.pmTotNoRrcConnectReq,0)) AS pmTotNoRrcConnectReq,
		SUM(IFNULL(A.pmTotNoRrcConnectReqCs,0)) AS pmTotNoRrcConnectReqCs,
		SUM(IFNULL(A.pmTotNoRrcConnectReqCsSucc,0)) AS pmTotNoRrcConnectReqCsSucc,
		SUM(IFNULL(A.pmTotNoRrcConnectReqPs,0)) AS pmTotNoRrcConnectReqPs,
		SUM(IFNULL(A.pmTotNoRrcConnectReqPsSucc,0)) AS pmTotNoRrcConnectReqPsSucc,
		SUM(IFNULL(A.pmTotNoRrcConnectReqSuccess,0)) AS pmTotNoRrcConnectReqSuccess,
	
		SUM(IFNULL(A.pmSumBestCs12Establish,0)) AS pmSumBestCs12Establish,
		SUM(IFNULL(A.pmSumDchDlRlcUserPacketThp,0)) AS pmSumDchDlRlcUserPacketThp,
		SUM(IFNULL(A.pmSumDchUlRlcUserPacketThp,0)) AS pmSumDchUlRlcUserPacketThp,
		
		
		SUM(IFNULL(B.pmChSwitchSuccFachUra,0)) AS pmChSwitchSuccFachUra,
		SUM(IFNULL(B.pmNoNormalRabReleasePacketUra,0)) AS pmNoNormalRabReleasePacketUra,
		SUM(IFNULL(B.pmNoSystemRabReleasePacketUra,0)) AS pmNoSystemRabReleasePacketUra,
		SUM(IFNULL(B.pmNoTimesCellFailAddToActSet,0)) AS pmNoTimesCellFailAddToActSet,
		SUM(IFNULL(B.pmNoTimesRlAddToActSet,0)) AS pmNoTimesRlAddToActSet,
		SUM(IFNULL(B.pmSamplesUlRssi,0)) AS pmSamplesUlRssi,
		SUM(IFNULL(B.pmSumUlRssi,0)) AS pmSumUlRssi,
	
	
		SUM(IFNULL(A.pmNoRabEstablishSuccessPacketStream,0)) AS pmNoRabEstablishSuccessPacketStream,
		SUM(IFNULL(A.pmNoRabEstablishAttemptPacketStream,0)) AS pmNoRabEstablishAttemptPacketStream,
		SUM(IFNULL(A.pmNoRrcCsReqDeniedAdm,0)) AS pmNoRrcCsReqDeniedAdm,
		SUM(IFNULL(A.pmNoOfNonHoReqDeniedSpeech,0)) AS pmNoOfNonHoReqDeniedSpeech,
		SUM(IFNULL(A.pmNoNormalRabReleasePacketStream,0)) AS pmNoNormalRabReleasePacketStream,
		SUM(IFNULL(A.pmNoOfNonHoReqDeniedInteractive,0)) AS pmNoOfNonHoReqDeniedInteractive,
		SUM(IFNULL(A.pmNoRabEstablishSuccessCS64,0)) AS pmNoRabEstablishSuccessCS64,
		SUM(IFNULL(A.pmNoRabEstablishAttemptCS64,0)) AS pmNoRabEstablishAttemptCS64,
		SUM(IFNULL(A.pmNoRabEstablishSuccessPacketStream128,0)) AS pmNoRabEstablishSuccessPacketStream128,
		SUM(IFNULL(A.pmNoRabEstablishAttemptPacketStream128,0)) AS pmNoRabEstablishAttemptPacketStream128,
		SUM(IFNULL(A.pmNoSystemRabReleaseCs64,0)) AS pmNoSystemRabReleaseCs64,
		SUM(IFNULL(A.pmNoNormalRabReleaseCs64,0)) AS pmNoNormalRabReleaseCs64,
		SUM(IFNULL(A.pmNoNormalRabReleasePacketStream128,0)) AS pmNoNormalRabReleasePacketStream128,
		SUM(IFNULL(A.pmNoRabEstablishSuccessCs57,0)) AS pmNoRabEstablishSuccessCs57,
		SUM(IFNULL(A.pmNoRabEstablishAttemptCs57,0)) AS pmNoRabEstablishAttemptCs57,
		SUM(IFNULL(A.pmNoOfNonHoReqDeniedCs,0)) AS pmNoOfNonHoReqDeniedCs,
		SUM(IFNULL(A.pmNoSystemRabReleaseCsStream,0)) AS pmNoSystemRabReleaseCsStream,
		SUM(IFNULL(A.pmNoNormalRabReleaseCsStream,0)) AS pmNoNormalRabReleaseCsStream,
		SUM(IFNULL(A.pmNoNormalRabReleasePsStreamHs,0)) AS pmNoNormalRabReleasePsStreamHs,
		SUM(IFNULL(B.pmNoRrcConnReqBlockTnCs,0)) AS pmNoRrcConnReqBlockTnCs,
		SUM(IFNULL(B.pmNoRabEstBlockTnSpeechBest,0)) AS pmNoRabEstBlockTnSpeechBest,
		SUM(IFNULL(B.pmNoRabEstBlockTnCs57Best,0)) AS pmNoRabEstBlockTnCs57Best,
		SUM(IFNULL(B.pmNoRabEstBlockTnCs64Best,0)) AS pmNoRabEstBlockTnCs64Best
		
	FROM ',GT_DB,'.tmp_table_pmnoloadsharingrrcconn_',WORKER_ID,' a LEFT JOIN ',GT_DB,'.tmp_table_pmNoTimesRlDelFrActSet_',WORKER_ID,' b
		ON a.neun =b.neun
		AND a.nedn =b.nedn
		AND a.mts =b.mts
		AND a.moid =b.moid
	GROUP BY A.DATA_DATE,A.DATA_HOUR,A.RNC_ID,A.CELL_ID
	;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	SET @SqlCmd=CONCAT('ALTER TABLE ',GT_DB,'.tmp_pm_umts_1_',WORKER_ID,' ADD INDEX `ix_key`(DATA_DATE,DATA_HOUR,RNC_ID,CELL_ID);');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	 
	SET @SqlCmd=CONCAT(' Drop TEMPORARY table if exists ',GT_DB,'.tmp_pm_umts_2_',WORKER_ID,';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('
	CREATE TEMPORARY TABLE  ',GT_DB,'.tmp_pm_umts_2_',WORKER_ID,' ENGINE=MYISAM AS 
	SELECT CONCAT(LEFT(LEFT(`mts`,10),4),''-'',SUBSTRING(LEFT(`mts`,10),5,2),''-'',SUBSTRING(LEFT(`mts`,10),7,2)) AS  DATA_DATE,
	CAST(RIGHT(LEFT(`mts`,10),2) AS SIGNED) AS DATA_HOUR,
	neun AS RNC_ID,
	REPLACE(`moid`,''ManagedElement=1,RncFunction=1,UtranCell='','''')  AS `CELL_ID`,
	SUM(pmSumUesWith1Rls1RlInActSet) AS UesWith1Rls1RlInAct,
	SUM(pmSumUesWith1Rls2RlInActSet + pmSumUesWith2Rls2RlInActSet) AS UesWith1Rls2RlInAct2Rls2RlInAct,
	SUM(pmSumUesWith1Rls3RlInActSet + pmSumUesWith2Rls3RlInActSet + pmSumUesWith3Rls3RlInActSet) AS UesWith1Rls3RlInActSet2Rls3RlInAct3Rls3RlInAct,
	SUM(pmSumUesWith2Rls4RlInActSet + pmSumUesWith3Rls4RlInActSet + pmSumUesWith4Rls4RlInActSet) AS UesWith2Rls4RlInActSet3Rls4RlInAct4Rls4RlInAct,
-- 		SUM(UesWith1Rls1RlInAct+UesWith1Rls2RlInAct2Rls2RlInAct+UesWith1Rls3RlInActSet2Rls3RlInAct3Rls3RlInAct+UesWith2Rls4RlInActSet3Rls4RlInAct4Rls4RlInAct) as UesWithmulRlsmulRlInAct
	SUM(IFNULL(pmSumBestAmr12200RabEstablish,0)) AS pmSumBestAmr12200RabEstablish,	
	SUM(IFNULL(pmSumUesWith1Rls1RlInActSet,0)) AS pmSumUesWith1Rls1RlInActSet,
	SUM(IFNULL(pmSumUesWith1Rls2RlInActSet,0)) AS pmSumUesWith1Rls2RlInActSet,
	SUM(IFNULL(pmSumUesWith1Rls3RlInActSet,0)) AS pmSumUesWith1Rls3RlInActSet,
	SUM(IFNULL(pmSumUesWith2Rls2RlInActSet,0)) AS pmSumUesWith2Rls2RlInActSet,
	SUM(IFNULL(pmSumUesWith2Rls3RlInActSet,0)) AS pmSumUesWith2Rls3RlInActSet,
	SUM(IFNULL(pmSumUesWith3Rls3RlInActSet,0)) AS pmSumUesWith3Rls3RlInActSet,
	
	SUM(IFNULL(pmSumUesWith2Rls4RlInActSet,0)) AS pmSumUesWith2Rls4RlInActSet,
	SUM(IFNULL(pmSumUesWith3Rls4RlInActSet,0)) AS pmSumUesWith3Rls4RlInActSet,
	SUM(IFNULL(pmSumUesWith4Rls4RlInActSet,0)) AS pmSumUesWith4Rls4RlInActSet
	FROM ',GT_DB,'.table_pmCmAttDlHls
	WHERE CAST(RIGHT(LEFT(mts,10),2) AS SIGNED) >= ',STARTHOUR,' AND CAST(RIGHT(LEFT(mts,10),2) AS SIGNED) < ',ENDHOUR,'
		AND CONCAT(LEFT(LEFT(mts,10),4),''-'',SUBSTRING(LEFT(mts,10),5,2),''-'',SUBSTRING(LEFT(mts,10),7,2)) = ''',SESSION_DATE,'''
	GROUP BY 
	CONCAT(LEFT(LEFT(`mts`,10),4),''-'',SUBSTRING(LEFT(`mts`,10),5,2),''-'',SUBSTRING(LEFT(`mts`,10),7,2)),
	CAST(RIGHT(LEFT(`mts`,10),2) AS SIGNED) ,neun,REPLACE(`moid`,''ManagedElement=1,RncFunction=1,UtranCell='','''');');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('ALTER TABLE ',GT_DB,'.tmp_pm_umts_2_',WORKER_ID,' ADD INDEX `ix_key`(DATA_DATE,DATA_HOUR,RNC_ID,CELL_ID);');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
		
	SET @SqlCmd=CONCAT(' Drop TEMPORARY table if exists ',GT_DB,'.tmp_pm_umts_3_',WORKER_ID,';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('
	CREATE TEMPORARY TABLE  ',GT_DB,'.tmp_pm_umts_3_',WORKER_ID,' ENGINE=MYISAM AS 
	SELECT CONCAT(LEFT(LEFT(`mts`,10),4),''-'',SUBSTRING(LEFT(`mts`,10),5,2),''-'',SUBSTRING(LEFT(`mts`,10),7,2)) AS  DATA_DATE,
	CAST(RIGHT(LEFT(`mts`,10),2) AS SIGNED) AS DATA_HOUR,
	neun AS RNC_ID,
	gt_strtok(REPLACE(`moid`,''ManagedElement=1,RncFunction=1,UtranCell='',''''),1,'','')  AS `CELL_ID`,
	SUM(pmNoSuccessOutIratHoSpeech +  pmNoSuccessOutIratHoMulti) AS NoSuccessOutHo,
	SUM(pmNoAttOutIratHoSpeech + pmNoAttOutIratHoMulti)AS NoAttOutHo,
	SUM(pmNoSuccessOutIratHoSpeech)AS pmNoSuccessOutIratHoSpeech,
	SUM(pmNoAttOutIratHoSpeech)AS pmNoAttOutIratHoSpeech,
	
	SUM(pmNoSuccessOutIratHoMulti) AS pmNoSuccessOutIratHoMulti,
	SUM(pmNoAttOutIratHoMulti)AS pmNoAttOutIratHoMulti,
	0 AS pmNoAttOutSbHoSpeech,
	0 AS pmNoSuccessOutSbHoSpeech
	FROM ',GT_DB,'.table_pmnoattoutirathomulti
	WHERE CAST(RIGHT(LEFT(mts,10),2) AS SIGNED) >= ',STARTHOUR,' AND CAST(RIGHT(LEFT(mts,10),2) AS SIGNED) < ',ENDHOUR,'
		AND CONCAT(LEFT(LEFT(mts,10),4),''-'',SUBSTRING(LEFT(mts,10),5,2),''-'',SUBSTRING(LEFT(mts,10),7,2)) = ''',SESSION_DATE,'''
	GROUP BY 
	CONCAT(LEFT(LEFT(`mts`,10),4),''-'',SUBSTRING(LEFT(`mts`,10),5,2),''-'',SUBSTRING(LEFT(`mts`,10),7,2)),
	CAST(RIGHT(LEFT(`mts`,10),2) AS SIGNED) ,`neun`,gt_strtok(REPLACE(`moid`,''ManagedElement=1,RncFunction=1,UtranCell='',''''),1,'','');');
	
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt; 
	
	SET @SqlCmd=CONCAT('ALTER TABLE ',GT_DB,'.tmp_pm_umts_3_',WORKER_ID,' ADD INDEX `ix_key`(DATA_DATE,DATA_HOUR,RNC_ID,CELL_ID);');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	SET @SqlCmd=CONCAT(' Drop TEMPORARY table if exists ',GT_DB,'.tmp_table_pmHsDowntimeMan_',WORKER_ID,';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	
	
	SET @SqlCmd=CONCAT('
	CREATE TEMPORARY TABLE ',GT_DB,'.tmp_table_pmHsDowntimeMan_',WORKER_ID,' ENGINE=MYISAM AS 
	SELECT 
		neun
		,nedn
		,mts
		,REPLACE(`moid`,'',Hsdsch=1'','''') as moid
		,CONCAT(LEFT(LEFT(mts,10),4),''-'',SUBSTRING(LEFT(mts,10),5,2),''-'',SUBSTRING(LEFT(mts,10),7,2)) AS  DATA_DATE
		,CAST(RIGHT(LEFT(mts,10),2) AS SIGNED) AS DATA_HOUR
		,neun AS RNC_ID
		,REPLACE(REPLACE(moid,''ManagedElement=1,RncFunction=1,UtranCell='',''''),'',Hsdsch=1'','''')  AS CELL_ID
		,pmSumHsDlRlcUserPacketThp
		,pmSamplesHsDlRlcTotPacketThp
		,pmSamplesHsDlRlcUserPacketThp
		,pmSumHsDlRlcTotPacketThp
	FROM ',GT_DB,'.table_pmHsDowntimeMan
	WHERE CAST(RIGHT(LEFT(mts,10),2) AS SIGNED) >= ',STARTHOUR,' AND CAST(RIGHT(LEFT(mts,10),2) AS SIGNED) < ',ENDHOUR,'
		AND CONCAT(LEFT(LEFT(mts,10),4),''-'',SUBSTRING(LEFT(mts,10),5,2),''-'',SUBSTRING(LEFT(mts,10),7,2)) = ''',SESSION_DATE,'''
	;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('ALTER TABLE ',GT_DB,'.tmp_table_pmHsDowntimeMan_',WORKER_ID,' ADD INDEX `ix_key`(neun,nedn,mts,moid);');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'SP_Generate_PM_counter_UMTS_Daily','tmp_table_pmHsDowntimeMan_', NOW());
	
	
	SET @SqlCmd=CONCAT(' Drop TEMPORARY table if exists ',GT_DB,'.tmp_table_pmSamplesRrcOnlyEstablish_',WORKER_ID,';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('
	CREATE TEMPORARY TABLE ',GT_DB,'.tmp_table_pmSamplesRrcOnlyEstablish_',WORKER_ID,' ENGINE=MYISAM AS 
	SELECT 
		neun
		,nedn
		,mts
		,moid
		,CONCAT(LEFT(LEFT(mts,10),4),''-'',SUBSTRING(LEFT(mts,10),5,2),''-'',SUBSTRING(LEFT(mts,10),7,2)) AS  DATA_DATE
		,CAST(RIGHT(LEFT(mts,10),2) AS SIGNED) AS DATA_HOUR
		,neun AS RNC_ID
		,REPLACE(moid,''ManagedElement=1,RncFunction=1,UtranCell='','''')  AS CELL_ID
		,pmTotNoTermRrcConnectReq
		,pmTotNoTermRrcConnectReqCs
		,pmTotNoTermRrcConnectReqCsSucc
		,pmTotNoTermRrcConnectReqSucc
		
	FROM ',GT_DB,'.table_pmSamplesRrcOnlyEstablish
	WHERE CAST(RIGHT(LEFT(mts,10),2) AS SIGNED) >= ',STARTHOUR,' AND CAST(RIGHT(LEFT(mts,10),2) AS SIGNED) < ',ENDHOUR,'
		AND CONCAT(LEFT(LEFT(mts,10),4),''-'',SUBSTRING(LEFT(mts,10),5,2),''-'',SUBSTRING(LEFT(mts,10),7,2)) = ''',SESSION_DATE,'''
	;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	
	SET @SqlCmd=CONCAT('ALTER TABLE ',GT_DB,'.tmp_table_pmSamplesRrcOnlyEstablish_',WORKER_ID,' ADD INDEX `ix_key`(neun,nedn,mts,moid);');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'SP_Generate_PM_counter_UMTS_Daily','tmp_table_pmSamplesRrcOnlyEstablish_', NOW());	
	
	
	SET @SqlCmd=CONCAT(' Drop TEMPORARY table if exists ',GT_DB,'.tmp_table_pmDlTrafficVolumeCs_',WORKER_ID,';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('
	CREATE TEMPORARY TABLE ',GT_DB,'.tmp_table_pmDlTrafficVolumeCs_',WORKER_ID,' ENGINE=MYISAM AS 
	SELECT 
		neun
		,nedn
		,mts
		,moid
		,CONCAT(LEFT(LEFT(mts,10),4),''-'',SUBSTRING(LEFT(mts,10),5,2),''-'',SUBSTRING(LEFT(mts,10),7,2)) AS  DATA_DATE
		,CAST(RIGHT(LEFT(mts,10),2) AS SIGNED) AS DATA_HOUR
		,neun AS RNC_ID
		,REPLACE(moid,''ManagedElement=1,RncFunction=1,UtranCell='','''')  AS CELL_ID
		,pmUlTrafficVolumePs128
		,pmUlTrafficVolumePs16
		,pmUlTrafficVolumePs384
		,pmUlTrafficVolumePs64
		,pmUlTrafficVolumePs8
		,pmUlTrafficVolumePsCommon
		,pmUlTrafficVolumePsIntEul
		,pmDlTrafficVolumePs128
		,pmDlTrafficVolumePs16
		,pmDlTrafficVolumePs384
		,pmDlTrafficVolumePs64
		,pmDlTrafficVolumePs8
		,pmDlTrafficVolumePsCommon
		,pmDlTrafficVolumePsIntHs
		
	FROM ',GT_DB,'.table_pmDlTrafficVolumeCs
	WHERE CAST(RIGHT(LEFT(mts,10),2) AS SIGNED) >= ',STARTHOUR,' AND CAST(RIGHT(LEFT(mts,10),2) AS SIGNED) < ',ENDHOUR,'
		AND CONCAT(LEFT(LEFT(mts,10),4),''-'',SUBSTRING(LEFT(mts,10),5,2),''-'',SUBSTRING(LEFT(mts,10),7,2)) = ''',SESSION_DATE,'''
	;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	
	SET @SqlCmd=CONCAT('ALTER TABLE ',GT_DB,'.tmp_table_pmDlTrafficVolumeCs_',WORKER_ID,' ADD INDEX `ix_key`(neun,nedn,mts,moid);');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'SP_Generate_PM_counter_UMTS_Daily','tmp_table_pmDlTrafficVolumeCs_', NOW());	
	
	
	
	SET @SqlCmd=CONCAT(' Drop TEMPORARY table if exists ',GT_DB,'.tmp_pm_umts_6_',WORKER_ID,';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('
	CREATE TEMPORARY TABLE ',GT_DB,'.tmp_pm_umts_6_',WORKER_ID,' ENGINE=MYISAM AS 
	SELECT A.neun,
		A.nedn,
		A.mts,
		A.moid,
		A.DATA_DATE,
		A.DATA_HOUR,
		A.RNC_ID,
		A.CELL_ID,
		SUM(IFNULL(A.pmSumHsDlRlcUserPacketThp,0)) AS pmSumHsDlRlcUserPacketThp,
	
		SUM(IFNULL(A.pmSamplesHsDlRlcTotPacketThp,0)) AS pmSamplesHsDlRlcTotPacketThp,
		SUM(IFNULL(A.pmSamplesHsDlRlcUserPacketThp,0)) AS pmSamplesHsDlRlcUserPacketThp,
	
		SUM(IFNULL(A.pmSumHsDlRlcTotPacketThp,0)) AS pmSumHsDlRlcTotPacketThp,
	
		SUM(IFNULL(B.pmTotNoTermRrcConnectReq,0)) AS pmTotNoTermRrcConnectReq,
		SUM(IFNULL(B.pmTotNoTermRrcConnectReqCs,0)) AS pmTotNoTermRrcConnectReqCs,
		SUM(IFNULL(B.pmTotNoTermRrcConnectReqCsSucc,0)) AS pmTotNoTermRrcConnectReqCsSucc,
		
		SUM(IFNULL(B.pmTotNoTermRrcConnectReqSucc,0)) AS pmTotNoTermRrcConnectReqSucc
		
	FROM ',GT_DB,'.tmp_table_pmHsDowntimeMan_',WORKER_ID,' a LEFT JOIN ',GT_DB,'.tmp_table_pmSamplesRrcOnlyEstablish_',WORKER_ID,' b
		ON a.neun =b.neun
		AND a.nedn =b.nedn
		AND a.mts =b.mts
		AND a.moid =b.moid
	GROUP BY A.DATA_DATE,A.DATA_HOUR,A.RNC_ID,A.CELL_ID
	;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	SET @SqlCmd=CONCAT('ALTER TABLE ',GT_DB,'.tmp_pm_umts_6_',WORKER_ID,' ADD INDEX `ix_key`(DATA_DATE,DATA_HOUR,RNC_ID,CELL_ID);');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	
	SET @SqlCmd=CONCAT(' Drop TEMPORARY table if exists ',GT_DB,'.tmp_pm_umts_7_',WORKER_ID,';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('
	CREATE TEMPORARY TABLE ',GT_DB,'.tmp_pm_umts_7_',WORKER_ID,' ENGINE=MYISAM AS 
	SELECT A.DATA_DATE,
		A.DATA_HOUR,
		A.RNC_ID,
		A.CELL_ID,
		A.pmSumHsDlRlcUserPacketThp,
		A.pmSamplesHsDlRlcTotPacketThp,
		A.pmSamplesHsDlRlcUserPacketThp,
		A.pmSumHsDlRlcTotPacketThp,
		A.pmTotNoTermRrcConnectReq,
		A.pmTotNoTermRrcConnectReqCs,
		A.pmTotNoTermRrcConnectReqCsSucc,
		A.pmTotNoTermRrcConnectReqSucc,
		SUM(IFNULL(B.pmUlTrafficVolumePs128,0)) AS pmUlTrafficVolumePs128,
		SUM(IFNULL(B.pmUlTrafficVolumePs16,0)) AS pmUlTrafficVolumePs16,
		SUM(IFNULL(B.pmUlTrafficVolumePs384,0)) AS pmUlTrafficVolumePs384,
		SUM(IFNULL(B.pmUlTrafficVolumePs64,0)) AS pmUlTrafficVolumePs64,
		SUM(IFNULL(B.pmUlTrafficVolumePs8,0)) AS pmUlTrafficVolumePs8,
		SUM(IFNULL(B.pmUlTrafficVolumePsCommon,0)) AS pmUlTrafficVolumePsCommon,
		SUM(IFNULL(B.pmUlTrafficVolumePsIntEul,0)) AS pmUlTrafficVolumePsIntEul,
		SUM(IFNULL(B.pmDlTrafficVolumePs128,0)) AS pmDlTrafficVolumePs128,
		SUM(IFNULL(B.pmDlTrafficVolumePs16,0)) AS pmDlTrafficVolumePs16,
		SUM(IFNULL(B.pmDlTrafficVolumePs384,0)) AS pmDlTrafficVolumePs384,
		SUM(IFNULL(B.pmDlTrafficVolumePs64,0)) AS pmDlTrafficVolumePs64,
		SUM(IFNULL(B.pmDlTrafficVolumePs8,0)) AS pmDlTrafficVolumePs8,
		SUM(IFNULL(B.pmDlTrafficVolumePsCommon,0)) AS pmDlTrafficVolumePsCommon,
		SUM(IFNULL(B.pmDlTrafficVolumePsIntHs,0)) AS pmDlTrafficVolumePsIntHs
	FROM ',GT_DB,'.tmp_pm_umts_6_',WORKER_ID,' a LEFT JOIN ',GT_DB,'.tmp_table_pmDlTrafficVolumeCs_',WORKER_ID,' b
		ON a.neun =b.neun
		AND a.nedn =b.nedn
		AND a.mts =b.mts
		AND a.moid =b.moid
	GROUP BY A.DATA_DATE,A.DATA_HOUR,A.RNC_ID,A.CELL_ID
	;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	SET @SqlCmd=CONCAT('ALTER TABLE ',GT_DB,'.tmp_pm_umts_7_',WORKER_ID,' ADD INDEX `ix_key`(DATA_DATE,DATA_HOUR,RNC_ID,CELL_ID);');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	
	
	SET @SqlCmd=CONCAT(' Drop TEMPORARY table if exists ',GT_DB,'.tmp_pm_umts_8_',WORKER_ID,';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('
	CREATE TEMPORARY TABLE ',GT_DB,'.tmp_pm_umts_8_',WORKER_ID,' ENGINE=MYISAM AS 
	SELECT 
			a.`DATA_DATE`,
			a.`DATA_HOUR`,
			a.RNC_ID ,
			a.`CELL_ID`,             
		     `NoRrcConnectReqSuccess`,
		     `NoRrcConnectReq`,
		     `NoRrcConnectReqSuccessCs`,
		     `NoRrcConnectReqCs`,
		     `NoRabEstablishSuccessCS`,
		     `NoRabEstablishAttemptCS`,
		     `NoRrcConnectReqSuccessPs`,
		     `NoRrcConnectReqPs`,
		     `NoRabEstablishSuccessPS`,
		     `NoRabEstablishAttemptPS`,
		     `NoRrcReqBlock`,
		     `NoRabReqBlockCS`,
		     `NoSystemRabReleaseCS`,
		     `NoNormalRabReleaseCS`,
		     `NoSystemRabReleasePS`,
		     `NoNormalRabReleasePS`,
		     `NoRabReqBlockPS`,
		     `NoOfCONGCALLS`,
		     `UesWith1Rls1RlInAct`,
		     `UesWith1Rls2RlInAct2Rls2RlInAct`,
		     `UesWith1Rls3RlInActSet2Rls3RlInAct3Rls3RlInAct`,
		     `UesWith2Rls4RlInActSet3Rls4RlInAct4Rls4RlInAct`,
		     `Downtimeman`,
		     `Downtimeauto`,
			A.pmNoDirRetryAtt,
		A.pmNoNormalNasSignReleasePs,
		A.pmNoNormalRabReleasePacket,
		A.pmNoNormalRabReleaseSpeech,
		A.pmNoNormalRbReleaseHs,
		A.pmNoRabEstablishAttemptPacketInteractive,
		A.pmNoRabEstablishAttemptPacketInteractiveEul,
		A.pmNoRabEstablishAttemptPacketInteractiveHs,
		A.pmNoRabEstablishAttemptSpeech,
		A.pmNoRabEstablishSuccessPacketInteractive,
		A.pmNoRabEstablishSuccessPacketInteractiveEul,
		A.pmNoRabEstablishSuccessPacketInteractiveHs,
		A.pmNoRabEstablishSuccessSpeech,
		A.pmNoSuccRbReconfOrigPsIntDch,
		A.pmNoSystemNasSignReleasePs,
		A.pmNoSystemRabReleasePacket,
		A.pmNoSystemRabReleaseSpeech,
		A.pmNoSystemRbReleaseHs,
		A.pmPsIntHsToFachSucc,
		A.pmSamplesDchDlRlcUserPacketThp,
		A.pmSamplesDchUlRlcUserPacketThp,
		A.pmSumBestCs64RabEstablish,
		A.pmSumBestDchPsIntRabEstablish,
		A.pmSumBestPsEulRabEstablish,
		A.pmSumBestPsHsAdchRabEstablish,
		A.pmTotNoRrcConnectReq,
		A.pmTotNoRrcConnectReqCs,
		A.pmTotNoRrcConnectReqCsSucc,
		A.pmTotNoRrcConnectReqPs,
		A.pmTotNoRrcConnectReqPsSucc,
		A.pmTotNoRrcConnectReqSuccess,
		
		A.pmChSwitchSuccFachUra,
		A.pmNoNormalRabReleasePacketUra,
		A.pmNoSystemRabReleasePacketUra,
		A.pmNoTimesCellFailAddToActSet,
		A.pmNoTimesRlAddToActSet,
		A.pmSamplesUlRssi,
		A.pmSumUlRssi,
	
		A.pmSumBestCs12Establish,
		A.pmSumDchDlRlcUserPacketThp,
		A.pmSumDchUlRlcUserPacketThp,
	
		B.pmSumBestAmr12200RabEstablish,	
		B.pmSumUesWith1Rls1RlInActSet,
		B.pmSumUesWith1Rls2RlInActSet,
		B.pmSumUesWith1Rls3RlInActSet,
		B.pmSumUesWith2Rls2RlInActSet,
		B.pmSumUesWith2Rls3RlInActSet,
		B.pmSumUesWith3Rls3RlInActSet,
	
		A.pmNoRabEstablishSuccessPacketStream,
		A.pmNoRabEstablishAttemptPacketStream,
		A.pmNoRrcCsReqDeniedAdm,
		A.pmNoOfNonHoReqDeniedSpeech,
		A.pmNoNormalRabReleasePacketStream,
		A.pmNoOfNonHoReqDeniedInteractive,
		A.pmNoRabEstablishSuccessCS64,
		A.pmNoRabEstablishAttemptCS64,
		A.pmNoRabEstablishSuccessPacketStream128,
		A.pmNoRabEstablishAttemptPacketStream128,
		A.pmNoSystemRabReleaseCs64,
		A.pmNoNormalRabReleaseCs64,
		A.pmNoNormalRabReleasePacketStream128,
		A.pmNoRabEstablishSuccessCs57,
		A.pmNoRabEstablishAttemptCs57,
		A.pmNoOfNonHoReqDeniedCs,
		A.pmNoSystemRabReleaseCsStream,
		A.pmNoNormalRabReleaseCsStream,
		A.pmNoNormalRabReleasePsStreamHs,
		A.pmNoRrcConnReqBlockTnCs,
		A.pmNoRabEstBlockTnSpeechBest,
		A.pmNoRabEstBlockTnCs57Best,
		A.pmNoRabEstBlockTnCs64Best,
		B.pmSumUesWith2Rls4RlInActSet,
		B.pmSumUesWith3Rls4RlInActSet,
		B.pmSumUesWith4Rls4RlInActSet	
	
		FROM ',GT_DB,'.tmp_pm_umts_1_',WORKER_ID,' a  JOIN ',GT_DB,'.tmp_pm_umts_2_',WORKER_ID,' b  
		ON 
		  a.`DATA_DATE`=b.`DATA_DATE` AND 
		  a.`DATA_HOUR`=b.`DATA_HOUR`  AND 
		  a.`RNC_ID`=b.`RNC_ID` AND  
		  a.`CELL_ID`=b.`CELL_ID`	
	;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	SET @SqlCmd=CONCAT('ALTER TABLE ',GT_DB,'.tmp_pm_umts_8_',WORKER_ID,' ADD INDEX `ix_key`(DATA_DATE,DATA_HOUR,RNC_ID,CELL_ID);');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT(' Drop TEMPORARY table if exists ',GT_DB,'.tmp_pm_umts_9_',WORKER_ID,';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('
	CREATE TEMPORARY TABLE ',GT_DB,'.tmp_pm_umts_9_',WORKER_ID,' ENGINE=MYISAM AS 
	SELECT 
			a.`DATA_DATE`,
			a.`DATA_HOUR`,
			a.RNC_ID ,
			a.`CELL_ID`,             
		     a.`NoRrcConnectReqSuccess`,
		     a.`NoRrcConnectReq`,
		     a.`NoRrcConnectReqSuccessCs`,
		     a.`NoRrcConnectReqCs`,
		     a.`NoRabEstablishSuccessCS`,
		     a.`NoRabEstablishAttemptCS`,
		     a.`NoRrcConnectReqSuccessPs`,
		     a.`NoRrcConnectReqPs`,
		     a.`NoRabEstablishSuccessPS`,
		     a.`NoRabEstablishAttemptPS`,
		     a.`NoRrcReqBlock`,
		     a.`NoRabReqBlockCS`,
		     a.`NoSystemRabReleaseCS`,
		     a.`NoNormalRabReleaseCS`,
		     a.`NoSystemRabReleasePS`,
		     a.`NoNormalRabReleasePS`,
			b.`NoSuccessOutHo`,
			b.`NoAttOutHo`,
		     a.`NoRabReqBlockPS`,
		     a.`NoOfCONGCALLS`,
		     a.`UesWith1Rls1RlInAct`,
		     a.`UesWith1Rls2RlInAct2Rls2RlInAct`,
		     a.`UesWith1Rls3RlInActSet2Rls3RlInAct3Rls3RlInAct`,
		     a.`UesWith2Rls4RlInActSet3Rls4RlInAct4Rls4RlInAct`,
			b.pmNoSuccessOutIratHoSpeech,
			b.pmNoAttOutIratHoSpeech,
		     a.`Downtimeman`,
		     a.`Downtimeauto`,
		a.pmNoDirRetryAtt,
		A.pmNoNormalNasSignReleasePs,
		A.pmNoNormalRabReleasePacket,
		A.pmNoNormalRabReleaseSpeech,
		A.pmNoNormalRbReleaseHs,
		A.pmNoRabEstablishAttemptPacketInteractive,
		A.pmNoRabEstablishAttemptPacketInteractiveEul,
		A.pmNoRabEstablishAttemptPacketInteractiveHs,
		A.pmNoRabEstablishAttemptSpeech,
		A.pmNoRabEstablishSuccessPacketInteractive,
		A.pmNoRabEstablishSuccessPacketInteractiveEul,
		A.pmNoRabEstablishSuccessPacketInteractiveHs,
		A.pmNoRabEstablishSuccessSpeech,
		A.pmNoSuccRbReconfOrigPsIntDch,
		A.pmNoSystemNasSignReleasePs,
		A.pmNoSystemRabReleasePacket,
		A.pmNoSystemRabReleaseSpeech,
		A.pmNoSystemRbReleaseHs,
		A.pmPsIntHsToFachSucc,
		A.pmSamplesDchDlRlcUserPacketThp,
		A.pmSamplesDchUlRlcUserPacketThp,
		A.pmSumBestCs64RabEstablish,
		A.pmSumBestDchPsIntRabEstablish,
		A.pmSumBestPsEulRabEstablish,
		A.pmSumBestPsHsAdchRabEstablish,
		A.pmTotNoRrcConnectReq,
		A.pmTotNoRrcConnectReqCs,
		A.pmTotNoRrcConnectReqCsSucc,
		A.pmTotNoRrcConnectReqPs,
		A.pmTotNoRrcConnectReqPsSucc,
		A.pmTotNoRrcConnectReqSuccess,
		
		A.pmChSwitchSuccFachUra,
		A.pmNoNormalRabReleasePacketUra,
		A.pmNoSystemRabReleasePacketUra,
		A.pmNoTimesCellFailAddToActSet,
		A.pmNoTimesRlAddToActSet,
		A.pmSamplesUlRssi,
		A.pmSumUlRssi,
	
		A.pmSumBestCs12Establish,
		A.pmSumDchDlRlcUserPacketThp,
		A.pmSumDchUlRlcUserPacketThp,
	
		a.pmSumBestAmr12200RabEstablish,	
		a.pmSumUesWith1Rls1RlInActSet,
		a.pmSumUesWith1Rls2RlInActSet,
		a.pmSumUesWith1Rls3RlInActSet,
		a.pmSumUesWith2Rls2RlInActSet,
		a.pmSumUesWith2Rls3RlInActSet,
		a.pmSumUesWith3Rls3RlInActSet,
	
		A.pmNoRabEstablishSuccessPacketStream,
		A.pmNoRabEstablishAttemptPacketStream,
		A.pmNoRrcCsReqDeniedAdm,
		A.pmNoOfNonHoReqDeniedSpeech,
		A.pmNoNormalRabReleasePacketStream,
		A.pmNoOfNonHoReqDeniedInteractive,
		A.pmNoRabEstablishSuccessCS64,
		A.pmNoRabEstablishAttemptCS64,
		A.pmNoRabEstablishSuccessPacketStream128,
		A.pmNoRabEstablishAttemptPacketStream128,
		A.pmNoSystemRabReleaseCs64,
		A.pmNoNormalRabReleaseCs64,
		A.pmNoNormalRabReleasePacketStream128,
		A.pmNoRabEstablishSuccessCs57,
		A.pmNoRabEstablishAttemptCs57,
		A.pmNoOfNonHoReqDeniedCs,
		A.pmNoSystemRabReleaseCsStream,
		A.pmNoNormalRabReleaseCsStream,
		A.pmNoNormalRabReleasePsStreamHs,
		A.pmNoRrcConnReqBlockTnCs,
		A.pmNoRabEstBlockTnSpeechBest,
		A.pmNoRabEstBlockTnCs57Best,
		A.pmNoRabEstBlockTnCs64Best,
		A.pmSumUesWith2Rls4RlInActSet,
		A.pmSumUesWith3Rls4RlInActSet,
		A.pmSumUesWith4Rls4RlInActSet,	
		B.pmNoSuccessOutIratHoMulti,
		B.pmNoAttOutIratHoMulti,
		B.pmNoAttOutSbHoSpeech,
		B.pmNoSuccessOutSbHoSpeech
	
		FROM ',GT_DB,'.tmp_pm_umts_8_',WORKER_ID,' a  LEFT JOIN ',GT_DB,'.tmp_pm_umts_3_',WORKER_ID,' b  
		ON 
		  a.`DATA_DATE`=b.`DATA_DATE` AND 
		  a.`DATA_HOUR`=b.`DATA_HOUR`  AND 
		  a.`RNC_ID`=b.`RNC_ID` AND  
		  a.`CELL_ID`=b.`CELL_ID`	
	;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	SET @SqlCmd=CONCAT('ALTER TABLE ',GT_DB,'.tmp_pm_umts_9_',WORKER_ID,' ADD INDEX `ix_key`(DATA_DATE,DATA_HOUR,RNC_ID,CELL_ID);');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	
	SET @SqlCmd=CONCAT(' Drop TEMPORARY table if exists ',GT_DB,'.tmp_table_pm_counter_umts_Ericsson_',WORKER_ID,';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	SET @SqlCmd=CONCAT('CREATE TEMPORARY TABLE ',GT_DB,'.tmp_table_pm_counter_umts_Ericsson_',WORKER_ID,'
					(
					`DATA_DATE` DATE DEFAULT NULL,
					`DATA_HOUR` TINYINT(4) DEFAULT NULL,
					`RNC_ID` VARCHAR(20) DEFAULT NULL,
					`CELL_ID` VARCHAR(20) DEFAULT NULL,
					NoRrcConnectReqSuccess INT(11) DEFAULT NULL,
					NoRrcConnectReq INT(11) DEFAULT NULL,
					NoRrcConnectReqSuccessCs INT(11) DEFAULT NULL,
					NoRrcConnectReqCs INT(11) DEFAULT NULL,
					NoRabEstablishSuccessCS INT(11) DEFAULT NULL,
					NoRabEstablishAttemptCS INT(11) DEFAULT NULL,
					NoRrcConnectReqSuccessPs INT(11) DEFAULT NULL,
					NoRrcConnectReqPs INT(11) DEFAULT NULL,
					NoRabEstablishSuccessPS INT(11) DEFAULT NULL,
					NoRabEstablishAttemptPS INT(11) DEFAULT NULL,
					NoRrcReqBlock INT(11) DEFAULT NULL,
					NoRabReqBlockCS INT(11) DEFAULT NULL,
					NoSystemRabReleaseCS INT(11) DEFAULT NULL,
					NoNormalRabReleaseCS INT(11) DEFAULT NULL,
					NoSystemRabReleasePS INT(11) DEFAULT NULL,
					NoNormalRabReleasePS INT(11) DEFAULT NULL,
					NoSuccessOutHo INT(11) DEFAULT NULL,
					NoAttOutHo INT(11) DEFAULT NULL,
					NoRabReqBlockPS INT(11) DEFAULT NULL,
					NoOfCONGCALLS INT(11) DEFAULT NULL,
					UesWith1Rls1RlInAct INT(11) DEFAULT NULL,
					UesWith1Rls2RlInAct2Rls2RlInAct INT(11) DEFAULT NULL,
					UesWith1Rls3RlInActSet2Rls3RlInAct3Rls3RlInAct INT(11) DEFAULT NULL,
					UesWith2Rls4RlInActSet3Rls4RlInAct4Rls4RlInAct INT(11) DEFAULT NULL,
					UesWithmulRlsmulRlInAct INT(11) DEFAULT NULL,
					pmNoSuccessOutIratHoSpeech INT(11) DEFAULT 0,
					pmNoAttOutIratHoSpeech INT(11) DEFAULT 0,
					Downtimeman INT(11) DEFAULT NULL,
					Downtimeauto INT(11) DEFAULT NULL,
					pmChSwitchSuccFachUra VARCHAR(100) DEFAULT NULL,
					pmNoDirRetryAtt VARCHAR(100) DEFAULT NULL,
					pmNoNormalNasSignReleasePs VARCHAR(100) DEFAULT NULL,
					pmNoNormalRabReleasePacket VARCHAR(100) DEFAULT NULL,
					pmNoNormalRabReleasePacketUra VARCHAR(100) DEFAULT NULL,
					pmNoNormalRabReleaseSpeech VARCHAR(100) DEFAULT NULL,
					pmNoNormalRbReleaseHs VARCHAR(100) DEFAULT NULL,
					pmNoRabEstablishAttemptPacketInteractive VARCHAR(100) DEFAULT NULL,
					pmNoRabEstablishAttemptPacketInteractiveEul VARCHAR(100) DEFAULT NULL,
					pmNoRabEstablishAttemptPacketInteractiveHs VARCHAR(100) DEFAULT NULL,
					pmNoRabEstablishAttemptSpeech VARCHAR(100) DEFAULT NULL,
					pmNoRabEstablishSuccessPacketInteractive VARCHAR(100) DEFAULT NULL,
					pmNoRabEstablishSuccessPacketInteractiveEul VARCHAR(100) DEFAULT NULL,
					pmNoRabEstablishSuccessPacketInteractiveHs VARCHAR(100) DEFAULT NULL,
					pmNoRabEstablishSuccessSpeech VARCHAR(100) DEFAULT NULL,
					pmNoSuccRbReconfOrigPsIntDch VARCHAR(100) DEFAULT NULL,
					pmNoSystemNasSignReleasePs VARCHAR(100) DEFAULT NULL,
					pmNoSystemRabReleasePacket VARCHAR(100) DEFAULT NULL,
					pmNoSystemRabReleasePacketUra VARCHAR(100) DEFAULT NULL,
					pmNoSystemRabReleaseSpeech VARCHAR(100) DEFAULT NULL,
					pmNoSystemRbReleaseHs VARCHAR(100) DEFAULT NULL,
					pmNoTimesCellFailAddToActSet VARCHAR(100) DEFAULT NULL,
					pmNoTimesRlAddToActSet VARCHAR(100) DEFAULT NULL,
					pmPsIntHsToFachSucc VARCHAR(100) DEFAULT NULL,
					pmSamplesDchDlRlcUserPacketThp VARCHAR(100) DEFAULT NULL,
					pmSamplesDchUlRlcUserPacketThp VARCHAR(100) DEFAULT NULL,
					pmSamplesHsDlRlcTotPacketThp VARCHAR(100) DEFAULT NULL,
					pmSamplesHsDlRlcUserPacketThp VARCHAR(100) DEFAULT NULL,
					pmSamplesUlRssi VARCHAR(100) DEFAULT NULL,
					pmSumBestAmr12200RabEstablish VARCHAR(100) DEFAULT NULL,
					pmSumBestCs64RabEstablish VARCHAR(100) DEFAULT NULL,
					pmSumBestDchPsIntRabEstablish VARCHAR(100) DEFAULT NULL,
					pmSumBestPsEulRabEstablish VARCHAR(100) DEFAULT NULL,
					pmSumBestPsHsAdchRabEstablish VARCHAR(100) DEFAULT NULL,
					pmSumUesWith1Rls1RlInActSet VARCHAR(100) DEFAULT NULL,
					pmSumUesWith1Rls2RlInActSet VARCHAR(100) DEFAULT NULL,
					pmSumUesWith1Rls3RlInActSet VARCHAR(100) DEFAULT NULL,
					pmSumUesWith2Rls2RlInActSet VARCHAR(100) DEFAULT NULL,
					pmSumUesWith2Rls3RlInActSet VARCHAR(100) DEFAULT NULL,
					pmSumUesWith3Rls3RlInActSet VARCHAR(100) DEFAULT NULL,
					pmSumUlRssi VARCHAR(100) DEFAULT NULL,
					pmTotNoRrcConnectReq VARCHAR(100) DEFAULT NULL,
					pmTotNoRrcConnectReqCs VARCHAR(100) DEFAULT NULL,
					pmTotNoRrcConnectReqCsSucc VARCHAR(100) DEFAULT NULL,
					pmTotNoRrcConnectReqPs VARCHAR(100) DEFAULT NULL,
					pmTotNoRrcConnectReqPsSucc VARCHAR(100) DEFAULT NULL,
					pmTotNoRrcConnectReqSuccess VARCHAR(100) DEFAULT NULL,
					pmTotNoTermRrcConnectReq VARCHAR(100) DEFAULT NULL,
					pmTotNoTermRrcConnectReqCs VARCHAR(100) DEFAULT NULL,
					pmTotNoTermRrcConnectReqCsSucc VARCHAR(100) DEFAULT NULL,
					pmUlTrafficVolumePs128 VARCHAR(100) DEFAULT NULL,
					pmUlTrafficVolumePs16 VARCHAR(100) DEFAULT NULL,
					pmUlTrafficVolumePs384 VARCHAR(100) DEFAULT NULL,
					pmUlTrafficVolumePs64 VARCHAR(100) DEFAULT NULL,
					pmUlTrafficVolumePs8 VARCHAR(100) DEFAULT NULL,
					pmUlTrafficVolumePsCommon VARCHAR(100) DEFAULT NULL,
					pmUlTrafficVolumePsIntEul VARCHAR(100) DEFAULT NULL,
					pmDlTrafficVolumePs128 VARCHAR(100) DEFAULT NULL,
					pmDlTrafficVolumePs16 VARCHAR(100) DEFAULT NULL,
					pmDlTrafficVolumePs384 VARCHAR(100) DEFAULT NULL,
					pmDlTrafficVolumePs64 VARCHAR(100) DEFAULT NULL,
					pmDlTrafficVolumePs8 VARCHAR(100) DEFAULT NULL,
					pmDlTrafficVolumePsCommon VARCHAR(100) DEFAULT NULL,
					pmDlTrafficVolumePsIntHs VARCHAR(100) DEFAULT NULL,
	
					pmTotNoTermRrcConnectReqSucc VARCHAR(100) DEFAULT NULL,
					pmSumBestCs12Establish VARCHAR(100) DEFAULT NULL,
					pmSumHsDlRlcTotPacketThp VARCHAR(100) DEFAULT NULL,
					pmSumDchDlRlcUserPacketThp VARCHAR(100) DEFAULT NULL,
					pmSumDchUlRlcUserPacketThp VARCHAR(100) DEFAULT NULL,
					pmSumHsDlRlcUserPacketThp VARCHAR(100) DEFAULT NULL,
	
					pmNoRabEstablishSuccessPacketStream VARCHAR(100) DEFAULT NULL,
					pmNoRabEstablishAttemptPacketStream VARCHAR(100) DEFAULT NULL,
					pmNoRrcCsReqDeniedAdm VARCHAR(100) DEFAULT NULL,
					pmNoOfNonHoReqDeniedSpeech VARCHAR(100) DEFAULT NULL,
					pmNoNormalRabReleasePacketStream VARCHAR(100) DEFAULT NULL,
					pmNoOfNonHoReqDeniedInteractive VARCHAR(100) DEFAULT NULL,
					pmNoRabEstablishSuccessCS64 VARCHAR(100) DEFAULT NULL,
					pmNoRabEstablishAttemptCS64 VARCHAR(100) DEFAULT NULL,
					pmNoRabEstablishSuccessPacketStream128 VARCHAR(100) DEFAULT NULL,
					pmNoRabEstablishAttemptPacketStream128 VARCHAR(100) DEFAULT NULL,
					pmNoRrcConnReqBlockTnCs VARCHAR(100) DEFAULT NULL,
					pmNoRabEstBlockTnSpeechBest VARCHAR(100) DEFAULT NULL,
					pmNoSystemRabReleaseCs64 VARCHAR(100) DEFAULT NULL,
					pmNoNormalRabReleaseCs64 VARCHAR(100) DEFAULT NULL,
					pmNoNormalRabReleasePacketStream128 VARCHAR(100) DEFAULT NULL,
					pmNoSuccessOutIratHoMulti VARCHAR(100) DEFAULT NULL,
					pmNoAttOutIratHoMulti VARCHAR(100) DEFAULT NULL,
					pmNoRabEstablishSuccessCs57 VARCHAR(100) DEFAULT NULL,
					pmNoRabEstablishAttemptCs57 VARCHAR(100) DEFAULT NULL,
					pmNoOfNonHoReqDeniedCs VARCHAR(100) DEFAULT NULL,
					pmNoSystemRabReleaseCsStream VARCHAR(100) DEFAULT NULL,
					pmNoNormalRabReleaseCsStream VARCHAR(100) DEFAULT NULL,
					pmNoNormalRabReleasePsStreamHs VARCHAR(100) DEFAULT NULL,
					pmNoRabEstBlockTnCs57Best VARCHAR(100) DEFAULT NULL,
					pmNoRabEstBlockTnCs64Best VARCHAR(100) DEFAULT NULL,
					pmSumUesWith2Rls4RlInActSet VARCHAR(100) DEFAULT NULL,
					pmSumUesWith3Rls4RlInActSet VARCHAR(100) DEFAULT NULL,
					pmSumUesWith4Rls4RlInActSet VARCHAR(100) DEFAULT NULL,					
					pmNoAttOutSbHoSpeech  VARCHAR(100) DEFAULT 0,	
					pmNoSuccessOutSbHoSpeech  VARCHAR(100) DEFAULT 0
					) ENGINE=MYISAM;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt; 
	
	
	SET @SqlCmd=CONCAT('INSERT INTO  ',GT_DB,'.tmp_table_pm_counter_umts_Ericsson_',WORKER_ID,' 
			(
		     `DATA_DATE`,
		     `DATA_HOUR`,
		     `RNC_ID` ,
		     `CELL_ID` ,
		     `NoRrcConnectReqSuccess`,
		     `NoRrcConnectReq`,
		     `NoRrcConnectReqSuccessCs`,
		     `NoRrcConnectReqCs`,
		     `NoRabEstablishSuccessCS`,
		     `NoRabEstablishAttemptCS`,
		     `NoRrcConnectReqSuccessPs`,
		     `NoRrcConnectReqPs`,
		     `NoRabEstablishSuccessPS`,
		     `NoRabEstablishAttemptPS`,
		     `NoRrcReqBlock`,
		     `NoRabReqBlockCS`,
		     `NoSystemRabReleaseCS`,
		     `NoNormalRabReleaseCS`,
		     `NoSystemRabReleasePS`,
		     `NoNormalRabReleasePS`,
		     `NoSuccessOutHo`,
		     `NoAttOutHo`,
		     `NoRabReqBlockPS`,
		     `NoOfCONGCALLS`,
		     `UesWith1Rls1RlInAct`,
		     `UesWith1Rls2RlInAct2Rls2RlInAct`,
		     `UesWith1Rls3RlInActSet2Rls3RlInAct3Rls3RlInAct`,
		     `UesWith2Rls4RlInActSet3Rls4RlInAct4Rls4RlInAct`,
		     `pmNoSuccessOutIratHoSpeech`,
		     `pmNoAttOutIratHoSpeech`,
		     `Downtimeman`,
		     `Downtimeauto`,
			pmChSwitchSuccFachUra, 
			pmNoDirRetryAtt, 
			pmNoNormalNasSignReleasePs, 
			pmNoNormalRabReleasePacket, 
			pmNoNormalRabReleasePacketUra, 
			pmNoNormalRabReleaseSpeech, 
			pmNoNormalRbReleaseHs, 
			pmNoRabEstablishAttemptPacketInteractive, 
			pmNoRabEstablishAttemptPacketInteractiveEul, 
			pmNoRabEstablishAttemptPacketInteractiveHs, 
			pmNoRabEstablishAttemptSpeech, 
			pmNoRabEstablishSuccessPacketInteractive, 
			pmNoRabEstablishSuccessPacketInteractiveEul, 
			pmNoRabEstablishSuccessPacketInteractiveHs, 
			pmNoRabEstablishSuccessSpeech, 
			pmNoSuccRbReconfOrigPsIntDch, 
			pmNoSystemNasSignReleasePs, 
			pmNoSystemRabReleasePacket, 
			pmNoSystemRabReleasePacketUra, 
			pmNoSystemRabReleaseSpeech, 
			pmNoSystemRbReleaseHs, 
			pmNoTimesCellFailAddToActSet, 
			pmNoTimesRlAddToActSet, 
			pmPsIntHsToFachSucc, 
			pmSamplesDchDlRlcUserPacketThp, 
			pmSamplesDchUlRlcUserPacketThp, 
			pmSamplesHsDlRlcTotPacketThp, 
			pmSamplesHsDlRlcUserPacketThp, 
			pmSamplesUlRssi, 
			pmSumBestAmr12200RabEstablish, 
			pmSumBestCs64RabEstablish, 
			pmSumBestDchPsIntRabEstablish, 
			pmSumBestPsEulRabEstablish, 
			pmSumBestPsHsAdchRabEstablish, 
			pmSumUesWith1Rls1RlInActSet, 
			pmSumUesWith1Rls2RlInActSet, 
			pmSumUesWith1Rls3RlInActSet, 
			pmSumUesWith2Rls2RlInActSet, 
			pmSumUesWith2Rls3RlInActSet, 
			pmSumUesWith3Rls3RlInActSet, 
			pmSumUlRssi, 
			pmTotNoRrcConnectReq, 
			pmTotNoRrcConnectReqCs, 
			pmTotNoRrcConnectReqCsSucc, 
			pmTotNoRrcConnectReqPs, 
			pmTotNoRrcConnectReqPsSucc, 
			pmTotNoRrcConnectReqSuccess, 
			pmTotNoTermRrcConnectReq, 
			pmTotNoTermRrcConnectReqCs, 
			pmTotNoTermRrcConnectReqCsSucc,
			pmUlTrafficVolumePs128,
			pmUlTrafficVolumePs16, 
			pmUlTrafficVolumePs384, 
			pmUlTrafficVolumePs64, 
			pmUlTrafficVolumePs8, 
			pmUlTrafficVolumePsCommon, 
			pmUlTrafficVolumePsIntEul, 
			pmDlTrafficVolumePs128, 
			pmDlTrafficVolumePs16, 
			pmDlTrafficVolumePs384, 
			pmDlTrafficVolumePs64, 
			pmDlTrafficVolumePs8, 
			pmDlTrafficVolumePsCommon, 
			pmDlTrafficVolumePsIntHs,
			pmTotNoTermRrcConnectReqSucc,
			pmSumBestCs12Establish,
			pmSumHsDlRlcTotPacketThp ,
			pmSumDchDlRlcUserPacketThp,
			pmSumDchUlRlcUserPacketThp,
			pmSumHsDlRlcUserPacketThp,
	
			pmNoRabEstablishSuccessPacketStream,
			pmNoRabEstablishAttemptPacketStream,
			pmNoRrcCsReqDeniedAdm,
			pmNoOfNonHoReqDeniedSpeech,
			pmNoNormalRabReleasePacketStream,
			pmNoOfNonHoReqDeniedInteractive,
			pmNoRabEstablishSuccessCS64,
			pmNoRabEstablishAttemptCS64,
			pmNoRabEstablishSuccessPacketStream128,
			pmNoRabEstablishAttemptPacketStream128,
			pmNoSystemRabReleaseCs64,
			pmNoNormalRabReleaseCs64,
			pmNoNormalRabReleasePacketStream128,
			pmNoRabEstablishSuccessCs57,
			pmNoRabEstablishAttemptCs57,
			pmNoOfNonHoReqDeniedCs,
			pmNoSystemRabReleaseCsStream,
			pmNoNormalRabReleaseCsStream,
			pmNoNormalRabReleasePsStreamHs,
			pmNoRrcConnReqBlockTnCs,
			pmNoRabEstBlockTnSpeechBest,
			pmNoRabEstBlockTnCs57Best,
			pmNoRabEstBlockTnCs64Best,
			pmSumUesWith2Rls4RlInActSet,
			pmSumUesWith3Rls4RlInActSet,
			pmSumUesWith4Rls4RlInActSet,	
			pmNoSuccessOutIratHoMulti,
			pmNoAttOutIratHoMulti,			
			pmNoAttOutSbHoSpeech,
			pmNoSuccessOutSbHoSpeech
			)
		SELECT 
			a.`DATA_DATE`,
			a.`DATA_HOUR`,
			a.RNC_ID ,
			a.`CELL_ID`,             
		      B.`NoRrcConnectReqSuccess`,
		     B.`NoRrcConnectReq`,
		      B.`NoRrcConnectReqSuccessCs`,
		      B.`NoRrcConnectReqCs`,
		      B.`NoRabEstablishSuccessCS`,
		     B.`NoRabEstablishAttemptCS`,
		     B.`NoRrcConnectReqSuccessPs`,
		     B.`NoRrcConnectReqPs`,
		     B.`NoRabEstablishSuccessPS`,
		     B.`NoRabEstablishAttemptPS`,
		     B.`NoRrcReqBlock`,
		     B.`NoRabReqBlockCS`,
		     B.`NoSystemRabReleaseCS`,
		     B.`NoNormalRabReleaseCS`,
		     B.`NoSystemRabReleasePS`,
		     B.`NoNormalRabReleasePS`,
	             B.`NoSuccessOutHo`,
	             B.`NoAttOutHo`,
		     B.`NoRabReqBlockPS`,
		     B.`NoOfCONGCALLS`,
		     B.`UesWith1Rls1RlInAct`,
		     B.`UesWith1Rls2RlInAct2Rls2RlInAct`,
		     B.`UesWith1Rls3RlInActSet2Rls3RlInAct3Rls3RlInAct`,
		     B.`UesWith2Rls4RlInActSet3Rls4RlInAct4Rls4RlInAct`,
		     b.`pmNoSuccessOutIratHoSpeech`,
		     b.`pmNoAttOutIratHoSpeech`,
		     B.`Downtimeman`,
		     B.`Downtimeauto`,
			B.pmChSwitchSuccFachUra,
			B.pmNoDirRetryAtt,
			B.pmNoNormalNasSignReleasePs,
			B.pmNoNormalRabReleasePacket,
			B.pmNoNormalRabReleasePacketUra,
			B.pmNoNormalRabReleaseSpeech,
			B.pmNoNormalRbReleaseHs,
			B.pmNoRabEstablishAttemptPacketInteractive,
			B.pmNoRabEstablishAttemptPacketInteractiveEul,
			B.pmNoRabEstablishAttemptPacketInteractiveHs,
			B.pmNoRabEstablishAttemptSpeech,
			B.pmNoRabEstablishSuccessPacketInteractive,
			B.pmNoRabEstablishSuccessPacketInteractiveEul,
			B.pmNoRabEstablishSuccessPacketInteractiveHs,
			B.pmNoRabEstablishSuccessSpeech,
			B.pmNoSuccRbReconfOrigPsIntDch,
			B.pmNoSystemNasSignReleasePs,
			B.pmNoSystemRabReleasePacket,
			B.pmNoSystemRabReleasePacketUra,
			B.pmNoSystemRabReleaseSpeech,
			B.pmNoSystemRbReleaseHs,
			B.pmNoTimesCellFailAddToActSet,
			B.pmNoTimesRlAddToActSet,
			B.pmPsIntHsToFachSucc,
			B.pmSamplesDchDlRlcUserPacketThp,
			B.pmSamplesDchUlRlcUserPacketThp,
			A.pmSamplesHsDlRlcTotPacketThp,
			A.pmSamplesHsDlRlcUserPacketThp,
			B.pmSamplesUlRssi,
			B.pmSumBestAmr12200RabEstablish,
			B.pmSumBestCs64RabEstablish,
			B.pmSumBestDchPsIntRabEstablish,
			B.pmSumBestPsEulRabEstablish,
			B.pmSumBestPsHsAdchRabEstablish,
			B.pmSumUesWith1Rls1RlInActSet,
			B.pmSumUesWith1Rls2RlInActSet,
			B.pmSumUesWith1Rls3RlInActSet,
			B.pmSumUesWith2Rls2RlInActSet,
			B.pmSumUesWith2Rls3RlInActSet,
			B.pmSumUesWith3Rls3RlInActSet,
			B.pmSumUlRssi,
			B.pmTotNoRrcConnectReq,
			B.pmTotNoRrcConnectReqCs,
			B.pmTotNoRrcConnectReqCsSucc,
			B.pmTotNoRrcConnectReqPs,
			B.pmTotNoRrcConnectReqPsSucc,
			B.pmTotNoRrcConnectReqSuccess,
			A.pmTotNoTermRrcConnectReq,
			A.pmTotNoTermRrcConnectReqCs,
			A.pmTotNoTermRrcConnectReqCsSucc,
			A.pmUlTrafficVolumePs128,
			A.pmUlTrafficVolumePs16,
			A.pmUlTrafficVolumePs384,
			A.pmUlTrafficVolumePs64,
			A.pmUlTrafficVolumePs8,
			A.pmUlTrafficVolumePsCommon,
			A.pmUlTrafficVolumePsIntEul,
			A.pmDlTrafficVolumePs128,
			A.pmDlTrafficVolumePs16,
			A.pmDlTrafficVolumePs384,
			A.pmDlTrafficVolumePs64,
			A.pmDlTrafficVolumePs8,
			A.pmDlTrafficVolumePsCommon,
			A.pmDlTrafficVolumePsIntHs,
	
			A.pmTotNoTermRrcConnectReqSucc,
			B.pmSumBestCs12Establish,
			A.pmSumHsDlRlcTotPacketThp ,
			B.pmSumDchDlRlcUserPacketThp,
			B.pmSumDchUlRlcUserPacketThp,
			A.pmSumHsDlRlcUserPacketThp,
	
			B.pmNoRabEstablishSuccessPacketStream,
			B.pmNoRabEstablishAttemptPacketStream,
			B.pmNoRrcCsReqDeniedAdm,
			B.pmNoOfNonHoReqDeniedSpeech,
			B.pmNoNormalRabReleasePacketStream,
			B.pmNoOfNonHoReqDeniedInteractive,
			B.pmNoRabEstablishSuccessCS64,
			B.pmNoRabEstablishAttemptCS64,
			B.pmNoRabEstablishSuccessPacketStream128,
			B.pmNoRabEstablishAttemptPacketStream128,
			B.pmNoSystemRabReleaseCs64,
			B.pmNoNormalRabReleaseCs64,
			B.pmNoNormalRabReleasePacketStream128,
			B.pmNoRabEstablishSuccessCs57,
			B.pmNoRabEstablishAttemptCs57,
			B.pmNoOfNonHoReqDeniedCs,
			B.pmNoSystemRabReleaseCsStream,
			B.pmNoNormalRabReleaseCsStream,
			B.pmNoNormalRabReleasePsStreamHs,
			B.pmNoRrcConnReqBlockTnCs,
			B.pmNoRabEstBlockTnSpeechBest,
			B.pmNoRabEstBlockTnCs57Best,
			B.pmNoRabEstBlockTnCs64Best,
			B.pmSumUesWith2Rls4RlInActSet,
			B.pmSumUesWith3Rls4RlInActSet,
			B.pmSumUesWith4Rls4RlInActSet,	
			B.pmNoSuccessOutIratHoMulti,
			B.pmNoAttOutIratHoMulti,		
			B.pmNoAttOutSbHoSpeech,
			B.pmNoSuccessOutSbHoSpeech
		FROM ',GT_DB,'.tmp_pm_umts_7_',WORKER_ID,' a  JOIN ',GT_DB,'.tmp_pm_umts_9_',WORKER_ID,' b  
		ON 
		  a.`DATA_DATE`=b.`DATA_DATE` AND 
		  a.`DATA_HOUR`=b.`DATA_HOUR`  AND 
		  a.`RNC_ID`=b.`RNC_ID` AND  
		  a.`CELL_ID`=b.`CELL_ID`	     
		;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt; 
		
	SET @SqlCmd=CONCAT('ALTER TABLE ',GT_DB,'.tmp_table_pm_counter_umts_Ericsson_',WORKER_ID,' ADD INDEX `ix_key`(DATA_DATE,DATA_HOUR,RNC_ID,CELL_ID);');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'SP_Generate_PM_counter_UMTS_Daily','Update NT Ercisson', NOW());
	
	SET @SqlCmd=CONCAT(' UPDATE ',GT_DB,'.`tmp_table_pm_counter_umts_Ericsson_',WORKER_ID,'` A, 
				    ',CURRENT_NT_DB,'.nt_rnc_current B
			     SET A.RNC_ID=B.RNC_ID
				WHERE A.RNC_ID=B.RNC_NAME;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	SET @SqlCmd=CONCAT(' Drop TEMPORARY table if exists ',GT_DB,'.tmp_nt_',WORKER_ID,';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('
	CREATE TEMPORARY TABLE ',GT_DB,'.tmp_nt_',WORKER_ID,' ENGINE=MYISAM AS 
	SELECT 
		RNC_ID,
		CELL_ID,
		REPLACE(CELL_NAME,''UtranCell '','''') AS CELL_NAME
	FROM ',CURRENT_NT_DB,'.	nt_current
	;');
	
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('ALTER TABLE ',GT_DB,'.tmp_nt_',WORKER_ID,' ADD INDEX `ix_key`(RNC_ID,CELL_ID,CELL_NAME);');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT(' UPDATE ',GT_DB,'.`tmp_table_pm_counter_umts_Ericsson_',WORKER_ID,'` A, 
				    ',GT_DB,'.tmp_nt_',WORKER_ID,' B
			     SET A.CELL_ID=B.CELL_ID
				WHERE A.RNC_ID=B.RNC_ID
				      AND A.CELL_ID = B.CELL_NAME;');
	
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('
		INSERT INTO ',GT_DB,'.`table_pm_counter_umts`
		    (`DATA_DATE`,
		     `DATA_HOUR`,
		     `RNC_ID`,
		     `CELL_ID`,
		     `NoRrcConnectReqSuccess`,
		     `NoRrcConnectReq`,
		     `NoRrcConnectReqSuccessCs`,
		     `NoRrcConnectReqCs`,
		     `NoRabEstablishSuccessCS`,
		     `NoRabEstablishAttemptCS`,
		     `NoRrcConnectReqSuccessPs`,
		     `NoRrcConnectReqPs`,
		     `NoRabEstablishSuccessPS`,
		     `NoRabEstablishAttemptPS`,
		     `NoRrcReqBlock`,
		     `NoRabReqBlockCS`,
		     `NoSystemRabReleaseCS`,
		     `NoNormalRabReleaseCS`,
		     `NoSystemRabReleasePS`,
		     `NoNormalRabReleasePS`,
		     `NoSuccessOutHo`,
		     `NoAttOutHo`,
		     `NoRabReqBlockPS`,
		     `NoOfCONGCALLS`,
		     `UesWith1Rls1RlInAct`,
		     `UesWith1Rls2RlInAct2Rls2RlInAct`,
		     `UesWith1Rls3RlInActSet2Rls3RlInAct3Rls3RlInAct`,
		     `UesWith2Rls4RlInActSet3Rls4RlInAct4Rls4RlInAct`,
		     `pmNoSuccessOutIratHoSpeech`,
		     `pmNoAttOutIratHoSpeech`,
		     `Downtimeman`,
		     `Downtimeauto`,
			pmChSwitchSuccFachUra, 
			pmNoDirRetryAtt, 
			pmNoNormalNasSignReleasePs, 
			pmNoNormalRabReleasePacket, 
			pmNoNormalRabReleasePacketUra, 
			pmNoNormalRabReleaseSpeech, 
			pmNoNormalRbReleaseHs, 
			pmNoRabEstablishAttemptPacketInteractive, 
			pmNoRabEstablishAttemptPacketInteractiveEul, 
			pmNoRabEstablishAttemptPacketInteractiveHs, 
			pmNoRabEstablishAttemptSpeech, 
			pmNoRabEstablishSuccessPacketInteractive, 
			pmNoRabEstablishSuccessPacketInteractiveEul, 
			pmNoRabEstablishSuccessPacketInteractiveHs, 
			pmNoRabEstablishSuccessSpeech, 
			pmNoSuccRbReconfOrigPsIntDch, 
			pmNoSystemNasSignReleasePs, 
			pmNoSystemRabReleasePacket, 
			pmNoSystemRabReleasePacketUra, 
			pmNoSystemRabReleaseSpeech, 
			pmNoSystemRbReleaseHs, 
			pmNoTimesCellFailAddToActSet, 
			pmNoTimesRlAddToActSet, 
			pmPsIntHsToFachSucc, 
			pmSamplesDchDlRlcUserPacketThp, 
			pmSamplesDchUlRlcUserPacketThp, 
			pmSamplesHsDlRlcTotPacketThp, 
			pmSamplesHsDlRlcUserPacketThp, 
			pmSamplesUlRssi, 
			pmSumBestAmr12200RabEstablish, 
			pmSumBestCs64RabEstablish, 
			pmSumBestDchPsIntRabEstablish, 
			pmSumBestPsEulRabEstablish, 
			pmSumBestPsHsAdchRabEstablish, 
			pmSumUesWith1Rls1RlInActSet, 
			pmSumUesWith1Rls2RlInActSet, 
			pmSumUesWith1Rls3RlInActSet, 
			pmSumUesWith2Rls2RlInActSet, 
			pmSumUesWith2Rls3RlInActSet, 
			pmSumUesWith3Rls3RlInActSet, 
			pmSumUlRssi, 
			pmTotNoRrcConnectReq, 
			pmTotNoRrcConnectReqCs, 
			pmTotNoRrcConnectReqCsSucc, 
			pmTotNoRrcConnectReqPs, 
			pmTotNoRrcConnectReqPsSucc, 
			pmTotNoRrcConnectReqSuccess, 
			pmTotNoTermRrcConnectReq, 
			pmTotNoTermRrcConnectReqCs, 
			pmTotNoTermRrcConnectReqCsSucc,
			pmUlTrafficVolumePs128,
			pmUlTrafficVolumePs16, 
			pmUlTrafficVolumePs384, 
			pmUlTrafficVolumePs64, 
			pmUlTrafficVolumePs8, 
			pmUlTrafficVolumePsCommon, 
			pmUlTrafficVolumePsIntEul, 
			pmDlTrafficVolumePs128, 
			pmDlTrafficVolumePs16, 
			pmDlTrafficVolumePs384, 
			pmDlTrafficVolumePs64, 
			pmDlTrafficVolumePs8, 
			pmDlTrafficVolumePsCommon, 
			pmDlTrafficVolumePsIntHs,
			pmTotNoTermRrcConnectReqSucc,
			pmSumBestCs12Establish,
			pmSumHsDlRlcTotPacketThp,
			pmSumDchDlRlcUserPacketThp,
			pmSumDchUlRlcUserPacketThp,
			pmSumHsDlRlcUserPacketThp,
	
			pmNoRabEstablishSuccessPacketStream,
			pmNoRabEstablishAttemptPacketStream,
			pmNoRrcCsReqDeniedAdm,
			pmNoOfNonHoReqDeniedSpeech,
			pmNoNormalRabReleasePacketStream,
			pmNoOfNonHoReqDeniedInteractive,
			pmNoRabEstablishSuccessCS64,
			pmNoRabEstablishAttemptCS64,
			pmNoRabEstablishSuccessPacketStream128,
			pmNoRabEstablishAttemptPacketStream128,
			pmNoSystemRabReleaseCs64,
			pmNoNormalRabReleaseCs64,
			pmNoNormalRabReleasePacketStream128,
			pmNoRabEstablishSuccessCs57,
			pmNoRabEstablishAttemptCs57,
			pmNoOfNonHoReqDeniedCs,
			pmNoSystemRabReleaseCsStream,
			pmNoNormalRabReleaseCsStream,
			pmNoNormalRabReleasePsStreamHs,
			pmNoRrcConnReqBlockTnCs,
			pmNoRabEstBlockTnSpeechBest,
			pmNoRabEstBlockTnCs57Best,
			pmNoRabEstBlockTnCs64Best,
			pmSumUesWith2Rls4RlInActSet,
			pmSumUesWith3Rls4RlInActSet,
			pmSumUesWith4Rls4RlInActSet,	
			pmNoSuccessOutIratHoMulti,
			pmNoAttOutIratHoMulti,
			pmNoAttOutSbHoSpeech,
			pmNoSuccessOutSbHoSpeech)
	SELECT
		  a.`DATA_DATE`,
		  a.`DATA_HOUR`,
		  a.`RNC_ID`,
		  a.`CELL_ID`,
		   IFNULL(A.`NoRrcConnectReqSuccess`,0),
		  IFNULL(A.`NoRrcConnectReq`,0),
		  IFNULL(A.`NoRrcConnectReqSuccessCs`,0),
		  IFNULL(A.`NoRrcConnectReqCs`,0),
		  IFNULL(A.`NoRabEstablishSuccessCS`,0),
		  IFNULL(A.`NoRabEstablishAttemptCS`,0),
		  IFNULL(A.`NoRrcConnectReqSuccessPs`,0),
		  IFNULL(A.`NoRrcConnectReqPs`,0),
		  IFNULL(A.`NoRabEstablishSuccessPS`,0),
		  IFNULL(A.`NoRabEstablishAttemptPS`,0),
		  IFNULL(A.`NoRrcReqBlock`,0),
		  IFNULL(A.`NoRabReqBlockCS`,0),
		  IFNULL(A.`NoSystemRabReleaseCS`,0),
		  IFNULL(A.`NoNormalRabReleaseCS`,0),
		  IFNULL(A.`NoSystemRabReleasePS`,0),
		  IFNULL(A.`NoNormalRabReleasePS`,0),
		  IFNULL(A.`NoSuccessOutHo`,0),
		  IFNULL(A.`NoAttOutHo`,0),
		  IFNULL(A.`NoRabReqBlockPS`,0),
		  IFNULL(A.`NoOfCONGCALLS`,0),
		  IFNULL(A.`UesWith1Rls1RlInAct`,0),
		  IFNULL(A.`UesWith1Rls2RlInAct2Rls2RlInAct`,0),
		  IFNULL(A.`UesWith1Rls3RlInActSet2Rls3RlInAct3Rls3RlInAct`,0),
		  IFNULL(A.`UesWith2Rls4RlInActSet3Rls4RlInAct4Rls4RlInAct`,0),
		  IFNULL(A.`pmNoSuccessOutIratHoSpeech`,0),
		  IFNULL(A.`pmNoAttOutIratHoSpeech`,0),
		  IFNULL(A.`Downtimeman`,0),
		  IFNULL(A.`Downtimeauto`,0),
		  IFNULL(A.pmChSwitchSuccFachUra,0),
		IFNULL(A.pmNoDirRetryAtt,0),
		IFNULL(A.pmNoNormalNasSignReleasePs,0),
		IFNULL(A.pmNoNormalRabReleasePacket,0),
		IFNULL(A.pmNoNormalRabReleasePacketUra,0),
		IFNULL(A.pmNoNormalRabReleaseSpeech,0),
		IFNULL(A.pmNoNormalRbReleaseHs,0),
		IFNULL(A.pmNoRabEstablishAttemptPacketInteractive,0),
		IFNULL(A.pmNoRabEstablishAttemptPacketInteractiveEul,0),
		IFNULL(A.pmNoRabEstablishAttemptPacketInteractiveHs,0),
		IFNULL(A.pmNoRabEstablishAttemptSpeech,0),
		IFNULL(A.pmNoRabEstablishSuccessPacketInteractive,0),
		IFNULL(A.pmNoRabEstablishSuccessPacketInteractiveEul,0),
		IFNULL(A.pmNoRabEstablishSuccessPacketInteractiveHs,0),
		IFNULL(A.pmNoRabEstablishSuccessSpeech,0),
		IFNULL(A.pmNoSuccRbReconfOrigPsIntDch,0),
		IFNULL(A.pmNoSystemNasSignReleasePs,0),
		IFNULL(A.pmNoSystemRabReleasePacket,0),
		IFNULL(A.pmNoSystemRabReleasePacketUra,0),
		IFNULL(A.pmNoSystemRabReleaseSpeech,0),
		IFNULL(A.pmNoSystemRbReleaseHs,0),
		IFNULL(A.pmNoTimesCellFailAddToActSet,0),
		IFNULL(A.pmNoTimesRlAddToActSet,0),
		IFNULL(A.pmPsIntHsToFachSucc,0),
		IFNULL(A.pmSamplesDchDlRlcUserPacketThp,0),
		IFNULL(A.pmSamplesDchUlRlcUserPacketThp,0),
		IFNULL(A.pmSamplesHsDlRlcTotPacketThp,0),
		IFNULL(A.pmSamplesHsDlRlcUserPacketThp,0),
		IFNULL(A.pmSamplesUlRssi,0),
		IFNULL(A.pmSumBestAmr12200RabEstablish,0),
		IFNULL(A.pmSumBestCs64RabEstablish,0),
		IFNULL(A.pmSumBestDchPsIntRabEstablish,0),
		IFNULL(A.pmSumBestPsEulRabEstablish,0),
		IFNULL(A.pmSumBestPsHsAdchRabEstablish,0),
		IFNULL(A.pmSumUesWith1Rls1RlInActSet,0),
		IFNULL(A.pmSumUesWith1Rls2RlInActSet,0),
		IFNULL(A.pmSumUesWith1Rls3RlInActSet,0),
		IFNULL(A.pmSumUesWith2Rls2RlInActSet,0),
		IFNULL(A.pmSumUesWith2Rls3RlInActSet,0),
		IFNULL(A.pmSumUesWith3Rls3RlInActSet,0),
		IFNULL(A.pmSumUlRssi,0),
		IFNULL(A.pmTotNoRrcConnectReq,0),
		IFNULL(A.pmTotNoRrcConnectReqCs,0),
		IFNULL(A.pmTotNoRrcConnectReqCsSucc,0),
		IFNULL(A.pmTotNoRrcConnectReqPs,0),
		IFNULL(A.pmTotNoRrcConnectReqPsSucc,0),
		IFNULL(A.pmTotNoRrcConnectReqSuccess,0),
		IFNULL(A.pmTotNoTermRrcConnectReq,0),
		IFNULL(A.pmTotNoTermRrcConnectReqCs,0),
		IFNULL(A.pmTotNoTermRrcConnectReqCsSucc,0),
		IFNULL(A.pmUlTrafficVolumePs128,0),
		IFNULL(A.pmUlTrafficVolumePs16,0),
		IFNULL(A.pmUlTrafficVolumePs384,0),
		IFNULL(A.pmUlTrafficVolumePs64,0),
		IFNULL(A.pmUlTrafficVolumePs8,0),
		IFNULL(A.pmUlTrafficVolumePsCommon,0),
		IFNULL(A.pmUlTrafficVolumePsIntEul,0),
		IFNULL(A.pmDlTrafficVolumePs128,0),
		IFNULL(A.pmDlTrafficVolumePs16,0),
		IFNULL(A.pmDlTrafficVolumePs384,0),
		IFNULL(A.pmDlTrafficVolumePs64,0),
		IFNULL(A.pmDlTrafficVolumePs8,0),
		IFNULL(A.pmDlTrafficVolumePsCommon,0),
		IFNULL(A.pmDlTrafficVolumePsIntHs,0),
		IFNULL(A.pmTotNoTermRrcConnectReqSucc,0),
		IFNULL(A.pmSumBestCs12Establish,0),
		IFNULL(A.pmSumHsDlRlcTotPacketThp ,0),
		IFNULL(A.pmSumDchDlRlcUserPacketThp,0),
		IFNULL(A.pmSumDchUlRlcUserPacketThp,0),
		IFNULL(A.pmSumHsDlRlcUserPacketThp,0),
	
		IFNULL(A.pmNoRabEstablishSuccessPacketStream,0),
		IFNULL(A.pmNoRabEstablishAttemptPacketStream,0),
		IFNULL(A.pmNoRrcCsReqDeniedAdm,0),
		IFNULL(A.pmNoOfNonHoReqDeniedSpeech,0),
		IFNULL(A.pmNoNormalRabReleasePacketStream,0),
		IFNULL(A.pmNoOfNonHoReqDeniedInteractive,0),
		IFNULL(A.pmNoRabEstablishSuccessCS64,0),
		IFNULL(A.pmNoRabEstablishAttemptCS64,0),
		IFNULL(A.pmNoRabEstablishSuccessPacketStream128,0),
		IFNULL(A.pmNoRabEstablishAttemptPacketStream128,0),
		IFNULL(A.pmNoSystemRabReleaseCs64,0),
		IFNULL(A.pmNoNormalRabReleaseCs64,0),
		IFNULL(A.pmNoNormalRabReleasePacketStream128,0),
		IFNULL(A.pmNoRabEstablishSuccessCs57,0),
		IFNULL(A.pmNoRabEstablishAttemptCs57,0),
		IFNULL(A.pmNoOfNonHoReqDeniedCs,0),
		IFNULL(A.pmNoSystemRabReleaseCsStream,0),
		IFNULL(A.pmNoNormalRabReleaseCsStream,0),
		IFNULL(A.pmNoNormalRabReleasePsStreamHs,0),
		IFNULL(A.pmNoRrcConnReqBlockTnCs,0),
		IFNULL(A.pmNoRabEstBlockTnSpeechBest,0),
		IFNULL(A.pmNoRabEstBlockTnCs57Best,0),
		IFNULL(A.pmNoRabEstBlockTnCs64Best,0),
		IFNULL(A.pmSumUesWith2Rls4RlInActSet,0),
		IFNULL(A.pmSumUesWith3Rls4RlInActSet,0),
		IFNULL(A.pmSumUesWith4Rls4RlInActSet,0),
		IFNULL(A.pmNoSuccessOutIratHoMulti,0),
		IFNULL(A.pmNoAttOutIratHoMulti,0),
		IFNULL(A.pmNoAttOutSbHoSpeech,0),
		IFNULL(A.pmNoSuccessOutSbHoSpeech,0)
		FROM ',GT_DB,'.`tmp_table_pm_counter_umts_Ericsson_',WORKER_ID,'` a   ;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT(' Drop TEMPORARY table if exists ',GT_DB,'.tmp_table_pmnoloadsharingrrcconn_',WORKER_ID,';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	SET @SqlCmd=CONCAT(' Drop TEMPORARY table if exists ',GT_DB,'.tmp_table_pmNoTimesRlDelFrActSet_',WORKER_ID,';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	SET @SqlCmd=CONCAT(' Drop TEMPORARY table if exists ',GT_DB,'.tmp_table_pmHsDowntimeMan_',WORKER_ID,';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	SET @SqlCmd=CONCAT(' Drop TEMPORARY table if exists ',GT_DB,'.tmp_table_pmSamplesRrcOnlyEstablish_',WORKER_ID,';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;	
	SET @SqlCmd=CONCAT(' Drop TEMPORARY table if exists ',GT_DB,'.tmp_table_pmDlTrafficVolumeCs_',WORKER_ID,';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	SET @SqlCmd=CONCAT(' Drop TEMPORARY table if exists ',GT_DB,'.tmp_nt_',WORKER_ID,';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT(' Drop TEMPORARY table if exists ',GT_DB,'.tmp_pm_umts_1_',WORKER_ID,';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT(' Drop TEMPORARY table if exists ',GT_DB,'.tmp_pm_umts_2_',WORKER_ID,';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT(' Drop TEMPORARY table if exists ',GT_DB,'.tmp_pm_umts_3_',WORKER_ID,';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT(' Drop TEMPORARY table if exists ',GT_DB,'.tmp_pm_umts_6_',WORKER_ID,';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT(' Drop TEMPORARY table if exists ',GT_DB,'.tmp_pm_umts_7_',WORKER_ID,';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT(' Drop TEMPORARY table if exists ',GT_DB,'.tmp_pm_umts_8_',WORKER_ID,';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT(' Drop TEMPORARY table if exists ',GT_DB,'.tmp_pm_umts_9_',WORKER_ID,';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
		
	SET @SqlCmd=CONCAT(' Drop TEMPORARY table if exists ',GT_DB,'.tmp_table_pm_counter_umts_Ericsson_',WORKER_ID,';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	CALL gt_gw_main.SP_Generate_PM_counter_HUA_UMTS_Daily(O_GT_DB);
	CALL gt_gw_main.SP_Generate_PM_counter_NSN_UMTS_Daily(O_GT_DB);
	INSERT INTO gt_gw_main.SP_LOG VALUES(GT_DB,'SP_Generate_PM_counter_UMTS_Daily',CONCAT('Done: ',TIMESTAMPDIFF(SECOND,START_TIME,SYSDATE()),' seconds.'), NOW());
	
END$$
DELIMITER ;
