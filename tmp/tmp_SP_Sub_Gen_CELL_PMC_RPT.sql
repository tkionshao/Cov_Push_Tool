CREATE DEFINER=`covmo`@`%` PROCEDURE `SP_Sub_Gen_CELL_PMC_RPT`(IN GT_DB VARCHAR (100),IN TMP_GT_DB VARCHAR (100),FLAG VARCHAR (100))
BEGIN
	DECLARE NT_DB VARCHAR (100);
	DECLARE DATA_HR CHAR(2) DEFAULT SUBSTRING(gt_strtok (TMP_GT_DB, 4, '_'), 1, 2);
	DECLARE DATA_QT CHAR(4) DEFAULT gt_strtok (TMP_GT_DB, 4, '_');
	DECLARE START_TIME DATETIME DEFAULT SYSDATE();
	DECLARE COLUMN_IFNULL_STR VARCHAR (15000) DEFAULT 
	'
		IFNULL(IMSI_CNT,0) as IMSI_CNT,
		IFNULL(Volte_Duration_Total,0) as Volte_Duration_Total,
		IFNULL(Volte_Duration_Cnt,0) as Volte_Duration_Cnt, 
		IFNULL(Dominant_tile_cnt,0) as Dominant_tile_cnt,
		IFNULL(PCI,0) as PCI,
		IFNULL(RRC_CONN_REQ,0) as RRC_CONN_REQ,
		IFNULL(RRC_CONN_REJ,0) as RRC_CONN_REJ,
		IFNULL(pcRrcConnTout,0) as pcRrcConnTout,
		IFNULL(INIT_CNXT_SETUP_ATT,0) as INIT_CNXT_SETUP_ATT,
		IFNULL(INIT_CNXT_SETUP_FAIL,0) as INIT_CNXT_SETUP_FAIL,
		IFNULL(pcS1NasFail,0) as pcS1NasFail,
		IFNULL(ERAB_SETUP_ATT,0) as ERAB_SETUP_ATT,
		IFNULL(pcS1CtxtTout,0) as pcS1CtxtTout,
		IFNULL(VOLTE_ATTEMPT,0) as VOLTE_ATTEMPT,
		IFNULL(pcS1CtxtRelQci1,0) as pcS1CtxtRelQci1,
		IFNULL(pcS1ErabReqQci2,0) as pcS1ErabReqQci2,
		IFNULL(pcS1CtxtRelQci2,0) as pcS1CtxtRelQci2,
		IFNULL(pcS1CtxtRel,0) as pcS1CtxtRel,
		IFNULL(pcS1CtxtNormalRel,0) as pcS1CtxtNormalRel,
		IFNULL(pcS1CtxtAbnormalRel,0) as pcS1CtxtAbnormalRel,
		IFNULL(RRC_CONN_REEST_ATT,0) as RRC_CONN_REEST_ATT,
		IFNULL(RRC_CONN_REEST_FAIL,0) as RRC_CONN_REEST_FAIL,
		IFNULL(pcS1CtxtAbnormalRelQci1,0) as pcS1CtxtAbnormalRelQci1,
		IFNULL(pcS1CtxtAbnormalRelQci2,0) as pcS1CtxtAbnormalRelQci2,
		IFNULL(pcRrcSetupTime,0) as pcRrcSetupTime,
		IFNULL(RRC_CONN_SETUP_COMPLETE,0) as RRC_CONN_SETUP_COMPLETE,
		IFNULL(pcS1ConnSetupTime,0) as pcS1ConnSetupTime,
		IFNULL(pcS1UECapInfoInd,0) as pcS1UECapInfoInd,
		IFNULL(pcS1ErabSetupTimeQci1,0) as pcS1ErabSetupTimeQci1,
		IFNULL(pcS1ErabSetupCntQci1,0) as pcS1ErabSetupCntQci1,
		IFNULL(pcS1ErabSetupTimeQci2,0) as pcS1ErabSetupTimeQci2,
		IFNULL(pcS1ErabSetupCntQci2,0) as pcS1ErabSetupCntQci2,
		IFNULL(pcSumRSRP,0) as pcSumRSRP,
		IFNULL(pcRrcMrRep,0) as pcRrcMrRep,
		IFNULL(pcSumRSRQ,0) as pcSumRSRQ,
		IFNULL(t1_tile_cnt,0) as t1_tile_cnt,
		IFNULL(t0_tile_cnt,0) as t0_tile_cnt,
		IFNULL(NBR_DISTANCE_VORONOI,0) as NBR_DISTANCE_VORONOI,
		IFNULL(pcHoExeAttIntraFreqIntraEnb,0) as pcHoExeAttIntraFreqIntraEnb,
		IFNULL(pcHoExeFailIntraFreqIntraEnb,0) as pcHoExeFailIntraFreqIntraEnb,
		IFNULL(pcX2HoPrepAttIntraFreq,0) as pcX2HoPrepAttIntraFreq,
		IFNULL(pcX2HoPrepFailIntraFreq,0) as pcX2HoPrepFailIntraFreq,
		IFNULL(pcS1HoPrepAttIntraFreq,0) as pcS1HoPrepAttIntraFreq,
		IFNULL(pcS1HoExeFailIntraFreq,0) as pcS1HoExeFailIntraFreq,
		IFNULL(pcS1HoExeAttIntraFreq,0) as pcS1HoExeAttIntraFreq,
		IFNULL(pcS1HoSucRel,0) as pcS1HoSucRel,
		IFNULL(pcHoExeAttInterFreqIntraEnb,0) as pcHoExeAttInterFreqIntraEnb,
		IFNULL(pcHoExeFailInterFreqIntraEnb,0) as pcHoExeFailInterFreqIntraEnb,
		IFNULL(pcX2HoPrepAttInterFreq,0) as pcX2HoPrepAttInterFreq,
		IFNULL(pcX2HoPrepFailInterFreq,0) as pcX2HoPrepFailInterFreq,
		IFNULL(pcX2HoExeFailInterFreq,0) as pcX2HoExeFailInterFreq,
		IFNULL(pcS1HoPrepAttInterFreq,0) as pcS1HoPrepAttInterFreq,
		IFNULL(pcS1HoExeFailInterFreq,0) as pcS1HoExeFailInterFreq,
		IFNULL(pcS1HoAttIrat,0) as pcS1HoAttIrat,
		IFNULL(4G_2G_HO_ATT,0) as 4G_2G_HO_ATT,
		IFNULL(4G_3G_HO_ATT,0) as 4G_3G_HO_ATT,
		IFNULL(pcS1HoExeIrat,0) as pcS1HoExeIrat,
		IFNULL(pcS1HoAttSrvcc,0) as pcS1HoAttSrvcc,
		IFNULL(pcS1HoIratFrom2G,0) as pcS1HoIratFrom2G,
		IFNULL(pcS1HoIratFrom3G,0) as pcS1HoIratFrom3G,
		IFNULL(pcS1HoIratCsfb,0) as pcS1HoIratCsfb,
		IFNULL(pcS1HoIratCsfbTo3G,0) as pcS1HoIratCsfbTo3G,
		IFNULL(pcS1HoIratCsfbTo2G,0) as pcS1HoIratCsfbTo2G,
		IFNULL(pcRedirAll,0) as pcRedirAll,
		IFNULL(pcRedirIntraLte,0) as pcRedirIntraLte,
		IFNULL(pcRedirTo3G,0) as pcRedirTo3G,
		IFNULL(pcRedirTo2G,0) as pcRedirTo2G,
		IFNULL(pcRsrpLev1,0) as pcRsrpLev1,
		IFNULL(pcRsrpLev2,0) as pcRsrpLev2,
		IFNULL(pcRsrpLev3,0) as pcRsrpLev3,
		IFNULL(pcRsrpLev4,0) as pcRsrpLev4,
		IFNULL(pcRsrpLev5,0) as pcRsrpLev5,
		IFNULL(pcRsrpLev6,0) as pcRsrpLev6,
		IFNULL(pcRsrpLev7,0) as pcRsrpLev7,
		IFNULL(pcRsrqLev1,0) as pcRsrqLev1,
		IFNULL(pcRsrqLev2,0) as pcRsrqLev2,
		IFNULL(pcRsrqLev3,0) as pcRsrqLev3,
		IFNULL(pcRsrqLev4,0) as pcRsrqLev4,
		IFNULL(pcSinrLev1,0) as pcSinrLev1,
		IFNULL(pcSinrLev2,0) as pcSinrLev2,
		IFNULL(pcSinrLev3,0) as pcSinrLev3,
		IFNULL(pcSinrLev4,0) as pcSinrLev4,
		IFNULL(pcSinrLev5,0) as pcSinrLev5,
		IFNULL(pcCqiL1Lev1,0) as pcCqiL1Lev1,
		IFNULL(pcCqiL1Lev2,0) as pcCqiL1Lev2,
		IFNULL(pcCqiL1Lev3,0) as pcCqiL1Lev3,
		IFNULL(pcCqiL2Lev1,0) as pcCqiL2Lev1,
		IFNULL(pcCqiL2Lev2,0) as pcCqiL2Lev2,
		IFNULL(pcCqiL2Lev3,0) as pcCqiL2Lev3,
		IFNULL(pcCaSerTime,0) as pcCaSerTime,
		IFNULL(pcX2HoExeAttIntraFreq,0) as pcX2HoExeAttIntraFreq,
		IFNULL(DL_VOLUME_SUM,0) as DL_VOLUME_SUM,
		IFNULL(UL_VOLUME_SUM,0) as UL_VOLUME_SUM,
		IFNULL(DL_THROUGHPUT_SUM,0) as DL_THROUGHPUT_SUM,
		IFNULL(DL_THROUGHPUT_CNT,0) as DL_THROUGHPUT_CNT,
		IFNULL(UL_THROUGHPUT_SUM,0) as UL_THROUGHPUT_SUM,
		IFNULL(UL_THROUGHPUT_CNT,0) as UL_THROUGHPUT_CNT,
		IFNULL(INDOOR_RADIO_CONN,0) as INDOOR_RADIO_CONN,
		IFNULL(VOLTE_FAILURE,0) as VOLTE_FAILURE ,
		IFNULL(CALL_DURATION_SUM,0) as CALL_DURATION_SUM,
		IFNULL(pcX2HoExeFailIntraFreq,0) as pcX2HoExeFailIntraFreq,
		IFNULL(pcX2HoExeAttInterFreq,0) as pcX2HoExeAttInterFreq,
		IFNULL(pcS1HoPrepFailIntraFreq,0) as pcS1HoPrepFailIntraFreq,
		IFNULL(pcS1ErabSetupTime,0) as pcS1ErabSetupTime,
		IFNULL(pcS1ErabSetupCnt,0) as pcS1ErabSetupCnt,
		IFNULL(pcS1SetupFailQci2,0) as pcS1SetupFailQci2,
		IFNULL(pcS1ErabReqQci6,0) as pcS1ErabReqQci6,
		IFNULL(pcPwHrLev1,0) as pcPwHrLev1,
		IFNULL(pcPwHrLev2,0) as pcPwHrLev2,
		IFNULL(pcPwHrLev3,0) as pcPwHrLev3,
		IFNULL(pcPwHrLev4,0) as pcPwHrLev4,
		IFNULL(pcPwHrLev5,0) as pcPwHrLev5,
		IFNULL(pcTaLev1,0) as pcTaLev1,
		IFNULL(pcTaLev2,0) as pcTaLev2,
		IFNULL(pcTaLev3,0) as pcTaLev3,
		IFNULL(pcTaLev4,0) as pcTaLev4,
		IFNULL(pcDlThpLev1,0) as pcDlThpLev1,
		IFNULL(pcDlThpLev2,0) as pcDlThpLev2,
		IFNULL(pcDlThpLev3,0) as pcDlThpLev3,
		IFNULL(pcDlThpLev4,0) as pcDlThpLev4,
		IFNULL(TOTAL_CALL_CNT,0) as TOTAL_CALL_CNT,
		IFNULL(CONFIDENCE_NO_POS,0) as CONFIDENCE_NO_POS,
		IFNULL(CONFIDENCE_LOW,0) as CONFIDENCE_LOW,
		IFNULL(CONFIDENCE_MED,0) as CONFIDENCE_MED,
		IFNULL(CONFIDENCE_HIGH,0) as CONFIDENCE_HIGH,
		IFNULL(IN_INTER_FREQ_HO,0) as IN_INTER_FREQ_HO,
		IFNULL(IN_INTRA_FREQ_HO,0) as IN_INTRA_FREQ_HO,
		IFNULL(VIDEO_DUR_SUM,0) as VIDEO_DUR_SUM,
		IFNULL(VIDEO_DUR_CNT,0) as VIDEO_DUR_CNT
	';
		
	DECLARE COLUMN_UPD_STR VARCHAR (13000) DEFAULT 
	'
		RPT_TABLE_NAME.IMSI_CNT =RPT_TABLE_NAME.IMSI_CNT +VALUES(IMSI_CNT),
		RPT_TABLE_NAME.Volte_Duration_Total =IFNULL(RPT_TABLE_NAME.Volte_Duration_Total,0) +VALUES(Volte_Duration_Total),
		RPT_TABLE_NAME.Volte_Duration_Cnt =IFNULL(RPT_TABLE_NAME.Volte_Duration_Cnt,0) +VALUES(Volte_Duration_Cnt),
		RPT_TABLE_NAME.Dominant_tile_cnt =RPT_TABLE_NAME.Dominant_tile_cnt,
		RPT_TABLE_NAME.PCI =RPT_TABLE_NAME.PCI ,
		RPT_TABLE_NAME.RRC_CONN_REQ =RPT_TABLE_NAME.RRC_CONN_REQ +VALUES(RRC_CONN_REQ),
		RPT_TABLE_NAME.RRC_CONN_REJ =RPT_TABLE_NAME.RRC_CONN_REJ +VALUES(RRC_CONN_REJ),
		RPT_TABLE_NAME.pcRrcConnTout =RPT_TABLE_NAME.pcRrcConnTout +VALUES(pcRrcConnTout),
		RPT_TABLE_NAME.INIT_CNXT_SETUP_ATT =RPT_TABLE_NAME.INIT_CNXT_SETUP_ATT +VALUES(INIT_CNXT_SETUP_ATT),
		RPT_TABLE_NAME.INIT_CNXT_SETUP_FAIL =RPT_TABLE_NAME.INIT_CNXT_SETUP_FAIL +VALUES(INIT_CNXT_SETUP_FAIL),
		RPT_TABLE_NAME.pcS1NasFail =RPT_TABLE_NAME.pcS1NasFail +VALUES(pcS1NasFail),
		RPT_TABLE_NAME.ERAB_SETUP_ATT =RPT_TABLE_NAME.ERAB_SETUP_ATT +VALUES(ERAB_SETUP_ATT),
		RPT_TABLE_NAME.pcS1CtxtTout =RPT_TABLE_NAME.pcS1CtxtTout +VALUES(pcS1CtxtTout),
		RPT_TABLE_NAME.VOLTE_ATTEMPT =IFNULL(RPT_TABLE_NAME.VOLTE_ATTEMPT,0) +VALUES(VOLTE_ATTEMPT),
		RPT_TABLE_NAME.pcS1CtxtRelQci1 =RPT_TABLE_NAME.pcS1CtxtRelQci1 +VALUES(pcS1CtxtRelQci1),
		RPT_TABLE_NAME.pcS1ErabReqQci2 =RPT_TABLE_NAME.pcS1ErabReqQci2 +VALUES(pcS1ErabReqQci2),
		RPT_TABLE_NAME.pcS1CtxtRelQci2 =RPT_TABLE_NAME.pcS1CtxtRelQci2 +VALUES(pcS1CtxtRelQci2),
		RPT_TABLE_NAME.pcS1CtxtRel =RPT_TABLE_NAME.pcS1CtxtRel +VALUES(pcS1CtxtRel),
		RPT_TABLE_NAME.pcS1CtxtNormalRel =RPT_TABLE_NAME.pcS1CtxtNormalRel +VALUES(pcS1CtxtNormalRel),
		RPT_TABLE_NAME.pcS1CtxtAbnormalRel =RPT_TABLE_NAME.pcS1CtxtAbnormalRel +VALUES(pcS1CtxtAbnormalRel),
		RPT_TABLE_NAME.RRC_CONN_REEST_ATT =RPT_TABLE_NAME.RRC_CONN_REEST_ATT +VALUES(RRC_CONN_REEST_ATT),
		RPT_TABLE_NAME.RRC_CONN_REEST_FAIL =RPT_TABLE_NAME.RRC_CONN_REEST_FAIL +VALUES(RRC_CONN_REEST_FAIL),
		RPT_TABLE_NAME.pcS1CtxtAbnormalRelQci1 =RPT_TABLE_NAME.pcS1CtxtAbnormalRelQci1 +VALUES(pcS1CtxtAbnormalRelQci1),
		RPT_TABLE_NAME.pcS1CtxtAbnormalRelQci2 =RPT_TABLE_NAME.pcS1CtxtAbnormalRelQci2 +VALUES(pcS1CtxtAbnormalRelQci2),
		RPT_TABLE_NAME.pcRrcSetupTime =RPT_TABLE_NAME.pcRrcSetupTime +VALUES(pcRrcSetupTime),
		RPT_TABLE_NAME.RRC_CONN_SETUP_COMPLETE =RPT_TABLE_NAME.RRC_CONN_SETUP_COMPLETE +VALUES(RRC_CONN_SETUP_COMPLETE),
		RPT_TABLE_NAME.pcS1ConnSetupTime =RPT_TABLE_NAME.pcS1ConnSetupTime +VALUES(pcS1ConnSetupTime),
		RPT_TABLE_NAME.pcS1UECapInfoInd =RPT_TABLE_NAME.pcS1UECapInfoInd +VALUES(pcS1UECapInfoInd),
		RPT_TABLE_NAME.pcS1ErabSetupTimeQci1 =RPT_TABLE_NAME.pcS1ErabSetupTimeQci1 +VALUES(pcS1ErabSetupTimeQci1),
		RPT_TABLE_NAME.pcS1ErabSetupCntQci1 =RPT_TABLE_NAME.pcS1ErabSetupCntQci1 +VALUES(pcS1ErabSetupCntQci1),
		RPT_TABLE_NAME.pcS1ErabSetupTimeQci2 =RPT_TABLE_NAME.pcS1ErabSetupTimeQci2 +VALUES(pcS1ErabSetupTimeQci2),
		RPT_TABLE_NAME.pcS1ErabSetupCntQci2 =RPT_TABLE_NAME.pcS1ErabSetupCntQci2 +VALUES(pcS1ErabSetupCntQci2),
		RPT_TABLE_NAME.pcSumRSRP =RPT_TABLE_NAME.pcSumRSRP +VALUES(pcSumRSRP),
		RPT_TABLE_NAME.pcRrcMrRep =RPT_TABLE_NAME.pcRrcMrRep +VALUES(pcRrcMrRep),
		RPT_TABLE_NAME.pcSumRSRQ =RPT_TABLE_NAME.pcSumRSRQ +VALUES(pcSumRSRQ),
		RPT_TABLE_NAME.t1_tile_cnt =RPT_TABLE_NAME.t1_tile_cnt +VALUES(t1_tile_cnt),
		RPT_TABLE_NAME.t0_tile_cnt =RPT_TABLE_NAME.t0_tile_cnt +VALUES(t0_tile_cnt),
		RPT_TABLE_NAME.NBR_DISTANCE_VORONOI =RPT_TABLE_NAME.NBR_DISTANCE_VORONOI ,
		RPT_TABLE_NAME.pcHoExeAttIntraFreqIntraEnb =RPT_TABLE_NAME.pcHoExeAttIntraFreqIntraEnb +VALUES(pcHoExeAttIntraFreqIntraEnb),
		RPT_TABLE_NAME.pcHoExeFailIntraFreqIntraEnb =RPT_TABLE_NAME.pcHoExeFailIntraFreqIntraEnb +VALUES(pcHoExeFailIntraFreqIntraEnb),
		RPT_TABLE_NAME.pcX2HoPrepAttIntraFreq =RPT_TABLE_NAME.pcX2HoPrepAttIntraFreq +VALUES(pcX2HoPrepAttIntraFreq),
		RPT_TABLE_NAME.pcX2HoPrepFailIntraFreq =RPT_TABLE_NAME.pcX2HoPrepFailIntraFreq +VALUES(pcX2HoPrepFailIntraFreq),
		RPT_TABLE_NAME.pcS1HoPrepAttIntraFreq =RPT_TABLE_NAME.pcS1HoPrepAttIntraFreq +VALUES(pcS1HoPrepAttIntraFreq),
		RPT_TABLE_NAME.pcS1HoExeFailIntraFreq =RPT_TABLE_NAME.pcS1HoExeFailIntraFreq +VALUES(pcS1HoExeFailIntraFreq),
		RPT_TABLE_NAME.pcS1HoExeAttIntraFreq =RPT_TABLE_NAME.pcS1HoExeAttIntraFreq +VALUES(pcS1HoExeAttIntraFreq),
		RPT_TABLE_NAME.pcS1HoSucRel =RPT_TABLE_NAME.pcS1HoSucRel +VALUES(pcS1HoSucRel),
		RPT_TABLE_NAME.pcHoExeAttInterFreqIntraEnb =RPT_TABLE_NAME.pcHoExeAttInterFreqIntraEnb +VALUES(pcHoExeAttInterFreqIntraEnb),
		RPT_TABLE_NAME.pcHoExeFailInterFreqIntraEnb =RPT_TABLE_NAME.pcHoExeFailInterFreqIntraEnb +VALUES(pcHoExeFailInterFreqIntraEnb),
		RPT_TABLE_NAME.pcX2HoPrepAttInterFreq =RPT_TABLE_NAME.pcX2HoPrepAttInterFreq +VALUES(pcX2HoPrepAttInterFreq),
		RPT_TABLE_NAME.pcX2HoPrepFailInterFreq =RPT_TABLE_NAME.pcX2HoPrepFailInterFreq +VALUES(pcX2HoPrepFailInterFreq),
		RPT_TABLE_NAME.pcX2HoExeFailInterFreq =RPT_TABLE_NAME.pcX2HoExeFailInterFreq +VALUES(pcX2HoExeFailInterFreq),
		RPT_TABLE_NAME.pcS1HoPrepAttInterFreq =RPT_TABLE_NAME.pcS1HoPrepAttInterFreq +VALUES(pcS1HoPrepAttInterFreq),
		RPT_TABLE_NAME.pcS1HoExeFailInterFreq =RPT_TABLE_NAME.pcS1HoExeFailInterFreq +VALUES(pcS1HoExeFailInterFreq),
		RPT_TABLE_NAME.pcS1HoAttIrat =RPT_TABLE_NAME.pcS1HoAttIrat +VALUES(pcS1HoAttIrat),
		RPT_TABLE_NAME.4G_2G_HO_ATT =RPT_TABLE_NAME.4G_2G_HO_ATT +VALUES(4G_2G_HO_ATT),
		RPT_TABLE_NAME.4G_3G_HO_ATT =RPT_TABLE_NAME.4G_3G_HO_ATT +VALUES(4G_3G_HO_ATT),
		RPT_TABLE_NAME.pcS1HoExeIrat =RPT_TABLE_NAME.pcS1HoExeIrat +VALUES(pcS1HoExeIrat),
		RPT_TABLE_NAME.pcS1HoAttSrvcc =RPT_TABLE_NAME.pcS1HoAttSrvcc +VALUES(pcS1HoAttSrvcc),
		RPT_TABLE_NAME.pcS1HoIratFrom2G =RPT_TABLE_NAME.pcS1HoIratFrom2G +VALUES(pcS1HoIratFrom2G),
		RPT_TABLE_NAME.pcS1HoIratFrom3G =RPT_TABLE_NAME.pcS1HoIratFrom3G +VALUES(pcS1HoIratFrom3G),
		RPT_TABLE_NAME.pcS1HoIratCsfb =RPT_TABLE_NAME.pcS1HoIratCsfb +VALUES(pcS1HoIratCsfb),
		RPT_TABLE_NAME.pcS1HoIratCsfbTo3G =RPT_TABLE_NAME.pcS1HoIratCsfbTo3G +VALUES(pcS1HoIratCsfbTo3G),
		RPT_TABLE_NAME.pcS1HoIratCsfbTo2G =RPT_TABLE_NAME.pcS1HoIratCsfbTo2G +VALUES(pcS1HoIratCsfbTo2G),
		RPT_TABLE_NAME.pcRedirAll =RPT_TABLE_NAME.pcRedirAll +VALUES(pcRedirAll),
		RPT_TABLE_NAME.pcRedirIntraLte =RPT_TABLE_NAME.pcRedirIntraLte +VALUES(pcRedirIntraLte),
		RPT_TABLE_NAME.pcRedirTo3G =RPT_TABLE_NAME.pcRedirTo3G +VALUES(pcRedirTo3G),
		RPT_TABLE_NAME.pcRedirTo2G =RPT_TABLE_NAME.pcRedirTo2G +VALUES(pcRedirTo2G),
		RPT_TABLE_NAME.pcRsrpLev1 =RPT_TABLE_NAME.pcRsrpLev1 +VALUES(pcRsrpLev1),
		RPT_TABLE_NAME.pcRsrpLev2 =RPT_TABLE_NAME.pcRsrpLev2 +VALUES(pcRsrpLev2),
		RPT_TABLE_NAME.pcRsrpLev3 =RPT_TABLE_NAME.pcRsrpLev3 +VALUES(pcRsrpLev3),
		RPT_TABLE_NAME.pcRsrpLev4 =RPT_TABLE_NAME.pcRsrpLev4 +VALUES(pcRsrpLev4),
		RPT_TABLE_NAME.pcRsrpLev5 =RPT_TABLE_NAME.pcRsrpLev5 +VALUES(pcRsrpLev5),
		RPT_TABLE_NAME.pcRsrpLev6 =RPT_TABLE_NAME.pcRsrpLev6 +VALUES(pcRsrpLev6),
		RPT_TABLE_NAME.pcRsrpLev7 =RPT_TABLE_NAME.pcRsrpLev7 +VALUES(pcRsrpLev7),
		RPT_TABLE_NAME.pcRsrqLev1 =RPT_TABLE_NAME.pcRsrqLev1 +VALUES(pcRsrqLev1),
		RPT_TABLE_NAME.pcRsrqLev2 =RPT_TABLE_NAME.pcRsrqLev2 +VALUES(pcRsrqLev2),
		RPT_TABLE_NAME.pcRsrqLev3 =RPT_TABLE_NAME.pcRsrqLev3 +VALUES(pcRsrqLev3),
		RPT_TABLE_NAME.pcRsrqLev4 =RPT_TABLE_NAME.pcRsrqLev4 +VALUES(pcRsrqLev4),
		RPT_TABLE_NAME.pcSinrLev1 =RPT_TABLE_NAME.pcSinrLev1 +VALUES(pcSinrLev1),
		RPT_TABLE_NAME.pcSinrLev2 =RPT_TABLE_NAME.pcSinrLev2 +VALUES(pcSinrLev2),
		RPT_TABLE_NAME.pcSinrLev3 =RPT_TABLE_NAME.pcSinrLev3 +VALUES(pcSinrLev3),
		RPT_TABLE_NAME.pcSinrLev4 =RPT_TABLE_NAME.pcSinrLev4 +VALUES(pcSinrLev4),
		RPT_TABLE_NAME.pcSinrLev5 =RPT_TABLE_NAME.pcSinrLev5 +VALUES(pcSinrLev5),
		RPT_TABLE_NAME.pcCqiL1Lev1 =RPT_TABLE_NAME.pcCqiL1Lev1 +VALUES(pcCqiL1Lev1),
		RPT_TABLE_NAME.pcCqiL1Lev2 =RPT_TABLE_NAME.pcCqiL1Lev2 +VALUES(pcCqiL1Lev2),
		RPT_TABLE_NAME.pcCqiL1Lev3 =RPT_TABLE_NAME.pcCqiL1Lev3 +VALUES(pcCqiL1Lev3),
		RPT_TABLE_NAME.pcCqiL2Lev1 =RPT_TABLE_NAME.pcCqiL2Lev1 +VALUES(pcCqiL2Lev1),
		RPT_TABLE_NAME.pcCqiL2Lev2 =RPT_TABLE_NAME.pcCqiL2Lev2 +VALUES(pcCqiL2Lev2),
		RPT_TABLE_NAME.pcCqiL2Lev3 =RPT_TABLE_NAME.pcCqiL2Lev3 +VALUES(pcCqiL2Lev3),
		RPT_TABLE_NAME.pcCaSerTime =RPT_TABLE_NAME.pcCaSerTime +VALUES(pcCaSerTime),
		RPT_TABLE_NAME.pcX2HoExeAttIntraFreq =RPT_TABLE_NAME.pcX2HoExeAttIntraFreq +VALUES(pcX2HoExeAttIntraFreq),
		RPT_TABLE_NAME.DL_VOLUME_SUM =RPT_TABLE_NAME.DL_VOLUME_SUM ,
		RPT_TABLE_NAME.UL_VOLUME_SUM =RPT_TABLE_NAME.UL_VOLUME_SUM ,
		RPT_TABLE_NAME.DL_THROUGHPUT_SUM =RPT_TABLE_NAME.DL_THROUGHPUT_SUM ,
		RPT_TABLE_NAME.DL_THROUGHPUT_CNT =RPT_TABLE_NAME.DL_THROUGHPUT_CNT ,
		RPT_TABLE_NAME.UL_THROUGHPUT_SUM =RPT_TABLE_NAME.UL_THROUGHPUT_SUM ,
		RPT_TABLE_NAME.UL_THROUGHPUT_CNT =RPT_TABLE_NAME.UL_THROUGHPUT_CNT ,
		RPT_TABLE_NAME.INDOOR_RADIO_CONN =RPT_TABLE_NAME.INDOOR_RADIO_CONN +VALUES(INDOOR_RADIO_CONN),
		RPT_TABLE_NAME.VOLTE_FAILURE =IFNULL(RPT_TABLE_NAME.VOLTE_FAILURE,0) +VALUES(VOLTE_FAILURE),
		RPT_TABLE_NAME.CALL_DURATION_SUM =RPT_TABLE_NAME.CALL_DURATION_SUM +VALUES(CALL_DURATION_SUM),
		RPT_TABLE_NAME.pcX2HoExeFailIntraFreq =RPT_TABLE_NAME.pcX2HoExeFailIntraFreq +VALUES(pcX2HoExeFailIntraFreq),
		RPT_TABLE_NAME.pcX2HoExeAttInterFreq =RPT_TABLE_NAME.pcX2HoExeAttInterFreq +VALUES(pcX2HoExeAttInterFreq),
		RPT_TABLE_NAME.pcS1HoPrepFailIntraFreq =RPT_TABLE_NAME.pcS1HoPrepFailIntraFreq +VALUES(pcS1HoPrepFailIntraFreq),
		RPT_TABLE_NAME.pcS1ErabSetupTime =IFNULL(RPT_TABLE_NAME.pcS1ErabSetupTime,0) +VALUES(pcS1ErabSetupTime),
		RPT_TABLE_NAME.pcS1ErabSetupCnt=IFNULL(RPT_TABLE_NAME.pcS1ErabSetupCnt,0)+VALUES(pcS1ErabSetupCnt),
		
		RPT_TABLE_NAME.pcS1SetupFailQci2=IFNULL(RPT_TABLE_NAME.pcS1SetupFailQci2,0)+VALUES(pcS1SetupFailQci2),
		RPT_TABLE_NAME.pcS1ErabReqQci6=IFNULL(RPT_TABLE_NAME.pcS1ErabReqQci6,0)+VALUES(pcS1ErabReqQci6),
		RPT_TABLE_NAME.pcPwHrLev1=IFNULL(RPT_TABLE_NAME.pcPwHrLev1,0)+VALUES(pcPwHrLev1),
		RPT_TABLE_NAME.pcPwHrLev2=IFNULL(RPT_TABLE_NAME.pcPwHrLev2,0)+VALUES(pcPwHrLev2),
		RPT_TABLE_NAME.pcPwHrLev3=IFNULL(RPT_TABLE_NAME.pcPwHrLev3,0)+VALUES(pcPwHrLev3),
		RPT_TABLE_NAME.pcPwHrLev4=IFNULL(RPT_TABLE_NAME.pcPwHrLev4,0)+VALUES(pcPwHrLev4),
		RPT_TABLE_NAME.pcPwHrLev5=IFNULL(RPT_TABLE_NAME.pcPwHrLev5,0)+VALUES(pcPwHrLev5),
		RPT_TABLE_NAME.pcTaLev1=IFNULL(RPT_TABLE_NAME.pcTaLev1,0)+VALUES(pcTaLev1),
		RPT_TABLE_NAME.pcTaLev2=IFNULL(RPT_TABLE_NAME.pcTaLev2,0)+VALUES(pcTaLev2),
		RPT_TABLE_NAME.pcTaLev3=IFNULL(RPT_TABLE_NAME.pcTaLev3,0)+VALUES(pcTaLev3),
		RPT_TABLE_NAME.pcTaLev4=IFNULL(RPT_TABLE_NAME.pcTaLev4,0)+VALUES(pcTaLev4),
		RPT_TABLE_NAME.pcDlThpLev1=IFNULL(RPT_TABLE_NAME.pcDlThpLev1,0)+VALUES(pcDlThpLev1),
		RPT_TABLE_NAME.pcDlThpLev2=IFNULL(RPT_TABLE_NAME.pcDlThpLev2,0)+VALUES(pcDlThpLev2),
		RPT_TABLE_NAME.pcDlThpLev3=IFNULL(RPT_TABLE_NAME.pcDlThpLev3,0)+VALUES(pcDlThpLev3),
		RPT_TABLE_NAME.pcDlThpLev4=IFNULL(RPT_TABLE_NAME.pcDlThpLev4,0)+VALUES(pcDlThpLev4),
		RPT_TABLE_NAME.TOTAL_CALL_CNT=IFNULL(RPT_TABLE_NAME.TOTAL_CALL_CNT,0) +VALUES(TOTAL_CALL_CNT),
		RPT_TABLE_NAME.CONFIDENCE_NO_POS=IFNULL(RPT_TABLE_NAME.CONFIDENCE_NO_POS,0) +VALUES(CONFIDENCE_NO_POS),
		RPT_TABLE_NAME.CONFIDENCE_LOW=IFNULL(RPT_TABLE_NAME.CONFIDENCE_LOW,0) +VALUES(CONFIDENCE_LOW),
		RPT_TABLE_NAME.CONFIDENCE_MED=IFNULL(RPT_TABLE_NAME.CONFIDENCE_MED,0) +VALUES(CONFIDENCE_MED),
		RPT_TABLE_NAME.CONFIDENCE_HIGH=IFNULL(RPT_TABLE_NAME.CONFIDENCE_HIGH,0) +VALUES(CONFIDENCE_HIGH),
		RPT_TABLE_NAME.IN_INTER_FREQ_HO=IFNULL(RPT_TABLE_NAME.IN_INTER_FREQ_HO,0) +VALUES(IN_INTER_FREQ_HO),
		RPT_TABLE_NAME.IN_INTRA_FREQ_HO=IFNULL(RPT_TABLE_NAME.IN_INTRA_FREQ_HO,0) +VALUES(IN_INTRA_FREQ_HO),
		RPT_TABLE_NAME.VIDEO_DUR_SUM=IFNULL(RPT_TABLE_NAME.VIDEO_DUR_SUM,0) +VALUES(VIDEO_DUR_SUM),
		RPT_TABLE_NAME.VIDEO_DUR_CNT=IFNULL(RPT_TABLE_NAME.VIDEO_DUR_CNT,0) +VALUES(VIDEO_DUR_CNT)
	';
		
	DECLARE TABLE_CELL_SUM_STR VARCHAR (15000) DEFAULT 
	'
		SUM(RRC_CONN_REQ),
		SUM(RRC_CONN_REJ),
		SUM(pcRrcConnTout),
		SUM(INIT_CNXT_SETUP_ATT),
		SUM(INIT_CNXT_SETUP_FAIL),
		SUM(pcS1NasFail),
		SUM(ERAB_SETUP_ATT),
		SUM(pcS1CtxtTout), 
		SUM(pcS1CtxtRelQci1),
		SUM(pcS1ErabReqQci2),
		SUM(pcS1CtxtRelQci2),
		SUM(pcS1CtxtRel),
		SUM(pcS1CtxtNormalRel),
		SUM(pcS1CtxtAbnormalRel),
		SUM(RRC_CONN_REEST_ATT),
		SUM(RRC_CONN_REEST_FAIL),
		SUM(pcS1CtxtAbnormalRelQci1),
		SUM(pcS1CtxtAbnormalRelQci2),
		SUM(pcRrcSetupTime),
		SUM(RRC_CONN_SETUP_COMPLETE),
		SUM(pcS1ConnSetupTime),
		SUM(pcS1UECapInfoInd),
		SUM(pcS1ErabSetupTimeQci1),
		SUM(pcS1ErabSetupCntQci1),
		SUM(pcS1ErabSetupTimeQci2),
		SUM(pcS1ErabSetupCntQci2),
		SUM(pcSumRSRP),
		SUM(pcRrcMrRep),
		SUM(pcSumRSRQ),
		SUM(pcHoExeAttIntraFreqIntraEnb),
		SUM(pcHoExeFailIntraFreqIntraEnb),
		SUM(pcX2HoPrepAttIntraFreq),
		SUM(pcX2HoPrepFailIntraFreq),
		SUM(pcS1HoPrepAttIntraFreq),
		SUM(pcS1HoExeFailIntraFreq),
		SUM(pcS1HoExeAttIntraFreq),
		SUM(pcS1HoSucRel),
		SUM(pcHoExeAttInterFreqIntraEnb),
		SUM(pcHoExeFailInterFreqIntraEnb),
		SUM(pcX2HoPrepAttInterFreq),
		SUM(pcX2HoPrepFailInterFreq),
		SUM(pcX2HoExeFailInterFreq),
		SUM(pcS1HoPrepAttInterFreq),
		SUM(pcS1HoExeFailInterFreq),
		SUM(pcS1HoAttIrat),
		SUM(4G_2G_HO_ATT),
		SUM(4G_3G_HO_ATT),
		SUM(pcS1HoExeIrat),
		SUM(pcS1HoAttSrvcc),
		SUM(pcS1HoIratFrom2G),
		SUM(pcS1HoIratFrom3G),
		SUM(pcS1HoIratCsfb),
		SUM(pcS1HoIratCsfbTo3G),
		SUM(pcS1HoIratCsfbTo2G),
		SUM(pcRedirAll),
		SUM(pcRedirIntraLte),
		SUM(pcRedirTo3G),
		SUM(pcRedirTo2G),
		SUM(pcRsrpLev1),
		SUM(pcRsrpLev2),
		SUM(pcRsrpLev3),
		SUM(pcRsrpLev4),
		SUM(pcRsrpLev5),
		SUM(pcRsrpLev6),
		SUM(pcRsrpLev7),
		SUM(pcRsrqLev1),
		SUM(pcRsrqLev2),
		SUM(pcRsrqLev3),
		SUM(pcRsrqLev4),
		SUM(pcSinrLev1),
		SUM(pcSinrLev2),
		SUM(pcSinrLev3),
		SUM(pcSinrLev4),
		SUM(pcSinrLev5),
		SUM(pcCqiL1Lev1),
		SUM(pcCqiL1Lev2),
		SUM(pcCqiL1Lev3),
		SUM(pcCqiL2Lev1),
		SUM(pcCqiL2Lev2),
		SUM(pcCqiL2Lev3),
		SUM(pcCaSerTime),
		SUM(pcX2HoExeAttIntraFreq),  
		SUM(CALL_DURATION_SUM),
		SUM(pcX2HoExeFailIntraFreq),
		SUM(pcX2HoExeAttInterFreq),
		SUM(pcS1HoPrepFailIntraFreq),
		SUM(pcS1SetupFailQci2),
		SUM(pcS1ErabReqQci6),
		SUM(pcPwHrLev1),
		SUM(pcPwHrLev2),
		SUM(pcPwHrLev3),
		SUM(pcPwHrLev4),
		SUM(pcPwHrLev5),
		SUM(pcTaLev1),
		SUM(pcTaLev2),
		SUM(pcTaLev3),
		SUM(pcTaLev4),
		SUM(pcDlThpLev1),
		SUM(pcDlThpLev2),
		SUM(pcDlThpLev3),
		SUM(pcDlThpLev4)
	';
		
	DECLARE TABLE_RPT_COL_STR VARCHAR (15000) DEFAULT 
	'
		IMSI_CNT,
		Volte_Duration_Total,
		Volte_Duration_Cnt,
		Dominant_tile_cnt,
		PCI,
		RRC_CONN_REQ,
		RRC_CONN_REJ,
		pcRrcConnTout,
		INIT_CNXT_SETUP_ATT,
		INIT_CNXT_SETUP_FAIL,
		pcS1NasFail,
		ERAB_SETUP_ATT,
		pcS1CtxtTout,
		VOLTE_ATTEMPT,
		pcS1CtxtRelQci1,
		pcS1ErabReqQci2,
		pcS1CtxtRelQci2,
		pcS1CtxtRel,
		pcS1CtxtNormalRel,
		pcS1CtxtAbnormalRel,
		RRC_CONN_REEST_ATT,
		RRC_CONN_REEST_FAIL,
		pcS1CtxtAbnormalRelQci1,
		pcS1CtxtAbnormalRelQci2,
		pcRrcSetupTime,
		RRC_CONN_SETUP_COMPLETE,
		pcS1ConnSetupTime,
		pcS1UECapInfoInd,
		pcS1ErabSetupTimeQci1,
		pcS1ErabSetupCntQci1,
		pcS1ErabSetupTimeQci2,
		pcS1ErabSetupCntQci2,
		pcSumRSRP,
		pcRrcMrRep,
		pcSumRSRQ,
		t1_tile_cnt,
		t0_tile_cnt,
		NBR_DISTANCE_VORONOI,
		pcHoExeAttIntraFreqIntraEnb,
		pcHoExeFailIntraFreqIntraEnb,
		pcX2HoPrepAttIntraFreq,
		pcX2HoPrepFailIntraFreq,
		pcS1HoPrepAttIntraFreq,
		pcS1HoExeFailIntraFreq,
		pcS1HoExeAttIntraFreq,
		pcS1HoSucRel,
		pcHoExeAttInterFreqIntraEnb,
		pcHoExeFailInterFreqIntraEnb,
		pcX2HoPrepAttInterFreq,
		pcX2HoPrepFailInterFreq,
		pcX2HoExeFailInterFreq,
		pcS1HoPrepAttInterFreq,
		pcS1HoExeFailInterFreq,
		pcS1HoAttIrat,
		4G_2G_HO_ATT,
		4G_3G_HO_ATT,
		pcS1HoExeIrat,
		pcS1HoAttSrvcc,
		pcS1HoIratFrom2G,
		pcS1HoIratFrom3G,
		pcS1HoIratCsfb,
		pcS1HoIratCsfbTo3G,
		pcS1HoIratCsfbTo2G,
		pcRedirAll,
		pcRedirIntraLte,
		pcRedirTo3G,
		pcRedirTo2G,
		pcRsrpLev1,
		pcRsrpLev2,
		pcRsrpLev3,
		pcRsrpLev4,
		pcRsrpLev5,
		pcRsrpLev6,
		pcRsrpLev7,
		pcRsrqLev1,
		pcRsrqLev2,
		pcRsrqLev3,
		pcRsrqLev4,
		pcSinrLev1,
		pcSinrLev2,
		pcSinrLev3,
		pcSinrLev4,
		pcSinrLev5,
		pcCqiL1Lev1,
		pcCqiL1Lev2,
		pcCqiL1Lev3,
		pcCqiL2Lev1,
		pcCqiL2Lev2,
		pcCqiL2Lev3,
		pcCaSerTime,
		pcX2HoExeAttIntraFreq,
		DL_VOLUME_SUM,
		UL_VOLUME_SUM,
		DL_THROUGHPUT_SUM,
		DL_THROUGHPUT_CNT,
		UL_THROUGHPUT_SUM,
		UL_THROUGHPUT_CNT,
		INDOOR_RADIO_CONN,
		VOLTE_FAILURE,
		CALL_DURATION_SUM,
		pcX2HoExeFailIntraFreq,
		pcX2HoExeAttInterFreq,
		pcS1HoPrepFailIntraFreq,
		pcS1ErabSetupTime,
		pcS1ErabSetupCnt,
		pcS1SetupFailQci2,
		pcS1ErabReqQci6,
		pcPwHrLev1,
		pcPwHrLev2,
		pcPwHrLev3,
		pcPwHrLev4,
		pcPwHrLev5,
		pcTaLev1,
		pcTaLev2,
		pcTaLev3,
		pcTaLev4,
		pcDlThpLev1,
		pcDlThpLev2,
		pcDlThpLev3,
		pcDlThpLev4,
		TOTAL_CALL_CNT,
		CONFIDENCE_NO_POS,
		CONFIDENCE_LOW,
		CONFIDENCE_MED,
		CONFIDENCE_HIGH,
		IN_INTER_FREQ_HO,
		IN_INTRA_FREQ_HO,
		VIDEO_DUR_SUM,
		VIDEO_DUR_CNT
	';
	DECLARE TABLE_CELL_COL_STR VARCHAR(15000) DEFAULT
	'
		RRC_CONN_REQ,
		RRC_CONN_REJ,
		pcRrcConnTout,
		INIT_CNXT_SETUP_ATT,
		INIT_CNXT_SETUP_FAIL,
		pcS1NasFail,
		ERAB_SETUP_ATT,
		pcS1CtxtTout, 
		pcS1CtxtRelQci1,
		pcS1ErabReqQci2,
		pcS1CtxtRelQci2,
		pcS1CtxtRel,
		pcS1CtxtNormalRel,
		pcS1CtxtAbnormalRel,
		RRC_CONN_REEST_ATT,
		RRC_CONN_REEST_FAIL,
		pcS1CtxtAbnormalRelQci1,
		pcS1CtxtAbnormalRelQci2,
		pcRrcSetupTime,
		RRC_CONN_SETUP_COMPLETE,
		pcS1ConnSetupTime,
		pcS1UECapInfoInd,
		pcS1ErabSetupTimeQci1,
		pcS1ErabSetupCntQci1,
		pcS1ErabSetupTimeQci2,
		pcS1ErabSetupCntQci2,
		pcSumRSRP,
		pcRrcMrRep,
		pcSumRSRQ,
		pcHoExeAttIntraFreqIntraEnb,
		pcHoExeFailIntraFreqIntraEnb,
		pcX2HoPrepAttIntraFreq,
		pcX2HoPrepFailIntraFreq,
		pcS1HoPrepAttIntraFreq,
		pcS1HoExeFailIntraFreq,
		pcS1HoExeAttIntraFreq,
		pcS1HoSucRel,
		pcHoExeAttInterFreqIntraEnb,
		pcHoExeFailInterFreqIntraEnb,
		pcX2HoPrepAttInterFreq,
		pcX2HoPrepFailInterFreq,
		pcX2HoExeFailInterFreq,
		pcS1HoPrepAttInterFreq,
		pcS1HoExeFailInterFreq,
		pcS1HoAttIrat,
		4G_2G_HO_ATT,
		4G_3G_HO_ATT,
		pcS1HoExeIrat,
		pcS1HoAttSrvcc,
		pcS1HoIratFrom2G,
		pcS1HoIratFrom3G,
		pcS1HoIratCsfb,
		pcS1HoIratCsfbTo3G,
		pcS1HoIratCsfbTo2G,
		pcRedirAll,
		pcRedirIntraLte,
		pcRedirTo3G,
		pcRedirTo2G,
		pcRsrpLev1,
		pcRsrpLev2,
		pcRsrpLev3,
		pcRsrpLev4,
		pcRsrpLev5,
		pcRsrpLev6,
		pcRsrpLev7,
		pcRsrqLev1,
		pcRsrqLev2,
		pcRsrqLev3,
		pcRsrqLev4,
		pcSinrLev1,
		pcSinrLev2,
		pcSinrLev3,
		pcSinrLev4,
		pcSinrLev5,
		pcCqiL1Lev1,
		pcCqiL1Lev2,
		pcCqiL1Lev3,
		pcCqiL2Lev1,
		pcCqiL2Lev2,
		pcCqiL2Lev3,
		pcCaSerTime,
		pcX2HoExeAttIntraFreq,
		CALL_DURATION_SUM,
		pcX2HoExeFailIntraFreq,
		pcX2HoExeAttInterFreq,
		pcS1HoPrepFailIntraFreq,
		pcS1SetupFailQci2,
		pcS1ErabReqQci6,
		pcPwHrLev1,
		pcPwHrLev2,
		pcPwHrLev3,
		pcPwHrLev4,
		pcPwHrLev5,
		pcTaLev1,
		pcTaLev2,
		pcTaLev3,
		pcTaLev4,
		pcDlThpLev1,
		pcDlThpLev2,
		pcDlThpLev3,
		pcDlThpLev4
	';
	
	SET @PU_ID = gt_strtok (GT_DB, 2, '_');
	SET @v_DATA_DATE = STR_TO_DATE(gt_strtok (GT_DB, 3, '_'), '%Y%m%d');
	SET NT_DB = CONCAT('gt_nt_', gt_strtok (GT_DB, 3, '_'));
	IF FLAG = 'HOURLY'
	THEN
	BEGIN
		SET @SqlCmd = CONCAT('DROP TABLE IF EXISTS ',GT_DB,'.tmp_rpt_cell_pmc_lte;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		SET @SqlCmd = CONCAT('CREATE TABLE IF NOT EXISTS ',GT_DB,'.tmp_rpt_cell_pmc_lte LIKE ',GT_DB,'.rpt_cell_pmc_lte_',DATA_HR);
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		SET @SqlCmd=CONCAT('INSERT INTO ',GT_DB,'.tmp_rpt_cell_pmc_lte 
					(DATA_DATE,DATA_HOUR,ENODEB_ID,CELL_ID,',TABLE_CELL_COL_STR,') 
					SELECT  
					DATA_DATE,DATA_HOUR,ENODEB_ID,IFNULL(CELL_ID, -1),',TABLE_CELL_SUM_STR,'
					FROM ',GT_DB,'.table_cell_lte 
					WHERE DATA_HOUR = ',DATA_HR,' 
					GROUP BY ENODEB_ID,CELL_ID ORDER BY NULL;
					');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt; 
		-- -------------------------------------------------------------------------------------------------------------------------------------    
		 
		-- -------------------------------------------------------------------------------------------------------------------------------------
		SET @SqlCmd = CONCAT('UPDATE ',GT_DB,'.tmp_rpt_cell_pmc_lte a 
				INNER JOIN 
				',GT_DB,'.rpt_cell_start_def_',DATA_HR,' b 
				ON a.cell_id = b.cell_id and a.enodeb_id = b.enodeb_id 
				SET a.imsi_cnt = b.imsi_cnt;
				');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		SET @SqlCmd = CONCAT('UPDATE ',GT_DB,'.tmp_rpt_cell_pmc_lte a 
				INNER JOIN 
				',GT_DB,'.rpt_cell_erab_end_def_',DATA_HR,' b 
				ON a.cell_id = b.cell_id and a.enodeb_id = b.enodeb_id
				SET a.Volte_Duration_Total = b.VOLTE_DURATION, 
				a.Volte_Duration_Cnt = b.VOLTE_END_CNT;
				');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		SET @SqlCmd = CONCAT('UPDATE ',GT_DB,'.tmp_rpt_cell_pmc_lte a
				INNER JOIN  
				',GT_DB,'.rpt_cell_dominatecallcell_',DATA_HR,' b
				ON a.CELL_ID = b.cell_id AND a.ENODEB_ID = b.ENODEB_ID
				SET a.Dominant_tile_cnt = b.TILE_CNT;
				');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		SET @SqlCmd = CONCAT('UPDATE ',GT_DB,'.tmp_rpt_cell_pmc_lte a
				INNER JOIN 
				( 
					SELECT CELL_ID ,ENODEB_ID ,
					SUM(DL_VOLUME_SUM) AS DL_VOLUME_SUM,
					SUM(UL_VOLUME_SUM) AS UL_VOLUME_SUM,
					SUM(DL_THROUPUT_SUM) AS DL_THROUGHPUT_SUM,
					SUM(UL_THROUPUT_SUM) AS UL_THROUGHPUT_SUM,
					SUM(DL_THROUPUT_CNT) AS DL_THROUGHPUT_CNT,
					SUM(UL_THROUPUT_CNT) AS UL_THROUGHPUT_CNT 
					FROM ',GT_DB,'.rpt_cell_position_def_',DATA_HR,' 
					GROUP BY cell_id, ENODEB_ID
				) AS T
				ON a.CELL_ID = T.cell_id AND a.ENODEB_ID = T.ENODEB_ID
				SET 
				a.DL_VOLUME_SUM = T.DL_VOLUME_SUM, 
				a.UL_VOLUME_SUM = T.UL_VOLUME_SUM, 
				a.DL_THROUGHPUT_SUM = T.DL_THROUGHPUT_SUM, 
				a.UL_THROUGHPUT_SUM = T.UL_THROUGHPUT_SUM,
				a.DL_THROUGHPUT_CNT = T.DL_THROUGHPUT_CNT,
				a.UL_THROUGHPUT_CNT = T.UL_THROUGHPUT_CNT;
				');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		SET @SqlCmd = CONCAT('UPDATE ',GT_DB,'.tmp_rpt_cell_pmc_lte a
				INNER JOIN  
				(
					SELECT 
					ENODEB_ID, CELL_ID,
					SUM(IF((INDOOR=1 AND CALL_STATUS IN(1,2,5,7)), RRC_SETUP_ATTEMPT, 0)) AS INDOOR_RADIO_CONN
					FROM ',GT_DB,'.rpt_cell_start_',DATA_HR,'
					GROUP BY ENODEB_ID, CELL_ID
				) AS T
				ON 
				a.CELL_ID = T.CELL_ID AND a.ENODEB_ID = T.ENODEB_ID
				SET 
				a.INDOOR_RADIO_CONN = T.INDOOR_RADIO_CONN
				');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;    
		SET @SqlCmd = CONCAT('UPDATE ',GT_DB,'.tmp_rpt_cell_pmc_lte a
				INNER JOIN  
				(
					SELECT 
					ENODEB_ID, CELL_ID, 
					SUM(SERVING_CNT) AS TOTAL_CALL_CNT
					FROM ',GT_DB,'.rpt_cell_end_def_',DATA_HR,'
					GROUP BY ENODEB_ID, CELL_ID
				) AS T
				ON 
				a.CELL_ID = T.CELL_ID AND a.ENODEB_ID = T.ENODEB_ID
				SET 
				a.TOTAL_CALL_CNT = T.TOTAL_CALL_CNT;
				');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		SET @SqlCmd = CONCAT(
				'UPDATE ',GT_DB,'.tmp_rpt_cell_pmc_lte a
				INNER JOIN  
				(
					SELECT 
					ENODEB_ID, CELL_ID, 
					SUM(IF(HO_COUNT > 0 AND NBR_TYPE IN (2,20), HO_COUNT,0)) AS IN_INTER_FREQ_HO,
					SUM(IF(HO_COUNT > 0 AND NBR_TYPE IN (1,10), HO_COUNT,0)) AS IN_INTRA_FREQ_HO
					FROM ',GT_DB,'.opt_nbr_inter_intra_lte_',DATA_HR,'
					GROUP BY ENODEB_ID, CELL_ID
				) AS T
				ON 
				a.CELL_ID = T.cell_id AND a.ENODEB_ID = T.ENODEB_ID
				SET 
				a. IN_INTER_FREQ_HO = T. IN_INTER_FREQ_HO,
				a. IN_INTRA_FREQ_HO = T. IN_INTRA_FREQ_HO;
				');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		
		SET @SqlCmd = CONCAT('UPDATE ',GT_DB,'.tmp_rpt_cell_pmc_lte a
				INNER JOIN  
				(
					SELECT 
					POS_LAST_S_CELL AS b, POS_LAST_S_ENODEB AS c, 
					SUM(IF(POS_LAST_CONFIDENCE IS NULL, 1, 0) ) AS CONFIDENCE_NO_POS,
					SUM(IF(POS_LAST_CONFIDENCE = 1, 1, 0))  AS CONFIDENCE_LOW,
					SUM(IF(POS_LAST_CONFIDENCE IN (2,3), 1, 0))  AS CONFIDENCE_MED,
					SUM(IF(POS_LAST_CONFIDENCE IN (4,5,6,7,11,12,21,22,23), 1, 0)) AS CONFIDENCE_HIGH
					FROM ',GT_DB,'.table_call_lte
					WHERE DATA_HOUR = ',DATA_HR,'
					GROUP BY POS_LAST_S_CELL, POS_LAST_S_ENODEB
				) AS T
				ON 
				a.CELL_ID = T.b AND a.ENODEB_ID = T.c
				SET 
				a.CONFIDENCE_NO_POS = T.CONFIDENCE_NO_POS,
				a.CONFIDENCE_LOW = T.CONFIDENCE_LOW,
				a.CONFIDENCE_MED = T.CONFIDENCE_MED,
				a.CONFIDENCE_HIGH = T.CONFIDENCE_HIGH;
				');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		  
		SET @SqlCmd = CONCAT('UPDATE ',GT_DB,'.tmp_rpt_cell_pmc_lte a
				INNER JOIN  
				(
					SELECT 
					ERAB_END_SERVING_ENODEB as b, ERAB_END_SERVING_CELL as c, 
					SUM(IF(QCI_ID=2 AND DURATION IS NOT NULL, DURATION, 0))AS VIDEO_DUR_SUM,
					SUM(IF(QCI_ID=2 AND DURATION IS NOT NULL, 1, 0))  AS VIDEO_DUR_CNT
					FROM ',GT_DB,'.table_erab_lte
					WHERE DATA_HOUR = ',DATA_HR,'
					GROUP BY ERAB_END_SERVING_ENODEB, ERAB_END_SERVING_CELL
				) AS T
				ON 
				a.CELL_ID = T.b AND a.ENODEB_ID = T.c
				SET 
				a.VIDEO_DUR_SUM = T.VIDEO_DUR_SUM,
				a.VIDEO_DUR_CNT = T.VIDEO_DUR_CNT;
				');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		SET @SqlCmd = CONCAT('UPDATE ',GT_DB,'.tmp_rpt_cell_pmc_lte a 
				INNER JOIN 
				',NT_DB,'.nt2_cell_lte b
				ON a.CELL_ID = b.cell_id AND a.ENODEB_ID = b.ENODEB_ID
				SET  
				a.PU_ID = b.PU_ID, 
				a.REGION_Name = b.REGION, 
				a.CLUSTER_Name =b.SUB_REGION, 
				a.REGION_ID = (b.CLUSTER_ID/10000), 
				a.CLUSTER_ID = (b.CLUSTER_ID%10000),
				a.Cell_Name = b.Cell_Name, 
				a.PCI = b.PCI, 
				a.NBR_DISTANCE_VORONOI = b.NBR_DISTANCE_VORONOI;
				');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;     
--	----------------------------------------------------------------
		SET @SqlCmd = CONCAT('INSERT INTO ',GT_DB,'.tmp_rpt_cell_pmc_lte 
					(DATA_DATE, DATA_HOUR, ENODEB_ID, CELL_ID, VOLTE_ATTEMPT, VOLTE_FAILURE) 
					SELECT c.DATA_DATE, c.DATA_HOUR, c.ENODEB_ID, c.CELL_ID, c.VOLTE_ATTEMPT, c.VOLTE_FAILURE
					FROM (
					    SELECT	
						b.DATA_DATE, 
						b.DATA_HOUR,
						IFNULL(b.ERAB_START_SERVING_ENODEB,0) AS ENODEB_ID, 
						IFNULL(b.ERAB_START_SERVING_CELL,0) AS CELL_ID,
						SUM(IF(b.QCI_ID=1, 1, 0)) AS VOLTE_ATTEMPT,
						SUM(IF(b.QCI_ID=1 AND b.ERAB_STATUS=6, 1, 0)) AS VOLTE_FAILURE
					    FROM (
						SELECT DATE(ERAB_START_TIME) AS DATA_DATE,HOUR(ERAB_START_TIME) AS DATA_HOUR,ERAB_START_SERVING_ENODEB,ERAB_START_SERVING_CELL,QCI_ID,ERAB_STATUS 
						FROM ',GT_DB,'.TABLE_ERAB_VOLTE_LTE_',DATA_HR,'00 
						UNION ALL 
						SELECT DATE(ERAB_START_TIME) AS DATA_DATE,HOUR(ERAB_START_TIME) AS DATA_HOUR,ERAB_START_SERVING_ENODEB,ERAB_START_SERVING_CELL,QCI_ID,ERAB_STATUS 
						FROM ',GT_DB,'.TABLE_ERAB_VOLTE_LTE_',DATA_HR,'15
						UNION ALL 
						SELECT DATE(ERAB_START_TIME) AS DATA_DATE,HOUR(ERAB_START_TIME) AS DATA_HOUR,ERAB_START_SERVING_ENODEB,ERAB_START_SERVING_CELL,QCI_ID,ERAB_STATUS 
						FROM ',GT_DB,'.TABLE_ERAB_VOLTE_LTE_',DATA_HR,'30
						UNION ALL 
						SELECT DATE(ERAB_START_TIME) AS DATA_DATE,HOUR(ERAB_START_TIME) AS DATA_HOUR,ERAB_START_SERVING_ENODEB,ERAB_START_SERVING_CELL,QCI_ID,ERAB_STATUS 
						FROM ',GT_DB,'.TABLE_ERAB_VOLTE_LTE_',DATA_HR,'45
					    ) AS b
					    WHERE b.DATA_DATE=''',@v_DATA_DATE,'''
					    GROUP BY b.ERAB_START_SERVING_ENODEB, b.ERAB_START_SERVING_CELL, b.DATA_DATE, b.DATA_HOUR
					) AS c 
					ON DUPLICATE KEY UPDATE 
					',GT_DB,'.tmp_rpt_cell_pmc_lte.VOLTE_ATTEMPT=c.VOLTE_ATTEMPT,',GT_DB,'.tmp_rpt_cell_pmc_lte.VOLTE_FAILURE=c.VOLTE_FAILURE;
					');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		    
		SET @SqlCmd = CONCAT('INSERT INTO ',GT_DB,'.tmp_rpt_cell_pmc_lte
				(DATA_DATE, DATA_HOUR, ENODEB_ID, CELL_ID, VOLTE_DURATION_TOTAL, VOLTE_DURATION_CNT)
				SELECT c.DATA_DATE, c.DATA_HOUR, c.ENODEB_ID, c.CELL_ID, c.VOLTE_DURATION_TOTAL, c.VOLTE_DURATION_CNT
				FROM (
					SELECT	
						b.DATA_DATE, 
						b.DATA_HOUR,
						IFNULL(b.ERAB_END_SERVING_ENODEB,0) AS ENODEB_ID, 
						IFNULL(b.ERAB_END_SERVING_CELL,0) AS CELL_ID,
						SUM(IF(b.QCI_ID=1 AND b.DURATION IS NOT NULL, b.DURATION, 0)) AS VOLTE_DURATION_TOTAL,
						SUM(IF(b.QCI_ID=1 AND b.DURATION IS NOT NULL, 1, 0)) AS VOLTE_DURATION_CNT
					FROM (
						SELECT DATE(ERAB_END_TIME) AS DATA_DATE,HOUR(ERAB_END_TIME) AS DATA_HOUR,ERAB_END_SERVING_ENODEB,ERAB_END_SERVING_CELL,QCI_ID,DURATION 
						FROM ',GT_DB,'.TABLE_ERAB_VOLTE_LTE_',DATA_HR,'00 
						UNION ALL 
						SELECT DATE(ERAB_END_TIME) AS DATA_DATE,HOUR(ERAB_END_TIME) AS DATA_HOUR,ERAB_END_SERVING_ENODEB,ERAB_END_SERVING_CELL,QCI_ID,DURATION 
						FROM ',GT_DB,'.TABLE_ERAB_VOLTE_LTE_',DATA_HR,'15
						UNION ALL 
						SELECT DATE(ERAB_END_TIME) AS DATA_DATE,HOUR(ERAB_END_TIME) AS DATA_HOUR,ERAB_END_SERVING_ENODEB,ERAB_END_SERVING_CELL,QCI_ID,DURATION 
						FROM ',GT_DB,'.TABLE_ERAB_VOLTE_LTE_',DATA_HR,'30
						UNION ALL 
						SELECT DATE(ERAB_END_TIME) AS DATA_DATE,HOUR(ERAB_END_TIME) AS DATA_HOUR,ERAB_END_SERVING_ENODEB,ERAB_END_SERVING_CELL,QCI_ID,DURATION 
						FROM ',GT_DB,'.TABLE_ERAB_VOLTE_LTE_',DATA_HR,'45
					) AS b
					WHERE b.DATA_DATE=''',@v_DATA_DATE,'''
					GROUP BY b.ERAB_END_SERVING_ENODEB, b.ERAB_END_SERVING_CELL, b.DATA_DATE, b.DATA_HOUR
				) AS c
				ON DUPLICATE KEY UPDATE 
				',GT_DB,'.tmp_rpt_cell_pmc_lte.VOLTE_DURATION_TOTAL=c.VOLTE_DURATION_TOTAL,',GT_DB,'.tmp_rpt_cell_pmc_lte.VOLTE_DURATION_CNT=c.VOLTE_DURATION_CNT;
				');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		    
		SET @SqlCmd = CONCAT('INSERT INTO ',GT_DB,'.tmp_rpt_cell_pmc_lte 
				(DATA_DATE, DATA_HOUR, ENODEB_ID, CELL_ID, pcS1ErabSetupTime, pcS1ErabSetupCnt) 
				SELECT c.DATA_DATE, c.DATA_HOUR, c.ENODEB_ID, c.CELL_ID, c.pcS1ErabSetupTime, c.pcS1ErabSetupCnt
				FROM (
					SELECT 
						b.DATA_DATE, b.DATA_HOUR,
						IFNULL(b.ERAB_START_SERVING_ENODEB,0) AS ENODEB_ID, 
						IFNULL(b.ERAB_START_SERVING_CELL,0) AS CELL_ID,
						SUM(b.ERAB_ACCESS_DELAY) AS pcS1ErabSetupTime,
						COUNT(b.ERAB_ACCESS_DELAY) AS pcS1ErabSetupCnt
					FROM (
						SELECT DATE(ERAB_START_TIME) AS DATA_DATE,HOUR(ERAB_START_TIME) AS DATA_HOUR,ERAB_START_SERVING_ENODEB,ERAB_START_SERVING_CELL,ERAB_ACCESS_DELAY 
						FROM ',GT_DB,'.TABLE_ERAB_LTE_',DATA_HR,'00 
						UNION ALL 
						SELECT DATE(ERAB_START_TIME) AS DATA_DATE,HOUR(ERAB_START_TIME) AS DATA_HOUR,ERAB_START_SERVING_ENODEB,ERAB_START_SERVING_CELL,ERAB_ACCESS_DELAY 
						FROM ',GT_DB,'.TABLE_ERAB_LTE_',DATA_HR,'15
						UNION ALL 
						SELECT DATE(ERAB_START_TIME) AS DATA_DATE,HOUR(ERAB_START_TIME) AS DATA_HOUR,ERAB_START_SERVING_ENODEB,ERAB_START_SERVING_CELL,ERAB_ACCESS_DELAY 
						FROM ',GT_DB,'.TABLE_ERAB_LTE_',DATA_HR,'30
						UNION ALL 
						SELECT DATE(ERAB_START_TIME) AS DATA_DATE,HOUR(ERAB_START_TIME) AS DATA_HOUR,ERAB_START_SERVING_ENODEB,ERAB_START_SERVING_CELL,ERAB_ACCESS_DELAY 
						FROM ',GT_DB,'.TABLE_ERAB_LTE_',DATA_HR,'45
					) AS b
					WHERE b.DATA_DATE=''',@v_DATA_DATE,'''
					GROUP BY b.ERAB_START_SERVING_ENODEB, b.ERAB_START_SERVING_CELL, b.DATA_DATE, b.DATA_HOUR
				) AS c 
				ON DUPLICATE KEY UPDATE 
				',GT_DB,'.tmp_rpt_cell_pmc_lte.pcS1ErabSetupTime=c.pcS1ErabSetupTime,',GT_DB,'.tmp_rpt_cell_pmc_lte.pcS1ErabSetupCnt=c.pcS1ErabSetupCnt;
				');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;  
-- 	----------------------------------------------------------------
		SET @SqlCmd = CONCAT('CREATE TEMPORARY TABLE IF NOT EXISTS ',GT_DB,'.tmp_data_hour AS
				SELECT DISTINCT DATA_HOUR FROM ',GT_DB,'.tmp_rpt_cell_pmc_lte;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		SET @hour_count = 0;
		WHILE(@hour_count < 24) DO 
			SET @isvalid = 0;
			SET @SqlCmd = CONCAT('SELECT COUNT(*) into @isvalid FROM ',GT_DB,'.tmp_data_hour WHERE DATA_HOUR = ',@hour_count,';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
			IF (@isvalid)THEN 
				SET @hour_count_lapd = LPAD(@hour_count, 2, '0');
				SET @rpt_target_table = CONCAT(GT_DB,'.rpt_cell_pmc_lte_',@hour_count_lapd);
				SET @SqlCmd = CONCAT('INSERT INTO ',GT_DB,'.rpt_cell_pmc_lte_',@hour_count_lapd,'
								(
									DATA_DATE,DATA_HOUR,ENODEB_ID,CELL_ID,
									CELL_Name,CLUSTER_id,CLUSTER_NAME,REGION_ID,REGION_NAME,PU_ID,
									',TABLE_RPT_COL_STR,'
								)
								SELECT	
								DATA_DATE,DATA_HOUR,ENODEB_ID,CELL_ID,CELL_Name,CLUSTER_id,CLUSTER_NAME,REGION_ID,REGION_NAME,PU_ID,',COLUMN_IFNULL_STR,'
								FROM ',GT_DB,'.tmp_rpt_cell_pmc_lte
								WHERE DATA_HOUR = ',@hour_count,' 
								ON DUPLICATE KEY UPDATE 
								',REPLACE(COLUMN_UPD_STR,'RPT_TABLE_NAME',CONCAT(@rpt_target_table)),';
								');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
			END IF;
			SET @hour_count = @hour_count + 1;
		END WHILE;
		
		INSERT INTO gt_gw_main.SP_LOG VALUES (TMP_GT_DB,'SP_Sub_Gen_CELL_PMC_RPT',CONCAT('HR Done: ',TIMESTAMPDIFF(SECOND, START_TIME, SYSDATE()),' seconds.'),NOW());
		
		SET @rpt_target_table = CONCAT(GT_DB, '.rpt_cell_pmc_lte_dy');
		SET @SqlCmd = CONCAT('INSERT INTO ',GT_DB,'.rpt_cell_pmc_lte_dy 
				(DATA_DATE,ENODEB_ID,CELL_ID,CELL_Name,CLUSTER_id,CLUSTER_NAME,REGION_ID,REGION_NAME,PU_ID,',TABLE_RPT_COL_STR,')
				SELECT	
				DATA_DATE,ENODEB_ID,CELL_ID,CELL_Name,CLUSTER_id,CLUSTER_NAME,REGION_ID,REGION_NAME,PU_ID,',COLUMN_IFNULL_STR,'
				FROM ',GT_DB,'.tmp_rpt_cell_pmc_lte
				ON DUPLICATE KEY UPDATE 
				',REPLACE(COLUMN_UPD_STR,'RPT_TABLE_NAME',CONCAT(@rpt_target_table)),';
				');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		
		SET @SqlCmd = CONCAT('UPDATE ',GT_DB,'.rpt_cell_pmc_lte_dy a 
				INNER JOIN 
				',GT_DB,'.rpt_cell_start_dy_def b 
				ON a.cell_id = b.cell_id and a.enodeb_id = b.enodeb_id 
				SET 
				a.imsi_cnt = b.imsi_cnt;
				');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		
		SET @SqlCmd = CONCAT('UPDATE ',GT_DB,'.rpt_cell_pmc_lte_dy a
				INNER JOIN  
				',GT_DB,'.rpt_cell_dominatecallcell_dy b
				ON a.CELL_ID = b.cell_id AND a.ENODEB_ID = b.ENODEB_ID
				SET 
				a.Dominant_tile_cnt = b.TILE_CNT;
				');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		
		SET @SqlCmd = CONCAT('UPDATE ',GT_DB,'.rpt_cell_pmc_lte_dy a
				INNER JOIN 
				( 
					SELECT CELL_ID ,ENODEB_ID ,
					SUM(DL_VOLUME_SUM) AS DL_VOLUME_SUM,
					SUM(UL_VOLUME_SUM) AS UL_VOLUME_SUM,
					SUM(DL_THROUPUT_SUM) AS DL_THROUGHPUT_SUM,
					SUM(UL_THROUPUT_SUM) AS UL_THROUGHPUT_SUM,
					SUM(DL_THROUPUT_CNT) AS DL_THROUGHPUT_CNT,
					SUM(UL_THROUPUT_CNT) AS UL_THROUGHPUT_CNT 
					FROM ',GT_DB,'.rpt_cell_position_dy_def 
					GROUP BY cell_id, ENODEB_ID
				) AS T
				ON a.CELL_ID = T.cell_id AND a.ENODEB_ID = T.ENODEB_ID
				SET 
					a.DL_VOLUME_SUM = T.DL_VOLUME_SUM, 
					a.UL_VOLUME_SUM = T.UL_VOLUME_SUM, 
					a.DL_THROUGHPUT_SUM = T.DL_THROUGHPUT_SUM, 
					a.UL_THROUGHPUT_SUM = T.UL_THROUGHPUT_SUM,
					a.DL_THROUGHPUT_CNT = T.DL_THROUGHPUT_CNT,
					a.UL_THROUGHPUT_CNT = T.UL_THROUGHPUT_CNT;
				');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;   
		SET @SqlCmd = CONCAT('DROP TABLE IF EXISTS ',GT_DB,'.tmp_data_hour;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		SET @SqlCmd = CONCAT('DROP TABLE IF EXISTS ',GT_DB,'.tmp_rpt_cell_pmc_lte;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		INSERT INTO gt_gw_main.SP_LOG VALUES(TMP_GT_DB,'SP_Sub_Gen_CELL_PMC_RPT',CONCAT('DY Done: ',TIMESTAMPDIFF(SECOND, START_TIME, SYSDATE()),' seconds.'),NOW());
		
	END;
	END IF;
	IF FLAG = 'DAILY'
	THEN
	BEGIN
		SET @SqlCmd = CONCAT('UPDATE ',GT_DB,'.rpt_cell_pmc_lte_dy a 
				INNER JOIN 
				',GT_DB,'.rpt_cell_start_dy_def b 
				ON a.cell_id = b.cell_id and a.enodeb_id = b.enodeb_id 
				SET 
				a.imsi_cnt = b.imsi_cnt;
				');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		
		SET @SqlCmd = CONCAT('UPDATE ',GT_DB,'.rpt_cell_pmc_lte_dy a
				INNER JOIN  
				',GT_DB,'.rpt_cell_dominatecallcell_dy b
				ON a.CELL_ID = b.cell_id AND a.ENODEB_ID = b.ENODEB_ID
				SET 
				a.Dominant_tile_cnt = b.TILE_CNT;
				');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		
		SET @SqlCmd = CONCAT('UPDATE ',GT_DB,'.rpt_cell_pmc_lte_dy a
				INNER JOIN 
				( 
					SELECT CELL_ID ,ENODEB_ID ,
					SUM(DL_VOLUME_SUM) AS DL_VOLUME_SUM,
					SUM(UL_VOLUME_SUM) AS UL_VOLUME_SUM,
					SUM(DL_THROUPUT_SUM) AS DL_THROUGHPUT_SUM,
					SUM(UL_THROUPUT_SUM) AS UL_THROUGHPUT_SUM,
					SUM(DL_THROUPUT_CNT) AS DL_THROUGHPUT_CNT,
					SUM(UL_THROUPUT_CNT) AS UL_THROUGHPUT_CNT 
					FROM ',GT_DB,'.rpt_cell_position_dy_def 
					GROUP BY cell_id, ENODEB_ID
				) AS T
				ON a.CELL_ID = T.cell_id AND a.ENODEB_ID = T.ENODEB_ID
				SET 
					a.DL_VOLUME_SUM = T.DL_VOLUME_SUM, 
					a.UL_VOLUME_SUM = T.UL_VOLUME_SUM, 
					a.DL_THROUGHPUT_SUM = T.DL_THROUGHPUT_SUM, 
					a.UL_THROUGHPUT_SUM = T.UL_THROUGHPUT_SUM,
					a.DL_THROUGHPUT_CNT = T.DL_THROUGHPUT_CNT,
					a.UL_THROUGHPUT_CNT = T.UL_THROUGHPUT_CNT;
				');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		
		SET @SqlCmd = CONCAT('UPDATE ',GT_DB,'.rpt_cell_pmc_lte_dy a
				INNER JOIN 
				',GT_DB,'.table_overshooting_severity_lte_d1 b 
				ON a.CELL_ID = b.cell_id AND a.ENODEB_ID = b.ENODEB_ID
				set 
				a.t1_tile_cnt = b.t1_tile_cnt, a.t0_tile_cnt = b.t0_tile_cnt;
				');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
	END;
	END IF;
