DELIMITER $$
USE `gt_gw_main`$$
DROP PROCEDURE IF EXISTS `SP_CreateDB_LTE`$$
CREATE DEFINER=`covmo`@`%` PROCEDURE `SP_Generate_PMW_LTE_Daily`(IN GT_DB VARCHAR(100))
a_label:
BEGIN
	DECLARE START_TIME DATETIME DEFAULT SYSDATE();
	DECLARE V_NT_DATE DATE DEFAULT DATE(NOW());	
	DECLARE v_cnt INT;
  	INSERT INTO gt_gw_main.SP_LOG VALUES(GT_DB,'SP_Generate_PMW_LTE_Daily','START', START_TIME);	
  	 
TRUNCATE TABLE gt_Gw_main.table_pmw;
	    SET SESSION max_heap_table_size = 1024*1024*1024*4; 
	    SET SESSION tmp_table_size = 1024*1024*1024*4; 
	    SET SESSION join_buffer_size = 1024*1024*1024; 
	    SET SESSION sort_buffer_size = 1024*1024*1024; 
	    SET SESSION read_buffer_size = 1024*1024*1024; 
 
	
	
  	SET @SqlCmd=CONCAT('
  	INSERT INTO gt_gw_main.table_pmw(
			`DATA_DATE`,
			`NODEID`,
			`ENODEB_ID`,
			`CELL_ID`,
			`CELL_NAME`,
			 PU_ID,
			 SUB_REGION_ID,
			 EUTRABAND,
			 EARFCN,
			`NoRrcConnectReqSuccess`,
			`NoRrcConnectAtt`,
			`NoErabEstabSucc`,
			`NoErabEstabAtt`,
			`NoErabRelAbnormal`,
			`NoErabRelTot`,
			`DataVolumePSDLMB`,
			`DataVolumePSULMB`,
			`CellTotalTime` ,
			`CellAvailabilityTime` ,
			`AvgNoActiveUser` ,
			`MaxNoActiveUser` ,
			`DataCoverAge`,
			`CSSRPSDen` ,
			`CSSRPSNum` ,
			`CellLoadActUEAvg`,
			`CellLoadActUEMax`,
			`MIMO_OL_den`,
			`MIMO_OL_num`,
			`MIMO_CL_den`,			
			`MIMO_CL_num`,
			`MaxCellTHPDL`,
			`AvgCellTHPDL`,
			`MaxCellTHPUL`,
			`AvgCellTHPUL`,
			`LTE_CQI_AVG_x_x`,
		        `LTE_CQI_1_15`,
		        `LTE_CQI_10_15`,
		        `LTE_CQI_07_09`,
		        `LTE_CQI_01_06`,
		        `LTE_DL_MCS_0_28`,
		        `LTE_DL_QPSK_0_9`,
		        `LTE_DL_16QAM_10_16`,
		        `LTE_DL_64QAM_17_28`,
		        `LTE_UL_MCS_0_28`,
		        `LTE_UL_QPSK_0_10` ,
		        `LTE_UL_16QAM_11_20` ,
			`LTE_UL_64QAM_21_28`,
		        `LTE_SCell_CONFIG_SUCC`,
		        `LTE_SCell_CONFIG_ATT`
			)
			SELECT
				STR_TO_DATE(A.`DAY`,"%d/%m/%Y") DATA_DATE  ,
				A.`NODEID`,
				B.ENODEB_ID,
				B.CELL_ID,
				B.`CELL_NAME`,
				B.PU_ID,
			        B.SUB_REGION_ID,
				B.EUTRABAND,
				B.DL_EARFCN,
				A.LTE_RRC_SSR_PS_num AS NoRrcConnectReqSuccess,
				A.LTE_RRC_SSR_PS_den AS NoRrcConnectAtt,
				A.LTE_ERAB_SSR_PS_num AS NoErabEstabSucc,
				A.LTE_ERAB_SSR_PS_den AS NoErabEstabAtt,
				A.LTE_ERAB_DR_PS_num AS NoErabRelAbnormal,
				A.LTE_ERAB_DR_PS_den AS NoErabRelTot,
				A.LTE_DATA_VOLUME_PS_DL AS DataVolumePSDLMB,
				A.LTE_DATA_VOLUME_PS_UL AS DataVolumePSULMB,
				A.LTE_CELL_AVAILABILITY_SYS_den AS CellTotalTime,
				A.LTE_CELL_AVAILABILITY_SYS_num AS CellAvailabilityTime,
				A.LTE_AVG_NO_ACTIVE_USER AS AvgNoActiveUser,         
				A.LTE_MAX_NO_ACTIVE_USER AS MaxNoActiveUser,        
				A.DATACOVERAGE AS DataCoverAge,         
				A.LTE_CSSR_PS_den AS CSSRPSDen,                
				A.LTE_CSSR_PS_num AS CSSRPSNum,                
				A.LTE_AVG_NO_ACTIVE_USER AS CellLoadActUEAvg,         
				A.LTE_MAX_NO_ACTIVE_USER AS CellLoadActUEMax,
				A.`LTE_MIMO_OL_den`,
				A.`LTE_MIMO_OL_num`,
				A.`LTE_MIMO_CL_den`,
				A.`LTE_MIMO_CL_num`,
				A.MaxCellTHPDL AS MaxCellTHPDL,
				A.AvgCellTHPDL   AS AvgCellTHPDL,
				A.MaxCellTHPUL AS MaxCellTHPUL,
				A.AvgCellTHPUL AS AvgCellTHPUL,
				A.`LTE_CQI_AVG_x_x`,
				A.`LTE_CQI_1_15`,
				A.`LTE_CQI_10_15`,
				A.`LTE_CQI_07_09`,
				A.`LTE_CQI_01_06`,
				A.`LTE_DL_MCS_0_28`,
				A.`LTE_DL_QPSK_0_9`,
				A.`LTE_DL_16QAM_10_16`,
				A.`LTE_DL_64QAM_17_28`,
				A.`LTE_UL_MCS_0_28`,
				A.`LTE_UL_QPSK_0_10` ,
				A.`LTE_UL_16QAM_11_20` ,
				A.`LTE_UL_64QAM_21_28`,
				A.`LTE_SCell_CONFIG_SUCC`,
				A.`LTE_SCell_CONFIG_ATT`
				FROM `gt_gw_main`.`table_pmw_source_huawei` A
				LEFT JOIN ',GT_DB,'.nt_cell_current_lte B
				ON A.`NODEID`=B.`CELL_OSS_NODE_ID`	
				;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt; 	
 	  SET @SqlCmd=CONCAT('INSERT INTO gt_gw_main.table_pmw
	(
				`DATA_DATE`,
				`NODEID`,
				`ENODEB_ID`,
				`CELL_ID`,
				`CELL_NAME`,	
				`PU_ID`,
				`SUB_REGION_ID`,
				 EUTRABAND,
				 EARFCN,
				`NoRrcConnectReqSuccess`,
				`NoRrcConnectAtt`,
				`NoErabEstabSucc`,
				`NoErabEstabAtt`,
				`NoErabRelAbnormal`,
				`NoErabRelTot`,
				`DataVolumePSDLMB`,
				`DataVolumePSULMB`,
				`CellTotalTime` ,
				`CellAvailabilityTime` ,
				`AvgNoActiveUser` ,
				`MaxNoActiveUser` ,
				`DataCoverAge`,
				`CSSRPSDen` ,
				`CSSRPSNum` ,
				`CellLoadActUEAvg`,
				`CellLoadActUEMax`,	
				`MIMO_OL_den`,
				`MIMO_OL_num`,
				`MIMO_CL_den`,			
				`MIMO_CL_num`,
				`MaxCellTHPDL`,
				`AvgCellTHPDL`,
				`MaxCellTHPUL`,
				`AvgCellTHPUL`,
				`LTE_CQI_AVG_x_x`,
				`LTE_CQI_1_15`,
				`LTE_CQI_10_15`,
				`LTE_CQI_07_09`,
				`LTE_CQI_01_06`,
				`LTE_DL_MCS_0_28`,
				`LTE_DL_QPSK_0_9`,
				`LTE_DL_16QAM_10_16`,
				`LTE_DL_64QAM_17_28`,
				`LTE_UL_MCS_0_28`,
				`LTE_UL_QPSK_0_10` ,
				`LTE_UL_16QAM_11_20` ,
				`LTE_UL_64QAM_21_28`,
				`LTE_SCell_CONFIG_SUCC`,
				`LTE_SCell_CONFIG_ATT` 
				)
			SELECT
				STR_TO_DATE(A.`DAY`,"%d/%m/%Y") DATA_DATE  ,
				A.`NODEID`,
				B.ENODEB_ID,
				B.CELL_ID,
				B.`CELL_NAME`,
				B.PU_ID,
			        B.SUB_REGION_ID,
				B.EUTRABAND,
				B.DL_EARFCN,
				A.LTE_RRC_SSR_PS_num AS NoRrcConnectReqSuccess,
				A.LTE_RRC_SSR_PS_den AS NoRrcConnectAtt,
				A.LTE_ERAB_SSR_PS_num AS NoErabEstabSucc,
				A.LTE_ERAB_SSR_PS_den AS NoErabEstabAtt,
				A.LTE_ERAB_DR_PS_num AS NoErabRelAbnormal,
				A.LTE_ERAB_DR_PS_den AS NoErabRelTot,
				A.LTE_DATA_VOLUME_PS_DL AS DataVolumePSDLMB,
				A.LTE_DATA_VOLUME_PS_UL AS DataVolumePSULMB,
				A.LTE_CELL_AVAILABILITY_SYS_den AS CellTotalTime,
				A.LTE_CELL_AVAILABILITY_SYS_num AS CellAvailabilityTime,
				A.LTE_AVG_NO_ACTIVE_USER AS AvgNoActiveUser,         
				A.LTE_MAX_NO_ACTIVE_USER AS MaxNoActiveUser,        
				A.DATACOVERAGE AS DataCoverAge,         
				A.LTE_CSSR_PS_den AS CSSRPSDen,                
				A.LTE_CSSR_PS_num AS CSSRPSNum,                
				A.LTE_AVG_NO_ACTIVE_USER AS CellLoadActUEAvg,         
				A.LTE_MAX_NO_ACTIVE_USER AS CellLoadActUEMax,				
				A.`LTE_MIMO_OL_den`,
				A.`LTE_MIMO_OL_num`,
				A.`LTE_MIMO_CL_den`,
				A.`LTE_MIMO_CL_num`,
				A.MaxCellTHPDL AS MaxCellTHPDL,
				A.AvgCellTHPDL   AS AvgCellTHPDL,
				A.MaxCellTHPUL AS MaxCellTHPUL,
				A.AvgCellTHPUL AS AvgCellTHPUL,
				A.`LTE_CQI_AVG_x_x`,
				A.`LTE_CQI_1_15`,
				A.`LTE_CQI_10_15`,
				A.`LTE_CQI_07_09`,
				A.`LTE_CQI_01_06`,
				A.`LTE_DL_MCS_0_28`,
				A.`LTE_DL_QPSK_0_9`,
				A.`LTE_DL_16QAM_10_16`,
				A.`LTE_DL_64QAM_17_28`,
				A.`LTE_UL_MCS_0_28`,
				A.`LTE_UL_QPSK_0_10` ,
				A.`LTE_UL_16QAM_11_20` ,
				A.`LTE_UL_64QAM_21_28`,
				A.`LTE_SCell_CONFIG_SUCC`,
				A.`LTE_SCell_CONFIG_ATT`
				FROM `gt_gw_main`.`table_pmw_source_nsn` A
				LEFT JOIN ',GT_DB,'.nt_cell_current_lte B
				ON A.`NODEID`=B.`CELL_OSS_NODE_ID`
					;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt; 
	
		
			
	INSERT INTO gt_gw_main.SP_LOG VALUES(GT_DB,'SP_Generate_PMW_LTE_Daily',CONCAT('Done: ',TIMESTAMPDIFF(SECOND,START_TIME,SYSDATE()),' seconds.'), NOW());
	
END$$
DELIMITER ;
