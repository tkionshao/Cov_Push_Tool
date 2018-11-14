DELIMITER $$
USE `gt_gw_main`$$
DROP PROCEDURE IF EXISTS `SP_CreateDB_LTE`$$
CREATE DEFINER=`covmo`@`%` PROCEDURE `sp_import_pmw_to_dailyDB`()
a_label:
BEGIN
	DECLARE done INT DEFAULT 0; 
	DECLARE START_TIME DATETIME DEFAULT SYSDATE();
	DECLARE gt_db VARCHAR(50) DEFAULT '';
	DECLARE V_NT_DATE DATE DEFAULT DATE(NOW());	
	DECLARE v_cnt INT;
	DECLARE v_data_date VARCHAR(100);
	DECLARE v_session_db VARCHAR(100);
	DECLARE v_RNC VARCHAR(100);	
	DECLARE cursor_name CURSOR
	FOR 
	SELECT B.DATA_DATE,B.SESSION_dB,RNC FROM 
	( 
	SELECT DISTINCT DATE_FORMAT(DATA_DATE,'%Y-%m-%d') DATA_DATE FROM gt_gw_main.table_pmw
	) A
	INNER JOIN (
	SELECT DISTINCT DATE_FORMAT(file_starttime,'%Y-%m-%d') DATA_dATE,SESSION_DB,RNC FROM gt_gw_main.session_information WHERE session_type='DAY'
	) B
	ON A.DATA_DATE=B.DATA_DATE ;		
	DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = 1; 		
  	INSERT INTO gt_gw_main.SP_LOG VALUES(GT_DB,'SP_Import_pmw_to_DailyDB','START', START_TIME);	
  	
 	OPEN cursor_name;
        REPEAT
        FETCH cursor_name INTO v_data_date,v_session_db,v_RNC;
        IF NOT done THEN
		
	
	
	
		SET @SqlCmd =CONCAT('TRUNCATE TABLE ',v_session_db,'.','table_pm_counter_lte'); 			
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		
	
		SET @SqlCmd =CONCAT('INSERT INTO  ',v_session_db,'.','table_pm_counter_lte(
			`DATA_DATE`, 
			DATA_HOUR,
			`CELL_ID`, 
			`CELL_NAME`, 
			`NODEID`, 
			`ENODEB_ID`,
			`PU_ID`, 
			`EUTRABAND`, 
			`EARFCN`,
			`SUB_REGION_ID`,
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
			
			  `DATA_DATE`, 
			   ''0'',
			  `CELL_ID`, 
			  `CELL_NAME`, 
			  `NODEID`, 
			  `ENODEB_ID`,
			  `PU_ID`, 
			  `EUTRABAND`, 
			  `EARFCN`, 
			   `SUB_REGION_ID`,
			  `NoRrcConnectReqSuccess`,
			  `NoRrcConnectAtt`,
			  `NoErabEstabSucc`,
			  `NoErabEstabAtt`,
			  `NoErabRelAbnormal`,
			  `NoErabRelTot`,
			  `DataVolumePSDLMB`,
			  `DataVolumePSULMB`,
			  `CellTotalTime`,
			  `CellAvailabilityTime`,
			  `AvgNoActiveUser`,
			  `MaxNoActiveUser`,
			  `DataCoverAge`,
			  `CSSRPSDen`,
			  `CSSRPSNum`,
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
			   FROM `gt_gw_main`.`table_pmw`
			WHERE data_date=''',v_data_date,'''
			AND PU_ID = ''',v_RNC,''';		
		'); 	
		
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		
        END IF;
        UNTIL done END REPEAT;
        CLOSE cursor_name;        		
	INSERT INTO gt_gw_main.SP_LOG VALUES(GT_DB,'SP_Import_pmw_to_DailyDB',CONCAT('Done: ',TIMESTAMPDIFF(SECOND,START_TIME,SYSDATE()),' seconds.'), NOW());
	
	
END$$
DELIMITER ;
