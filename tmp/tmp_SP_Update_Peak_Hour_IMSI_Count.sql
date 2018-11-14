CREATE DEFINER=`covmo`@`%` PROCEDURE `SP_Update_Peak_Hour_IMSI_Count`(IN GT_Daily_DB VARCHAR(50))
BEGIN	
	SET @SqlCmd = CONCAT ('UPDATE ',GT_Daily_DB,'.rpt_cell_start_dy A
				INNER JOIN 
				(SELECT ENODEB_ID, CELL_ID, MAX(imsi_cnt) AS imsi_cnt FROM ',GT_Daily_DB,'.rpt_cell_start GROUP BY ENODEB_ID, CELL_ID) B
				ON A.ENODEB_ID = B.ENODEB_ID AND A.CELL_ID = B.CELL_ID
				SET A.peak_hour_imsi_cnt = B.imsi_cnt;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd = CONCAT ('UPDATE ',GT_Daily_DB,'.rpt_cell_start_dy_def A
				INNER JOIN ',GT_Daily_DB,'.rpt_cell_start_dy B
				ON A.ENODEB_ID = B.ENODEB_ID AND A.CELL_ID = B.CELL_ID
				SET A.peak_hour_imsi_cnt = B.peak_hour_imsi_cnt;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
