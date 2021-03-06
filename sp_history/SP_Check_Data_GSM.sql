DELIMITER $$
USE `gt_gw_main`$$
DROP PROCEDURE IF EXISTS `SP_CreateDB_LTE`$$
CREATE DEFINER=`covmo`@`%` PROCEDURE `SP_Check_Data_GSM`(IN GT_DB VARCHAR(100))
BEGIN
        DECLARE CURRENT_NT_DB VARCHAR(50) DEFAULT CONCAT('gt_nt_',gt_strtok(GT_DB,3,'_'));
        SET @SqlCmd=CONCAT('SELECT COUNT(*) INTO @S1
                            FROM (
                                SELECT A.SERVING_CID FROM ',GT_DB,'.table_event_gsm A,',CURRENT_NT_DB,'.nt_cell_current_gsm B
                                WHERE A.serving_cid=B.CELL_ID AND A.serving_bsc = b.BSC_ID AND A.serving_lac = B.LAC
                                AND A.SERVING_CID IS NOT NULL AND A.serving_bsc IS NOT NULL AND A.serving_lac IS NOT NULL
                                LIMIT 1    ) AA');
        PREPARE Stmt FROM @SqlCmd;
        EXECUTE Stmt;
        DEALLOCATE PREPARE Stmt; 
 
        SET @SqlCmd=CONCAT('SELECT COUNT(*) INTO @S2
                            FROM (
                                SELECT A.SERVING_CID FROM ',GT_DB,'.table_rsl_gsm A,',CURRENT_NT_DB,'.nt_cell_current_gsm B
                                WHERE A.serving_cid=B.CELL_ID AND A.serving_bsc = b.BSC_ID AND A.serving_lac = B.LAC
                                AND A.SERVING_CID IS NOT NULL AND A.serving_bsc IS NOT NULL AND A.serving_lac IS NOT NULL
                                LIMIT 1    ) AA');
        PREPARE Stmt FROM @SqlCmd;
        EXECUTE Stmt;
        DEALLOCATE PREPARE Stmt; 
 
        SET @SqlCmd=CONCAT('SELECT COUNT(*) INTO @S3
                            FROM (
                                SELECT A.start_cell_id FROM ',GT_DB,'.table_call_gsm A,',CURRENT_NT_DB,'.nt_cell_current_gsm B
                                WHERE A.start_cell_id=B.CELL_ID AND A.start_bsc_id = b.BSC_ID AND A.start_lac = B.LAC
                                AND A.start_cell_id IS NOT NULL AND A.start_bsc_id IS NOT NULL AND A.start_lac IS NOT NULL
                                LIMIT 1    ) AA');
        PREPARE Stmt FROM @SqlCmd;
        EXECUTE Stmt;
        DEALLOCATE PREPARE Stmt; 
 
	IF (@S1 + @S2 + @S3) > 0 THEN
		SELECT 1;
	ELSE
		SELECT 0;
	END IF;
END$$
DELIMITER ;
