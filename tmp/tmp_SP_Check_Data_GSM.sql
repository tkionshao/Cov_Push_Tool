CREATE DEFINER=`covmo`@`%` PROCEDURE `SP_Check_Data_GSM`(IN GT_DB VARCHAR(100))
BEGIN
        DECLARE CURRENT_NT_DB VARCHAR(50) DEFAULT CONCAT('gt_nt_',gt_strtok(GT_DB,3,'_'));
        SET @SqlCmd=CONCAT('SELECT COUNT(*) 
                            FROM (
                                SELECT A.SERVING_CID FROM ',GT_DB,'.table_event_gsm A,',CURRENT_NT_DB,'.nt_cell_current_gsm B
                                WHERE A.SERVING_CID=B.CELL_ID
                                AND A.SERVING_CID IS NOT NULL
                                LIMIT 1    ) AA');
        PREPARE Stmt FROM @SqlCmd;
        EXECUTE Stmt;
        DEALLOCATE PREPARE Stmt; 
