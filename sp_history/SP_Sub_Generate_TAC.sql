DELIMITER $$
USE `gt_gw_main`$$
DROP PROCEDURE IF EXISTS `SP_CreateDB_LTE`$$
CREATE DEFINER=`covmo`@`%` PROCEDURE `SP_Sub_Generate_TAC`(IN GT_DB VARCHAR(100), IN KIND VARCHAR(20), IN VENDOR_SOURCE VARCHAR(20),IN GT_COVMO VARCHAR(100))
BEGIN
        DECLARE RNC_ID INT;
        DECLARE O_GT_DB VARCHAR(100) DEFAULT GT_DB;
        DECLARE START_TIME DATETIME DEFAULT SYSDATE();
        DECLARE PARTITION_ID INT DEFAULT SUBSTRING(RIGHT(GT_DB,18),10,2) ;
        DECLARE SH_EH VARCHAR(9) DEFAULT RIGHT(GT_DB,9);
        DECLARE STARTHOUR VARCHAR(2) DEFAULT SUBSTRING(RIGHT(GT_DB,18),10,2);
        DECLARE ENDHOUR VARCHAR(2) DEFAULT IF(SUBSTRING(RIGHT(GT_DB,18),15,2)='00','24',SUBSTRING(RIGHT(GT_DB,18),15,2));
        DECLARE RUN VARCHAR(20);
        
       SELECT gt_strtok(GT_DB,2,'_') INTO RNC_ID;
        CALL SP_Sub_Set_Session_Param(GT_DB);
 
        SELECT REPLACE(GT_DB,SH_EH,'0000_0000') INTO GT_DB;
        
        IF VENDOR_SOURCE = 'GW' THEN
                IF KIND = 'DAILY' THEN
                        SET RUN = '_tmp';
                ELSEIF KIND = 'RERUN' THEN
                        SET RUN = '_rerun';
                END IF;
        ELSEIF VENDOR_SOURCE = 'AP' THEN
                SET RUN = '';
        END IF;
        
        SET @SqlCmd=CONCAT('ALTER TABLE ',GT_DB,RUN,'.table_tac TRUNCATE PARTITION h',PARTITION_ID,';');
        PREPARE Stmt FROM @SqlCmd;
        EXECUTE Stmt;
        DEALLOCATE PREPARE Stmt;         
        
        INSERT INTO gt_gw_main.sp_log VALUES(O_GT_DB,'SP_Sub_Generate_TAC','Insert Data to table_tac', NOW());
        
        SET @SqlCmd=CONCAT('INSERT INTO  ',GT_DB,RUN,'.table_tac
                            (
			     `DATA_DATE`,`DATA_HOUR`,
			     `RNC_ID`,
                             `MAKE_ID`,
                             `MODEL_ID`,
                             `CALL_TYPE`,
                             `CALL_STATUS`,
                             `TTL_CNT`,
                             `RRC_CONNECT_DURATION_SUM`,
                             `RRC_CONNECT_DURATION_CNT`,
                             `RRC_CONNECT_DURATION_SUM_3600`,
                             `DL_VOLUME_SUM`, 
                             `UL_VOLUME_SUM`,
                             `TRAFFIC_VOLUME_SUM`,
                             `CALL_STATUS_3_CNT`,
                             `CALL_STATUS_2_CNT`,
                             `CALL_STATUS_124_CNT`,
                             `POS_FIRST_RSCP_SUM`,
                             `POS_FIRST_RSCP_CNT`,
                             `POS_FIRST_ECN0_SUM`,
                             `POS_FIRST_ECN0_CNT`,
                             `POS_AS1_RSCP_SUM`,
                             `POS_AS1_RSCP_CNT`,
                             `POS_AS1_ECN0_SUM`,
                             `POS_AS1_ECN0_CNT`,
                             `POS_FIRST_RSCP_S_SUM`,
                             `POS_FIRST_RSCP_S_CNT`,
                             `POS_FIRST_ECN0_S_SUM`,
                             `POS_FIRST_ECN0_S_CNT`,
			     `CONVERSATIONALCALL_CNT`,
			     `STREAMINGCALL_CNT`,
			     `INTERACTIVECALL_CNT`,
			     `BACKGROUNDCALL_CNT`,
			     `EMERGENCYCALL_CNT` )                               
			SELECT 
				`DATA_DATE`,`DATA_HOUR`
				,POS_FIRST_RNC
				,MAKE_ID
				,MODEL_ID
				,CALL_TYPE
				,CALL_STATUS
				,COUNT(*) AS TTL_CNT
				,SUM(RRC_CONNECT_DURATION/1000) AS RRC_CONNECT_DURATION_SUM
				,COUNT(RRC_CONNECT_DURATION) AS RRC_CONNECT_DURATION_CNT
				,SUM(RRC_CONNECT_DURATION/3600) AS RRC_CONNECT_DURATION_SUM_CS
				,SUM(DL_TRAFFIC_VOLUME) AS DL_VOLUME_SUM
				,SUM(UL_TRAFFIC_VOLUME) AS UL_VOLUME_SUM
				,CASE WHEN (IFNULL(SUM(UL_TRAFFIC_VOLUME),0) + IFNULL(SUM(DL_TRAFFIC_VOLUME),0))=0 THEN NULL ELSE (IFNULL(SUM(UL_TRAFFIC_VOLUME),0) + IFNULL(SUM(DL_TRAFFIC_VOLUME),0)) END AS TRAFFIC_VOLUME_SUM
				,SUM(IF(CALL_STATUS=3, 1, 0)) AS CALL_STATUS_3_CNT
				,SUM(IF(CALL_STATUS=2, 1, 0)) AS CALL_STATUS_2_CNT
				,SUM(IF( CALL_STATUS IN (1,2,4) ,1,0)) AS CALL_STATUS_124_CNT
				,SUM(POS_FIRST_RSCP) AS POS_FIRST_RSCP_SUM
				,COUNT(POS_FIRST_RSCP) AS POS_FIRST_RSCP_CNT
				,SUM(POS_FIRST_ECN0) AS POS_FIRST_ECN0_SUM
				,COUNT(POS_FIRST_ECN0) AS POS_FIRST_ECN0_CNT
				,SUM(POS_AS1_RSCP) AS POS_AS1_RSCP_SUM
				,COUNT(POS_AS1_RSCP) AS POS_AS1_RSCP_CNT
				,SUM(POS_AS1_ECN0) AS POS_AS1_ECN0_SUM
				,COUNT(POS_AS1_ECN0) AS POS_AS1_ECN0_CNT
				,SUM(IF(SIMULATED = 0, POS_FIRST_RSCP, NULL)) AS POS_FIRST_RSCP_S_SUM
				,COUNT(IF(SIMULATED = 0, POS_FIRST_RSCP, NULL)) AS POS_FIRST_RSCP_S_CNT
				,SUM(IF(SIMULATED = 0, POS_FIRST_ECN0, NULL)) AS POS_FIRST_ECN0_S_SUM
				,COUNT(IF(SIMULATED = 0, POS_FIRST_ECN0, NULL)) AS POS_FIRST_ECN0_S_CNT
				,SUM(IF(RRC_REQUEST_TYPE IN (0,5),1,0)) AS CONVERSATIONALCALL_CNT
				,SUM(IF(RRC_REQUEST_TYPE IN (1,6),1,0)) AS STREAMINGCALL_CNT
				,SUM(IF(RRC_REQUEST_TYPE IN (2,7),1,0)) AS INTERACTIVECALL_CNT
				,SUM(IF(RRC_REQUEST_TYPE IN (3,8),1,0)) AS BACKGROUNDCALL_CNT
				,SUM(IF(RRC_REQUEST_TYPE IN (9),1,0)) AS EMERGENCYCALL_CNT
			FROM ',GT_DB,RUN,'.table_call
			WHERE DATA_HOUR >= ',STARTHOUR,' AND DATA_HOUR < ',ENDHOUR,'
			AND IMEI_NEW IS NOT NULL 
			AND POS_FIRST_RNC = ',RNC_ID,'
			GROUP BY 
					DATA_DATE
					, DATA_HOUR
					, POS_FIRST_RNC
					, MAKE_ID
					, MODEL_ID
					, CALL_TYPE 
					, CALL_STATUS
			ORDER BY NULL;');
        PREPARE Stmt FROM @SqlCmd;
        EXECUTE Stmt;
        DEALLOCATE PREPARE Stmt;                 
        
        
        INSERT INTO gt_gw_main.sp_log VALUES(O_GT_DB,'SP_Sub_Generate_TAC','Update the value of MAKE_MODEL, MAKE_ID, MODEL_ID in table_tac', NOW());
 
	SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS ',GT_DB,RUN,'.tmp_dim_handset;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;	
	
	SET @SqlCmd=CONCAT('CREATE TEMPORARY TABLE ',GT_DB,RUN,'.tmp_dim_handset AS 
		SELECT make_id,model_id,manufacturer,model FROM ',GT_COVMO,'.dim_handset 
		GROUP BY make_id,model_id;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
						
	SET @SqlCmd=CONCAT('CREATE INDEX IX_model_id ON ',GT_DB,RUN,'.tmp_dim_handset (`make_id`,`model_id`,manufacturer,model);');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
        
        SET @SqlCmd=CONCAT(' UPDATE ',GT_DB,RUN,'.table_tac A, 
					',GT_DB,RUN,'.tmp_dim_handset B
				SET A.MAKE_MODEL=CONCAT(B.manufacturer,''-'',B.model)
				WHERE A.MAKE_ID=B.MAKE_ID
					AND A.MODEL_ID=B.MODEL_ID
					AND DATA_HOUR >= ',STARTHOUR,' AND DATA_HOUR < ',ENDHOUR,';');
        PREPARE Stmt FROM @SqlCmd;
        EXECUTE Stmt;
        DEALLOCATE PREPARE Stmt; 
        
        SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS ',GT_DB,RUN,'.tmp_dim_handset;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;	
        
        INSERT INTO gt_gw_main.sp_log VALUES(O_GT_DB,'SP_Sub_Generate_TAC',CONCAT('Done: ',TIMESTAMPDIFF(SECOND,START_TIME,SYSDATE()),' seconds.'), NOW());
                        
END$$
DELIMITER ;
