DELIMITER $$
USE `gt_gw_main`$$
DROP PROCEDURE IF EXISTS `SP_CreateDB_LTE`$$
CREATE DEFINER=`covmo`@`%` PROCEDURE `SP_NDC_IMSI_export_5mins`(IN GT_DB VARCHAR(100),IN PATH VARCHAR(100),IN TECH_MASK TINYINT(4),IN GT_COVMO VARCHAR(100))
a_label:
BEGIN
    DECLARE START_TIME DATETIME DEFAULT SYSDATE();
    DECLARE STEP_START_TIME DATETIME DEFAULT SYSDATE();
    DECLARE WORKER_ID VARCHAR(10) DEFAULT CONNECTION_ID();
    DECLARE RNC_ID INT;
    DECLARE DATA_DATE VARCHAR(8);
    DECLARE DATA_S_QRT VARCHAR(4);
    DECLARE DATA_E_QRT VARCHAR(4);
    DECLARE FOLDER_PATH VARCHAR(20);
    DECLARE D_GT_DB VARCHAR(100);
    DECLARE SH_EH VARCHAR(9) DEFAULT RIGHT(GT_DB,9);
    DECLARE SH VARCHAR(4) DEFAULT gt_strtok(SH_EH,1,'_');
     SELECT gt_strtok(GT_DB,2,'_') INTO RNC_ID;
     SELECT gt_strtok(GT_DB,3,'_') INTO DATA_DATE;
     SELECT gt_strtok(GT_DB,4,'_') INTO DATA_S_QRT;
     SELECT gt_strtok(GT_DB,5,'_') INTO DATA_E_QRT;
    SELECT REPLACE(GT_DB,SH_EH,'0000_0000') INTO D_GT_DB;
    SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS ',GT_DB,'.tmp_dim_handset;');
    PREPARE Stmt FROM @SqlCmd;
    EXECUTE Stmt;
    DEALLOCATE PREPARE Stmt;
    SET @SqlCmd=CONCAT('CREATE TEMPORARY TABLE ',GT_DB,'.tmp_dim_handset AS
        SELECT A.`make_id`,B.`model_id`,A.`manufacturer`,B.`model` FROM  `',GT_COVMO,'`.`dim_handset_id` A ,`',GT_COVMO,'`.`dim_handset_m_id` B
        WHERE A.`make_id`=B.`make_id`;');
    PREPARE Stmt FROM @SqlCmd;
    EXECUTE Stmt;
    DEALLOCATE PREPARE Stmt;
    SET @SqlCmd=CONCAT('CREATE INDEX idx_make ON ',GT_DB,'.tmp_dim_handset(`make_id`,`model_id`,`manufacturer`,`model`);');
    PREPARE Stmt FROM @SqlCmd;
    EXECUTE Stmt;
    DEALLOCATE PREPARE Stmt; 
    SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS ',GT_DB,'.tmp_tbl_imsi_',WORKER_ID,';');
    PREPARE Stmt FROM @SqlCmd;
    EXECUTE Stmt;
    DEALLOCATE PREPARE Stmt;
    SET @SqlCmd=CONCAT('CREATE TEMPORARY TABLE ',GT_DB,'.tmp_tbl_imsi_',WORKER_ID,'
                (
                IMSI VARCHAR(20) DEFAULT NULL,
                START_TIME DATETIME DEFAULT NULL,
                END_TIME DATETIME DEFAULT NULL,
                IMEI VARCHAR(20) DEFAULT NULL,
                MAKE_ID SMALLINT(6) DEFAULT NULL,
                MODEL_ID MEDIUMINT(9) DEFAULT NULL,
                CALL_TYPE TINYINT(4) DEFAULT NULL,
                CALL_STATUS TINYINT(4) DEFAULT NULL,
                MNC VARCHAR(3) DEFAULT NULL,
                MCC CHAR(3) DEFAULT NULL,
                APN VARCHAR(100) DEFAULT NULL,
                START_CELL_ID VARCHAR(50) DEFAULT NULL,
                START_RNC_ID VARCHAR(50) DEFAULT NULL,
                START_LATITUDE DOUBLE DEFAULT NULL,
                START_LONGITUDE DOUBLE DEFAULT NULL,
                POS_FIRST_RSCP DOUBLE DEFAULT NULL,
                POS_FIRST_ECN0 DOUBLE DEFAULT NULL,
                END_CELL_ID VARCHAR(50) DEFAULT NULL,
                END_RNC_ID VARCHAR(50) DEFAULT NULL,  
                END_LATITUDE DOUBLE DEFAULT NULL,
                END_LONGITUDE DOUBLE DEFAULT NULL,
                POS_LAST_RSCP DOUBLE DEFAULT NULL,    
                POS_LAST_ECN0 DOUBLE DEFAULT NULL,    
                UL_TRAFFIC_VOLUME DOUBLE DEFAULT NULL,
                UL_THROUGHPUT_MAX DOUBLE DEFAULT NULL,
                DL_TRAFFIC_VOLUME DOUBLE DEFAULT NULL,
                DL_THROUGHPUT_MAX DOUBLE DEFAULT NULL,
                TECH VARCHAR(5) DEFAULT NULL,  
                KEY `IX_MAKE` (`MAKE_ID`,`MODEL_ID`)
            ) ENGINE=MYISAM;');
    PREPARE Stmt FROM @SqlCmd;
    EXECUTE Stmt;
    DEALLOCATE PREPARE Stmt;
    IF TECH_MASK=2 THEN 
        SET @SqlCmd=CONCAT('INSERT INTO ',GT_DB,'.tmp_tbl_imsi_',WORKER_ID,'
                    (
                    IMSI,
                    START_TIME,
                    END_TIME,
                    IMEI,
                    MAKE_ID,
                    MODEL_ID,
                    CALL_TYPE,
                    CALL_STATUS,
                    MNC,
                    MCC,
                    APN,
                    START_CELL_ID,
                    START_RNC_ID,
                    START_LATITUDE,
                    START_LONGITUDE,
                    POS_FIRST_RSCP,
                    POS_FIRST_ECN0,
                    END_CELL_ID,
                    END_RNC_ID,  
                    END_LATITUDE,
                    END_LONGITUDE,
                    POS_LAST_RSCP,    
                    POS_LAST_ECN0,    
                    UL_TRAFFIC_VOLUME,
                    UL_THROUGHPUT_MAX,
                    DL_TRAFFIC_VOLUME,
                    DL_THROUGHPUT_MAX,
                    TECH)
                SELECT
                    IMSI,
                    START_TIME,
                    END_TIME,
                    IMEI,
                    MAKE_ID,
                    MODEL_ID,
                    CALL_TYPE,
                    CALL_STATUS,
                    MNC,
                    MCC,
                    `ACCESS_POINT_NAME` AS APN,
                    START_CELL_ID,
                    START_RNC_ID,
                    gt_covmo_proj_geohash_to_lat(POS_FIRST_LOC) AS START_LATITUDE,
                    gt_covmo_proj_geohash_to_lng(POS_FIRST_LOC) AS START_LONGITUDE,
                    POS_FIRST_RSCP,
                    POS_FIRST_ECN0,
                    END_CELL_ID,
                    END_RNC_ID,
                    gt_covmo_proj_geohash_to_lat(POS_LAST_LOC) AS END_LATITUDE,
                    gt_covmo_proj_geohash_to_lng(POS_LAST_LOC) AS END_LONGITUDE,
                    POS_LAST_RSCP,
                    POS_LAST_ECN0,
                    UL_TRAFFIC_VOLUME,
                    UL_THROUGHPUT_MAX,
                    DL_TRAFFIC_VOLUME,
                    DL_THROUGHPUT_MAX,
                    ''UMTS'' AS TECH
                FROM ',GT_DB,'.table_call #for 5 mins #15404
                #FROM ',D_GT_DB,'.table_call_',SH,'
                WHERE (IMSI REGEXP ''^[0-9]*$'')=1 AND IMSI IS NOT NULL AND (`POS_FIRST_LOC` IS NOT NULL OR `POS_LAST_LOC` IS NOT NULL) ;');
        PREPARE Stmt FROM @SqlCmd;
        EXECUTE Stmt;
        DEALLOCATE PREPARE Stmt;
         INSERT INTO gt_gw_main.SP_LOG VALUES(GT_DB,'SP_NDC_IMSI_export',CONCAT('SELECT IMSI FROM table_call_imsi_umts,umts:',GT_DB,'_',WORKER_ID), NOW());
        SET STEP_START_TIME := SYSDATE();
        SET @SqlCmd=CONCAT('
                     SELECT 
                        IMSI,
                        START_TIME,
                        END_TIME,
                        IMEI,
                        B.`MANUFACTURER` AS MAKER,
                        B.`MODEL` AS MODEL,
                        (CASE WHEN CALL_TYPE =10 THEN ''Voice'' WHEN CALL_TYPE =11 THEN ''Video''
                            WHEN CALL_TYPE=12 THEN ''PS R99'' WHEN CALL_TYPE=13 THEN ''PS HSPA'' 
                            WHEN CALL_TYPE=14 THEN ''Multi RAB'' WHEN CALL_TYPE=15 THEN ''Signaling'' 
                            WHEN CALL_TYPE=16 THEN ''SMS'' WHEN CALL_TYPE IN (18,19) THEN ''Unspecified'' 
                            ELSE ''Unspecified'' END) AS CALL_TYPE,
                        (CASE WHEN call_status=1 THEN ''Normal'' 
                            WHEN call_status=2 THEN ''DROP''
                            WHEN call_status=3 THEN ''Block'' 
                            WHEN call_status=6 THEN ''Setup failure'' 
                            ELSE ''Unspecified'' END) AS CALL_STATUS,
                        MNC,
                        MCC,
                        APN,
                        START_CELL_ID,
                        START_RNC_ID,
                        START_LATITUDE,
                        START_LONGITUDE,
                        POS_FIRST_RSCP,
                        POS_FIRST_ECN0,
                        END_CELL_ID,
                        END_RNC_ID,
                        END_LATITUDE,
                        END_LONGITUDE,
                        POS_LAST_RSCP,
                        POS_LAST_ECN0,
                        UL_TRAFFIC_VOLUME,
                        UL_THROUGHPUT_MAX,
                        DL_TRAFFIC_VOLUME,
                        DL_THROUGHPUT_MAX,
                        TECH
                    FROM ',GT_DB,'.tmp_tbl_imsi_',WORKER_ID,' A FORCE INDEX (IX_MAKE)
                    LEFT JOIN ',GT_DB,'.tmp_dim_handset B
                    ON A.MAKE_ID=B.MAKE_ID AND A.MODEL_ID=B.MODEL_ID
                    INTO OUTFILE ''',PATH,'/GT_',RNC_ID,'_',DATA_DATE,DATA_S_QRT,'_',DATA_DATE,DATA_E_QRT,'.CALL.csv''
                    FIELDS TERMINATED BY ''\t''
                    OPTIONALLY ENCLOSED BY ''''
                    LINES TERMINATED BY ''\n'';');
        PREPARE Stmt FROM @SqlCmd;
        EXECUTE Stmt;
        DEALLOCATE PREPARE Stmt;
         INSERT INTO gt_gw_main.SP_LOG VALUES(GT_DB,'SP_NDC_IMSI_export',CONCAT('EXPORT FROM tmp_tbl_imsi,umts:',GT_DB,'_',WORKER_ID), NOW());
        SET STEP_START_TIME := SYSDATE();
    ELSEIF TECH_MASK=4 THEN  
        SET @SqlCmd=CONCAT('INSERT INTO ',GT_DB,'.tmp_tbl_imsi_',WORKER_ID,'
                    (
                    IMSI,
                    START_TIME,
                    END_TIME,
                    IMEI,
                    MAKE_ID,
                    MODEL_ID,
                    CALL_TYPE,
                    CALL_STATUS,
                    MNC,
                    MCC,
                    APN,
                    START_CELL_ID,
                    START_RNC_ID,
                    START_LATITUDE,
                    START_LONGITUDE,
                    POS_FIRST_RSCP,
                    POS_FIRST_ECN0,
                    END_CELL_ID,
                    END_RNC_ID,  
                    END_LATITUDE,
                    END_LONGITUDE,
                    POS_LAST_RSCP,    
                    POS_LAST_ECN0,    
                    UL_TRAFFIC_VOLUME,
                    UL_THROUGHPUT_MAX,
                    DL_TRAFFIC_VOLUME,
                    DL_THROUGHPUT_MAX,
                    TECH)
                SELECT
                    IMSI,
                    START_TIME,
                    END_TIME,
                    IMEI,
                    MAKE_ID,
                    MODEL_ID,
                    CALL_TYPE,
                    CALL_STATUS,
                    MNC,
                    MCC,
                    APN,
                    CONCAT(START_ENODEB_ID,START_CELL_ID) AS START_CELL_ID,
                    ''',RNC_ID,''' AS START_ENODEB_ID,
                    gt_covmo_proj_geohash_to_lat(POS_FIRST_LOC) AS START_LATITUDE,
                    gt_covmo_proj_geohash_to_lng(POS_FIRST_LOC) AS START_LONGITUDE,
                    POS_FIRST_RSRP,
                    POS_FIRST_RSRQ,
                    CONCAT(END_ENODEB_ID,END_CELL_ID) AS END_CELL_ID,
                    ''',RNC_ID,''' AS END_ENODEB_ID,
                    gt_covmo_proj_geohash_to_lat(POS_LAST_LOC) AS END_LATITUDE,
                    gt_covmo_proj_geohash_to_lng(POS_LAST_LOC) AS END_LONGITUDE,
                    POS_LAST_RSRP,
                    POS_LAST_RSRQ,
                    UL_VOLUME,
                    UL_THROUPUT_MAX,
                    DL_VOLUME,
                    DL_THROUPUT_MAX,
                    ''LTE'' AS TECH
                FROM ',D_GT_DB,'.table_call_lte_',SH,'
                WHERE (IMSI REGEXP ''^[0-9]*$'')=1 AND IMSI IS NOT NULL AND (`POS_FIRST_LOC` IS NOT NULL OR `POS_LAST_LOC` IS NOT NULL) ;');
        PREPARE Stmt FROM @SqlCmd;
        EXECUTE Stmt;
        DEALLOCATE PREPARE Stmt;
         INSERT INTO gt_gw_main.SP_LOG VALUES(GT_DB,'SP_NDC_IMSI_export',CONCAT('SELECT IMSI FROM table_call_imsi_lte,lte:',GT_DB,'_',WORKER_ID), NOW());
        SET STEP_START_TIME := SYSDATE();
        SET @SqlCmd=CONCAT('
                    SELECT 
                        IMSI,
                        START_TIME,
                        END_TIME,
                        IMEI,
                        B.`MANUFACTURER` AS MAKER,
                        B.`MODEL` AS MODEL,                        
                        (CASE WHEN A.CALL_TYPE=21 THEN ''DATA'' 
                            WHEN A.CALL_TYPE=22 THEN ''Signaling'' 
                            WHEN A.CALL_TYPE=23 THEN ''VoLTE'' 
                            WHEN A.CALL_TYPE=24 THEN ''SMS'' 
                            ELSE ''Unspecified'' END) AS `CALL_TYPE`,
                        (CASE WHEN A.CALL_STATUS=1 THEN ''Normal'' 
                            WHEN A.CALL_STATUS=2 THEN ''DROP'' 
                            WHEN A.CALL_STATUS=3 THEN ''Block'' 
                            WHEN A.CALL_STATUS=5 THEN ''CS fallback'' 
                            WHEN A.CALL_STATUS=6 THEN ''Setup failure'' 
                            ELSE ''Unspecified'' END) AS `CALL_STATUS`,
                        MNC,
                        MCC,
                        APN,
                        START_CELL_ID,
                        START_RNC_ID,
                        START_LATITUDE,
                        START_LONGITUDE,
                        POS_FIRST_RSCP,
                        POS_FIRST_ECN0,
                        END_CELL_ID,
                        END_RNC_ID,
                        END_LATITUDE,
                        END_LONGITUDE,
                        POS_LAST_RSCP,
                        POS_LAST_ECN0,
                        UL_TRAFFIC_VOLUME,
                        UL_THROUGHPUT_MAX,
                        DL_TRAFFIC_VOLUME,
                        DL_THROUGHPUT_MAX,
                        TECH
                    FROM ',GT_DB,'.tmp_tbl_imsi_',WORKER_ID,' A FORCE INDEX (IX_MAKE)
                    LEFT JOIN ',GT_DB,'.tmp_dim_handset B
                    ON A.MAKE_ID=B.MAKE_ID AND A.MODEL_ID=B.MODEL_ID
                    INTO OUTFILE ''',PATH,'/GT_',RNC_ID,'_',DATA_DATE,DATA_S_QRT,'_',DATA_DATE,DATA_E_QRT,'.CALL.csv''
                    FIELDS TERMINATED BY ''\t''
                    OPTIONALLY ENCLOSED BY ''''
                    LINES TERMINATED BY ''\n'';');
        PREPARE Stmt FROM @SqlCmd;
        EXECUTE Stmt;
        DEALLOCATE PREPARE Stmt;
         INSERT INTO gt_gw_main.SP_LOG VALUES(GT_DB,'SP_NDC_IMSI_export',CONCAT('EXPORT FROM tmp_tbl_imsi,lte:',GT_DB,'_',WORKER_ID), NOW());
        SET STEP_START_TIME := SYSDATE();
    END IF;
    SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS ',GT_DB,'.tmp_tbl_imsi_',WORKER_ID,';');
    PREPARE Stmt FROM @SqlCmd;
    EXECUTE Stmt;
    DEALLOCATE PREPARE Stmt;
    SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS ',GT_DB,'.tmp_dim_handset;');
    PREPARE Stmt FROM @SqlCmd;
    EXECUTE Stmt;
    DEALLOCATE PREPARE Stmt;
    INSERT INTO gt_gw_main.sp_log VALUES('gt_gw_main','SP_NDC_IMSI_export',CONCAT(CONNECTION_ID(),' Done: ',TIMESTAMPDIFF(SECOND,START_TIME,SYSDATE()),' seconds.'), NOW());
END$$
DELIMITER ;
