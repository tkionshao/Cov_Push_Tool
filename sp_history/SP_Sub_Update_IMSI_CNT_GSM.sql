DELIMITER $$
USE `gt_gw_main`$$
DROP PROCEDURE IF EXISTS `SP_CreateDB_LTE`$$
CREATE DEFINER=`covmo`@`%` PROCEDURE `SP_Sub_Update_IMSI_CNT_GSM`(IN GT_DB VARCHAR(100), IN KIND VARCHAR(20), IN VENDOR_SOURCE VARCHAR(20), IN RTYPE CHAR(1))
BEGIN
	DECLARE BSC_ID INT;
	DECLARE O_GT_DB VARCHAR(100) DEFAULT GT_DB;
	DECLARE START_TIME DATETIME DEFAULT SYSDATE();
	DECLARE SH_EH VARCHAR(9) DEFAULT RIGHT(GT_DB,9);
	DECLARE STARTHOUR VARCHAR(2) DEFAULT SUBSTRING(RIGHT(GT_DB,18),10,2);
	DECLARE ENDHOUR VARCHAR(2) DEFAULT IF(SUBSTRING(RIGHT(GT_DB,18),15,2)='00','24',SUBSTRING(RIGHT(GT_DB,18),15,2));
	DECLARE RUN VARCHAR(20);
	DECLARE WORKER_ID VARCHAR(10) DEFAULT CONNECTION_ID();
	DECLARE STR_HR SMALLINT(6);
	DECLARE MAX_HR SMALLINT(6);
	DECLARE v_k SMALLINT(6);
	DECLARE v_k_Diff SMALLINT(6);
	DECLARE qry_tbl_name VARCHAR(50);
	DECLARE qry_tbl_name2 VARCHAR(50);
	DECLARE qry_tbl_name3 VARCHAR(50);
	DECLARE qry_tbl_name4 VARCHAR(50);
		
	SELECT gt_strtok(GT_DB,2,'_') INTO BSC_ID;
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
	INSERT INTO gt_gw_main.sp_log VALUES(O_GT_DB,'SP_Sub_Update_IMSI_CNT_GSM','Start', NOW());
	
	INSERT INTO gt_gw_main.sp_log VALUES(O_GT_DB,'SP_Sub_Generate_Start_GSM','Create table tmp_imsi_distinct gsm', NOW());
	SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE  IF EXISTS ',GT_DB,RUN,'.tmp_imsi_distinct_gsm_',WORKER_ID,';');
	PREPARE stmt FROM @sqlcmd;
	EXECUTE stmt;
	DEALLOCATE PREPARE stmt;
	
	IF RTYPE = 'h' THEN
		INSERT INTO sp_log VALUES(O_GT_DB,'SP_Sub_Generate_Start_GSM','hourly', NOW());
		
		SET @SqlCmd=CONCAT('CREATE TEMPORARY TABLE ',GT_DB,RUN,'.`tmp_imsi_distinct_gsm_',WORKER_ID,'` (
						`DATA_DATE` DATE DEFAULT NULL,
						`DATA_HOUR` TINYINT(4) DEFAULT NULL,
						`INDOOR` TINYINT(4) DEFAULT NULL,
						`MOVING` TINYINT(4) DEFAULT NULL,
						`BSC_ID` MEDIUMINT(9) DEFAULT NULL,
						`CELL_ID` MEDIUMINT(9) DEFAULT NULL,
						`CALL_TYPE` TINYINT(4) DEFAULT NULL,
						`CALL_STATUS` TINYINT(4) DEFAULT NULL,
						`IMSI` VARCHAR(20) DEFAULT NULL
					) ENGINE=MYISAM DEFAULT CHARSET=latin1;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		
		SET @SqlCmd=CONCAT('INSERT INTO  ',GT_DB,RUN,'.`tmp_imsi_distinct_gsm_',WORKER_ID,'`
					(`DATA_DATE`,`DATA_HOUR`,`INDOOR`,`MOVING`,`BSC_ID`,`CELL_ID`,`CALL_TYPE`,`CALL_STATUS`,`IMSI`
					 )
					SELECT
						 DATA_DATE
						, DATA_HOUR
						, INDOOR
						, MOVING
						, POS_FIRST_BSC_ID AS BSC_ID
						, POS_FIRST_CELL_ID AS CELL_ID
						, CALL_TYPE 
						, CALL_STATUS
						, IMSI
					FROM ',GT_DB,RUN,'.table_call_gsm
					WHERE POS_FIRST_BSC_ID =',BSC_ID,'
					AND DATA_HOUR >= ',STARTHOUR,' AND DATA_HOUR < ',ENDHOUR,'
					GROUP BY  
						DATA_DATE
						,DATA_HOUR
						,CALL_TYPE
						,CALL_STATUS
						,MOVING
						,INDOOR
						,POS_FIRST_BSC_ID
						,POS_FIRST_CELL_ID
						,IMSI');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;	
		INSERT INTO gt_gw_main.sp_log VALUES(O_GT_DB,'SP_Sub_Generate_Start_GSM','Update IMSI_CNT gsm', NOW());
		
		SET @SqlCmd=CONCAT(' UPDATE ',GT_DB,RUN,'.table_tile_start_gsm_c A,
		(
				    SELECT
						 DATA_DATE
						, DATA_HOUR
						, INDOOR
						, MOVING
						, BSC_ID
						, CELL_ID
						, CALL_TYPE 
						, CALL_STATUS
						, COUNT(DISTINCT IMSI) AS IMSI_CNT
					FROM ',GT_DB,RUN,'.`tmp_imsi_distinct_gsm_',WORKER_ID,'` 
					GROUP BY  DATA_DATE
						, DATA_HOUR
						, INDOOR
						, MOVING
						, BSC_ID
						, CELL_ID
						, CALL_TYPE 
						, CALL_STATUS
		) B
					SET 	A.IMSI_CNT=B.IMSI_CNT
					WHERE   A.DATA_DATE = B.DATA_DATE
						AND A.DATA_HOUR = B.DATA_HOUR
						AND A.INDOOR = B.INDOOR
						AND A.MOVING = B.MOVING
						AND A.BSC_ID = B.BSC_ID
						AND A.CELL_ID = B.CELL_ID
						AND A.CALL_TYPE  = B.CALL_TYPE
						AND A.CALL_STATUS = B.CALL_STATUS;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		SET @SqlCmd=CONCAT(' UPDATE ',GT_DB,RUN,'.table_tile_start_gsm_c_def A,
		(
				    SELECT
						 DATA_DATE
						, DATA_HOUR
						, BSC_ID
						, CELL_ID
						, COUNT(DISTINCT IMSI) AS IMSI_CNT
					FROM ',GT_DB,RUN,'.`tmp_imsi_distinct_gsm_',WORKER_ID,'` 
					GROUP BY  DATA_DATE
						, DATA_HOUR
						, BSC_ID
						, CELL_ID
		) B
					SET 	A.IMSI_CNT=B.IMSI_CNT
					WHERE   A.DATA_DATE = B.DATA_DATE
						AND A.DATA_HOUR = B.DATA_HOUR
						AND A.BSC_ID = B.BSC_ID
						AND A.CELL_ID = B.CELL_ID;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		SET @SqlCmd=CONCAT(' UPDATE ',GT_DB,'.table_tile_start_gsm_dy_c A,
		(
				    SELECT
						 DATA_DATE
						, INDOOR
						, MOVING
						, BSC_ID
						, CELL_ID
						, CALL_TYPE 
						, CALL_STATUS
						, COUNT(DISTINCT IMSI) AS IMSI_CNT
					FROM ',GT_DB,RUN,'.`tmp_imsi_distinct_gsm_',WORKER_ID,'` 
					GROUP BY  DATA_DATE
						, INDOOR
						, MOVING
						, BSC_ID
						, CELL_ID
						, CALL_TYPE 
						, CALL_STATUS
		) B
					SET 	A.IMSI_CNT = CASE WHEN IFNULL(A.IMSI_CNT,0) > B.IMSI_CNT THEN A.IMSI_CNT ELSE B.IMSI_CNT END
					WHERE   A.DATA_DATE = B.DATA_DATE
						AND A.INDOOR = B.INDOOR
						AND A.MOVING = B.MOVING
						AND A.BSC_ID = B.BSC_ID
						AND A.CELL_ID = B.CELL_ID
						AND A.CALL_TYPE  = B.CALL_TYPE
						AND A.CALL_STATUS = B.CALL_STATUS;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		SET @SqlCmd=CONCAT(' UPDATE ',GT_DB,'.table_tile_start_gsm_dy_c_def A,
		(
				    SELECT
						 DATA_DATE
						, BSC_ID
						, CELL_ID
						, COUNT(DISTINCT IMSI) AS IMSI_CNT
					FROM ',GT_DB,RUN,'.`tmp_imsi_distinct_gsm_',WORKER_ID,'` 
					GROUP BY  DATA_DATE
						, BSC_ID
						, CELL_ID
		) B
					SET 	A.IMSI_CNT = CASE WHEN IFNULL(A.IMSI_CNT,0) > B.IMSI_CNT THEN A.IMSI_CNT ELSE B.IMSI_CNT END
					WHERE   A.DATA_DATE = B.DATA_DATE
						AND A.BSC_ID = B.BSC_ID
						AND A.CELL_ID = B.CELL_ID');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
	
	ELSEIF RTYPE = 'd' THEN
		INSERT INTO sp_log VALUES(O_GT_DB,'SP_Sub_Generate_Start_AGR','daily', NOW());
	
		SET @SqlCmd=CONCAT('CREATE TEMPORARY TABLE ',GT_DB,RUN,'.`tmp_imsi_distinct_gsm_',WORKER_ID,'` (
						`DATA_DATE` DATE DEFAULT NULL,
						`INDOOR` TINYINT(4) DEFAULT NULL,
						`MOVING` TINYINT(4) DEFAULT NULL,
						`BSC_ID` MEDIUMINT(9) DEFAULT NULL,
						`CELL_ID` MEDIUMINT(9) DEFAULT NULL,
						`CALL_TYPE` TINYINT(4) DEFAULT NULL,
						`CALL_STATUS` TINYINT(4) DEFAULT NULL,
						`IMSI` VARCHAR(20) DEFAULT NULL
					) ENGINE=MYISAM DEFAULT CHARSET=latin1;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		
		SET @SqlCmd=CONCAT('INSERT INTO  ',GT_DB,RUN,'.`tmp_imsi_distinct_gsm_',WORKER_ID,'`
					(`DATA_DATE`,`INDOOR`,`MOVING`,`BSC_ID`,`CELL_ID`,`CALL_TYPE`,`CALL_STATUS`,`IMSI`
					 )
					SELECT
						 DATA_DATE
						, INDOOR
						, MOVING
						, POS_FIRST_BSC_ID AS BSC_ID
						, POS_FIRST_CELL_ID AS CELL_ID
						, CALL_TYPE 
						, CALL_STATUS
						, IMSI
					FROM ',GT_DB,RUN,'.table_call_gsm
					WHERE POS_FIRST_BSC_ID =',BSC_ID,'
					AND DATA_HOUR >= ',STARTHOUR,' AND DATA_HOUR < ',ENDHOUR,'
					GROUP BY  
						DATA_DATE
						,CALL_TYPE
						,CALL_STATUS
						,MOVING
						,INDOOR
						,POS_FIRST_BSC_ID
						,POS_FIRST_CELL_ID
						,IMSI');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;	
		INSERT INTO gt_gw_main.sp_log VALUES(O_GT_DB,'SP_Sub_Generate_Start_GSM','Update IMSI_CNT gsm', NOW());
		SET @SqlCmd=CONCAT(' UPDATE ',GT_DB,'.table_tile_start_gsm_dy_c A,
		(
				    SELECT
						 DATA_DATE
						, INDOOR
						, MOVING
						, BSC_ID
						, CELL_ID
						, CALL_TYPE 
						, CALL_STATUS
						, COUNT(DISTINCT IMSI) AS IMSI_CNT
					FROM ',GT_DB,RUN,'.`tmp_imsi_distinct_gsm_',WORKER_ID,'` 
					GROUP BY  DATA_DATE
						, INDOOR
						, MOVING
						, BSC_ID
						, CELL_ID
						, CALL_TYPE 
						, CALL_STATUS
		) B
					SET 	A.IMSI_CNT = B.IMSI_CNT
					WHERE   A.DATA_DATE = B.DATA_DATE
						AND A.INDOOR = B.INDOOR
						AND A.MOVING = B.MOVING
						AND A.BSC_ID = B.BSC_ID
						AND A.CELL_ID = B.CELL_ID
						AND A.CALL_TYPE  = B.CALL_TYPE
						AND A.CALL_STATUS = B.CALL_STATUS;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		SET @SqlCmd=CONCAT(' UPDATE ',GT_DB,'.table_tile_start_gsm_dy_c_def A,
		(
				    SELECT
						 DATA_DATE
						, BSC_ID
						, CELL_ID
						, COUNT(DISTINCT IMSI) AS IMSI_CNT
					FROM ',GT_DB,RUN,'.`tmp_imsi_distinct_gsm_',WORKER_ID,'` 
					GROUP BY  DATA_DATE
						, BSC_ID
						, CELL_ID
		) B
					SET 	A.IMSI_CNT = B.IMSI_CNT
					WHERE   A.DATA_DATE = B.DATA_DATE
						AND A.BSC_ID = B.BSC_ID
						AND A.CELL_ID = B.CELL_ID');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
	
		SET @SqlCmd=CONCAT(' UPDATE ',GT_DB,'.table_tile_start_gsm_dy A,
		(
				    SELECT
						 DATA_DATE
						, BSC_ID
						, CELL_ID
						, COUNT(DISTINCT IMSI) AS IMSI_CNT
					FROM ',GT_DB,RUN,'.`tmp_imsi_distinct_gsm_',WORKER_ID,'` 
					GROUP BY  DATA_DATE
						, BSC_ID
						, CELL_ID
		) B
					SET 	A.IMSI_CNT = B.IMSI_CNT
					WHERE   A.DATA_DATE = B.DATA_DATE
						AND A.BSC_ID = B.BSC_ID
						AND A.CELL_ID = B.CELL_ID');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
	
		SET @SqlCmd=CONCAT(' UPDATE ',GT_DB,'.table_tile_start_gsm_dy_def A,
		(
				    SELECT
						 DATA_DATE
						, BSC_ID
						, CELL_ID
						, COUNT(DISTINCT IMSI) AS IMSI_CNT
					FROM ',GT_DB,RUN,'.`tmp_imsi_distinct_gsm_',WORKER_ID,'` 
					GROUP BY  DATA_DATE
						, BSC_ID
						, CELL_ID
		) B
					SET 	A.IMSI_CNT = B.IMSI_CNT
					WHERE   A.DATA_DATE = B.DATA_DATE
						AND A.BSC_ID = B.BSC_ID
						AND A.CELL_ID = B.CELL_ID');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
	
		SET STR_HR=0;
		SET MAX_HR=24;
		BEGIN	
			SET v_k=STR_HR;
			SET v_k_Diff=1;
			WHILE v_k < MAX_HR DO
			BEGIN
				SET qry_tbl_name=CONCAT('table_tile_start_gsm','_',RIGHT(CONCAT(RIGHT(CONCAT('0',v_k),2)),2));
				SET qry_tbl_name2=CONCAT('table_tile_start_gsm_c','_',RIGHT(CONCAT(RIGHT(CONCAT('0',v_k),2)),2));
				SET qry_tbl_name3=CONCAT('table_tile_start_gsm_c_def','_',RIGHT(CONCAT(RIGHT(CONCAT('0',v_k),2)),2));
				SET qry_tbl_name4=CONCAT('table_tile_start_gsm_def','_',RIGHT(CONCAT(RIGHT(CONCAT('0',v_k),2)),2));
				
					SET @SqlCmd=CONCAT(' UPDATE ',GT_DB,'.',qry_tbl_name,' A,
					(
							    SELECT
									 DATA_DATE
									, BSC_ID
									, CELL_ID
									, COUNT(DISTINCT IMSI) AS IMSI_CNT
								FROM ',GT_DB,RUN,'.`tmp_imsi_distinct_gsm_',WORKER_ID,'` 
								GROUP BY  DATA_DATE
									, BSC_ID
									, CELL_ID
					) B
								SET 	A.IMSI_CNT = B.IMSI_CNT
								WHERE   A.DATA_DATE = B.DATA_DATE
									AND A.BSC_ID = B.BSC_ID
									AND A.CELL_ID = B.CELL_ID');
					PREPARE Stmt FROM @SqlCmd;
					EXECUTE Stmt;
					DEALLOCATE PREPARE Stmt;
	
					SET @SqlCmd=CONCAT(' UPDATE ',GT_DB,'.',qry_tbl_name2,' A,
					(
							    SELECT
									 DATA_DATE
									, BSC_ID
									, CELL_ID
									, COUNT(DISTINCT IMSI) AS IMSI_CNT
								FROM ',GT_DB,RUN,'.`tmp_imsi_distinct_gsm_',WORKER_ID,'` 
								GROUP BY  DATA_DATE
									, BSC_ID
									, CELL_ID
					) B
								SET 	A.IMSI_CNT = B.IMSI_CNT
								WHERE   A.DATA_DATE = B.DATA_DATE
									AND A.BSC_ID = B.BSC_ID
									AND A.CELL_ID = B.CELL_ID');
					PREPARE Stmt FROM @SqlCmd;
					EXECUTE Stmt;
					DEALLOCATE PREPARE Stmt;
					SET @SqlCmd=CONCAT(' UPDATE ',GT_DB,'.',qry_tbl_name3,' A,
					(
							    SELECT
									 DATA_DATE
									, BSC_ID
									, CELL_ID
									, COUNT(DISTINCT IMSI) AS IMSI_CNT
								FROM ',GT_DB,RUN,'.`tmp_imsi_distinct_gsm_',WORKER_ID,'` 
								GROUP BY  DATA_DATE
									, BSC_ID
									, CELL_ID
					) B
								SET 	A.IMSI_CNT = B.IMSI_CNT
								WHERE   A.DATA_DATE = B.DATA_DATE
									AND A.BSC_ID = B.BSC_ID
									AND A.CELL_ID = B.CELL_ID');
					PREPARE Stmt FROM @SqlCmd;
					EXECUTE Stmt;
					DEALLOCATE PREPARE Stmt;
					SET @SqlCmd=CONCAT(' UPDATE ',GT_DB,'.',qry_tbl_name4,' A,
					(
							    SELECT
									 DATA_DATE
									, BSC_ID
									, CELL_ID
									, COUNT(DISTINCT IMSI) AS IMSI_CNT
								FROM ',GT_DB,RUN,'.`tmp_imsi_distinct_gsm_',WORKER_ID,'` 
								GROUP BY  DATA_DATE
									, BSC_ID
									, CELL_ID
					) B
								SET 	A.IMSI_CNT = B.IMSI_CNT
								WHERE   A.DATA_DATE = B.DATA_DATE
									AND A.BSC_ID = B.BSC_ID
									AND A.CELL_ID = B.CELL_ID');
					PREPARE Stmt FROM @SqlCmd;
					EXECUTE Stmt;
					DEALLOCATE PREPARE Stmt;
	
								
					SET v_k=v_k+v_k_Diff;
				
			END;
			END WHILE;
		END;
	END IF;
	
	INSERT INTO gt_gw_main.sp_log VALUES(O_GT_DB,'SP_Sub_Update_IMSI_CNT_GSM',CONCAT(' Done: ',TIMESTAMPDIFF(SECOND,START_TIME,SYSDATE()),' seconds.'), NOW());
	
END$$
DELIMITER ;
