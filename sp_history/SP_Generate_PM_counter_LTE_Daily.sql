DELIMITER $$
USE `gt_gw_main`$$
DROP PROCEDURE IF EXISTS `SP_CreateDB_LTE`$$
CREATE DEFINER=`covmo`@`%` PROCEDURE `SP_Generate_PM_counter_LTE_Daily`(IN GT_DB VARCHAR(50))
a_label:
BEGIN
	DECLARE START_TIME DATETIME DEFAULT SYSDATE();
	DECLARE V_NT_DATE DATE DEFAULT DATE(NOW());	
	DECLARE v_cnt INT;
	DECLARE STARTHOUR VARCHAR(2) DEFAULT SUBSTRING(RIGHT(GT_DB,18),10,2);
	DECLARE ENDHOUR VARCHAR(2) DEFAULT IF(SUBSTRING(RIGHT(GT_DB,18),15,2)='00','24',SUBSTRING(RIGHT(GT_DB,18),15,2));
	DECLARE SH_EH VARCHAR(9) DEFAULT RIGHT(GT_DB,9);
	DECLARE WORKER_ID VARCHAR(10) DEFAULT CONNECTION_ID();
	DECLARE PARTITION_ID INT DEFAULT SUBSTRING(RIGHT(GT_DB,18),10,2);
	DECLARE FILEDATE VARCHAR(10) DEFAULT gt_strtok(GT_DB,3,'_');
	DECLARE SESSION_DATE VARCHAR(18) DEFAULT CONCAT(LEFT(FILEDATE,4),'-',SUBSTRING(FILEDATE,5,2),'-',SUBSTRING(FILEDATE,7,2));
	
	SELECT REPLACE(GT_DB,SH_EH,'0000_0000') INTO GT_DB;
  	INSERT INTO gt_gw_main.SP_LOG VALUES(GT_DB,'SP_Generate_PM_counter_LTE_Daily','Start', START_TIME);	
  	
	SET @SqlCmd=CONCAT('ALTER TABLE ',GT_DB,'.table_pm_counter_lte TRUNCATE PARTITION h',PARTITION_ID,';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
 
	INSERT INTO gt_gw_main.SP_LOG VALUES(GT_DB,'SP_Generate_PM_counter_LTE_Daily','pm1_1', NOW());
 
 	SET @SqlCmd=CONCAT('Drop TEMPORARY table if exists ',GT_DB,'.tmp_join_pm1_1_',WORKER_ID,';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	SET @SqlCmd=CONCAT('CREATE TEMPORARY TABLE ',GT_DB,'.tmp_join_pm1_1_',WORKER_ID,' ENGINE=MYISAM AS 
				SELECT 
					CONCAT(LEFT(LEFT(a.`mts`,10),4),"-",SUBSTRING(LEFT(a.`mts`,10),5,2),"-",SUBSTRING(LEFT(a.`mts`,10),7,2)) AS DATA_DATE
					,CAST(RIGHT(LEFT(a.`mts`,10),2) AS SIGNED) AS DATA_HOUR
					,LEFT(REPLACE(gt_strtok(A.`moid`,3,","),"EUtranCellFDD=",""),6) AS ENODEB_ID
					,RIGHT(REPLACE(gt_strtok(A.`moid`,3,","),"EUtranCellFDD=",""),1) AS CELL_ID
					,a.neun AS neun
					,a.nedn AS nedn
					,a.nesw AS nesw
					,a.gp AS gp
					,a.mts AS mts
					,a.moid AS moid
					,pmRrcConnEstabSucc
					,pmRrcConnEstabAtt
					,pmS1SigConnEstabSucc
					,pmS1SigConnEstabAtt
					,pmErabEstabSuccInit
					,pmErabEstabAttInit
					,pmErabEstabSuccAdded
					,pmErabEstabAttAdded
					,pmErabRelAbnormalEnbAct
					,pmErabRelNormalEnbAct
				FROM  ',GT_DB,'.table_pmuethptimedl_lte a
				WHERE CAST(RIGHT(LEFT(a.`mts`,10),2) AS SIGNED) >= ',STARTHOUR,' AND CAST(RIGHT(LEFT(a.`mts`,10),2) AS SIGNED) < ',ENDHOUR,'
					AND CONCAT(LEFT(LEFT(mts,10),4),''-'',SUBSTRING(LEFT(mts,10),5,2),''-'',SUBSTRING(LEFT(mts,10),7,2)) = ''',SESSION_DATE,'''
				;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('ALTER TABLE ',GT_DB,'.tmp_join_pm1_1_',WORKER_ID,' ADD INDEX `ix_key`(neun,nedn,nesw,gp,mts,moid);');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	INSERT INTO gt_gw_main.SP_LOG VALUES(GT_DB,'SP_Generate_PM_counter_LTE_Daily','pm1_2', NOW());
	
	SET @SqlCmd=CONCAT('Drop TEMPORARY table if exists ',GT_DB,'.tmp_join_pm1_2_',WORKER_ID,';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	SET @SqlCmd=CONCAT('CREATE TEMPORARY TABLE ',GT_DB,'.tmp_join_pm1_2_',WORKER_ID,' ENGINE=MYISAM AS 
				SELECT 
					neun AS neun
					,nedn AS nedn
					,nesw AS nesw
					,gp AS gp
					,mts AS mts
					,moid AS moid
					,pmErabRelAbnormalEnb
					,pmErabRelNormalEnb
				FROM  ',GT_DB,'.table_pmactivedrbdlsum_lte 
				WHERE CAST(RIGHT(LEFT(`mts`,10),2) AS SIGNED) >= ',STARTHOUR,' AND CAST(RIGHT(LEFT(`mts`,10),2) AS SIGNED) < ',ENDHOUR,'
					AND CONCAT(LEFT(LEFT(mts,10),4),''-'',SUBSTRING(LEFT(mts,10),5,2),''-'',SUBSTRING(LEFT(mts,10),7,2)) = ''',SESSION_DATE,'''
				;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('ALTER TABLE ',GT_DB,'.tmp_join_pm1_2_',WORKER_ID,' ADD INDEX `ix_key`(neun,nedn,nesw,gp,mts,moid);');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	INSERT INTO gt_gw_main.SP_LOG VALUES(GT_DB,'SP_Generate_PM_counter_LTE_Daily','pm1', NOW());
	SET @SqlCmd=CONCAT(' Drop TEMPORARY table if exists ',GT_DB,'.tmp_pm_1_',WORKER_ID,';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('CREATE TEMPORARY TABLE ',GT_DB,'.tmp_pm_1_',WORKER_ID,' ENGINE=MYISAM AS 
				SELECT DATA_DATE,
					DATA_HOUR,
					ENODEB_ID,
					CELL_ID,
					SUM(pmRrcConnEstabSucc) AS NoRrcConnectReqSuccess,
					SUM(pmRrcConnEstabAtt) AS NoRrcConnectAtt,
					SUM(pmS1SigConnEstabSucc) AS NoS1SigConnEstabSucc,
					SUM(pmS1SigConnEstabAtt) AS NoS1SigConnEstabAtt,
					SUM(pmErabEstabSuccInit) AS NoErabEstabSuccInit,
					SUM(pmErabEstabAttInit) AS NoErabEstabAttInit,
					SUM(pmErabEstabSuccAdded) AS NoErabEstabSuccAdded,
					SUM(pmErabEstabAttAdded) AS NoErabEstabAttAdded,
					SUM(pmErabRelAbnormalEnb) AS NoErabRelAbnormalEnb,
					SUM(pmErabRelNormalEnb) AS NoErabRelNormalEnb,
					SUM(pmErabRelAbnormalEnbAct) AS NoErabRelAbnormalEnbAct,
					SUM(pmErabRelNormalEnbAct) AS NoErabRelNormalEnbAct,
					SUM(pmRrcConnEstabSucc) AS NoRrcConnEstabSucc,
					SUM(pmRrcConnEstabAtt) AS NoRrcConnEstabAtt,
					SUM(pmErabRelAbnormalEnb + pmErabRelNormalEnb) AS NoErabRelAbnormal
				FROM  ',GT_DB,'.tmp_join_pm1_1_',WORKER_ID,' a  
				JOIN ',GT_DB,'.tmp_join_pm1_2_',WORKER_ID,' b
				ON  a.neun =b.neun
				AND a.nedn =b.nedn
				AND a.nesw =b.nesw
				AND a.mts =b.mts
				AND a.gp =b.gp
				AND a.moid =b.moid
				GROUP BY DATA_DATE,DATA_HOUR,ENODEB_ID,CELL_ID
				;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('ALTER TABLE ',GT_DB,'.tmp_pm_1_',WORKER_ID,' ADD INDEX `key_moid`(data_date,data_hour,enodeb_id,cell_id);');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	INSERT INTO gt_gw_main.SP_LOG VALUES(GT_DB,'SP_Generate_PM_counter_LTE_Daily','pm2_1', NOW());
	
	SET @SqlCmd=CONCAT('Drop TEMPORARY table if exists ',GT_DB,'.tmp_join_pm2_1_',WORKER_ID,';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	SET @SqlCmd=CONCAT('CREATE TEMPORARY TABLE ',GT_DB,'.tmp_join_pm2_1_',WORKER_ID,' ENGINE=MYISAM AS 
				SELECT 
					CONCAT(LEFT(LEFT(B.`mts`,10),4),"-",SUBSTRING(LEFT(B.`mts`,10),5,2),"-",SUBSTRING(LEFT(B.`mts`,10),7,2)) AS DATA_DATE
					,CAST(RIGHT(LEFT(B.`mts`,10),2) AS SIGNED) AS DATA_HOUR
					,LEFT(REPLACE(gt_strtok(b.`moid`,3,","),"EUtranCellFDD=",""),6) AS ENODEB_ID
					,RIGHT(REPLACE(gt_strtok(b.`moid`,3,","),"EUtranCellFDD=",""),1) AS CELL_ID
					,b.pmHoExeSuccWcdma AS pmHoExeSuccWcdma
					,b.pmHoExeAttWcdma AS pmHoExeAttWcdma
				FROM  ',GT_DB,'.table_pmhoexeattwcdma_lte b
				WHERE CAST(RIGHT(LEFT(b.`mts`,10),2) AS SIGNED) >= ',STARTHOUR,' AND CAST(RIGHT(LEFT(b.`mts`,10),2) AS SIGNED) < ',ENDHOUR,'
					AND CONCAT(LEFT(LEFT(mts,10),4),''-'',SUBSTRING(LEFT(mts,10),5,2),''-'',SUBSTRING(LEFT(mts,10),7,2)) = ''',SESSION_DATE,'''
				;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;	
	
	SET @SqlCmd=CONCAT('ALTER TABLE ',GT_DB,'.tmp_join_pm2_1_',WORKER_ID,' ADD INDEX `ENODEB_ID`(DATA_DATE,DATA_HOUR,ENODEB_ID,CELL_ID);');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	INSERT INTO gt_gw_main.SP_LOG VALUES(GT_DB,'SP_Generate_PM_counter_LTE_Daily','pm2_2', NOW());
	
	SET @SqlCmd=CONCAT('Drop TEMPORARY table if exists ',GT_DB,'.tmp_join_pm2_2_',WORKER_ID,';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	SET @SqlCmd=CONCAT('CREATE TEMPORARY TABLE ',GT_DB,'.tmp_join_pm2_2_',WORKER_ID,' ENGINE=MYISAM AS 
				SELECT 
					CONCAT(LEFT(LEFT(a.`mts`,10),4),"-",SUBSTRING(LEFT(a.`mts`,10),5,2),"-",SUBSTRING(LEFT(a.`mts`,10),7,2)) AS DATA_DATE
					,CAST(RIGHT(LEFT(a.`mts`,10),2) AS SIGNED) AS DATA_HOUR
					,LEFT(REPLACE(gt_strtok(A.`moid`,3,","),"EUtranCellFDD=",""),6) AS ENODEB_ID
					,RIGHT(REPLACE(gt_strtok(A.`moid`,3,","),"EUtranCellFDD=",""),1) AS CELL_ID
					,a.pmHoExeSuccLteIntraF AS pmHoExeSuccLteIntraF
					,a.pmHoExeSuccLteInterF AS pmHoExeSuccLteInterF
					,a.pmHoExeAttLteIntraF AS pmHoExeAttLteIntraF
					,a.pmHoExeAttLteInterF AS pmHoExeAttLteInterF
				FROM  ',GT_DB,'.table_pmhoprepsucclteintraf_lte a
				WHERE CAST(RIGHT(LEFT(a.`mts`,10),2) AS SIGNED) >= ',STARTHOUR,' AND CAST(RIGHT(LEFT(a.`mts`,10),2) AS SIGNED) < ',ENDHOUR,'
					AND CONCAT(LEFT(LEFT(mts,10),4),''-'',SUBSTRING(LEFT(mts,10),5,2),''-'',SUBSTRING(LEFT(mts,10),7,2)) = ''',SESSION_DATE,'''
				;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('ALTER TABLE ',GT_DB,'.tmp_join_pm2_2_',WORKER_ID,' ADD INDEX `ENODEB_ID`(DATA_DATE,DATA_HOUR,ENODEB_ID,CELL_ID);');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	INSERT INTO gt_gw_main.SP_LOG VALUES(GT_DB,'SP_Generate_PM_counter_LTE_Daily','pm2', NOW());
	
	SET @SqlCmd=CONCAT('Drop TEMPORARY table if exists ',GT_DB,'.tmp_pm_2_',WORKER_ID,';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('
				CREATE TEMPORARY TABLE  ',GT_DB,'.tmp_pm_2_',WORKER_ID,' ENGINE=MYISAM AS 
				SELECT A.DATA_DATE,
					A.DATA_HOUR,
					A.ENODEB_ID,
					A.CELL_ID,
					SUM(a.pmHoExeSuccLteIntraF+a.pmHoExeSuccLteInterF+IFNULL(b.pmHoExeSuccWcdma,0)) NoHoExeSucc,
					SUM(a.pmHoExeAttLteIntraF+a.pmHoExeAttLteInterF+IFNULL(b.pmHoExeAttWcdma,0)) NoHoExeAtt,
					SUM(a.pmHoExeSuccLteIntraF) NoHoExeSuccLteIntraF,
					SUM(a.pmHoExeAttLteIntraF) NoHoExeAttLteIntraF,
					SUM(a.pmHoExeSuccLteInterF) NoHoExeSuccLteInterF,
					SUM(a.pmHoExeAttLteInterF) NoHoExeAttLteInterF,
					SUM(IFNULL(b.pmHoExeSuccWcdma,0)) NoHoExeSuccWcdma,
					SUM(IFNULL(b.pmHoExeAttWcdma,0)) NoHoExeAttWcdma
				FROM ',GT_DB,'.tmp_join_pm2_2_',WORKER_ID,' a
				LEFT JOIN ',GT_DB,'.tmp_join_pm2_1_',WORKER_ID,' b
				ON A.DATA_DATE=B.DATA_DATE AND A.DATA_HOUR=B.DATA_HOUR AND A.ENODEB_ID=B.ENODEB_ID AND A.CELL_ID=B.CELL_ID
				GROUP BY A.DATA_DATE,A.DATA_HOUR,A.ENODEB_ID,A.CELL_ID;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt; 
	
	SET @SqlCmd=CONCAT('ALTER TABLE ',GT_DB,'.tmp_pm_2_',WORKER_ID,' ADD INDEX `key_moid`(DATA_DATE,DATA_HOUR,ENODEB_ID,CELL_ID);');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	SET @SqlCmd=CONCAT('
				INSERT INTO ',GT_DB,'.`table_pm_counter_lte`
				    (`DATA_DATE`,
				     `DATA_HOUR`,
				     `ENODEB_ID`,
				     `CELL_ID`,
				     `NoRrcConnectReqSuccess`,
				     `NoRrcConnectAtt`,
				     `NoS1SigConnEstabSucc`,
				     `NoS1SigConnEstabAtt`,
				     `NoErabEstabSuccInit`,
				     `NoErabEstabAttInit`,
				     `NoErabEstabSuccAdded`,
				     `NoErabEstabAttAdded`,
				     `NoErabRelAbnormalEnb`,
				     `NoErabRelNormalEnb`,
				     `NoErabRelAbnormalEnbAct`,
				     `NoErabRelNormalEnbAct`,
				     `NoRrcConnEstabSucc`,
				     `NoRrcConnEstabAtt`,
				     `NoErabRelAbnormal`,
				     `NoHoExeSucc`,
				     `NoHoExeAtt`,
				     `NoHoExeSuccLteIntraF`,
				     `NoHoExeAttLteIntraF`,
				     `NoHoExeSuccLteInterF`,
				     `NoHoExeAttLteInterF`,
				     `NoHoExeSuccWcdma`,
				     `NoHoExeAttWcdma`)
				SELECT
				  a.`data_date`,
				  a.`data_hour`,
				  a.`enodeb_id`,
				  a.`CELL_ID`,
				  `NoRrcConnectReqSuccess`,
				  `NoRrcConnectAtt`,
				  `NoS1SigConnEstabSucc`,
				  `NoS1SigConnEstabAtt`,
				  `NoErabEstabSuccInit`,
				  `NoErabEstabAttInit`,
				  `NoErabEstabSuccAdded`,
				  `NoErabEstabAttAdded`,
				  `NoErabRelAbnormalEnb`,
				  `NoErabRelNormalEnb`,
				  `NoErabRelAbnormalEnbAct`,
				  `NoErabRelNormalEnbAct`,
				  `NoRrcConnEstabSucc`,
				  `NoRrcConnEstabAtt`,
				  `NoErabRelAbnormal`,
				  `NoHoExeSucc`,
				  `NoHoExeAtt`,
				  `NoHoExeSuccLteIntraF`,
				  `NoHoExeAttLteIntraF`,
				  `NoHoExeSuccLteInterF`,
				  `NoHoExeAttLteInterF`,
				  `NoHoExeSuccWcdma`,
				  `NoHoExeAttWcdma`
				FROM ',GT_DB,'.`tmp_pm_1_',WORKER_ID,'` a
				JOIN ',GT_DB,'.`tmp_pm_2_',WORKER_ID,'` b
				ON a.`data_date`=b.`data_date` AND a.`data_hour`=b.`data_hour` AND
				  a.`enodeb_id`=b.`enodeb_id` AND  
				  a.`CELL_ID`=b.`CELL_ID`;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt; 
		
	SET @SqlCmd=CONCAT(' Drop TEMPORARY table if exists ',GT_DB,'.tmp_pm_1_',WORKER_ID,';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT(' Drop TEMPORARY table if exists ',GT_DB,'.tmp_pm_2_',WORKER_ID,';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
		
	INSERT INTO gt_gw_main.SP_LOG VALUES(GT_DB,'SP_Generate_PM_counter_LTE_Daily',CONCAT('Done: ',timestampDIFF(SECOND,START_TIME,SYSDATE()),' seconds.'), NOW());
	
END$$
DELIMITER ;
