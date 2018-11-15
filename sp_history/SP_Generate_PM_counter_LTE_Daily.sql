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
	DECLARE CURRENT_NT_DB VARCHAR(50) DEFAULT CONCAT('gt_nt_',gt_strtok(GT_DB,3,'_'));
	
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
				
					,pmErabRelAbnormalMmeAct
					,pmCellDowntimeAuto
					,pmCellDowntimeMan					
					,pmPdcpLatPktTransDl
					,pmPdcpLatTimeDl
					,pmPdcpVolDlDrb
					,pmSessionTimeUe
					,pmUeThpTimeDl
					,pmUeThpTimeUl
					,pmUeThpVolUl
					,pmPdcpVolDlDrbLastTTI
					,pmRrcConnEstabAttReatt
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
					CONCAT(LEFT(LEFT(`mts`,10),4),"-",SUBSTRING(LEFT(`mts`,10),5,2),"-",SUBSTRING(LEFT(`mts`,10),7,2)) AS DATA_DATE
					,CAST(RIGHT(LEFT(`mts`,10),2) AS SIGNED) AS DATA_HOUR
					,LEFT(REPLACE(gt_strtok(`moid`,3,","),"EUtranCellFDD=",""),6) AS ENODEB_ID
					,RIGHT(REPLACE(gt_strtok(`moid`,3,","),"EUtranCellFDD=",""),1) AS CELL_ID
					
					,neun AS neun
					,nedn AS nedn
					,nesw AS nesw
					,gp AS gp
					,mts AS mts
					,moid AS moid
					,pmErabRelAbnormalEnb
					,pmErabRelNormalEnb
					,pmErabEstabAttAddedQci
					,pmErabRelMme
					,pmUeCtxtRelNormalEnb
					,pmErabEstabAttInitQci
					,pmErabEstabSuccAddedQci
					,pmErabEstabSuccInitQci
					,pmErabRelAbnormalEnbQci
					,pmErabRelMmeQci
					,pmErabRelNormalEnbQci
					,pmUeCtxtRelMme
					,pmUeCtxtRelSCWcdma
					
					,pmUeCtxtRelAbnormalEnb
					,pmErabRelAbnormalEnbActQci
					,pmErabRelAbnormalMmeActQci
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
				SELECT B.DATA_DATE,
					B.DATA_HOUR,
					B.ENODEB_ID,
					B.CELL_ID,
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
					SUM(pmErabRelAbnormalEnb + pmErabRelNormalEnb) AS NoErabRelAbnormal,
					SUM(IFNULL(a.pmRrcConnEstabSucc,0)) AS pmRrcConnEstabSucc,
					SUM(IFNULL(a.pmRrcConnEstabAtt,0)) AS pmRrcConnEstabAtt,
					SUM(IFNULL(a.pmS1SigConnEstabSucc,0)) AS pmS1SigConnEstabSucc,
					SUM(IFNULL(a.pmS1SigConnEstabAtt,0)) AS pmS1SigConnEstabAtt,
					SUM(IFNULL(a.pmErabEstabSuccInit,0)) AS pmErabEstabSuccInit,
					SUM(IFNULL(a.pmErabEstabAttInit,0)) AS pmErabEstabAttInit,
					SUM(IFNULL(a.pmErabEstabSuccAdded,0)) AS pmErabEstabSuccAdded,
					SUM(IFNULL(a.pmErabEstabAttAdded,0)) AS pmErabEstabAttAdded,
					SUM(IFNULL(a.pmErabRelAbnormalEnbAct,0)) AS pmErabRelAbnormalEnbAct,
					SUM(IFNULL(a.pmErabRelNormalEnbAct,0)) AS pmErabRelNormalEnbAct,
				
					SUM(IFNULL(a.pmErabRelAbnormalMmeAct,0)) AS pmErabRelAbnormalMmeAct,
					SUM(IFNULL(a.pmCellDowntimeAuto,0)) AS pmCellDowntimeAuto,
					SUM(IFNULL(a.pmCellDowntimeMan,0)) AS pmCellDowntimeMan,					
					SUM(IFNULL(a.pmPdcpLatPktTransDl,0)) AS pmPdcpLatPktTransDl,
					SUM(IFNULL(a.pmPdcpLatTimeDl,0)) AS pmPdcpLatTimeDl,
					SUM(IFNULL(a.pmPdcpVolDlDrb,0)) AS pmPdcpVolDlDrb,
					SUM(IFNULL(a.pmSessionTimeUe,0)) AS pmSessionTimeUe,
					SUM(IFNULL(a.pmUeThpTimeDl,0)) AS pmUeThpTimeDl,
					SUM(IFNULL(a.pmUeThpTimeUl,0)) AS pmUeThpTimeUl,
					SUM(IFNULL(a.pmUeThpVolUl,0)) AS pmUeThpVolUl,
					SUM(IFNULL(a.pmPdcpVolDlDrbLastTTI,0)) AS pmPdcpVolDlDrbLastTTI,
					SUM(IFNULL(a.pmRrcConnEstabAttReatt,0)) AS pmRrcConnEstabAttReatt,
	
					SUM(IFNULL(b.pmErabRelAbnormalEnb,0)) AS pmErabRelAbnormalEnb,
					SUM(IFNULL(b.pmErabRelNormalEnb,0)) AS pmErabRelNormalEnb,
					SUM(IFNULL(b.pmErabEstabAttAddedQci,0)) AS pmErabEstabAttAddedQci,
					SUM(IFNULL(b.pmErabRelMme,0)) AS pmErabRelMme,
					SUM(IFNULL(b.pmUeCtxtRelNormalEnb,0)) AS pmUeCtxtRelNormalEnb,
					SUM(IFNULL(b.pmErabEstabAttInitQci,0)) AS pmErabEstabAttInitQci,
					SUM(IFNULL(b.pmErabEstabSuccAddedQci,0)) AS pmErabEstabSuccAddedQci,
					SUM(IFNULL(b.pmErabEstabSuccInitQci,0)) AS pmErabEstabSuccInitQci,
					SUM(IFNULL(b.pmErabRelAbnormalEnbQci,0)) AS pmErabRelAbnormalEnbQci,
					SUM(IFNULL(b.pmErabRelMmeQci,0)) AS pmErabRelMmeQci,
					SUM(IFNULL(b.pmErabRelNormalEnbQci,0)) AS pmErabRelNormalEnbQci,
					SUM(IFNULL(b.pmUeCtxtRelMme,0)) AS pmUeCtxtRelMme,
					SUM(IFNULL(b.pmUeCtxtRelSCWcdma,0)) AS pmUeCtxtRelSCWcdma,
	
					SUM(IFNULL(b.pmUeCtxtRelAbnormalEnb,0)) AS pmUeCtxtRelAbnormalEnb,
					SUM(IFNULL(b.pmErabRelAbnormalEnbActQci,0)) AS pmErabRelAbnormalEnbActQci,
					SUM(IFNULL(b.pmErabRelAbnormalMmeActQci,0)) AS pmErabRelAbnormalMmeActQci
				FROM  ',GT_DB,'.tmp_join_pm1_1_',WORKER_ID,' a  
				RIGHT JOIN ',GT_DB,'.tmp_join_pm1_2_',WORKER_ID,' b
				ON  A.DATA_DATE=B.DATA_DATE AND A.DATA_HOUR=B.DATA_HOUR AND A.ENODEB_ID=B.ENODEB_ID AND A.CELL_ID=B.CELL_ID
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
					,a.pmHoPrepAttLteIntraF AS pmHoPrepAttLteIntraF
					,a.pmHoPrepSuccLteIntraF AS pmHoPrepSuccLteIntraF
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
					SUM(IFNULL(b.pmHoExeAttWcdma,0)) NoHoExeAttWcdma,
					SUM(IFNULL(a.pmHoExeSuccLteIntraF,0)) AS pmHoExeSuccLteIntraF,
					SUM(IFNULL(a.pmHoExeSuccLteInterF,0)) AS pmHoExeSuccLteInterF,
					SUM(IFNULL(a.pmHoExeAttLteIntraF,0)) AS pmHoExeAttLteIntraF,
					SUM(IFNULL(a.pmHoExeAttLteInterF,0)) AS pmHoExeAttLteInterF,
					SUM(IFNULL(a.pmHoPrepAttLteIntraF,0)) AS pmHoPrepAttLteIntraF,
					SUM(IFNULL(a.pmHoPrepSuccLteIntraF,0)) AS pmHoPrepSuccLteIntraF
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
				     `NoHoExeAttWcdma`,
					pmRrcConnEstabSucc,
					pmRrcConnEstabAtt,
					pmS1SigConnEstabSucc,
					pmS1SigConnEstabAtt,
					pmErabEstabSuccInit,
					pmErabEstabAttInit,
					pmErabEstabSuccAdded,
					pmErabEstabAttAdded,
					pmErabRelAbnormalEnbAct,
					pmErabRelNormalEnbAct,
				
					pmErabRelAbnormalMmeAct,
					pmCellDowntimeAuto,
					pmCellDowntimeMan,					
					pmPdcpLatPktTransDl,
					pmPdcpLatTimeDl,
					pmPdcpVolDlDrb,
					pmSessionTimeUe,
					pmUeThpTimeDl,
					pmUeThpTimeUl,
					pmUeThpVolUl,
					pmPdcpVolDlDrbLastTTI,
					pmRrcConnEstabAttReatt,
	
					pmErabRelAbnormalEnb,
					pmErabRelNormalEnb,
					pmErabEstabAttAddedQci,
					pmErabRelMme,
					pmUeCtxtRelNormalEnb,
					pmErabEstabAttInitQci,
					pmErabEstabSuccAddedQci,
					pmErabEstabSuccInitQci,
					pmErabRelAbnormalEnbQci,
					pmErabRelMmeQci,
					pmErabRelNormalEnbQci,
					pmUeCtxtRelMme,
					pmUeCtxtRelSCWcdma,
					pmHoExeSuccLteIntraF,
					pmHoExeAttLteIntraF,				
					pmHoPrepAttLteIntraF,
					pmHoPrepSuccLteIntraF,
	
					pmUeCtxtRelAbnormalEnb,
					pmErabRelAbnormalEnbActQci,
					pmErabRelAbnormalMmeActQci
					
				)
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
				  `NoHoExeAttWcdma`,
				a.pmRrcConnEstabSucc,
					a.pmRrcConnEstabAtt,
					a.pmS1SigConnEstabSucc,
					a.pmS1SigConnEstabAtt,
					a.pmErabEstabSuccInit,
					a.pmErabEstabAttInit,
					a.pmErabEstabSuccAdded,
					a.pmErabEstabAttAdded,
					a.pmErabRelAbnormalEnbAct,
					a.pmErabRelNormalEnbAct,
				
					a.pmErabRelAbnormalMmeAct,
					a.pmCellDowntimeAuto,
					a.pmCellDowntimeMan,					
					a.pmPdcpLatPktTransDl,
					a.pmPdcpLatTimeDl,
					a.pmPdcpVolDlDrb,
					a.pmSessionTimeUe,
					a.pmUeThpTimeDl,
					a.pmUeThpTimeUl,
					a.pmUeThpVolUl,
					a.pmPdcpVolDlDrbLastTTI,
					a.pmRrcConnEstabAttReatt,
	
					a.pmErabRelAbnormalEnb,
					a.pmErabRelNormalEnb,
					a.pmErabEstabAttAddedQci,
					a.pmErabRelMme,
					a.pmUeCtxtRelNormalEnb,
					a.pmErabEstabAttInitQci,
					a.pmErabEstabSuccAddedQci,
					a.pmErabEstabSuccInitQci,
					a.pmErabRelAbnormalEnbQci,
					a.pmErabRelMmeQci,
					a.pmErabRelNormalEnbQci,
					a.pmUeCtxtRelMme,
					a.pmUeCtxtRelSCWcdma,
					b.pmHoExeSuccLteIntraF AS pmHoExeSuccLteIntraF,					
					b.pmHoExeAttLteIntraF AS pmHoExeAttLteIntraF,					
					b.pmHoPrepAttLteIntraF,
					b.pmHoPrepSuccLteIntraF,
	
					a.pmUeCtxtRelAbnormalEnb,
					a.pmErabRelAbnormalEnbActQci,
					a.pmErabRelAbnormalMmeActQci
				FROM ',GT_DB,'.`tmp_pm_1_',WORKER_ID,'` a
				JOIN ',GT_DB,'.`tmp_pm_2_',WORKER_ID,'` b
				ON a.`data_date`=b.`data_date` AND a.`data_hour`=b.`data_hour` AND
				  a.`enodeb_id`=b.`enodeb_id` AND  
				  a.`CELL_ID`=b.`CELL_ID`;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt; 
	SET @SqlCmd=CONCAT('UPDATE ',GT_DB,'.`table_pm_counter_lte` A, ',CURRENT_NT_DB,'.nt_cell_current_lte B
			SET 
				A.CELL_NAME = B.CELL_NAME
			WHERE A.ENODEB_ID = B.ENODEB_ID AND A.CELL_ID = B.CELL_ID
				AND A.DATA_HOUR >= ',STARTHOUR,' AND A.DATA_HOUR < ',ENDHOUR,';');
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
		
	INSERT INTO gt_gw_main.SP_LOG VALUES(GT_DB,'SP_Generate_PM_counter_LTE_Daily',CONCAT('Done: ',TIMESTAMPDIFF(SECOND,START_TIME,SYSDATE()),' seconds.'), NOW());
	
END$$
DELIMITER ;
