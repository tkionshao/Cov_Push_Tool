DELIMITER $$
USE `gt_gw_main`$$
DROP PROCEDURE IF EXISTS `SP_CreateDB_LTE`$$
CREATE DEFINER=`covmo`@`%` PROCEDURE `SP_Sub_Update_IMSI_CNT_LTE`(IN GT_DB VARCHAR(100), IN RTYPE CHAR(1))
BEGIN
	DECLARE WORKER_ID VARCHAR(10) DEFAULT CONNECTION_ID();
	DECLARE START_TIME DATETIME DEFAULT SYSDATE();
	
	DECLARE SH_EH VARCHAR(9) DEFAULT RIGHT(GT_DB,9);
	DECLARE STARTHOUR VARCHAR(2) DEFAULT SUBSTRING(RIGHT(GT_DB,18),10,2);
	DECLARE ENDHOUR VARCHAR(2) DEFAULT IF(SUBSTRING(RIGHT(GT_DB,18),15,2)='00','24',SUBSTRING(RIGHT(GT_DB,18),15,2));
	SELECT REPLACE(GT_DB,SH_EH,'0000_0000') INTO GT_DB;
	
	INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'SP_Sub_Update_IMSI_CNT_LTE','Start', NOW());
	
	
	IF RTYPE = 'h' THEN
		INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'SP_Sub_Update_IMSI_CNT_LTE','hourly', NOW());
	
		SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS ',GT_DB,'.tmp_cell_update_imsi_start','_',WORKER_ID,';');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		
		SET @SqlCmd=CONCAT('CREATE TEMPORARY TABLE ',GT_DB,'.tmp_cell_update_imsi_start','_',WORKER_ID,' 
					(
						DATA_DATE DATE NOT NULL,
						DATA_HOUR SMALLINT(6) NOT NULL,
						CALL_TYPE TINYINT(4) NOT NULL,
						CALL_STATUS TINYINT(4) NOT NULL,
						MOVING TINYINT(4) NOT NULL,
						INDOOR TINYINT(4) NOT NULL,
						SUB_REGION_ID MEDIUMINT(9) NOT NULL,
						EUTRABAND SMALLINT(6) NOT NULL,
						EARFCN MEDIUMINT(9) NOT NULL,
						ENODEB_ID MEDIUMINT(9)NOT NULL,
						CELL_ID SMALLINT(6) UNSIGNED NOT NULL,
						IMSI_CNT INT(11) DEFAULT NULL,
					  PRIMARY KEY(ENODEB_ID,CELL_ID,CALL_TYPE,CALL_STATUS,MOVING,INDOOR,SUB_REGION_ID,EUTRABAND,EARFCN,DATA_DATE,`DATA_HOUR`)
					) ENGINE=MyISAM DEFAULT CHARSET=latin1;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
	
		SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS ',GT_DB,'.tmp_cell_update_imsi_end','_',WORKER_ID,';');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		
		SET @SqlCmd=CONCAT('CREATE TEMPORARY TABLE ',GT_DB,'.tmp_cell_update_imsi_end','_',WORKER_ID,' 
					(
						DATA_DATE DATE NOT NULL,
						DATA_HOUR SMALLINT(6) NOT NULL,
						CALL_TYPE TINYINT(4) NOT NULL,
						CALL_STATUS TINYINT(4) NOT NULL,
						MOVING TINYINT(4) NOT NULL,
						INDOOR TINYINT(4) NOT NULL,
						SUB_REGION_ID MEDIUMINT(9) NOT NULL,
						EUTRABAND SMALLINT(6) NOT NULL,
						EARFCN MEDIUMINT(9) NOT NULL,
						ENODEB_ID MEDIUMINT(9)NOT NULL,
						CELL_ID SMALLINT(6) UNSIGNED NOT NULL,
						IMSI_CNT INT(11) DEFAULT NULL,
					  PRIMARY KEY(ENODEB_ID,CELL_ID,CALL_TYPE,CALL_STATUS,MOVING,INDOOR,SUB_REGION_ID,EUTRABAND,EARFCN,DATA_DATE,`DATA_HOUR`)
					) ENGINE=MyISAM DEFAULT CHARSET=latin1;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		SET @SqlCmd=CONCAT('INSERT INTO ',GT_DB,'.tmp_cell_update_imsi_start','_',WORKER_ID,'
				   SELECT
				  `DATA_DATE`,
				  `DATA_HOUR`,
				  `CALL_TYPE`,
				  `CALL_STATUS`,
				  `MOVING`,
				  `INDOOR`,
				  `SUB_REGION_ID`,
				  `EUTRABAND`,
				  `EARFCN`,
				  `ENODEB_ID`,
				  `CELL_ID`,
				   COUNT(DISTINCT IMSI) AS IMSI_CNT
				FROM ',GT_DB,'.`rpt_cell_imsi_start`
				WHERE DATA_HOUR = ',STARTHOUR,'
				GROUP BY DATA_DATE,DATA_HOUR,CALL_TYPE,CALL_STATUS,MOVING,INDOOR,SUB_REGION_ID,EUTRABAND,EARFCN,ENODEB_ID,CELL_ID
		;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
	
		SET @SqlCmd=CONCAT('INSERT INTO ',GT_DB,'.tmp_cell_update_imsi_end','_',WORKER_ID,'
				   SELECT
				  `DATA_DATE`,
				  `DATA_HOUR`,
				  `CALL_TYPE`,
				  `CALL_STATUS`,
				  `MOVING`,
				  `INDOOR`,
				  `SUB_REGION_ID`,
				  `EUTRABAND`,
				  `EARFCN`,
				  `ENODEB_ID`,
				  `CELL_ID`,
				   COUNT(DISTINCT IMSI) AS IMSI_CNT
				FROM ',GT_DB,'.`rpt_cell_imsi_end`
				WHERE DATA_HOUR = ',STARTHOUR,'
				GROUP BY DATA_DATE,DATA_HOUR,CALL_TYPE,CALL_STATUS,MOVING,INDOOR,SUB_REGION_ID,EUTRABAND,EARFCN,ENODEB_ID,CELL_ID
		;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		SET @SqlCmd=CONCAT('UPDATE ',GT_DB,'.rpt_cell_start A ,',GT_DB,'.tmp_cell_update_imsi_start','_',WORKER_ID,' B
				    SET A.IMSI_CNT=IFNULL(B.IMSI_CNT,0)
				    WHERE A.DATA_DATE=B.DATA_DATE
				    AND A.DATA_HOUR=B.DATA_HOUR
				    AND A.CALL_TYPE=B.CALL_TYPE
				    AND A.CALL_STATUS=B.CALL_STATUS
				    AND A.MOVING=B.MOVING
				    AND A.INDOOR=B.INDOOR
				    AND A.SUB_REGION_ID=B.SUB_REGION_ID
				    AND A.EUTRABAND=B.EUTRABAND
				    AND A.EARFCN=B.EARFCN
				    AND A.ENODEB_ID=B.ENODEB_ID
				    AND A.CELL_ID=B.CELL_ID
		;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		SET @SqlCmd=CONCAT('UPDATE ',GT_DB,'.rpt_cell_end A ,',GT_DB,'.tmp_cell_update_imsi_end','_',WORKER_ID,' B
				    SET A.IMSI_CNT=IFNULL(B.IMSI_CNT,0)
				    WHERE A.DATA_DATE=B.DATA_DATE
				    AND A.DATA_HOUR=B.DATA_HOUR
				    AND A.CALL_TYPE=B.CALL_TYPE
				    AND A.CALL_STATUS=B.CALL_STATUS
				    AND A.MOVING=B.MOVING
				    AND A.INDOOR=B.INDOOR
				    AND A.SUB_REGION_ID=B.SUB_REGION_ID
				    AND A.EUTRABAND=B.EUTRABAND
				    AND A.EARFCN=B.EARFCN
				    AND A.ENODEB_ID=B.ENODEB_ID
				    AND A.CELL_ID=B.CELL_ID
		;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		SET @SqlCmd=CONCAT('UPDATE ',GT_DB,'.rpt_cell_start_dy A ,',GT_DB,'.tmp_cell_update_imsi_start','_',WORKER_ID,' B
				    SET A.IMSI_CNT=(CASE WHEN IFNULL(A.IMSI_CNT,0) > IFNULL(B.IMSI_CNT,0) THEN IFNULL(A.IMSI_CNT,0)
					WHEN IFNULL(A.IMSI_CNT,0) <= IFNULL(B.IMSI_CNT,0) THEN IFNULL(B.IMSI_CNT,0)
					END)
				    WHERE A.DATA_DATE=B.DATA_DATE
				    AND A.CALL_TYPE=B.CALL_TYPE
				    AND A.CALL_STATUS=B.CALL_STATUS
				    AND A.MOVING=B.MOVING
				    AND A.INDOOR=B.INDOOR
				    AND A.SUB_REGION_ID=B.SUB_REGION_ID
				    AND A.EUTRABAND=B.EUTRABAND
				    AND A.EARFCN=B.EARFCN
				    AND A.ENODEB_ID=B.ENODEB_ID
				    AND A.CELL_ID=B.CELL_ID
		;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
	
		SET @SqlCmd=CONCAT('UPDATE ',GT_DB,'.rpt_cell_end_dy A ,',GT_DB,'.tmp_cell_update_imsi_end','_',WORKER_ID,' B
				    SET A.IMSI_CNT=(CASE WHEN IFNULL(A.IMSI_CNT,0) > IFNULL(B.IMSI_CNT,0) THEN IFNULL(A.IMSI_CNT,0)
					WHEN IFNULL(A.IMSI_CNT,0) <= IFNULL(B.IMSI_CNT,0) THEN IFNULL(B.IMSI_CNT,0)
					END)
				    WHERE A.DATA_DATE=B.DATA_DATE
				    AND A.CALL_TYPE=B.CALL_TYPE
				    AND A.CALL_STATUS=B.CALL_STATUS
				    AND A.MOVING=B.MOVING
				    AND A.INDOOR=B.INDOOR
				    AND A.SUB_REGION_ID=B.SUB_REGION_ID
				    AND A.EUTRABAND=B.EUTRABAND
				    AND A.EARFCN=B.EARFCN
				    AND A.ENODEB_ID=B.ENODEB_ID
				    AND A.CELL_ID=B.CELL_ID
		;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		
		SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS ',GT_DB,'.tmp_cell_update_imsi_start_def','_',WORKER_ID,';');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		SET @SqlCmd=CONCAT('CREATE TEMPORARY TABLE ',GT_DB,'.tmp_cell_update_imsi_start_def','_',WORKER_ID,' 
					(
						DATA_DATE DATE NOT NULL,
						DATA_HOUR SMALLINT(6) NOT NULL,
						SUB_REGION_ID MEDIUMINT(9) NOT NULL,
						EUTRABAND SMALLINT(6) NOT NULL,
						EARFCN MEDIUMINT(9) NOT NULL,
						ENODEB_ID MEDIUMINT(9)NOT NULL,
						CELL_ID SMALLINT(6) UNSIGNED NOT NULL,
						IMSI_CNT INT(11) DEFAULT NULL,
					  PRIMARY KEY(ENODEB_ID,CELL_ID,SUB_REGION_ID,EUTRABAND,EARFCN,DATA_DATE,`DATA_HOUR`)
					) ENGINE=MyISAM DEFAULT CHARSET=latin1;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		
		SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS ',GT_DB,'.tmp_cell_update_imsi_end_def','_',WORKER_ID,';');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		SET @SqlCmd=CONCAT('CREATE TEMPORARY TABLE ',GT_DB,'.tmp_cell_update_imsi_end_def','_',WORKER_ID,' 
					(
						DATA_DATE DATE NOT NULL,
						DATA_HOUR SMALLINT(6) NOT NULL,
						SUB_REGION_ID MEDIUMINT(9) NOT NULL,
						EUTRABAND SMALLINT(6) NOT NULL,
						EARFCN MEDIUMINT(9) NOT NULL,
						ENODEB_ID MEDIUMINT(9)NOT NULL,
						CELL_ID SMALLINT(6) UNSIGNED NOT NULL,
						IMSI_CNT INT(11) DEFAULT NULL,
					  PRIMARY KEY(ENODEB_ID,CELL_ID,SUB_REGION_ID,EUTRABAND,EARFCN,DATA_DATE,`DATA_HOUR`)
					) ENGINE=MyISAM DEFAULT CHARSET=latin1;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		
		SET @SqlCmd=CONCAT('INSERT INTO ',GT_DB,'.tmp_cell_update_imsi_start_def','_',WORKER_ID,'
				   SELECT
				  `DATA_DATE`,
				  `DATA_HOUR`,
				  `SUB_REGION_ID`,
				  `EUTRABAND`,
				  `EARFCN`,
				  `ENODEB_ID`,
				  `CELL_ID`,
				   COUNT(DISTINCT IMSI) AS IMSI_CNT
				FROM ',GT_DB,'.`rpt_cell_imsi_start_def`
				WHERE DATA_HOUR = ',STARTHOUR,'
				GROUP BY DATA_DATE,DATA_HOUR,SUB_REGION_ID,EUTRABAND,EARFCN,ENODEB_ID,CELL_ID
		;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		
		SET @SqlCmd=CONCAT('INSERT INTO ',GT_DB,'.tmp_cell_update_imsi_end_def','_',WORKER_ID,'
				   SELECT
				  `DATA_DATE`,
				  `DATA_HOUR`,
				  `SUB_REGION_ID`,
				  `EUTRABAND`,
				  `EARFCN`,
				  `ENODEB_ID`,
				  `CELL_ID`,
				   COUNT(DISTINCT IMSI) AS IMSI_CNT
				FROM ',GT_DB,'.`rpt_cell_imsi_end_def`
				WHERE DATA_HOUR = ',STARTHOUR,'
				GROUP BY DATA_DATE,DATA_HOUR,SUB_REGION_ID,EUTRABAND,EARFCN,ENODEB_ID,CELL_ID
		;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		
		SET @SqlCmd=CONCAT('UPDATE ',GT_DB,'.rpt_cell_start_def A ,',GT_DB,'.tmp_cell_update_imsi_start_def','_',WORKER_ID,' B
				    SET A.IMSI_CNT=IFNULL(B.IMSI_CNT,0)
				    WHERE A.DATA_DATE=B.DATA_DATE
				    AND A.DATA_HOUR=B.DATA_HOUR
				    AND A.SUB_REGION_ID=B.SUB_REGION_ID
				    AND A.EUTRABAND=B.EUTRABAND
				    AND A.EARFCN=B.EARFCN
				    AND A.ENODEB_ID=B.ENODEB_ID
				    AND A.CELL_ID=B.CELL_ID;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		
		SET @SqlCmd=CONCAT('UPDATE ',GT_DB,'.rpt_cell_end_def A ,',GT_DB,'.tmp_cell_update_imsi_end_def','_',WORKER_ID,' B
				    SET A.IMSI_CNT=IFNULL(B.IMSI_CNT,0)
				    WHERE A.DATA_DATE=B.DATA_DATE
				    AND A.DATA_HOUR=B.DATA_HOUR
				    AND A.SUB_REGION_ID=B.SUB_REGION_ID
				    AND A.EUTRABAND=B.EUTRABAND
				    AND A.EARFCN=B.EARFCN
				    AND A.ENODEB_ID=B.ENODEB_ID
				    AND A.CELL_ID=B.CELL_ID;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		
		SET @SqlCmd=CONCAT('UPDATE ',GT_DB,'.rpt_cell_start_dy_def A ,',GT_DB,'.tmp_cell_update_imsi_start_def','_',WORKER_ID,' B
				    SET A.IMSI_CNT=(CASE WHEN IFNULL(A.IMSI_CNT,0) > IFNULL(B.IMSI_CNT,0) THEN IFNULL(A.IMSI_CNT,0)
					WHEN IFNULL(A.IMSI_CNT,0) <= IFNULL(B.IMSI_CNT,0) THEN IFNULL(B.IMSI_CNT,0)
					END)
				    WHERE A.DATA_DATE=B.DATA_DATE
				    AND A.SUB_REGION_ID=B.SUB_REGION_ID
				    AND A.EUTRABAND=B.EUTRABAND
				    AND A.EARFCN=B.EARFCN
				    AND A.ENODEB_ID=B.ENODEB_ID
				    AND A.CELL_ID=B.CELL_ID;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		
		SET @SqlCmd=CONCAT('UPDATE ',GT_DB,'.rpt_cell_end_dy_def A ,',GT_DB,'.tmp_cell_update_imsi_end_def','_',WORKER_ID,' B
				    SET A.IMSI_CNT=(CASE WHEN IFNULL(A.IMSI_CNT,0) > IFNULL(B.IMSI_CNT,0) THEN IFNULL(A.IMSI_CNT,0)
					WHEN IFNULL(A.IMSI_CNT,0) <= IFNULL(B.IMSI_CNT,0) THEN IFNULL(B.IMSI_CNT,0)
					END)
				    WHERE A.DATA_DATE=B.DATA_DATE
				    AND A.SUB_REGION_ID=B.SUB_REGION_ID
				    AND A.EUTRABAND=B.EUTRABAND
				    AND A.EARFCN=B.EARFCN
				    AND A.ENODEB_ID=B.ENODEB_ID
				    AND A.CELL_ID=B.CELL_ID;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		
		SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS ',GT_DB,'.tmp_cell_update_imsi_start','_',WORKER_ID,';');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;		
		
		SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS ',GT_DB,'.tmp_cell_update_imsi_start_def','_',WORKER_ID,';');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS ',GT_DB,'.tmp_cell_update_imsi_end','_',WORKER_ID,';');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;		
		
		SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS ',GT_DB,'.tmp_cell_update_imsi_end_def','_',WORKER_ID,';');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
	
	ELSEIF RTYPE = 'd' THEN
		INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'SP_Sub_Update_IMSI_CNT_LTE','daily', NOW());
		SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS ',GT_DB,'.tmp_cell_update_imsi_start','_',WORKER_ID,';');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		
		SET @SqlCmd=CONCAT('CREATE TEMPORARY TABLE ',GT_DB,'.tmp_cell_update_imsi_start','_',WORKER_ID,' 
					(
						DATA_DATE DATE NOT NULL,
						CALL_TYPE TINYINT(4) NOT NULL,
						CALL_STATUS TINYINT(4) NOT NULL,
						MOVING TINYINT(4) NOT NULL,
						INDOOR TINYINT(4) NOT NULL,
						SUB_REGION_ID MEDIUMINT(9) NOT NULL,
						EUTRABAND SMALLINT(6) NOT NULL,
						EARFCN MEDIUMINT(9) NOT NULL,
						ENODEB_ID MEDIUMINT(9)NOT NULL,
						CELL_ID SMALLINT(6) UNSIGNED NOT NULL,
						IMSI_CNT INT(11) DEFAULT NULL,
					  PRIMARY KEY(ENODEB_ID,CELL_ID,CALL_TYPE,CALL_STATUS,MOVING,INDOOR,SUB_REGION_ID,EUTRABAND,EARFCN,DATA_DATE)
					) ENGINE=MyISAM DEFAULT CHARSET=latin1;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
	
		SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS ',GT_DB,'.tmp_cell_update_imsi_end','_',WORKER_ID,';');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		
		SET @SqlCmd=CONCAT('CREATE TEMPORARY TABLE ',GT_DB,'.tmp_cell_update_imsi_end','_',WORKER_ID,' 
					(
						DATA_DATE DATE NOT NULL,
						CALL_TYPE TINYINT(4) NOT NULL,
						CALL_STATUS TINYINT(4) NOT NULL,
						MOVING TINYINT(4) NOT NULL,
						INDOOR TINYINT(4) NOT NULL,
						SUB_REGION_ID MEDIUMINT(9) NOT NULL,
						EUTRABAND SMALLINT(6) NOT NULL,
						EARFCN MEDIUMINT(9) NOT NULL,
						ENODEB_ID MEDIUMINT(9)NOT NULL,
						CELL_ID SMALLINT(6) UNSIGNED NOT NULL,
						IMSI_CNT INT(11) DEFAULT NULL,
					  PRIMARY KEY(ENODEB_ID,CELL_ID,CALL_TYPE,CALL_STATUS,MOVING,INDOOR,SUB_REGION_ID,EUTRABAND,EARFCN,DATA_DATE)
					) ENGINE=MyISAM DEFAULT CHARSET=latin1;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		
		SET @SqlCmd=CONCAT('INSERT INTO ',GT_DB,'.tmp_cell_update_imsi_start','_',WORKER_ID,'
				   SELECT
				  `DATA_DATE`,
				  `CALL_TYPE`,
				  `CALL_STATUS`,
				  `MOVING`,
				  `INDOOR`,
				  `SUB_REGION_ID`,
				  `EUTRABAND`,
				  `EARFCN`,
				  `ENODEB_ID`,
				  `CELL_ID`,
				   COUNT(DISTINCT IMSI) AS IMSI_CNT
				FROM ',GT_DB,'.`rpt_cell_imsi_start`
				GROUP BY DATA_DATE,CALL_TYPE,CALL_STATUS,MOVING,INDOOR,SUB_REGION_ID,EUTRABAND,EARFCN,ENODEB_ID,CELL_ID
		;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
	
		SET @SqlCmd=CONCAT('INSERT INTO ',GT_DB,'.tmp_cell_update_imsi_end','_',WORKER_ID,'
				   SELECT
				  `DATA_DATE`,
				  `CALL_TYPE`,
				  `CALL_STATUS`,
				  `MOVING`,
				  `INDOOR`,
				  `SUB_REGION_ID`,
				  `EUTRABAND`,
				  `EARFCN`,
				  `ENODEB_ID`,
				  `CELL_ID`,
				   COUNT(DISTINCT IMSI) AS IMSI_CNT
				FROM ',GT_DB,'.`rpt_cell_imsi_end`
				GROUP BY DATA_DATE,CALL_TYPE,CALL_STATUS,MOVING,INDOOR,SUB_REGION_ID,EUTRABAND,EARFCN,ENODEB_ID,CELL_ID
		;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		
		SET @SqlCmd=CONCAT('UPDATE ',GT_DB,'.rpt_cell_start_dy A ,',GT_DB,'.tmp_cell_update_imsi_start','_',WORKER_ID,' B
				    SET A.IMSI_CNT=B.IMSI_CNT
				    WHERE A.DATA_DATE=B.DATA_DATE
				    AND A.CALL_TYPE=B.CALL_TYPE
				    AND A.CALL_STATUS=B.CALL_STATUS
				    AND A.MOVING=B.MOVING
				    AND A.INDOOR=B.INDOOR
				    AND A.SUB_REGION_ID=B.SUB_REGION_ID
				    AND A.EUTRABAND=B.EUTRABAND
				    AND A.EARFCN=B.EARFCN
				    AND A.ENODEB_ID=B.ENODEB_ID
				    AND A.CELL_ID=B.CELL_ID
		;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
	
		SET @SqlCmd=CONCAT('UPDATE ',GT_DB,'.rpt_cell_end_dy A ,',GT_DB,'.tmp_cell_update_imsi_end','_',WORKER_ID,' B
				    SET A.IMSI_CNT=B.IMSI_CNT
				    WHERE A.DATA_DATE=B.DATA_DATE
				    AND A.CALL_TYPE=B.CALL_TYPE
				    AND A.CALL_STATUS=B.CALL_STATUS
				    AND A.MOVING=B.MOVING
				    AND A.INDOOR=B.INDOOR
				    AND A.SUB_REGION_ID=B.SUB_REGION_ID
				    AND A.EUTRABAND=B.EUTRABAND
				    AND A.EARFCN=B.EARFCN
				    AND A.ENODEB_ID=B.ENODEB_ID
				    AND A.CELL_ID=B.CELL_ID
		;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		
		SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS ',GT_DB,'.tmp_cell_update_imsi_start_def','_',WORKER_ID,';');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		SET @SqlCmd=CONCAT('CREATE TEMPORARY TABLE ',GT_DB,'.tmp_cell_update_imsi_start_def','_',WORKER_ID,' 
					(
						DATA_DATE DATE NOT NULL,
						SUB_REGION_ID MEDIUMINT(9) NOT NULL,
						EUTRABAND SMALLINT(6) NOT NULL,
						EARFCN MEDIUMINT(9) NOT NULL,
						ENODEB_ID MEDIUMINT(9)NOT NULL,
						CELL_ID SMALLINT(6) UNSIGNED NOT NULL,
						IMSI_CNT INT(11) DEFAULT NULL,
					  PRIMARY KEY(ENODEB_ID,CELL_ID,SUB_REGION_ID,EUTRABAND,EARFCN,DATA_DATE)
					) ENGINE=MyISAM DEFAULT CHARSET=latin1;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS ',GT_DB,'.tmp_cell_update_imsi_end_def','_',WORKER_ID,';');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		SET @SqlCmd=CONCAT('CREATE TEMPORARY TABLE ',GT_DB,'.tmp_cell_update_imsi_end_def','_',WORKER_ID,' 
					(
						DATA_DATE DATE NOT NULL,
						SUB_REGION_ID MEDIUMINT(9) NOT NULL,
						EUTRABAND SMALLINT(6) NOT NULL,
						EARFCN MEDIUMINT(9) NOT NULL,
						ENODEB_ID MEDIUMINT(9)NOT NULL,
						CELL_ID SMALLINT(6) UNSIGNED NOT NULL,
						IMSI_CNT INT(11) DEFAULT NULL,
					  PRIMARY KEY(ENODEB_ID,CELL_ID,SUB_REGION_ID,EUTRABAND,EARFCN,DATA_DATE)
					) ENGINE=MyISAM DEFAULT CHARSET=latin1;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;	
	
		SET @SqlCmd=CONCAT('INSERT INTO ',GT_DB,'.tmp_cell_update_imsi_start_def','_',WORKER_ID,'
				   SELECT
				  `DATA_DATE`,
				  `SUB_REGION_ID`,
				  `EUTRABAND`,
				  `EARFCN`,
				  `ENODEB_ID`,
				  `CELL_ID`,
				   COUNT(DISTINCT IMSI) AS IMSI_CNT
				FROM ',GT_DB,'.`rpt_cell_imsi_start_def`
				GROUP BY DATA_DATE,SUB_REGION_ID,EUTRABAND,EARFCN,ENODEB_ID,CELL_ID
		;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
	
		SET @SqlCmd=CONCAT('INSERT INTO ',GT_DB,'.tmp_cell_update_imsi_end_def','_',WORKER_ID,'
				   SELECT
				  `DATA_DATE`,
				  `SUB_REGION_ID`,
				  `EUTRABAND`,
				  `EARFCN`,
				  `ENODEB_ID`,
				  `CELL_ID`,
				   COUNT(DISTINCT IMSI) AS IMSI_CNT
				FROM ',GT_DB,'.`rpt_cell_imsi_end_def`
				GROUP BY DATA_DATE,SUB_REGION_ID,EUTRABAND,EARFCN,ENODEB_ID,CELL_ID
		;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
	
		SET @SqlCmd=CONCAT('UPDATE ',GT_DB,'.rpt_cell_start_dy_def A ,',GT_DB,'.tmp_cell_update_imsi_start_def','_',WORKER_ID,' B
				    SET A.IMSI_CNT=B.IMSI_CNT
				    WHERE A.DATA_DATE=B.DATA_DATE
				    AND A.SUB_REGION_ID=B.SUB_REGION_ID
				    AND A.EUTRABAND=B.EUTRABAND
				    AND A.EARFCN=B.EARFCN
				    AND A.ENODEB_ID=B.ENODEB_ID
				    AND A.CELL_ID=B.CELL_ID;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
	
		SET @SqlCmd=CONCAT('UPDATE ',GT_DB,'.rpt_cell_end_dy_def A ,',GT_DB,'.tmp_cell_update_imsi_end_def','_',WORKER_ID,' B
				    SET A.IMSI_CNT=B.IMSI_CNT
				    WHERE A.DATA_DATE=B.DATA_DATE
				    AND A.SUB_REGION_ID=B.SUB_REGION_ID
				    AND A.EUTRABAND=B.EUTRABAND
				    AND A.EARFCN=B.EARFCN
				    AND A.ENODEB_ID=B.ENODEB_ID
				    AND A.CELL_ID=B.CELL_ID;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;	
	
		SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS ',GT_DB,'.tmp_cell_update_imsi_start','_',WORKER_ID,';');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;		
		
		SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS ',GT_DB,'.tmp_cell_update_imsi_start_def','_',WORKER_ID,';');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS ',GT_DB,'.tmp_cell_update_imsi_end','_',WORKER_ID,';');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;		
		
		SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS ',GT_DB,'.tmp_cell_update_imsi_end_def','_',WORKER_ID,';');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
	END IF;
	
	INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'SP_Sub_Update_IMSI_CNT_LTE',CONCAT(' Done: ',TIMESTAMPDIFF(SECOND,START_TIME,SYSDATE()),' seconds.'), NOW());
	
END$$
DELIMITER ;
