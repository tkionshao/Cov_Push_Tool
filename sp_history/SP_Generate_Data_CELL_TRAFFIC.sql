DELIMITER $$
USE `operations_monitor`$$
DROP PROCEDURE IF EXISTS `SP_CreateDB_LTE`$$
CREATE DEFINER=`covmo`@`%` PROCEDURE `SP_Generate_Data_CELL_TRAFFIC`(IN exDate VARCHAR(10),IN exHour TINYINT(2))
BEGIN
                SET @v_DATA_DATE_FORMAT= DATE_FORMAT(exDate,'%Y%m%d');
                SET @SUB_GT_DB_DATE = DATE_SUB(exDate,INTERVAL 1 DAY);
                SET @LAST_NT_DATE =  DATE_FORMAT(@SUB_GT_DB_DATE, '%Y%m%d');
                SET @NT_DB =CONCAT('gt_nt_',@v_DATA_DATE_FORMAT);
                SET @SqlCmd=CONCAT('DROP  TABLE IF EXISTS operations_monitor.tmp_cell_traffic;');
                PREPARE Stmt FROM @SqlCmd;
                EXECUTE Stmt;
                DEALLOCATE PREPARE Stmt;
 
                
		SET @SqlCmd=CONCAT('DROP    TABLE IF EXISTS operations_monitor.`tmp_table_cell_agg_lte_hr_',@v_DATA_DATE_FORMAT,'`;');
                PREPARE Stmt FROM @SqlCmd;
                EXECUTE Stmt;
                DEALLOCATE PREPARE Stmt;
 
		SET @SqlCmd=CONCAT('CREATE    TABLE operations_monitor.`tmp_table_cell_agg_lte_hr_',@v_DATA_DATE_FORMAT,'`
				( `DATA_DATE` DATETIME NOT NULL,
				  `DATA_HOUR` TINYINT(4) NOT NULL,
				  `CELL_ID` TINYINT(4) UNSIGNED NOT NULL DEFAULT ''0'',
				  `ENODEB_ID` MEDIUMINT(9) NOT NULL DEFAULT ''0'',
				  `PU_ID` MEDIUMINT(9) NOT NULL,
				  `EARFCN` MEDIUMINT(9) NOT NULL,
				  `EUTRABAND` SMALLINT(6) NOT NULL,
				  `CELL_NAME` VARCHAR(50) CHARACTER SET utf8 DEFAULT NULL,
				  `INIT_CALL_CNT` INT(11) DEFAULT NULL,
				  PRIMARY KEY (`CELL_ID`,`ENODEB_ID`,`PU_ID`,`DATA_DATE`,`DATA_HOUR`),
				  KEY `INIT_CALL_CNT` (INIT_CALL_CNT)
				) ENGINE=MYISAM DEFAULT CHARSET=latin1 DELAY_KEY_WRITE=1;'); 
                PREPARE Stmt FROM @SqlCmd;
                EXECUTE Stmt;
                DEALLOCATE PREPARE Stmt;
        
		
 
               SET @SqlCmd=CONCAT('INSERT INTO operations_monitor.`tmp_table_cell_agg_lte_hr_',@v_DATA_DATE_FORMAT,'`
				( `DATA_DATE`,`DATA_HOUR`,`CELL_ID`,`ENODEB_ID`,`PU_ID`,`EARFCN`,`EUTRABAND`,`CELL_NAME`,`INIT_CALL_CNT`)
				 SELECT `DATA_DATE`,`DATA_HOUR`,`CELL_ID`,`ENODEB_ID`,`PU_ID`,`EARFCN`,`EUTRABAND`,`CELL_NAME`,`INIT_CALL_CNT` FROM
				gt_global_statistic_g1.`table_cell_agg_lte_hr_',@v_DATA_DATE_FORMAT,'`  WHERE INIT_CALL_CNT > 0
				and DATA_HOUR  = ''',exHour,''' ;'); 
                PREPARE Stmt FROM @SqlCmd;
                EXECUTE Stmt;
                DEALLOCATE PREPARE Stmt;
 
 
 
                SET @SqlCmd=CONCAT('CREATE  TABLE operations_monitor.tmp_cell_traffic 
                                        (
                                                `DATA_DATE` datetime DEFAULT NULL,
                                                `PU_NAME` varchar(15) DEFAULT NULL,
                                                `ENODEB_ID`  mediumint(9) DEFAULT NULL,
                                                `CELL_ID` mediumint(9) DEFAULT NULL,
                                                `SUB_REGION_ID` mediumint(9) DEFAULT NULL,
                                                `ECI` INT(11) DEFAULT NULL,
                                                `CELL_NAME` varchar(50) DEFAULT NULL,
                                                `CLUSTER_NAME_REGION` varchar(100) DEFAULT NULL,
                                                `CLUSTER_NAME_SUB_REGION` varchar(100) DEFAULT NULL,
                                                `CELL_OSS_NODE_ID` varchar(20) DEFAULT NULL,
                                                `ENODEB_OSS_NODE_ID` varchar(20) DEFAULT NULL,
                                                `ENODEB_VENDOR` varchar(50) DEFAULT NULL,
                                                `ACT_STATE` varchar(50) DEFAULT NULL,
                                                `GTCE` varchar(50) DEFAULT NULL,
                                                `CELL_TRAFFIC_DATA_COVERAGE` float DEFAULT NULL,
                                                `GTCE_AVAILABILITY` FLOAT DEFAULT NULL, 
                                                `CELL_TRAFFIC_1ST_TIME_IN_24HR` tinyint(4) DEFAULT NULL,
                                                `NO_CELL_TRAFFIC_IN_24HR` INT(1) DEFAULT 1,
                                                `CELL_NEVER_GEN_TRAFFIC` INT(1) DEFAULT 1,
                                                `COVMO_VALID` SMALLINT(6) DEFAULT 0,
                                                `TRAFFIC_H00` INT(11) DEFAULT NULL,
                                                `TRAFFIC_H01` INT(11) DEFAULT NULL,
                                                `TRAFFIC_H02` INT(11) DEFAULT NULL,
                                                `TRAFFIC_H03` INT(11) DEFAULT NULL,
                                                `TRAFFIC_H04` INT(11) DEFAULT NULL,
                                                `TRAFFIC_H05` INT(11) DEFAULT NULL,
                                                `TRAFFIC_H06` INT(11) DEFAULT NULL,
                                                `TRAFFIC_H07` INT(11) DEFAULT NULL,
                                                `TRAFFIC_H08` INT(11) DEFAULT NULL,
                                                `TRAFFIC_H09` INT(11) DEFAULT NULL,
                                                `TRAFFIC_H10` INT(11) DEFAULT NULL,
                                                `TRAFFIC_H11` INT(11) DEFAULT NULL,
                                                `TRAFFIC_H12` INT(11) DEFAULT NULL,
                                                `TRAFFIC_H13` INT(11) DEFAULT NULL,
                                                `TRAFFIC_H14` INT(11) DEFAULT NULL,
                                                `TRAFFIC_H15` INT(11) DEFAULT NULL,
                                                `TRAFFIC_H16` INT(11) DEFAULT NULL,
                                                `TRAFFIC_H17` INT(11) DEFAULT NULL,
                                                `TRAFFIC_H18` INT(11) DEFAULT NULL,
                                                `TRAFFIC_H19` INT(11) DEFAULT NULL,
                                                `TRAFFIC_H20` INT(11) DEFAULT NULL,
                                                `TRAFFIC_H21` INT(11) DEFAULT NULL,
                                                `TRAFFIC_H22` INT(11) DEFAULT NULL,
                                                `TRAFFIC_H23` INT(11) DEFAULT NULL,
                                                KEY `IX_CELL_ID` (`ENODEB_ID`,`CELL_ID`)
                                        )  ENGINE=MYISAM DEFAULT CHARSET=latin1;');                
                PREPARE Stmt FROM @SqlCmd;
                EXECUTE Stmt;
                DEALLOCATE PREPARE Stmt;
 
        
 
                SET @SqlCmd=CONCAT('INSERT INTO  operations_monitor.tmp_cell_traffic(
                                DATA_DATE,PU_NAME,ENODEB_ID,CELL_ID,SUB_REGION_ID,ECI,CELL_NAME,
                                CELL_TRAFFIC_DATA_COVERAGE,GTCE_AVAILABILITY,CELL_TRAFFIC_1ST_TIME_IN_24HR,NO_CELL_TRAFFIC_IN_24HR,CELL_NEVER_GEN_TRAFFIC,
                                COVMO_VALID,
				TRAFFIC_H00,TRAFFIC_H01,TRAFFIC_H02,TRAFFIC_H03,TRAFFIC_H04,TRAFFIC_H05,TRAFFIC_H06,TRAFFIC_H07,TRAFFIC_H08,TRAFFIC_H09,
				TRAFFIC_H10,TRAFFIC_H11,TRAFFIC_H12,TRAFFIC_H13,TRAFFIC_H14,TRAFFIC_H15,TRAFFIC_H16,TRAFFIC_H17,TRAFFIC_H18,TRAFFIC_H19,
				TRAFFIC_H20,TRAFFIC_H21,TRAFFIC_H22,TRAFFIC_H23)
                                        SELECT
                                                a.`DATA_DATE`,
                                                a.`PU_ID`,
                                                a.`ENODEB_ID`,
                                                a.`CELL_ID`,
                                                a.`SUB_REGION_ID`,
                                                (a.ENODEB_ID*256)+a.CELL_ID AS ECI, 
                                                a.CELL_NAME,
                                                ROUND(COUNT(DISTINCT b.`DATA_HOUR`)/24*100) AS CELL_TRAFFIC_DATA_COVERAGE,
                                                ROUND(COUNT(DISTINCT a.`DATA_HOUR`)/24*100) AS GTCE_AVAILABILITY,
                                                MIN(a.DATA_HOUR) AS CELL_TRAFFIC_1ST_TIME_IN_24HR,
                                                CASE WHEN SUM(`RRC_CONN_SETUP_ATTEMPT`)>0 THEN 0 ELSE 1 END AS NO_CELL_TRAFFIC_IN_24HR,
                                                0 AS CELL_NEVER_GEN_TRAFFIC,
                                                CASE WHEN b.ENODEB_ID is not null and b.CELL_ID is not null then 1
                                                else 0 end as COVMO_VALID,
                                                SUM(CASE WHEN a.DATA_HOUR = 0 THEN `RRC_CONN_SETUP_ATTEMPT` ELSE 0 END) AS TRAFFIC_H00,
                                                SUM(CASE WHEN a.DATA_HOUR = 1 THEN `RRC_CONN_SETUP_ATTEMPT` ELSE 0 END) AS TRAFFIC_H01,
                                                SUM(CASE WHEN a.DATA_HOUR = 2 THEN `RRC_CONN_SETUP_ATTEMPT` ELSE 0 END) AS TRAFFIC_H02,
                                                SUM(CASE WHEN a.DATA_HOUR = 3 THEN `RRC_CONN_SETUP_ATTEMPT` ELSE 0 END) AS TRAFFIC_H03,
                                                SUM(CASE WHEN a.DATA_HOUR = 4 THEN `RRC_CONN_SETUP_ATTEMPT` ELSE 0 END) AS TRAFFIC_H04,
                                                SUM(CASE WHEN a.DATA_HOUR = 5 THEN `RRC_CONN_SETUP_ATTEMPT` ELSE 0 END) AS TRAFFIC_H05,
                                                SUM(CASE WHEN a.DATA_HOUR = 6 THEN `RRC_CONN_SETUP_ATTEMPT` ELSE 0 END) AS TRAFFIC_H06,
                                                SUM(CASE WHEN a.DATA_HOUR = 7 THEN `RRC_CONN_SETUP_ATTEMPT` ELSE 0 END) AS TRAFFIC_H07,
                                                SUM(CASE WHEN a.DATA_HOUR = 8 THEN `RRC_CONN_SETUP_ATTEMPT` ELSE 0 END) AS TRAFFIC_H08,
                                                SUM(CASE WHEN a.DATA_HOUR = 9 THEN `RRC_CONN_SETUP_ATTEMPT` ELSE 0 END) AS TRAFFIC_H09,
                                                SUM(CASE WHEN a.DATA_HOUR = 10 THEN `RRC_CONN_SETUP_ATTEMPT` ELSE 0 END) AS TRAFFIC_H10,
                                                SUM(CASE WHEN a.DATA_HOUR = 11 THEN `RRC_CONN_SETUP_ATTEMPT` ELSE 0 END) AS TRAFFIC_H11,
                                                SUM(CASE WHEN a.DATA_HOUR = 12 THEN `RRC_CONN_SETUP_ATTEMPT` ELSE 0 END) AS TRAFFIC_H12,
                                                SUM(CASE WHEN a.DATA_HOUR = 13 THEN `RRC_CONN_SETUP_ATTEMPT` ELSE 0 END) AS TRAFFIC_H13,
                                                SUM(CASE WHEN a.DATA_HOUR = 14 THEN `RRC_CONN_SETUP_ATTEMPT` ELSE 0 END) AS TRAFFIC_H14,
                                                SUM(CASE WHEN a.DATA_HOUR = 15 THEN `RRC_CONN_SETUP_ATTEMPT` ELSE 0 END) AS TRAFFIC_H15,
                                                SUM(CASE WHEN a.DATA_HOUR = 16 THEN `RRC_CONN_SETUP_ATTEMPT` ELSE 0 END) AS TRAFFIC_H16,
                                                SUM(CASE WHEN a.DATA_HOUR = 17 THEN `RRC_CONN_SETUP_ATTEMPT` ELSE 0 END) AS TRAFFIC_H17,
                                                SUM(CASE WHEN a.DATA_HOUR = 18 THEN `RRC_CONN_SETUP_ATTEMPT` ELSE 0 END) AS TRAFFIC_H18,
                                                SUM(CASE WHEN a.DATA_HOUR = 19 THEN `RRC_CONN_SETUP_ATTEMPT` ELSE 0 END) AS TRAFFIC_H19,
                                                SUM(CASE WHEN a.DATA_HOUR = 20 THEN `RRC_CONN_SETUP_ATTEMPT` ELSE 0 END) AS TRAFFIC_H20,
                                                SUM(CASE WHEN a.DATA_HOUR = 21 THEN `RRC_CONN_SETUP_ATTEMPT` ELSE 0 END) AS TRAFFIC_H21,
                                                SUM(CASE WHEN a.DATA_HOUR = 22 THEN `RRC_CONN_SETUP_ATTEMPT` ELSE 0 END) AS TRAFFIC_H22,
                                                SUM(CASE WHEN a.DATA_HOUR = 23 THEN `RRC_CONN_SETUP_ATTEMPT` ELSE 0 END) AS TRAFFIC_H23
                                                FROM `gt_global_statistic_g1`.`table_cell_lte_hr_',@v_DATA_DATE_FORMAT,'` a  
						left join
						operations_monitor.`tmp_table_cell_agg_lte_hr_',@v_DATA_DATE_FORMAT,'` b  
                                                ON a.ENODEB_ID = b.ENODEB_ID and a.CELL_ID = b.CELL_ID
                                                and a.DATA_DATE = b.DATA_DATE and a.DATA_HOUR = b.DATA_HOUR
                                                WHERE a.DATA_DATE = ''',exDate,''' and a.DATA_HOUR = ''',exHour,'''  
                                                GROUP BY `DATA_DATE`,`ENODEB_ID`,`CELL_ID`
                                        ;');
                PREPARE Stmt FROM @SqlCmd;
                EXECUTE Stmt;
                DEALLOCATE PREPARE Stmt;
        
        
                SET @SqlCmd=CONCAT('DROP  TEMPORARY  TABLE IF EXISTS operations_monitor.tmp_cell_traffic_all;');
                PREPARE Stmt FROM @SqlCmd;
                EXECUTE Stmt;
                DEALLOCATE PREPARE Stmt;
                
		
                SET @SqlCmd=CONCAT('CREATE TEMPORARY  TABLE operations_monitor.tmp_cell_traffic_all 
                LIKE operations_monitor.cell_traffic;');
                PREPARE Stmt FROM @SqlCmd;
                EXECUTE Stmt;
                DEALLOCATE PREPARE Stmt;
        
                
                SET @SqlCmd=CONCAT('DROP   TABLE IF EXISTS operations_monitor.tmp_nt_cell;');
                PREPARE Stmt FROM @SqlCmd;
                EXECUTE Stmt;
                DEALLOCATE PREPARE Stmt;
		
		
                SET @SqlCmd=CONCAT('CREATE    TABLE operations_monitor.tmp_nt_cell (
                                  `ENODEB_ID` MEDIUMINT(9) DEFAULT NULL,
                                  `ENODEB_NAME` VARCHAR(50) DEFAULT NULL,
                                  `ENODEB_LOC_ID` VARCHAR(20) DEFAULT NULL,
                                  `ENODEB_NE_ID` VARCHAR(20) DEFAULT NULL,
                                  `ENODEB_OSS_NODE_ID` VARCHAR(20) DEFAULT NULL,
                                  `CELL_ID` MEDIUMINT(9) DEFAULT NULL,
                                  `CELL_NAME` VARCHAR(50) DEFAULT NULL,
                                  `CELL_LOC_ID` VARCHAR(20) DEFAULT NULL,
                                  `CELL_NE_ID` VARCHAR(20) DEFAULT NULL,
                                  `CELL_OSS_NODE_ID` VARCHAR(20) DEFAULT NULL,
                                  `EUTRABAND` MEDIUMINT(9) DEFAULT NULL,
                                  `PCI` SMALLINT(6) DEFAULT NULL,
                                  `BWCHANNEL` FLOAT DEFAULT NULL,
                                  `DL_EARFCN` MEDIUMINT(9) DEFAULT NULL,
                                  `UL_EARFCN` MEDIUMINT(9) DEFAULT NULL,
                                  `ENODEB_TYPE` VARCHAR(50) DEFAULT NULL,
                                  `ENODEB_VENDOR` VARCHAR(50) DEFAULT NULL,
                                  `INDOOR` TINYINT(4) DEFAULT NULL,
                                  `IP` VARCHAR(100) DEFAULT NULL,
                                  `CLUSTER_NAME_REGION` VARCHAR(100) DEFAULT NULL,
                                  `CLUSTER_NAME_SUB_REGION` VARCHAR(100) DEFAULT NULL,
                                  `CLUSTER_NAME_STRUCTURE_TYPE` VARCHAR(100) DEFAULT NULL,
                                  `ACT_START` DATE DEFAULT NULL,
                                  `ACT_END` DATE DEFAULT NULL,
                                  `PLAN_START` DATE DEFAULT NULL,
                                  `PLAN_END` DATE DEFAULT NULL,
                                  `ACT_STATE` VARCHAR(50) DEFAULT NULL,
                                  `ENODEB_MODEL` VARCHAR(100) DEFAULT NULL,
                                  `CLUSTER_NAME_ZONE` VARCHAR(100) DEFAULT NULL,
                                  `CELL_RADIUS` MEDIUMINT(9) DEFAULT NULL,
                                  `AMDINSTATE_LOCKED` TINYINT(4) DEFAULT NULL,
                                  `OPERSTATE_ENABLE` TINYINT(4) DEFAULT NULL,
                                  `REGION_ID` MEDIUMINT(9) DEFAULT NULL,
                                  `SUB_REGION_ID` MEDIUMINT(9) DEFAULT NULL,
                                  `NBR_DISTANCE_4G_CM` DOUBLE DEFAULT NULL,
                                  `NBR_DISTANCE_4G_VORONOI` DOUBLE DEFAULT NULL,
                                  `PU_ID` MEDIUMINT(9) DEFAULT NULL,
                                  `SITE_DENSITY_TYPE` TINYINT(4) DEFAULT NULL,
                                  `FLAG` SMALLINT(6) DEFAULT ''0'',
                                   PRIMARY KEY (`ENODEB_ID`,`CELL_ID`)
                                ) ENGINE=MYISAM DEFAULT CHARSET=utf8 DELAY_KEY_WRITE=1;');
                PREPARE Stmt FROM @SqlCmd;
                EXECUTE Stmt;
                DEALLOCATE PREPARE Stmt;
        
   
        
                SET @SqlCmd=CONCAT('
                INSERT INTO `operations_monitor`.`tmp_nt_cell` (ENODEB_ID,ENODEB_NAME,ENODEB_LOC_ID,ENODEB_NE_ID,ENODEB_OSS_NODE_ID,CELL_ID,CELL_NAME,CELL_LOC_ID,CELL_NE_ID,CELL_OSS_NODE_ID,EUTRABAND,PCI,BWCHANNEL,DL_EARFCN,UL_EARFCN,ENODEB_TYPE,ENODEB_VENDOR,INDOOR,IP,CLUSTER_NAME_REGION,CLUSTER_NAME_SUB_REGION,CLUSTER_NAME_STRUCTURE_TYPE,ACT_START,ACT_END,PLAN_START,PLAN_END,ACT_STATE,ENODEB_MODEL,CLUSTER_NAME_ZONE,CELL_RADIUS,AMDINSTATE_LOCKED,OPERSTATE_ENABLE,REGION_ID,SUB_REGION_ID,NBR_DISTANCE_4G_CM,NBR_DISTANCE_4G_VORONOI,PU_ID,SITE_DENSITY_TYPE,FLAG)
                SELECT
		  `ENODEB_ID`,
		  `ENODEB_NAME`,
		  `ENODEB_LOC_ID`,
		  `ENODEB_NE_ID`,
		  `ENODEB_OSS_NODE_ID`,
		  `CELL_ID`,
		  `CELL_NAME`,
		  `CELL_LOC_ID`,
		  `CELL_NE_ID`,
		  `CELL_OSS_NODE_ID`,
		  `EUTRABAND`,
		  `PCI`,
		  `BWCHANNEL`,
		  `DL_EARFCN`,
		  `UL_EARFCN`,
		  `ENODEB_TYPE`,
		  `ENODEB_VENDOR`,
		  `INDOOR`,
		  `IP`,
		  `CLUSTER_NAME_REGION`,
		  `CLUSTER_NAME_SUB_REGION`,
		  `CLUSTER_NAME_STRUCTURE_TYPE`,
		  `ACT_START`,
		  `ACT_END`,
		  `PLAN_START`,
		  `PLAN_END`,
		  `ACT_STATE`,
		  `ENODEB_MODEL`,
		  `CLUSTER_NAME_ZONE`,
		  `CELL_RADIUS`,
		  `AMDINSTATE_LOCKED`,
		  `OPERSTATE_ENABLE`,
		  `REGION_ID`,
		  `SUB_REGION_ID`,
		  `NBR_DISTANCE_4G_CM`,
		  `NBR_DISTANCE_4G_VORONOI`,
		  `PU_ID`,
		  `SITE_DENSITY_TYPE`,
		  `FLAG`
                FROM ',@NT_DB,'.nt_cell_current_lte;
                ');
                PREPARE Stmt FROM @SqlCmd;
                EXECUTE Stmt;
                DEALLOCATE PREPARE Stmt;
        
                SET @SqlCmd=CONCAT('
                INSERT ignore INTO `operations_monitor`.`tmp_nt_cell` (ENODEB_ID,ENODEB_NAME,ENODEB_LOC_ID,ENODEB_NE_ID,ENODEB_OSS_NODE_ID,CELL_ID,CELL_NAME,CELL_LOC_ID,CELL_NE_ID,CELL_OSS_NODE_ID,EUTRABAND,PCI,BWCHANNEL,DL_EARFCN,UL_EARFCN,ENODEB_TYPE,ENODEB_VENDOR,INDOOR,IP,CLUSTER_NAME_REGION,CLUSTER_NAME_SUB_REGION,CLUSTER_NAME_STRUCTURE_TYPE,ACT_START,ACT_END,PLAN_START,PLAN_END,ACT_STATE,ENODEB_MODEL,CLUSTER_NAME_ZONE,CELL_RADIUS,AMDINSTATE_LOCKED,OPERSTATE_ENABLE,REGION_ID,SUB_REGION_ID,NBR_DISTANCE_4G_CM,NBR_DISTANCE_4G_VORONOI,PU_ID,SITE_DENSITY_TYPE,FLAG)
                SELECT   `ENODEB_ID`,
		  `ENODEB_NAME`,
		  `ENODEB_LOC_ID`,
		  `ENODEB_NE_ID`,
		  `ENODEB_OSS_NODE_ID`,
		  `CELL_ID`,
		  `CELL_NAME`,
		  `CELL_LOC_ID`,
		  `CELL_NE_ID`,
		  `CELL_OSS_NODE_ID`,
		  `EUTRABAND`,
		  `PCI`,
		  `BWCHANNEL`,
		  `DL_EARFCN`,
		  `UL_EARFCN`,
		  `ENODEB_TYPE`,
		  `ENODEB_VENDOR`,
		  `INDOOR`,
		  `IP`,
		  `CLUSTER_NAME_REGION`,
		  `CLUSTER_NAME_SUB_REGION`,
		  `CLUSTER_NAME_STRUCTURE_TYPE`,
		  `ACT_START`,
		  `ACT_END`,
		  `PLAN_START`,
		  `PLAN_END`,
		  `ACT_STATE`,
		  `ENODEB_MODEL`,
		  `CLUSTER_NAME_ZONE`,
		  `CELL_RADIUS`,
		  `AMDINSTATE_LOCKED`,
		  `OPERSTATE_ENABLE`,
		  `REGION_ID`,
		  `SUB_REGION_ID`,
		  `NBR_DISTANCE_4G_CM`,
		  `NBR_DISTANCE_4G_VORONOI`,
		   CASE 
		   WHEN  PU_ID  IS NOT NULL  THEN PU_ID
		   ELSE ''UNKNOWN_PU'' END AS `PU_ID`,
		  `SITE_DENSITY_TYPE`,
		  `FLAG`
                FROM ',@NT_DB,'.nt_cell_current_lte_dump
                where enodeb_id > 0;
                ');
                PREPARE Stmt FROM @SqlCmd;
                EXECUTE Stmt;
                DEALLOCATE PREPARE Stmt;
        
                
   
                SET @SqlCmd=CONCAT('
                INSERT  ignore INTO `operations_monitor`.`tmp_cell_traffic_all`
                (select 
                ''',exDate,''' AS DATA_DATE, 
                a.PU_ID,
                a.ENODEB_ID,
                a.CELL_ID,
                a.SUB_REGION_ID,
                (a.ENODEB_ID*256)+a.CELL_ID AS ECI,
                a.CELL_NAME,
                a.CLUSTER_NAME_REGION,
                a.CLUSTER_NAME_SUB_REGION,
                a.CELL_OSS_NODE_ID,
                a.ENODEB_OSS_NODE_ID,
                a.ENODEB_VENDOR,
                a.ACT_STATE,
                null as GTCE,
                CASE WHEN b.CELL_TRAFFIC_DATA_COVERAGE IS NULL THEN 0 ELSE b.CELL_TRAFFIC_DATA_COVERAGE END AS CELL_TRAFFIC_DATA_COVERAGE,
                CASE WHEN b.GTCE_AVAILABILITY IS NULL THEN 0 ELSE b.GTCE_AVAILABILITY END AS GTCE_AVAILABILITY,
                b.CELL_TRAFFIC_1ST_TIME_IN_24HR,
                CASE WHEN b.NO_CELL_TRAFFIC_IN_24HR IS NULL THEN 1 ELSE 0 END AS NO_CELL_TRAFFIC_IN_24HR ,
                CASE WHEN b.CELL_NEVER_GEN_TRAFFIC IS NULL THEN 1 ELSE 0 END AS CELL_NEVER_GEN_TRAFFIC,
                b.covmo_valid,
        IFNULL(TRAFFIC_H00,0),IFNULL(TRAFFIC_H01,0),IFNULL(TRAFFIC_H02,0),IFNULL(TRAFFIC_H03,0),IFNULL(TRAFFIC_H04,0),
        IFNULL(TRAFFIC_H05,0),IFNULL(TRAFFIC_H06,0),IFNULL(TRAFFIC_H07,0),IFNULL(TRAFFIC_H08,0),IFNULL(TRAFFIC_H09,0),
        IFNULL(TRAFFIC_H10,0),IFNULL(TRAFFIC_H11,0),IFNULL(TRAFFIC_H12,0),IFNULL(TRAFFIC_H13,0),IFNULL(TRAFFIC_H14,0),
        IFNULL(TRAFFIC_H15,0),IFNULL(TRAFFIC_H16,0),IFNULL(TRAFFIC_H17,0),IFNULL(TRAFFIC_H18,0),IFNULL(TRAFFIC_H19,0),
                IFNULL(TRAFFIC_H20,0),IFNULL(TRAFFIC_H21,0),IFNULL(TRAFFIC_H22,0),IFNULL(TRAFFIC_H23,0)
                FROM   tmp_cell_traffic b right  join  `operations_monitor`.`tmp_nt_cell` a
                on a.ENODEB_ID = b.ENODEB_ID
                and a.CELL_ID=b.CELL_ID)
                UNION(
                SELECT 
                ''',exDate,''' AS DATA_DATE, 
                b.PU_NAME,
                b.ENODEB_ID,
                b.CELL_ID,
                b.SUB_REGION_ID,
                (b.ENODEB_ID*256)+b.CELL_ID AS ECI,
                b.CELL_NAME,
                a.CLUSTER_NAME_REGION,
                a.CLUSTER_NAME_SUB_REGION,
                a.CELL_OSS_NODE_ID,
                a.ENODEB_OSS_NODE_ID,
                a.ENODEB_VENDOR,
                a.ACT_STATE,
                NULL AS GTCE,
                CASE WHEN b.CELL_TRAFFIC_DATA_COVERAGE IS NULL THEN 0 ELSE b.CELL_TRAFFIC_DATA_COVERAGE END AS CELL_TRAFFIC_DATA_COVERAGE,
                CASE WHEN b.GTCE_AVAILABILITY IS NULL THEN 0 ELSE b.GTCE_AVAILABILITY END AS GTCE_AVAILABILITY,
                b.CELL_TRAFFIC_1ST_TIME_IN_24HR,
                CASE WHEN b.NO_CELL_TRAFFIC_IN_24HR IS NULL THEN 1 ELSE 0 END AS NO_CELL_TRAFFIC_IN_24HR ,
                CASE WHEN b.CELL_NEVER_GEN_TRAFFIC IS NULL THEN 1 ELSE 0 END AS CELL_NEVER_GEN_TRAFFIC,
                b.covmo_valid,
		IFNULL(TRAFFIC_H00,0),IFNULL(TRAFFIC_H01,0),IFNULL(TRAFFIC_H02,0),IFNULL(TRAFFIC_H03,0),IFNULL(TRAFFIC_H04,0),
		IFNULL(TRAFFIC_H05,0),IFNULL(TRAFFIC_H06,0),IFNULL(TRAFFIC_H07,0),IFNULL(TRAFFIC_H08,0),IFNULL(TRAFFIC_H09,0),
		IFNULL(TRAFFIC_H10,0),IFNULL(TRAFFIC_H11,0),IFNULL(TRAFFIC_H12,0),IFNULL(TRAFFIC_H13,0),IFNULL(TRAFFIC_H14,0),
		IFNULL(TRAFFIC_H15,0),IFNULL(TRAFFIC_H16,0),IFNULL(TRAFFIC_H17,0),IFNULL(TRAFFIC_H18,0),IFNULL(TRAFFIC_H19,0),
		IFNULL(TRAFFIC_H20,0),IFNULL(TRAFFIC_H21,0),IFNULL(TRAFFIC_H22,0),IFNULL(TRAFFIC_H23,0)
                FROM   tmp_cell_traffic b   LEFT  JOIN  `operations_monitor`.`tmp_nt_cell` a
                ON a.ENODEB_ID = b.ENODEB_ID
                AND a.CELL_ID=b.CELL_ID
                )
                
                
                ;');
 
                PREPARE Stmt FROM @SqlCmd;
                EXECUTE Stmt;
                DEALLOCATE PREPARE Stmt;
                
                SET @SqlCmd=CONCAT('UPDATE operations_monitor.tmp_cell_traffic_all A,`gt_global_statistic_g1`.`table_cell_agg_lte_dy_',@v_DATA_DATE_FORMAT,'` B
                                                SET A.COVMO_VALID= 1
                                                WHERE A.ENODEB_ID=B.ENODEB_ID AND A.CELL_ID=B.CELL_ID   and  a.DATA_DATE = ''',exDate,'''  and b.INIT_CALL_CNT > 0
                                        ;');
                PREPARE Stmt FROM @SqlCmd;
                EXECUTE Stmt;
                DEALLOCATE PREPARE Stmt;
                
                
                
                
                SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE if exists pu_server_mapping ;');
                PREPARE Stmt FROM @SqlCmd;
                EXECUTE Stmt;
                DEALLOCATE PREPARE Stmt;
                
                SET @SqlCmd=CONCAT('CREATE TEMPORARY TABLE  pu_server_mapping  (
                                  `RNC` MEDIUMINT(9) DEFAULT NULL,
                                  `DP_NAME` VARCHAR(50) DEFAULT NULL,
                                  `RNC_NAME` VARCHAR(50) DEFAULT NULL
                                ) ENGINE=MYISAM DEFAULT CHARSET=utf8;');
                PREPARE Stmt FROM @SqlCmd;
                EXECUTE Stmt;
                DEALLOCATE PREPARE Stmt;
                
                SET @SqlCmd=CONCAT('INSERT INTO pu_server_mapping
                                SELECT a.RNC,
                                b.DP_NAME,
                                a.RNC_NAME
                                FROM gt_gw_main.rnc_information a LEFT JOIN gt_gw_main.server_information b
                                ON gt_strtok(a.GW_URI,3,'':'')=gt_strtok(b.DB_URI,3,'':'')  ;');
                PREPARE Stmt FROM @SqlCmd;
                EXECUTE Stmt;
                DEALLOCATE PREPARE Stmt;
                SET @SqlCmd=CONCAT('UPDATE operations_monitor.`tmp_cell_traffic_all` A, pu_server_mapping B
                                        SET 
                                        A.GTCE = B.DP_NAME,
                                        A.PU_NAME = CASE WHEN B.RNC =0 THEN ''no traffic&wina'' ELSE B.RNC_NAME END                                                                                                                                           
                                        WHERE A.PU_NAME = B.RNC  and  DATA_DATE = ''',exDate,'''');
                PREPARE Stmt FROM @SqlCmd;
                EXECUTE Stmt;
                DEALLOCATE PREPARE Stmt;
                
                
                
                
                SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE if exists tmp_coverage ;');
                PREPARE Stmt FROM @SqlCmd;
                EXECUTE Stmt;
                DEALLOCATE PREPARE Stmt;
                
                SET @SqlCmd=CONCAT('CREATE TEMPORARY TABLE  tmp_coverage  (
                                  `DATA_DATE` datetime DEFAULT NULL,
                                  `ENODEB_ID`  mediumint(9) DEFAULT NULL,
                                  `CELL_ID` mediumint(9) DEFAULT NULL,
                                  `CELL_TRAFFIC_DATA_COVERAGE` float DEFAULT NULL,
                                  `GTCE_AVAILABILITY` FLOAT DEFAULT NULL
                                ) ENGINE=MYISAM DEFAULT CHARSET=utf8;');
                PREPARE Stmt FROM @SqlCmd;
                EXECUTE Stmt;
                DEALLOCATE PREPARE Stmt;
                
                SET @SqlCmd=CONCAT('INSERT INTO tmp_coverage(DATA_DATE,ENODEB_ID,CELL_ID,CELL_TRAFFIC_DATA_COVERAGE,GTCE_AVAILABILITY)
                                SELECT a.DATA_DATE,a.ENODEB_ID,a.CELL_ID,
                                ROUND(COUNT(DISTINCT b.`DATA_HOUR`)/24*100) AS CELL_TRAFFIC_DATA_COVERAGE,
                                ROUND(COUNT(DISTINCT a.`DATA_HOUR`)/24*100) AS GTCE_AVAILABILITY
                                FROM `gt_global_statistic_g1`.`table_cell_lte_hr_',@v_DATA_DATE_FORMAT,'` a  
                                left join operations_monitor.`tmp_table_cell_agg_lte_hr_',@v_DATA_DATE_FORMAT,'` b
                                ON a.ENODEB_ID = b.ENODEB_ID and a.CELL_ID = b.CELL_ID
                                and a.DATA_DATE = b.DATA_DATE 
                                WHERE a.DATA_DATE = ''',exDate,'''  --  and b.INIT_CALL_CNT > 0
                                GROUP BY a.`DATA_DATE`,a.`ENODEB_ID`,a.`CELL_ID`;');
                PREPARE Stmt FROM @SqlCmd;
                EXECUTE Stmt;
                DEALLOCATE PREPARE Stmt;
                SET @SqlCmd=CONCAT('UPDATE operations_monitor.`tmp_cell_traffic_all` A, tmp_coverage B
                                        SET 
                                                
                                                A.CELL_TRAFFIC_DATA_COVERAGE = B.CELL_TRAFFIC_DATA_COVERAGE,
                                                A.GTCE_AVAILABILITY = b.GTCE_AVAILABILITY
                                                WHERE a.DATA_DATE = ''',exDate,''' 
                and A.DATA_DATE = B.DATA_DATE
                and a.ENODEB_ID = b.ENODEB_ID and a.CELL_ID = b.CELL_ID; ');
                PREPARE Stmt FROM @SqlCmd;
                EXECUTE Stmt;
                DEALLOCATE PREPARE Stmt;
                
         
                SET @SqlCmd=CONCAT('DROP  TABLE IF EXISTS operations_monitor.`cell_traffic_',@LAST_NT_DATE,'` ;');
                PREPARE Stmt FROM @SqlCmd;
                EXECUTE Stmt;
                DEALLOCATE PREPARE Stmt;
                
                SET @SqlCmd=CONCAT('CREATE  temporary TABLE operations_monitor.`cell_traffic_',@LAST_NT_DATE,'`
                                                (
                                                        `DATA_DATE` datetime DEFAULT NULL,
                                                        `PU_NAME` varchar(15) DEFAULT NULL,
                                                        `ENODEB_ID`  mediumint(9) DEFAULT NULL,
                                                        `CELL_ID` mediumint(9) DEFAULT NULL,
                                                        `SUB_REGION_ID` mediumint(9) DEFAULT NULL,
                                                        `CELL_NEVER_GEN_TRAFFIC` INT(1) DEFAULT 1,
                                                        KEY `idx_enb_cell` (`ENODEB_ID`,CELL_ID)
                                                )  ENGINE=MYISAM DEFAULT CHARSET=latin1;');                
                PREPARE Stmt FROM @SqlCmd;
                EXECUTE Stmt;
                DEALLOCATE PREPARE Stmt;
                
                
                SET @SqlCmd=CONCAT('INSERT INTO `cell_traffic_',@LAST_NT_DATE,'` 
                                SELECT DATA_DATE,PU_NAME,ENODEB_ID,CELL_ID,SUB_REGION_ID,CELL_NEVER_GEN_TRAFFIC FROM cell_traffic
                                WHERE DATA_DATE = ''',@SUB_GT_DB_DATE,''' 
                ;');
                PREPARE Stmt FROM @SqlCmd;
                EXECUTE Stmt;
                DEALLOCATE PREPARE Stmt;
                
                
                
                
                SET @SqlCmd=CONCAT('UPDATE operations_monitor.`tmp_cell_traffic_all` A,cell_traffic_',@LAST_NT_DATE,'  B
                                        SET 
                                                A.CELL_NEVER_GEN_TRAFFIC = 0
                                                WHERE a.ENODEB_ID = b.ENODEB_ID and a.CELL_ID = b.CELL_ID
                                                and B.CELL_NEVER_GEN_TRAFFIC = 0;');
                PREPARE Stmt FROM @SqlCmd;
                EXECUTE Stmt;
                DEALLOCATE PREPARE Stmt;
                
                SET @SqlCmd=CONCAT('INSERT INTO `operations_monitor`.`cell_traffic`(DATA_DATE,PU_NAME,ENODEB_ID,CELL_ID,SUB_REGION_ID,ECI,CELL_NAME,CLUSTER_NAME_REGION,CLUSTER_NAME_SUB_REGION,CELL_OSS_NODE_ID,ENODEB_OSS_NODE_ID,ENODEB_VENDOR,ACT_STATE,GTCE,CELL_TRAFFIC_DATA_COVERAGE,GTCE_AVAILABILITY,CELL_TRAFFIC_1ST_TIME_IN_24HR,NO_CELL_TRAFFIC_IN_24HR,CELL_NEVER_GEN_TRAFFIC,COVMO_VALID,TRAFFIC_H00,TRAFFIC_H01,TRAFFIC_H02,TRAFFIC_H03,TRAFFIC_H04,TRAFFIC_H05,TRAFFIC_H06,TRAFFIC_H07,TRAFFIC_H08,TRAFFIC_H09,TRAFFIC_H10,TRAFFIC_H11,TRAFFIC_H12,TRAFFIC_H13,TRAFFIC_H14,TRAFFIC_H15,TRAFFIC_H16,TRAFFIC_H17,TRAFFIC_H18,TRAFFIC_H19,TRAFFIC_H20,TRAFFIC_H21,TRAFFIC_H22,TRAFFIC_H23)
                        SELECT 
                                DATA_DATE,
                                PU_NAME,
                                ENODEB_ID,
                                CELL_ID,
                                SUB_REGION_ID,
                                ECI,
                                CELL_NAME,
                                CLUSTER_NAME_REGION,
                                CLUSTER_NAME_SUB_REGION,
                                CELL_OSS_NODE_ID,
                                ENODEB_OSS_NODE_ID,
                                ENODEB_VENDOR,
                                ACT_STATE,
                                GTCE,
                                CELL_TRAFFIC_DATA_COVERAGE,
                                GTCE_AVAILABILITY,
                                CELL_TRAFFIC_1ST_TIME_IN_24HR,
                                NO_CELL_TRAFFIC_IN_24HR,
                                CELL_NEVER_GEN_TRAFFIC,
                                COVMO_VALID,
                                TRAFFIC_H00,
                                TRAFFIC_H01,
                                TRAFFIC_H02,
                                TRAFFIC_H03,
                                TRAFFIC_H04,
                                TRAFFIC_H05,
                                TRAFFIC_H06,
                                TRAFFIC_H07,
                                TRAFFIC_H08,
                                TRAFFIC_H09,
                                TRAFFIC_H10,
                                TRAFFIC_H11,
                                TRAFFIC_H12,
                                TRAFFIC_H13,
                                TRAFFIC_H14,
                                TRAFFIC_H15,
                                TRAFFIC_H16,
                                TRAFFIC_H17,
                                TRAFFIC_H18,
                                TRAFFIC_H19,
                                TRAFFIC_H20,
                                TRAFFIC_H21,
                                TRAFFIC_H22,
                                TRAFFIC_H23 
                        FROM  operations_monitor.tmp_cell_traffic_all
                        WHERE ENODEB_ID > 0
                        ON DUPLICATE KEY UPDATE 
                operations_monitor.cell_traffic.TRAFFIC_H00=operations_monitor.cell_traffic.TRAFFIC_H00+VALUES(TRAFFIC_H00),
                operations_monitor.cell_traffic.TRAFFIC_H01=operations_monitor.cell_traffic.TRAFFIC_H01+VALUES(TRAFFIC_H01),
                operations_monitor.cell_traffic.TRAFFIC_H02=operations_monitor.cell_traffic.TRAFFIC_H02+VALUES(TRAFFIC_H02),
                operations_monitor.cell_traffic.TRAFFIC_H03=operations_monitor.cell_traffic.TRAFFIC_H03+VALUES(TRAFFIC_H03),
                operations_monitor.cell_traffic.TRAFFIC_H04=operations_monitor.cell_traffic.TRAFFIC_H04+VALUES(TRAFFIC_H04),
                operations_monitor.cell_traffic.TRAFFIC_H05=operations_monitor.cell_traffic.TRAFFIC_H05+VALUES(TRAFFIC_H05),
                operations_monitor.cell_traffic.TRAFFIC_H06=operations_monitor.cell_traffic.TRAFFIC_H06+VALUES(TRAFFIC_H06),
                operations_monitor.cell_traffic.TRAFFIC_H07=operations_monitor.cell_traffic.TRAFFIC_H07+VALUES(TRAFFIC_H07),
                operations_monitor.cell_traffic.TRAFFIC_H08=operations_monitor.cell_traffic.TRAFFIC_H08+VALUES(TRAFFIC_H08),
                operations_monitor.cell_traffic.TRAFFIC_H09=operations_monitor.cell_traffic.TRAFFIC_H09+VALUES(TRAFFIC_H09),
                operations_monitor.cell_traffic.TRAFFIC_H10=operations_monitor.cell_traffic.TRAFFIC_H10+VALUES(TRAFFIC_H10),
                operations_monitor.cell_traffic.TRAFFIC_H11=operations_monitor.cell_traffic.TRAFFIC_H11+VALUES(TRAFFIC_H11),
                operations_monitor.cell_traffic.TRAFFIC_H12=operations_monitor.cell_traffic.TRAFFIC_H12+VALUES(TRAFFIC_H12),
                operations_monitor.cell_traffic.TRAFFIC_H13=operations_monitor.cell_traffic.TRAFFIC_H13+VALUES(TRAFFIC_H13),
                operations_monitor.cell_traffic.TRAFFIC_H14=operations_monitor.cell_traffic.TRAFFIC_H14+VALUES(TRAFFIC_H14),
                operations_monitor.cell_traffic.TRAFFIC_H15=operations_monitor.cell_traffic.TRAFFIC_H15+VALUES(TRAFFIC_H15),
                operations_monitor.cell_traffic.TRAFFIC_H16=operations_monitor.cell_traffic.TRAFFIC_H16+VALUES(TRAFFIC_H16),
                operations_monitor.cell_traffic.TRAFFIC_H17=operations_monitor.cell_traffic.TRAFFIC_H17+VALUES(TRAFFIC_H17),
                operations_monitor.cell_traffic.TRAFFIC_H18=operations_monitor.cell_traffic.TRAFFIC_H18+VALUES(TRAFFIC_H18),
                operations_monitor.cell_traffic.TRAFFIC_H19=operations_monitor.cell_traffic.TRAFFIC_H19+VALUES(TRAFFIC_H19),
                operations_monitor.cell_traffic.TRAFFIC_H20=operations_monitor.cell_traffic.TRAFFIC_H20+VALUES(TRAFFIC_H20),
                operations_monitor.cell_traffic.TRAFFIC_H21=operations_monitor.cell_traffic.TRAFFIC_H21+VALUES(TRAFFIC_H21),
                operations_monitor.cell_traffic.TRAFFIC_H22=operations_monitor.cell_traffic.TRAFFIC_H22+VALUES(TRAFFIC_H22),
                operations_monitor.cell_traffic.TRAFFIC_H23=operations_monitor.cell_traffic.TRAFFIC_H23+VALUES(TRAFFIC_H23);');
                PREPARE Stmt FROM @SqlCmd;
                EXECUTE Stmt;
                DEALLOCATE PREPARE Stmt;
                
                
                SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS operations_monitor.tmp_cell_covmo_traffic ;');
                PREPARE Stmt FROM @SqlCmd;
                EXECUTE Stmt;
                DEALLOCATE PREPARE Stmt;
                
                                
                
                SET @SqlCmd=CONCAT('UPDATE operations_monitor.`cell_traffic` A,tmp_cell_traffic_all B
                                        SET 
                                                A.CELL_TRAFFIC_1ST_TIME_IN_24HR = 
                                                CASE WHEN A.CELL_TRAFFIC_1ST_TIME_IN_24HR  IS NULL THEN B.CELL_TRAFFIC_1ST_TIME_IN_24HR
                                                ELSE A.CELL_TRAFFIC_1ST_TIME_IN_24HR  END
                                                WHERE a.DATA_DATE = ''',exDate,''' 
                                                and A.DATA_DATE = B.DATA_DATE
                                                and a.ENODEB_ID = b.ENODEB_ID and a.CELL_ID = b.CELL_ID; ');
                PREPARE Stmt FROM @SqlCmd;
                EXECUTE Stmt;
                DEALLOCATE PREPARE Stmt;
                
                
                
                SET @SqlCmd=CONCAT('DROP  TABLE IF EXISTS operations_monitor.tmp_cell_traffic;');
                PREPARE Stmt FROM @SqlCmd;
                EXECUTE Stmt;
                DEALLOCATE PREPARE Stmt;
                
                SET @SqlCmd=CONCAT('DROP  TEMPORARY  TABLE IF EXISTS operations_monitor.tmp_cell_traffic_all;');
                PREPARE Stmt FROM @SqlCmd;
                EXECUTE Stmt;
                DEALLOCATE PREPARE Stmt;
                
                
                SET @SqlCmd=CONCAT('DROP   TABLE IF EXISTS operations_monitor.tmp_nt_cell;');
                PREPARE Stmt FROM @SqlCmd;
                EXECUTE Stmt;
                DEALLOCATE PREPARE Stmt;
                
                SET @SqlCmd=CONCAT('DROP   TABLE IF EXISTS operations_monitor.`cell_traffic_',@LAST_NT_DATE,'` ;');
                PREPARE Stmt FROM @SqlCmd;
                EXECUTE Stmt;
                DEALLOCATE PREPARE Stmt;
 
 		SET @SqlCmd=CONCAT('DROP    TABLE IF EXISTS operations_monitor.`tmp_table_cell_agg_lte_hr_',@v_DATA_DATE_FORMAT,'`;');
                PREPARE Stmt FROM @SqlCmd;
                EXECUTE Stmt;
                DEALLOCATE PREPARE Stmt;
        
        
        
END$$
DELIMITER ;
