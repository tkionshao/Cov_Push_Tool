DELIMITER $$
USE `gt_gw_main`$$
DROP PROCEDURE IF EXISTS `SP_CreateDB_LTE`$$
CREATE DEFINER=`covmo`@`%` PROCEDURE `SP_Check_Missing_TAC`()
BEGIN
	
	SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS tmp_dim_handset_ap;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	
	SET @SqlCmd=CONCAT('CREATE TEMPORARY TABLE `tmp_dim_handset_ap` (
				  `tac` VARCHAR(8) NOT NULL,
				  PRIMARY KEY (`tac`)
				) ENGINE=MYISAM DEFAULT CHARSET=latin1;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt; 
	
	
	SET @SqlCmd=CONCAT('INSERT INTO tmp_dim_handset_ap 
				SELECT TAC FROM `gt_gw_main`.`dim_handset_ap` ;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt; 
	
	SET @SqlCmd=CONCAT('SELECT LEFT(A.IMEI,8) AS TAC,COUNT(*) AS CNT FROM gt_gw_main.dim_imsi_imei A
				WHERE NOT EXISTS 
				(SELECT NULL FROM tmp_dim_handset_ap B
				WHERE B.TAC = LEFT(A.IMEI,8))
				AND LEFT(A.IMEI,8) NOT IN 
					(''01355100'',
					''01355600'',
					''35231705'',
					''35565805'',
					''35853305'',
					''35919517'',
					''38652500'',
					''86630201'',
					''FFFFFFFF'')
				GROUP BY LEFT(A.IMEI,8)
				ORDER BY COUNT(*) DESC;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt; 
	
	
	SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS tmp_dim_handset_ap;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
END$$
DELIMITER ;
