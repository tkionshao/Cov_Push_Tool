CREATE DEFINER=`covmo`@`%` PROCEDURE `SP_Check_Missing_MNC`()
BEGIN
	
	SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS tmp_dim_mnc_ap;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('CREATE TEMPORARY TABLE `tmp_dim_mnc_ap` (
				  `imsi` MEDIUMINT(9) NOT NULL,
				  PRIMARY KEY (`imsi`)
				) ENGINE=MYISAM DEFAULT CHARSET=latin1;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt; 
	
	SET @SqlCmd=CONCAT('INSERT INTO tmp_dim_mnc_ap 
				SELECT imsi FROM `gt_gw_main`.`dim_mnc_ap` ;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt; 
	
	SET @SqlCmd=CONCAT('SELECT LEFT(A.IMSI,3) AS MCC,RIGHT(A.IMSI,2) AS MNC FROM gt_gw_main.dim_imsi_imei A
				WHERE NOT EXISTS 
				(SELECT NULL FROM tmp_dim_mnc_ap B
				WHERE B.imsi = LEFT(A.IMSI,5));');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt; 
	
	SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS tmp_dim_mnc_ap;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
