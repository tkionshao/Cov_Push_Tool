CREATE DEFINER=`covmo`@`%` PROCEDURE `SP_Check_eNBMappingList`()
BEGIN
	SET @SqlCmd=CONCAT('SELECT COUNT(*) INTO @match_CLUSTER_NAME_SUB_REGION 
		FROM gt_gw_main.pu_enodeb_mapping WHERE CLUSTER_NAME_SUB_REGION 
		IN (SELECT region_name COLLATE utf8_swedish_ci FROM gt_covmo.usr_pu_region);');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('SELECT COUNT(*) INTO @dis_CLUSTER_NAME_SUB_REGION FROM (SELECT DISTINCT CLUSTER_NAME_SUB_REGION FROM gt_gw_main.pu_enodeb_mapping) AS A;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	
	IF @match_CLUSTER_NAME_SUB_REGION = 0 THEN
		SET @SqlCmd=CONCAT('SELECT ''gt_gw_main.pu_enodeb_mapping column CLUSTER_NAME_SUB_REGION(from /tmp/eNBMappingList/Gesamtliste_CDGS_Verteilung_YYYYMMDD.csv column CDGS_Name_Herst) did not as our expected (gt_covmo.usr_pu_region column region_name) Please check /tmp/eNBMappingList/Gesamtliste_CDGS_Verteilung_YYYYMMDD.csv (DTAG Maintain)''
		INTO OUTFILE ''/opt/covmo/parser/nt/log/eNBMappingList.fail.detail''
		FIELDS TERMINATED BY '',''
		LINES TERMINATED BY ''\n'';');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
	
	END IF;
	
