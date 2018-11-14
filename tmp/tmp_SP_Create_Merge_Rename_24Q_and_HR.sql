CREATE DEFINER=`covmo`@`%` PROCEDURE `SP_Create_Merge_Rename_24Q_and_HR`(IN GT_DB VARCHAR(100),IN TBL_NAME VARCHAR(100))
BEGIN
	DECLARE START_TIME DATETIME DEFAULT SYSDATE();
	DECLARE UNION_STR TEXT ;
	DECLARE UNION_HR_STR TEXT ;
	
	DECLARE MAX_HR SMALLINT(6);
	DECLARE MAX_QR SMALLINT(6);
	DECLARE STR_HR SMALLINT(6);
	DECLARE STR_QR SMALLINT(6);
	
	DECLARE qry_tbl_name VARCHAR(50);
	DECLARE qry_tbl_name_hr VARCHAR(50);
	DECLARE v_i SMALLINT(6);
	DECLARE v_k SMALLINT(6);
	DECLARE v_k_Diff SMALLINT(6);
	DECLARE v_i_Diff SMALLINT(6);
 
	SET MAX_HR=24;
	SET MAX_QR=60;
	SET STR_HR=0;
	SET STR_QR=0;
	SET qry_tbl_name='';
	SET v_k=STR_HR;
	SET v_k_Diff=1;
	WHILE v_k < MAX_HR DO
	BEGIN
		SET v_i=STR_QR;
		SET v_i_Diff=15;
		WHILE v_i < MAX_QR DO
		BEGIN
			SET qry_tbl_name=CONCAT(TBL_NAME,'_',RIGHT(CONCAT(RIGHT(CONCAT('0',v_k),2),RIGHT(CONCAT('0',v_i),2)),4));
			SET @SqlCmd=CONCAT('DROP TABLE IF EXISTS ',GT_DB,'.',qry_tbl_name,';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
			SET @SqlCmd=CONCAT('CREATE TABLE ',GT_DB,'.',qry_tbl_name,' LIKE ',GT_DB,'.',TBL_NAME,';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
		
			IF UNION_STR IS NULL THEN
				SET UNION_STR = CONCAT(GT_DB,'.',qry_tbl_name,'');
			ELSE
				SET UNION_STR = CONCAT(UNION_STR,',',GT_DB,'.',qry_tbl_name,'');
			END IF;	
			IF UNION_HR_STR IS NULL THEN
				SET UNION_HR_STR = CONCAT(GT_DB,'.',qry_tbl_name,'');
			ELSE
				SET UNION_HR_STR = CONCAT(UNION_HR_STR,',',GT_DB,'.',qry_tbl_name,'');
			END IF;	
			SET v_i=v_i+v_i_Diff;
		END;
		END WHILE;
		SET qry_tbl_name_hr=CONCAT(TBL_NAME,'_',RIGHT(CONCAT('0',v_k),2));
		SET @SqlCmd=CONCAT('DROP TABLE IF EXISTS ',GT_DB,'.',qry_tbl_name_hr,';');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		SET @SqlCmd=CONCAT('CREATE TABLE ',GT_DB,'.',qry_tbl_name_hr,' LIKE ',GT_DB,'.',TBL_NAME,';');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		SET @SqlCmd=CONCAT('ALTER TABLE ',GT_DB,'.',qry_tbl_name_hr,' ENGINE = MRG_MYISAM UNION=(',UNION_HR_STR,');');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		SET UNION_HR_STR = NULL;
	
		SET v_i=STR_QR;
		SET v_k=v_k+v_k_Diff;
	END;
	END WHILE;
	SET v_i=STR_QR;			
	
	SET @SqlCmd=CONCAT('ALTER TABLE ',GT_DB,'.',TBL_NAME,' ENGINE = MRG_MYISAM UNION=(',UNION_STR,');');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
