CREATE DEFINER=`covmo`@`%` PROCEDURE `SP_Create_Merge_Rename_24QI`(IN GT_DB VARCHAR(100),IN TBL_NAME VARCHAR(100),IN TABLE_NUM TINYINT(2), IN BOTTON_STR VARCHAR(100))
BEGIN
	DECLARE START_TIME DATETIME DEFAULT SYSDATE();
	DECLARE NEW_TBL_NAME VARCHAR(100) ;
	DECLARE UNION_STR TEXT ;
	DECLARE tbl_num SMALLINT(6) DEFAULT 0;
	
	DECLARE MAX_HR SMALLINT(6);
	DECLARE MAX_QR SMALLINT(6);
	DECLARE STR_HR SMALLINT(6);
	DECLARE STR_QR SMALLINT(6);
	
	DECLARE qry_tbl_name VARCHAR(50);
	DECLARE v_i SMALLINT(6);
	DECLARE v_k SMALLINT(6);
	DECLARE v_k_Diff SMALLINT(6);
	DECLARE v_i_Diff SMALLINT(6);
 
	SET MAX_HR=24;
	SET MAX_QR=60;
	SET STR_HR=0;
	SET STR_QR=0;
	SET tbl_num=TABLE_NUM;
	SET qry_tbl_name='';
	SET v_i=STR_QR;
	SET v_i_Diff=15;
	WHILE v_i < MAX_QR DO
	BEGIN
		SET v_k=STR_HR;
		SET v_k_Diff=1;
		WHILE v_k < MAX_HR DO
		BEGIN
			SET NEW_TBL_NAME = REPLACE(TBL_NAME,BOTTON_STR,'');
			SET qry_tbl_name=CONCAT(NEW_TBL_NAME,'_',RIGHT(CONCAT(RIGHT(CONCAT('0',v_k),2),RIGHT(CONCAT('0',v_i),2)),4),BOTTON_STR);
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
			
			SET v_k=v_k+v_k_Diff;
		END;
		END WHILE;
		SET v_k=STR_HR;
		SET v_i=v_i+v_i_Diff;
	END;
	END WHILE;
	SET v_i=STR_QR;			
	
	SET @SqlCmd=CONCAT('ALTER TABLE ',GT_DB,'.',TBL_NAME,' ENGINE = MRG_MYISAM UNION=(',UNION_STR,');');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
