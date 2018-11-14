DELIMITER $$
USE `gt_gw_main`$$
DROP PROCEDURE IF EXISTS `SP_CreateDB_LTE`$$
CREATE DEFINER=`covmo`@`%` PROCEDURE `SP_Import_Handset`(IN GT_COVMO VARCHAR(100))
BEGIN
	DECLARE START_TIME DATETIME DEFAULT SYSDATE();
	DECLARE WORKER_ID VARCHAR(10) DEFAULT CONNECTION_ID();
	
	INSERT INTO gt_gw_main.sp_log VALUES('gt_gw_main','SP_Import_Handset','Start', NOW());
	
	SELECT `value` INTO @source
	FROM `gt_gw_main`.`integration_param`
	WHERE gt_group = 'system' AND gt_name = 'endUser';
	
	
	
	
	SET @SqlCmd=CONCAT('SELECT MAX(make_id) INTO @make_id FROM ',GT_COVMO,'.dim_handset_id;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('SELECT MAX(model_id) INTO @model_rank FROM ',GT_COVMO,'.dim_handset_m_id;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('INSERT INTO ',GT_COVMO,'.dim_handset_id
			SELECT 
				@rank:= @rank + 1 AS new_make_id ,
				B.manufacturer 
			FROM ',GT_COVMO,'.dim_handset_id A RIGHT JOIN
				(SELECT DISTINCT manufacturer FROM gt_gw_main.dim_handset) B
				ON A.manufacturer = B.manufacturer
				,(SELECT @rank := ',@make_id,') c
			WHERE A.make_id IS NULL;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('INSERT INTO ',GT_COVMO,'.dim_handset_m_id
			SELECT M.make_id, N.model, N.new_model_id FROM ',GT_COVMO,'.dim_handset_id M,
			(
				SELECT 
					@rank:= @rank + 1 AS new_model_id ,
					B.manufacturer AS manufacturer,
					B.model AS model
				FROM 
					',GT_COVMO,'.dim_handset_m_id A RIGHT JOIN
					(SELECT DISTINCT model,manufacturer FROM gt_gw_main.dim_handset) B
					ON A.model = B.model
					,(SELECT @rank := ',@model_rank,') c
				WHERE A.model IS NULL
			) N
			WHERE M.manufacturer = N.manufacturer;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS gt_gw_main.tmp_dim_handset_',WORKER_ID,';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt; 
	
	SET @SqlCmd=CONCAT('CREATE TEMPORARY TABLE gt_gw_main.tmp_dim_handset_',WORKER_ID,'
			SELECT 
				A.tac,A.manufacturer,A.model,''',@source,''' AS source,B.make_id,C.model_id
			FROM gt_gw_main.dim_handset A, ',GT_COVMO,'.dim_handset_id B, ',GT_COVMO,'.dim_handset_m_id C
			WHERE A.manufacturer = B.manufacturer AND A.model = C.model AND B.make_id = C.make_id;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	SET @SqlCmd=CONCAT('INSERT INTO ',GT_COVMO,'.dim_handset
			SELECT N.* FROM ',GT_COVMO,'.dim_handset M RIGHT JOIN
				tmp_dim_handset_',WORKER_ID,' N
			ON M.tac = N.tac
			WHERE M.tac IS NULL;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	SET @SqlCmd=CONCAT('UPDATE ',GT_COVMO,'.dim_handset M ,
				tmp_dim_handset_',WORKER_ID,' N
			SET 
				M.manufacturer = N.manufacturer,
				M.model = N.model,
				M.make_id = N.make_id,
				M.model_id = N.model_id,
				M.source = N.source
			WHERE M.tac = N.tac;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS gt_gw_main.tmp_dim_handset_',WORKER_ID,';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt; 
	
	INSERT INTO gt_gw_main.sp_log VALUES('gt_gw_main','SP_Import_Handset',CONCAT('Done: ',TIMESTAMPDIFF(SECOND,START_TIME,SYSDATE()),' seconds.'), NOW());
END$$
DELIMITER ;
