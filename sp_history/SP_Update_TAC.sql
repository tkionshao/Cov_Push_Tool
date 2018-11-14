DELIMITER $$
USE `gt_gw_main`$$
DROP PROCEDURE IF EXISTS `SP_CreateDB_LTE`$$
CREATE DEFINER=`covmo`@`%` PROCEDURE `SP_Update_TAC`(IN GT_GW_MAIN VARCHAR(50),IN GT_COVMO VARCHAR(50),IN PROJECT_NAME VARCHAR(50))
BEGIN	
        DECLARE START_TIME DATETIME DEFAULT SYSDATE();
	
	DECLARE EXIT HANDLER FOR SQLEXCEPTION
	BEGIN
		REPLACE INTO GT_COVMO.`sys_config`(group_name,tech_mask,att_name,att_value,category,visible,readonly)
				VALUES('HeartBeat','0','tac_sp','-1','System',0,1);
	END;	
	
       	INSERT INTO gt_gw_main.SP_LOG VALUES(GT_COVMO,'SP_Update_TAC','Start', START_TIME);
	
	-- ----------update \t \n
	SET @SqlCmd=CONCAT('UPDATE ',GT_COVMO,'.`tmp_tac_import`
				SET `manufacturer` = TRIM(REPLACE(REPLACE(manufacturer,''\r'',''''),''\n'','''')),`model` = TRIM(REPLACE(REPLACE(model,''\r'',''''),''\n'',''''));');
	PREPARE stmt FROM @sqlcmd;
	EXECUTE stmt;
	DEALLOCATE PREPARE stmt;
	
	SET @SqlCmd=CONCAT('UPDATE ',GT_COVMO,'.`dim_handset_m_id`
				SET `model` = TRIM(REPLACE(REPLACE(model,''\r'',''''),''\n'','''')),`full_name` = TRIM(REPLACE(REPLACE(full_name,''\r'',''''),''\n'',''''));');
	PREPARE stmt FROM @sqlcmd;
	EXECUTE stmt;
	DEALLOCATE PREPARE stmt;
	
	SET @SqlCmd=CONCAT('UPDATE ',GT_COVMO,'.`dim_handset`
				SET `manufacturer` = TRIM(REPLACE(REPLACE(manufacturer,''\r'',''''),''\n'','''')),`model` = TRIM(REPLACE(REPLACE(model,''\r'',''''),''\n'',''''));');
	PREPARE stmt FROM @sqlcmd;
	EXECUTE stmt;
	DEALLOCATE PREPARE stmt;
	
	SET @SqlCmd=CONCAT('UPDATE ',GT_COVMO,'.`dim_handset_id`
				SET `manufacturer` = TRIM(REPLACE(REPLACE(manufacturer,''\r'',''''),''\n'','''')),`full_name` = TRIM(REPLACE(REPLACE(full_name,''\r'',''''),''\n'',''''));');
	PREPARE stmt FROM @sqlcmd;
	EXECUTE stmt;
	DEALLOCATE PREPARE stmt;
	
	-- ------------------------create TMP_NEW_TAC insert new tac
	SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE  IF EXISTS ',GT_COVMO,'.TMP_NEW_TAC');
	PREPARE stmt FROM @sqlcmd;
	EXECUTE stmt;	
	DEALLOCATE PREPARE stmt;	
	
	SET @SqlCmd=CONCAT('CREATE TEMPORARY TABLE ',GT_COVMO,'.TMP_NEW_TAC AS
				SELECT tac,manufacturer,model,gt_covmo_csv_count(manufacturer,'' '') as make_string_cnt
				,gt_covmo_csv_count(model,'' '') as modle_string_cnt FROM ',GT_COVMO,'.tmp_tac_import A
				WHERE NOT EXISTS 
				(SELECT NULL FROM ',GT_COVMO,'.dim_handset B WHERE B.tac=A.tac);');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('ALTER TABLE ',GT_COVMO,'.TMP_NEW_TAC ADD INDEX (tac);');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('ALTER TABLE ',GT_COVMO,'.TMP_NEW_TAC ADD INDEX (model);');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('ALTER TABLE ',GT_COVMO,'.TMP_NEW_TAC ADD COLUMN source varchar(20);');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	-- --------------------------insert changed manufacturer or model
	SET @SqlCmd=CONCAT('INSERT INTO ',GT_COVMO,'.TMP_NEW_TAC(tac,manufacturer,model,source,make_string_cnt,modle_string_cnt)
				SELECT A.tac,A.manufacturer,A.model,B.source,gt_covmo_csv_count(A.manufacturer,'' '') as make_string_cnt
				,gt_covmo_csv_count(A.model,'' '') as modle_string_cnt FROM ',GT_COVMO,'.tmp_tac_import A,',GT_COVMO,'.dim_handset B
				WHERE 				
				(A.TAC=B.tac AND A.manufacturer<>B.manufacturer)
				OR (A.TAC=B.tac AND A.MODEL<>B.MODEL);');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	-- ----------------------source
	SET @SqlCmd=CONCAT('UPDATE ',GT_COVMO,'.TMP_NEW_TAC SET source="',PROJECT_NAME,'/',CURDATE()+0,'";');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('SELECT COUNT(*) INTO @Source FROM ',GT_COVMO,'.TMP_NEW_TAC;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	-- ----------------------
	SET @SqlCmd=CONCAT('ALTER TABLE ',GT_COVMO,'.TMP_NEW_TAC ADD COLUMN make_id SMALLINT(6);');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('ALTER TABLE ',GT_COVMO,'.TMP_NEW_TAC ADD COLUMN model_id MEDIUMINT(9) AFTER make_id;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('ALTER TABLE ',GT_COVMO,'.TMP_NEW_TAC ADD COLUMN strtok1 varchar(100) AFTER model_id;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('ALTER TABLE ',GT_COVMO,'.TMP_NEW_TAC ADD COLUMN strtok2 varchar(100) AFTER strtok1;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('UPDATE ',GT_COVMO,'.TMP_NEW_TAC 
				SET strtok1=gt_strtok(UPPER(manufacturer),1,'' ''),
				strtok2=gt_strtok(UPPER(manufacturer),2,'' '');');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	-- ------------------------------------------------------------
	SET @SqlCmd=CONCAT('CREATE INDEX `key_manufacturer` ON ',GT_COVMO,'.TMP_NEW_TAC (manufacturer);');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('CREATE INDEX `key_make_id` ON ',GT_COVMO,'.TMP_NEW_TAC (make_id);');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('CREATE INDEX `key_model_id` ON ',GT_COVMO,'.TMP_NEW_TAC (model_id);');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('CREATE INDEX `key_model` ON ',GT_COVMO,'.TMP_NEW_TAC (model);');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('CREATE INDEX `key_strtok1` ON ',GT_COVMO,'.TMP_NEW_TAC (strtok1);');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('CREATE INDEX `key_strtok2` ON ',GT_COVMO,'.TMP_NEW_TAC (strtok2);');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	-- --------------------------------------------create tmp_dim_handset
	SET @SqlCmd=CONCAT('DROP TABLE IF EXISTS ',GT_COVMO,'.TMP_dim_handset;');
	PREPARE stmt FROM @sqlcmd;
	EXECUTE stmt;
	DEALLOCATE PREPARE stmt;
 	
	SET @SqlCmd=CONCAT('CREATE TABLE ',GT_COVMO,'.`TMP_dim_handset` LIKE ',GT_COVMO,'.`dim_handset`');
	PREPARE stmt FROM @sqlcmd;
	EXECUTE stmt;
	DEALLOCATE PREPARE stmt;
	
	SET @SqlCmd=CONCAT('INSERT INTO ',GT_COVMO,'.`TMP_dim_handset` 
				SELECT * FROM ',GT_COVMO,'.dim_handset;');
	PREPARE stmt FROM @sqlcmd;
	EXECUTE stmt;
	DEALLOCATE PREPARE stmt;
	
	SET @SqlCmd=CONCAT('ALTER TABLE ',GT_COVMO,'.TMP_dim_handset ADD COLUMN strtok1 varchar(100);');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('ALTER TABLE ',GT_COVMO,'.TMP_dim_handset ADD COLUMN strtok2 varchar(100) AFTER strtok1;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('UPDATE ',GT_COVMO,'.TMP_dim_handset 
				SET strtok1=gt_strtok(UPPER(manufacturer),1,'' ''),
				strtok2=gt_strtok(UPPER(manufacturer),2,'' '');');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('CREATE INDEX `key_strtok1` ON ',GT_COVMO,'.TMP_dim_handset (strtok1);');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('CREATE INDEX `key_strtok2` ON ',GT_COVMO,'.TMP_dim_handset (strtok2);');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	-- --------------------------------------------create tmp_dim_handset_m_id
	SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE  IF EXISTS ',GT_COVMO,'.TMP_dim_handset_m_id;');
	PREPARE stmt FROM @sqlcmd;
	EXECUTE stmt;
	DEALLOCATE PREPARE stmt;
		
	SET @SqlCmd=CONCAT('CREATE TEMPORARY TABLE ',GT_COVMO,'.TMP_dim_handset_m_id LIKE ',GT_COVMO,'.dim_handset_m_id;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('INSERT INTO ',GT_COVMO,'.TMP_dim_handset_m_id 
				SELECT * FROM ',GT_COVMO,'.dim_handset_m_id;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;	
	
	SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE  IF EXISTS ',GT_COVMO,'.TMP_dim_handset_m_id_1;');
	PREPARE stmt FROM @sqlcmd;
	EXECUTE stmt;
	DEALLOCATE PREPARE stmt;
		
	SET @SqlCmd=CONCAT('CREATE TEMPORARY TABLE ',GT_COVMO,'.TMP_dim_handset_m_id_1 AS
				SELECT * FROM ',GT_COVMO,'.dim_handset_m_id;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('ALTER TABLE ',GT_COVMO,'.TMP_dim_handset_m_id_1 ADD INDEX (full_name);');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;	
	
	-- --------------------------------------------create tmp_dim_handset_id
	SET @SqlCmd=CONCAT('DROP TABLE  IF EXISTS ',GT_COVMO,'.TMP_dim_handset_id;');
	PREPARE stmt FROM @sqlcmd;
	EXECUTE stmt;
	DEALLOCATE PREPARE stmt;
	
	SET @SqlCmd=CONCAT('CREATE TABLE ',GT_COVMO,'.TMP_dim_handset_id LIKE ',GT_COVMO,'.dim_handset_id;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;	
	
	SET @SqlCmd=CONCAT('INSERT INTO ',GT_COVMO,'.TMP_dim_handset_id 
				SELECT * FROM ',GT_COVMO,'.dim_handset_id;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;	
	
	SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE  IF EXISTS ',GT_COVMO,'.TMP_dim_handset_id_1;');
	PREPARE stmt FROM @sqlcmd;
	EXECUTE stmt;
	DEALLOCATE PREPARE stmt;
	
	SET @SqlCmd=CONCAT('CREATE TEMPORARY TABLE ',GT_COVMO,'.TMP_dim_handset_id_1 LIKE ',GT_COVMO,'.dim_handset_id;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	-- ---------------------------------------------------------insert new make_id
	INSERT INTO gt_gw_main.SP_LOG VALUES(GT_COVMO,'SP_Update_TAC','ADD NEW manufacturer', START_TIME);
	
 	SET @SqlCmd=CONCAT('INSERT INTO ',GT_COVMO,'.TMP_dim_handset_id(manufacturer,full_name)
				SELECT DISTINCT manufacturer,manufacturer FROM ',GT_COVMO,'.TMP_NEW_TAC				
				WHERE make_string_cnt=1 
				AND manufacturer NOT IN (SELECT full_name FROM ',GT_COVMO,'.dim_handset_id)
				AND gt_strtok(UPPER(manufacturer),1,'' '') NOT IN (SELECT (gt_strtok(UPPER(full_name),1,'' '')) FROM ',GT_COVMO,'.dim_handset_id)
				AND gt_strtok(UPPER(manufacturer),1,'' '') NOT IN (SELECT IFNULL((gt_strtok(UPPER(full_name),2,'' '')),''NULL'') FROM ',GT_COVMO,'.dim_handset_id);				
				');				
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	-- make_string_cnt>1
	SET @SqlCmd=CONCAT('INSERT INTO ',GT_COVMO,'.TMP_dim_handset_id(manufacturer,full_name)
				SELECT DISTINCT manufacturer,manufacturer FROM ',GT_COVMO,'.TMP_NEW_TAC				
				WHERE make_string_cnt>1 
				AND manufacturer NOT IN (SELECT full_name FROM ',GT_COVMO,'.dim_handset_id)
				AND gt_strtok(UPPER(manufacturer),1,'' '') NOT IN (SELECT (gt_strtok(UPPER(full_name),1,'' ''))FROM ',GT_COVMO,'.dim_handset_id)
				AND gt_strtok(UPPER(manufacturer),2,'' '') NOT IN (SELECT IFNULL((gt_strtok(UPPER(full_name),1,'' '')),''NULL'') FROM ',GT_COVMO,'.dim_handset_id)
				AND gt_strtok(UPPER(manufacturer),1,'' '') NOT IN (SELECT (gt_strtok(UPPER(full_name),2,'' ''))FROM ',GT_COVMO,'.dim_handset_id)
				AND gt_strtok(UPPER(manufacturer),2,'' '') NOT IN (SELECT IFNULL((gt_strtok(UPPER(full_name),2,'' '')),''NULL'') FROM ',GT_COVMO,'.dim_handset_id);				
				');				
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	-- ---------------------------------------------------------insert new model_id
	INSERT INTO gt_gw_main.SP_LOG VALUES(GT_COVMO,'SP_Update_TAC','ADD NEW MODEL', START_TIME);
	SET @SqlCmd=CONCAT('INSERT INTO ',GT_COVMO,'.TMP_dim_handset_m_id(model,full_name)
				SELECT DISTINCT model,model FROM ',GT_COVMO,'.TMP_NEW_TAC A
				WHERE NOT EXISTS 
				(SELECT NULL FROM ',GT_COVMO,'.TMP_dim_handset_m_id_1 B WHERE B.full_name=A.model)
				AND NOT EXISTS 
				(SELECT NULL FROM ',GT_COVMO,'.TMP_dim_handset C WHERE C.TAC=A.TAC);');		
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	-- ---------------------------------------------Check TMP_dim_handset_id and fix
	-- ---------------the same kind of manufacturer
	SET @SqlCmd=CONCAT('INSERT INTO ',GT_COVMO,'.TMP_dim_handset_id_1(make_id,manufacturer,full_name)
				SELECT make_id,manufacturer,full_name FROM ',GT_COVMO,'.TMP_dim_handset_id				
				WHERE gt_strtok(UPPER(manufacturer),1,'' '') IN (SELECT UPPER(full_name) FROM ',GT_COVMO,'.TMP_dim_handset_id)
				AND gt_strtok(UPPER(manufacturer),2,'' '') IN (SELECT gt_strtok(UPPER(full_name),2,'' '') FROM ',GT_COVMO,'.TMP_dim_handset_id) 
				OR gt_strtok(UPPER(manufacturer),2,'' '') IN (SELECT UPPER(full_name) FROM ',GT_COVMO,'.TMP_dim_handset_id)
				AND gt_strtok(UPPER(manufacturer),2,'' '') IN (SELECT gt_strtok(UPPER(full_name),2,'' '') FROM ',GT_COVMO,'.TMP_dim_handset_id);
			   ');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('DELETE FROM ',GT_COVMO,'.TMP_dim_handset_id			
			    WHERE make_id IN (SELECT `make_id` FROM ',GT_COVMO,'.TMP_dim_handset_id_1);
			   ');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	-- ---------------repeat manufacturer
	SET @SqlCmd=CONCAT('TRUNCATE TABLE ',GT_COVMO,'.TMP_dim_handset_id_1;');
	PREPARE stmt FROM @sqlcmd;
	EXECUTE stmt;
	DEALLOCATE PREPARE stmt;
	
	SET @SqlCmd=CONCAT('INSERT INTO ',GT_COVMO,'.TMP_dim_handset_id_1(make_id,manufacturer,full_name)
				SELECT A.make_id,A.manufacturer,A.full_name FROM ',GT_COVMO,'.TMP_dim_handset_id A JOIN ',GT_COVMO,'.TMP_dim_handset_id B ON A.manufacturer=B.manufacturer 
				WHERE A.make_id > B.make_id ;
			   ');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('DELETE FROM ',GT_COVMO,'.TMP_dim_handset_id			
			    WHERE make_id IN (SELECT `make_id` FROM ',GT_COVMO,'.TMP_dim_handset_id_1);
			   ');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	-- -----------------------create new TMP_dim_handset_id_1	
	SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE  IF EXISTS ',GT_COVMO,'.TMP_dim_handset_id_1;');
	PREPARE stmt FROM @sqlcmd;
	EXECUTE stmt;
	DEALLOCATE PREPARE stmt;
	
	SET @SqlCmd=CONCAT('CREATE TEMPORARY TABLE ',GT_COVMO,'.TMP_dim_handset_id_1 AS
				SELECT make_id,manufacturer,
				IFNULL((gt_strtok(UPPER(manufacturer),1,'' '')),''NULL'') AS strtok1,IFNULL((gt_strtok(UPPER(manufacturer),2,'' '')),''NULL'') AS strtok2
				FROM ',GT_COVMO,'.TMP_dim_handset_id;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;	
	-- ----
	SET @SqlCmd=CONCAT('CREATE INDEX `key_strtok1` ON ',GT_COVMO,'.TMP_dim_handset_id_1 (strtok1);');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('CREATE INDEX `key_strtok2` ON ',GT_COVMO,'.TMP_dim_handset_id_1 (strtok2);');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
		
	-- ---------------Update TMP_dim_handset make_id & manufacturer
	SET @SqlCmd=CONCAT('SELECT COUNT(*) INTO @D_id FROM ',GT_COVMO,'.dim_handset_id;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('SELECT COUNT(*) INTO @TMP_D_id FROM ',GT_COVMO,'.TMP_dim_handset_id;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	IF @D_id > @TMP_D_id THEN
		SET @SqlCmd=CONCAT('UPDATE ',GT_COVMO,'.TMP_dim_handset A, ',GT_COVMO,'.TMP_dim_handset_id_1 B
					SET A.MAKE_ID=B.MAKE_ID, A.manufacturer=B.manufacturer
					WHERE gt_covmo_csv_count(A.manufacturer,'' '')=1  AND
					A.strtok1 = B.strtok1 AND 
					A.strtok2 = B.strtok2;' );
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		
		SET @SqlCmd=CONCAT('UPDATE ',GT_COVMO,'.TMP_dim_handset A, ',GT_COVMO,'.TMP_dim_handset_id_1 B
					SET A.MAKE_ID=B.MAKE_ID, A.manufacturer=B.manufacturer
					WHERE gt_covmo_csv_count(A.manufacturer,'' '')=1  AND
					A.strtok1 = B.strtok1 AND 
					A.strtok2 <> B.strtok2;' );
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		
		SET @SqlCmd=CONCAT('UPDATE ',GT_COVMO,'.TMP_dim_handset A, ',GT_COVMO,'.TMP_dim_handset_id_1 B
					SET A.MAKE_ID=B.MAKE_ID, A.manufacturer=B.manufacturer
					WHERE gt_covmo_csv_count(A.manufacturer,'' '')=1  AND
					A.strtok1 <> B.strtok1 AND 
					A.strtok2 = B.strtok2;' );
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		
		SET @SqlCmd=CONCAT('UPDATE ',GT_COVMO,'.TMP_dim_handset A, ',GT_COVMO,'.TMP_dim_handset_id_1 B
					SET A.MAKE_ID=B.MAKE_ID, A.manufacturer=B.manufacturer
					WHERE gt_covmo_csv_count(A.manufacturer,'' '')>1  AND
					A.strtok1 = B.strtok1 AND 
					A.strtok2 = B.strtok2;' );
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		
		SET @SqlCmd=CONCAT('UPDATE ',GT_COVMO,'.TMP_dim_handset A, ',GT_COVMO,'.TMP_dim_handset_id_1 B
					SET A.MAKE_ID=B.MAKE_ID, A.manufacturer=B.manufacturer
					WHERE gt_covmo_csv_count(A.manufacturer,'' '')>1  AND
					A.strtok1 = B.strtok1;' );
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		
		SET @SqlCmd=CONCAT('UPDATE ',GT_COVMO,'.TMP_dim_handset A, ',GT_COVMO,'.TMP_dim_handset_id_1 B
					SET A.MAKE_ID=B.MAKE_ID, A.manufacturer=B.manufacturer
					WHERE gt_covmo_csv_count(A.manufacturer,'' '')>1  AND
					A.strtok1 = B.strtok2;' );
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		
		SET @SqlCmd=CONCAT('UPDATE ',GT_COVMO,'.TMP_dim_handset A, ',GT_COVMO,'.TMP_dim_handset_id_1 B
					SET A.MAKE_ID=B.MAKE_ID, A.manufacturer=B.manufacturer
					WHERE gt_covmo_csv_count(A.manufacturer,'' '')>1  AND
					A.strtok2 = B.strtok1;' );
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		
	END IF;
	-- ---------------------------------------------
	
	-- -----------------------MAKE_ID
	SET @SqlCmd=CONCAT('UPDATE ',GT_COVMO,'.TMP_NEW_TAC A,',GT_COVMO,'.TMP_dim_handset_id_1 B
				SET A.MAKE_ID=B.MAKE_ID, A.manufacturer=B.manufacturer
				WHERE A.MAKE_ID IS NULL AND A.make_string_cnt=1  AND
				A.strtok1 = B.strtok1
				AND A.strtok2 = B.strtok2;' );
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('UPDATE ',GT_COVMO,'.TMP_NEW_TAC A,',GT_COVMO,'.TMP_dim_handset_id_1 B
				SET A.MAKE_ID=B.MAKE_ID, A.manufacturer=B.manufacturer
				WHERE A.MAKE_ID IS NULL AND A.make_string_cnt=1  AND
				A.strtok1 = B.strtok1
				AND A.strtok1 <> B.strtok2;' );
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('UPDATE ',GT_COVMO,'.TMP_NEW_TAC A,',GT_COVMO,'.TMP_dim_handset_id_1 B
				SET A.MAKE_ID=B.MAKE_ID, A.manufacturer=B.manufacturer
				WHERE A.MAKE_ID IS NULL AND A.make_string_cnt=1  AND
				A.strtok1 <> B.strtok1
				AND A.strtok1 = B.strtok2;' );
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('UPDATE ',GT_COVMO,'.TMP_NEW_TAC A,',GT_COVMO,'.TMP_dim_handset_id_1 B
				SET A.MAKE_ID=B.MAKE_ID, A.manufacturer=B.manufacturer
				WHERE A.MAKE_ID IS NULL AND A.make_string_cnt>1  AND
				A.strtok1 = B.strtok1
				AND A.strtok2 = B.strtok2;' );
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('UPDATE ',GT_COVMO,'.TMP_NEW_TAC A,',GT_COVMO,'.TMP_dim_handset_id_1 B
				SET A.MAKE_ID=B.MAKE_ID, A.manufacturer=B.manufacturer
				WHERE A.MAKE_ID IS NULL AND A.make_string_cnt>1  AND
				A.strtok1 = B.strtok1;' );
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
		
	SET @SqlCmd=CONCAT('UPDATE ',GT_COVMO,'.TMP_NEW_TAC A,',GT_COVMO,'.TMP_dim_handset_id_1 B
				SET A.MAKE_ID=B.MAKE_ID, A.manufacturer=B.manufacturer
				WHERE A.MAKE_ID IS NULL AND A.make_string_cnt>1  AND
				A.strtok1 = B.strtok2;' );
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('UPDATE ',GT_COVMO,'.TMP_NEW_TAC A,',GT_COVMO,'.TMP_dim_handset_id_1 B
				SET A.MAKE_ID=B.MAKE_ID, A.manufacturer=B.manufacturer
				WHERE A.MAKE_ID IS NULL AND A.make_string_cnt>1  AND
				A.strtok2 = B.strtok1;' );
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	-- -----------------------------------------model_id
	SET @SqlCmd=CONCAT('UPDATE ',GT_COVMO,'.TMP_NEW_TAC A,',GT_COVMO,'.TMP_dim_handset_m_id B
				SET A.model_id=B.model_id
				WHERE A.model_id IS NULL 
				AND 
				A.model = B.model;' );		
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('UPDATE ',GT_COVMO,'.TMP_NEW_TAC A,',GT_COVMO,'.TMP_dim_handset B
				SET A.model_id=B.model_id
				WHERE A.model_id IS NULL 
				AND 
				A.tac = B.tac;' );		
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	INSERT INTO gt_gw_main.SP_LOG VALUES(GT_COVMO,'SP_Update_TAC','UPDATE DIM_HANDSET', START_TIME);
	
	-- ----------------------replace into
	SET @SqlCmd=CONCAT('DROP INDEX `key_strtok1` ON ',GT_COVMO,'.TMP_dim_handset;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('DROP INDEX `key_strtok2` ON ',GT_COVMO,'.TMP_dim_handset;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('ALTER TABLE ',GT_COVMO,'.TMP_dim_handset DROP COLUMN strtok1;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('ALTER TABLE ',GT_COVMO,'.TMP_dim_handset DROP COLUMN strtok2;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('RENAME TABLE ',GT_COVMO,'.dim_handset TO ',GT_COVMO,'.TMP_dim_handset_1;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('RENAME TABLE ',GT_COVMO,'.TMP_dim_handset TO ',GT_COVMO,'.dim_handset;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('RENAME TABLE ',GT_COVMO,'.TMP_dim_handset_1 TO ',GT_COVMO,'.TMP_dim_handset;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('REPLACE INTO ',GT_COVMO,'.dim_handset(tac,manufacturer,model,source,make_id,model_id)
				SELECT LPAD(tac,8,''0''),manufacturer,model,source,make_id,model_id FROM ',GT_COVMO,'.TMP_NEW_TAC;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	-- ----------------------dim_handset_id	
	INSERT INTO gt_gw_main.SP_LOG VALUES(GT_COVMO,'SP_Update_TAC','UPDATE dim_handset_id', START_TIME);
	
	SET @SqlCmd=CONCAT('RENAME TABLE ',GT_COVMO,'.dim_handset_id TO ',GT_COVMO,'.TMP_dim_handset_id_2,
			    ',GT_COVMO,'.TMP_dim_handset_id TO ',GT_COVMO,'.dim_handset_id,
			    ',GT_COVMO,'.TMP_dim_handset_id_2 TO ',GT_COVMO,'.TMP_dim_handset_id;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	-- ----------------------dim_handset_m_id
	INSERT INTO gt_gw_main.SP_LOG VALUES(GT_COVMO,'SP_Update_TAC','UPDATE dim_handset_m_id', START_TIME);
	
	SET @SqlCmd=CONCAT('REPLACE INTO ',GT_COVMO,'.dim_handset_m_id(make_id,model,model_id,full_name)
				SELECT make_id,model,model_id,model
				FROM ',GT_COVMO,'.dim_handset
				GROUP BY make_id,model,model_id;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	#INSERT INTO gt_gw_main.SP_LOG VALUES(GT_COVMO,'SP_Update_TAC','UPDATE dim_handset_m_id2', START_TIME);
	-- check it function(?!)
	-- SET @SqlCmd=CONCAT('DELETE FROM ',GT_COVMO,'.dim_handset_m_id WHERE MAKE_ID=''0'';');						
	
	-- -----------------------------------------------------------------------------------------
	SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS ',GT_COVMO,'.TMP_NEW_TAC;');
	PREPARE stmt FROM @sqlcmd;
	EXECUTE stmt;
	DEALLOCATE PREPARE stmt;
	
	SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS ',GT_COVMO,'.TMP_OLD_TAC;');
	PREPARE stmt FROM @sqlcmd;
	EXECUTE stmt;
	DEALLOCATE PREPARE stmt;
	
	SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE IF EXISTS ',GT_COVMO,'.TMP_SOURCE_TAC;');
	PREPARE stmt FROM @sqlcmd;
	EXECUTE stmt;
	DEALLOCATE PREPARE stmt;
	
	SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE  IF EXISTS ',GT_COVMO,'.TMP_dim_handset_m_id;');
	PREPARE stmt FROM @sqlcmd;
	EXECUTE stmt;
	DEALLOCATE PREPARE stmt;
	
	SET @SqlCmd=CONCAT('DROP TABLE  IF EXISTS ',GT_COVMO,'.TMP_dim_handset_id;');
	PREPARE stmt FROM @sqlcmd;
	EXECUTE stmt;
	DEALLOCATE PREPARE stmt;
	
	SET @SqlCmd=CONCAT('DROP TEMPORARY TABLE  IF EXISTS ',GT_COVMO,'.TMP_dim_handset_id_1;');
	PREPARE stmt FROM @sqlcmd;
	EXECUTE stmt;
	DEALLOCATE PREPARE stmt;
	
	SET @SqlCmd=CONCAT('DROP TABLE  IF EXISTS ',GT_COVMO,'.TMP_dim_handset;');
	PREPARE stmt FROM @sqlcmd;
	EXECUTE stmt;
	DEALLOCATE PREPARE stmt;
	-- -----------------------------------------------------------------------------------------
	-- fix dim_handset model_id
	SET @SqlCmd=CONCAT('SELECT COUNT(*) INTO @D_m_id FROM ',GT_COVMO,'.dim_handset A JOIN ',GT_COVMO,'.dim_handset_m_id B ON A.make_id=B.make_id WHERE A.model_id=B.model_id AND A.model<>B.model;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	IF @D_m_id > 0 THEN
		DROP TEMPORARY TABLE IF EXISTS  tmp_model_id;
		
		SET @SqlCmd=CONCAT('CREATE TEMPORARY TABLE tmp_model_id AS 
					SELECT A.model_id FROM ',GT_COVMO,'.`dim_handset` A	
					GROUP BY A.model_id HAVING COUNT(*)>1;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		
		CREATE INDEX ix_id ON tmp_model_id(model_id);
		-- 
		DROP  TEMPORARY TABLE IF EXISTS tmp_model;
		
		SET @SqlCmd=CONCAT('CREATE TEMPORARY TABLE tmp_model AS 
					SELECT DISTINCT A.make_id,A.model_id,A.model FROM ',GT_COVMO,'.dim_handset A 
					WHERE EXISTS 
					( SELECT NULL FROM tmp_model_id B WHERE  A.model_id = B.model_id );');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		
		CREATE INDEX ix_id ON tmp_model (make_id,model_id,model);
		--
		DROP  TEMPORARY TABLE IF EXISTS tmp_model_s;
		
		CREATE TEMPORARY TABLE tmp_model_s AS 
		SELECT  AA.make_id,AA.model_id,AA.model FROM tmp_model AA 
		GROUP BY  AA.make_id,AA.model_id HAVING COUNT(*) >1 ;
		
		CREATE INDEX ix_id ON tmp_model_s (make_id,model_id,model);
		--
		DROP  TEMPORARY TABLE IF EXISTS tmp_model_d;
		
		CREATE TEMPORARY TABLE tmp_model_d AS
		SELECT   A.make_id,A.model_id,A.model FROM tmp_model A
		WHERE EXISTS 
		  (SELECT NULL FROM tmp_model_s B WHERE A.model_id = B.model_id AND A.make_id = B.make_id AND A.model <> B.model)
		ORDER BY   A.make_id,A.model_id,A.model;
		
		CREATE INDEX ix_id ON tmp_model_d (make_id,model);
		--
		ALTER TABLE dim_handset ADD INDEX model (model);
		
		SET @SqlCmd=CONCAT('UPDATE ',GT_COVMO,'.dim_handset A, tmp_model_d B
					SET A.model_id=0
					WHERE A.make_id=B.make_id AND A.model=B.model;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		
		ALTER TABLE dim_handset DROP INDEX model;
		--
		DROP  TEMPORARY TABLE IF EXISTS tmp_make_id;
		
		SET @SqlCmd=CONCAT('CREATE TEMPORARY TABLE tmp_make_id AS 
					SELECT DISTINCT(make_id)FROM ',GT_COVMO,'.dim_handset WHERE model_id=0;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		--
		DROP  TEMPORARY TABLE IF EXISTS tmp_max_model;
		
		SET @SqlCmd=CONCAT('CREATE TEMPORARY TABLE tmp_max_model AS 
					SELECT A.make_id,MAX(A.model_id) model_id FROM ',GT_COVMO,'.dim_handset A
					WHERE EXISTS 
					( SELECT NULL FROM tmp_make_id B WHERE  A.make_id = B.make_id)
					GROUP BY make_id;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		--
		DROP  TEMPORARY TABLE IF EXISTS max_model_id;
		
		CREATE TEMPORARY TABLE max_model_id
		(
			id SMALLINT(6) NOT NULL AUTO_INCREMENT,
			make_id SMALLINT(6) NOT NULL,
			model_id MEDIUMINT(9) NOT NULL,
			PRIMARY KEY (id)
		);
		
		INSERT INTO `max_model_id` (`make_id`,`model_id`)
		SELECT make_id,model_id FROM tmp_max_model;
		-- 
		SET @v_i=1;
		SET @j_i=1;
		SET @Quotient=1;
		SET @check_cnt=1;
		SET @v_R_Max =(SELECT COUNT(make_id) FROM max_model_id);
		WHILE @v_i <= @v_R_Max DO
			BEGIN
				SET @mak_id =(SELECT make_id FROM max_model_id WHERE id=@v_i);
				SET @SqlCmd=CONCAT('SET @v_T_Max = (SELECT COUNT(make_id) FROM ',GT_COVMO,'.dim_handset WHERE model_id=0 AND make_id=@mak_id);');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				SET @mod_id =(SELECT model_id FROM max_model_id WHERE make_id=@mak_id);
				WHILE @j_i <= @v_T_Max DO
					BEGIN
						SET @SqlCmd=CONCAT('UPDATE ',GT_COVMO,'.dim_handset
									SET model_id=@mod_id+@j_i
									WHERE model_id=0 AND make_id=@mak_id LIMIT 1;');
						PREPARE Stmt FROM @SqlCmd;
						EXECUTE Stmt;
						DEALLOCATE PREPARE Stmt;
						
						SET @j_i=@j_i+@Quotient;
						SET @check_cnt=@check_cnt+@Quotient;
					END;
				END WHILE;
				SET @j_i=1;
				SET @v_i=@v_i+@Quotient;
			END;
		END WHILE;
		--
		SET @SqlCmd=CONCAT('TRUNCATE TABLE ',GT_COVMO,'.`dim_handset_m_id`;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		
		SET @SqlCmd=CONCAT('INSERT INTO ',GT_COVMO,'.`dim_handset_m_id` (
					  `make_id`,
					  `model`,
					  `model_id`,
					  `full_name`)
					SELECT make_id,model,model_id,model
					FROM ',GT_COVMO,'.dim_handset
					GROUP BY make_id,model,model_id;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
	END IF;
	
	IF @Source > 0 THEN
		SET @SqlCmd=CONCAT('UPDATE ',GT_COVMO,'.sys_config
					SET att_value=0, att_unit=''',NOW(),'''
					WHERE group_name=''HeartBeat'' AND att_name=''tac_sp'';');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
	ELSE
		SET @SqlCmd=CONCAT('UPDATE ',GT_COVMO,'.sys_config
					SET att_value=0
					WHERE group_name=''HeartBeat'' AND att_name=''tac_sp'';');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
	END IF;
	
	INSERT INTO gt_gw_main.SP_LOG VALUES(GT_COVMO,'SP_Update_TAC',CONCAT('Done: ',TIMESTAMPDIFF(SECOND,START_TIME,SYSDATE()),' seconds.'), NOW());
END$$
DELIMITER ;
