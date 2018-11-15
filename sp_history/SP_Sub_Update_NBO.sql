DELIMITER $$
USE `gt_gw_main`$$
DROP PROCEDURE IF EXISTS `SP_CreateDB_LTE`$$
CREATE DEFINER=`covmo`@`%` PROCEDURE `SP_Sub_Update_NBO`(IN GT_DB VARCHAR(100))
BEGIN
	DECLARE START_TIME DATETIME DEFAULT SYSDATE();
	DECLARE CURRENT_NT_DB VARCHAR(50) DEFAULT CONCAT('gt_nt_',gt_strtok(GT_DB,3,'_'));
	
	INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'SP_Sub_Update_NBO','Start ', NOW());
	CALL SP_Sub_Set_Session_Param(GT_DB);
	
	
	
	SET @SqlCmd=CONCAT('DROP TABLE IF EXISTS `',GT_DB,'`.`tmp_opt_nbr_result`;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt; 
	
	SET @SqlCmd=CONCAT('DROP TABLE IF EXISTS `',GT_DB,'`.`tmp_opt_nbr_result_pri`;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt; 
	
	SET @SqlCmd=CONCAT('DROP TABLE IF EXISTS `',GT_DB,'`.`tmp_pri_sum`;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt; 
	
	SET @SqlCmd=CONCAT('DROP TABLE IF EXISTS `',GT_DB,'`.`tmp_nt_neighbor_all`;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt; 
	
			
	SET @SqlCmd=CONCAT('CREATE TABLE ',GT_DB,'.tmp_opt_nbr_result ENGINE=MYISAM
				SELECT
				  `RNC_ID`,`CELL_ID`,`NBR_RNC_ID`,`NBR_CELL_ID`,
				  SUM(IFNULL(`AS_TOTAL_COUNT`,0)) AS AS_TOTAL_COUNT,
				  SUM(IFNULL(`MS_RSCP_AVG`,0)) AS MS_RSCP_AVG,
				  SUM(IFNULL(`MS_ECN0_AVG`,0)) AS MS_ECN0_AVG,
				  SUM(IFNULL(`DISTANCE_METER`,0)) AS DISTANCE_METER,
				  SUM(IFNULL(`EVENT_COUNT`,0)) AS EVENT_COUNT
				FROM `',GT_DB,'`.`opt_nbr_result` --  FORCE INDEX(IX_RNC)
				GROUP BY `RNC_ID`,`CELL_ID`,`NBR_RNC_ID`,`NBR_CELL_ID`;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt; 
	
	
	SET @SqlCmd=CONCAT('CREATE TABLE ',GT_DB,'.`tmp_opt_nbr_result_pri` (
				  `RNC_ID` VARCHAR(10) DEFAULT NULL,
				  `CELL_ID` MEDIUMINT(9) DEFAULT NULL,
				  `NBR_RNC_ID` VARCHAR(10) DEFAULT NULL,
				  `NBR_CELL_ID` MEDIUMINT(9) DEFAULT NULL,
				  `RANK` INT(11) DEFAULT NULL,
				  `PRI_KIND` VARCHAR(15) NOT NULL DEFAULT ''''
				) ENGINE=MYISAM;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('INSERT INTO `',GT_DB,'`.`tmp_opt_nbr_result_pri`
				(	`RNC_ID`,
					`CELL_ID`,
					`NBR_RNC_ID`,
					`NBR_CELL_ID`,
					`RANK`,
					`PRI_KIND`)
				SELECT `RNC_ID`,`CELL_ID`,`NBR_RNC_ID`,`NBR_CELL_ID`,`RANK`,`PRI_KIND`
				FROM
				(
					SELECT `RNC_ID`,`CELL_ID`,`NBR_RNC_ID`,`NBR_CELL_ID`,`AS_TOTAL_COUNT`,
						@num := IF(@RNC_ID=RNC_ID AND @CELL_ID = CELL_ID , @num + 1, 1) AS `RANK`,
						@RNC_ID := RNC_ID AS dummy1,
						@CELL_ID := CELL_ID AS dummy2, 
						''PRI_AS'' AS `PRI_KIND`
					FROM ',GT_DB,'.tmp_opt_nbr_result  t ,(SELECT @num := 0) r,(SELECT @RNC_ID:='''') s,(SELECT @CELL_ID:='''') w
					ORDER BY  `RNC_ID`,`CELL_ID`,`AS_TOTAL_COUNT` DESC ,`NBR_RNC_ID`,`NBR_CELL_ID` 
				) AA;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt; 
	
	SET @SqlCmd=CONCAT('INSERT INTO `',GT_DB,'`.`tmp_opt_nbr_result_pri`
				(
					`RNC_ID`,
					`CELL_ID`,
					`NBR_RNC_ID`,
					`NBR_CELL_ID`,
					`RANK`,
					`PRI_KIND`)
				SELECT `RNC_ID`,`CELL_ID`,`NBR_RNC_ID`,`NBR_CELL_ID`,`RANK`,`PRI_KIND`
				FROM
				(
					SELECT `RNC_ID`,`CELL_ID`,`NBR_RNC_ID`,`NBR_CELL_ID`,`EVENT_COUNT`,
						@num := IF(@RNC_ID=RNC_ID AND @CELL_ID = CELL_ID , @num + 1, 1) AS `RANK`,
						@RNC_ID := RNC_ID AS dummy1,
						@CELL_ID := CELL_ID AS dummy2, 
						''PRI_EVENT'' AS `PRI_KIND`
					FROM ',GT_DB,'.tmp_opt_nbr_result  t ,(SELECT @num := 0) r,(SELECT @RNC_ID:='''') s,(SELECT @CELL_ID:='''') w
					ORDER BY  `RNC_ID`,`CELL_ID`,`EVENT_COUNT` DESC,`NBR_RNC_ID`,`NBR_CELL_ID`  
				) AA;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt; 
	
	SET @SqlCmd=CONCAT('INSERT INTO `',GT_DB,'`.`tmp_opt_nbr_result_pri`
				(
					`RNC_ID`,
					`CELL_ID`,
					`NBR_RNC_ID`,
					`NBR_CELL_ID`,
					`RANK`,
					`PRI_KIND`)
				SELECT `RNC_ID`,`CELL_ID`,`NBR_RNC_ID`,`NBR_CELL_ID`,`RANK`,`PRI_KIND`
				FROM
				(
					SELECT `RNC_ID`,`CELL_ID`,`NBR_RNC_ID`,`NBR_CELL_ID`,`MS_RSCP_AVG`,
						@num := IF(@RNC_ID=RNC_ID AND @CELL_ID = CELL_ID , @num + 1, 1) AS `RANK`,
						@RNC_ID := RNC_ID AS dummy1,
						@CELL_ID := CELL_ID AS dummy2, 
						''PRI_RSCP'' AS `PRI_KIND`
					FROM ',GT_DB,'.tmp_opt_nbr_result  t ,(SELECT @num := 0) r,(SELECT @RNC_ID:='''') s,(SELECT @CELL_ID:='''') w
					ORDER BY  `RNC_ID`,`CELL_ID`,`MS_RSCP_AVG` DESC,`NBR_RNC_ID`,`NBR_CELL_ID`  
				) AA;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt; 
	
	SET @SqlCmd=CONCAT('INSERT INTO `',GT_DB,'`.`tmp_opt_nbr_result_pri`
				(
					`RNC_ID`,
					`CELL_ID`,
					`NBR_RNC_ID`,
					`NBR_CELL_ID`,
					`RANK`,
					`PRI_KIND`)
				SELECT `RNC_ID`,`CELL_ID`,`NBR_RNC_ID`,`NBR_CELL_ID`,`RANK`,`PRI_KIND`
				FROM
				(
					SELECT `RNC_ID`,`CELL_ID`,`NBR_RNC_ID`,`NBR_CELL_ID`,`MS_ECN0_AVG`,
						@num := IF(@RNC_ID=RNC_ID AND @CELL_ID = CELL_ID , @num + 1, 1) AS `RANK`,
						@RNC_ID := RNC_ID AS dummy1,
						@CELL_ID := CELL_ID AS dummy2, 
						''PRI_ECNO'' AS `PRI_KIND`
					FROM ',GT_DB,'.tmp_opt_nbr_result  t ,(SELECT @num := 0) r,(SELECT @RNC_ID:='''') s,(SELECT @CELL_ID:='''') w
					ORDER BY  `RNC_ID`,`CELL_ID`,`MS_ECN0_AVG` DESC ,`NBR_RNC_ID`,`NBR_CELL_ID` 
				) AA;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt; 
	
	SET @SqlCmd=CONCAT('INSERT INTO `',GT_DB,'`.`tmp_opt_nbr_result_pri`
				(
					`RNC_ID`,
					`CELL_ID`,
					`NBR_RNC_ID`,
					`NBR_CELL_ID`,
					`RANK`,
					`PRI_KIND`)
				SELECT `RNC_ID`,`CELL_ID`,`NBR_RNC_ID`,`NBR_CELL_ID`,`RANK`,`PRI_KIND`
				FROM
				(
					SELECT `RNC_ID`,`CELL_ID`,`NBR_RNC_ID`,`NBR_CELL_ID`,`DISTANCE_METER`,
						@num := IF(@RNC_ID=RNC_ID AND @CELL_ID = CELL_ID , @num + 1, 1) AS `RANK`,
						@RNC_ID := RNC_ID AS dummy1,
						@CELL_ID := CELL_ID AS dummy2, 
						''PRI_DISTANCE'' AS `PRI_KIND`
					FROM ',GT_DB,'.tmp_opt_nbr_result  t ,(SELECT @num := 0) r,(SELECT @RNC_ID:='''') s,(SELECT @CELL_ID:='''') w
					ORDER BY `RNC_ID`,`CELL_ID`,`DISTANCE_METER`,`NBR_RNC_ID`,`NBR_CELL_ID`
				) AA; ');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt; 
	
	SET @SqlCmd=CONCAT('INSERT INTO `',GT_DB,'`.`tmp_opt_nbr_result_pri`
				(
					`RNC_ID`,
					`CELL_ID`,
					`NBR_RNC_ID`,
					`NBR_CELL_ID`,
					`RANK`,
					`PRI_KIND`)
			    SELECT 
				   A.`RNC_ID`,
				   A.`CELL_ID`,
				   A.`NBR_RNC_ID`,
				   A.`NBR_CELL_ID`,
				   (RANK+ PRI_MAX) AS `RANK`,
				   `PRI_KIND`
			    FROM
			    (
				SELECT `RNC_ID`,`CELL_ID`,`NBR_RNC_ID`,`NBR_CELL_ID`,`EVENT_COUNT`,
					@num := IF(@RNC_ID=RNC_ID AND @CELL_ID = CELL_ID , @num + 1, 1) AS `RANK`,
					@RNC_ID := RNC_ID AS dummy1,
					@CELL_ID := CELL_ID AS dummy2, 
					''PRI_PRIORITY'' AS `PRI_KIND`
				FROM ',GT_DB,'.opt_nbr_result  t 
				,(SELECT @num := 0) r,(SELECT @RNC_ID:='''') s,(SELECT @CELL_ID:='''') w
				WHERE `CM_NBR_STATE`=0 	 
				ORDER BY `RNC_ID`,`CELL_ID`,`EVENT_COUNT` DESC ,`NBR_RNC_ID`,`NBR_CELL_ID`  
			    ) A
			    ,(SELECT RNC_ID,CELL_ID,MAX(PRIORITY) AS PRI_MAX FROM ',CURRENT_NT_DB,'.nt_neighbor_current
					WHERE `NBR_TYPE`=1 GROUP BY RNC_ID,CELL_ID) B
			    WHERE A.RNC_ID=B.RNC_ID AND A.CELL_ID=B.CELL_ID
			    ; ');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt; 
	
	SET @SqlCmd=CONCAT('CREATE INDEX IX_RNC_CELL ON ',GT_DB,'.tmp_opt_nbr_result_pri(`RNC_ID`,`CELL_ID`, `NBR_RNC_ID`, `NBR_CELL_ID`);');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt; 
	
	SET @SqlCmd=CONCAT('	CREATE TABLE ',GT_DB,'.tmp_pri_sum ENGINE=MYISAM
				SELECT  RNC_ID,CELL_ID,NBR_RNC_ID,NBR_CELL_ID,
					SUM(CASE WHEN PRI_KIND=''PRI_AS'' THEN RANK ELSE 0 END) AS PRI_AS, 
					SUM(CASE WHEN PRI_KIND=''PRI_EVENT'' THEN RANK ELSE 0 END) AS PRI_EVENT,  
					SUM(CASE WHEN PRI_KIND=''PRI_RSCP'' THEN RANK ELSE 0 END) AS PRI_RSCP,  
					SUM(CASE WHEN PRI_KIND=''PRI_ECNO'' THEN RANK ELSE 0 END) AS PRI_ECNO ,  
					SUM(CASE WHEN PRI_KIND=''PRI_DISTANCE'' THEN RANK ELSE 0 END) AS PRI_DISTANCE  ,  
					SUM(CASE WHEN PRI_KIND=''PRI_PRIORITY'' THEN RANK ELSE 0 END) AS PRI_PRIORITY  
				FROM ',GT_DB,'.tmp_opt_nbr_result_pri FORCE INDEX(IX_RNC_CELL)
				GROUP BY RNC_ID,CELL_ID,NBR_RNC_ID,NBR_CELL_ID ; ');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt; 
	
	SET @SqlCmd=CONCAT('CREATE INDEX IX_RNC_CELL ON ',GT_DB,'.tmp_pri_sum(`RNC_ID`,`CELL_ID`, `NBR_RNC_ID`, `NBR_CELL_ID`);');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt; 
	
	SET @SqlCmd=CONCAT('UPDATE ',GT_DB,'.opt_nbr_result A,
				',GT_DB,'.tmp_pri_sum B 
				SET A.`PRI_PRIORITY` = B.PRI_PRIORITY,
				  A.`PRI_AS` = B.PRI_AS,
				  A.`PRI_EVENT` = B.PRI_EVENT,
				  A.`PRI_RSCP` = B.PRI_RSCP,
				  A.`PRI_ECNO` = B.PRI_ECNO,
				  A.`PRI_DISTANCE` = B.PRI_DISTANCE
				WHERE 
				    A.`RNC_ID` = B.RNC_ID
				    AND A.`CELL_ID` = B.CELL_ID
				    AND A.`NBR_RNC_ID` = B.NBR_RNC_ID
				    AND A.`NBR_CELL_ID` = B.NBR_CELL_ID;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd=CONCAT('UPDATE ',GT_DB,'.opt_nbr_result A, ',CURRENT_NT_DB,'.nt_neighbor_current B
				SET A.PRI_PRIORITY=B.PRIORITY
				WHERE A.CM_NBR_STATE=1 AND B.`NBR_TYPE`=1
				AND A.RNC_ID=B.RNC_ID
				AND A.CELL_ID=B.CELL_ID
				AND A.NBR_RNC_ID=B.NBR_RNC_ID
				AND A.NBR_CELL_ID=B.NBR_CELL_ID	;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt; 
	
	SET @SqlCmd=CONCAT('CREATE TABLE ',GT_DB,'.tmp_nt_neighbor_all ENGINE=MYISAM
				SELECT RNC_ID,CELL_ID,MAX(PRIORITY) AS PRI_MAX FROM ',CURRENT_NT_DB,'.nt_neighbor_current 
				WHERE `NBR_TYPE`=1 GROUP BY RNC_ID,CELL_ID;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt; 
	
	SET @SqlCmd=CONCAT('CREATE INDEX IX_RNC_CELL ON ',GT_DB,'.tmp_nt_neighbor_all(`RNC_ID`,`CELL_ID`,`PRI_MAX`);');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt; 
	
	SET @SqlCmd =CONCAT('SELECT COUNT(*) INTO @INDEX_FLAG  FROM INFORMATION_SCHEMA.STATISTICS 
				WHERE table_schema =  ''',GT_DB, ''' 
				AND table_name = ''opt_nbr_result'' AND index_name = ''IX_RNC_CELL'''); 
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	IF @INDEX_FLAG=0 THEN 
		SET @SqlCmd=CONCAT('CREATE INDEX IX_RNC_CELL ON ',GT_DB,'.opt_nbr_result(`CM_NBR_STATE`, `RNC_ID`,`CELL_ID`, `NBR_RNC_ID`, `NBR_CELL_ID`,`PRI_PRIORITY`);');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt; 
	END IF ;
		
	SET @SqlCmd=CONCAT('UPDATE ',GT_DB,'.opt_nbr_result A FORCE INDEX(IX_RNC_CELL)
				,',GT_DB,'.tmp_nt_neighbor_all B  FORCE INDEX(IX_RNC_CELL)
				SET A.PRI_PRIORITY=B.PRI_MAX
				WHERE A.CM_NBR_STATE=1 AND A.RNC_ID=B.RNC_ID
				AND A.CELL_ID=B.CELL_ID 
				AND A.PRI_PRIORITY=0;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt; 
	
	SET @SqlCmd=CONCAT('DROP TABLE ',GT_DB,'.tmp_opt_nbr_result;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt; 
	
	SET @SqlCmd=CONCAT('DROP TABLE ',GT_DB,'.tmp_opt_nbr_result_pri;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt; 
	
	SET @SqlCmd=CONCAT('DROP TABLE ',GT_DB,'.tmp_pri_sum;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt; 
	
	SET @SqlCmd=CONCAT('DROP TABLE ',GT_DB,'.tmp_nt_neighbor_all;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt; 
	
	CALL Sp_Sub_Generate_NBO_Report(GT_DB);
	
	INSERT INTO gt_gw_main.sp_log VALUES(GT_DB,'SP_Sub_Update_NBO',CONCAT('Done: ',TIMESTAMPDIFF(SECOND,START_TIME,SYSDATE()),' seconds.'), NOW());
	
END$$
DELIMITER ;
