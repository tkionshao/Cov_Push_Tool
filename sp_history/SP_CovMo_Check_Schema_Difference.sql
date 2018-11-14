DELIMITER $$
USE `gt_gw_main`$$
DROP PROCEDURE IF EXISTS `SP_CreateDB_LTE`$$
CREATE DEFINER=`covmo`@`%` PROCEDURE `SP_CovMo_Check_Schema_Difference`(IN org_db VARCHAR(100),IN new_db VARCHAR(100))
BEGIN
	
	DECLARE rowCountaltercolumn INT DEFAULT 0;
	DECLARE rowCountaddtable INT DEFAULT 0;
	DECLARE rowCountdroptable INT DEFAULT 0;
	DECLARE excludeTable VARCHAR(1024) DEFAULT '';
	DECLARE v_i INT DEFAULT 0;
	
	DECLARE Altercolumn CURSOR FOR SELECT DISTINCT CONCAT('`',table_name,'`') AS table_name
					FROM
					(
					    SELECT
						table_name,column_name,ordinal_position,
						column_type ,is_nullable,COLUMN_KEY,COUNT(1) rowcount
					    FROM information_schema.COLUMNS
					    WHERE  (table_schema=org_db OR table_schema=new_db)
					    GROUP BY table_name,column_name,column_type,is_nullable,COLUMN_KEY
					    HAVING COUNT(1)=1
					) A
					WHERE EXISTS 
					(
					SELECT NULL FROM information_schema.TABLES B
					WHERE B.table_schema=new_db AND B.table_name=A.table_name AND B.TABLE_TYPE='BASE TABLE'
					)
					ORDER BY table_name;
	
	DECLARE Addtable CURSOR FOR SELECT A.table_name 
					FROM 
					(SELECT table_name FROM information_schema.TABLES WHERE table_schema=new_db AND TABLE_TYPE='BASE TABLE') A
					LEFT JOIN 
					(SELECT table_name FROM information_schema.TABLES WHERE table_schema=org_db AND TABLE_TYPE='BASE TABLE') B
					ON A.table_name=B.table_name
					WHERE B.table_name IS NULL;
	
	DECLARE Droptable CURSOR FOR SELECT A.table_name  
					FROM 
					(SELECT table_name FROM information_schema.TABLES WHERE table_schema=org_db AND TABLE_TYPE='BASE TABLE') A
					LEFT JOIN 
					(SELECT table_name FROM information_schema.TABLES WHERE table_schema=new_db AND TABLE_TYPE='BASE TABLE') B
					ON A.table_name=B.table_name
					WHERE B.table_name IS NULL;
	
	SET @@session.group_concat_max_len = @@global.max_allowed_packet;
			
	SET @SqlCmd =CONCAT('DROP TEMPORARY TABLE IF EXISTS ',org_db,'.tmp_table_command;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
			
	SET @SqlCmd =CONCAT('CREATE TEMPORARY TABLE ',org_db,'.tmp_table_command
				(
					COMMAND_id INT(6) NOT NULL AUTO_INCREMENT,
					COMMAND_STR varchar(2000),
					TBL_NAME VARCHAR(100),
					PRI_FLAG TINYINT(2),
					SORT_TYPE TINYINT(2),
					PRIMARY KEY (COMMAND_id)
				) ENGINE=MyISAM AUTO_INCREMENT=1 DEFAULT CHARSET=utf8
				;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
			
	SET @SqlCmd =CONCAT('DROP TEMPORARY TABLE IF EXISTS ',org_db,'.tmp_column_command;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
			
	SET @SqlCmd =CONCAT('CREATE TEMPORARY TABLE ',org_db,'.tmp_column_command
				(
					COMMAND_STR varchar(2000),
					TBL_NAME VARCHAR(100),
					PRI_FLAG TINYINT(2),
					SORT_TYPE TINYINT(2)
				) ENGINE=MyISAM DEFAULT CHARSET=utf8
				;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
			
	SET @SqlCmd =CONCAT('DROP TEMPORARY TABLE IF EXISTS ',org_db,'.tmp_column_group_command;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
			
	SET @SqlCmd =CONCAT('CREATE TEMPORARY TABLE ',org_db,'.tmp_column_group_command
				(
					COMMAND_id INT(6) NOT NULL AUTO_INCREMENT,
					COMMAND_STR varchar(2000),
					TBL_NAME VARCHAR(100),
					PRIMARY KEY (COMMAND_id)
				) ENGINE=MyISAM AUTO_INCREMENT=1 DEFAULT CHARSET=utf8
				;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	OPEN Addtable;
	BEGIN
		DECLARE exit_flag INT DEFAULT 0;
		DECLARE tbl_name VARCHAR(64);
		DECLARE CONTINUE HANDLER FOR NOT FOUND SET exit_flag = 1;
		AddtableLoop: LOOP
		FETCH Addtable INTO tbl_name;
		    IF exit_flag THEN LEAVE AddtableLoop; 
		    END IF;
			SET @SqlCmd =CONCAT('INSERT INTO ',org_db,'.tmp_table_command
						(COMMAND_STR,SORT_TYPE)
						VALUES ( ''',CONCAT('CREATE TABLE IF NOT EXISTS ',org_db,'.',tbl_name,' LIKE ',new_db,'.',tbl_name,';'),''',1);');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
			SET excludeTable=CONCAT(excludeTable,',`',tbl_name,'`');
			SET rowCountaddtable = rowCountaddtable + 1;
		END LOOP;
	END;
	CLOSE Addtable;	
	OPEN Droptable;
	BEGIN
		DECLARE exit_flag INT DEFAULT 0;
		DECLARE tbl_name VARCHAR(64);
		DECLARE CONTINUE HANDLER FOR NOT FOUND SET exit_flag = 1;
		DroptableLoop: LOOP
		FETCH Droptable INTO tbl_name;
		    IF exit_flag THEN LEAVE DroptableLoop; 
		    END IF;
			SET @SqlCmd =CONCAT('INSERT INTO ',org_db,'.tmp_table_command
						(COMMAND_STR,SORT_TYPE)
						VALUES ( ''',CONCAT('DROP TABLE IF EXISTS ',org_db,'.',tbl_name,';'),''',2);');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
			SET excludeTable=CONCAT(excludeTable,',`',tbl_name,'`');
			SET rowCountdroptable = rowCountdroptable + 1;
		END LOOP;
	END;
	CLOSE Droptable; 
	
	IF excludeTable<>'' THEN 
		SET excludeTable=RIGHT(excludeTable,LENGTH(excludeTable)-1);
	END IF;		
	
	OPEN Altercolumn;
	BEGIN
		DECLARE exit_flag INT DEFAULT 0;
		DECLARE tbl_name VARCHAR(64);
		DECLARE CONTINUE HANDLER FOR NOT FOUND SET exit_flag = 1;
		AltercolumnLoop: LOOP
		FETCH Altercolumn INTO tbl_name;
		    IF exit_flag THEN LEAVE AltercolumnLoop; 
		    END IF;
			IF LOCATE(tbl_name,excludeTable)=0 THEN 
	
				SET tbl_name = REPLACE(tbl_name,'`','');
					
				SET @SqlCmd =CONCAT('DROP TEMPORARY TABLE IF EXISTS ',org_db,'.tmp_new_column;');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				
				SET @SqlCmd =CONCAT('CREATE TEMPORARY TABLE ',org_db,'.tmp_new_column
							(
								TABLE_NAME VARCHAR(100),
								ORDINAL_POSITION SMALLINT(6),
								PRE_COLUMN_NAME VARCHAR(100),
								COLUMN_NAME VARCHAR(100),
								COLUMN_TYPE VARCHAR(500),
								COLUMN_DEFAULT VARCHAR(50),
								IS_NULLABLE VARCHAR(50),
								EXTRA VARCHAR(50)
							) ENGINE=MyISAM DEFAULT CHARSET=utf8;');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				
				SET @SqlCmd=CONCAT('INSERT INTO ',org_db,'.tmp_new_column
							(table_name,ordinal_position,pre_column_name,column_name,column_type,column_default,is_nullable,extra)
							SELECT B.table_name,ordinal_position,A.pre_column_name,B.column_name,column_type,column_default,is_nullable,extra 
							FROM 
							(SELECT table_name,column_name,ordinal_position,data_type,column_type,column_default,is_nullable,extra,COLUMN_KEY 
							FROM information_schema.columns
							WHERE table_schema=''',new_db,''' AND table_name=''',tbl_name,''') B
							LEFT JOIN 
							(SELECT table_name,column_name AS pre_column_name,ordinal_position+1 AS pre_ordinal_position
							FROM information_schema.columns
							WHERE table_schema=''',new_db,''' AND table_name=''',tbl_name,''') A
							ON  A.table_name=B.table_name AND A.pre_ordinal_position=B.ordinal_position  
							WHERE (SELECT COUNT(*) FROM information_schema.TABLES C WHERE C.table_schema=''',org_db,''' AND C.table_name=''',tbl_name,''' AND C.TABLE_TYPE=''BASE TABLE'') =1
							ORDER BY B.table_name,B.ordinal_position;');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				
				SET @SqlCmd =CONCAT('INSERT INTO ',org_db,'.tmp_column_command 
							(COMMAND_STR,TBL_NAME,PRI_FLAG,SORT_TYPE)
							SELECT CASE WHEN dd.cnt=2 THEN CONCAT('' MODIFY '',dd.new_column_name,'' '',dd.column_type,CASE dd.is_nullable WHEN ''NO'' THEN '' NOT NULL '' ELSE '' DEFAULT NULL '' END,'''',CASE WHEN dd.COLUMN_DEFAULT IN (''CURRENT_TIMESTAMP'') THEN CONCAT('' DEFAULT '',dd.COLUMN_DEFAULT,'' '') WHEN dd.COLUMN_DEFAULT <>'''' THEN CONCAT('' DEFAULT '''''',dd.COLUMN_DEFAULT,'''''''') ELSE '''' END,'''',dd.extra,'''')
							WHEN (dd.cnt=1 AND dd.new_column_name IS NULL) THEN CONCAT('' DROP COLUMN '',dd.old_column_name,'''')
							WHEN (dd.cnt=1 AND dd.new_column_name IS NOT NULL) THEN CONCAT('' ADD '',dd.new_column_name,'' '',dd.column_type,CASE dd.is_nullable WHEN ''NO'' THEN '' NOT NULL '' ELSE '' DEFAULT NULL '' END,'''',CASE WHEN IFNULL(dd.COLUMN_DEFAULT,'''') IN (''CURRENT_TIMESTAMP'') THEN CONCAT('' DEFAULT '',dd.COLUMN_DEFAULT,'' '') WHEN IFNULL(dd.COLUMN_DEFAULT,'''') <>'''' THEN CONCAT('' DEFAULT '''''',dd.COLUMN_DEFAULT,'''''''') ELSE '''' END,'''',IFNULL(dd.extra,''''),CASE WHEN dd.pre_column_name IS NOT NULL THEN CONCAT('' AFTER '',dd.pre_column_name) ELSE '''' END,'''')
							ELSE '''' END AS COMMAND_STR,
							dd.table_name AS TBL_NAME,
							dd.ordinal_position AS PRI_FLAG,
							CASE WHEN dd.cnt=2 THEN 3 WHEN (dd.cnt=1 AND dd.new_column_name IS NULL) THEN 5 WHEN (dd.cnt=1 AND dd.new_column_name IS NOT NULL) THEN 4 ELSE 0 END AS SORT_TYPE
							FROM 
							(
								SELECT distinct concat(''`'',bb.column_name,''`'') AS old_column_name,bb.cnt,''',tbl_name,''' AS table_name,concat(''`'',cc.column_name,''`'') AS new_column_name,cc.ordinal_position,CASE cc.column_type WHEN ''TIMESTAMP'' THEN CONCAT(cc.column_type,'' NULL'') ELSE cc.column_type END AS column_type,cc.is_nullable,cc.COLUMN_DEFAULT,cc.extra,concat(''`'',cc.pre_column_name,''`'') AS pre_column_name
								
								FROM 
								(
									SELECT column_name,SUM(rowcount) AS cnt
									FROM 
									(
										SELECT table_schema,table_name,column_name,ordinal_position,column_type ,is_nullable,extra,COUNT(1) rowcount        
										FROM information_schema.columns
										WHERE (table_schema=''',org_db,''' OR table_schema=''',new_db,''')
										AND TABLE_NAME=''',tbl_name,'''
										GROUP BY table_name,column_name,ordinal_position,column_type,is_nullable,extra
										HAVING COUNT(1)=1
									) aa
									GROUP BY column_name
								) bb
								LEFT JOIN ',org_db,'.tmp_new_column cc
								ON bb.column_name=cc.column_name
								order by cc.table_name,cc.ordinal_position
							) dd
							WHERE dd.CNT IS NOT NULL;');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt; 
				
				SET @SqlCmd =CONCAT('DROP TEMPORARY TABLE IF EXISTS ',org_db,'.tmp_new_column;');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
			END IF;
			SET rowCountaltercolumn = rowCountaltercolumn + 1;		
		END LOOP;
	END;
	CLOSE Altercolumn; 
	
	IF rowCountaltercolumn>0 THEN 
		SET @SqlCmd =CONCAT('INSERT INTO ',org_db,'.tmp_column_command 
					(COMMAND_STR,TBL_NAME,SORT_TYPE)
					SELECT CONCAT(CASE A.CONSTRAINT_TYPE WHEN ''PRIMARY KEY'' THEN '' DROP PRIMARY KEY'' ELSE CONCAT('' DROP INDEX `'',A.index_name,''`'') END) AS COMMAND_STR
					,A.table_name,6
					FROM 
					(
						SELECT s.table_name,s.index_name,GROUP_CONCAT(s.COLUMN_NAME ORDER BY SEQ_IN_INDEX) AS index_column,t.CONSTRAINT_TYPE 
						FROM (SELECT table_name,index_name,COLUMN_NAME,SEQ_IN_INDEX FROM INFORMATION_SCHEMA.STATISTICS WHERE table_schema=''',new_db,''') s
						LEFT JOIN (SELECT table_name,CONSTRAINT_NAME,CONSTRAINT_TYPE FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS WHERE table_schema=''',new_db,''') t
						ON t.TABLE_NAME=s.TABLE_NAME AND s.INDEX_NAME=t.CONSTRAINT_NAME 
						WHERE s.table_name IN 
						(
							SELECT A.table_name 
							FROM (SELECT table_name FROM information_schema.TABLES WHERE table_schema=''',new_db,''' AND TABLE_TYPE=''BASE TABLE'') A
							JOIN (SELECT table_name FROM information_schema.TABLES WHERE table_schema=''',org_db,''' AND TABLE_TYPE=''BASE TABLE'') B
							ON A.table_name=B.table_name
						)
						GROUP BY s.table_name,s.index_name) A
					JOIN 
					(
						SELECT s.table_name,s.index_name,GROUP_CONCAT(s.COLUMN_NAME ORDER BY SEQ_IN_INDEX) AS index_column,t.CONSTRAINT_TYPE 
						FROM (SELECT table_name,index_name,COLUMN_NAME,SEQ_IN_INDEX FROM INFORMATION_SCHEMA.STATISTICS WHERE table_schema=''',org_db,''') s
						LEFT JOIN (SELECT table_name,CONSTRAINT_NAME,CONSTRAINT_TYPE FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS WHERE table_schema=''',org_db,''') t
						ON t.TABLE_NAME=s.TABLE_NAME AND s.INDEX_NAME=t.CONSTRAINT_NAME 
						WHERE s.table_name IN 
						(
							SELECT A.table_name 
							FROM (SELECT table_name FROM information_schema.TABLES WHERE table_schema=''',new_db,''' AND TABLE_TYPE=''BASE TABLE'') A
							JOIN (SELECT table_name FROM information_schema.TABLES WHERE table_schema=''',org_db,''' AND TABLE_TYPE=''BASE TABLE'') B
							ON A.table_name=B.table_name
						)
						GROUP BY s.table_name,s.index_name) B
					ON A.table_name=B.table_name AND A.index_name=B.index_name
					WHERE A.index_column<>B.index_column;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		
		SET @SqlCmd =CONCAT('INSERT INTO ',org_db,'.tmp_column_command 
					(COMMAND_STR,TBL_NAME,SORT_TYPE)
					SELECT CONCAT(CASE A.CONSTRAINT_TYPE WHEN ''PRIMARY KEY'' THEN '' DROP PRIMARY KEY'' ELSE CONCAT('' DROP INDEX `'',A.index_name,''`'') END) AS COMMAND_STR
					,A.table_name,6 AS SORT_TYPE
					FROM 
					(
						SELECT s.table_name,s.index_name,GROUP_CONCAT(s.COLUMN_NAME ORDER BY SEQ_IN_INDEX) AS index_column,t.CONSTRAINT_TYPE 
						FROM (SELECT table_name,index_name,COLUMN_NAME,SEQ_IN_INDEX FROM INFORMATION_SCHEMA.STATISTICS WHERE table_schema=''',org_db,''') s
						LEFT JOIN (SELECT table_name,CONSTRAINT_NAME,CONSTRAINT_TYPE FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS WHERE table_schema=''',org_db,''') t
						ON t.TABLE_NAME=s.TABLE_NAME AND s.INDEX_NAME=t.CONSTRAINT_NAME 
						WHERE s.table_name IN 
						(
							SELECT A.table_name 
							FROM (SELECT table_name FROM information_schema.TABLES WHERE table_schema=''',new_db,''' AND TABLE_TYPE=''BASE TABLE'') A
							JOIN (SELECT table_name FROM information_schema.TABLES WHERE table_schema=''',org_db,''' AND TABLE_TYPE=''BASE TABLE'') B
							ON A.table_name=B.table_name
						)
						GROUP BY s.table_name,s.index_name) A
					LEFT JOIN 
					(
						SELECT s.table_name,s.index_name,GROUP_CONCAT(s.COLUMN_NAME ORDER BY SEQ_IN_INDEX) AS index_column,t.CONSTRAINT_TYPE 
						FROM (SELECT table_name,index_name,COLUMN_NAME,SEQ_IN_INDEX FROM INFORMATION_SCHEMA.STATISTICS WHERE table_schema=''',new_db,''') s
						LEFT JOIN (SELECT table_name,CONSTRAINT_NAME,CONSTRAINT_TYPE FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS WHERE table_schema=''',new_db,''') t
						ON t.TABLE_NAME=s.TABLE_NAME AND s.INDEX_NAME=t.CONSTRAINT_NAME 
						WHERE s.table_name IN 
						(
							SELECT A.table_name 
							FROM (SELECT table_name FROM information_schema.TABLES WHERE table_schema=''',new_db,''' AND TABLE_TYPE=''BASE TABLE'') A
							JOIN (SELECT table_name FROM information_schema.TABLES WHERE table_schema=''',org_db,''' AND TABLE_TYPE=''BASE TABLE'') B
							ON A.table_name=B.table_name
						)
						GROUP BY s.table_name,s.index_name) B
					ON A.table_name=B.table_name AND A.index_name=B.index_name
					WHERE B.table_name IS NULL AND B.index_name IS NULL;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		
		SET @SqlCmd =CONCAT('INSERT INTO ',org_db,'.tmp_column_command 
					(COMMAND_STR,TBL_NAME,SORT_TYPE)
					SELECT CONCAT(CONCAT(CASE A.CONSTRAINT_TYPE WHEN ''PRIMARY KEY'' THEN '' ADD PRIMARY KEY('' WHEN ''UNIQUE'' THEN  CONCAT('' ADD UNIQUE '',A.index_name,''('') ELSE CONCAT('' ADD INDEX '',A.index_name,''('') END,A.index_column,'')'')) AS COMMAND_STR,A.table_name,7 AS SORT_TYPE
					FROM 
					(
						SELECT s.table_name,s.index_name,GROUP_CONCAT(s.COLUMN_NAME ORDER BY SEQ_IN_INDEX) AS index_column,t.CONSTRAINT_TYPE 
						FROM (SELECT table_name,index_name,COLUMN_NAME,SEQ_IN_INDEX FROM INFORMATION_SCHEMA.STATISTICS WHERE table_schema=''',new_db,''') s
						LEFT JOIN (SELECT table_name,CONSTRAINT_NAME,CONSTRAINT_TYPE FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS WHERE table_schema=''',new_db,''') t
						ON t.TABLE_NAME=s.TABLE_NAME AND s.INDEX_NAME=t.CONSTRAINT_NAME 
						WHERE s.table_name IN 
						(
							SELECT A.table_name 
							FROM (SELECT table_name FROM information_schema.TABLES WHERE table_schema=''',new_db,''' AND TABLE_TYPE=''BASE TABLE'') A
							JOIN (SELECT table_name FROM information_schema.TABLES WHERE table_schema=''',org_db,''' AND TABLE_TYPE=''BASE TABLE'') B
							ON A.table_name=B.table_name
						)
						GROUP BY s.table_name,s.index_name) A
					LEFT JOIN 
					(
						SELECT s.table_name,s.index_name,GROUP_CONCAT(s.COLUMN_NAME ORDER BY SEQ_IN_INDEX) AS index_column,t.CONSTRAINT_TYPE
						FROM (SELECT table_name,index_name,COLUMN_NAME,SEQ_IN_INDEX FROM INFORMATION_SCHEMA.STATISTICS WHERE table_schema=''',org_db,''') s
						LEFT JOIN (SELECT table_name,CONSTRAINT_NAME,CONSTRAINT_TYPE FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS WHERE table_schema=''',org_db,''') t
						ON t.TABLE_NAME=s.TABLE_NAME AND s.INDEX_NAME=t.CONSTRAINT_NAME 
						WHERE s.table_name IN 
						(
							SELECT A.table_name 
							FROM (SELECT table_name FROM information_schema.TABLES WHERE table_schema=''',new_db,''' AND TABLE_TYPE=''BASE TABLE'') A
							JOIN (SELECT table_name FROM information_schema.TABLES WHERE table_schema=''',org_db,''' AND TABLE_TYPE=''BASE TABLE'') B
							ON A.table_name=B.table_name
						)
						GROUP BY s.table_name,s.index_name) B
					ON A.table_name=B.table_name AND A.index_name=B.index_name
					WHERE B.index_name IS NULL OR A.index_column<>B.index_column;');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
	END IF;	
		
	SET @col_cnt=0;
	SET @SqlCmd =CONCAT('SELECT MAX(COMMAND_ID) into @col_cnt FROM ',org_db,'.tmp_table_command;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET v_i=1;
	WHILE v_i <= @col_cnt DO
	BEGIN
		SET @PRI_FLAG=0;
	        SET @COMMAND_STR='';
	        SET @TBL_NAME = '';
	
		SET @SqlCmd =CONCAT('SELECT COMMAND_STR,TBL_NAME,PRI_FLAG INTO @COMMAND_STR,@TBL_NAME,@PRI_FLAG FROM ',org_db,'.tmp_table_command WHERE COMMAND_ID=',v_i,';');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
	
		IF TRIM(IFNULL(@COMMAND_STR,'')) <> '' THEN
			SELECT @COMMAND_STR AS query_str;
			SET @SqlCmd=@COMMAND_STR;
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
			INSERT INTO gt_gw_main.sp_log VALUES(org_db,'SP_CovMo_Check_Schema_Difference',@COMMAND_STR, NOW());
		END IF;
		SET v_i = v_i + 1;
	END;
	END WHILE; 
	
	SET @SqlCmd =CONCAT('INSERT INTO ',org_db,'.tmp_column_group_command 
				(COMMAND_STR,TBL_NAME)
				SELECT CONCAT(''ALTER TABLE ',org_db,'.'',TBL_NAME,'' '',GROUP_CONCAT(COMMAND_STR ORDER BY SORT_TYPE,PRI_FLAG),'';'') AS COMMAND_STR,TBL_NAME FROM ',org_db,'.tmp_column_command GROUP BY TBL_NAME;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;	
	
	SET @col_cnt=0;
	SET @SqlCmd =CONCAT('SELECT MAX(COMMAND_ID) into @col_cnt FROM ',org_db,'.tmp_column_group_command;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET v_i=1;
	WHILE v_i <= @col_cnt DO
	BEGIN
		SET @COMMAND_STR='';
	        SET @TBL_NAME = '';
	
		SET @SqlCmd =CONCAT('SELECT COMMAND_STR,TBL_NAME INTO @COMMAND_STR,@TBL_NAME FROM ',org_db,'.tmp_column_group_command WHERE COMMAND_ID=',v_i,';');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
	
		IF TRIM(IFNULL(@COMMAND_STR,'')) <> '' THEN
 			SELECT @COMMAND_STR AS query_str;
			SET @SqlCmd=@COMMAND_STR;
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
			INSERT INTO gt_gw_main.sp_log VALUES(org_db,'SP_CovMo_Check_Schema_Difference',@COMMAND_STR, NOW());
		END IF;
		SET v_i = v_i + 1;
	END;
	END WHILE; 
	
	SET @SqlCmd =CONCAT('DROP TEMPORARY TABLE IF EXISTS ',org_db,'.tmp_table_command;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd =CONCAT('DROP TEMPORARY TABLE IF EXISTS ',org_db,'.tmp_column_group_command;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
	SET @SqlCmd =CONCAT('DROP TEMPORARY TABLE IF EXISTS ',org_db,'.tmp_column_command;');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;	
	
	SET @SqlCmd =CONCAT('DROP DATABASE IF EXISTS ',new_db,';');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt;
	
END$$
DELIMITER ;
