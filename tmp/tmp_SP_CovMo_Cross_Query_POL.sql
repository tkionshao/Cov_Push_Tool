CREATE DEFINER=`covmo`@`%` PROCEDURE `SP_CovMo_Cross_Query_POL`(IN PU_ID VARCHAR(1000),IN START_DATE DATE,IN END_DATE DATE,IN sql_str VARCHAR(10000),IN V_WORK_ID INT(11),TECH_MASK TINYINT(4),IN PLOYGON_ID MEDIUMINT(9),IN IMSI_GID MEDIUMINT(9))
a_label:
BEGIN
	DECLARE done INT DEFAULT 0;  
	DECLARE UNION_STR VARCHAR(20000) DEFAULT '';
	DECLARE GT_DB VARCHAR(100) DEFAULT 'gt_temp_cache';
	DECLARE V_SESSIONDB VARCHAR(100);
	DECLARE CCQ_TABLE VARCHAR(100) DEFAULT 'rpt_ccq';
	DECLARE WORKER_ID VARCHAR(10) DEFAULT CONNECTION_ID();
	DECLARE i INT DEFAULT 0;
	DECLARE v_1 INT DEFAULT 0;
	DECLARE org_str VARCHAR(10000) DEFAULT '';
	DECLARE dis_str VARCHAR(10000) DEFAULT '';
	DECLARE mrg_str VARCHAR(10000) DEFAULT '';
	DECLARE v_schema VARCHAR(10000) DEFAULT '';
	DECLARE v_select VARCHAR(10000) DEFAULT '';
	DECLARE v_new_schema VARCHAR(10000) DEFAULT '';
	DECLARE v_source_table VARCHAR(100) DEFAULT '';
	DECLARE V_CNT INT DEFAULT 0;
	DECLARE PU_ALL VARCHAR(1000) DEFAULT '';
	DECLARE ALLPU TINYINT(2) DEFAULT 0;
	DECLARE SESSION_NAME VARCHAR(100);
	DECLARE COL_STR VARCHAR(2048);
	
	DECLARE SP_ERROR CONDITION FOR SQLSTATE '99996';
	DECLARE KPI_ERROR CONDITION FOR SQLSTATE '99999';	
	DECLARE EXIT HANDLER FOR 1064 
	BEGIN
		SET @v_i=1;	
		SET @j_i=1;
			
		WHILE @v_i <= @v_R_Max DO
			BEGIN
				WHILE @j_i <= @v_T_Max DO
					BEGIN
						SET SESSION_NAME = CONCAT('gt_',gt_strtok(gt_covmo_csv_get(PU_ALL,@v_i),1,'|'),'_',DATE_FORMAT(@START_DATE_J,'%Y%m%d'),'_0000_0000');
						SET @START_DATE_J=DATE_ADD(@START_DATE_J,INTERVAL 1 DAY);
						
						SET @SqlCmd=CONCAT('SELECT COUNT(*) INTO @SESSION_CNT FROM `gt_covmo`.`session_information` WHERE `SESSION_DB` = ''', SESSION_NAME,''';');
						PREPARE Stmt FROM @SqlCmd;
						EXECUTE Stmt;
						DEALLOCATE PREPARE Stmt;
						
						IF @SESSION_CNT>0 THEN 
							SET @SqlCmd=CONCAT('DROP TABLE IF EXISTS ',GT_DB,'.',CCQ_TABLE,'_',REPLACE(SESSION_NAME,'_0000_0000',''),'_',gt_strtok(gt_covmo_csv_get(PU_ALL,@v_i),2,'|'),';');
							PREPARE Stmt FROM @SqlCmd;
							EXECUTE Stmt;
							DEALLOCATE PREPARE Stmt;
						END IF;
						SET @j_i=@j_i+@Quotient_j;
					END;
				END WHILE;			
				SET @START_DATE_J=START_DATE;
				SET @Quotient_j=1;
				SET @j_i=1;
				SET @v_i=@v_i+@Quotient_v;
			END;
		END WHILE;
	END;
	
   	CALL gt_gw_main.`SP_Sub_Set_Session_Param_LTE`('');
	
	INSERT INTO gt_gw_main.sp_log VALUES('gt_gw_main','SP_CovMo_Cross_Query','Cross Query Start', NOW());
	IF PU_ID='' THEN 
		SET @SqlCmd=CONCAT('SELECT GROUP_CONCAT(DISTINCT CONCAT(A.RNC,''|'',B.`TECHNOLOGY`) SEPARATOR '','' ) INTO @PU_GC 
					FROM `gt_covmo`.`session_information` A,`gt_covmo`.`rnc_information` B
					WHERE `SESSION_START`>=''',START_DATE,''' AND `SESSION_END` <''',DATE_ADD(END_DATE,INTERVAL 1 DAY),'''
					AND A.`RNC`=B.`RNC`',CASE TECH_MASK WHEN 1 THEN ' AND B.`TECHNOLOGY`=''GSM''' WHEN 2 THEN ' AND B.`TECHNOLOGY`=''UMTS''' WHEN 4 THEN ' AND B.`TECHNOLOGY`=''LTE''' ELSE '' END  ,';');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		SET PU_ALL=IFNULL(@PU_GC,'');
		SET ALLPU=1;
	ELSE 
		SET @SqlCmd=CONCAT('SELECT GROUP_CONCAT(DISTINCT CONCAT(A.RNC,''|'',B.`TECHNOLOGY`) SEPARATOR '','' ) INTO @PU_GC 
					FROM `gt_covmo`.`session_information` A,`gt_covmo`.`rnc_information` B
					WHERE `SESSION_START`>=''',START_DATE,''' AND `SESSION_END` <''',DATE_ADD(END_DATE,INTERVAL 1 DAY),'''
					AND A.`RNC`=B.`RNC`',CASE TECH_MASK WHEN 1 THEN ' AND B.`TECHNOLOGY`=''GSM''' WHEN 2 THEN ' AND B.`TECHNOLOGY`=''UMTS''' WHEN 4 THEN ' AND B.`TECHNOLOGY`=''LTE''' ELSE '' END ,' 
					AND A.`RNC`',CASE WHEN gt_covmo_csv_count(PU_ID,',')>1 THEN CONCAT(' IN (',PU_ID,')') ELSE CONCAT('=',PU_ID) END,';');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		SET PU_ALL=IFNULL(@PU_GC,'');
		SET ALLPU=0;
	END IF;	
	IF IFNULL(@PU_GC,0)=0 THEN 
 		SELECT 'No Data available!' AS NoSessionAvailable;
		LEAVE a_label;
	ELSE
		SET @col_str_avg='';
		SET @col_str_col='';
		SET @col_str_dis='';
		SET @col_str_mrg='';
		SET @col_str_from_dis='';
		SET @col_str_from_mrg='';
			
		SET CCQ_TABLE:=CONCAT(CCQ_TABLE,'_',WORKER_ID);
		SET @SqlCmd=CONCAT('CREATE DATABASE IF NOT EXISTS ',GT_DB);    
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;  
		
		SET v_source_table =TRIM(SUBSTRING_INDEX(TRIM(SUBSTRING_INDEX(sql_str,'FROM ',-1)),' AS ',1));	
		
		SET @col_str=TRIM(SUBSTRING_INDEX(TRIM(SUBSTRING_INDEX(sql_str,'FROM ',1)),'SELECT ',-1));	
		
		SET @col_str_from=CONCAT(' FROM ',TRIM(SUBSTRING_INDEX(sql_str,'FROM ', -1)));	
		
		IF LOCATE('HAVING ',@col_str_from)>0 THEN 
			SET @col_str_from_dis=TRIM(SUBSTRING_INDEX(@col_str_from, 'HAVING ', 1));
		ELSE 
			SET @col_str_from_dis=@col_str_from;
		END IF;
		IF LOCATE('ORDER ',@col_str_from_dis)>0 THEN 
			SET @col_str_from_dis=TRIM(SUBSTRING_INDEX(@col_str_from_dis, 'ORDER ', 1));
		END IF;
		IF LOCATE('WHERE ',@col_str_from)>0 THEN 
			IF LOCATE('GROUP ',@col_str_from)>0 THEN
			
			SET @col_str_from_mrg=CONCAT(TRIM(SUBSTRING_INDEX(@col_str_from, 'WHERE ', 1)),' GROUP '
						, TRIM(SUBSTRING_INDEX(@col_str_from, 'GROUP ', -1)));	
			ELSE 
				SET @col_str_from_mrg=CONCAT(TRIM(SUBSTRING_INDEX(@col_str_from, 'WHERE ', 1)),'');		
			END IF;
			
		ELSE 
			SET @col_str_from_mrg=@col_str_from;
		END IF;	
				
		SET v_schema='';
		SET v_select='';
		SET @col_cnt=(LENGTH(@col_str) - LENGTH(REPLACE(@col_str, '|', '')))+1;
		SET i=1;
		WHILE i <= @col_cnt DO
		BEGIN
			IF LOCATE('AVG(',gt_strtok(@col_str, i, '|'))>0  THEN
				BEGIN
					SET @col_str_avg=TRIM(SUBSTRING_INDEX(gt_strtok(@col_str, i, '|'),' AS',1));
					SET @col_str_col=TRIM(SUBSTRING_INDEX(gt_strtok(@col_str, i, '|'),' AS',-1));
					SET @col_str_org=gt_strtok(@col_str, i, '|');
					SET @col_str_dis=CONCAT(REPLACE(@col_str_avg,'AVG(','SUM('),' AS ',@col_str_col,'_num,',REPLACE(@col_str_avg,'AVG(','COUNT('),' AS ',@col_str_col,'_den',',',@col_str_org);
					SET @col_str_mrg=CONCAT('SUM(',@col_str_col,'_num)/','SUM(',@col_str_col,'_den) AS ',@col_str_col);
					SET v_schema =CONCAT(v_schema,@col_str_col,'_num' ,' VARCHAR(100),');
					SET v_schema =CONCAT(v_schema,@col_str_col,'_den' ,' VARCHAR(100),');
					SET v_schema =CONCAT(v_schema,@col_str_col ,' VARCHAR(100),');
					SET v_select =CONCAT(v_select,@col_str_col,'_num,');
					SET v_select =CONCAT(v_select,@col_str_col,'_den,');
					SET v_select =CONCAT(v_select,@col_str_col ,',');
					
				END;
			ELSEIF gt_strtok(@col_str, i, '|') REGEXP '(POW(.*)/SUM(.*))'>0 AND LOCATE('_POWER_',gt_strtok(@col_str, i, '|'))=0 THEN
				BEGIN			   
					SET @col_str_source=TRIM(REPLACE(REPLACE(SUBSTRING_INDEX(gt_strtok(@col_str, i, '|'),' AS',1),'\r',''),'\n',''));
					
					IF LEFT(@col_str_source,1)='(' AND RIGHT(@col_str_source,1)=')' THEN
					  SET @col_str_source=SUBSTR(@col_str_source,2,LENGTH(@col_str_source)-2);
					END IF; 
					SET @col_str_sum_1=CONCAT(TRIM(SUBSTRING_INDEX(@col_str_source,')/',1)),')');
					SET @col_str_sum_1_LEFT=CONCAT(TRIM(SUBSTRING_INDEX(@col_str_sum_1,'),2',1)),')');
					SET @col_str_sum_1_RIGHT=TRIM(SUBSTRING_INDEX(@col_str_sum_1,'),',-1));
					SET @col_str_sum_1_NO_LEFT=CONCAT(TRIM(SUBSTRING_INDEX(@col_str_sum_1_LEFT,'POW(',-1)));
					SET @col_str_sum_2=TRIM(SUBSTRING_INDEX(@col_str_source,')/',-1));
	
					SET @col_str_col=TRIM(SUBSTRING_INDEX(gt_strtok(@col_str, i, '|'),' AS',-1));
					SET @col_str_org=gt_strtok(@col_str, i, '|');						
	
						SET @col_str_mrg=CONCAT('POW(SUM(',@col_str_col,'_num),',@col_str_sum_1_RIGHT,'/','SUM(',@col_str_col,'_den) AS ',@col_str_col);
						SET @col_str_dis=CONCAT(@col_str_sum_1_NO_LEFT,' AS ',@col_str_col,'_num,',@col_str_sum_2,' AS ',@col_str_col,'_den',',',@col_str_org);	
				
	
	
	
					SET v_schema =CONCAT(v_schema,@col_str_col,'_num' ,' VARCHAR(100),');
					SET v_schema =CONCAT(v_schema,@col_str_col,'_den' ,' VARCHAR(100),');
					SET v_schema =CONCAT(v_schema,@col_str_col ,' VARCHAR(100),');
					SET v_select =CONCAT(v_select,@col_str_col,'_num,');
					SET v_select =CONCAT(v_select,@col_str_col,'_den,');
					SET v_select =CONCAT(v_select,@col_str_col,',');
				END;
			ELSEIF gt_strtok(@col_str, i, '|') REGEXP '1\\-\\(SUM(.*)/SUM(.*))'>0  THEN
				BEGIN			   
					SET @col_str_source=TRIM(REPLACE(REPLACE(SUBSTRING_INDEX(gt_strtok(@col_str, i, '|'),' AS',1),'\r',''),'\n',''));
					
					IF LEFT(@col_str_source,1)='(' AND RIGHT(@col_str_source,1)=')' THEN
					  SET @col_str_source=SUBSTR(@col_str_source,2,LENGTH(@col_str_source)-2);
					END IF;
					SET @col_str_sum_1=REPLACE(TRIM(SUBSTRING_INDEX(@col_str_source,'/',1)),'1-(','');
					SET @col_str_sum_2=SUBSTRING(TRIM(SUBSTRING_INDEX(@col_str_source,'/',-1)),1,LENGTH(TRIM(SUBSTRING_INDEX(@col_str_source,'/',-1)))-1);
					SET @col_str_col=TRIM(SUBSTRING_INDEX(gt_strtok(@col_str, i, '|'),' AS',-1));
					SET @col_str_org=gt_strtok(@col_str, i, '|');						
					SET @col_str_dis=CONCAT(@col_str_sum_1,' AS ',@col_str_col,'_num,',@col_str_sum_2,' AS ',@col_str_col,'_den',',',@col_str_org);
					SET @col_str_mrg=CONCAT('1-(SUM(',@col_str_col,'_num)/','SUM(',@col_str_col,'_den)) AS ',@col_str_col);
					SET v_schema =CONCAT(v_schema,@col_str_col,'_num' ,' VARCHAR(100),');
					SET v_schema =CONCAT(v_schema,@col_str_col,'_den' ,' VARCHAR(100),');
					SET v_schema =CONCAT(v_schema,@col_str_col ,' VARCHAR(100),');
					SET v_select =CONCAT(v_select,@col_str_col,'_num,');
					SET v_select =CONCAT(v_select,@col_str_col,'_den,');
					SET v_select =CONCAT(v_select,@col_str_col,',');
				END;
			ELSEIF LOCATE('ROUND(',gt_strtok(@col_str, i, '|'))>0  THEN
				BEGIN			   
					SET @col_str_source=TRIM(REPLACE(REPLACE(SUBSTRING_INDEX(gt_strtok(@col_str, i, '|'),' AS',1),'\r',''),'\n',''));
					
					IF LEFT(@col_str_source,1)='(' AND RIGHT(@col_str_source,1)=')' THEN
					  SET @col_str_source=SUBSTR(@col_str_source,2,LENGTH(@col_str_source)-2);
					END IF;
					
					IF LOCATE('ROUND(SUM(',@col_str_source)>0 THEN 
						SET @col_str_sum_1=REPLACE(TRIM(SUBSTRING_INDEX(@col_str_source,'),',1)),'ROUND(SUM(','');
						SET @col_str_sum_2=TRIM(SUBSTRING_INDEX(@col_str_source,'),',-1));
						SET @col_str_col=TRIM(SUBSTRING_INDEX(gt_strtok(@col_str, i, '|'),' AS',-1));
						SET @col_str_org=gt_strtok(@col_str, i, '|');	
						
						SET @col_str_dis=CONCAT('SUM(',@col_str_sum_1,') AS ',@col_str_col);
						SET @col_str_mrg=CONCAT('ROUND(SUM(',@col_str_col,'),',@col_str_sum_2,' AS ',@col_str_col);
						SET v_schema =CONCAT(v_schema,@col_str_col,' VARCHAR(100),');
						SET v_select =CONCAT(v_select,@col_str_col,',');
	
					
					ELSEIF	LOCATE('ROUND((SUM',@col_str_source)>0 THEN 
						SET @col_str_sum_1=REPLACE(TRIM(SUBSTRING_INDEX(@col_str_source,'/',1)),'ROUND((','');
						SET @col_str_sum_2=CONCAT(TRIM(SUBSTRING_INDEX(SUBSTRING_INDEX(@col_str_source,'/',-1),'))',1)),')');
						SET @col_str_sum_3=TRIM(SUBSTRING_INDEX(SUBSTRING_INDEX(@col_str_source,'/',-1),'))',-1));
						SET @col_str_col=TRIM(SUBSTRING_INDEX(gt_strtok(@col_str, i, '|'),' AS',-1));
						SET @col_str_org=gt_strtok(@col_str, i, '|');						
						SET @col_str_dis=CONCAT(@col_str_sum_1,' AS ',@col_str_col,'_num,',@col_str_sum_2,' AS ',@col_str_col,'_den',',',@col_str_org);
						SET @col_str_mrg=CONCAT('ROUND((SUM(',@col_str_col,'_num)/','SUM(',@col_str_col,'_den))',@col_str_sum_3,' AS ',@col_str_col);
						SET v_schema =CONCAT(v_schema,@col_str_col,'_num' ,' VARCHAR(100),');
						SET v_schema =CONCAT(v_schema,@col_str_col,'_den' ,' VARCHAR(100),');
						SET v_schema =CONCAT(v_schema,@col_str_col ,' VARCHAR(100),');
						SET v_select =CONCAT(v_select,@col_str_col,'_num,');
						SET v_select =CONCAT(v_select,@col_str_col,'_den,');
						SET v_select =CONCAT(v_select,@col_str_col,',');
	
	
					
					ELSE
						SET @col_str_sum_1=REPLACE(TRIM(SUBSTRING_INDEX(@col_str_source,'/',1)),'ROUND((','');
						SET @col_str_sum_2=TRIM(SUBSTRING_INDEX(SUBSTRING_INDEX(@col_str_source,'/',-1),')',1));
						SET @col_str_sum_3=CONCAT(TRIM(SUBSTRING_INDEX(SUBSTRING_INDEX(@col_str_source,')',-2),')',1)),')');
						SET @col_str_col=TRIM(SUBSTRING_INDEX(gt_strtok(@col_str, i, '|'),' AS',-1));
						SET @col_str_org=gt_strtok(@col_str, i, '|');						
						SET @col_str_dis=CONCAT(@col_str_sum_1,' AS ',@col_str_col,'_num,',@col_str_sum_2,' AS ',@col_str_col,'_den',',',@col_str_org);
						SET @col_str_mrg=CONCAT('ROUND((',@col_str_col,'_num/',@col_str_col,'_den)',@col_str_sum_3,' AS ',@col_str_col);
						SET v_schema =CONCAT(v_schema,@col_str_col,'_num' ,' VARCHAR(100),');
						SET v_schema =CONCAT(v_schema,@col_str_col,'_den' ,' VARCHAR(100),');
						SET v_schema =CONCAT(v_schema,@col_str_col ,' VARCHAR(100),');
						SET v_select =CONCAT(v_select,@col_str_col,'_num,');
						SET v_select =CONCAT(v_select,@col_str_col,'_den,');
						SET v_select =CONCAT(v_select,@col_str_col,',');
					END IF;
				END;
			ELSEIF gt_strtok(@col_str, i, '|') REGEXP 'SUM(.*)/SUM(.*)'>0  THEN
				BEGIN	   
					SET @col_str_source=TRIM(REPLACE(REPLACE(SUBSTRING_INDEX(gt_strtok(@col_str, i, '|'),' AS',1),'\r',''),'\n',''));
			
					IF LEFT(@col_str_source,1)='(' AND RIGHT(@col_str_source,1)=')' THEN
					  SET @col_str_source=SUBSTR(@col_str_source,2,LENGTH(@col_str_source)-2);
	
					END IF;	
	
					IF LOCATE('100*',gt_strtok(@col_str, i, '|')) >0  THEN
						SET @col_str_source=TRIM(SUBSTRING_INDEX(@col_str_source,'100*',-1));
						SET @col_str_100='100*';
					ELSEIF  LOCATE('*100',gt_strtok(@col_str, i, '|')) >0  THEN 
						SET @col_str_source=TRIM(SUBSTRING_INDEX(@col_str_source,'*100',1));
						SET @col_str_100='100*';
					ELSE 
						SET @col_str_100='';
					END IF;
					
					
					SET @col_str_sum_d=TRIM(SUBSTRING_INDEX(@col_str_source,')/',1));
					IF @col_str_sum_d REGEXP 'SUM(.*)-SUM(.*)'>0  THEN
						SET @col_str_sum_1_LEFT=TRIM(SUBSTRING_INDEX(@col_str_sum_d,'-',1));
						SET @col_str_sum_1_RIGHT=TRIM(SUBSTRING_INDEX(@col_str_sum_d,'-',-1)); 
						SET @col_str_sum_2=CONCAT(TRIM(SUBSTRING_INDEX(@col_str_source,')/',-1)),')');
						SET @col_str_col=TRIM(SUBSTRING_INDEX(gt_strtok(@col_str, i, '|'),' AS',-1));
						SET @col_str_org=gt_strtok(@col_str, i, '|');
						SET @col_str_mrg=CONCAT(@col_str_100,'SUM(',@col_str_col,'_num)/','SUM(',@col_str_col,'_den) AS ',@col_str_col);
						SET @col_str_dis=CONCAT(@col_str_sum_1_LEFT,'-',@col_str_sum_1_RIGHT,' AS ',@col_str_col,'_num,',@col_str_sum_2,' AS ',@col_str_col,'_den',',',@col_str_org);					
					ELSE
						
						SET @col_str_sum_1=CONCAT(TRIM(SUBSTRING_INDEX(@col_str_source,')/',1)),')');
						SET @col_str_sum_1_LEFT=TRIM(SUBSTRING_INDEX(@col_str_sum_1,'SUM(',1));
						SET @col_str_sum_1_NO_LEFT=CONCAT('SUM(',TRIM(SUBSTRING_INDEX(@col_str_sum_1,'(SUM(',-1)));
						SET @col_str_sum_2=TRIM(SUBSTRING_INDEX(@col_str_source,')/',-1));
						IF LOCATE('IFNULL(',@col_str_sum_1_LEFT)>0  THEN
							SET @col_str_sum_2_LEFT=LEFT(@col_str_sum_2,LENGTH(@col_str_sum_2)-3);
							SET @col_str_sum_2_RIGHT=RIGHT(@col_str_sum_2,3);
						ELSE 
							SET @col_str_sum_2_RIGHT=')';
							SET @col_str_sum_2_LEFT=LEFT(@col_str_sum_2,LENGTH(@col_str_sum_2)-1);
						END IF;
	
						SET @col_str_col=TRIM(SUBSTRING_INDEX(gt_strtok(@col_str, i, '|'),' AS',-1));
						SET @col_str_org=gt_strtok(@col_str, i, '|');						
						IF LENGTH(IFNULL(@col_str_sum_1_LEFT,''))=0 THEN
							SET @col_str_mrg=CONCAT(@col_str_100,'SUM(',@col_str_col,'_num)/','SUM(',@col_str_col,'_den) AS ',@col_str_col);
							SET @col_str_dis=CONCAT(@col_str_sum_1,' AS ',@col_str_col,'_num,',@col_str_sum_2,' AS ',@col_str_col,'_den',',',@col_str_org);	
						ELSE 
							SET @col_str_mrg=CONCAT(@col_str_100,@col_str_sum_1_LEFT,'SUM(',@col_str_col,'_num)/','SUM(',@col_str_col,'_den)',@col_str_sum_2_RIGHT,' AS ',@col_str_col);
							SET @col_str_dis=CONCAT(@col_str_sum_1_NO_LEFT,' AS ',@col_str_col,'_num,',@col_str_sum_2_LEFT,' AS ',@col_str_col,'_den',',',@col_str_org);
	END IF;
					END IF;
					
					SET v_schema =CONCAT(v_schema,@col_str_col,'_num' ,' VARCHAR(100),');
					SET v_schema =CONCAT(v_schema,@col_str_col,'_den' ,' VARCHAR(100),');
					SET v_schema =CONCAT(v_schema,@col_str_col ,' VARCHAR(100),');
					SET v_select =CONCAT(v_select,@col_str_col,'_num,');
					SET v_select =CONCAT(v_select,@col_str_col,'_den,');
					SET v_select =CONCAT(v_select,@col_str_col,',');
				END;
			ELSEIF gt_strtok(@col_str, i, '|') REGEXP 'SUM(.*)'>0  THEN			
					SET @col_str_source=TRIM(REPLACE(REPLACE(SUBSTRING_INDEX(gt_strtok(@col_str, i, '|'),' AS',1),'\r',''),'\n',''));
					IF LEFT(@col_str_source,1)='(' AND RIGHT(@col_str_source,1)=')' THEN
					  SET @col_str_source=SUBSTR(@col_str_source,2,LENGTH(@col_str_source)-2);
					END IF;
					SET @col_str_col=TRIM(SUBSTRING_INDEX(gt_strtok(@col_str, i, '|'),' AS',-1));
					SET @col_str_org=gt_strtok(@col_str, i, '|');
					SET @col_str_dis=CONCAT(@col_str_source,' AS ',@col_str_col);
					SET @col_str_mrg=CONCAT('SUM(',@col_str_col,') AS ',@col_str_col);
					SET v_schema =CONCAT(v_schema,@col_str_col,' VARCHAR(100),');
					SET v_select =CONCAT(v_select,@col_str_col,',');
			ELSEIF LOCATE('MAX(',gt_strtok(@col_str, i, '|'))>0  THEN			
					SET @col_str_source=TRIM(REPLACE(REPLACE(SUBSTRING_INDEX(gt_strtok(@col_str, i, '|'),' AS',1),'\r',''),'\n',''));
					IF LEFT(@col_str_source,1)='(' AND RIGHT(@col_str_source,1)=')' THEN
					  SET @col_str_source=SUBSTR(@col_str_source,2,LENGTH(@col_str_source)-2);
					END IF;
					SET @col_str_col=TRIM(SUBSTRING_INDEX(gt_strtok(@col_str, i, '|'),' AS',-1));
					SET @col_str_org=gt_strtok(@col_str, i, '|');
					SET @col_str_dis=CONCAT(@col_str_source,' AS ',@col_str_col);
					SET @col_str_mrg=CONCAT('MAX(',@col_str_col,') AS ',@col_str_col);
					SET v_schema =CONCAT(v_schema,@col_str_col,' VARCHAR(100),');
					SET v_select =CONCAT(v_select,@col_str_col,',');
			ELSE 
				BEGIN
					SET @col_str_col=TRIM(SUBSTRING_INDEX(gt_strtok(@col_str, i, '|'),' AS',-1));
					SET @col_str_org=gt_strtok(@col_str, i, '|');
					SET @col_str_dis=gt_strtok(@col_str, i, '|');
					
					IF LOCATE('COUNT(',gt_strtok(@col_str, i, '|'))>0 THEN 
						SET @col_str_mrg=CONCAT('SUM(',@col_str_col,') AS ',@col_str_col);
					ELSE SET @col_str_mrg=@col_str_col; 
					END IF;
					
					SET v_schema =CONCAT(v_schema,@col_str_col ,' VARCHAR(100),');
					SET v_select =CONCAT(v_select,@col_str_col ,',');
				END;
			END IF;
			
			IF i=1 THEN
				BEGIN
					SET org_str=CONCAT(@col_str_org);
					SET dis_str=CONCAT(@col_str_dis);
					SET mrg_str=CONCAT(@col_str_mrg);
				END;
			ELSE 
				BEGIN
					SET org_str=CONCAT(org_str,',',@col_str_org);
					SET dis_str=CONCAT(dis_str,',',@col_str_dis);
					SET mrg_str=CONCAT(mrg_str,',',@col_str_mrg);
				END;
			END IF; 
			SET @col_str_avg='';
			SET @col_str_col='';
			SET @col_str_org='';
			SET @col_str_dis='';
			SET @col_str_mrg='';
			SET i = i + 1;
		END;
		END WHILE;
		
		SET v_schema =CONCAT(' (',LEFT(v_schema,LENGTH(v_schema)-1),') ');
		SET v_select =CONCAT(' (',LEFT(v_select,LENGTH(v_select)-1),') ');
		CALL gt_schedule.sp_job_create('SP_CovMo_Cross_Query',GT_DB);
		SET @V_Multi_PU = @JOB_ID;	
		SET @v_i=1;
		SET @j_i=1;  
		SET @Quotient_v=1;
		SET @Quotient_j=1;	
		SET @START_DATE_J=START_DATE;
		SET @v_R_Max=gt_covmo_csv_count(PU_ALL,',');
		SET @v_T_Max=DATEDIFF(END_DATE,START_DATE)+1;
		
		WHILE @v_i <= @v_R_Max DO
		BEGIN
			WHILE @j_i <= @v_T_Max DO
				BEGIN
					SET SESSION_NAME = CONCAT('gt_',gt_strtok(gt_covmo_csv_get(PU_ALL,@v_i),1,'|'),'_',DATE_FORMAT(@START_DATE_J,'%Y%m%d'),'_0000_0000');
					SET @START_DATE_J=DATE_ADD(@START_DATE_J,INTERVAL 1 DAY)  ;
				
					SET @SqlCmd=CONCAT('SELECT COUNT(*) INTO @SESSION_CNT FROM `gt_covmo`.`session_information` WHERE `SESSION_DB` = ''', SESSION_NAME,''';');
					PREPARE Stmt FROM @SqlCmd;
					EXECUTE Stmt;
					DEALLOCATE PREPARE Stmt;
					
					IF @SESSION_CNT>0 THEN 
						SET @SqlCmd=CONCAT('DROP TABLE IF EXISTS ',GT_DB,'.',CCQ_TABLE,'_',REPLACE(SESSION_NAME,'_0000_0000',''),'_',gt_strtok(gt_covmo_csv_get(PU_ALL,@v_i),2,'|'),';');
						PREPARE Stmt FROM @SqlCmd;
						EXECUTE Stmt;
						DEALLOCATE PREPARE Stmt;
						 
						SET @SqlCmd=CONCAT('CREATE TABLE ',GT_DB,'.',CCQ_TABLE,'_',REPLACE(SESSION_NAME,'_0000_0000',''),'_',gt_strtok(gt_covmo_csv_get(PU_ALL,@v_i),2,'|'),v_schema,' ENGINE=MYISAM DEFAULT CHARSET=utf8;');
						PREPARE Stmt FROM @SqlCmd;
						EXECUTE Stmt;
						DEALLOCATE PREPARE Stmt;
												
						CALL gt_schedule.sp_job_add_task(CONCAT('CALL gt_gw_main.SP_CovMo_Cross_Query_Parallel(''',GT_DB,''',''',SESSION_NAME,''',''',v_source_table,''',''',CONCAT(CCQ_TABLE,'_',REPLACE(SESSION_NAME,'_0000_0000',''),'_',gt_strtok(gt_covmo_csv_get(PU_ALL,@v_i),2,'|')),''',''',CONCAT(' SELECT ',REPLACE(dis_str,"'","''"),' ',REPLACE(@col_str_from_dis,"'","''")),''',''',v_schema,''',''',WORKER_ID,''',''',v_select,''',''',gt_strtok(gt_covmo_csv_get(PU_ALL,@v_i),2,'|'),''',',PLOYGON_ID,',',IMSI_GID,');'),@V_Multi_PU); 
						IF (@v_i=1 AND @j_i=1) THEN 
							SET UNION_STR:=CONCAT(GT_DB,'.',CCQ_TABLE,'_',REPLACE(SESSION_NAME,'_0000_0000',''),'_',gt_strtok(gt_covmo_csv_get(PU_ALL,@v_i),2,'|'),',');
						ELSE 
							SET UNION_STR:=CONCAT(UNION_STR,GT_DB,'.',CCQ_TABLE,'_',REPLACE(SESSION_NAME,'_0000_0000',''),'_',gt_strtok(gt_covmo_csv_get(PU_ALL,@v_i),2,'|'),',');
						END IF; 
					END IF;
					SET @j_i=@j_i+@Quotient_j;
				END;
			END WHILE;
			SET @START_DATE_J=START_DATE;
			SET @Quotient_j=1;
			SET @j_i=1;
			SET @v_i=@v_i+@Quotient_v;
		END;
		END WHILE;
		SET UNION_STR =LEFT(UNION_STR,LENGTH(UNION_STR)-1);
		CALL gt_schedule.sp_job_enable_event();
		CALL gt_schedule.sp_job_start(@V_Multi_PU);
		CALL gt_schedule.sp_job_wait(@V_Multi_PU);
		CALL gt_schedule.sp_job_disable_event();
	 
		SET @SqlCmd=CONCAT('SELECT `STATUS` INTO @JOB_STATUS FROM `gt_schedule`.`job_history` WHERE ID=',@V_Multi_PU,';');
		PREPARE Stmt FROM @SqlCmd;
		EXECUTE Stmt;
		DEALLOCATE PREPARE Stmt;
		
		IF @JOB_STATUS='FINISHED' THEN 
			SET @SqlCmd=CONCAT('DROP TABLE IF EXISTS ',GT_DB,'.',CCQ_TABLE,';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
			
			SET @SqlCmd=CONCAT('CREATE TABLE ',GT_DB,'.',CCQ_TABLE,v_schema,' ENGINE=MRG_MYISAM DEFAULT CHARSET=utf8 INSERT_METHOD=FIRST UNION=(',UNION_STR,')');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt; 
			IF V_WORK_ID > 100 THEN
				SET @SqlCmd=REPLACE(CONCAT('SELECT SQL_CALC_FOUND_ROWS ',mrg_str,' ',@col_str_from_mrg,';'),v_source_table,CONCAT(GT_DB,'.',CCQ_TABLE));
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
				SELECT FOUND_ROWS() INTO @V_CNT;
				SET @SqlCmd=CONCAT('REPLACE INTO `gt_covmo`.`tbl_qry_totalcount`(`WORK_ID`,`QRY_TIME`,`TOTAL_CNT`)
							SELECT ',V_WORK_ID,' AS `WORK_ID`,''',NOW(),''' AS `QRY_TIME`,',@V_CNT,' AS `TOTAL_CNT`;');
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
			ELSE 
				SET @SqlCmd=REPLACE(CONCAT('SELECT ',mrg_str,' ',@col_str_from_mrg,';'),v_source_table,CONCAT(GT_DB,'.',CCQ_TABLE));
				PREPARE Stmt FROM @SqlCmd;
				EXECUTE Stmt;
				DEALLOCATE PREPARE Stmt;
			END IF;
			
			SET @SqlCmd=CONCAT('DROP TABLE IF EXISTS ',GT_DB,'.',CCQ_TABLE,';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
			
			SET @v_i=1;	
			SET @j_i=1;
				
			WHILE @v_i <= @v_R_Max DO
				BEGIN
					WHILE @j_i <= @v_T_Max DO
						BEGIN
							SET SESSION_NAME = CONCAT('gt_',gt_strtok(gt_covmo_csv_get(PU_ALL,@v_i),1,'|'),'_',DATE_FORMAT(@START_DATE_J,'%Y%m%d'),'_0000_0000');
							SET @START_DATE_J=DATE_ADD(@START_DATE_J,INTERVAL 1 DAY);
							SET @SqlCmd=CONCAT('SELECT COUNT(*) INTO @SESSION_CNT FROM `gt_covmo`.`session_information` WHERE `SESSION_DB` = ''', SESSION_NAME,''';');
							PREPARE Stmt FROM @SqlCmd;
							EXECUTE Stmt;
							DEALLOCATE PREPARE Stmt;
							
							IF @SESSION_CNT>0 THEN 
								SET @SqlCmd=CONCAT('DROP TABLE IF EXISTS ',GT_DB,'.',CCQ_TABLE,'_',REPLACE(SESSION_NAME,'_0000_0000',''),'_',gt_strtok(gt_covmo_csv_get(PU_ALL,@v_i),2,'|'),';');
								PREPARE Stmt FROM @SqlCmd;
								EXECUTE Stmt;
								DEALLOCATE PREPARE Stmt;
							END IF;
							SET @j_i=@j_i+@Quotient_j;
						END;
					END WHILE;			
					SET @START_DATE_J=START_DATE;
					SET @Quotient_j=1;
					SET @j_i=1;
					SET @v_i=@v_i+@Quotient_v;
				END;
			END WHILE;
		ELSE
			SET @v_i=1;	
			SET @j_i=1;
				
			WHILE @v_i <= @v_R_Max DO
				BEGIN
					WHILE @j_i <= @v_T_Max DO
						BEGIN
							SET SESSION_NAME = CONCAT('gt_',gt_strtok(gt_covmo_csv_get(PU_ALL,@v_i),1,'|'),'_',DATE_FORMAT(@START_DATE_J,'%Y%m%d'),'_0000_0000');
							SET @START_DATE_J=DATE_ADD(@START_DATE_J,INTERVAL 1 DAY);
							
							SET @SqlCmd=CONCAT('SELECT COUNT(*) INTO @SESSION_CNT FROM `gt_covmo`.`session_information` WHERE `SESSION_DB` = ''', SESSION_NAME,''';');
							PREPARE Stmt FROM @SqlCmd;
							EXECUTE Stmt;
							DEALLOCATE PREPARE Stmt;
							
							IF @SESSION_CNT>0 THEN 
								SET @SqlCmd=CONCAT('DROP TABLE IF EXISTS ',GT_DB,'.',CCQ_TABLE,'_',REPLACE(SESSION_NAME,'_0000_0000',''),'_',gt_strtok(gt_covmo_csv_get(PU_ALL,@v_i),2,'|'),';');
								PREPARE Stmt FROM @SqlCmd;
								EXECUTE Stmt;
								DEALLOCATE PREPARE Stmt;
							END IF;
							SET @j_i=@j_i+@Quotient_j;
						END;
					END WHILE;			
					SET @START_DATE_J=START_DATE;
					SET @Quotient_j=1;
					SET @j_i=1;
					SET @v_i=@v_i+@Quotient_v;
				END;
			END WHILE;
			
			INSERT INTO `gt_gw_main`.`tbl_rpt_error` (`PID`,`CreateTime`,`error_str`) VALUES (V_WORK_ID,NOW(),CONCAT(V_WORK_ID,' Main Parallel Jobs Fail - SP_CovMo_Cross_Query'));
			SIGNAL SP_ERROR
				SET MESSAGE_TEXT = 'Main Parallel Jobs Fail - SP_CovMo_Cross_Query';
		END IF;
	END IF;
	INSERT INTO gt_gw_main.sp_log VALUES('gt_gw_main','SP_CovMo_Cross_Query',CONCAT(V_WORK_ID,' Done: ',TIMESTAMPDIFF(SECOND,START_TIME,SYSDATE()),' seconds.'), NOW());
