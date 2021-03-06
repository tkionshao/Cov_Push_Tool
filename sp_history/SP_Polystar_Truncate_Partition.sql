DELIMITER $$
USE `gt_gw_main`$$
DROP PROCEDURE IF EXISTS `SP_CreateDB_LTE`$$
CREATE DEFINER=`covmo`@`%` PROCEDURE `SP_Polystar_Truncate_Partition`(IN TECH_MASK TINYINT(2),IN GT_DB VARCHAR(50),IN gt_polystar_db VARCHAR (20),TIME_TYPE TINYINT(4))
BEGIN
	DECLARE START_TIME DATETIME DEFAULT SYSDATE();
	DECLARE RNC_ID INT;
	DECLARE DATA_DATE VARCHAR(8);
	DECLARE DATA_QRT VARCHAR(4);
	DECLARE DATA_HOUR VARCHAR(4);
	DECLARE FOLDER_PATH VARCHAR(20);
	DECLARE DAILY_DB VARCHAR(25);
	DECLARE SH_EH VARCHAR(9) DEFAULT RIGHT(GT_DB,9);
	DECLARE CURRENT_NT_DB VARCHAR(50) DEFAULT CONCAT('gt_nt_',gt_strtok(GT_DB,3,'_'));
	DECLARE PARTITIONDY VARCHAR(5);
	DECLARE PARTITIONHR VARCHAR(5);
	DECLARE PARTITIONDYTMP VARCHAR(5);
	DECLARE DATA_DATE_END VARCHAR(20);
	DECLARE 60_DAY DATETIME ;
	DECLARE 90_DAY DATETIME ;
	
 	SELECT gt_strtok(GT_DB,2,'_') INTO RNC_ID;
 	SELECT gt_strtok(GT_DB,3,'_') INTO DATA_DATE;
 	SELECT gt_strtok(GT_DB,4,'_') INTO DATA_QRT;
	SELECT LEFT(DATA_QRT,2) INTO DATA_HOUR;
	
	SET DATA_DATE_END= CONCAT(DATE(DATA_DATE),' 23:59:59');
	SELECT DATE(SUBDATE(DATA_DATE,60)) INTO 60_DAY;
	SELECT DATE(SUBDATE(DATA_DATE,90)) INTO 90_DAY;
 
	INSERT INTO gt_gw_main.sp_log VALUES('gt_gw_main','SP_Polystar_Truncate_Partition',CONCAT('START TO TRUNCATE PARTITION REPORTS, TYPE:',TIME_TYPE,', DATA_DATE:',DATA_DATE,', DATA_HOUR:',DATA_HOUR,', TECH_MASK:',TECH_MASK,''),NOW());	
	
	SELECT CONCAT('d',LPAD((TO_DAYS(DATA_DATE) MOD 90),2,0)) INTO PARTITIONDY;
	SELECT LPAD((TO_DAYS(DATA_DATE) MOD 60),2,0) INTO PARTITIONDYTMP;
	SELECT CONCAT('h',PARTITIONDYTMP,'',DATA_HOUR,'') INTO PARTITIONHR;
	
	IF TECH_MASK IN (0,4) AND TIME_TYPE= 2 THEN
	
			SET @SqlCmd=CONCAT(' DELETE FROM ',gt_polystar_db,'.polystar_session_information
						WHERE START_DATE=''',60_DAY,'''						
						AND DATA_HOUR <> ''-1''						
						AND SESSION_TYPE=''DAY''
						;');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT(' DELETE FROM ',gt_polystar_db,'.polystar_session_information
						WHERE START_DATE=''',DATA_DATE,'''
						AND END_DATE=''',DATA_DATE_END,'''
						AND DATA_HOUR=',DATA_HOUR,'
						AND TECH_MASK=',TECH_MASK,'
						AND SESSION_TYPE=''DAY''
						;');			
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT('	ALTER  TABLE ',gt_polystar_db,'.rpt_family2_cell_def_lte_hr
						TRUNCATE PARTITION ',PARTITIONHR,';
							');			
					
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT('	ALTER  TABLE ',gt_polystar_db,'.rpt_family2_cell_lte_hr
						TRUNCATE PARTITION ',PARTITIONHR,';
							');					
					
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT('	ALTER  TABLE ',gt_polystar_db,'.rpt_family2_cell_tile_def_lte_hr
						TRUNCATE PARTITION ',PARTITIONHR,';
							');					
					
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT('	ALTER  TABLE ',gt_polystar_db,'.rpt_family2_cell_tile_lte_hr
						TRUNCATE PARTITION ',PARTITIONHR,';
							');					
					
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT('	ALTER  TABLE ',gt_polystar_db,'.rpt_family2_tile_def_lte_hr
						TRUNCATE PARTITION ',PARTITIONHR,';
							');					
					
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT('	ALTER  TABLE ',gt_polystar_db,'.rpt_family2_tile_lte_hr
						TRUNCATE PARTITION ',PARTITIONHR,';
							');					
					
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT('	ALTER  TABLE ',gt_polystar_db,'.rpt_family2_tile_max_lte_hr
						TRUNCATE PARTITION ',PARTITIONHR,';
							');					
					
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
		
			SET @SqlCmd=CONCAT('	ALTER  TABLE ',gt_polystar_db,'.rpt_family2_tile_median_lte_hr
						TRUNCATE PARTITION ',PARTITIONHR,';
							');					
					
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT('	ALTER  TABLE ',gt_polystar_db,'.rpt_family2_tile_min_lte_hr
						TRUNCATE PARTITION ',PARTITIONHR,';
							');					
					
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT('	ALTER  TABLE ',gt_polystar_db,'.rpt_family2_reg_2_lte_hr
						TRUNCATE PARTITION ',PARTITIONHR,';
							');					
					
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT('	ALTER  TABLE ',gt_polystar_db,'.rpt_family2_reg_3_lte_hr
						TRUNCATE PARTITION ',PARTITIONHR,';
							');					
					
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
		
			SET @SqlCmd=CONCAT('	ALTER  TABLE ',gt_polystar_db,'.rpt_type2_cell_tile_def_lte_hr
						TRUNCATE PARTITION ',PARTITIONHR,';
							');					
					
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT('	ALTER  TABLE ',gt_polystar_db,'.rpt_type2_cell_tile_lte_hr
						TRUNCATE PARTITION ',PARTITIONHR,';
							');					
					
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT('	ALTER  TABLE ',gt_polystar_db,'.rpt_type2_cell_def_lte_hr
						TRUNCATE PARTITION ',PARTITIONHR,';
							');					
					
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT('	ALTER  TABLE ',gt_polystar_db,'.rpt_type2_cell_lte_hr
						TRUNCATE PARTITION ',PARTITIONHR,';
							');					
					
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
			
			SET @SqlCmd=CONCAT('	ALTER  TABLE ',gt_polystar_db,'.rpt_type2_tile_def_lte_hr
						TRUNCATE PARTITION ',PARTITIONHR,';
							');					
					
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT('	ALTER  TABLE ',gt_polystar_db,'.rpt_type2_tile_lte_hr
						TRUNCATE PARTITION ',PARTITIONHR,';
							');					
					
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT('	ALTER  TABLE ',gt_polystar_db,'.rpt_type2_tile_max_lte_hr
						TRUNCATE PARTITION ',PARTITIONHR,';
							');					
					
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
		
			SET @SqlCmd=CONCAT('	ALTER  TABLE ',gt_polystar_db,'.rpt_type2_tile_median_lte_hr
						TRUNCATE PARTITION ',PARTITIONHR,';
							');					
					
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT('	ALTER  TABLE ',gt_polystar_db,'.rpt_type2_tile_min_lte_hr
						TRUNCATE PARTITION ',PARTITIONHR,';
							');					
					
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT('	ALTER  TABLE ',gt_polystar_db,'.rpt_type2_reg_2_lte_hr
						TRUNCATE PARTITION ',PARTITIONHR,';
							');					
					
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT('	ALTER  TABLE ',gt_polystar_db,'.rpt_type2_reg_3_lte_hr
						TRUNCATE PARTITION ',PARTITIONHR,';
							');					
					
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT('	ALTER  TABLE ',gt_polystar_db,'.rpt_host_cell_def_lte_hr
						TRUNCATE PARTITION ',PARTITIONHR,';
							');			
					
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT('	ALTER  TABLE ',gt_polystar_db,'.rpt_host_cell_lte_hr
						TRUNCATE PARTITION ',PARTITIONHR,';
							');					
					
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT('	ALTER  TABLE ',gt_polystar_db,'.rpt_host_cell_tile_def_lte_hr
						TRUNCATE PARTITION ',PARTITIONHR,';
							');					
					
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT('	ALTER  TABLE ',gt_polystar_db,'.rpt_host_cell_tile_lte_hr
						TRUNCATE PARTITION ',PARTITIONHR,';
							');					
					
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
				
			SET @SqlCmd=CONCAT('	ALTER  TABLE ',gt_polystar_db,'.rpt_host_tile_def_lte_hr
						TRUNCATE PARTITION ',PARTITIONHR,';
							');					
					
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT('	ALTER  TABLE ',gt_polystar_db,'.rpt_host_tile_lte_hr
						TRUNCATE PARTITION ',PARTITIONHR,';
							');					
					
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT('	ALTER  TABLE ',gt_polystar_db,'.rpt_host_tile_max_lte_hr
						TRUNCATE PARTITION ',PARTITIONHR,';
							');					
					
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
		
			SET @SqlCmd=CONCAT('	ALTER  TABLE ',gt_polystar_db,'.rpt_host_tile_median_lte_hr
						TRUNCATE PARTITION ',PARTITIONHR,';
							');					
					
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT('	ALTER  TABLE ',gt_polystar_db,'.rpt_host_tile_min_lte_hr
						TRUNCATE PARTITION ',PARTITIONHR,';
							');					
					
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT('	ALTER  TABLE ',gt_polystar_db,'.rpt_host_reg_2_lte_hr
						TRUNCATE PARTITION ',PARTITIONHR,';
							');					
					
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT('	ALTER  TABLE ',gt_polystar_db,'.rpt_host_reg_3_lte_hr
						TRUNCATE PARTITION ',PARTITIONHR,';
							');					
					
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT('	ALTER  TABLE ',gt_polystar_db,'.rpt_useragent_cell_def_lte_hr
						TRUNCATE PARTITION ',PARTITIONHR,';
							');			
					
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT('	ALTER  TABLE ',gt_polystar_db,'.rpt_useragent_cell_lte_hr
						TRUNCATE PARTITION ',PARTITIONHR,';
							');					
					
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT('	ALTER  TABLE ',gt_polystar_db,'.rpt_useragent_cell_tile_def_lte_hr
						TRUNCATE PARTITION ',PARTITIONHR,';
							');					
					
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT('	ALTER  TABLE ',gt_polystar_db,'.rpt_useragent_cell_tile_lte_hr
						TRUNCATE PARTITION ',PARTITIONHR,';
							');					
					
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
				
			SET @SqlCmd=CONCAT('	ALTER  TABLE ',gt_polystar_db,'.rpt_useragent_tile_def_lte_hr
						TRUNCATE PARTITION ',PARTITIONHR,';
							');					
					
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT('	ALTER  TABLE ',gt_polystar_db,'.rpt_useragent_tile_lte_hr
						TRUNCATE PARTITION ',PARTITIONHR,';
							');					
					
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT('	ALTER  TABLE ',gt_polystar_db,'.rpt_useragent_tile_max_lte_hr
						TRUNCATE PARTITION ',PARTITIONHR,';
							');					
					
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
		
			SET @SqlCmd=CONCAT('	ALTER  TABLE ',gt_polystar_db,'.rpt_useragent_tile_median_lte_hr
						TRUNCATE PARTITION ',PARTITIONHR,';
							');					
					
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT('	ALTER  TABLE ',gt_polystar_db,'.rpt_useragent_tile_min_lte_hr
						TRUNCATE PARTITION ',PARTITIONHR,';
							');					
					
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT('	ALTER  TABLE ',gt_polystar_db,'.rpt_useragent_reg_2_lte_hr
						TRUNCATE PARTITION ',PARTITIONHR,';
							');					
					
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT('	ALTER  TABLE ',gt_polystar_db,'.rpt_useragent_reg_3_lte_hr
						TRUNCATE PARTITION ',PARTITIONHR,';
							');					
					
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
	
			SET @SqlCmd=CONCAT('	ALTER  TABLE ',gt_polystar_db,'.rpt_top_httphost_lte_hr
						TRUNCATE PARTITION ',PARTITIONHR,';
							');					
					
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT('	ALTER  TABLE ',gt_polystar_db,'.rpt_xdr_apn_lte_hr
						TRUNCATE PARTITION ',PARTITIONHR,';
							');					
					
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT('	ALTER  TABLE ',gt_polystar_db,'.rpt_xdr_appfamily_lte_hr
						TRUNCATE PARTITION ',PARTITIONHR,';
							');					
					
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT('	ALTER  TABLE ',gt_polystar_db,'.rpt_xdr_apptype_lte_hr
						TRUNCATE PARTITION ',PARTITIONHR,';
							');					
					
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT('	ALTER  TABLE ',gt_polystar_db,'.rpt_xdr_top_apptype_lte_hr
						TRUNCATE PARTITION ',PARTITIONHR,';
							');					
					
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT('	ALTER  TABLE ',gt_polystar_db,'.rpt_xdr_top_sub_lte_hr
						TRUNCATE PARTITION ',PARTITIONHR,';
							');					
					
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
			
	END IF;
	IF TECH_MASK IN (0,4) AND TIME_TYPE= 3 THEN
		
			SET @SqlCmd=CONCAT(' DELETE FROM ',gt_polystar_db,'.polystar_session_information
						WHERE START_DATE=''',90_DAY,'''						
						AND DATA_HOUR = ''-1''						
						AND SESSION_TYPE=''DAY''
						;');	
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT(' DELETE FROM ',gt_polystar_db,'.polystar_session_information
						WHERE START_DATE=''',DATA_DATE,'''
						AND END_DATE=''',DATA_DATE_END,'''
						AND DATA_HOUR=''-1''
						AND TECH_MASK=',TECH_MASK,'
						AND SESSION_TYPE=''DAY''
						;');			
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT('	ALTER  TABLE ',gt_polystar_db,'.rpt_family2_cell_def_lte_dy
						TRUNCATE PARTITION ',PARTITIONdy,';
							');					
					
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT('	ALTER  TABLE ',gt_polystar_db,'.rpt_family2_cell_lte_dy
						TRUNCATE PARTITION ',PARTITIONdy,';
							');					
					
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT('	ALTER  TABLE ',gt_polystar_db,'.rpt_family2_cell_tile_def_lte_dy
						TRUNCATE PARTITION ',PARTITIONdy,';
							');					
					
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT('	ALTER  TABLE ',gt_polystar_db,'.rpt_family2_cell_tile_lte_dy
						TRUNCATE PARTITION ',PARTITIONdy,';
							');					
					
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT('	ALTER  TABLE ',gt_polystar_db,'.rpt_family2_tile_def_lte_dy
						TRUNCATE PARTITION ',PARTITIONdy,';
							');					
					
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT('	ALTER  TABLE ',gt_polystar_db,'.rpt_family2_tile_lte_dy
						TRUNCATE PARTITION ',PARTITIONdy,';
							');					
					
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT('	ALTER  TABLE ',gt_polystar_db,'.rpt_family2_tile_max_lte_dy
						TRUNCATE PARTITION ',PARTITIONdy,';
							');					
					
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
		
			SET @SqlCmd=CONCAT('	ALTER  TABLE ',gt_polystar_db,'.rpt_family2_tile_median_lte_dy
						TRUNCATE PARTITION ',PARTITIONdy,';
							');					
					
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT('	ALTER  TABLE ',gt_polystar_db,'.rpt_family2_tile_min_lte_dy
						TRUNCATE PARTITION ',PARTITIONdy,';
							');					
					
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT('	ALTER  TABLE ',gt_polystar_db,'.rpt_family2_reg_2_lte_dy
						TRUNCATE PARTITION ',PARTITIONdy,';
							');					
					
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT('	ALTER  TABLE ',gt_polystar_db,'.rpt_family2_reg_3_lte_dy
						TRUNCATE PARTITION ',PARTITIONdy,';
							');					
					
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT('	ALTER  TABLE ',gt_polystar_db,'.rpt_type2_cell_tile_def_lte_dy
						TRUNCATE PARTITION ',PARTITIONdy,';
							');					
					
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT('	ALTER  TABLE ',gt_polystar_db,'.rpt_type2_cell_tile_lte_dy
						TRUNCATE PARTITION ',PARTITIONdy,';
							');					
					
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT('	ALTER  TABLE ',gt_polystar_db,'.rpt_type2_cell_def_lte_dy
						TRUNCATE PARTITION ',PARTITIONdy,';
							');					
					
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT('	ALTER  TABLE ',gt_polystar_db,'.rpt_type2_cell_lte_dy
						TRUNCATE PARTITION ',PARTITIONdy,';
							');					
					
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;		
	
			SET @SqlCmd=CONCAT('	ALTER  TABLE ',gt_polystar_db,'.rpt_type2_tile_def_lte_dy
						TRUNCATE PARTITION ',PARTITIONdy,';
							');					
					
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT('	ALTER  TABLE ',gt_polystar_db,'.rpt_type2_tile_lte_dy
						TRUNCATE PARTITION ',PARTITIONdy,';
							');					
					
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT('	ALTER  TABLE ',gt_polystar_db,'.rpt_type2_tile_max_lte_dy
						TRUNCATE PARTITION ',PARTITIONdy,';
							');					
					
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
		
			SET @SqlCmd=CONCAT('	ALTER  TABLE ',gt_polystar_db,'.rpt_type2_tile_median_lte_dy
						TRUNCATE PARTITION ',PARTITIONdy,';
							');					
					
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT('	ALTER  TABLE ',gt_polystar_db,'.rpt_type2_tile_min_lte_dy
						TRUNCATE PARTITION ',PARTITIONdy,';
							');					
					
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT('	ALTER  TABLE ',gt_polystar_db,'.rpt_type2_reg_2_lte_dy
						TRUNCATE PARTITION ',PARTITIONdy,';
							');					
					
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT('	ALTER  TABLE ',gt_polystar_db,'.rpt_type2_reg_3_lte_dy
						TRUNCATE PARTITION ',PARTITIONdy,';
							');					
					
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT('	ALTER  TABLE ',gt_polystar_db,'.rpt_host_cell_def_lte_dy
						TRUNCATE PARTITION ',PARTITIONdy,';
							');					
					
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT('	ALTER  TABLE ',gt_polystar_db,'.rpt_host_cell_lte_dy
						TRUNCATE PARTITION ',PARTITIONdy,';
							');					
					
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT('	ALTER  TABLE ',gt_polystar_db,'.rpt_host_cell_tile_def_lte_dy
						TRUNCATE PARTITION ',PARTITIONdy,';
							');					
					
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT('	ALTER  TABLE ',gt_polystar_db,'.rpt_host_cell_tile_lte_dy
						TRUNCATE PARTITION ',PARTITIONdy,';
							');					
					
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;	
	
			SET @SqlCmd=CONCAT('	ALTER  TABLE ',gt_polystar_db,'.rpt_host_tile_def_lte_dy
						TRUNCATE PARTITION ',PARTITIONdy,';
							');					
					
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT('	ALTER  TABLE ',gt_polystar_db,'.rpt_host_tile_lte_dy
						TRUNCATE PARTITION ',PARTITIONdy,';
							');					
					
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT('	ALTER  TABLE ',gt_polystar_db,'.rpt_host_tile_max_lte_dy
						TRUNCATE PARTITION ',PARTITIONdy,';
							');					
					
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
		
			SET @SqlCmd=CONCAT('	ALTER  TABLE ',gt_polystar_db,'.rpt_host_tile_median_lte_dy
						TRUNCATE PARTITION ',PARTITIONdy,';
							');					
					
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT('	ALTER  TABLE ',gt_polystar_db,'.rpt_host_tile_min_lte_dy
						TRUNCATE PARTITION ',PARTITIONdy,';
							');					
					
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT('	ALTER  TABLE ',gt_polystar_db,'.rpt_host_reg_2_lte_dy
						TRUNCATE PARTITION ',PARTITIONdy,';
							');					
					
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT('	ALTER  TABLE ',gt_polystar_db,'.rpt_host_reg_3_lte_dy
						TRUNCATE PARTITION ',PARTITIONdy,';
							');					
					
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT('	ALTER  TABLE ',gt_polystar_db,'.rpt_useragent_cell_def_lte_dy
						TRUNCATE PARTITION ',PARTITIONdy,';
							');					
					
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT('	ALTER  TABLE ',gt_polystar_db,'.rpt_useragent_cell_lte_dy
						TRUNCATE PARTITION ',PARTITIONdy,';
							');					
					
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT('	ALTER  TABLE ',gt_polystar_db,'.rpt_useragent_cell_tile_def_lte_dy
						TRUNCATE PARTITION ',PARTITIONdy,';
							');					
					
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT('	ALTER  TABLE ',gt_polystar_db,'.rpt_useragent_cell_tile_lte_dy
						TRUNCATE PARTITION ',PARTITIONdy,';
							');					
					
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;	
	
			SET @SqlCmd=CONCAT('	ALTER  TABLE ',gt_polystar_db,'.rpt_useragent_tile_def_lte_dy
						TRUNCATE PARTITION ',PARTITIONdy,';
							');					
					
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT('	ALTER  TABLE ',gt_polystar_db,'.rpt_useragent_tile_lte_dy
						TRUNCATE PARTITION ',PARTITIONdy,';
							');					
					
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT('	ALTER  TABLE ',gt_polystar_db,'.rpt_useragent_tile_max_lte_dy
						TRUNCATE PARTITION ',PARTITIONdy,';
							');					
					
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
		
			SET @SqlCmd=CONCAT('	ALTER  TABLE ',gt_polystar_db,'.rpt_useragent_tile_median_lte_dy
						TRUNCATE PARTITION ',PARTITIONdy,';
							');					
					
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT('	ALTER  TABLE ',gt_polystar_db,'.rpt_useragent_tile_min_lte_dy
						TRUNCATE PARTITION ',PARTITIONdy,';
							');					
					
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT('	ALTER  TABLE ',gt_polystar_db,'.rpt_useragent_reg_2_lte_dy
						TRUNCATE PARTITION ',PARTITIONdy,';
							');					
					
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT('	ALTER  TABLE ',gt_polystar_db,'.rpt_useragent_reg_3_lte_dy
						TRUNCATE PARTITION ',PARTITIONdy,';
							');					
					
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT('	ALTER  TABLE ',gt_polystar_db,'.rpt_top_httphost_lte_dy
						TRUNCATE PARTITION ',PARTITIONdy,';
							');					
					
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT('	ALTER  TABLE ',gt_polystar_db,'.rpt_xdr_apn_lte_dy
						TRUNCATE PARTITION ',PARTITIONdy,';
							');					
					
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT('	ALTER  TABLE ',gt_polystar_db,'.rpt_xdr_appfamily_lte_dy
						TRUNCATE PARTITION ',PARTITIONdy,';
							');					
					
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT('	ALTER  TABLE ',gt_polystar_db,'.rpt_xdr_apptype_lte_dy
						TRUNCATE PARTITION ',PARTITIONdy,';
							');					
					
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT('	ALTER  TABLE ',gt_polystar_db,'.rpt_xdr_top_apptype_lte_dy
						TRUNCATE PARTITION ',PARTITIONdy,';
							');					
					
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT('	ALTER  TABLE ',gt_polystar_db,'.rpt_xdr_top_sub_lte_dy
						TRUNCATE PARTITION ',PARTITIONdy,';
							');					
					
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	END IF;
	
	IF TECH_MASK IN (0,2) AND TIME_TYPE= 2 THEN
	
			SET @SqlCmd=CONCAT(' DELETE FROM ',gt_polystar_db,'.polystar_session_information
						WHERE START_DATE=''',60_DAY,'''					
						AND DATA_HOUR <> ''-1''						
						AND SESSION_TYPE=''DAY''
						;');			
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT(' DELETE FROM ',gt_polystar_db,'.polystar_session_information
						WHERE START_DATE=''',DATA_DATE,'''
						AND END_DATE=''',DATA_DATE_END,'''
						AND DATA_HOUR=',DATA_HOUR,'
						AND TECH_MASK=',TECH_MASK,'
						AND SESSION_TYPE=''DAY''
						;');			
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT('	ALTER  TABLE ',gt_polystar_db,'.rpt_family2_cell_def_umts_hr
						TRUNCATE PARTITION ',PARTITIONHR,';
							');			
					
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT('	ALTER  TABLE ',gt_polystar_db,'.rpt_family2_cell_umts_hr
						TRUNCATE PARTITION ',PARTITIONHR,';
							');					
					
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT('	ALTER  TABLE ',gt_polystar_db,'.rpt_family2_cell_tile_def_umts_hr
						TRUNCATE PARTITION ',PARTITIONHR,';
							');					
					
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT('	ALTER  TABLE ',gt_polystar_db,'.rpt_family2_cell_tile_umts_hr
						TRUNCATE PARTITION ',PARTITIONHR,';
							');					
					
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT('	ALTER  TABLE ',gt_polystar_db,'.rpt_family2_tile_def_umts_hr
						TRUNCATE PARTITION ',PARTITIONHR,';
							');					
					
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT('	ALTER  TABLE ',gt_polystar_db,'.rpt_family2_tile_umts_hr
						TRUNCATE PARTITION ',PARTITIONHR,';
							');					
					
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT('	ALTER  TABLE ',gt_polystar_db,'.rpt_family2_tile_max_umts_hr
						TRUNCATE PARTITION ',PARTITIONHR,';
							');					
					
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
		
			SET @SqlCmd=CONCAT('	ALTER  TABLE ',gt_polystar_db,'.rpt_family2_tile_median_umts_hr
						TRUNCATE PARTITION ',PARTITIONHR,';
							');					
					
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT('	ALTER  TABLE ',gt_polystar_db,'.rpt_family2_tile_min_umts_hr
						TRUNCATE PARTITION ',PARTITIONHR,';
							');					
					
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT('	ALTER  TABLE ',gt_polystar_db,'.rpt_family2_reg_2_umts_hr
						TRUNCATE PARTITION ',PARTITIONHR,';
							');					
					
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT('	ALTER  TABLE ',gt_polystar_db,'.rpt_family2_reg_3_umts_hr
						TRUNCATE PARTITION ',PARTITIONHR,';
							');					
					
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT('	ALTER  TABLE ',gt_polystar_db,'.rpt_type2_cell_tile_def_umts_hr
						TRUNCATE PARTITION ',PARTITIONHR,';
							');					
					
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT('	ALTER  TABLE ',gt_polystar_db,'.rpt_type2_cell_tile_umts_hr
						TRUNCATE PARTITION ',PARTITIONHR,';
							');					
					
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT('	ALTER  TABLE ',gt_polystar_db,'.rpt_type2_cell_def_umts_hr
						TRUNCATE PARTITION ',PARTITIONHR,';
							');					
					
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT('	ALTER  TABLE ',gt_polystar_db,'.rpt_type2_cell_umts_hr
						TRUNCATE PARTITION ',PARTITIONHR,';
							');					
					
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;		
	
			SET @SqlCmd=CONCAT('	ALTER  TABLE ',gt_polystar_db,'.rpt_type2_tile_def_umts_hr
						TRUNCATE PARTITION ',PARTITIONHR,';
							');					
					
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT('	ALTER  TABLE ',gt_polystar_db,'.rpt_type2_tile_umts_hr
						TRUNCATE PARTITION ',PARTITIONHR,';
							');					
					
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT('	ALTER  TABLE ',gt_polystar_db,'.rpt_type2_tile_max_umts_hr
						TRUNCATE PARTITION ',PARTITIONHR,';
							');					
					
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
		
			SET @SqlCmd=CONCAT('	ALTER  TABLE ',gt_polystar_db,'.rpt_type2_tile_median_umts_hr
						TRUNCATE PARTITION ',PARTITIONHR,';
							');					
					
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT('	ALTER  TABLE ',gt_polystar_db,'.rpt_type2_tile_min_umts_hr
						TRUNCATE PARTITION ',PARTITIONHR,';
							');					
					
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT('	ALTER  TABLE ',gt_polystar_db,'.rpt_type2_reg_2_umts_hr
						TRUNCATE PARTITION ',PARTITIONHR,';
							');					
					
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT('	ALTER  TABLE ',gt_polystar_db,'.rpt_type2_reg_3_umts_hr
						TRUNCATE PARTITION ',PARTITIONHR,';
							');					
					
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT('	ALTER  TABLE ',gt_polystar_db,'.rpt_host_cell_def_umts_hr
						TRUNCATE PARTITION ',PARTITIONHR,';
							');			
					
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT('	ALTER  TABLE ',gt_polystar_db,'.rpt_host_cell_umts_hr
						TRUNCATE PARTITION ',PARTITIONHR,';
							');					
					
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT('	ALTER  TABLE ',gt_polystar_db,'.rpt_host_cell_tile_def_umts_hr
						TRUNCATE PARTITION ',PARTITIONHR,';
							');					
					
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT('	ALTER  TABLE ',gt_polystar_db,'.rpt_host_cell_tile_umts_hr
						TRUNCATE PARTITION ',PARTITIONHR,';
							');					
					
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
				
			SET @SqlCmd=CONCAT('	ALTER  TABLE ',gt_polystar_db,'.rpt_host_tile_def_umts_hr
						TRUNCATE PARTITION ',PARTITIONHR,';
							');					
					
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT('	ALTER  TABLE ',gt_polystar_db,'.rpt_host_tile_umts_hr
						TRUNCATE PARTITION ',PARTITIONHR,';
							');					
					
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT('	ALTER  TABLE ',gt_polystar_db,'.rpt_host_tile_max_umts_hr
						TRUNCATE PARTITION ',PARTITIONHR,';
							');					
					
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
		
			SET @SqlCmd=CONCAT('	ALTER  TABLE ',gt_polystar_db,'.rpt_host_tile_median_umts_hr
						TRUNCATE PARTITION ',PARTITIONHR,';
							');					
					
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT('	ALTER  TABLE ',gt_polystar_db,'.rpt_host_tile_min_umts_hr
						TRUNCATE PARTITION ',PARTITIONHR,';
							');					
					
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT('	ALTER  TABLE ',gt_polystar_db,'.rpt_host_reg_2_umts_hr
						TRUNCATE PARTITION ',PARTITIONHR,';
							');					
					
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT('	ALTER  TABLE ',gt_polystar_db,'.rpt_host_reg_3_umts_hr
						TRUNCATE PARTITION ',PARTITIONHR,';
							');					
					
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT('	ALTER  TABLE ',gt_polystar_db,'.rpt_useragent_cell_def_umts_hr
						TRUNCATE PARTITION ',PARTITIONHR,';
							');			
					
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT('	ALTER  TABLE ',gt_polystar_db,'.rpt_useragent_cell_umts_hr
						TRUNCATE PARTITION ',PARTITIONHR,';
							');					
					
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT('	ALTER  TABLE ',gt_polystar_db,'.rpt_useragent_cell_tile_def_umts_hr
						TRUNCATE PARTITION ',PARTITIONHR,';
							');					
					
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT('	ALTER  TABLE ',gt_polystar_db,'.rpt_useragent_cell_tile_umts_hr
						TRUNCATE PARTITION ',PARTITIONHR,';
							');					
					
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
				
			SET @SqlCmd=CONCAT('	ALTER  TABLE ',gt_polystar_db,'.rpt_useragent_tile_def_umts_hr
						TRUNCATE PARTITION ',PARTITIONHR,';
							');					
					
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT('	ALTER  TABLE ',gt_polystar_db,'.rpt_useragent_tile_umts_hr
						TRUNCATE PARTITION ',PARTITIONHR,';
							');					
					
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT('	ALTER  TABLE ',gt_polystar_db,'.rpt_useragent_tile_max_umts_hr
						TRUNCATE PARTITION ',PARTITIONHR,';
							');					
					
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
		
			SET @SqlCmd=CONCAT('	ALTER  TABLE ',gt_polystar_db,'.rpt_useragent_tile_median_umts_hr
						TRUNCATE PARTITION ',PARTITIONHR,';
							');					
					
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT('	ALTER  TABLE ',gt_polystar_db,'.rpt_useragent_tile_min_umts_hr
						TRUNCATE PARTITION ',PARTITIONHR,';
							');					
					
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT('	ALTER  TABLE ',gt_polystar_db,'.rpt_useragent_reg_2_umts_hr
						TRUNCATE PARTITION ',PARTITIONHR,';
							');					
					
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT('	ALTER  TABLE ',gt_polystar_db,'.rpt_useragent_reg_3_umts_hr
						TRUNCATE PARTITION ',PARTITIONHR,';
							');					
					
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT('	ALTER  TABLE ',gt_polystar_db,'.rpt_top_httphost_umts_hr
						TRUNCATE PARTITION ',PARTITIONHR,';
							');					
					
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT('	ALTER  TABLE ',gt_polystar_db,'.rpt_xdr_apn_umts_hr
						TRUNCATE PARTITION ',PARTITIONHR,';
							');					
					
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT('	ALTER  TABLE ',gt_polystar_db,'.rpt_xdr_appfamily_umts_hr
						TRUNCATE PARTITION ',PARTITIONHR,';
							');					
					
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT('	ALTER  TABLE ',gt_polystar_db,'.rpt_xdr_apptype_umts_hr
						TRUNCATE PARTITION ',PARTITIONHR,';
							');					
					
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT('	ALTER  TABLE ',gt_polystar_db,'.rpt_xdr_top_apptype_umts_hr
						TRUNCATE PARTITION ',PARTITIONHR,';
							');					
					
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT('	ALTER  TABLE ',gt_polystar_db,'.rpt_xdr_top_sub_umts_hr
						TRUNCATE PARTITION ',PARTITIONHR,';
							');					
					
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
			
	END IF;
	IF TECH_MASK IN (0,2) AND TIME_TYPE= 3 THEN
	
			SET @SqlCmd=CONCAT(' DELETE FROM ',gt_polystar_db,'.polystar_session_information
						WHERE START_DATE=''',90_DAY,'''						
						AND DATA_HOUR = ''-1''						
						AND SESSION_TYPE=''DAY''
						;');			
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT(' DELETE FROM ',gt_polystar_db,'.polystar_session_information
						WHERE START_DATE=''',DATA_DATE,'''
						AND END_DATE=''',DATA_DATE_END,'''
						AND DATA_HOUR=''-1''
						AND TECH_MASK=',TECH_MASK,'
						AND SESSION_TYPE=''DAY''
						;');			
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT('	ALTER  TABLE ',gt_polystar_db,'.rpt_family2_cell_def_umts_dy
						TRUNCATE PARTITION ',PARTITIONdy,';
							');					
					
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT('	ALTER  TABLE ',gt_polystar_db,'.rpt_family2_cell_umts_dy
						TRUNCATE PARTITION ',PARTITIONdy,';
							');					
					
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT('	ALTER  TABLE ',gt_polystar_db,'.rpt_family2_cell_tile_def_umts_dy
						TRUNCATE PARTITION ',PARTITIONdy,';
							');					
					
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT('	ALTER  TABLE ',gt_polystar_db,'.rpt_family2_cell_tile_umts_dy
						TRUNCATE PARTITION ',PARTITIONdy,';
							');					
					
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT('	ALTER  TABLE ',gt_polystar_db,'.rpt_family2_tile_def_umts_dy
						TRUNCATE PARTITION ',PARTITIONdy,';
							');					
					
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT('	ALTER  TABLE ',gt_polystar_db,'.rpt_family2_tile_umts_dy
						TRUNCATE PARTITION ',PARTITIONdy,';
							');					
					
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT('	ALTER  TABLE ',gt_polystar_db,'.rpt_family2_tile_max_umts_dy
						TRUNCATE PARTITION ',PARTITIONdy,';
							');					
					
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
		
			SET @SqlCmd=CONCAT('	ALTER  TABLE ',gt_polystar_db,'.rpt_family2_tile_median_umts_dy
						TRUNCATE PARTITION ',PARTITIONdy,';
							');					
					
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT('	ALTER  TABLE ',gt_polystar_db,'.rpt_family2_tile_min_umts_dy
						TRUNCATE PARTITION ',PARTITIONdy,';
							');					
					
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT('	ALTER  TABLE ',gt_polystar_db,'.rpt_family2_reg_2_umts_dy
						TRUNCATE PARTITION ',PARTITIONdy,';
							');					
					
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT('	ALTER  TABLE ',gt_polystar_db,'.rpt_family2_reg_3_umts_dy
						TRUNCATE PARTITION ',PARTITIONdy,';
							');					
					
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT('	ALTER  TABLE ',gt_polystar_db,'.rpt_type2_cell_tile_def_umts_dy
						TRUNCATE PARTITION ',PARTITIONdy,';
							');					
					
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT('	ALTER  TABLE ',gt_polystar_db,'.rpt_type2_cell_tile_umts_dy
						TRUNCATE PARTITION ',PARTITIONdy,';
							');					
					
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT('	ALTER  TABLE ',gt_polystar_db,'.rpt_type2_cell_def_umts_dy
						TRUNCATE PARTITION ',PARTITIONdy,';
							');					
					
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT('	ALTER  TABLE ',gt_polystar_db,'.rpt_type2_cell_umts_dy
						TRUNCATE PARTITION ',PARTITIONdy,';
							');					
					
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;		
	
			SET @SqlCmd=CONCAT('	ALTER  TABLE ',gt_polystar_db,'.rpt_type2_tile_def_umts_dy
						TRUNCATE PARTITION ',PARTITIONdy,';
							');					
					
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT('	ALTER  TABLE ',gt_polystar_db,'.rpt_type2_tile_umts_dy
						TRUNCATE PARTITION ',PARTITIONdy,';
							');					
					
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT('	ALTER  TABLE ',gt_polystar_db,'.rpt_type2_tile_max_umts_dy
						TRUNCATE PARTITION ',PARTITIONdy,';
							');					
					
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
		
			SET @SqlCmd=CONCAT('	ALTER  TABLE ',gt_polystar_db,'.rpt_type2_tile_median_umts_dy
						TRUNCATE PARTITION ',PARTITIONdy,';
							');					
					
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT('	ALTER  TABLE ',gt_polystar_db,'.rpt_type2_tile_min_umts_dy
						TRUNCATE PARTITION ',PARTITIONdy,';
							');					
					
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT('	ALTER  TABLE ',gt_polystar_db,'.rpt_type2_reg_2_umts_dy
						TRUNCATE PARTITION ',PARTITIONdy,';
							');					
					
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT('	ALTER  TABLE ',gt_polystar_db,'.rpt_type2_reg_3_umts_dy
						TRUNCATE PARTITION ',PARTITIONdy,';
							');					
					
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT('	ALTER  TABLE ',gt_polystar_db,'.rpt_host_cell_def_umts_dy
						TRUNCATE PARTITION ',PARTITIONdy,';
							');					
					
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT('	ALTER  TABLE ',gt_polystar_db,'.rpt_host_cell_umts_dy
						TRUNCATE PARTITION ',PARTITIONdy,';
							');					
					
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT('	ALTER  TABLE ',gt_polystar_db,'.rpt_host_cell_tile_def_umts_dy
						TRUNCATE PARTITION ',PARTITIONdy,';
							');					
					
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT('	ALTER  TABLE ',gt_polystar_db,'.rpt_host_cell_tile_umts_dy
						TRUNCATE PARTITION ',PARTITIONdy,';
							');					
					
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;	
	
			SET @SqlCmd=CONCAT('	ALTER  TABLE ',gt_polystar_db,'.rpt_host_tile_def_umts_dy
						TRUNCATE PARTITION ',PARTITIONdy,';
							');					
					
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT('	ALTER  TABLE ',gt_polystar_db,'.rpt_host_tile_umts_dy
						TRUNCATE PARTITION ',PARTITIONdy,';
							');					
					
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT('	ALTER  TABLE ',gt_polystar_db,'.rpt_host_tile_max_umts_dy
						TRUNCATE PARTITION ',PARTITIONdy,';
							');					
					
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
		
			SET @SqlCmd=CONCAT('	ALTER  TABLE ',gt_polystar_db,'.rpt_host_tile_median_umts_dy
						TRUNCATE PARTITION ',PARTITIONdy,';
							');					
					
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT('	ALTER  TABLE ',gt_polystar_db,'.rpt_host_tile_min_umts_dy
						TRUNCATE PARTITION ',PARTITIONdy,';
							');					
					
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT('	ALTER  TABLE ',gt_polystar_db,'.rpt_host_reg_2_umts_dy
						TRUNCATE PARTITION ',PARTITIONdy,';
							');					
					
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT('	ALTER  TABLE ',gt_polystar_db,'.rpt_host_reg_3_umts_dy
						TRUNCATE PARTITION ',PARTITIONdy,';
							');					
					
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT('	ALTER  TABLE ',gt_polystar_db,'.rpt_useragent_cell_def_umts_dy
						TRUNCATE PARTITION ',PARTITIONdy,';
							');					
					
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT('	ALTER  TABLE ',gt_polystar_db,'.rpt_useragent_cell_umts_dy
						TRUNCATE PARTITION ',PARTITIONdy,';
							');					
					
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT('	ALTER  TABLE ',gt_polystar_db,'.rpt_useragent_cell_tile_def_umts_dy
						TRUNCATE PARTITION ',PARTITIONdy,';
							');					
					
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT('	ALTER  TABLE ',gt_polystar_db,'.rpt_useragent_cell_tile_umts_dy
						TRUNCATE PARTITION ',PARTITIONdy,';
							');					
					
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;	
	
			SET @SqlCmd=CONCAT('	ALTER  TABLE ',gt_polystar_db,'.rpt_useragent_tile_def_umts_dy
						TRUNCATE PARTITION ',PARTITIONdy,';
							');					
					
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT('	ALTER  TABLE ',gt_polystar_db,'.rpt_useragent_tile_umts_dy
						TRUNCATE PARTITION ',PARTITIONdy,';
							');					
					
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT('	ALTER  TABLE ',gt_polystar_db,'.rpt_useragent_tile_max_umts_dy
						TRUNCATE PARTITION ',PARTITIONdy,';
							');					
					
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
		
			SET @SqlCmd=CONCAT('	ALTER  TABLE ',gt_polystar_db,'.rpt_useragent_tile_median_umts_dy
						TRUNCATE PARTITION ',PARTITIONdy,';
							');					
					
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT('	ALTER  TABLE ',gt_polystar_db,'.rpt_useragent_tile_min_umts_dy
						TRUNCATE PARTITION ',PARTITIONdy,';
							');					
					
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT('	ALTER  TABLE ',gt_polystar_db,'.rpt_useragent_reg_2_umts_dy
						TRUNCATE PARTITION ',PARTITIONdy,';
							');					
					
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT('	ALTER  TABLE ',gt_polystar_db,'.rpt_useragent_reg_3_umts_dy
						TRUNCATE PARTITION ',PARTITIONdy,';
							');					
					
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT('	ALTER  TABLE ',gt_polystar_db,'.rpt_top_httphost_umts_dy
						TRUNCATE PARTITION ',PARTITIONdy,';
							');					
					
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT('	ALTER  TABLE ',gt_polystar_db,'.rpt_xdr_apn_umts_dy
						TRUNCATE PARTITION ',PARTITIONdy,';
							');					
					
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT('	ALTER  TABLE ',gt_polystar_db,'.rpt_xdr_appfamily_umts_dy
						TRUNCATE PARTITION ',PARTITIONdy,';
							');					
					
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT('	ALTER  TABLE ',gt_polystar_db,'.rpt_xdr_apptype_umts_dy
						TRUNCATE PARTITION ',PARTITIONdy,';
							');					
					
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT('	ALTER  TABLE ',gt_polystar_db,'.rpt_xdr_top_apptype_umts_dy
						TRUNCATE PARTITION ',PARTITIONdy,';
							');					
					
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	
			SET @SqlCmd=CONCAT('	ALTER  TABLE ',gt_polystar_db,'.rpt_xdr_top_sub_umts_dy
						TRUNCATE PARTITION ',PARTITIONdy,';
							');					
					
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
	END IF;
	
	INSERT INTO gt_gw_main.SP_LOG VALUES(GT_DB,'SP_Polystar_Truncate_Partition',CONCAT('Done ! TYPE:',TIME_TYPE,', DATA_DATE:',DATA_DATE,', DATA_HOUR:',DATA_HOUR,', TECH_MASK:',TECH_MASK,' Cost: ',TIMESTAMPDIFF(SECOND,START_TIME,SYSDATE()),' seconds.'), NOW());
END$$
DELIMITER ;
