DELIMITER $$
USE `gt_gw_main`$$
DROP PROCEDURE IF EXISTS `SP_CreateDB_LTE`$$
CREATE DEFINER=`covmo`@`%` PROCEDURE `SP_Log_MYSQL_Status`()
BEGIN
	SET @SqlCmd=CONCAT('CREATE TABLE IF NOT EXISTS gt_gw_main.`session_status_log` (
		  `LOG_DATE` datetime DEFAULT NULL,
		  `VARIABLE_NAME` varchar(64) NOT NULL DEFAULT '''',
		  `VARIABLE_VALUE` varchar(1024) DEFAULT NULL
		) ENGINE=MyISAM DEFAULT CHARSET=utf8
		');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt; 
	
	SET @SqlCmd=CONCAT('
		INSERT INTO gt_gw_main.session_status_log
		(LOG_DATE,VARIABLE_NAME,VARIABLE_VALUE)
		SELECT NOW(),A.VARIABLE_NAME,A.VARIABLE_VALUE FROM information_schema.SESSION_STATUS A
		WHERE VARIABLE_NAME IN
		(''Aborted_clients'',''Aborted_connects'',''Connections'',''Created_tmp_disk_tables'',''Created_tmp_files'',
		''Created_tmp_tables'',''Delayed_errors'',''Delayed_insert_threads'',''Delayed_writes'',''Flush_commands'',
		''Key_blocks_not_flushed'',''Key_blocks_unused'',''Key_blocks_used'',''Key_read_requests'',''Key_reads'',
		''Key_write_requests'',''Key_writes'',''Last_query_cost'',''Max_used_connections'',''Not_flushed_delayed_rows'',
		''Open_files'',''Open_streams'',''Open_table_definitions'',''Open_tables'',''Opened_files'',''Opened_table_definitions'',
		''Opened_tables'',''Qcache_free_blocks'',''Qcache_free_memory'',''Qcache_hits'',''Qcache_inserts'',''Qcache_lowmem_prunes'',
		''Qcache_not_cached'',''Qcache_queries_in_cache'',''Qcache_total_blocks'',''Queries'',''Table_locks_immediate'',
		''Table_locks_waited'',''Tc_log_max_pages_used'',''Tc_log_page_size'',''Tc_log_page_waits'',''Threads_cached'',
		''Threads_connected'',''Threads_created'',''Threads_running'',''Uptime'',''Uptime_since_flush_status'');');
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt; 
	
END$$
DELIMITER ;
