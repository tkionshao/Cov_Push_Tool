CREATE DEFINER=`covmo`@`%` PROCEDURE `SP_Auto_Sub_Generate_View`(IN JOBID VARCHAR(10))
BEGIN
	DECLARE no_more_maps INT;
	DECLARE v_SI INT;	
	DECLARE T_STATUS VARCHAR(20);
	DECLARE T_NAME VARCHAR(100);	
	DECLARE UNION_STR VARCHAR(6000);
	DECLARE DB VARCHAR(50) DEFAULT 'gt_gw_main';
	
	DECLARE csr2 CURSOR FOR
	SELECT `STATUS` AS T_STATUS, gt_strtok(COMMAND,2,'''') AS T_NAME FROM gt_schedule.job_task_history 
	WHERE JOB_ID = JOBID;
	DECLARE CONTINUE HANDLER FOR NOT FOUND SET no_more_maps = 1;
	SET no_more_maps = 0;
	
	SET no_more_maps = 0;
	OPEN csr2;
	dept_loop:REPEAT
                FETCH csr2 INTO T_STATUS,T_NAME;
                IF no_more_maps = 0 THEN
                        IF no_more_maps THEN
				LEAVE dept_loop;
			END IF;
	
			IF T_STATUS = 'FINISHED' THEN
				IF UNION_STR IS NULL THEN
					SET UNION_STR = CONCAT('SELECT * FROM ',DB,'.`',T_NAME,'`');
				ELSE
					SET UNION_STR = CONCAT(UNION_STR,' UNION ','SELECT * FROM ',DB,'.`',T_NAME,'`');
				END IF;	
			END IF;
                END IF;
        UNTIL no_more_maps
        END REPEAT dept_loop;
	CLOSE csr2;
	select UNION_STR;
 
	set @RETURN_UNION_STR = UNION_STR;
