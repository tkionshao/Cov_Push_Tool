#! /bin/bash
source ./instance.cnf
echo ${MYSQL_CONN}
MYSQLCMD="mysql ${MYSQL_CONN}"

${MYSQLCMD} -s -N -e "SELECT CONCAT('sh ./bin/single_dump_tool.sh ',ROUTINE_SCHEMA,' ',ROUTINE_NAME) FROM information_schema.routines WHERE routine_type = 'PROCEDURE' AND ROUTINE_SCHEMA IN (${DUMPTARGETDB});" > ./tmp/command_ready_to_single_dump.sh
