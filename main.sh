#! /bin/bash
source ./instance.cnf
NOW=`date +%Y%m%d%h%M`
# dump all in one sp
read -p "Version? (ex:777_07.02 or SP_777)" SQLFILENAME
sh ./bin/all_in_one_dump.sh ${SQLFILENAME}

# dump singles sp
sh ./bin/make_single_dump_command.sh
sh ./tmp/command_ready_to_single_dump.sh

# log
echo "sql file: ${SQLFILENAME}.sql"
echo "time: ${NOW}"
