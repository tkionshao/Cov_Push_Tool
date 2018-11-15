#! /bin/bash
source ./instance.cnf
NOW=`date +%Y%m%d`
# dump all in one sp
read -p "Version? (ex:777_07.02 or SP_777)" SQLFILENAME
sh ./bin/all_in_one_dump.sh ${SQLFILENAME}
zip -r ./sp_release/tmp_sql_file.zip ./sp_release/${SQLFILENAME}/

# dump singles sp
sh ./bin/make_single_dump_command.sh
sh ./tmp/command_ready_to_single_dump.sh

# dump tables
sh ./bin/table_dump.sh

# log
echo "sql file: ${SQLFILENAME}"
echo "time: ${NOW}"
