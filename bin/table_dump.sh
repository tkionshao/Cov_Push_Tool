#!/bin/bash
source ./instance.cnf
MYSQLCMD="mysqldump ${MYSQL_CONN} --default-character-set=utf8 "
IFS=,
for TABLE in ${TABLELIST}
do
  x=`echo $x | sed 's/CD/QW/'`
  tmp=`echo ${TABLE}|sed 's/ /./'`
  echo "Export ${tmp}"
  echo "${MYSQLCMD}${TABLE} > ${TABLESDIR}/${tmp}.sql" >> ${TABLESDIR}/tmp_table_script.sh
  # ${MYSQLCMD}${TABLE} > "${TABLESDIR}/${tmp}.sql" >> ${TABLESDIR}/tmp_table_script.sh
done
IFS=$oldIFS

sh ${TABLESDIR}/tmp_table_script.sh
rm ${TABLESDIR}/tmp_table_script.sh

echo "tables dump finished."
