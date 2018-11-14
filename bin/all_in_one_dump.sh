#!/bin/bash
source ./instance.cnf
SPVERSION=$1
path=${SPALLINONE}/${SPVERSION}
filename=/${VERSION}.sql
MYSQLCMD="mysqldump ${MYSQL_CONN} --default-character-set=utf8 "
DBCONFIGFILE=${path}/db.config
# SP export
rm -rf ${path}
mkdir ${path} 

echo "use gt_gw_main;" >> $path$filename
echo "Set global log_bin_trust_function_creators=1;" >> $path$filename
echo "CREATE DATABASE IF NOT EXISTS operations_monitor;" >> $path$filename
${MYSQLCMD} gt_gw_main tbl_rpt_other --routines --no-create-db --event>> $path$filename

echo "use gt_global_statistic;" >> $path$filename
${MYSQLCMD}  gt_global_statistic  nw_config --routines --no-create-db >> $path$filename

echo "use operations_monitor;" >> $path$filename
${MYSQLCMD}  operations_monitor   --routines --no-create-db --no-create-info --no-data --event>> $path$filename

echo "use gt_global_statistic;" >> $path$filename
${MYSQLCMD} gt_global_statistic  sp_version_nw >> $path$filename

echo "use gt_gw_main;" >> $path$filename
${MYSQLCMD} gt_gw_main sp_version >> $path$filename
# DONE


# Make db.config
echo "sql_names=(${SPVERSION}.sql  )" > ${DBCONFIGFILE}
echo "#/opt/covmo/gt_covmo_sock.sh" >> ${DBCONFIGFILE}
echo "#/opt/covmo/all_sock.sh" >> ${DBCONFIGFILE}
echo "sql_cmds=( /opt/covmo/all_sock.sh )" >> ${DBCONFIGFILE}
echo "#file:/data/mysql_3308/gt_covmo -> cp /data/mysql_3308/gt_covmo_YYYYMMDD_HHMM" >> ${DBCONFIGFILE}
echo "#dump:gt_gw_main ->  /data/gt_gw_main_YYYYMMDD_HHMM" >> ${DBCONFIGFILE}
echo "backups=( dump:gt_gw_main )" >> ${DBCONFIGFILE}
# DONE

echo "All in one SP dump finished."
