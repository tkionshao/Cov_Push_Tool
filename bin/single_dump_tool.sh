#! /bin/bash
source ./instance.cnf
DUMPDIR=./sp_history/
NOW=`date +%Y%m%d`
DBNAME=${1}
SPNAME=${2}
TMPFILE=./tmp/tmp_${SPNAME}.sql
SPFILE=${DUMPDIR}${SPNAME}.sql
SPTEMP=./tmp/${DBNAME}_${SPNAME}.tmp
SQLSTMT="SELECT COUNT(1) FROM mysql.proc WHERE db='${DBNAME}' AND name='${SPNAME}'"
PROC_EXISTS=`mysql ${MYSQL_CONN} -ANe"${SQLSTMT}" | awk '{print $1}'`
if [ ${PROC_EXISTS} -eq 1 ]
then
    rm -f ${TMPFILE}
#    echo "Export ${DBNAME}.${SPNAME}"
    SQLSTMT="SELECT type FROM mysql.proc WHERE db='${DBNAME}' AND name='${SPNAME}'"
    PROC_TYPE=`mysql ${MYSQL_CONN} -ANe"${SQLSTMT}" | awk '{print $1}'`
    SQLSTMT="SHOW CREATE ${PROC_TYPE} ${DBNAME}.${SPNAME}\G"

    echo "DELIMITER \$\$" > ${SPFILE}
    echo "USE \`$DBNAME\`\$\$" >> ${SPFILE}
    echo "DROP PROCEDURE IF EXISTS \`SP_CreateDB_LTE\`\$\$" >> ${SPFILE}

    mysql ${MYSQL_CONN} -ANe"${SQLSTMT}" > ${TMPFILE}

    #
    # Remove Top 3 Lines
    #
    LINECOUNT=`wc -l < ${TMPFILE}`
    (( LINECOUNT -= 3 ))
    tail -${LINECOUNT} < ${TMPFILE} > ${SPTEMP}

    #
    # Remove Bottom 3 Lines
    #
    LINECOUNT=`wc -l < ${SPTEMP}`
    (( LINECOUNT -= 4 ))
    head -${LINECOUNT} < ${SPTEMP} > ${TMPFILE}
    cat ${TMPFILE} >> ${SPFILE}  

    echo "END\$\$" >> ${SPFILE}
    echo "DELIMITER ;" >> ${SPFILE}

else
    echo "Stored Procedure ${DBNAME}.${SPNAME} Does Not Exist"
fi
rm -f ${SPTEMP}
rm -f ${TMPFILE}
# ls -l |wc -l
