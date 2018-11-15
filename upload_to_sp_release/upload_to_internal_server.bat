RMDIR .\tmp /S
MKDIR .\tmp
pscp -pw covmo123 covmo@192.168.1.221:"/home/covmo/covmo_sp_main/sp_release/*.zip" .\tmp
7za.exe x ".\tmp\tmp_sql_file.zip" -oc:.\tmp 
DEL .\tmp\tmp_sql_file.zip
XCOPY ".\tmp\sp_release\*" "\\192.168.3.242\Project3\CovMo\Module\Database\SP_Release\" /e
pause
