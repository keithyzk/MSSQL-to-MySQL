#!/usr/bin/python

#mssql connections string 
# odbcConString = 'DRIVER={FreeTDS}; SERVER=db.com; DATABASE=dbName; UID=username; PWD=password'

MSSQL_host = 'tcp:192.168.20' 
MSSQL_database = ''
MSSQL_user = 'sa' 
MSSQL_password = ''
# ENCRYPT defaults to yes starting in ODBC Driver 18. It's good to always specify ENCRYPT=yes on the client side to avoid MITM attacks.
# odbcConString = 'DRIVER={ODBC Driver 17 for SQL Server};SERVER='+MSSQL_host+';DATABASE='+MSSQL_database+';UID='+MSSQL_user+';PWD='+MSSQL_password+''
odbcConString = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={MSSQL_host};DATABASE={MSSQL_database};UID={MSSQL_user};PWD={MSSQL_password};"


#mysql connections info
MYSQL_host="192.168.20."
MYSQL_user=""
MYSQL_passwd=""
MYSQL_db=""

#tables to retrieve and recreate
# list_of_tables = [['original_name', 'new_name_or_same']]
list_of_tables = ['mlc_project','ACT_EVT_LOG', 'adasdas_userinfo', 'dms_convertersconfig', 'dms_teamnotice', 'edoc2_release_c_City_Old', 'eform_m_client']
list_of_ignore_tables = []
