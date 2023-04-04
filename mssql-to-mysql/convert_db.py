#!/usr/bin/python
import pymysql
import pyodbc
import sys
import datetime

import includes.config as config
import includes.functions as functions
import includes.sqlserver_datatypes as data_types

#connection for MSSQL. (Note: you must have FreeTDS installed and configured!)
ms_conn = pyodbc.connect(config.odbcConString)
ms_cursor = ms_conn.cursor()

#connection for MySQL
# my_conn = MySQLdb.connect(host=config.MYSQL_host,user=config.MYSQL_user, passwd=config.MYSQL_passwd, db=config.MYSQL_db)
my_conn = pymysql.connect(host=config.MYSQL_host,user=config.MYSQL_user, passwd=config.MYSQL_passwd, db=config.MYSQL_db)
my_cursor = my_conn.cursor()

# final_table_list = []
# final_new_table_names = {}
# if config.list_of_tables:
#     for in_tables in config.list_of_tables:
#         final_table_list.append(in_tables[0])
#         final_new_table_names[in_tables[0]] = in_tables[1]

#     ms_tables = "','".join(map(str, final_table_list))
#     ms_tables = "WHERE name in ('"+ms_tables+"')"
# else:
#     ms_tables = "WHERE type in ('U')" #tables are 'U' and views are 'V'

ms_tables = "WHERE type in ('U')"


ms_cursor.execute("SELECT * FROM sysobjects %s;" % ms_tables ) #sysobjects is a table in MSSQL db's containing meta data about the database. (Note: this may vary depending on your MSSQL version!)
ms_tables = ms_cursor.fetchall()
noLength = [56, 58, 61, 35, 42] #list of MSSQL data types that don't require a defined lenght ie. datetime

for tbl in ms_tables:
    # crtTable = final_new_table_names[tbl[0]]
    crtTable = tbl[0]
    ms_cursor.execute("SELECT * FROM syscolumns WHERE id = OBJECT_ID('%s')" % tbl[0]) #syscolumns: see sysobjects above.
    columns = ms_cursor.fetchall()
    attr = ""
    sattr = ""
    for col in columns:
        colType = data_types.data_types[str(col.xtype)] #retrieve the column type based on the data type id
        
        if col.length == -1 and col.xtype == 231:
            col.xtype = 35
            colType = data_types.data_types[str(col.xtype)]

        #make adjustments to account for data types present in MSSQL but not supported in MySQL (NEEDS WORK!)
        if col.xtype == 60:
            colType = "float"
            attr += "`"+col.name +"` "+ colType + "(" + str(col.length) + "),"
            sattr += "cast(["+col.name+"] as varchar("+str(col.xprec)+")) as ["+col.name+"],"
        elif col.xtype == 104:
            colType = "bit"
            attr += "`"+col.name +"` "+ colType + "(" + str(col.length) +"),"
            sattr += "cast(["+col.name+"] as int) as ["+col.name+"]," 

        elif col.xtype == 106:
            colType = "decimal"
            attr += "`"+col.name +"` "+ colType + "(" + str(col.xprec) + "," + str(col.xscale) + "),"
            sattr += "cast(["+col.name+"] as varchar("+str(col.xprec)+")) as ["+col.name+"],"            
        elif col.xtype == 108:
            colType = "decimal"
            attr += "`"+col.name +"` "+ colType + "(" + str(col.xprec) + "," + str(col.xscale) + ")," 
            sattr += "cast(["+col.name+"] as varchar("+str(col.xprec)+")) as ["+col.name+"],"      
        elif col.xtype in noLength:
            attr += "`"+col.name +"` "+ colType + ","
            sattr += "["+col.name+"],"
        else:
            attr += "`"+col.name +"` "+ colType + "(" + str(col.length) + "),"
            sattr += "["+col.name+"],"
    
    attr = attr[:-1]
    sattr = sattr[:-1]
    print(attr[:-1])
    print(sattr[:-1])
    print('=========')

    


    if functions.check_table_exists(my_cursor, crtTable):
        my_cursor.execute("drop table "+crtTable)
    print("CREATE TABLE " + crtTable + " (" + attr + ");")
    print("#####################")
    my_cursor.execute("CREATE TABLE " + crtTable + " (" + attr + ");") #create the new table and all columns
    ms_cursor.execute("SELECT "+sattr+" FROM "+ tbl[0])
    tbl_data = ms_cursor.fetchall()

    for row in tbl_data:
        new_row = list(row)

        for i in functions.common_iterable(new_row): 
       
            if new_row[i] == None:
               new_row[i] = 0
            elif type(new_row[i]) == datetime.datetime:
        
                new_row[i] = new_row[i].date().isoformat()
                
        row = tuple(new_row)
        my_conn.ping(True)
       
        query_string = "INSERT INTO `" + crtTable + "` VALUES %r;" % (tuple(new_row),)
        
       
        my_cursor.execute(query_string)
        my_conn.commit() #mysql commit changes to database 
        
my_cursor.close()
my_conn.close() #mysql close connection
ms_conn.close() #mssql close connection
