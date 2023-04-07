#!/usr/bin/python
import pymysql
import pyodbc
import sys
import datetime

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

import includes.config as config
import includes.functions as functions
import includes.sqlserver_datatypes as data_types

import threading


'''
#connection for MSSQL. (Note: you must have FreeTDS installed and configured!)
ms_conn = pyodbc.connect(config.odbcConString)
ms_cursor = ms_conn.cursor()

pool_size = 10
max_overflow = 20
my_conn = create_engine('mysql+pymysql://'+config.MYSQL_user+':'+config.MYSQL_passwd+'@'+config.MYSQL_host+':3306/'+config.MYSQL_db, pool_size=pool_size, max_overflow=max_overflow, pool_recycle=3600)
Session = sessionmaker(bind=my_conn)
my_session = Session()
'''

pool_size = 10
max_overflow = 20

ms_conn = pyodbc.connect(config.odbcConString)
ms_cursor = ms_conn.cursor()



ms_tables = "WHERE type in ('U')"

ms_cursor.execute("SELECT * FROM sysobjects %s;" % ms_tables ) #sysobjects is a table in MSSQL db's containing meta data about the database. (Note: this may vary depending on your MSSQL version!)
ms_tables = ms_cursor.fetchall()
# remove the list of ignore tables
if config.list_of_ignore_tables:
    for ignore_table in config.list_of_ignore_tables:
        for table in ms_tables:
            if table[0] == ignore_table:
                ms_tables.remove(table)


noLength = [56, 58, 61, 35, 42] #list of MSSQL data types that don't require a defined lenght ie. datetime

def process_table(ms_session,my_session,tbl):

    ms_cursor = ms_session.cursor()
    Session = sessionmaker(bind=my_session)
    my_cursor = Session()

    crtTable = tbl[0]
    ms_cursor.execute("SELECT * FROM syscolumns WHERE id = OBJECT_ID('%s')" % tbl[0]) #syscolumns: see sysobjects above.
    columns = ms_cursor.fetchall()
    attr = ""
    sattr = ""
    for col in columns:
        colType = data_types.data_types[str(col.xtype)] #retrieve the column type based on the data type id

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

        # change the data type for the column if it is a varchar(max) to text
        elif col.length == -1 and col.xtype == 231:
            col.xtype = 35
            colType = data_types.data_types[str(col.xtype)]
        # change the data type for the column if it is a varbinary(max) to longblob
        elif col.length == -1 and col.xtype == 165:
            colType = "longblob"
            attr += "`"+col.name +"` "+ colType + ","
            sattr += "["+col.name+"],"

        elif col.xtype in noLength:
            attr += "`"+col.name +"` "+ colType + ","
            sattr += "["+col.name+"],"
        else:
            attr += "`"+col.name +"` "+ colType + "(" + str(col.length) + "),"
            sattr += "["+col.name+"],"
    
    attr = attr[:-1]
    sattr = sattr[:-1]

    ms_cursor.execute("select b.column_name from information_schema.table_constraints a inner join information_schema.constraint_column_usage b on a.constraint_name = b.constraint_name where a.constraint_type = 'PRIMARY KEY' and a.table_name = '%s'" % tbl[0])
    primary_key_col_list = ms_cursor.fetchall()
    primary_key_col = ','.join(["`{}`".format(item[0]) for item in primary_key_col_list])

    if functions.check_table_exists(my_cursor, crtTable):
        # print("Table already exists: " + crtTable)
        # ms_cursor.close()
        # my_cursor.close()
        # lock.release()
        # return
        drop_sql = "drop table "+crtTable
        my_cursor.execute(text(drop_sql))

    create_tb_sql = "CREATE TABLE " + crtTable + " (" + attr + " ,primary key(" + primary_key_col + "));"
    my_cursor.execute(text(create_tb_sql))
    my_cursor.commit()

    print("==================================================")
    print("Table created: " + crtTable)

    ms_query = "SELECT "+sattr+" FROM "+ tbl[0]
    chunks = pd.read_sql(ms_query, ms_session, chunksize=10000)
    # if chunks is None:
    #     print("No data in table: " + crtTable)
    #     continue
    # else:
    #     print("Inserting data into table: " + crtTable)
    for chunk in chunks:
        chunk.to_sql(crtTable, my_session, if_exists='append', index=False)
    print("Data inserted into table: " + crtTable)

    print("create index for table: " + crtTable)
    # create index
    get_index_sql = "SELECT t.name as TableName, i.name as IndexName, c.name as ColumnName FROM sys.tables t \
             INNER JOIN sys.indexes i ON t.object_id = i.object_id \
             INNER JOIN sys.index_columns ic ON i.object_id = ic.object_id and i.index_id = ic.index_id \
             INNER JOIN sys.columns c ON t.object_id = c.object_id and ic.column_id = c.column_id \
             WHERE i.is_primary_key = 0 and t.name = '" + crtTable + "'" 
    ms_cursor.execute(get_index_sql)
    index_lists = ms_cursor.fetchall()

    index_dict = {}
    for index in index_lists:
        if index[0] not in index_dict:
            index_dict[index[0]] = {index[1]: [index[2]]}
        elif index[1] not in index_dict[index[0]]:
            index_dict[index[0]][index[1]] = [index[2]]
        else:
            index_dict[index[0]][index[1]].append(index[2])
    for table in index_dict:
        for index in index_dict[table]:
            index_cols = ', '.join(index_dict[table][index])
            create_index_sql = "create index {0} on {1} ({2});".format(index, table, index_cols)
            # print("create index {0} on {1} ({2});".format(index, table, index_cols))
            try:
                my_cursor.execute(text(create_index_sql))
                my_cursor.commit()
            except Exception as e:
                print(e)
                print("create index failed: " + create_index_sql)
                continue

    print("")

    ms_cursor.close()
    my_cursor.close()


def thread_process():
    ms_thread_conn = pyodbc.connect(config.odbcConString)
    my_thread_conn = create_engine('mysql+pymysql://'+config.MYSQL_user+':'+config.MYSQL_passwd+'@'+config.MYSQL_host+':3306/'+config.MYSQL_db, pool_size=pool_size, max_overflow=max_overflow, pool_recycle=3600)

    while True:
        # 使用锁保证线程安全
        lock.acquire()
        # 如果没有待处理的字符串则退出线程
        if len(ms_tables) == 0:
            lock.release()
            break
        # 获取待处理的table
        tbl = ms_tables.pop(0)
        lock.release()
        # 执行 process_table 函数
        process_table(ms_thread_conn,my_thread_conn,tbl)

    # ms_thread_conn.close()
    # my_thread_conn.close()

# 使用锁保证并发处理时数据的一致性
lock = threading.Lock()


# 创建四个线程
num_threads = 4
threads = []
for i in range(num_threads):
    t = threading.Thread(target=thread_process)
    threads.append(t)
    t.start()

# 等待所有线程执行完毕
for t in threads:
    t.join()

print("执行结束")

