#!/usr/bin/python3


import pyodbc
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

import includes.config as config
import includes.functions as functions
import includes.sqlserver_datatypes as data_types

import threading

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


def check_process(ms_session,my_session,tbl):
    ms_cursor = ms_session.cursor()
    Session = sessionmaker(bind=my_session)
    my_cursor = Session()

    chk_tab = tbl[0]
    #count_sql = "SELECT COUNT(*) FROM %s" % tbl[0]
    count_sql = "SELECT COUNT(*) FROM "+ tbl[0] 
    ms_tb_query = pd.read_sql(count_sql, ms_session)
    ms_tb_count = ms_tb_query.iat[0,0]
    # print(ms_tb_count)
    try:
        my_tb_query = pd.read_sql(count_sql, my_session)
        my_tb_count = my_tb_query.iat[0,0]
    except Exception as e:
        print(e)
        print("get %s count error! error sql: %s" % chk_tab,count_sql)

    if ms_tb_count == my_tb_count:
        print("Table {} is OK!,table count: {}".format(chk_tab, ms_tb_count))
    else:
        print("*******************************************")
        print("SQLServer Table %s count is %s" % (chk_tab,ms_tb_count))
        print("MySQL Table %s count is %s" % (chk_tab,my_tb_count))
        print("Table %s is not OK!" % chk_tab)
        print("*******************************************")
    ms_cursor.close()
    my_cursor.close()


def check_thread_process():
    ms_thread_conn = pyodbc.connect(config.odbcConString)
    my_thread_conn = create_engine('mysql+pymysql://'+config.MYSQL_user+':'+config.MYSQL_passwd+'@'+config.MYSQL_host+':3306/'+config.MYSQL_db, pool_size=pool_size, max_overflow=max_overflow, pool_recycle=3600)

    while True:
        lock.acquire()
        if len(ms_tables) == 0:
            lock.release()
            break
        tbl = ms_tables.pop(0)
        lock.release()
        check_process(ms_thread_conn,my_thread_conn,tbl)

lock = threading.Lock()

num_threads = 4
threads = []
for i in range(num_threads):
    t = threading.Thread(target=check_thread_process)
    threads.append(t)
    t.start()

for t in threads:
    t.join()

print("check end")