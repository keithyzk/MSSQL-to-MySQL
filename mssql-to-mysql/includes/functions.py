#!/usr/bin/python

#found this at http://stackoverflow.com/questions/12325608/iterate-over-a-dict-or-list-in-python
from sqlalchemy import text

def common_iterable(obj):
    if isinstance(obj, dict):
        return obj
    else:
        return (index for index, value in enumerate(obj))

#found this at http://stackoverflow.com/questions/17044259/python-how-to-check-if-table-exists
def check_table_exists(dbcur, tablename):
    sql = 'SELECT EXISTS(SELECT * FROM information_schema.tables WHERE table_name="{tablename}")'.format(tablename=tablename)
    check_tb = dbcur.execute(text(sql))
    result = check_tb.fetchone()
    if result[0] == 1:
        return True
    return False


