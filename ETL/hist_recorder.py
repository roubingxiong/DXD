# -*- coding: UTF-8 -*-
#!/usr/bin/python

import sqlite3

import os
import datetime,time
import logging
import logging.config

dxd_sqlite=os.path.join(os.getcwd(), 'dxd.sqlite3')

logger = logging.getLogger('DXD_ETL')

conn = sqlite3.connect(dxd_sqlite)


ddl='''
CREATE TABLE hist_job_status (
  id integer PRIMARY KEY autoincrement,
  table_name varchar(100) NOT NULL,
  type varchar(50) not null,
  run_date date,
  status varchar(50),
  data_file varchar(100),
  ctrl_file varchar(100),
  ctrl_count varchar(50),
  update_user varchar(50) DEFAULT 'dxd',
  update_date date DEFAULT current_date,
  update_datetime datetime DEFAULT current_timestamp,
  create_date date DEFAULT current_date,
  create_datetime datetime DEFAULT current_timestamp,
  create_user varchar(50) DEFAULT 'dxd'

);
'''

def createHistTable():
    conn.execute('DROP TABLE hist_job_status')
    conn.execute(ddl)
    conn.commit()

def recordNewJob(table_name='t_dummy', type='extract', status='Start', run_date='9999-12-31'):
    insert_sql = "insert INTO %s (table_name, type, status, run_date) VALUES ('%s', '%s', '%s', '%s')" % ('hist_job_status',table_name, type, status, run_date)

    try:
        logger.info('add one running record ->%s', insert_sql)
        conn.execute(insert_sql)
        id = conn.execute('SELECT max(id) from hist_job_status').fetchone()[0]
        print id
        conn.commit()
    except:
        raise

    return id

def updateDataCtrlFile(id, date_file='dummy.dat', ctrl_file='dummy.ctrl', ctrl_count=''):
    update_date, update_datetime = getDateNdatetime()
    update_sql = "update hist_job_status SET data_file='%s', ctrl_file='%s', ctrl_count='%s',update_date='%s', update_datetime='%s' WHERE id = %s "%(date_file, ctrl_file, ctrl_count,update_date, update_datetime, id)

    try:
        conn.execute(update_sql)
        conn.commit()
    except:
        raise

    return

def getDateNdatetime():
    curr_date = datetime.datetime.today().strftime('%Y-%m-%d')
    curr_datetime = datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S')

    return curr_date, curr_datetime

# createHistTable()
id = recordNewJob()
# updateDataCtrlFile(id=9)