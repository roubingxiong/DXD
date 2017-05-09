#!/usr/bin/env python2
# -*- coding: utf-8 -*-

import argparse
import datetime
import logging.config

import MySQLdb

import file_watcher
from email_handler import *
from mysql_reader_writer import TableExtractor, TableLoader
from bi_sdxd_etl_workflow_handler import *

def get_log_table_list(conn, messager):

    config = messager['config']
    schema = config.get('source', 'db')
    from_date = messager['from_date']
    to_date = messager['to_date']

    tbl_sql = "SELECT TABLE_NAME FROM information_schema.tables WHERE table_schema = '%s' and table_type='BASE TABLE' and (create_time >= '%s' and create_time < '%s')and table_name LIKE 't_decision_rule_log_%s';"%(schema,from_date, to_date,'%')

    logger.info('getting t_decision_rule_log_yyyymmdd table list from database %s', schema)
    tbl_list = []

    try:
        logger.info('running sql->%s', tbl_sql)
        # conn = messager['']
        cursor = conn.cursor()
        cursor.execute(tbl_sql)
        for row in cursor.fetchall():
            tbl_list.append(str(row[0]).lower())
    except:
        raise
    else:
        logger.info('decision log table list->%s', tbl_list)
        return tbl_list

def check_create_table(conn, table_name, config):
    schema = config.get('target', 'db')
    tbl_sql = "SELECT TABLE_NAME FROM information_schema.tables WHERE table_schema = '%s' and table_type='BASE TABLE' and table_name ='%s';"%(schema, table_name)
    ddl = str(config.get('target', 't_decision_rule_log'))%table_name
    # print ddl
    cursor = conn.cursor()
    cursor.execute(tbl_sql)
    if cursor.rowcount == 0:
        logger.warn('table [%s] not exist in target database', table_name)
        logger.info('creating table->%s', table_name)
        cursor.execute(ddl)
    else:
        logger.warn('table [%s] exist in target database', table_name)

if __name__ == "__main__":
    start_time = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
    global today
    today = datetime.datetime.today()
    global now_timestamp
    now_timestamp = today.strftime('%Y%m%d_%H%M%S.%f')
    global today_str
    today_str = today.strftime('%Y-%m-%d')
    global curr_dir
    curr_dir = os.path.dirname(__file__)

    # global messager
    messager = {'table_name': '', 'action':'','data_hub':'',  'data_file': '', 'ctrl_file': '', 'ctrl_count': '', 'data_path_file': '', 'ctrl_path_file': '', 'status': 'Start', 'from_date': '9999-12-31', 'to_date': '9999-12-31','log':'log'}

    print '-INFO: parsing arguments'
    global runDate, runMode, actionType, tblName
    parser = argparse.ArgumentParser(description='Process')
    parser.add_argument('-d', help='date for running with format "yyyymmdd" e.g 20170101')
    parser.add_argument('-m', help="date for running with format 't+n' e.g t+3")
    parser.add_argument('-a', required=True, help="action of process, support option: 'extract'or 'load'")
    parser.add_argument('-t', required=True, help='table object name you want to extract or load')
    args = parser.parse_args()
    runDate = str(args.d)
    runMode = str(args.m)
    actionType = str(args.a)
    tblName = str(args.t)


    global logger
    logger, logger_file = get_logger(curr_dir, tblName, runDate, start_time)
    logger.info('job start at %s', start_time)

    try:
        logger.info('set configuration from config file')
        config = get_config(cfg_dir=curr_dir)
        messager['config'] = config
        messager = reload_messager(actionType, runDate, runMode, tblName,config, logger_file, messager)

        data_hub = config.get('global', 'data_hub')
        messager_list = []
        if actionType == 'extract':
            srcConn = get_conn_by_type(config, 'source')
            messager['connection'] = srcConn
            table_list = get_log_table_list(srcConn, messager)
            for table_name in table_list:
                logger.debug('extracting table %s', table_name)
                # srcConn = get_conn_by_type(config, 'source')
                msg = reload_messager(actionType, runDate, runMode, table_name,config, logger_file, messager)
                logger.info('creating Table Extractor')
                worker = TableExtractor(srcConn)
                msg = worker.extractTableToFile(msg)
                print(msg)
                messager_list.append(msg)
        else:
            tgtConn = get_conn_by_type(config, 'target')
            messager['connection'] = tgtConn
            logger.info('watching file %s', 't_decision_rule_log_[0-9]{8}.ctrl.[0-9]{8}.[0-9]{8}')
            file_list =  file_watcher.watch_files(dir=data_hub, pattern='t_decision_rule_log_[0-9]{8}.ctrl.[0-9]{8}.[0-9]{8}')
            if len(file_list) > 0:
                for file in file_list:
                    table_name = str(file).split('.')[0]
                    check_create_table(tgtConn, table_name, config)

                    messager = reload_messager(actionType, runDate, runMode, table_name,config, logger_file, messager)
                    logger.info('creating Table Loader')
                    worker = TableLoader(tgtConn)
                    messager = worker.loadFileToTable(messager)
                    print "-------------" + messager
                    messager_list.append(messager)
            else:
                logger.warn('%s not exist or file watcher is time out')
                # raise Exception('%s not exist or file watcher is time out', ctrl_file)

    except Exception as e:
        logger.exception(e.message)
        logger.error('some exception occured, now sending error email')
        messager['status'] = 'Fail'
        # messager['exception'] = e.message
        raise
    else:
        messager['status'] = 'Success'
        logger.info('Congrats! the processing complete successfully')
        logger.info('clean data file older than 7 days')
        data_file_list = file_watcher.clean_file(dir=messager['data_hub'], days=7)

        logger.info('clean log file older than 30 days')
        log_dir = os.path.dirname(logger_file)
        log_file_list = file_watcher.clean_file(dir=log_dir, days=30)
    finally:
        messager['connection'].close()
        logger.info('complete at %s', datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
        print "-------------" + str(messager_list)
        sendJobStatusEmail(messager=messager_list, attachment=[logger_file])
