#!/usr/bin/env python
# -*- coding: utf-8 -*-

import smtplib
import MySQLdb
import getopt, sys
import argparse
import time,datetime
import logging
import logging.config

import ConfigParser
import file_watcher

from email_handler import *

from MySqlReaderWriter import TableExtractor, TableLoader

def main(messager):

    config = messager['config']

    runDateStr = messager['from_date']
    tblName = messager['table']

    format_date_str = datetime.datetime.strptime(runDateStr, '%Y-%m-%d').strftime('%Y%m%d')

    data_hub = config.get('global', 'dataHub')
    messager['data_hub'] = data_hub

    if actionType == 'extract':
        srcConn = getConnByType(config,'source')

        logger.info('creating Table Extractor')
        worker = TableExtractor(srcConn)

        messager = worker.extractTableToFile(messager)

    else:
        ctrl_file = tblName+'.ctrl.' + format_date_str # tableName.ctrl.yyyymmdd
        logger.info('watching file %s', ctrl_file)
        if file_watcher.watch_file(dir=data_hub, filename=ctrl_file):
            tgtConn = getConnByType(config, 'target')
            logger.info('creating Table Loader')
            worker = TableLoader(tgtConn)
            messager = worker.loadFileToTable(messager)
        else:
            logger.warn('%s not exist or file watcher is time out', ctrl_file)
            etlStat = {}
            messager['status'] = 'Fail'

    return messager

def validate_argument(actionType, runDate, runMode, tblName, messager):
    config = messager['config']
    # runDateStr = runDate
    try:
        if runMode == 'None':
            runMode = config.get('global', 'runMode')
            # logger.info('get run mode %s from config file', runMode)

        if not runDate == 'None':
            runDateStr = datetime.datetime.strptime(runDate, '%Y%m%d').strftime('%Y-%m-%d')  # formating run date
            logger.info('run date: %s', runDateStr)
        elif not runMode == 'None':
            offset = runMode.replace(' ', '').replace('Tt', '')
            runDateStr = (datetime.datetime.today() - datetime.timedelta(days=int(offset))).strftime('%Y-%m-%d')
            logger.info('run mode: %s', runMode)
            logger.info('run date: %s', runDate)

        messager['action'] = actionType
        messager['table'] = tblName
        messager['mode'] = runMode
        messager['from_date'] = runDateStr
    except:
        raise
    else:
        logger.info('action Type: %s', actionType)
        logger.info('table name: %s', tblName)

    return messager


def get_config(cfg_dir):
    cfgfile = os.path.join(cfg_dir, 'init.cfg')
    if os.access(cfgfile, os.R_OK) and os.stat(cfgfile).st_size > 0:
        # print "-INFO: get the system config file->" + cfgfile
        logger.info('get the system config file->%s', cfgfile)
    else:
        # print "-ERROR: %s is not exist or the content is empty, application exit" % cfgfile
        logger.error('%s is not exist or the content is empty, application exit', cfgfile)
        exit(1)
    # print "-INFO: loading system and logging configuration from the file"
    logger.info('loading system configuration from the file')
    config = ConfigParser.ConfigParser()
    config.read(cfgfile)
    return config


def get_logger(curr_dir, tblName, runDate, start_time):
    log_dir = os.path.join(curr_dir, 'log')
    if not os.path.isdir(log_dir):
        print '%s is not exist, now creating it'%log_dir
        os.mkdir(log_dir)

    logger = logging.getLogger('DXD_ETL')
    logger.setLevel(logging.DEBUG)
    logger_file = os.path.join(log_dir, os.path.basename(__file__) + '_' + tblName + '_' + runDate +  '_' + start_time+ '.log')
    log_file_handler = logging.FileHandler(logger_file)
    # 终端Handler
    log_console_handler = logging.StreamHandler()
    log_console_handler.setLevel(logging.DEBUG)
    log_formater = logging.Formatter('%(asctime)s - %(levelname)s - [%(name)s] - %(funcName)s - %(message)s')
    log_file_handler.setFormatter(log_formater)
    log_console_handler.setFormatter(log_formater)
    logger.addHandler(log_file_handler)
    logger.addHandler(log_console_handler)
    logging._addHandlerRef(log_file_handler)
    logging._addHandlerRef(log_console_handler)

    return logger, logger_file


def getConnByType(config, type):

    host = config.get(type, 'host')
    user = config.get(type, 'user')
    passwd = config.get(type, 'passwd')
    db = config.get(type, 'db')
    charset = config.get(type, 'charset')
    logger.info('connecting %s database, host: %s\tuser:%s\tdatabase:%s\tcharset:%s', type, host, user, db, charset)

    try:
        conn = MySQLdb.connect(host=host, user=user, passwd=passwd, db=db, charset=charset) #, charset=charset
    except Exception as e:
        raise

    return conn


if __name__ == "__main__":
    start_time = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
    global today
    today = datetime.datetime.today()
    global now_timestamp
    now_timestamp = today.strftime('%Y%m%d_%H%M%S.%f')
    global today_str
    today_str = today.strftime('%Y-%m-%d')

    messager = {'table': '', 'action':'','data_hub':'',  'data_file': '', 'ctrl_file': '', 'ctrl_count': '', 'data_path_file': '', 'ctrl_path_file': '', 'status': 'Start', 'from_date': '9999-12-31', 'to_date': '9999-12-31','log':'log'}

    print '-INFO: parsing arguments'
    parser = argparse.ArgumentParser(description='Process')
    parser.add_argument('-d', help='date for running with format "yyyymmdd"')
    parser.add_argument('-m', default='t+1', help='date for running with format "yyyymmdd"')
    parser.add_argument('-a', required=True, help='action of process, extract(e) or load(l)')
    # parser.add_argument('-o', required=True, nargs='+', help='table object name you want to extract or load')
    parser.add_argument('-t', required=True, help='table object name you want to extract or load')
    args = parser.parse_args()
    runDate = str(args.d)
    runMode = str(args.m)
    actionType = str(args.a)
    tblName = str(args.t)

    curr_dir = os.getcwd()

    global logger
    logger, logger_file = get_logger(curr_dir, tblName, runDate, start_time)
    logger.info('job start at %s', start_time)
    messager['log'] = logger_file

    logger.info('set configuration from config file')
    config = get_config(cfg_dir=curr_dir)
    messager['config'] = config

    logger.info('validating argument')
    messager = validate_argument(actionType, runDate, runMode, tblName, messager)

    sendJobStatusEmail(messager=messager)
    try:
        messager = main(messager)
    except Exception as e:
        logger.exception(e.message)
        logger.error('some exception occured, now sending error email')
        sendJobStatusEmail(messager=messager)
        raise
    else:
        logger.info('Congrats! the processing complete successfully')

    finally:
        logger.info('clean data file older than 7 days')
        data_file_list = file_watcher.clean_file(dir=messager['data_hub'], days=7)

        logger.info('clean log file older than 30 days')
        log_dir = os.path.dirname(logger_file)
        log_file_list = file_watcher.clean_file(dir=log_dir, days=30)

        logger.info('complete at %s', datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
        sendJobStatusEmail(messager=messager, attachment=[logger_file])
