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
import FileWatcher

from sendEmail import *

from mysql_reader_writer import TableExtractor, TableLoader

def main(runDate, runMode, actionType, tblName):

    logger.info('preparing environment')
    curr_dir = os.getcwd()

    logger.info('reading system configuration')
    config = get_config(curr_dir)

    try:
        if runMode == 'None':
            runMode = config.get('global', 'runMode')
            # logger.info('get run mode %s from config file', runMode)

        if not runDate == 'None':
            runDate = datetime.datetime.strftime(runDate, '%Y-%m-%d')
            logger.info('run date: %s', runDate)
        elif not runMode == 'None':
            offset = runMode.replace(' ', '').replace('Tt','')
            runDate = (datetime.datetime.today() - datetime.timedelta(days=int(offset))).strftime('%Y-%m-%d')
            logger.info('run mode: %s', runMode)
            logger.info('run date: %s', runDate)
    except:
        raise
    else:
        logger.info('action Type: %s', actionType)
        logger.info('table name: %s', tblName)


    datahub = config.get('global', 'dataHub')

    if os.path.isdir(datahub) and os.access(datahub, os.R_OK):
        logger.info('data hub %s is a readable directory', datahub)
    else:
        logger.error('data hub %s is not a existing or readable directory, please check with your ops team', datahub)
        logger.info('application terminate')
        exit(1)

    if actionType == 'extract':
        srcConn = getConnByType(config,'source')

        logger.info('creating Table Extractor')
        worker = TableExtractor(srcConn)

        etlStat = worker.extractTableToFile(tblName, datahub, runDate, 'FULL')

    else:
        filename = tblName+'.ctrl.20170422'
        if FileWatcher.watch_file(dir=datahub, filename=filename):
            tgtConn = getConnByType(config, 'target')
            logger.info('creating Table Loader')
            worker = TableLoader(tgtConn)
            etlStat = worker.loadFileToTable(tblName, datahub, runDate)
        else:
            logger.warn(tblName+'.ctrl.20170422'+' not exist or file watcher is time out')
            etlStat = {}
            etlStat['status'] = 'F'


    if etlStat['status'] == 'S':
        # print "-INFO: extract success"
        logger.info('extract success')
        sendEmail()
    else:
        # print "-INFO: extract fail"
        logger.info('extract fail')


def get_config(curr_dir):
    cfgfile = os.path.join(curr_dir, 'init.cfg')
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


def get_logger(curr_dir, now_timestamp):
    log_dir = os.path.join(curr_dir, 'log')
    if not os.path.isdir(log_dir):
        print '%s is not exist, now creating it'%log_dir
        os.mkdir(log_dir)

    logger = logging.getLogger('DXD_ETL')
    logger.setLevel(logging.DEBUG)
    logger_file = os.path.join(log_dir, os.path.basename(__file__) + '_' + now_timestamp + '.log')
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
        logging.exception(e.message)
        logging.info('failed to connect database, application terminate')
        exit(1)

    return conn


if __name__ == "__main__":
    start_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    global today
    today = datetime.datetime.today()
    global now_timestamp
    now_timestamp = today.strftime('%Y%m%d_%H%M%S.%f')
    global today_str
    today_str = today.strftime('%Y-%m-%d')

    messager = {'table': '', 'data_file': '', 'ctrl_file': '', 'ctrlCnt': '', 'data_path_file': '', 'ctrl_path_file': '', 'status': 'BEGIN', 'from_date': '9999-12-31', 'to_date': '9999-12-31','log':'log'}

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

    global logger
    logger, logger_file = get_logger(os.getcwd(), now_timestamp)

    logger.info('')
    sendJobStatusEmail(starttime=start_time, table=tblName, messager=messager)
    try:
        main(runDate, runMode, actionType, tblName)
    except Exception as e:
        logger.exception(e.message)
        logger.error('some exception occured, now sending error email')
        sendJobErrorEmail(starttime=datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),table=tblName,messager=messager)
        raise
    else:
        logger.info('Congrats! the processing complete successfully. now sending success email')
        sendJobSuccessEmail(starttime=datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),table=tblName, messager=messager)

    logger.info('complete at %s', datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
