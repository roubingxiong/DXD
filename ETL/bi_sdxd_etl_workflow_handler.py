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
from FileWatcher import *

from MySqlReaderWriter import TableExtractor, TableLoader

def usage():
    print 'usage'

def main():

    today = datetime.datetime.today()
    now_timestamp = today.strftime('%Y%m%d_%H%M%S.%f')
    today_str = today.strftime('%Y-%m-%d')

    curr_dir = os.getcwd()
    log_dir = os.path.join(curr_dir, 'log')
    cfgfile = os.path.join(curr_dir, 'init.cfg')

    if os.access(cfgfile, os.R_OK) and os.stat(cfgfile).st_size > 0:
        print "-INFO: find the app config file at " + cfgfile
    else:
        print "-ERROR: %s is not exist, application exit"
        exit(1)

    print "-INFO: loading system and logging configuration from the file"
    config = ConfigParser.ConfigParser()
    config.read(cfgfile)

    if not os.path.isdir(log_dir):
        os.mkdir(log_dir)

    global logger
    logger = logging.getLogger('DXD_ETL')
    logger.setLevel(logging.DEBUG)

    log_file_handler = logging.FileHandler(os.path.join(log_dir, os.path.basename(__file__) + '_' + now_timestamp + '.log'))
    # 终端Handler
    log_console_handler = logging.StreamHandler()
    log_console_handler.setLevel(logging.DEBUG)

    log_formater =logging.Formatter('%(asctime)s - %(levelname)s - [%(name)s] - %(funcName)s - %(message)s')

    log_file_handler.setFormatter(log_formater)
    log_console_handler.setFormatter(log_formater)

    logger.addHandler(log_file_handler)
    logger.addHandler(log_console_handler)

    logging._addHandlerRef(log_file_handler)
    logging._addHandlerRef(log_console_handler)
    # logging.info('looging')

    #create a filehandler
    logger.info('config file loading successfully')

    logger.info('parsing arguments')

    parser = argparse.ArgumentParser(description='Process')
    parser.add_argument('-d', help='specify a date for running with format "yyyy-mm-dd"')
    parser.add_argument('-t', required=True, help='type of process, extract(e) or load(l)')
    # parser.add_argument('-o', required=True, nargs='+', help='table object name you want to extract or load')
    parser.add_argument('-o', required=True, help='table object name you want to extract or load')
    args = parser.parse_args()
    runDate = str(args.d)
    jobType = str(args.t)
    tblName = str(args.o)

    run_mode = config.get('global', 'runMode')

    offset = run_mode.replace(' ', '').replace('T','').replace('BD', '')

    if runDate == 'None':
        runDate = (today - datetime.timedelta(days=int(offset))).strftime('%Y-%m-%d')
        logger.warn('the running business date do not assigned, default mode is %s', run_mode)

    logger.info('The processing with Run Date: %s\tProcess Type:%s\tDatabase Table:%s', runDate, jobType, tblName)


    datahub = config.get('global', 'dataHub')

    if os.path.isdir(datahub) and os.access(datahub, os.R_OK):
        logger.info('data hub %s is a readable directory', datahub)
    else:
        logger.error('data hub %s is not a existing or readable directory, please check with your ops team', datahub)
        logger.info('application terminate')
        exit(1)


    logger.debug('')
    conn = getConnByType(config,jobType)

    charset = config.get('global','charset')
    extractor = TableExtractor(conn, charset)

    etlStat = extractor.extractTableToFile(tblName, datahub, runDate, 'FULL')

    if etlStat['status'] == 'S':
        print "-INFO: extract success"
    else:
        print "-INFO: extract fail"


    print "-INFO: ------------load file into table-------------"
    dataHub = config.get('global', 'dataHub')

    jobType = 'load'
    conn = getConnByType(config, jobType)

    print "-INFO: create a file watcher"
    filewatcher = FileWatcher(dataHub)

    if filewatcher.watch_file(tblName+'.ctrl.20170422'):
        loader = TableLoader(conn)
        loader.loadFileToTable(tblName, datahub, runDate)

    conn.close()


def getConnByType(config, jobType):

    if jobType == 'extract':
        type = 'source'
    elif jobType == 'load':
        type = 'target'
    else:
        logger.error('this action type is not supported currently')
        logger.info('application terminate')
        exit(1)

    host = config.get(type, 'host')
    user = config.get(type, 'user')
    passwd = config.get(type, 'passwd')
    db = config.get(type, 'db')
    charset = config.get(type, 'charset')
    logger.info('connecting %s database, host: %s\tuser:%s\tdatabase:%s', type, host, user, db)

    try:
        conn = MySQLdb.connect(host=host, user=user, passwd=passwd, db=db, charset=charset) #, charset=charset
    except Exception as e:
        logging.exception(e.message)
        logging.info('failed to connect database, application terminate')
        exit(1)

    return conn


if __name__ == "__main__":
    main()

