#!/usr/bin/env python2
# -*- coding: utf-8 -*-

import argparse
import datetime
import logging.config

import MySQLdb

import file_watcher
from email_handler import *
from mysql_reader_writer import TableExtractor, TableLoader


def main(messager):

    config = messager['config']

    data_hub = config.get('global', 'data_hub')

    if actionType == 'extract':
        srcConn = get_conn_by_type(config, 'source')

        logger.info('creating Table Extractor')
        worker = TableExtractor(srcConn)

        messager = worker.extractTableToFile(messager)

    else:
        ctrl_file = messager['ctrl_file'] # tableName.ctrl.yyyymmdd.yyyymmdd
        logger.info('watching file %s', ctrl_file)
        if file_watcher.watch_file(dir=data_hub, filename=ctrl_file):
            tgtConn = get_conn_by_type(config, 'target')
            logger.info('creating Table Loader')
            worker = TableLoader(tgtConn)
            messager = worker.loadFileToTable(messager)
        else:
            logger.warn('%s not exist or file watcher is time out', ctrl_file)
            messager['status'] = 'Fail'

    return messager

def reload_messager(actionType,runDate, runMode,  tblName, config, logFile, messager={}):
    messager['config'] = config
    messager['log'] = logFile
    default_run_mode = 't+3'

    if actionType not in ('extract', 'load'):
        raise ("action type '%s' not recognized, you only can assign 'extract' or 'load'", actionType)
    else:
        messager['action'] = actionType

    if runMode in ('None', ''):
        try:
            runMode = config.get('global', 'runMode')
        except (ConfigParser.NoOptionError,ConfigParser.NoSectionError, Exception) as e:
            logger.warn(e.message)
            logger.info("run mode do not assigned, using default value '%s'", default_run_mode)
            runMode = default_run_mode
        else:
            logger.info('get run mode (%s) from config file', runMode)

    messager['mode'] = runMode

    offset = runMode.replace(' ', '').replace('T', '').replace('t', '')
    logger.debug('offset %s days',offset)

    try:
        if runDate == 'None' or runDate.replace(' ', '') == '':
            runDate = datetime.datetime.today()
            logger.warn("run date not assigned, using the date of today as the run date")
        else:
            runDate = datetime.datetime.strptime(runDate, '%Y%m%d')

        to_date = runDate
        from_date = to_date - datetime.timedelta(days=int(offset))

        format_from_date = from_date.strftime('%Y%m%d')
        format_to_date = to_date.strftime('%Y%m%d')

        to_date = to_date.strftime('%Y-%m-%d')
        from_date = from_date.strftime('%Y-%m-%d')

    except:

        logger.error("date '%s' is not match the format(yyyymmdd) or a invalid date", runDate)
        raise
    else:
        messager['from_date'] = from_date
        messager['to_date'] = to_date
        logger.info('will process data updated from %s to %s(not included)', from_date, to_date)



    messager['table_name'] = tblName
    messager['data_hub'] = config.get('global', 'data_hub')
    messager['data_file'] = tblName + ".dat." + format_from_date + "." + format_to_date
    messager['ctrl_file'] = tblName + ".ctrl." + format_from_date + "." + format_to_date

    messager['ctrl_count'] = ''
    messager['data_path_file'] = os.path.join(messager['data_hub'] , messager['data_file'])
    messager['ctrl_path_file'] = os.path.join(messager['data_hub'] , messager['ctrl_file'])

    logger.info('\naction: %s\nrundate: %s\nmode: %s\ntable: %s\nfrom: %s\nto: %s', actionType, to_date, runMode, tblName, from_date, to_date)

    messager['status'] = 'Start'

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
    if runDate in ('None', ''):
        runDate = datetime.datetime.today().strftime('%Y%m%d')

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
    log_formater = logging.Formatter('%(asctime)s - %(levelname)s - [%(name)s] - %(filename)s - %(funcName)s - %(message)s')
    log_file_handler.setFormatter(log_formater)
    log_console_handler.setFormatter(log_formater)
    logger.addHandler(log_file_handler)
    logger.addHandler(log_console_handler)
    logging._addHandlerRef(log_file_handler)
    logging._addHandlerRef(log_console_handler)

    return logger, logger_file


def get_conn_by_type(config, type):

    try:
        host = config.get(type, 'host')
        user = config.get(type, 'user')
        passwd = config.get(type, 'passwd')
        db = config.get(type, 'db')
        charset = config.get(type, 'charset')
        port = int(config.get(type, 'port'))
        conn = MySQLdb.connect(host=host, user=user, passwd=passwd, db=db, charset=charset, port=port) #, charset=charset
    except:
        logger.error('failed to connect %s database, \nhost: %s\nuser:%s\ndatabase:%s\ncharset:%s\tport:%s', type, host, user, db, charset,port)
        logger.error('can not get connection, please verify you connection information for database')
        raise
    else:
        logger.info('connecting %s database, \nhost: %s\nuser:%s\ndatabase:%s\ncharset:%s\nport:%s', type, host, user, db, charset,port)

    return conn


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

    global messager
    messager = {'table_name': '', 'action':'','data_hub':'',  'data_file': '', 'ctrl_file': '', 'ctrl_count': '', 'data_path_file': '', 'ctrl_path_file': '', 'status': 'Start', 'from_date': '9999-12-31', 'to_date': '9999-12-31','log':'log'}

    print '-INFO: parsing arguments'
    global runDate, runMode, actionType, tblName
    parser = argparse.ArgumentParser(description='Process')
    parser.add_argument('-d', help='date for running with format "yyyymmdd" e.g 20170101')
    parser.add_argument('-m', help="date for running with format 't+n' e.g t+3")
    parser.add_argument('-a', required=True, help="action of process, support option: 'extract'or 'load'")
    # parser.add_argument('-o', required=True, nargs='+', help='table object name you want to extract or load')
    parser.add_argument('-t', required=True, help='table object name you want to extract or load')
    args = parser.parse_args()
    runDate = str(args.d)
    runMode = str(args.m)
    actionType = str(args.a)
    tblName = str(args.t)

    # parser.print_help()

    global logger
    logger, logger_file = get_logger(curr_dir, tblName, runDate, start_time)
    logger.info('job start at %s', start_time)

    try:
        logger.info('set configuration from config file')
        config = get_config(cfg_dir=curr_dir)

        logger.info('validating argument')
        messager = reload_messager(actionType, runDate, runMode, tblName,config, logger_file, messager)
        messager['status'] = 'Start'

        # messager['exception'] = ''
        sendJobStatusEmail(messager=messager)

        # config = messager['config']

        messager = main(messager)
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
        logger.info('complete at %s', datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
        sendJobStatusEmail(messager=messager, attachment=[logger_file])
