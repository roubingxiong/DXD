#!/usr/bin/env python2
# -*- coding: utf-8 -*-

import argparse
import datetime
import logging.config

import MySQLdb

import file_watcher
from email_handler_v0 import *
from mysql_reader_writer import TableExtractor, TableLoader


def rebuild_messager(actionType, runDate, runMode, tblName, config, logFile):
    messager['config'] = config
    messager['log'] = logFile
    default_run_mode = 't+3'

    if runMode in ('None', ''):
        try:
            runMode = config.get('global', 'runMode')
        except (ConfigParser.NoOptionError,ConfigParser.NoSectionError, Exception) as e:
            logger.warn(e.message)
            logger.debug("run mode do not assigned, using default value '%s'", default_run_mode)
            runMode = default_run_mode

    messager['mode'] = runMode

    offset = runMode.replace(' ', '').replace('T', '').replace('t', '')
    logger.debug('offset %s days',offset)

    try:
        if runDate in ('None', ''):
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

    messager['action_type'] = actionType
    messager['table_name'] = tblName
    messager['data_hub'] = config.get('global', 'data_hub')
    messager['data_file'] = tblName + ".dat." + format_from_date + "." + format_to_date
    messager['ctrl_file'] = tblName + ".ctrl." + format_from_date + "." + format_to_date
    messager['ctrl_count'] = ''
    messager['data_path_file'] = os.path.join(messager['data_hub'] , messager['data_file'])
    messager['ctrl_path_file'] = os.path.join(messager['data_hub'] , messager['ctrl_file'])

    logger.info('\naction: %s\nrun_date: %s\nmode: %s\ntable: %s\nfrom: %s\nto: %s', actionType, to_date, runMode, tblName, from_date, to_date)

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


def get_logger(curr_dir, table_name, run_date, start_time, action_type):

    log_dir = os.path.join(curr_dir, 'log')
    if not os.path.isdir(log_dir):
        print '%s is not exist, now creating it'%log_dir
        os.mkdir(log_dir)

    logger = logging.getLogger('DXD_ETL')
    logger.setLevel(logging.DEBUG)

    log_formatter = logging.Formatter('%(asctime)s - %(levelname)s - [%(name)s] - %(filename)s - %(funcName)s - %(message)s')

    # file Handler
    logger_file = os.path.join(log_dir, "%s_%s_%s_%s_%s.log"%(os.path.basename(__file__), action_type, table_name, run_date, start_time))
    log_file_handler = logging.FileHandler(logger_file)
    log_file_handler.setFormatter(log_formatter)

    # console Handler
    log_console_handler = logging.StreamHandler()
    log_console_handler.setLevel(logging.DEBUG)
    log_console_handler.setFormatter(log_formatter)

    logger.addHandler(log_file_handler)
    logger.addHandler(log_console_handler)

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


def get_report_file(rpt_dir, table_name, run_date, action_type):

    base_name = "%s_%s_%s.rpt" % (action_type,table_name, run_date)
    abs_file = os.path.join(rpt_dir, base_name)

    try:
        if os.path.isfile(abs_file):
            os.remove(abs_file)

        report = open(abs_file, 'a+')
    except:
        raise
    else:
        return abs_file, report


def get_status_list(file, from_date, to_date):
    # print(file)
    status_list = []

    try:
        with open(file, 'r') as tbl:
            for line in tbl.readlines():
                line = line.strip().split('|')
                row = {}
                row['action_type'] = line[0]
                row['table_name'] = line[1]
                row['data_file'] = line[2]
                row['ctrl_file'] = line[3]
                row['ctrl_count'] = line[4]
                row['status'] = line[5]
                row['from_date'] = from_date
                row['to_date'] = to_date

                status_list.append(row)
                logger.debug(line)
    except:
        raise
    finally:
        return status_list


def get_table_list(file):
    table_list = []
    try:
        with open(file, 'r') as tbl:
            for line in tbl.readlines():
                table_list.append(line.strip())
    except:
        raise
    finally:
        logger.info('get table list->%s', table_list)
        return table_list


def get_decision_rule_log_table_list(messager):
    table_list = []
    from_date = messager['from_date']
    to_date = messager['to_date']

    start_date = datetime.datetime.strptime(from_date, '%Y-%m-%d')
    end_date = datetime.datetime.strptime(to_date, '%Y-%m-%d')

    curr_date = end_date - datetime.timedelta(days=1)

    while(curr_date >= start_date):
        curr_date_str = curr_date.strftime('%Y%m%d')
        log_table = 't_decision_rule_log_' + curr_date_str
        table_list.append(log_table)
        curr_date = curr_date - datetime.timedelta(days=1)

    logger.info('get decision rule log table list->%s', table_list)
    return table_list


def write_rept(report, messager):
    action = messager['action_type']
    table = messager['table_name']
    data_file = messager['data_file']
    ctrl_file = messager['ctrl_file']
    row_count = messager['ctrl_count']
    status = messager['status']
    report.write("%s|%s|%s|%s|%s|%s\n"%(action, table, data_file, ctrl_file, row_count, status))
    report.flush()
    return report


def check_table(src_conn, table_name, tgt_conn='None' ):

    ddl_sql = "SHOW CREATE TABLE %s"% table_name;   # SELECT * FROM information_schema.tables WHERE table_name = '%s', table_name;

    try:
        logger.info('checking source table->%s', table_name)
        src_cursor = src_conn.cursor()
        src_cursor.execute(ddl_sql)
        table, create_table = src_cursor.fetchone()
    except:
        raise

    if tgt_conn not in ('None', ''):
        try:
            logger.info('checking target table->%s', table_name)
            tgt_cursor = tgt_conn.cursor()
            tgt_cursor.execute(create_table)
            tgt_conn.commit()
        except Exception as e:
            error_code = e.args[0]  # 1050 Table 't_user_profile_identity' already exists # 1146 Table 't_user_profile_identity' doesn't exists
            error_msg = e.args[1]
            if error_code == 1050:
                logger.debug(error_msg)
            else:
                raise
        else:
            logger.info("created target table->%s\n%s", table_name, create_table)


def parse_args():
    global runDate, runMode, actionType, tblName
    parser = argparse.ArgumentParser(description='Process')
    parser.add_argument('-d', default=today_str, help='date for running with format "yyyymmdd" e.g 20170101')
    parser.add_argument('-m', help="date for running with format 't+n' e.g t+3")
    parser.add_argument('-a', default='extract', required=True, choices=['extract', 'load'],
                        help="action of process, support option: 'extract'or 'load'")
    parser.add_argument('-t', help='table object name you want to extract or load')
    args = parser.parse_args()
    runDate = str(args.d)
    runMode = str(args.m)
    actionType = str(args.a)
    tblName = str(args.t)

    if tblName in ('None', ''):
        tblName = "all"

    return runDate, runMode, actionType, tblName


if __name__ == "__main__":
    start_time = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
    today_str = datetime.datetime.today().strftime('%Y%m%d')
    curr_dir = os.path.dirname(__file__)

    messager = {'table_name': 'all', 'action_type':'extract','data_hub':'',  'data_file': '', 'ctrl_file': '', 'ctrl_count': '', 'data_path_file': '', 'ctrl_path_file': '', 'status': 'Start', 'from_date': '9999-12-31', 'to_date': '9999-12-31','log':'log'}

    print '-INFO: parsing arguments'
    global runDate, runMode, actionType, tblName
    runDate, runMode, actionType, tblName = parse_args()

    # preparing logger
    global logger
    logger, logger_file = get_logger(curr_dir=curr_dir, table_name=tblName, run_date=runDate, start_time=start_time, action_type=actionType)
    logger.info('job start at %s', start_time)

    try:
        logger.info('get configuration from config file')
        config = get_config(cfg_dir=curr_dir)

        rpt_dir = config.get('global', 'report_dir')
        report_file_name, report_file = get_report_file(rpt_dir=rpt_dir, table_name=tblName, run_date=runDate, action_type=actionType)

        # preparing table list for processing
        if tblName.lower() == 't_decision_rule_log':
            messager = rebuild_messager(actionType, runDate, runMode, tblName, config, logger_file)
            table_list = get_decision_rule_log_table_list(messager)

        elif tblName == 'all':
            table_list_file = config.get('global','table_list')
            table_list = get_table_list(table_list_file)

            messager = rebuild_messager(actionType, runDate, runMode, 't_decision_rule_log', config, logger_file)
            log_table_list = get_decision_rule_log_table_list(messager)

            table_list = table_list + log_table_list

        else:
            table_list = [tblName]
        logger.info('full pending tables for processing->%s', table_list)

        # get database connection
        tgt_conn = get_conn_by_type(config, 'target')
        src_conn = get_conn_by_type(config, 'source')
        data_hub = config.get('global', 'data_hub')

        # core flow for extracting or loading data
        if actionType == 'extract':
            logger.info('********** creating table extractor **********')
            extractor = TableExtractor(src_conn)
            for table in table_list:
                try:
                    logger.info('>>>>>>>>>> extracting table->%s >>>>>>>>>>', table)
                    messager = rebuild_messager(actionType, runDate, runMode, table, config, logger_file)
                    check_table(src_conn=src_conn, table_name=table)
                    extractor.extractTableToFile(messager)
                except Exception as e:
                    messager['status'] = 'Fail'
                    logger.exception(e.message)
                else:
                    messager['status'] = 'Success'
                finally:
                    # logger.debug(messager)
                    logger.info('<<<<<<<<<< end extract table->%s <<<<<<<<<<', table)
                    write_rept(report_file, messager)

        elif actionType == 'load':
            logger.info('********** creating table loader **********')
            loader = TableLoader(tgt_conn)
            for table in table_list:
                try:
                    logger.info('>>>>>>>>>> loading table->%s >>>>>>>>>>', table)
                    messager = rebuild_messager(actionType, runDate, runMode, table, config, logger_file)
                    check_table(src_conn=src_conn, tgt_conn=tgt_conn, table_name=table)
                    ctrl_file = messager['ctrl_file'] # tableName.ctrl.yyyymmdd.yyyymmdd
                    if file_watcher.watch_file(dir=data_hub, filename=ctrl_file):
                        loader.loadFileToTable(messager)
                except Exception as e:
                    messager['status'] = 'Fail'
                    logger.exception(e.message)
                else:
                    messager['status'] = 'Success'
                finally:
                    # logger.debug(messager)
                    logger.info('<<<<<<<<<< end load table->%s <<<<<<<<<<', table)
                    write_rept(report_file, messager)

        logger.info('getting status list for email')
        status_list = get_status_list(report_file_name, messager['from_date'], messager['to_date'])
    except Exception as e:
        logger.exception(e.message)
        logger.error('some exception occurred, please check exception message')
        raise
    else:
        logger.info('The Flow complete successfully')

        logger.info('clean report file older than 30 days')
        data_file_list = file_watcher.clean_file(dir=rpt_dir, days=30)

        logger.info('clean data file older than 7 days')
        data_file_list = file_watcher.clean_file(dir=messager['data_hub'], days=7)

        logger.info('clean log file older than 30 days')
        log_dir = os.path.dirname(logger_file)
        log_file_list = file_watcher.clean_file(dir=log_dir, days=30)
    finally:
        logger.info('complete at %s', datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
        sendJobStatusEmail(config=config, messager=status_list, attachment=[logger_file])
