#!/usr/bin/env python
# -*- coding: UTF-8 -*-
# Date:2017-05-17

__author__ = 'roubingxiong'

import datetime

import MySQLdb

from email_handler import *

logger = logging.getLogger('DXD_ETL')


def rebuild_messager(actionType, runDate, runMode, tblName, config, logFile, messager):
    messager['config'] = config
    messager['log'] = logFile
    default_run_mode = 't+3'

    if runMode in ('None', ''):
        try:
            runMode = config.get('global', 'runMode')
            logger.debug('get run mode [%s] from config file', runMode)
        except  Exception as e:
            logger.warn(e.message)
            logger.debug("run mode do not assigned, using default value '%s'", default_run_mode)
            runMode = default_run_mode

    messager['run_mode'] = runMode

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
                row['run_mode'] = line[6]
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


def get_decision_rule_log_table_list(messager, conn='None',):

    log_tbl_date_list = []
    log_table_list = []
    from_date = messager['from_date']
    to_date = messager['to_date']

    log_tbl_sql = "SELECT CAST(apply_time as DATE) AS log_tbl_date FROM t_decision_application WHERE update_time >='%s' and update_time < '%s' GROUP BY 1;" % (from_date, to_date)
    #log_tbl_sql = "SELECT DATE_FORMAT(apply_time, '%Y-%m-%d') log_tbl_date,CONCAT('t_decision_rule_log_', DATE_FORMAT(apply_time, '%Y%m%d')) AS log_table, min(update_time) AS min_update_time, max(update_time) AS max_update_time  FROM t_decision_application WHERE update_time >='2014-08-15' and update_time < '2017-05-11' GROUP BY 1;" % (from_date, to_date)


    try:
        cursor = conn.cursor()
        logger.info('running sql to get log table date->%s', log_tbl_sql)
        cursor.execute(log_tbl_sql)
        log_tbl_date_list = cursor.fetchall()

    except:
        raise

    for date_str in log_tbl_date_list:
        date_str = date_str[0]
        date_format = date_str.strftime('%Y%m%d') #datetime.datetime.strptime(date_str, '%Y-%m-%d').strftime('%Y%m%d')
        log_table = 't_decision_rule_log_' + date_format
        log_table_list.append(log_table)

    logger.info('get decision rule log table list->%s', log_table_list)

    return log_table_list


def write_rept(report, messager):
    action = messager['action_type']
    table = messager['table_name']
    data_file = messager['data_file']
    ctrl_file = messager['ctrl_file']
    row_count = messager['ctrl_count']
    status = messager['status']
    run_mode = messager['run_mode']
    report.write("%s|%s|%s|%s|%s|%s|%s\n"%(action, table, data_file, ctrl_file, row_count, status, run_mode))
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


def split_time(from_date='2017-05-01', to_date='2017-05-10', hours=1):

    datetime_snippet_list = []

    from_datetime = datetime.datetime.strptime(from_date, '%Y-%m-%d')
    to_datetime = datetime.datetime.strptime(to_date, '%Y-%m-%d') + datetime.timedelta(seconds=-1) # not include the day

    snippet_from_datetime = from_datetime
    snippet_to_datetime = from_datetime

    while snippet_to_datetime < to_datetime:
        snippet = []
        snippet_to_datetime = snippet_from_datetime + datetime.timedelta(hours=hours)
        if snippet_to_datetime >= to_datetime:
            snippet_to_datetime = to_datetime

        snippet.append(snippet_from_datetime.strftime('%Y-%m-%d %H:%M:%S'))
        snippet.append( snippet_to_datetime.strftime('%Y-%m-%d %H:%M:%S'))

        snippet_from_datetime = snippet_to_datetime + datetime.timedelta(seconds=1)

        datetime_snippet_list.append(snippet)

    return datetime_snippet_list
