#!/usr/bin/env python
# -*- coding: UTF-8 -*-
# Date:2017-05-17

__author__ = 'roubingxiong'

import datetime

import MySQLdb

from email_handler import *

logger = logging.getLogger('DXD_ETL')

def build_message_box(actionType, runDate, runMode, tableName, config=ConfigParser.ConfigParser(), logFile='', srcConn=None, tgtConn=None):
    # msg = {'table_name':None  ,'data_file':None ,'ctrl_file':None ,'ctrl_count':None
    #     ,'data_path_file':None ,'ctrl_path_file':None , 'from_time':None , 'to_time':None, 'status':None
    #     , 'from_date':None, 'to_date':None, 'action_type':None, 'run_mode':None}
    msg_list = []

    env_info = {'action_type':None, 'run_mode':None, 'config':None, 'log_file':None, 'from_date':None, 'to_date':None, 'data_hub':None}

    commander = {'env_info':None, 'msg_list':None}

    default_run_mode = 't+3'

    logger.info('preparing environment variables')
    # preparing environment variables
    if runMode in ('None', ''):
        try:
            runMode = config.get('global', 'runMode')
            logger.debug('get run mode [%s] from config file', runMode)
        except  Exception as e:
            logger.warn(e.message)
            logger.debug("run mode do not assigned, using default value '%s'", default_run_mode)
            runMode = default_run_mode

    offset = runMode.replace(' ', '').replace('T', '').replace('t', '')

    if runDate in ('None', ''):
        runDate = datetime.datetime.today()
        logger.warn("run date not assigned, using the date of today as the run date")
    else:
        runDate = datetime.datetime.strptime(runDate, '%Y%m%d')

    to_date = runDate
    from_date = to_date - datetime.timedelta(days=int(offset))

    from_date_yyyy_mm_dd = from_date.strftime('%Y-%m-%d')

    to_date_yyyymmdd = to_date.strftime('%Y%m%d')
    from_date_yyyymmdd = from_date.strftime('%Y%m%d')

    to_time_Y_m_d_H_M_S = (to_date - datetime.timedelta(seconds=1)).strftime('%Y-%m-%d %H:%M:%S')  # to_date - 1 second = including last second of previous day
    from_time_Y_m_d_H_M_S = from_date.strftime('%Y-%m-%d %H:%M:%S')

    to_date = to_date - datetime.timedelta(days=1)  # reset to_date to be previous day
    to_date_yyyy_mm_dd = to_date.strftime('%Y-%m-%d')

    data_hub = config.get('global', 'data_hub')

    # load environment information
    env_info['action_type'] = actionType
    env_info['run_mode'] = runMode
    env_info['config'] = config
    env_info['from_date'] = from_date_yyyy_mm_dd
    env_info['to_date'] = to_date_yyyy_mm_dd
    env_info['log_file'] = logFile
    env_info['data_hub'] = data_hub

    logger.debug('base environment information:\n%s', env_info)

    logger.info('preparing table cases')
    table_metadata_list = []

    if not srcConn:
        src_conn = get_conn_by_type(config=config, type='source')
    else:
        src_conn = srcConn

    if tableName.lower() == 't_decision_rule_log':
        table_metadata_list = get_decision_rule_log_table_list(from_time=from_time_Y_m_d_H_M_S, to_time=to_time_Y_m_d_H_M_S, conn=src_conn)
    elif tableName == 'normal':
        table_list_file = config.get('global', 'table_list')
        table_metadata_list = get_table_list(table_list_file)
    elif tableName == 'all':
        table_list_file = config.get('global','table_list')
        table_metadata_list = get_table_list(table_list_file)

        log_table_metadata_list = get_decision_rule_log_table_list(from_time=from_time_Y_m_d_H_M_S, to_time=to_time_Y_m_d_H_M_S, conn=src_conn)

        table_metadata_list = table_metadata_list + log_table_metadata_list
    else:
        table_metadata_list = [(tableName, '', '')]

    for table_name, from_time, to_time in table_metadata_list:
        msg = {}
        msg['table_name'] = table_name
        msg['data_hub'] = config.get('global', 'data_hub')

        data_file = table_name + ".dat." + from_date_yyyymmdd + "." + to_date_yyyymmdd
        msg['data_file'] = data_file

        ctrl_file = table_name + ".ctrl." + from_date_yyyymmdd + "." + to_date_yyyymmdd
        msg['ctrl_file'] = ctrl_file

        msg['ctrl_count'] = ''
        msg['data_path_file'] = os.path.join(data_hub , data_file)
        msg['ctrl_path_file'] = os.path.join(data_hub , ctrl_file)
        if not '' in (from_time, to_time):
            msg['from_time'] = from_time
            msg['to_time'] = to_time
        else:
            msg['from_time'] = from_time_Y_m_d_H_M_S
            msg['to_time'] = to_time_Y_m_d_H_M_S

        msg['status'] = ''

        msg_list.append(msg)

    logger.debug('metadata for processing tables:\n%s', msg_list)

    commander['env_info'] = env_info
    commander['msg_list'] = msg_list

    return commander


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


def get_report_file(rpt_dir, table_name, run_date, action_type, from_date, to_date):

    base_name = "%s_%s_%s_%s_%s.rpt" % (action_type, run_date, table_name, from_date, to_date)
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


def get_init_status_list(action_type, table_name, run_mode, to_date):
    # print(file)
    status_list = []

    row = {}
    row['action_type'] = action_type
    row['table_name'] = table_name
    row['data_file'] = ''
    row['ctrl_file'] = ''
    row['ctrl_count'] = ''
    row['status'] = 'Fail'
    row['run_mode'] = run_mode
    row['from_date'] = ''
    row['to_date'] = to_date

    status_list.append(row)

    return status_list


def get_table_list(table_list_file):

    table_metadata_list = []

    try:
        with open(table_list_file, 'r') as tbl:
            for line in tbl.readlines():
                table_metadata = (line.strip(), '', '') #(table_name, from_time, to_time)
                table_metadata_list.append(table_metadata)
    except:
        raise
    finally:
        return table_metadata_list


def get_decision_rule_log_table_list(from_time, to_time, conn):

    table_metadata_list = []

    # log_tbl_sql = "SELECT CAST(apply_time as DATE) AS log_tbl_date FROM t_decision_application WHERE update_time >='%s' and update_time < '%s' GROUP BY 1;" % (from_time, to_time)
    log_tbl_sql = "SELECT CONCAT('t_decision_rule_log_', DATE_FORMAT(apply_time, '%%Y%%m%%d')) AS log_table, CAST(min(update_time) AS CHAR(19)) AS min_update_time, CAST(max(update_time) AS CHAR(19)) AS max_update_time  FROM t_decision_application WHERE update_time >='%s' and update_time <= '%s' GROUP BY 1;" % (from_time, to_time)

    try:
        cursor = conn.cursor()
        logger.info('running sql to get log table date->%s', log_tbl_sql)
        cursor.execute(log_tbl_sql)

        for item in cursor.fetchall():
            table_metadata_list.append(item)

        logger.debug('get metadata for incremental(table_name, min_update_time, max_update_time):\n%s',table_metadata_list)
    except:
        raise
    finally:
        return table_metadata_list

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


def split_time(from_datetime='1900-01-01 00:00:00', to_datetime='1900-01-01 23:59:59', hours=1):

    datetime_snippet_list = []

    from_datetime = datetime.datetime.strptime(from_datetime, '%Y-%m-%d %H:%M:%S')
    to_datetime = datetime.datetime.strptime(to_datetime, '%Y-%m-%d %H:%M:%S')

    snippet_from_datetime = from_datetime
    snippet_to_datetime = from_datetime

    while snippet_to_datetime < to_datetime:

        snippet_to_datetime = snippet_from_datetime + datetime.timedelta(hours=hours)
        if snippet_to_datetime >= to_datetime:
            snippet_to_datetime = to_datetime

        datetime_snippet_list.append((snippet_from_datetime.strftime('%Y-%m-%d %H:%M:%S'), snippet_to_datetime.strftime('%Y-%m-%d %H:%M:%S')))

        snippet_from_datetime = snippet_to_datetime + datetime.timedelta(seconds=1)

    # print(datetime_snippet_list)
    return datetime_snippet_list