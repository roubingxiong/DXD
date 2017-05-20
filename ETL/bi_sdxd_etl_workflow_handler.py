#!/usr/bin/env python2
# -*- coding: utf-8 -*-

from utils import *
from TableExtractor import TableExtractor
from TableLoader import TableLoader
import file_watcher
# import TableLoader, TableExtractor, file_watcher
import email_handler
import argparse
import utils

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

    print '-INFO: parsing arguments'
    # parsing & validate arguments
    runDate, runMode, actionType, tblName = parse_args()

    # initial status list
    status_list = get_init_status_list(action_type=actionType, table_name=tblName, run_mode=runMode, to_date=runDate)

    try:
        # preparing logger
        logger, logger_file = get_logger(curr_dir=curr_dir, table_name=tblName, run_date=runDate, start_time=start_time, action_type=actionType)
        logger.info('---------- workflow start at %s----------', datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

        logger.info('get configuration from config file')
        config = get_config(cfg_dir=curr_dir)

        # get database connection
        tgt_conn = get_conn_by_type(config, 'target')
        src_conn = get_conn_by_type(config, 'source')
        data_hub = config.get('global', 'data_hub')

        logger.info('build message box')
        commander = build_message_box(actionType=actionType, runDate=runDate, runMode=runMode, tableName=tblName, config=config, logFile=logger_file, srcConn=src_conn)
        msg_list = commander['msg_list']
        env_info = commander['env_info']

        logger.info('getting report file')
        from_date = env_info['from_date']
        to_date = env_info['to_date']
        rpt_dir = config.get('global', 'report_dir')
        report_file_name, report_file = get_report_file(rpt_dir=rpt_dir, table_name=tblName, run_date=runDate, action_type=actionType, from_date=from_date, to_date=to_date)


        # core workflow for extracting or loading data
        if actionType == 'extract':
            logger.info('********** creating table extractor **********')
            extractor = TableExtractor(src_conn)
            for msg in msg_list:
                try:
                    table_name = msg['table_name']
                    logger.info('>>>>>>>>>> extracting table->%s >>>>>>>>>>', table_name)
                    msg = dict(msg, **env_info)  # attach the env_info
                    check_table(src_conn=src_conn, table_name=table_name)
                    extractor.extractTableToFile(msg)
                except Exception as e:
                    msg['status'] = 'Fail'
                    logger.exception(e.message)
                else:
                    msg['status'] = 'Success'
                finally:
                    logger.debug(msg)
                    logger.info('<<<<<<<<<< end extract table->%s <<<<<<<<<<', table_name)
                    write_rept(report_file, msg)

        elif actionType == 'load':
            logger.info('********** creating table loader **********')
            loader = TableLoader(tgt_conn)
            for msg in msg_list:
                try:
                    table_name = msg['table_name']
                    logger.info('>>>>>>>>>> loading table->%s >>>>>>>>>>', table_name)
                    msg = dict(msg, **env_info)  # merge msg and env_info
                    check_table(src_conn=src_conn, tgt_conn=tgt_conn, table_name=table_name)
                    ctrl_file = msg['ctrl_file'] # tableName.ctrl.yyyymmdd.yyyymmdd
                    if file_watcher.watch_file(dir=data_hub, filename=ctrl_file):
                        loader.loadFileToTable(msg)
                except Exception as e:
                    msg['status'] = 'Fail'
                    logger.exception(e.message)
                else:
                    msg['status'] = 'Success'
                finally:
                    logger.debug(msg)
                    logger.info('<<<<<<<<<< end load table->%s <<<<<<<<<<', table_name)
                    write_rept(report_file, msg)

        logger.info('getting status list for email')
        status_list = get_status_list(report_file_name, env_info['from_date'], env_info['to_date'])
    except Exception as e:
        logger.exception(e.message)
        logger.error('some exception occurred, please check exception message')
        raise
    else:

        logger.info('clean report file older than 30 days')
        data_file_list = file_watcher.clean_file(dir=rpt_dir, days=30)

        logger.info('clean data file older than 7 days')
        data_file_list = file_watcher.clean_file(dir=data_hub, days=7)

        logger.info('clean log file older than 30 days')
        log_dir = os.path.dirname(logger_file)
        log_file_list = file_watcher.clean_file(dir=log_dir, days=30)
    finally:
        tgt_conn.close()
        src_conn.close()
        report_file.close()

        logger.info('sending email notification')
        email_handler.sendJobStatusEmail(config=config, messager=status_list, attachment=[logger_file])

        logger.info('---------- workflow complete at %s----------', datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
