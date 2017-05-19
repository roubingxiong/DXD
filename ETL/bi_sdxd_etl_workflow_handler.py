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

    messager = {'table_name': '', 'action_type':'','data_hub':'',  'data_file': '', 'ctrl_file': '', 'ctrl_count': '', 'data_path_file': '', 'ctrl_path_file': '', 'status': 'Start', 'from_date': '9999-12-31', 'to_date': '9999-12-31','log':'log'}
    status_list = [messager]

    print '-INFO: parsing arguments'
    #global runDate, runMode, actionType, tblName
    runDate, runMode, actionType, tblName = parse_args()

    # preparing logger
    #global logger
    logger, logger_file = get_logger(curr_dir=curr_dir, table_name=tblName, run_date=runDate, start_time=start_time, action_type=actionType)
    logger.info('job start at %s', start_time)

    try:
        logger.info('get configuration from config file')
        config = get_config(cfg_dir=curr_dir)

        rpt_dir = config.get('global', 'report_dir')
        report_file_name, report_file = get_report_file(rpt_dir=rpt_dir, table_name=tblName, run_date=runDate, action_type=actionType)

        # get database connection
        tgt_conn = get_conn_by_type(config, 'target')
        src_conn = get_conn_by_type(config, 'source')
        data_hub = config.get('global', 'data_hub')

        # preparing table list for processing
        if tblName.lower() == 't_decision_rule_log':
            messager = utils.rebuild_messager(actionType, runDate, runMode, tblName, config, logger_file, messager)
            table_list = get_decision_rule_log_table_list(messager=messager, conn=src_conn)
        elif tblName == 'normal':
            table_list_file = config.get('global', 'table_list')
            table_list = get_table_list(table_list_file)
        elif tblName == 'all':
            table_list_file = config.get('global','table_list')
            table_list = get_table_list(table_list_file)

            messager = utils.rebuild_messager(actionType, runDate, runMode, 't_decision_rule_log', config, logger_file, messager)
            log_table_list = get_decision_rule_log_table_list(src_conn, messager)

            table_list = table_list + log_table_list
        else:
            table_list = [tblName]
        logger.info('full list of pending tables->%s', table_list)

        # core workflow for extracting or loading data
        if actionType == 'extract':
            logger.info('********** creating table extractor **********')
            extractor = TableExtractor(src_conn)
            for table in table_list:
                try:
                    logger.info('>>>>>>>>>> extracting table->%s >>>>>>>>>>', table)
                    messager = utils.rebuild_messager(actionType, runDate, runMode, table, config, logger_file, messager)
                    check_table(src_conn=src_conn, table_name=table)
                    extractor.extractTableToFile(messager)
                except Exception as e:
                    messager['status'] = 'Fail'
                    logger.exception(e.message)
                else:
                    messager['status'] = 'Success'
                finally:
                    logger.debug(messager)
                    logger.info('<<<<<<<<<< end extract table->%s <<<<<<<<<<', table)
                    write_rept(report_file, messager)

        elif actionType == 'load':
            logger.info('********** creating table loader **********')
            loader = TableLoader(tgt_conn)
            for table in table_list:
                try:
                    logger.info('>>>>>>>>>> loading table->%s >>>>>>>>>>', table)
                    messager = utils.rebuild_messager(actionType, runDate, runMode, table, config, logger_file, messager)
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
                    logger.debug(messager)
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
        # email_handler.sendJobStatusEmail(config=config, messager=status_list, attachment=[logger_file])
