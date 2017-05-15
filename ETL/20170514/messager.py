# -*- coding: UTF-8 -*-
#!/usr/bin/python

import logging.config
import datetime
import os

logger = logging.getLogger('DXD_ETL')


class Messager():

    def __init__(self):

        self.messager = {
            'table_name': '',
            'action_type':'',
            'data_hub':'',
            'data_file': '',
            'ctrl_file': '',
            'ctrl_count': '',
            'data_path_file': '',
            'ctrl_path_file': '',
            'status': 'Start',
            'from_date': '9999-12-31',
            'to_date': '9999-12-31',
            'log':'log'
        }

    def set_table_name(self, table):
        self.messager['table_name'] = table
        return self

    def get_table_name(self):
        return self.messager['table_name']

    def set_action_type(self, action_type):
        self.messager['action_type'] = action_type
        return self



    def reload_messager(actionType, runDate, runMode,  tblName, config, logFile, messager={}):
        messager['config'] = config
        messager['log'] = logFile
        default_run_mode = 't+3'

        if runMode in ('None', ''):
            try:
                runMode = config.get('global', 'runMode')
            except Exception as e:
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