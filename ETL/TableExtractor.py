#!/usr/bin/env python
# -*- coding: UTF-8 -*-
# Date:2017-05-16

__author__ = 'roubingxiong'

import os
import logging
import codecs
import sys
from BaseReaderWriter import BaseReaderWriter
import split_time


logger = logging.getLogger('DXD_ETL')

class TableExtractor(BaseReaderWriter):

    def __init__(self, conn):
        BaseReaderWriter.__init__(self, conn)

    def extractTableToFile(self, messager={}, delimiter='|~|'):
        try:
            ctrlFilePathName = messager['ctrl_path_file']
            dataFilePathName = messager['data_path_file']
            fromDateStr = messager['from_date']
            toDateStr = messager['to_date']
            table = messager['table_name']
            runMode = messager['run_mode']
            dataFile = codecs.open(dataFilePathName, 'w+', self.charset)
            logger.info('create data file->%s', dataFilePathName)
            ctrlFile = codecs.open(ctrlFilePathName, 'w+', self.charset)
            logger.info('create control file->%s', ctrlFilePathName)
        except:
            raise

        try:
            col_list = self.getColumnList(table)

            col_str_list = []
            for col in col_list:
                col_str_list.append("ifnull(%s, '')" % col)  #  convert null to empty, otherwise the column would be missed in data file

            col_str = ','.join(col_str_list) #used in sql

            separator = delimiter
            header = separator.join(col_list) #used in data file

            logger.info('column->%s',col_str)

            logger.info('writing header to data file\n%s', header)
            dataFile.write(header + '\n')

            datetime_snipet_list = split_time.split_time(from_date=fromDateStr, to_date=toDateStr, hours=1)
            ctrlCnt = 0
            cursor = self.conn.cursor()
            for from_time, to_time in datetime_snipet_list:
                ex_sql = "select REPLACE(REPLACE(concat_ws('%s',%s), CHAR(10), ''), CHAR(13), '')  FROM %s where update_time>='%s' and update_time<='%s';" % (separator, col_str, table, from_time, to_time) # not include end date

                logger.info('running extract sql->%s', ex_sql)
                cursor.execute(ex_sql)
                row_count = cursor.rowcount;
                ctrlCnt = ctrlCnt + row_count

                for row in cursor.fetchall():
                    dataFile.write(row[0] + '\n')
                    dataFile.flush()

                logger.debug('batch read and write [%s] rows', row_count)

            logger.debug('total [%s] rows return', ctrlCnt)

            ctrl_info = "%s\n%s\n%s%s%s\n%s\n%s"%(ctrlCnt, table, fromDateStr, separator, toDateStr, separator, runMode)
            logger.info('writing information to control file\n%s', ctrl_info)
            ctrlFile.write(ctrl_info)

            messager['ctrl_count'] = ctrlCnt
        except:
            self.conn.rollback()
            ctrlFile.close()
            dataFile.close()
            os.remove(dataFilePathName)
            os.remove(ctrlFilePathName)
            logger.exception('Exception occurred while generating data&control file. All database operation rollback, data and control file removed')
            raise
        else:
            logger.info('Congrats! Table %s extracted successfully!', table)
        # finally:
            # logger.info('close database connection')
            # self.conn.close()
            # return messager
            # pass




