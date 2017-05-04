# -*- coding: UTF-8 -*-
#!/usr/bin/env python2

__author__ = 'zhxiong'
import MySQLdb # this is a third party module, you can find from internet and install it
import time
import datetime
import os
from os import path, access, R_OK, W_OK
import ConfigParser
import logging
import codecs

import sys


today = datetime.date.today()
yestoday = today - datetime.timedelta(days=1)
checkAcc_date = yestoday.strftime('%Y-%m-%d')

todayYYYYMMDD = today.strftime('%Y%m%d')

logger = logging.getLogger('DXD_ETL')

class BaseReaderWriter():

    def __init__(self, conn): # the config file put in the same location with this script

        self.conn = conn
        self.charset = conn.get_character_set_info()['name']
        reload(sys)
        sys.setdefaultencoding(self.charset)

    def getColumnList(self, table):
        logger.info('getting columns from table ddl')
        col_list = []
        sql = "desc %s;"% table
        try:
            logger.info('running sql->%s', sql)
            cursor = self.conn.cursor()
            cursor.execute(sql)
            for row in cursor.fetchall():
                col_list.append(row[0])
        except:
            raise
        else:
            logger.info('column list->%s', col_list)
            return col_list

    def setCharset(self, charset='utf8'):
        self.charset = charset
        return self.charset

class TableExtractor(BaseReaderWriter):

    def __init__(self, conn):
        BaseReaderWriter.__init__(self, conn)

    def extractTableToFile(self, messager={}, delimiter='|'):
        try:
            ctrlFilePathName = messager['ctrl_path_file']
            dataFilePathName = messager['data_path_file']
            fromDateStr = messager['from_date']
            toDateStr = messager['to_date']
            table = messager['table_name']
            mode = messager['mode']
            dataFile = codecs.open(dataFilePathName, 'w+', self.charset)
            logger.info('create data file->%s', dataFilePathName)
            ctrlFile = codecs.open(ctrlFilePathName, 'w+', self.charset)
            logger.info('create control file->%s', ctrlFilePathName)
        except:
            raise

        col_list = self.getColumnList(table)
        col_str = ','.join(col_list) #used in sql
        separator = delimiter
        header = separator.join(col_list) #used in data file

        logger.info('column->%s',col_str)

        ex_sql = "select concat_ws('%s',%s) FROM %s where update_time>='%s' and update_time<'%s';" % (separator, col_str, table, fromDateStr, toDateStr) # not include end date

        try:
            logger.info('running extract sql->%s', ex_sql)
            cursor = self.conn.cursor()
            cursor.execute(ex_sql)
            ctrlCnt = cursor.rowcount;
            logger.debug('total %s rows return', ctrlCnt)

            logger.info('writing header to data file\n%s', header)
            dataFile.write(header + '\n')

            logger.info('writing data to data file')
            for row in cursor.fetchall():
                dataFile.write(row[0] + '\n')
                dataFile.flush()

            ctrl_info = "%s\n%s\n%s%s%s\n%s\n%s"%(ctrlCnt, table, fromDateStr, separator, toDateStr, separator, mode)
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
        finally:
            logger.info('close database connection')
            self.conn.close()
            return messager


class TableLoader(BaseReaderWriter):

    def __init__(self, conn):
        BaseReaderWriter.__init__(self, conn)

    def loadFileToTable(self, messager, checkCtrlFile='Y'):

        table = messager['table_name']
        ctrlFilePathName = messager['ctrl_path_file']
        dataFilePathName = messager['data_path_file']

        try:
            # dataFile = open(dataFilePathName, 'r')
            dataFile = codecs.open(dataFilePathName, 'r', self.charset)
            logger.info('opening data file->%s', dataFilePathName)
            # ctrlFile = open(ctrlFilePathName, 'r')
            ctrlFile = codecs.open(ctrlFilePathName, 'r', self.charset)
            logger.info('openings data file->%s', ctrlFilePathName)
        except:
            logger.error('failed to open data file(%s) or control file(%s)', dataFilePathName, ctrlFilePathName)
            raise

        logger.info('reading control file')
        ctrlInfo = ctrlFile.readlines()
        ctrlCnt = str(ctrlInfo[0]).strip()
        ctrlTable = str(ctrlInfo[1]).strip()
        ctrlDate = str(ctrlInfo[2]).strip()
        ctrlSeparator = str(ctrlInfo[3]).strip()
        mode = str(ctrlInfo[4]).strip()
        logger.info('control information:\ncount:%s\nsource table:%s\ndate:%s\ndelimiter:%s\nmode:%s',ctrlCnt, ctrlTable, ctrlDate,ctrlSeparator,mode)

        logger.info('reading header of data file')
        col_position = []
        header = dataFile.readline().strip()    # C1|C2|C3

        col_str = header.replace(ctrlSeparator, ',') # C1,C2,C3
        logger.debug('column in data file->%s', col_str)

        col_list = header.split(ctrlSeparator)  # ['C1', 'C2', 'C3']

        for n in range(len(col_list)):
            col_position.append('%s')

        col_position = ','.join(col_position)   # %s,%s,%s

        try:
            id_index = col_list.index('id')  # got index of the field called "id"
            logger.debug('column "id" position->%s', id_index)
        except:
            logger.error('table %s do not have primary key "id", abnormally', table)
            raise

        logger.info('reading data of data file')
        record_list = []
        id_list = []

        for row in dataFile.readlines():
            # print row.decode('utf8')
            row_list = str(row).strip().split(ctrlSeparator)
            id_list.append(row_list[id_index])
            # logger.debug(id_list)
            record_list.append(row_list)

        logger.info('control file validation')
        rowCnt = len(record_list)

        if int(rowCnt) == int(ctrlCnt):
            # print "-INFO: row count  match control count [%s]"% rowCnt
            logger.info('row count  match control count [%s]', ctrlCnt)
        else:
            # print "-INFO: row count [%s] mismatch control count [%s]"% rowCnt, ctrlCnt
            logger.error('row count [%s] mismatch control count [%s]', rowCnt, ctrlCnt)
            raise Exception('row count [%s] mismatch control count [%s]', rowCnt, ctrlCnt)

        try:
            cursor = self.conn.cursor()
            step = 1000

            logger.info('deleting duplicate from target table %s', table)
            del_id_total = 0
            for i in range(0, len(id_list), step):
                bulk_id = str(tuple(id_list[i:i+step]))
                tgt_del_sql = "delete from %s WHERE id in %s" % (table, bulk_id)

                cursor.execute(tgt_del_sql)

                del_row_count = cursor.rowcount
                logger.debug('deleting %s rows', del_row_count)
                del_id_total = del_id_total + cursor.rowcount

                if del_row_count == 0:
                    logger.info('no duplicate id exist in target table')
                    break

            logger.debug("total [%s] duplicate rows deleted", del_id_total)

            logger.info('inserting new record into target table %s', table)
            tgt_ins_sql = "INSERT INTO %s (%s) values (%s) " % (table, col_str, col_position)
            ins_id_total = 0
            for i in range(0, rowCnt, step):
                bulk_record = record_list[i:i+step]

                cursor.executemany(tgt_ins_sql, bulk_record)

                ins_row_count = cursor.rowcount
                logger.debug('inserting %s rows', ins_row_count)
                ins_id_total = ins_id_total + ins_row_count
            logger.debug("total [%s] new rows inserted", ins_id_total)

            messager['ctrl_count'] = rowCnt
            self.conn.commit()
            logger.info('changes committed')
        except:
            self.conn.rollback()
            logger.exception('Exception occured while loading records, the transaction roll back, database would not be effected')
            raise
        else:
            logger.info('Congrats! File loading successfully')
        finally:
            ctrlFile.close()
            dataFile.close()
            logger.info('close database connection')
            self.conn.close()
            return messager
