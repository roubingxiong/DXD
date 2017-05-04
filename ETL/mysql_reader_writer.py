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

today = datetime.date.today()
yestoday = today - datetime.timedelta(days=1)
checkAcc_date = yestoday.strftime('%Y-%m-%d')

todayYYYYMMDD = today.strftime('%Y%m%d')

logger = logging.getLogger('DXD_ETL')

class BaseReaderWriter():

    def __init__(self, conn): # the config file put in the same location with this script

        self.conn = conn
        self.charset = conn.get_character_set_info()['name']


    def getColumnList(self, table):
        logger.info('getting columns from table ddl')
        colList = []
        sql = "desc %s"% table
        try:
            logger.info('running sql->%s', sql)
            cursor = self.conn.cursor()
            cursor.execute(sql)
            for row in cursor.fetchall():
                colList.append(row[0])
        except:
            raise
        else:
            return colList

    def setCharset(self, charset='utf8'):
        self.charset = charset
        return self.charset

class TableExtractor(BaseReaderWriter):

    def __init__(self, conn):
        BaseReaderWriter.__init__(self, conn)

    def extractTableToFile(self, messager={}, loadType='INC', delimiter='|'):
        try:
            ctrlFilePathName = messager['ctrl_path_file']
            dataFilePathName = messager['data_path_file']
            fromDateStr = messager['from_date']
            toDateStr = messager['to_date']
            table = messager['table_name']
            mode = messager['mode']
            open(dataFilePathName, 'w+')

            dataFile = codecs.open(dataFilePathName, 'w+', self.charset)
            ctrlFile = codecs.open(ctrlFilePathName, 'w+', self.charset)
        except:
            raise

        colList = self.getColumnList(table)
        colStr = '' #used in sql
        header = '' #used in data file
        separator = delimiter
        linesep = os.linesep
        for index in range(len(colList)):
            if index == 0:
                colStr = colList[index]
                header = colList[index]
            else:
                colStr = colStr + ', ' + colList[index]
                header = header + separator + colList[index]

        logger.info('column->%s',colStr)
        logger.debug('header->%s',header)

        ex_sql = "select concat_ws('%s',%s) FROM %s where update_time>='%s' and update_time<'%s';" % (separator, colStr, table, fromDateStr, toDateStr)

        try:
            logger.info('running extract sql->%s', ex_sql)
            cursor = self.conn.cursor()
            cursor.execute(ex_sql)
            ctrlCnt = cursor.rowcount;
            logger.debug('%s rows return', ctrlCnt)

            logger.info('writing records to data file->%s',dataFilePathName)
            dataFile.write(header + linesep)

            for row in cursor.fetchall():
                dataFile.write(row[0] + linesep)
                dataFile.flush()

            logger.info('writing control file->%s',ctrlFilePathName)
            ctrlFile.writelines([str(ctrlCnt), linesep, table, linesep, fromDateStr + separator + toDateStr, linesep, separator,linesep,mode])

            messager['ctrl_count'] = ctrlCnt

        except:
            self.conn.rollback()
            ctrlFile.close()
            dataFile.close()
            os.remove(dataFilePathName)
            os.remove(ctrlFilePathName)
            logger.exception('Exception occured, all database operation rollback, data and control file removed')
            raise
        else:
            logger.info('Congrats! Table extracted successfully!')
        finally:
            logger.info('close database connection')
            self.conn.close()
            return messager



class TableLoader(BaseReaderWriter):

    def __init__(self, conn):
        BaseReaderWriter.__init__(self, conn)

    def loadFileToTable(self, messager,checkCtrlFile='Y'):
        table = messager['table_name']

        ctrlFilePathName = messager['ctrl_path_file']
        dataFilePathName = messager['data_path_file']

        try:
            dataFile = codecs.open(dataFilePathName, 'r', self.charset)
            ctrlFile = codecs.open(ctrlFilePathName, 'r', self.charset)
        except:
            logger.error('failed to open data file(%s) or control file(%s)', dataFilePathName, ctrlFilePathName)
            raise

        logger.info('reading control file->%s', ctrlFilePathName)
        ctrlInfo = ctrlFile.readlines()
        ctrlCnt = str(ctrlInfo[0]).replace('\n','').replace('\r','')
        ctrlTable = str(ctrlInfo[1]).replace('\n','').replace('\r','')
        ctrlDate = str(ctrlInfo[2]).replace('\n','').replace('\r','')
        ctrlSeparator = str(ctrlInfo[3]).replace('\n','').replace('\r','')
        mode = str(ctrlInfo[4]).replace('\n','').replace('\r','')
        logger.info('Information from control file\n\tcount:%s\tsource table:%s\tdate:%s\tdelimiter:%s\tmode:%s',ctrlCnt, ctrlTable, ctrlDate,ctrlSeparator,mode)

        logger.info('reading data file->%s', dataFilePathName)
        recordList = []
        colStr = ''
        colPos = ''
        header = dataFile.readline()
        colList = header.split(ctrlSeparator)

        logger.debug('column list in data file->%s', colList)

        id_index = colList.index('id')  # got index of the field called "id"
        logger.debug('id_index->%s', id_index)
        idList = []

        # dataFile.seek(0,2)
        for row in dataFile.readlines():
            # logger.info('first row->%s',row)
            # exit()
            row_list = row.split(ctrlSeparator)
            #print row_list
            idList.append(row_list[id_index])
            recordList.append(row_list)

        # exit()
        logger.debug('id list->%s', idList)

        logger.info('control file validation')
        rowCnt = len(recordList)

        if int(rowCnt) == int(ctrlCnt):
            # print "-INFO: row count  match control count [%s]"% rowCnt
            logger.info('row count  match control count [%s]', ctrlCnt)
        else:
            # print "-INFO: row count [%s] mismatch control count [%s]"% rowCnt, ctrlCnt
            logger.error('row count [%s] mismatch control count [%s]', rowCnt, ctrlCnt)
            raise Exception('row count [%s] mismatch control count [%s]', rowCnt, ctrlCnt)

        for index in range(len(colList)):
            if index == 0:
                colStr = colList[index]
                colPos = "%s"
            else:
                colStr = colStr + ', ' + colList[index]
                colPos = colPos + ',' + '%s'

        # print "-INFO: column-> " + colStr
        logger.info('columns in data file->%s', colStr)

        tgt_del_sql = "delete from %s WHERE id in "%table + str(tuple(idList)).replace('u','')

        tgt_ins_sql = "INSERT INTO " + table + " (" + colStr + ") values (" + colPos + ") "

        try:
            cursor = self.conn.cursor()
            step = 1000
            logger.info('cleaning table %s in target database with duplicate id, sql->%s',table, tgt_del_sql)
            # cursor.execute(tgt_del_sql)
            dup_id_count = len(idList)
            for i in range(0, dup_id_count, step):
                tgt_del_sql = "delete from %s WHERE id in "%table + str(tuple(idList[i:i+step])).replace('u','')

                if i + step >= dup_id_count:
                    # print "-INFO: inserting " + str(rowCnt%step) + " rows"
                    logger.debug('deleting %s duplicate rows', dup_id_count%step)
                else:
                    # print "-INFO: inserting " + str(step) + " rows"
                    logger.debug('deleting %s duplicate rows', step)

                cursor.execute(tgt_del_sql)



            logger.debug("delete %s duplicate rows",dup_id_count)

            logger.info('loading data into target table')

            for i in range(0, rowCnt, step):
                if i + step >= rowCnt:
                    # print "-INFO: inserting " + str(rowCnt%step) + " rows"
                    logger.debug('inserting %s rows', rowCnt%step)
                else:
                    # print "-INFO: inserting " + str(step) + " rows"
                    logger.debug('inserting %s rows', step)

                bulk_record = recordList[i:i+step]

                cursor.executemany(tgt_ins_sql, bulk_record)
            messager['ctrl_count'] = rowCnt
            self.conn.commit()
            logger.info('changes committed')
        except:
            self.conn.rollback()
            logger.exception('errors occurs while loading records, the transaction roll back, database would not be effected')
            raise
        else:
            logger.info('Congrats! File loading successfully')
        finally:
            ctrlFile.close()
            dataFile.close()
            logger.info('close database connection')
            self.conn.close()
            return messager
