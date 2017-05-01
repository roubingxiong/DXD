# -*- coding: UTF-8 -*-
#!/usr/bin/python

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
        self.status = 'In Progress'   # I->In Progress S->Success  F->Fail
        self.charset = conn.get_character_set_info()['name']
        self.messager = {'status':self.status}

    def refreshETLMessager(self, messager={}):
        self.messager = messager

        fromDateStr = messager['from_date']
        location = messager['data_hub']
        table = messager['table']

        format_date_str = datetime.datetime.strptime(fromDateStr, '%Y-%m-%d').strftime('%Y%m%d')

        dataFileName = table + '.dat.' + format_date_str  # + '.' + nowStr
        dataFilePathName = os.path.join(location, dataFileName)
        ctrlFileName = table + '.ctrl.' + format_date_str
        ctrlFilePathName = os.path.join(location, ctrlFileName)
        ctrlCnt = 0

        toDateStr = (datetime.datetime.strptime(fromDateStr, '%Y-%m-%d') - datetime.timedelta(days=-1)).strftime('%Y-%m-%d')

        self.messager['data_file'] = dataFileName
        self.messager['ctrl_file'] = ctrlFileName
        self.messager['ctrl_count'] = ctrlCnt
        self.messager['data_path_file'] = dataFilePathName
        self.messager['ctrl_path_file'] = ctrlFilePathName
        self.messager['to_date'] = toDateStr

        return self.messager

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
        except Exception:
            raise
        else:
            return colList

    def setCharset(self, charset='utf8'):
        self.charset = charset
        return self.charset

    def setStatus(self, status='In Progress'):
        self.status = status
        self.messager['status'] = self.status
        return self.status


class TableExtractor(BaseReaderWriter):

    def __init__(self, conn):
        BaseReaderWriter.__init__(self, conn)

    def extractTableToFile(self, messager={}, loadType='INC', delimiter='|'):

        self.refreshETLMessager(messager)

        try:
            ctrlFilePathName = self.messager['ctrl_path_file']
            dataFilePathName = self.messager['data_path_file']
            fromDateStr = self.messager['from_date']
            toDateStr = self.messager['to_date']
            table = self.messager['table']
            dataFile = codecs.open(dataFilePathName, 'w', self.charset)
            ctrlFile = codecs.open(ctrlFilePathName, 'w', self.charset)
        except:
            raise

        colList = self.getColumnList(table)
        colStr = '' #used in sql
        header = '' #used in data file
        separator = delimiter
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
            dataFile.write(header + '\n')
            for row in cursor.fetchall():
                dataFile.write(row[0] + '\n')
                dataFile.flush()

            logger.info('writing control file->%s',ctrlFilePathName)
            ctrlFile.writelines([str(ctrlCnt), '\n', table, '\n', fromDateStr, '\n', separator])

            self.messager['ctrl_count'] = ctrlCnt
            self.messager['status'] = 'S'

        except Exception as e:
            self.conn.rollback()
            self.messager['status'] = 'Fail'
            ctrlFile.close()
            dataFile.close()
            os.remove(dataFilePathName)
            os.remove(ctrlFilePathName)
            logger.error('Exception occured, all database operation rollback, data and control file removed')
            raise
        else:
            self.setStatus('Success')
            logger.info('Congrats! Table extracted successfully!')
        finally:
            logger.info('close database connection')
            self.conn.close()
            return self.messager



class TableLoader(BaseReaderWriter):

    def __init__(self, conn):
        BaseReaderWriter.__init__(self, conn)

    def loadFileToTable(self, messager,checkCtrlFile='Y'):
        table = messager['table']

        self.refreshETLMessager(messager)

        ctrlFilePathName = self.messager['ctrl_path_file']
        dataFilePathName = self.messager['data_path_file']

        try:
            dataFile = codecs.open(dataFilePathName, 'r', self.charset)
            ctrlFile = codecs.open(ctrlFilePathName, 'r', self.charset)
        except Exception as e:
            logger.error('failed to open data file(%s) or control file(%s)', dataFilePathName, ctrlFilePathName)
            raise

        logger.info('reading control file->%s', ctrlFilePathName)
        ctrlInfo = ctrlFile.readlines()
        ctrlCnt = ctrlInfo[0]
        ctrlTable = ctrlInfo[1]
        ctrlDate = ctrlInfo[2]
        ctrlSeparator = ctrlInfo[3]
        logger.info('Information from control file\n\tcount:%s\tsource table:%s\tdate:%s\tdelimiter:%s',ctrlCnt, ctrlTable, ctrlDate,ctrlSeparator)

        logger.info('reading data file->%s', dataFilePathName)
        recordList = []
        colStr = ''
        colPos = ''
        header = dataFile.readline()
        colList = header.split(ctrlSeparator)#self.getColumnList(table)
        id_index = colList.index('id')  # got index of the field called "id"
        idList = []

        for row in dataFile.readlines():
            row_list = row.split(ctrlSeparator)
            idList.append(row_list[id_index])
            recordList.append(row_list)

        logger.debug('id list->%s', idList)

        logger.info('control file validation')
        rowCnt = len(recordList)
        if int(rowCnt) == int(ctrlCnt):
            # print "-INFO: row count  match control count [%s]"% rowCnt
            logger.info('row count  match control count [%s]', rowCnt)
        else:
            # print "-INFO: row count [%s] mismatch control count [%s]"% rowCnt, ctrlCnt
            logger.info('row count [%s] mismatch control count [%s]', rowCnt, ctrlCnt)
            self.setStatus('Fail')

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
            logger.info('cleaning table %s in target database with duplicate id, sql->%s',table, tgt_del_sql)
            cursor.execute(tgt_del_sql)
            logger.debug("delete %s duplicate rows",cursor.rowcount)

            logger.info('loading data into target table')
            step = 1000
            for i in range(0, rowCnt, step):
                if i + step >= rowCnt:
                    # print "-INFO: inserting " + str(rowCnt%step) + " rows"
                    logger.debug('inserting %s rows', rowCnt%step)
                else:
                    # print "-INFO: inserting " + str(step) + " rows"
                    logger.debug('inserting %s rows', step)

                cursor.executemany(tgt_ins_sql, recordList[i:i+step])
            messager['ctrl_count'] = rowCnt
            self.conn.commit()

        except Exception as e:
            self.conn.rollback()
            self.setStatus('Fail')
            # print "-ERROR: errors occurs while inserting records, the transaction roll back, database would not be effected"
            logger.error('errors occurs while loading records, the transaction roll back, database would not be effected')
            raise
        else:
            self.setStatus('Success')
            # print "-INFO: Congrats! File loading success"
            logger.info('Congrats! File loading successfully')
        finally:
            ctrlFile.close()
            dataFile.close()
            logger.info('close database connection')
            self.conn.close()
            return self.messager
