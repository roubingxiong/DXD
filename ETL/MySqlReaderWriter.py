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

status = {'I': 'In Progress', 'S': 'Success', 'F': 'Fail'}

class BaseReaderWriter():

    def __init__(self, conn): # the config file put in the same location with this script

        self.conn = conn
        self.status = 'I'   # I->In Progress S->Success  F->Fail
        self.charset = conn.get_character_set_info()['name']
        self.etlStat = {'table': '', 'data_file': '', 'ctrl_file': '', 'ctrlCnt': '', 'data_path_file': '', 'ctrl_path_file': '', 'status': self.status, 'from_date': '9999-12-31', 'to_date': '9999-12-31'}

    def refreshETLStat(self, location, runDate, table):
        logger.info('refreshing ETL status with the location, run date and table')
        runDateStr = datetime.datetime.strptime(runDate, '%Y-%m-%d').strftime('%Y%m%d')
        # nowStr = datetime.datetime.today().strftime('%Y%m%d_%H%M%S')
        dataFileName = table + '.dat.' + runDateStr  # + '.' + nowStr
        dataFilePathName = os.path.join(location, dataFileName)
        ctrlFileName = table + '.ctrl.' + runDateStr
        ctrlFilePathName = os.path.join(location, ctrlFileName)
        ctrlCnt = 0
        fromDate = runDate
        toDate = (datetime.datetime.strptime(fromDate, '%Y-%m-%d') - datetime.timedelta(days=-1)).strftime('%Y-%m-%d')
        # refresh extractStat
        self.etlStat['table'] = table
        self.etlStat['data_file'] = dataFileName
        self.etlStat['ctrl_file'] = ctrlFileName
        self.etlStat['ctrlCnt'] = ctrlCnt
        self.etlStat['data_path_file'] = dataFilePathName
        self.etlStat['ctrl_path_file'] = ctrlFilePathName
        self.etlStat['from_date'] = fromDate
        self.etlStat['to_date'] = toDate

        return self.etlStat

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

    def setStatus(self, status='I'):
        self.status = status
        self.etlStat['status'] = self.status
        return self.status


class TableExtractor(BaseReaderWriter):

    def __init__(self, conn):
        BaseReaderWriter.__init__(self, conn)

    def extractTableToFile(self, table, location, runDate='9999-12-31', loadType='INC', delimiter='|'):

        self.refreshETLStat(location, runDate, table)

        ctrlFilePathName = self.etlStat['ctrl_path_file']
        dataFilePathName = self.etlStat['data_path_file']
        fromDate = self.etlStat['from_date']
        toDate = self.etlStat['to_date']

        try:
            dataFile = codecs.open(dataFilePathName, 'w', self.charset)
            ctrlFile = codecs.open(ctrlFilePathName, 'w', self.charset)
        except Exception as e:
            logger.exception(e.message)
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

        startRow = 0
        toRow = 1000

        # src_inc_id_sql = "select id, update_time FROM %s WHERE update_time>='%s' and update_time<'%s';"%(table, fromDate, toDate)
        # src_cnt_sql = "select count(1) from %s WHERE update_time>='%s' and update_time<'%s';"%(table, fromDate, toDate)
        ex_sql = "select concat_ws('%s',%s) FROM %s where update_time>='%s' and update_time<'%s';" % (separator, colStr, table, fromDate, toDate)

        try:
            logger.info('running extract sql->%s', ex_sql)
            cursor = self.conn.cursor()
            cursor.execute(ex_sql)
            ctrlCnt = cursor.rowcount;

            logger.info('writing records to data file->%s',dataFilePathName)
            dataFile.write(header + '\n')
            for row in cursor.fetchall():
                dataFile.write(row[0] + '\n')
                dataFile.flush()

            logger.info('writing control file->%s',ctrlFilePathName)
            ctrlFile.writelines([str(ctrlCnt), '\n', table, '\n', runDate, '\n', separator])

            self.etlStat['ctrlCnt'] = ctrlCnt
            self.etlStat['status'] = 'S'

        except Exception as e:
            logger.exception(e.message)
            self.conn.rollback()
            self.etlStat['status'] = 'F'
            ctrlFile.close()
            dataFile.close()
            # os.remove(dataFilePathName)
            # os.remove(ctrlFilePathName)
            logger.error('Exception occured, all database operation rollback')
            raise
        else:
            logger.info('Congrats! Table extracted successfully!')

        finally:
            ctrlFile.close()
            dataFile.close()
            logger.info('close database connection')
            self.conn.close()
            return self.etlStat



class TableLoader(BaseReaderWriter):

    def __init__(self, conn):
        # self.conn = mysqlConn #MySQLdb.connect()
        BaseReaderWriter.__init__(self, conn)

    def loadFileToTable(self, table, location, runDate='9999-12-31', checkCtrlFile='Y'):

        self.refreshETLStat(location, runDate, table)

        ctrlFilePathName = self.etlStat['ctrl_path_file']
        dataFilePathName = self.etlStat['data_path_file']

        try:
            dataFile = codecs.open(dataFilePathName, 'r', self.charset)
            ctrlFile = codecs.open(ctrlFilePathName, 'r', self.charset)
        except Exception as e:
            logger.exception(e.message)
            logger.error('failed to open data file(%s) or control file(%s)', dataFilePathName, ctrlFilePathName)
            raise

        logger.info('reading control file->%s', ctrlFilePathName)
        ctrlInfo = ctrlFile.readlines()
        ctrlCnt = ctrlInfo[0]
        ctrlTable = ctrlInfo[1]
        ctrlDate = ctrlInfo[2]
        ctrlSeparator = ctrlInfo[3]
        logger.info('Information from control file\ncount:%s\tsource table:%s\tdate:%s\tdelimiter:%s',ctrlCnt, ctrlTable, ctrlDate,ctrlSeparator)

        recordList = []

        colStr = ''
        colPos = ''

        logger.info('reading data file->%s', dataFilePathName)
        header = dataFile.readline()
        colList = header.split(ctrlSeparator)#self.getColumnList(table)
        idList = []

        for row in dataFile.readlines():
            recordList.append(row.split(ctrlSeparator))

        for row in recordList:
            idList.append(row[0])

        logger.debug('id list->%s', idList)

        logger.info('control file validation')
        rowCnt = len(recordList)
        if int(rowCnt) == int(ctrlCnt):
            # print "-INFO: row count  match control count [%s]"% rowCnt
            logger.info('row count  match control count [%s]', rowCnt)
        else:
            # print "-INFO: row count [%s] mismatch control count [%s]"% rowCnt, ctrlCnt
            logger.info('row count [%s] mismatch control count [%s]', rowCnt, ctrlCnt)
            self.setStatus('F')


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
            logger.info('cleaning table %s with duplicate id, sql->%s',table, tgt_del_sql)
            cursor.execute(tgt_del_sql)
            logger.debug("delete %s history rows",cursor.rowcount)

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

            self.conn.commit()

        except Exception as e:
            logger.exception(e.message)
            self.conn.rollback()
            self.setStatus('F')
            # print "-ERROR: errors occurs while inserting records, the transaction roll back, database would not be effected"
            logger.error('errors occurs while inserting records, the transaction roll back, database would not be effected')
            raise
        else:
            self.setStatus('S')
            # print "-INFO: Congrats! File loading success"
            logger.info('Congrats! File loading successfully')
        finally:
            ctrlFile.close()
            dataFile.close()
            logger.info('close database connection')
            self.conn.close()
            return self.etlStat

    #MySQLdb
    def mysqlbulkloader(name, attributes, fieldsep, rowsep, nullval, filehandle):
        global connection
        cursor = connection.cursor()
        sql = "LOAD DATA LOCAL INFILE '%s' INTO TABLE %s FIELDS TERMINATED BY '%s' LINES TERMINATED BY '%s' (%s);" % \
                (filehandle, name, fieldsep, rowsep, ', '.join(attributes))
        cursor.execute(sql)
