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

    def __init__(self, mysqlConn, coding='utf-8'): # the config file put in the same location with this script

        self.conn = mysqlConn
        self.status = 'I'   # I->In Progress S->Success  F->Fail
        self.coding = coding
        self.etlStat = {'table': '', 'data_file': '', 'ctrl_file': '', 'ctrlCnt': '', 'data_path_file': '', 'ctrl_path_file': '', 'status': self.status, 'from_date': '9999-12-31', 'to_date': '9999-12-31'}

    def refreshETLStat(self, location, runDate, table):

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
        colList = []
        sql = "desc %s"% table
        try:
            cursor = self.conn.cursor()
            cursor.execute(sql)
            for row in cursor.fetchall():
                colList.append(row[0])
        except Exception:
            raise
        else:
            return colList

    def setCoding(self, coding='utf-8'):
        self.coding = coding
        return self.coding

    def setStatus(self, status='I'):
        self.status = status
        self.etlStat['status'] = self.status
        return self.status

    def getToday(self):
        return self.today


class TableExtractor(BaseReaderWriter):

    def __init__(self, mysqlConn):
        BaseReaderWriter.__init__(self,mysqlConn)

    def extractTableToFile(self, table, location, runDate='9999-12-31', loadType='INC', delimiter='|'):

        self.refreshETLStat(location, runDate, table)

        ctrlFilePathName = self.etlStat['ctrl_path_file']
        dataFilePathName = self.etlStat['data_path_file']
        fromDate = self.etlStat['from_date']
        toDate = self.etlStat['to_date']

        try:
            dataFile = codecs.open(dataFilePathName, 'w', self.coding)
            ctrlFile = codecs.open(ctrlFilePathName, 'w', self.coding)
        except Exception:
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

        print "-INFO: column-> " + colStr
        print "-INFO: header-> " + header

        startRow = 0
        toRow = 1000
        if loadType == 'INC':
            sql = "select concat_ws('%s',%s) FROM %s WHERE update_time >= '%s' AND update_time < '%s';"% (separator,colStr,table, fromDate, toDate)
        elif loadType == 'FULL':
            # sql = "select SQL_CALC_FOUND_ROWS concat_ws('%s',%s) FROM %s limit %s, %s;"% (separator,colStr,table, startRow, toRow)
            sql = "select SQL_CALC_FOUND_ROWS concat_ws('%s',%s) FROM %s " % (separator, colStr, table)
        else:
            print "-INFO: only 'INC'->incremental extract and 'FULL' full extract are accepted. invalid load Type %s "% loadType

        row_cnt_sql = "select FOUND_ROWS()"

        cursor = self.conn.cursor()



        try:
            print "-INFO: running sql->\n" + sql

            cursor.execute(sql)
            ctrlCnt = cursor.rowcount;

            # cursor.execute(row_cnt_sql)
            # ctrlCnt = long(cursor.fetchone()[0])

            print ctrlCnt
            print "-INFO: writing data file..."
            dataFile.write(header + '\n')
            for row in cursor.fetchall():
                dataFile.write(row[0] + '\n')
                dataFile.flush()

             #write information in ctrl file
            print "-INFO: writing control file..."
            ctrlFile.writelines([str(ctrlCnt), '\n', table, '\n', runDate, '\n', separator])

            self.etlStat['ctrlCnt'] = ctrlCnt
            self.etlStat['status'] = 'S'

        except:
            self.conn.rollback()
            self.etlStat['status'] = 'F'
            print "-ERROR: error happened while writing data and control file"
            # os.remove()
            raise
        finally:
            ctrlFile.close()
            dataFile.close()
            return self.etlStat



class TableLoader(BaseReaderWriter):

    def __init__(self, mysqlConn):
        # self.conn = mysqlConn #MySQLdb.connect()
        BaseReaderWriter.__init__(self, mysqlConn)

    def loadFileToTable(self, table, location, runDate='9999-12-31'):

        self.refreshETLStat(location, runDate, table)

        ctrlFilePathName = self.etlStat['ctrl_path_file']
        dataFilePathName = self.etlStat['data_path_file']

        try:
            dataFile = codecs.open(dataFilePathName, 'r', self.coding)
            ctrlFile = codecs.open(ctrlFilePathName, 'r', self.coding)
        except IOError, Exception:
            raise

        print "-INFO: reading control file->" + ctrlFilePathName
        ctrlInfo = ctrlFile.readlines()
        ctrlCnt = ctrlInfo[0]
        ctrlTable = ctrlInfo[1]
        ctrlDate = ctrlInfo[2]
        ctrlSeparator = ctrlInfo[3]

        print "-INFO: count:%s table:%s date:%s delimiter:%s" % (ctrlCnt, ctrlTable, ctrlDate,ctrlSeparator)

        recordList = []

        colStr = ''
        colPos = ''

        print "-INFO: reading data file->" + dataFilePathName
        header = dataFile.readline()
        colList = header.split(ctrlSeparator)#self.getColumnList(table)


        for row in dataFile.readlines():
                recordList.append(row.split(ctrlSeparator))

        #cntrl file validation
        print "-INFO: control file validation "
        rowCnt = len(recordList)
        if int(rowCnt) == int(ctrlCnt):
            print "-INFO: row count  match control count [%s]"% rowCnt
        else:
            print "-INFO: row count [%s] mismatch control count [%s]"% rowCnt, ctrlCnt
            self.setStatus('F')


        for index in range(len(colList)):
            if index == 0:
                colStr = colList[index]
                colPos = "%s"
            else:
                colStr = colStr + ', ' + colList[index]
                colPos = colPos + ',' + '%s'

        print "-INFO: column-> " + colStr

        sql = "INSERT INTO " + table + " (" + colStr + ") values (" + colPos + ") "

        cursor = self.conn.cursor()

        try:
            print "-INFO: running sql->\n" + sql

            step = 10000
            for i in range(0, rowCnt, step):
                if i + step >= rowCnt:
                    print "-INFO: inserting " + str(rowCnt%step) + " rows"
                else:
                    print "-INFO: inserting " + str(step) + " rows"

                cursor.executemany(sql, recordList[i:i+step])

            self.conn.commit()

        except Exception:
            self.conn.rollback()
            self.setStatus('F')
            print "-ERROR: errors occurs while inserting records, the transaction roll back, database would not be effected"
            raise
        else:
            self.setStatus('S')
            print "-INFO: Congrats! File loading success"
        finally:
            ctrlFile.close()
            dataFile.close()
            return self.etlStat

    #MySQLdb
    def mysqlbulkloader(name, attributes, fieldsep, rowsep, nullval, filehandle):
        global connection
        cursor = connection.cursor()
        sql = "LOAD DATA LOCAL INFILE '%s' INTO TABLE %s FIELDS TERMINATED BY '%s' LINES TERMINATED BY '%s' (%s);" % \
                (filehandle, name, fieldsep, rowsep, ', '.join(attributes))
        cursor.execute(sql)
