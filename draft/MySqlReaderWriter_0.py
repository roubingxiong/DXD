# -*- coding: UTF-8 -*-
#!/usr/bin/python

__author__ = 'zhxiong'
import MySQLdb # this is a third party module, you can find from internet and install it
import time
import datetime
import os
from os import path, access, R_OK, W_OK
import ConfigParser

today = datetime.date.today()
yestoday = today - datetime.timedelta(days=1)
checkAcc_date = yestoday.strftime('%Y-%m-%d')

todayYYYYMMDD = today.strftime('%Y%m%d')

class BaseReaderWriter():
    # conn, config = ''


    def __init__(self, mysqlConn): # the config file put in the same location with this script
        self.conn = mysqlConn
        self.today = datetime.date.today().strftime('%Y-%m-%d')
        # yestoday.strftime('%Y-%m-%d')
        # yeterday = today - datetime.timedelta(days=1)

    def getCursor(self):
        self.cursor = self.conn.cursor()

        return self.cursor

    def getColumnList(self, table):
        colList = []
        sql = "desc %s"% table
        try:
            cursor = self.getCursor()
            cursor.execute(sql)
            for row in cursor.fetchall():
                colList.append(row[0])
        except Exception:
            raise
        else:
            return colList
    def getToday(self):
        return self.today

    def getRecCnt(self, table, type, indField='update_time', value='2017-02-02'):
        currDay=value
        nextDay=(datetime.datetime.strptime(currDay,'%Y-%m-%d') + datetime.timedelta(days=1)).strftime('%Y-%m-%d')

        if type == 'F':
            sql = "select count(1) FROM %s"% table
        elif type == 'I':
            sql = "select COUNT(1) FROM %s WHERE %s >= '%s' AND %s < '%s'"% (table, indField, currDay, indField, nextDay)
        else:
            print "--ERROR：wrong argument, only F(full) and I(incremental) accepted"

        print 'sql for run: ' + sql
        try:
            cursor = self.getCursor()
            cursor.execute(sql)
            for row in cursor.fetchall():
                rowCnt = row[0]
                break
            print rowCnt
            return rowCnt
        except Exception:
            raise

    def done(self):
        self.getDbConnection()

    def getDbClosed(self):
        self.conn.close()

class MySQLTableExtractor(BaseReaderWriter):

    def __init__(self, mysqlConn):
        print "-INFO: initial MySQLTableReader"
        BaseReaderWriter.__init__(self, mysqlConn) # initial parent class construct function

    def extractTableToFile(self, table, file, delimiter='|', type='I', indField='update_time', value='2099-12-30'):
        colList = self.getColumnList(table)
        colStr = str(colList).replace('[','').replace(']','')
        recCnt = self.getRecCnt(table, type, indField, value)

        currDay=value
        nextDay=(datetime.datetime.strptime(currDay,'%Y-%m-%d') - datetime.timedelta(days=1)).strftime('%Y-%m-%d')

        if type == 'F':
            sql = "select %s FROM %s"% colStr,table
        elif type == 'I':
            sql = "select %s FROM %s WHERE %s >= '%s' AND %s < %s"% (colStr,table, indField, currDay, indField, nextDay)
        else:
            print "--ERROR：wrong argument, only F(full) and I(incremental) accepted"
            # raise "--ERROR：wrong argument, only F(full) and I(incremental) accepted"

        try:
            result = self.cursor.execute(sql)

            for row in result.fetchall():
                print row
        except Exception:
            raise

    def getFileRowCnt(self, file):
        pass

    #MySQLdb
    def mysqlbulkloader(name, attributes, fieldsep, rowsep, nullval, filehandle):
        global connection
        cursor = connection.cursor()
        sql = "LOAD DATA LOCAL INFILE '%s' INTO TABLE %s FIELDS TERMINATED BY '%s' LINES TERMINATED BY '%s' (%s);" % \
                (filehandle, name, fieldsep, rowsep, ', '.join(attributes))
        cursor.execute(sql)



    def exportDateTableToFile(self, table, filename, date=checkAcc_date):
        pass
    def exportTablesToFiles(self, tables=[], location = ''):
        pass

    def exportFromQueryToFile(self, query, filename = ''):
        pass

    def exportFromQueriesToFile(self, query = [], location = ''):
        pass

    def readTable(self):
        pass






















class MySQLTableWriter(BaseReaderWriter):

    def __init__(self,cfgFile = 'init.cfg', type='target'):
        print "-INFO: initial MySQLTableWriter"
        BaseReaderWriter.__init__(self, cfgFile, type) # initial parent class construct function

        self.srcDir = self.config.get('global', 'dataHub')

    def import_mysql_table_stmt(self, table, filename, delimiter =',', encloased_by ='"', line_term_by ='\\r\\n', ignore_line_num = 0):
        delimiter = ','
        encloased_by = '"'
        line_term_by = '\\r\\n'
        import_stmt = " LOAD DATA LOCAL INFILE " + "'" + filename.replace('\\', '/') + "'" + \
                        " INTO TABLE " + table + \
                        " FIELDS TERMINATED BY " + "'" + delimiter + "'" + \
                        " OPTIONALLY ENCLOSED BY " + "'" + encloased_by + "'" + \
                        " LINES TERMINATED BY " + "'" + line_term_by + "'" + \
                        " IGNORE " + str(ignore_line_num) + " LINES " + \
                        " ; "
        print import_stmt
        return import_stmt

    def importFullFileToTable(self, table, filename='', date=checkAcc_date, delimiter = ',', encloased_by = '"', line_term_by = '\\r\\n', ignore_line_num = 0):
        print "-INFO: running importFullFileToTable"
        query = 'select * from ' + table
        count = 'select COUNT(1) from ' + table
        if len(filename) == 0:
            filename = table + '.dat_full_' + date
        else:
            filename = filename + '.dat_full_' + date

        srcFile = os.path.join(self.srcDir, filename)

        if not os.access(srcFile, R_OK):
            print "-ERROR: " + srcFile + " not exists or current user do not have read access, please make sure your upstream process completed"
            print "-INFO：Processing exit"
            exit()

        print "-INFO: source file would be " + srcFile
        stmt = self.import_mysql_table_stmt(table, srcFile, delimiter, encloased_by, line_term_by, ignore_line_num)

        try:
            print "-INFO: executing query"
            self.cursor.execute(stmt)
            self.conn.commit()
        except Exception:
            self.conn.rollback()
            raise
        else:
            print "-INFO: Congratulations! Processing complete successfully"

    def importFilesToTables(self, tables=[], location = ''):
        pass

    def exportFromQueryToFile(self, query, filename = ''):
        pass
