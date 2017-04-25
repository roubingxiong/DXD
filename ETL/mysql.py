#!/usr/bin/python
# -*- coding: UTF-8 -*-
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

    def __init__(self, cfgFile = 'init.cfg', type='source'): # the config file put in the same location with this script
        print "-INFO: initial BaseReaderWriter"
        self.config = ConfigParser.ConfigParser()
        print "-INFO: reading config file"
        self.config.read(cfgFile)
        self.type = type
        self.conn = self.getDbConnection()

        print "-INFO: get connection cursor"
        self.cursor = self.conn.cursor()

    def getDbConnection(self,db='information_schema'):
        print "-INFO: connecting " + self.type + " database"
        host = self.config.get(self.type, 'host')
        user = self.config.get(self.type, 'user')
        passwd = self.config.get(self.type, 'passwd')
        db = self.config.get(self.type, 'db')
        charset = self.config.get(self.type, 'charset')
        try:
            self.conn = MySQLdb.connect(host=host,user=user,passwd=passwd,db=db,charset=charset)
        except Exception:
            raise
        return self.conn

    def done(self):
        self.getDbConnection()

    def getDbClosed(self):
        self.conn.close()

class MySQLTableReader(BaseReaderWriter):

    def __init__(self, cfgFile = 'init.cfg', type='source'):
        print "-INFO: initial MySQLTableReader"
        BaseReaderWriter.__init__(self, cfgFile, type) # initial parent class construct function

        self.tgtDir = self.config.get('global', 'dataHub')

    def export_mysql_table_stmt(self, query, tgtFile, delimiter =',', encloased_by ='"', line_term_by ='\\r\\n', header = False):
        print "-INFO: preparing sql statement"
        delimiter = ','
        encloased_by = '"'
        line_term_by = '\\r\\n'
        export_stmt = query + \
                     " INTO OUTFILE " + "'" + tgtFile.replace('\\', '/') + "'" + \
                     " FIELDS TERMINATED BY " + "'" + delimiter + "'" + \
                     " OPTIONALLY ENCLOSED BY " + "'" + encloased_by + "'" + \
                     " LINES TERMINATED BY " + "'" + line_term_by + "'" + \
                     " ; "
        print export_stmt
        return export_stmt

    def exportFullTableToFile(self, table, filename='', date=checkAcc_date, delimiter = ',', encloased_by = '"', line_term_by = '\\r\\n', header = False):
        print "-INFO: running exportFullTableToFile"
        query = 'select * from ' + table
        count = 'select COUNT(1) from ' + table

        if len(filename) == 0:
            filename = table + '.dat_full_' + date
        else:
            filename = filename + '.dat_full_' + date

        tgtFile = os.path.join(self.tgtDir, filename)

        if os.path.isfile(tgtFile) and os.access(tgtFile, W_OK):
            print "-WARR: " + tgtFile + " already exists, removing it now"
            os.remove(tgtFile)
        elif not os.path.isfile(tgtFile):
            print "-INFO: " + tgtFile + " does not exist, it's good"
        else:
            print "-ERROR: " + tgtFile + " already exists and current user do not have write permission, please manually remove it"
            print "-INFO：Processing exit"
            exit()

        print "-INFO: target file would be " + tgtFile
        stmt = self.export_mysql_table_stmt(query, tgtFile, delimiter, encloased_by, line_term_by, header)

        try:
            print "--INFO: executing query"
            self.cursor.execute(stmt)
            self.conn.commit()
        except Exception:
            self.conn.rollback()
            raise
        else:
            print "-INFO: Congratulations! Processing complete successfully"

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

from timeit import Timer

extractor = MySQLTableReader(cfgFile = 'init.cfg', type='source')

extractor.exportFullTableToFile(table='src_order', date=checkAcc_date)

extractor.done()

# loader = MySQLTableWriter(cfgFile = 'init.cfg', type='target')
#
# loader.importFullFileToTable(table='tgt_order', filename='src_order', date=checkAcc_date)
#
# loader.done()
#
# import cProfile
#
# cProfile.run("loader.importFullFileToTable(table='tgt_order', filename='src_order', date=checkAcc_date)")




# connectionPool = [1]*10000
# for i in range(10000):
#     time.sleep(2)
#     print i
#     # a =  MySQLdb.connect("127.0.0.1", "robin", "robin123", "robin")
#     reader = MySQLTableReader()
#     connectionPool.append(reader)
#     del reader
#     # connectionPool.append(MySQLTableReader())





