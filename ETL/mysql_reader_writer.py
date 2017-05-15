# -*- coding: UTF-8 -*-
#!/usr/bin/env python2

__author__ = 'zhxiong'

import os
import logging
import codecs
import sys

import split_time

logger = logging.getLogger('DXD_ETL')


class BaseReaderWriter():

    def __init__(self, conn): # the config file put in the same location with this script

        self.conn = conn
        self.charset = conn.get_character_set_info()['name']

        logger.info('resetting default system charset with [%s] same as the database', self.charset)
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
                col_list.append(str(row[0]).lower())
        except:
            raise
        else:
            logger.info('column list in table %s->%s', table, col_list)
            return col_list

    def is_table_exist(self, table):
        pass

    def create_table_if_not_exist(self, table, ddl):
        pass

    def setCharset(self, charset='utf8'):
        self.charset = charset
        return self.charset


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


class TableLoader(BaseReaderWriter):

    def __init__(self, conn):
        BaseReaderWriter.__init__(self, conn)

    def loadFileToTable(self, messager, checkCtrlFile='Y'):

        table = messager['table_name']
        ctrlFilePathName = messager['ctrl_path_file']
        dataFilePathName = messager['data_path_file']

        try:
            dataFile = open(dataFilePathName, 'r')
            # dataFile = codecs.open(dataFilePathName, 'r', self.charset)
            logger.info('opening data file->%s', dataFilePathName)
            ctrlFile = open(ctrlFilePathName, 'r')
            # ctrlFile = codecs.open(ctrlFilePathName, 'r', self.charset)
            logger.info('openings data file->%s', ctrlFilePathName)
        except Exception as e:
            logger.error('failed to open data file(%s) or control file(%s)', dataFilePathName, ctrlFilePathName)
            raise e

        logger.info('reading control file')
        ctrlInfo = ctrlFile.readlines()
        ctrlCnt = str(ctrlInfo[0]).strip()
        ctrlTable = str(ctrlInfo[1]).strip()
        ctrlDate = str(ctrlInfo[2]).strip()
        ctrlSeparator = str(ctrlInfo[3]).strip()
        mode = str(ctrlInfo[4]).strip()
        logger.info('control information:\ncount:%s\ntable:%s\ndate:%s\ndelimiter:%s\nmode:%s',ctrlCnt, ctrlTable, ctrlDate,ctrlSeparator,mode)

        logger.info('reading header of data file')
        header = dataFile.readline().strip()    # C1|C2|C3
        # header = 'C1|C2|C3'
        logger.debug('header->%s', header)

        file_col_list = header.split(ctrlSeparator)  # ['C1', 'C2', 'C3']
        logger.info('columns list in header->%s', file_col_list)

        tbl_col_list = self.getColumnList(table)    # ['C1', 'C3', 'C4']
        # logger.info('columns in target table %s->%s', table, tbl_col_list)

        comm_col_list = list(set(file_col_list).intersection(tbl_col_list))  # ['C1', 'C3']
        logger.info('common columns list->%s', comm_col_list)

        if len(comm_col_list) <> len(file_col_list) or len(comm_col_list) <> len(tbl_col_list):
            logger.warn('columns are not different between source table and target table')

        comm_col_str = ','.join(comm_col_list)  # C1,C3
        logger.info('common columns with target table->%s', comm_col_str)

        comm_col_index_list = []
        for col in comm_col_list:
            index = file_col_list.index(col)
            comm_col_index_list.append(index)
            # logger.debug('finding common column->[%s] and index->[%s] in data file', col, index)

        logger.info('get common column index in data file->%s', comm_col_index_list)

        if 'id' in comm_col_list:
            id_index = file_col_list.index('id')
            logger.debug('column "id" index->%s', id_index)
        else:
            logger.error('it is abnormal without primary key "id" in common column list')
            raise Exception('it is abnormal without primary key "id" in common column list')

        col_position = []
        for n in range(len(comm_col_list)):
            col_position.append('%s')
        col_position = ','.join(col_position)   # %s,%s

        logger.info('reading record of data file')
        record_list = []
        id_list = []

        for line in dataFile.readlines():
            # logger.debug('line 2->%s', line)
            record = []
            line = str(line).strip().split(ctrlSeparator)
            # logger.debug('line 3->%s', line)
            for index in comm_col_index_list:
                # logger.debug('index->%s', index)
                try:
                    record.append(line[index])
                except:
                    print line
                    print index
                    exit()
            id_list.append(line[id_index])  # collecting id list for deletion
            record_list.append(record)  # collecting records list for inserting

        # print id_list

        logger.info('control file validation')
        rowCnt = len(record_list)

        if int(rowCnt) == int(ctrlCnt):
            # print "-INFO: row count  match control count [%s]"% rowCnt
            logger.info('row count[%s] match control count [%s]', rowCnt, ctrlCnt)
        else:
            # print "-INFO: row count [%s] mismatch control count [%s]"% rowCnt, ctrlCnt
            logger.error('row count [%s] mismatch control count [%s]', rowCnt, ctrlCnt)
            raise Exception('row count [%s] mismatch control count [%s]', rowCnt, ctrlCnt)

        try:
            cursor = self.conn.cursor()
            step = 1000

            logger.info('deleting duplicate from target table %s', table)
            del_id_total = 0
            for i in range(0, len(id_list)-1, step):
                bulk_id = str(tuple(id_list[i:i+step]))
                tgt_del_sql = "delete from %s WHERE id in %s" % (table, bulk_id)

                cursor.execute(tgt_del_sql)

                del_row_count = cursor.rowcount
                logger.debug('deleting %s rows', del_row_count)
                del_id_total = del_id_total + cursor.rowcount

            logger.debug("total [%s] duplicate rows deleted", del_id_total)

            logger.info('inserting new record into target table %s', table)
            tgt_ins_sql = "INSERT INTO %s (%s) values (%s) " % (table, comm_col_str, col_position)
            logger.debug('pre sql->%s', tgt_ins_sql)
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
        # finally:
        #     ctrlFile.close()
        #     dataFile.close()
        #     # logger.info('close database connection')
        #     # self.conn.close()
        #     return messager
