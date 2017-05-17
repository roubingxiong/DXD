# -*- coding: UTF-8 -*-
#!/usr/bin/env python2

__author__ = 'zhxiong'

import os
import logging
import codecs
import sys
import BaseReaderWriter
import split_time

logger = logging.getLogger('DXD_ETL')

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

        col_position = []
        for n in range(len(comm_col_list)):
            col_position.append('%s')
        col_position = ','.join(col_position)   # %s,%s

        try:
            cursor = self.conn.cursor()

            logger.info('reading record of data file')
            record_list = []
            id_list = []

            block_size = 1000 # 1000 rows
            line_num = 0
            total_num = 0
            rowCnt = 0
            while 1:
                line = dataFile.readline()
                if not line.strip():
                    logger.info('read data file completed, total [%s] rows', total_num)
                    break
                else:
                    line_num += 1

                record = []
                line = str(line).strip().split(ctrlSeparator)
                for index in comm_col_index_list:
                    try:
                        record.append(line[index])
                    except:
                        print line
                        print index
                        exit()

                record_list.append(record)  # collecting records list for inserting

                if line_num == block_size:
                    logger.info('replacing into new record to target table %s', table)
                    tgt_ins_sql = "REPLACE INTO %s (%s) values (%s) " % (table, comm_col_str, col_position)

                    cursor.executemany(tgt_ins_sql, record_list)

                    print(cursor.rowcount)


                    record_list = []
                    id_list = []
                    line_num = 1
                    continue

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
