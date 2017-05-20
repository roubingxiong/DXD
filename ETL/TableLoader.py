#!/usr/bin/env python
# -*- coding: UTF-8 -*-
# Date:2017-05-16

__author__ = 'roubingxiong'

import os
import logging
import codecs
import sys


from BaseReaderWriter import BaseReaderWriter

logger = logging.getLogger('DXD_ETL')

class TableLoader(BaseReaderWriter):

    def __init__(self, conn):
        BaseReaderWriter.__init__(self, conn)

    def loadFileToTable(self, messager, checkCtrlFile='Y'):

        tableName = messager['table_name']
        ctrlFilePathName = messager['ctrl_path_file']
        dataFilePathName = messager['data_path_file']

        try:
            dataFile = open(dataFilePathName, 'r')
            # dataFile = codecs.open(dataFilePathName, 'r', self.charset)
            logger.info('opening data file->%s', dataFilePathName)
            ctrlFile = open(ctrlFilePathName, 'r')
            # ctrlFile = codecs.open(ctrlFilePathName, 'r', self.charset)
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
        logger.info('control information:\ncount:%s\ntable:%s\ndate:%s\ndelimiter:%s\nmode:%s',ctrlCnt, ctrlTable, ctrlDate,ctrlSeparator,mode)

        logger.info('reading header of data file')
        header = dataFile.readline().strip()    # C1|C2|C3
        # header = 'C1|C2|C3'
        logger.debug('header->%s', header)

        file_col_list = header.split(ctrlSeparator)  # ['C1', 'C2', 'C3']
        logger.info('columns list in header->%s', file_col_list)

        tbl_col_list = self.getColumnList(tableName)    # ['C1', 'C3', 'C4']
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

        logger.info('reading record of data file')
        try:
            cursor = self.conn.cursor()

            MB = 0.5
            block_size = int(MB*1024*1024) # 0.5 MB

            logger.info('data file block size for reading each time is [%s] MB', MB)
            total_count = 0
            while True:
                lines = dataFile.readlines(block_size)

                if not lines:
                    logger.info('complete to read and update records, total [%s] rows', total_count)
                    break
                else:
                    record_list = []
                    block_line_num = 0
                    for line in lines:
                        block_line_num += 1
                        record = []
                        line = str(line).strip().split(ctrlSeparator)
                        for index in comm_col_index_list:
                            record.append(line[index])  # pick up common fields

                        record_list.append(record)  # collecting records list for inserting

                    total_count = total_count + block_line_num

                    tgt_ins_sql = "REPLACE INTO %s (%s) values (%s) " % (tableName, comm_col_str, col_position)

                    logger.info('replacing into [%s] rows to target table', block_line_num)
                    cursor.executemany(tgt_ins_sql, record_list)

            # verify the ctrl count is equal to row count
            if int(ctrlCnt) == int(total_count):
                logger.info('control count [%s] match data row count [%s]', ctrlCnt, total_count)
            else:
                logger.error('control count [%s] mismatch data row count [%s]', ctrlCnt, total_count)
                raise Exception('control count [%s] mismatch data row count [%s]' % (ctrlCnt, total_count))

            messager['ctrl_count'] = total_count
            self.conn.commit()
            logger.info('changes committed')
        except:
            self.conn.rollback()
            logger.exception('Exception occured while loading records, the transaction roll back, database would not be effected')
            raise
        else:
            logger.info('Congrats! Table [%s] loading successfully', tableName)
        finally:
            ctrlFile.close()
            dataFile.close()
