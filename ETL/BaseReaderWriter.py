#!/usr/bin/env python
# -*- coding: UTF-8 -*-
# Date:2017-05-16

__author__ = 'roubingxiong'

import sys
import logging

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




