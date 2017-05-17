# -*- coding: UTF-8 -*-
#!/usr/bin/python

import linecache
import logging

logger = logging.getLogger('DXD_ETL')
def split_file(file='', block_size=1):
    with open('C:\\Users\\Robin Xiong\\PycharmProjects\\DXD\\ETL\\HUB\\test.txt') as f:
        # header = f.readline()
        line_num = 1
        block_size = int(block_size)

        while True:

            data = f.readlines(block_size)

            if not data:
                logger.info('complete to read file->')
                break
            else:
                for line in data:
                    print(line.strip().split('|~|'))




        print("total: ", line_num)


split_file()