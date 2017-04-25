# -*- coding: UTF-8 -*-
#!/usr/bin/python
import MySQLdb # this is a third party module, you can find from internet and install it
import time
import datetime
import os
from os import path, access, R_OK, W_OK
import ConfigParser
import pygrametl
from pygrametl.datasources import SQLSource

# conn = MySQLdb.connect(host='localhost',db="bi_dxd", user="robin", passwd="robin123")
#
# sql = "select * FROM t_user_zhima;"
# resultsSource = SQLSource(connection=conn, query=sql)
#
# for row in resultsSource:
#     print row

config = ConfigParser.ConfigParser()
config.read('init_test.cfg')

print config.get('global', 'hello')

