# -*- coding: UTF-8 -*-
#!/usr/bin/python


from faker import Factory, Faker

fake = Factory.create('zh_CN')

# p = fake.provider()

print fake.name(), fake.city_name()
# 'Lucy Cechtelar'

print fake.address()
# "426 Jordy Lodge
#  Cartwrightshire, SC 88120-6700"

print fake.text()

import job_handler
import ConfigParser
config = ConfigParser.ConfigParser()
config.read('init.cfg')

conn = job_handler.getConnByType(config, 'extract')

table = 't_user_zhima'

sql = "desc " + table



cursor = conn.cursor()
cursor.execute(sql)

for row in cursor.fetchall():
    field = row[0]


    tp = str(row[1])
    typeInfo = []
    dataType = ''
    dataLen=''
    if tp.count('(') >0 and tp.count('char') > 0:
        typeInfo = tp.replace(')', '').split('(')
        dataType = typeInfo[0]
        dateLen = typeInfo[1]
    else:
        typeInfo = typeInfo.append(tp)
        dataType = typeInfo[0]
        dataLen = 0

    null = row[2]
    key = row[3]
    default = row[4]
    extra = row[5]
    if default <> "":
        print 'use default for field->' + field
    elif null == 'NO' and (key == 'PRI' or key == 'UNI'):
        col = " max(" + field + ") + FLOOR(0 + RAND() *(10)) "

    elif null == 'NO' and dataLen > 0:
        typeInfo = tp.replace(')', '').split('(')
        dataType = typeInfo[0]
        dateLen = typeInfo[1]

#
# sql = "INSERT INTO " + table + " (" + colStr + ") values (" + colPos + ") "
# cursor.executemany(sql, recordList)

# Sint velit eveniet. Rerum atque repellat voluptatem quia rerum. Numquam excepturi
# beatae sint laudantium consequatur. Magni occaecati itaque sint et sit tempore. Nesciunt
# amet quidem. Iusto deleniti cum autem ad quia aperiam.
# A consectetur quos aliquam. In iste aliquid et aut similique suscipit. Consequatur qui
# quaerat iste minus hic expedita. Consequuntur error magni et laboriosam. Aut aspernatur
# voluptatem sit aliquam. Dolores voluptatum est.
# Aut molestias et maxime. Fugit autem facilis quos vero. Eius quibusdam possimus est.
# Ea quaerat et quisquam. Deleniti sunt quam. Adipisci consequatur id in occaecati.
# Et sint et. Ut ducimus quod nemo ab voluptatum.
