# -*- coding: UTF-8 -*-
#!/usr/bin/python

import datetime,time

def split_time(from_date='2017-05-01', to_date='2017-05-10', hours=1):

    datetime_snipet_list = []

    from_datetime = datetime.datetime.strptime(from_date, '%Y-%m-%d')
    to_datetime = datetime.datetime.strptime(to_date, '%Y-%m-%d') + datetime.timedelta(seconds=-1)

    block_from_datetime = from_datetime
    block_to_datetime = from_datetime

    while block_to_datetime < to_datetime:
        one_block = []
        block_to_datetime = block_from_datetime + datetime.timedelta(hours=hours)
        if block_to_datetime >= to_datetime:
            block_to_datetime = to_datetime

        one_block.append(block_from_datetime.strftime('%Y-%m-%d %H:%M:%S'))
        one_block.append( block_to_datetime.strftime('%Y-%m-%d %H:%M:%S'))

        block_from_datetime = block_to_datetime + datetime.timedelta(seconds=1)

        # print(one_block)
        datetime_snipet_list.append(one_block)

    return datetime_snipet_list

# datetime_snipet_list = split_time(hours=1)
#
# for from_time, toDateStr in datetime_snipet_list:
#     print from_time
#     print toDateStr
