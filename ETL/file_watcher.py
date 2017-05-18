#!/usr/bin/env python2
# -*- coding: utf-8 -*-

import os, sys, time
from subprocess import call
import logging
import re

logger = logging.getLogger('DXD_ETL')

# cntl = "table + '.ctrl.' + runDateStr"

def watch_file(dir, filename, expireTime=1800):
    absFile = os.path.join(dir, filename)
    logger.info('start watching file %s', absFile)
    # print "-INFO: start watching file %s"%(absFile)

    this = last = size = 0

    freq = 5 #10s each check

    check_times_limitation = expireTime/freq

    for i in range(check_times_limitation):
        if os.path.isfile(absFile) and os.access(absFile, os.R_OK):
            prop = os.stat(absFile)
            size = prop.st_size
            this = prop.st_mtime
            # print this, last

        if this == last and this <> 0:
            # print "cool! the file is coming and it is stable"
            logger.info('cool! %s is coming and it is stable',absFile)
            return filename
        elif size == 0 and this <>0:
            logger.error('%s is coming, but is an empty file', absFile)
            # print "the file is coming, but is an empty file"
            return False
        else:
            last = this

        time.sleep(freq)

    # print 'file watching run out of time'
    logger.error('%s is not exist, file watching run out of %s seconds', absFile, expireTime)
    raise Exception(absFile + ' is not exist, file watching run out of ' + str(expireTime) + ' seconds' )


def watch_files(dir, pattern='t_decision_rule_log_[0-9]{8}.ctrl.[0-9]{8}.[0-9]{8}'):
    files = os.listdir(dir)
    stab_file_list =[]
    if len(files) == 0:
        logger.warn("no files match %s", pattern)
    else:
        for file in files:
            if re.match(pattern, file):
                watch_file(dir, file)
                stab_file_list.append(file)

    logger.info("find data file with pattern %s", pattern)

    return stab_file_list

def clean_file(dir=os.getcwd(), days=7):
    logger.info('clean files older than %s days under directory %s', days, dir)
    now = time.time()
    cutoff = now - (days * 86400)
    count = 0
    files = os.listdir(dir)
    for xfile in files:
        file_path_name = os.path.join(dir, xfile)
        if os.path.isfile(file_path_name):
            t = os.stat(file_path_name)
            c = t.st_ctime

            # delete file if older than 10 days
            if c < cutoff:
                os.remove(file_path_name)
                count += 1
                logger.info('remove file %s', xfile)
    logger.info('%i file remove', count)
    return count

# clean_file(dir=os.path.join(os.getcwd(), 'log'), days=7)

# watch_file(dir=os.path.join(os.getcwd(), 'log'), filename='proj.log', expireTime=10)
