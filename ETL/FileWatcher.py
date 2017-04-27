# -*- coding: UTF-8 -*-
#!/usr/bin/python

import os,time

class FileWatcher():

    def __init__(self, location):
        self.watch_dir = os.path.abspath(location)

        if  not os.path.isdir(self.watch_dir) or not os.access(self.watch_dir, os.R_OK):
            print "-ERROR: %s is not a directory or not readable"% (self.watch_dir)

    def watch_file(self, filename, expireTime=10, ):
        absFile = os.path.join(self.watch_dir, filename)

        print "-INFO: the file watcher is watching file %s"%(absFile)
        this = last = size = 0
        freq = 5 #10s each check
        for i in range(expireTime/freq):
            if os.path.isfile(absFile) and os.access(absFile, os.R_OK):
                prop = os.stat(absFile)
                size = prop.st_size
                this = prop.st_mtime
                print this, last

            if this == last and this <> 0:
                print "-INFO: %s is stable"% (absFile)
                return filename
            elif size == 0 and this <>0:
                print "-INFO: %s is empty file"% (absFile)
                return False
            else:
                last = this

            time.sleep(freq)


    def watch_dir(self):
        pass

