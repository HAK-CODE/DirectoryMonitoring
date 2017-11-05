'''
author: HAK
time  : 11:00 PM, 05/11/2017
'''

import os, time
import sys

#Get process id
print('Process id is',str(os.getpid()))
filepath = sys.argv[1]
fileObj = None

#keep running untill a process releases file
while True:
    time.sleep(3)
    if os.path.exists(filepath):
        try:
            fileObj = open(filepath, 'a')
            print('trying to open file ', filepath)
            if fileObj:
                print('file not locked',filepath)
        except OSError:
            print('file is locked',filepath)
        finally:
            if fileObj:
                fileObj.close()
                break
    else:
        print('file path not exist')
        sys.exit()
sys.exit()
