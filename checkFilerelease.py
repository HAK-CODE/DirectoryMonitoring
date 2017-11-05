import os, time
import sys

print('Process id',str(os.getpid()))
filepath = sys.argv[1]
fileObj = None

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
