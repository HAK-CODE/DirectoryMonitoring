'''
author: HAK
time  : 10:00 PM, 28/10/2017
'''

from   watchdog.events import FileSystemEventHandler
from   watchdog.observers import Observer
import time
from   fileInfo import FILE_INFO
from   directoryInfo import PATH_INFO_PROVIDER

'''
An event handler that use to listen triggered events from FileSystemEvent
This class onsist of one function on_created which provide two basic event logging generators
1. When a file is created
2. When a directory is created

Returns:
-------FILE--------
1. Path
2. Name
3. Extension
4. Time of creation
5. Size (BYTES)
------------------- 

-----DIRECTORY-----
1. Path
2. Name
3. Time of creation
4. Size (BYTES)
-------------------
'''

class Handler(FileSystemEventHandler):
    content = []
    with open('ignore.txt') as f:
        content = f.readlines()

    @staticmethod
    def on_created(event):
        PATH = event.src_path
        if PATH not in Handler.content:
            #Check if a event is generated for directory or not
            if event.is_directory == True:
                DIR = PATH_INFO_PROVIDER(PATH)
                print('----------------------------- NEW DIRECTORY CREATED -----------------------------')
                print("PATH  : ", DIR.DIRBASIC()[0])
                print("NAME  : ", DIR.DIRBASIC()[1])
                print("CTIME : ", DIR.DIRBASIC()[2])
                print("ATIME : ", DIR.DIRATIME())
                print("SIZE  : ", DIR.DIRBASIC()[3], "BYTES")
                print('---------------------------------------------------------------------------------')
            else:
                FILE = FILE_INFO(event.src_path)
                print('-------------------------------- NEW FILE CREATED -------------------------------')
                print("PATH  : ", FILE.FILEBASIC()[0])
                print("NAME  : ", FILE.FILEBASIC()[1])
                print("EXT   : ", FILE.FILEBASIC()[2])
                print("CTIME : ", FILE.FILEBASIC()[3])
                print("SIZE  : ", FILE.FILEBASIC()[4], "BYTES")
                print('---------------------------------------------------------------------------------')


class Watcher:
    DIRECTORY = ""
    def __init__(self, DIRECTORY):
        self.DIRECTORY = DIRECTORY

    def run(self):
        event_handler = Handler()
        observer = Observer()
        observer.schedule(event_handler, self.DIRECTORY, recursive=True)
        observer.start()
        try:
            while True:
                time.sleep(1)
        except:
            observer.stop()
            print("Observer Stopped.")
        observer.join()