'''
author: HAK
time  : 10:00 PM, 28/10/2017
'''

from     watchdog.events    import FileSystemEventHandler
from     watchdog.observers import Observer
import   time
from     fileInfo           import FILE_INFO
from     directoryInfo      import PATH_INFO_PROVIDER
from     server             import TCPSERVER
import   configparser

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
    server = None
    serverConfig = {}
    with open('ignore.txt') as f:
        content = f.readlines()


    def attachServer(self, server_status):
        if(server_status == True):
            config = configparser.ConfigParser()
            config.sections()
            config.read('server.ini')
            topSecret = config['hak.server.com']
            self.server = TCPSERVER(topSecret['IP'], int(topSecret['Port']), int(topSecret['Buffer']))
            self.server.sendData('Connect to server on port '+str(self.server.TCP_PORT))


    def on_created(self, event):
        PATH = event.src_path
        print("Path is : "+PATH)
        if PATH not in Handler.content:
            #Check if a event is generated for directory or not
            if event.is_directory == True:
                DIR = PATH_INFO_PROVIDER(PATH)
                if self.server != None:
                    self.send_info(DIR.DIRBASIC(), True)
                print('----------------------------- NEW DIRECTORY CREATED -----------------------------')
                print("PATH  : ", DIR.DIRBASIC()[0])
                print("NAME  : ", DIR.DIRBASIC()[1])
                print("CTIME : ", DIR.DIRBASIC()[2])
                print("SIZE  : ", DIR.DIRBASIC()[3], "BYTES")
                print('---------------------------------------------------------------------------------')
            else:
                FILE = FILE_INFO(event.src_path)
                if self.server != None:
                    self.send_info(FILE.FILEBASIC(), False)
                print('-------------------------------- NEW FILE CREATED -------------------------------')
                print("PATH  : ", FILE.FILEBASIC()[0])
                print("NAME  : ", FILE.FILEBASIC()[1])
                print("EXT   : ", FILE.FILEBASIC()[2])
                print("CTIME : ", FILE.FILEBASIC()[3])
                print("SIZE  : ", FILE.FILEBASIC()[4], "BYTES")
                print('---------------------------------------------------------------------------------')

    def send_info(self, data, DIR=False):
        if DIR == True:
            dataGram = ("\n----------------------------- NEW DIRECTORY CREATED -----------------------------\n"
                        "PATH  : "  +data[0] +
                        "\nNAME  : "+data[1] +
                        "\nCTIME : "+data[2] +
                        "\nSIZE  : "+str(data[3])+" BYTES\n"+
                        "---------------------------------------------------------------------------------")
            self.server.sendData(dataGram)
        else:
            dataGram = ("\n-------------------------------- NEW FILE CREATED -------------------------------\n"
                        "PATH  : "   + data[0] +
                        "\nNAME  : " + data[1] +
                        "\nEXT   : " + data[2] +
                        "\nCTIME : " + data[3] +
                        "\nSIZE  : " + str(data[4]) + " BYTES\n" +
                        "---------------------------------------------------------------------------------")
            self.server.sendData(dataGram)

class Watcher:
    DIRECTORY = ""
    SERVER = False
    def __init__(self, DIRECTORY, SERVER = False):
        self.DIRECTORY = DIRECTORY
        if SERVER != False:
            self.SERVER = True


    def run(self):
        event_handler = Handler()
        if self.SERVER != False:
            event_handler.attachServer(self.SERVER)
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