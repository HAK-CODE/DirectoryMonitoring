'''
author: HAK
time  : 11:00 PM, 05/11/2017
'''

import os, time
import sys
from Config import ConfigPaths
from shutil import copy
import csv
import ntpath
import subprocess
from subprocess import DEVNULL
from colorama import Fore
headers = ['File Name', 'Timestamp']

#Get process id (Get the path for file check if its doe copying or downloading)
#--------------------------------------------------------------------------------------------
print('Process id is',str(os.getpid()))
filepath = sys.argv[1]
fileObj = None
#--------------------------------------------------------------------------------------------


#Directories paths to put files
#--------------------------------------------------------------------------------------------
paths_list = [x[1] for x in ConfigPaths.config.items('hak.paths')]
#--------------------------------------------------------------------------------------------


#Csvs paths to put files
#--------------------------------------------------------------------------------------------
csv_list = [x[1] for x in ConfigPaths.config.items('hak.csv')]
#--------------------------------------------------------------------------------------------


#Check for each directory existence
#--------------------------------------------------------------------------------------------
for paths in paths_list:
    if not os.path.isdir(paths):
        print(Fore.RED,'Provided directory for some variable not exist '+paths,Fore.RESET)
        sys.exit(0)
#--------------------------------------------------------------------------------------------


#Update csv for files added
#--------------------------------------------------------------------------------------------
def update_csv(path, timeOfFile, pathName):
    csvFile = None
    while True:
        try:
            csvFile = open(path, 'a')
            if csvFile:
                csvFile = open(path, 'a')
                writercsv = csv.DictWriter(csvFile, delimiter=',', lineterminator='\n', fieldnames=headers)
                writercsv.writerow({'File Name': str(pathName),
                                    'Timestamp': timeOfFile})
                print(Fore.GREEN,'CSV file not locked', csvFile.name, Fore.RESET)
        except OSError:
            print('csv locked', csvFile.name)
        finally:
            if csvFile:
                print(Fore.GREEN,'CSV file written and closed', csvFile.name, Fore.RESET)
                csvFile.close()
                break
        time.sleep(2)
# --------------------------------------------------------------------------------------------


# Keep running until a process releases file
#---------------------------------------------------------------------------------------------
if os.path.exists(filepath):
    while True:
        try:
            fileObj = open(filepath, 'a')
            print('trying to open file',filepath)
            if fileObj:
                print(Fore.GREEN,'file not locked',filepath,Fore.RESET)
        except OSError:
            print(Fore.GREEN,'file is locked',filepath,Fore.RESET)
        finally:
            if fileObj:
                fileObj.close()

                # Convert file from .js to .json and get file creation time
                newname = filepath.replace('.js', '.json') if '.json' not in filepath else filepath
                filectime = time.ctime(os.path.getmtime(filepath))
                os.rename(filepath, newname)

                # Start a new process based on file type (name)
                if 'SENSOR' in newname:
                    copy(newname, paths_list[3])
                    update_csv(csv_list[3], filectime, paths_list[3] + '/' + ntpath.basename(newname))
                    subprocess.Popen(['python3', 'sensorcsvProcessing.py', paths_list[3] + '/' + ntpath.basename(newname)])
                elif 'LOG' in newname:
                    copy(newname, paths_list[2])
                    update_csv(csv_list[2], filectime, paths_list[2] + '/' + ntpath.basename(newname))
                elif 'ERRORS' in newname:
                    copy(newname, paths_list[0])
                    update_csv(csv_list[0], filectime, paths_list[0] + '/' + ntpath.basename(newname))
                elif ntpath.basename(newname).startswith('METER'):
                    copy(newname, paths_list[4])
                    update_csv(csv_list[4], filectime, paths_list[4] + '/' + ntpath.basename(newname))
                    subprocess.Popen(['python3', 'metercsvProcessing.py', paths_list[4]+'/'+ntpath.basename(newname)])
                elif ntpath.basename(newname).startswith('INVERTER'):
                    copy(newname, paths_list[1])
                    update_csv(csv_list[1], filectime, paths_list[1] + '/' + ntpath.basename(newname))
                    subprocess.Popen(['python3', 'invertercsvProcessing.py', paths_list[1]+'/'+ntpath.basename(newname)])
                os.remove(newname)
                break
        time.sleep(2)
    sys.exit(1)
else:
   print('file path not exist', filepath)
   sys.exit(0)
#----------------------------------------------------------------------------------------------