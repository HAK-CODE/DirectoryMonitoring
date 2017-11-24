'''
author: HAK
time  : 11:00 PM, 05/11/2017
'''

import os, time
import sys
import configparser
from shutil import copy
import csv
import ntpath
headers = ['File Name', 'Timestamp']

#Get process id (Get the path for file check if its doe copying or downloading)
#--------------------------------------------------------------------------------------------
print('Process id is',str(os.getpid()))
filepath = sys.argv[1]
fileObj = None
#--------------------------------------------------------------------------------------------


#Directories paths to put files
#--------------------------------------------------------------------------------------------
config = configparser.ConfigParser()
config.sections()
config.read('./Config/fileDistribution.ini')
paths_list = [x[1] for x in config.items('hak.paths')]
#--------------------------------------------------------------------------------------------


#Csvs paths to put files
#--------------------------------------------------------------------------------------------
csv_list = [x[1] for x in config.items('hak.csv')]
#print(csv_list)
#--------------------------------------------------------------------------------------------


#Check for each directory existence
#--------------------------------------------------------------------------------------------
for paths in paths_list:
    if not os.path.isdir(paths):
        print('Provided directory for some variable not exist '+paths)
        sys.exit(0)
#--------------------------------------------------------------------------------------------


#Update csv for files added
#--------------------------------------------------------------------------------------------
def update_csv(path, timeoffile, pathName):
    csvfile = None
    while True:
        try:
            csvfile = open(path, 'a')
            if csvfile:
                csvfile = open(path, 'a')
                writercsv = csv.DictWriter(csvfile, delimiter=',', lineterminator='\n', fieldnames=headers)
                writercsv.writerow({'File Name': str(pathName),
                                    'Timestamp': timeoffile})
                print('csv file not locked')
        except OSError:
            print('csv locked')
        finally:
            if csvfile:
                print('CSV file written and closed.')
                csvfile.close()
                break
        time.sleep(3)
# --------------------------------------------------------------------------------------------


#keep running untill a process releases file
#---------------------------------------------------------------------------------------------
while True:
    if os.path.exists(filepath):
        try:
            fileObj = open(filepath, 'a')
            print('trying to open file ',filepath)
            if fileObj:
                print('file not locked',filepath)
        except OSError:
            print('file is locked',filepath)
        finally:
            if fileObj:
                fileObj.close()
                csvfile = None
                newname = filepath.replace('.js', '.json') if '.json' not in filepath else filepath
                filectime = time.ctime(os.path.getmtime(filepath))
                os.rename(filepath, newname)
                if 'SENSOR' in newname:
                    copy(newname, paths_list[3])
                    update_csv(csv_list[3], filectime, paths_list[3] + '/' + ntpath.basename(newname))
                    commandExe = 'start python ' + 'sensorcsvProcessing.py' + ' ' + '\"' + paths_list[3] + '/' + ntpath.basename(newname) + '\"'
                    os.system(commandExe)
                elif 'LOG' in newname:
                    copy(newname, paths_list[2])
                    update_csv(csv_list[2], filectime, paths_list[2] + '/' + ntpath.basename(newname))
                elif 'ERRORS' in newname:
                    copy(newname, paths_list[0])
                    update_csv(csv_list[0], filectime, paths_list[0] + '/' + ntpath.basename(newname))
                elif ntpath.basename(newname).startswith('METER'):
                    copy(newname, paths_list[4])
                    update_csv(csv_list[4], filectime, paths_list[4] + '/' + ntpath.basename(newname))
                    commandExe = 'start python ' + 'metercsvProcessing.py' + ' ' + '\"' + paths_list[4] + '/' + ntpath.basename(newname) + '\"'
                    os.system(commandExe)
                elif ntpath.basename(newname).startswith('INVERTER'):
                    copy(newname, paths_list[1])
                    update_csv(csv_list[1], filectime, paths_list[1] + '/' + ntpath.basename(newname))
                    commandExe = 'start python ' + 'invertercsvProcessing.py' + ' ' + '\"' + paths_list[1] + '/' + ntpath.basename(newname) + '\"'
                    os.system(commandExe)
                os.remove(newname)
                break
        time.sleep(3)
    else:
        print('file path not exist')
        break
        sys.exit(0)
sys.exit(1)
#----------------------------------------------------------------------------------------------