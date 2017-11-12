'''
author: HAK
time  : 11:00 PM, 09/11/2017
'''

import os
import configparser
import sys
from shutil import copy
import time
import csv
import ntpath
import os
headers = ['File Name', 'Timestamp']


#Directory path to get files
#--------------------------------------------------------------------------------------------
directory_path = 'C:/Users/Hammad Ali Khan/Desktop/DC'
#--------------------------------------------------------------------------------------------


#Check for directory existence
if not os.path.isdir(directory_path):
    print('Given directory does not exist')
    sys.exit(1)
#--------------------------------------------------------------------------------------------


#Directories paths to put files
#--------------------------------------------------------------------------------------------
config = configparser.ConfigParser()
config.sections()
config.read('./fileDistribution.ini')
paths_list = [x[1] for x in config.items('hak.paths')]
#--------------------------------------------------------------------------------------------


#Csvs paths to put files
#--------------------------------------------------------------------------------------------
csv_list = [x[1] for x in config.items('hak.csv')]
print(csv_list)
#--------------------------------------------------------------------------------------------


#Update csv for files added
#--------------------------------------------------------------------------------------------
def update_csv(path, timeoffile, pathName):
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
                csvfile.close()
                break
        time.sleep(3)
#--------------------------------------------------------------------------------------------


#Check for each directory existence
#--------------------------------------------------------------------------------------------
for paths in paths_list:
    if not os.path.isdir(paths):
        print('Provided directory for some variable not exist')
        sys.exit(1)
#--------------------------------------------------------------------------------------------


countFiles = [0, 0, 0, 0, 0]
for file in os.listdir(directory_path):
    fullPath = os.path.join(directory_path, file)
    newname = fullPath.replace('.js','.json') if '.json' not in fullPath else fullPath
    filectime = time.ctime(os.path.getmtime(fullPath))
    os.rename(fullPath, newname)

    if 'SENSOR' in newname:
        countFiles[0] += 1
        copy(newname, paths_list[3])
        update_csv(csv_list[3], filectime, paths_list[3]+'/'+ntpath.basename(newname))
    elif 'LOG' in newname:
        countFiles[1] += 1
        copy(newname, paths_list[2])
        update_csv(csv_list[2], filectime, paths_list[2]+'/'+ntpath.basename(newname))
    elif 'ERRORS' in newname:
        countFiles[2] += 1
        copy(newname, paths_list[0])
        update_csv(csv_list[0], filectime, paths_list[0]+'/'+ntpath.basename(newname))
    elif ntpath.basename(newname).startswith('METER'):
        countFiles[4] += 1
        copy(newname, paths_list[4])
        update_csv(csv_list[4], filectime, paths_list[4]+'/'+ntpath.basename(newname))
    elif ntpath.basename(newname).startswith('INVERTER'):
        countFiles[3] += 1
        copy(newname, paths_list[1])
        commandExe = 'start python '+'jsontocsvProcessing.py'+' '+'\"'+paths_list[1]+'/'+ntpath.basename(newname)+'\"'
        os.system(commandExe)
        update_csv(csv_list[1], filectime, paths_list[1]+'/'+ntpath.basename(newname))
    os.remove(newname)
#--------------------------------------------------------------------------------------------


print('SENSOR:',countFiles[0])
print('LOG:',countFiles[1])
print('ERRORS:',countFiles[2])
print('INVERTER:',countFiles[3])
print('METER:',countFiles[4])