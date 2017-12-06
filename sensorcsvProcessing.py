'''
author: HAK
time  : 02:00 AM, 07/11/2017
'''

import json
import numpy as np
import pandas as pd
import sys
import time
import os
import datetime
from Config import ConfigPaths, predixConnection
from colorama import Fore


def CheckOldData():
    try:
        with open("DefaultDataStore/Default_Store.csv" , "r") as file:
            lines = file.readlines()
        for i in lines:
            data=i.split(";")
            predixConnection.timeSeries.queue(data[0], value=data[1], timestamp=data[2].replace('\n',''))
            predixConnection.timeSeries.send()
            print(data)
        os.remove("DefaultDataStore/Default_Store.csv")
    except Exception:
        print ("No Internet :(")
        print ("Old Data Not Found! :)")


'''
1 argument is for CSV FILE DEFINED in FILE
2 argument is for JSON FILE
'''
#PATHS FOR CSV's
#-------------------------------------------------------------------------------------------------
PATH_TO_CSV_SENSOR_AGGREGATED = ConfigPaths.config['hak.aggregated.csv']['SENSORS_AGGREGATED_CSV']
PATH_TO_CSV_SENSOR_INVERTERS = sorted(list(dict(ConfigPaths.config.items('hak.sensors')).values()))
PATH_OF_JSON_FILE = sys.argv[1]
#-------------------------------------------------------------------------------------------------


if PATH_OF_JSON_FILE == '':
    print(Fore.YELLOW,'PATH TO JSON FILE NOT DEFINED', Fore.RESET)
    sys.exit(1)


# JSON file for parsing data
#-------------------------------------------------------------------------------------------------
data = json.load(open(PATH_OF_JSON_FILE, mode='r', encoding='utf-8', errors='ignore'))


'''
JS file consist data in following form
Invert 1 = |0 Internal| |1 Ambient| |2 Solar| |4 Wind|
Invert 2 = |0 Internal|
Invert 3 = |0 Internal|
'''


# DATA DICTIONARY that is used to whole dataframe for aggregated file
#-------------------------------------------------------------------------------------------------
DATA_DICT = {'10': 0, '11': 0, '12': 0, '14': 0, '20': 0, '30': 0}


# SENSOR DICTIONARIES for individual sensor as only internal temperature differ and others remain
# same like ambient, wind and solar
#-------------------------------------------------------------------------------------------------
SENSOR_INDIVIDUAL_1 = {'10': 0, '11': 0, '12': 0, '14': 0}
SENSOR_INDIVIDUAL_2 = {'20': 0, '21': 0, '22': 0, '24': 0}
SENSOR_INDIVIDUAL_3 = {'30': 0, '31': 0, '32': 0, '34': 0}

parser_tag = ['1', '2', '3']
SENSOR_11 = None
SENSOR_12 = None
SENSOR_14 = None


# Loop through the parser
#-------------------------------------------------------------------------------------------------
for items in parser_tag:
    # Get the data items from json file
    json_data = data['Body'][items]

    # Check if json does contain data
    if json_data != None:
        for key, value in json_data.items():
            if key == '1':
                SENSOR_11 = value['Value']
            elif key == '2':
                SENSOR_12 = value['Value']
            elif key == '4':
                SENSOR_14 = value['Value']
            dict_key = items + key
            if json_data[key]['Value'] != None:
                DATA_DICT[dict_key] = json_data[key]['Value']
                if items == '1':
                    SENSOR_INDIVIDUAL_1[dict_key] = json_data[key]['Value']
                elif items == '2':
                    SENSOR_INDIVIDUAL_2[dict_key] = json_data[key]['Value']
                    SENSOR_INDIVIDUAL_2['21'] = SENSOR_11 if SENSOR_11 != None else np.nan
                    SENSOR_INDIVIDUAL_2['22'] = SENSOR_12 if SENSOR_12 != None else np.nan
                    SENSOR_INDIVIDUAL_2['24'] = SENSOR_14 if SENSOR_14 != None else np.nan
                elif items == '3':
                    SENSOR_INDIVIDUAL_3[dict_key] = json_data[key]['Value']
                    SENSOR_INDIVIDUAL_3['31'] = SENSOR_11 if SENSOR_11 != None else np.nan
                    SENSOR_INDIVIDUAL_3['32'] = SENSOR_12 if SENSOR_12 != None else np.nan
                    SENSOR_INDIVIDUAL_3['34'] = SENSOR_14 if SENSOR_14 != None else np.nan
            else:
                DATA_DICT[dict_key] = np.nan
    else:
        DATA_DICT[dict_key] = np.nan
#-------------------------------------------------------------------------------------------------


DATA_DICT['Timestamp'] = data['Head']['Timestamp']
SENSOR_INDIVIDUAL_1['Timestamp'] = data['Head']['Timestamp']
SENSOR_INDIVIDUAL_2['Timestamp'] = data['Head']['Timestamp']
SENSOR_INDIVIDUAL_3['Timestamp'] = data['Head']['Timestamp']


df = pd.DataFrame.from_records([DATA_DICT], index='10')
df_s1 = pd.DataFrame.from_records([SENSOR_INDIVIDUAL_1], index='10')
df_s2 = pd.DataFrame.from_records([SENSOR_INDIVIDUAL_2], index='20')
df_s3 = pd.DataFrame.from_records([SENSOR_INDIVIDUAL_3], index='30')
df_list = [df_s1, df_s2, df_s3]

# Below code consist dataframe push for predix
#-------------------------------------------------------------------------------------------------

# Check for old data that havent been pushed due to unavaible service or network issues
CheckOldData()
df_s1_p = pd.DataFrame.from_records([SENSOR_INDIVIDUAL_1], index='Timestamp')
df_s2_p = pd.DataFrame.from_records([SENSOR_INDIVIDUAL_2], index='Timestamp')
df_s3_p = pd.DataFrame.from_records([SENSOR_INDIVIDUAL_3], index='Timestamp')

# data frames list for predix
df_for_predix = [df_s1_p, df_s2_p, df_s3_p]
with open("Config/Tags.csv", "r") as file:
    tag = file.readlines()

# Loop through dataframes for predix
k=0
for i in df_for_predix:
    timeStamp = str(i.index[0]).replace('T', ' ')
    timeStamp = timeStamp.replace('+05:00', '')
    unixTimeStamp = int(time.mktime(datetime.datetime.strptime(timeStamp, "%Y-%m-%d %H:%M:%S").timetuple()))
    for j in range(len(list(i))):
        tag[k] = tag[k].replace("\n","")
        try:
            predixConnection.timeSeries.queue(tag[k], value=str(i.iloc[0, j]), timestamp=unixTimeStamp * 1000, quality=3)
            predixConnection.timeSeries.send()
            print(tag[k], str(i.iloc[0, j]))
        except Exception:
            print("No internet")
            with open("DefaultDataStore/Default_Store.csv", "a") as file:
                file.write(tag[k] + ";" + str(i.iloc[0,j]) + ";" + str(unixTimeStamp * 1000) + "\n")
                print(i.iloc[0, j], tag[k])
        k=k+1
#-------------------------------------------------------------------------------------------------


JOB_SCHEDULE = [[PATH_TO_CSV_SENSOR_AGGREGATED, df], [PATH_TO_CSV_SENSOR_INVERTERS[0], df_s1], [PATH_TO_CSV_SENSOR_INVERTERS[1], df_s2], [PATH_TO_CSV_SENSOR_INVERTERS[2], df_s3]]
fileObj = None
count = 0
if os.path.exists(PATH_TO_CSV_SENSOR_AGGREGATED) \
        and os.path.exists(PATH_TO_CSV_SENSOR_INVERTERS[0]) \
        and os.path.exists(PATH_TO_CSV_SENSOR_INVERTERS[1]) \
        and os.path.exists(PATH_TO_CSV_SENSOR_INVERTERS[2]):
    while True:
        try:
            fileObj = open(JOB_SCHEDULE[count][0], 'a')
            print('trying to open file', JOB_SCHEDULE[count][0])
            if fileObj:
                print(Fore.GREEN, 'file not locked', JOB_SCHEDULE[count][0], Fore.RESET)
                JOB_SCHEDULE[count][1].to_csv(JOB_SCHEDULE[count][0], mode='a', header=False)
        except OSError:
            print(Fore.RED, 'file is locked', JOB_SCHEDULE[count][0], Fore.RESET)
        finally:
            if fileObj:
                fileObj.close()
                print(Fore.GREEN, 'written and closed', fileObj.name, Fore.RESET)
                count += 1
                if count > 3:
                    break
        time.sleep(2)
else:
    print(Fore.RED, 'One of path not exist', Fore.RESET)
    sys.exit(0)