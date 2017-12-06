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
from Config import ConfigPaths, predixConnection
from colorama import Fore

'''
This file is used to process inverter file and distribute data to particular
inverter csv. It takes two arguments 
1 argument is for CSV FILE DEFINED in FILE
2 argument is for JSON FILE
'''

#PATHS FOR CSV's
#-------------------------------------------------------------------------------------------------
PATH_TO_CSV_INVERTER_AGGREGATED = ConfigPaths.config['hak.aggregated.csv']['INVERTER_AGGREGATED_CSV']
PATH_TO_CSV_INVERTER_1 = ConfigPaths.config['hak.inverters']['INVERTER_1']
PATH_TO_CSV_INVERTER_2 = ConfigPaths.config['hak.inverters']['INVERTER_2']
PATH_TO_CSV_INVERTER_3 = ConfigPaths.config['hak.inverters']['INVERTER_3']
PATH_OF_JSON_FILE = sys.argv[1]
#-------------------------------------------------------------------------------------------------


#JSON file for parsing data
#-------------------------------------------------------------------------------------------------
if PATH_OF_JSON_FILE == '':
    print(Fore.YELLOW,'PATH TO JSON FILE NOT DEFINED', Fore.RESET)
    sys.exit(1)

data = json.load(open(PATH_OF_JSON_FILE, mode='r'))

DATA_DICT = {'1': [], '2': [], '3': []}
keys = ['DAY_ENERGY', 'PAC', 'TOTAL_ENERGY', 'YEAR_ENERGY']
flag = True

for items in keys:
    json_data = (data['Body'][items]['Values'])
    for dict_key in ['1','2','3']:
        if dict_key in json_data:
            DATA_DICT[dict_key].append(json_data[dict_key])
        else:
            DATA_DICT[dict_key].append(np.nan)
        if flag:
            DATA_DICT[dict_key].append(data['Head']['Timestamp'])
    flag = False

df = pd.DataFrame(DATA_DICT).transpose()[[0,2,3,4,1]]
rows = []

for row in df.iterrows():
    index, data = row
    rows.append(data.tolist())

df_1 = pd.DataFrame(rows[0]).transpose()
df_2 = pd.DataFrame(rows[1]).transpose()
df_3 = pd.DataFrame(rows[2]).transpose()

df_1.to_csv('./DATA FOR BOKEH/data.csv', header=False)

with open("Config/Tags.csv", "r") as file:
    tag = file.readlines()

tag = [t.replace('\n','') for t in tag[-12:]]
df_1.set_index(4)

JOB_SCHEDULE = [[PATH_TO_CSV_INVERTER_AGGREGATED, df],[PATH_TO_CSV_INVERTER_1, df_1], [PATH_TO_CSV_INVERTER_2, df_2], [PATH_TO_CSV_INVERTER_3, df_3]]
fileObj = None
count = 0
if os.path.exists(PATH_TO_CSV_INVERTER_AGGREGATED) \
            and os.path.exists(PATH_TO_CSV_INVERTER_1) \
            and os.path.exists(PATH_TO_CSV_INVERTER_2) \
            and os.path.exists(PATH_TO_CSV_INVERTER_3):
    while True:
        try:
            fileObj = open(JOB_SCHEDULE[count][0], 'a')
            print('trying to open file', JOB_SCHEDULE[count][0])
            if fileObj:
                print(Fore.GREEN,'file not locked', JOB_SCHEDULE[count][0], Fore.RESET)
                JOB_SCHEDULE[count][1].to_csv(JOB_SCHEDULE[count][0], mode='a', header=False, index=True if count == 0 else False)
        except OSError:
            print(Fore.RED,'file is locked',JOB_SCHEDULE[count][0], Fore.RESET)
        finally:
            if fileObj:
                fileObj.close()
                print(Fore.GREEN, 'written and closed', fileObj.name, Fore.RESET)
                count += 1
                if count > 3:
                    break
        time.sleep(2)
    sys.exit(1)
else:
    print(Fore.RED,'One of path not exist', Fore.RESET)
    sys.exit(0)