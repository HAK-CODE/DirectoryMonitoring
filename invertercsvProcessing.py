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
from Config import ConfigPaths
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


for items in keys:
    json_data = (data['Body'][items]['Values'])
    for dict_key in ['1','2','3']:
        if dict_key in json_data:
            DATA_DICT[dict_key].append(json_data[dict_key])
        else:
            DATA_DICT[dict_key].append(np.nan)


for keys in DATA_DICT:
    DATA_DICT[keys].append(data['Head']['Timestamp'])


df = pd.DataFrame(DATA_DICT)
df = df.transpose()

rows = []

for row in df.iterrows():
    index, data = row
    rows.append(data.tolist())

df_1 = pd.DataFrame(rows[0]).transpose()
df_2 = pd.DataFrame(rows[1]).transpose()
df_3 = pd.DataFrame(rows[2]).transpose()

JOB_SCHEDULE = [[PATH_TO_CSV_INVERTER_1, df_1], [PATH_TO_CSV_INVERTER_2, df_2], [PATH_TO_CSV_INVERTER_3, df_3]]
fileObj = None
count = 0
if os.path.exists(PATH_TO_CSV_INVERTER_AGGREGATED) \
            and os.path.exists(PATH_TO_CSV_INVERTER_1) \
            and os.path.exists(PATH_TO_CSV_INVERTER_2) \
            and os.path.exists(PATH_TO_CSV_INVERTER_3):
    while True:
        try:
            if count == 0:
                fileObj = open(PATH_TO_CSV_INVERTER_AGGREGATED, 'a')
                print('trying to open file ',PATH_TO_CSV_INVERTER_AGGREGATED)
            else:
                fileObj = open(JOB_SCHEDULE[count - 1][0], 'a')
                print('trying to open file ', JOB_SCHEDULE[count - 1][0])
            if fileObj:
                if count == 0:
                    print(Fore.GREEN,'file not locked',PATH_TO_CSV_INVERTER_AGGREGATED, Fore.RESET)
                    df.to_csv(PATH_TO_CSV_INVERTER_AGGREGATED, mode='a', header=False)
                else:
                    print(Fore.GREEN,'file not locked', JOB_SCHEDULE[count - 1][0], Fore.RESET)
                    JOB_SCHEDULE[count - 1][1].to_csv(JOB_SCHEDULE[count - 1][0], mode='a', header=False, index=False)
        except OSError:
            if count == 0:
                print(Fore.RED,'file is locked',PATH_TO_CSV_INVERTER_AGGREGATED, Fore.RESET)
            else:
                print(Fore.RED,'file is locked',JOB_SCHEDULE[count - 1][0], Fore.RESET)
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