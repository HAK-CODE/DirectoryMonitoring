"""
author: HAK
time  : 02:00 AM, 07/11/2017
"""

import json
import pandas as pd
import sys
import time
import os
from Config import ConfigPaths
from colorama import Fore

'''
1 argument is for CSV FILE DEFINED in FILE
2 argument is for JSON FILE
'''

# PATHS FOR CSV's
#-------------------------------------------------------------------------------------------------
PATH_TO_CSV_METER_AGGREGATED = ConfigPaths.config['hak.aggregated.csv']['METER_AGGREGATED_CSV']
PATH_OF_JSON_FILE = sys.argv[1]
#-------------------------------------------------------------------------------------------------


# JSON file for parsing data
#-------------------------------------------------------------------------------------------------
if PATH_OF_JSON_FILE == '':
    print(Fore.YELLOW,'PATH TO JSON FILE NOT DEFINED', Fore.RESET)
    sys.exit(1)


# JSON file for parsing data
#-------------------------------------------------------------------------------------------------
data = json.load(open(PATH_OF_JSON_FILE, mode='r'))


# DATA DICTIONARY for creating dataframe
#-------------------------------------------------------------------------------------------------
DATA_DICT = {'Code': "", 'Reason': "", 'UserMessage': "", 'Timestamp': ""}
json_data = (data['Head']['Status'])


for dict_key in ['Code', 'Reason', 'UserMessage']:
    if dict_key in json_data:
        DATA_DICT[dict_key] = json_data[dict_key]
    else:
        DATA_DICT[dict_key] = ""


DATA_DICT['Timestamp'] = data['Head']['Timestamp']
df = pd.DataFrame.from_records([DATA_DICT], index='Code')
df = df[['Reason', 'UserMessage', 'Timestamp']]
fileObj = None

if os.path.exists(PATH_TO_CSV_METER_AGGREGATED):
    while True:
        try:
            fileObj = open(PATH_TO_CSV_METER_AGGREGATED, 'a')
            print('trying to open file ', PATH_TO_CSV_METER_AGGREGATED)
            if fileObj:
                print(Fore.GREEN,'file not locked', PATH_TO_CSV_METER_AGGREGATED)
                df.to_csv(PATH_TO_CSV_METER_AGGREGATED, mode='a', header=False)
        except OSError:
            print(Fore.RED,'file is locked', PATH_TO_CSV_METER_AGGREGATED, Fore.RESET)
        finally:
            if fileObj:
                fileObj.close()
                print(Fore.GREEN, 'written and closed', fileObj.name, Fore.RESET)
                break
        time.sleep(2)
    sys.exit(1)
else:
    print(Fore.RED,'file path not exist', PATH_TO_CSV_METER_AGGREGATED, Fore.RESET)
    sys.exit(0)