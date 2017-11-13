import json
import numpy as np
import pandas as pd
import sys
import time
import os

'''
1 argument is for CSV FILE DEFINED in FILE
2 argument is for JSON FILE
'''

PATH_TO_CSV_INVERTER_AGGREGATED = 'C:\\Users\\Hammad Ali Khan\\Desktop\\DC DATA\\INVERTER\\INVERTER_AGGREGATE.csv'
PATH_OF_JSON_FILE = sys.argv[1]
#print(PATH_TO_CSV_INVERTER_AGGREGATED)
#print(PATH_OF_JSON_FILE)

if PATH_OF_JSON_FILE == '':
    print('PATHS NOT DEFINED')
    sys.exit(1)

with open(PATH_OF_JSON_FILE) as data_file:
    data = json.load(data_file)

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

while True:
    if os.path.exists(PATH_TO_CSV_INVERTER_AGGREGATED):
        try:
            fileObj = open(PATH_TO_CSV_INVERTER_AGGREGATED, 'a')
            print('trying to open file ',PATH_TO_CSV_INVERTER_AGGREGATED)
            if fileObj:
                print('file not locked',PATH_TO_CSV_INVERTER_AGGREGATED)
                df.to_csv(PATH_TO_CSV_INVERTER_AGGREGATED, mode='a', header=False)
        except OSError:
            print('file is locked',PATH_TO_CSV_INVERTER_AGGREGATED)
        finally:
            if fileObj:
                fileObj.close()
                break
    time.sleep(3)