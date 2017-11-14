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

PATH_TO_CSV_SENSOR_AGGREGATED = 'C:/Users/hammad.ali/Desktop/DC DATA/SENSOR/SENSOR_AGGREGATE.csv'
PATH_OF_JSON_FILE = sys.argv[1]
print(PATH_TO_CSV_SENSOR_AGGREGATED)
print(PATH_OF_JSON_FILE)

if PATH_OF_JSON_FILE == '':
    print('PATHS NOT DEFINED')
    sys.exit(1)

with open(PATH_OF_JSON_FILE) as data_file:
    data = json.load(data_file)

DATA_DICT = {'10': 0, '11': 0, '12': 0, '14': 0, '20': 0, '30': 0}
parser_tag = ['1', '2', '3']
for items in parser_tag:
    json_data = data['Body'][items]
    for key, value in json_data.items():
        dict_key = items + key
        if json_data[key]['Value'] != None or json_data[key]['Value'] != '':
            DATA_DICT[dict_key] = json_data[key]['Value']
        else:
            DATA_DICT[dict_key] = np.nan

DATA_DICT['Timestamp'] = data['Head']['Timestamp']
df = pd.DataFrame.from_records([DATA_DICT], index='10')

while True:
    if os.path.exists(PATH_TO_CSV_SENSOR_AGGREGATED):
        try:
            fileObj = open(PATH_TO_CSV_SENSOR_AGGREGATED, 'a')
            print('trying to open file ', PATH_TO_CSV_SENSOR_AGGREGATED)
            if fileObj:
                print('file not locked', PATH_TO_CSV_SENSOR_AGGREGATED)
                df.to_csv(PATH_TO_CSV_SENSOR_AGGREGATED, mode='a', header=False)
        except OSError:
            print('file is locked', PATH_TO_CSV_SENSOR_AGGREGATED)
        finally:
            if fileObj:
                fileObj.close()
                break
    time.sleep(3)