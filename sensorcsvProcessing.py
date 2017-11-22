import json
import numpy as np
import pandas as pd
import sys
import time
import os
from UploadData import saif_validate,saif_create





'''
1 argument is for CSV FILE DEFINED in FILE
2 argument is for JSON FILE
'''
'''
-------------------------------------------------------------------------------------------------
PATHS FOR CSV's
-------------------------------------------------------------------------------------------------
'''
PATH_TO_CSV_SENSOR_AGGREGATED = 'C:/Users/hammad.ali/Desktop/DC DATA/SENSOR/SENSOR_AGGREGATE.csv'
PATH_TO_CSV_SENSOR_INVERTER_1 = 'C:/Users/hammad.ali/Desktop/DC DATA/SENSOR/SENSOR_INVERTER_1.csv'
PATH_TO_CSV_SENSOR_INVERTER_2 = 'C:/Users/hammad.ali/Desktop/DC DATA/SENSOR/SENSOR_INVERTER_2.csv'
PATH_TO_CSV_SENSOR_INVERTER_3 = 'C:/Users/hammad.ali/Desktop/DC DATA/SENSOR/SENSOR_INVERTER_3.csv'
PATH_OF_JSON_FILE = sys.argv[1]







print(PATH_TO_CSV_SENSOR_AGGREGATED)
print(PATH_OF_JSON_FILE)

if PATH_OF_JSON_FILE == '':
    print('PATHS NOT DEFINED')
    sys.exit(1)

'''
-------------------------------------------------------------------------------------------------
JSON file for parsing data
-------------------------------------------------------------------------------------------------
'''

with open(PATH_OF_JSON_FILE) as data_file:
    data = json.load(data_file)


DATA_DICT = {'10': 0, '11': 0, '12': 0, '14': 0, '20': 0, '30': 0}
SENSOR_INDIVIDUAL_1 = {'10': 0, '11': 0, '12': 0, '14': 0}
SENSOR_INDIVIDUAL_2 = {'20': 0, '21': 0, '22': 0, '24': 0}
SENSOR_INDIVIDUAL_3 = {'30': 0, '31': 0, '32': 0, '34': 0}
parser_tag = ['1', '2', '3']

SENSOR_11 = None
SENSOR_12 = None
SENSOR_14 = None

for items in parser_tag:
    json_data = data['Body'][items]
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

DATA_DICT['Timestamp'] = data['Head']['Timestamp']
SENSOR_INDIVIDUAL_1['Timestamp'] = data['Head']['Timestamp']
SENSOR_INDIVIDUAL_2['Timestamp'] = data['Head']['Timestamp']
SENSOR_INDIVIDUAL_3['Timestamp'] = data['Head']['Timestamp']

df = pd.DataFrame.from_records([DATA_DICT], index='10')
df_s1 = pd.DataFrame.from_records([SENSOR_INDIVIDUAL_1], index='10')
df_s2 = pd.DataFrame.from_records([SENSOR_INDIVIDUAL_2], index='20')
df_s3 = pd.DataFrame.from_records([SENSOR_INDIVIDUAL_3], index='30')
df_list = [df_s1, df_s2, df_s3]


# print(df_list)
print(saif_create(df_s1.to_dict(),df_s2.to_dict(),df_s3.to_dict()))
#saif_validate()
# commadexe = 'python '+'UploadData.py '+str('SENSOR')
# os.system(commadexe)


JOB_SCHEDULE = [[PATH_TO_CSV_SENSOR_INVERTER_1, df_s1], [PATH_TO_CSV_SENSOR_INVERTER_2, df_s2], [PATH_TO_CSV_SENSOR_INVERTER_3, df_s3]]
fileObj = None
count = 0
while True:
    if os.path.exists(PATH_TO_CSV_SENSOR_AGGREGATED) \
            and os.path.exists(PATH_TO_CSV_SENSOR_INVERTER_1)  \
            and os.path.exists(PATH_TO_CSV_SENSOR_INVERTER_2) \
            and os.path.exists(PATH_TO_CSV_SENSOR_INVERTER_3):
        try:
            if count == 0:
                fileObj = open(PATH_TO_CSV_SENSOR_AGGREGATED, 'a')
                print('trying to open file ', PATH_TO_CSV_SENSOR_AGGREGATED)
            else:
                fileObj = open(JOB_SCHEDULE[count - 1][0], 'a')
                print('trying to open file ', JOB_SCHEDULE[count - 1][0])
            if fileObj:
                if count == 0:
                    print('file not locked', PATH_TO_CSV_SENSOR_AGGREGATED)
                    df.to_csv(PATH_TO_CSV_SENSOR_AGGREGATED, mode='a', header=False)
                else:
                    print('file not locked', JOB_SCHEDULE[count - 1][0])
                    JOB_SCHEDULE[count - 1][1].to_csv(JOB_SCHEDULE[count - 1][0], mode='a', header=False)
        except OSError:
            if count == 0:
                print('file is locked', PATH_TO_CSV_SENSOR_AGGREGATED)
            else:
                print('file is locked', JOB_SCHEDULE[count - 1][0])
        finally:
            if fileObj:
                fileObj.close()
                count += 1
                if count > 3:
                    break
    else:
        print('One of path not exist')
        break
    time.sleep(3)
sys.exit(1)