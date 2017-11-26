import json
import numpy as np
import pandas as pd
import sys
import time
import os
import configparser

'''
This file is used to process inverter file and distribute data to particular
inverter csv. It takes two arguments 
1 argument is for CSV FILE DEFINED in FILE
2 argument is for JSON FILE
'''

#-------------------------------------------------------------------------------------------------
#PATHS FOR CSV's
#-------------------------------------------------------------------------------------------------
config = configparser.ConfigParser()
config.sections()
config.read('./Config/fileDistribution.ini')
PATH_TO_CSV_INVERTER_AGGREGATED = config['hak.aggregated.csv']['INVERTER_AGGREGATED_CSV']
PATH_TO_CSV_INVERTER_1 = config['hak.inverters']['INVERTER_1']
PATH_TO_CSV_INVERTER_2 = config['hak.inverters']['INVERTER_2']
PATH_TO_CSV_INVERTER_3 = config['hak.inverters']['INVERTER_3']
PATH_OF_JSON_FILE = sys.argv[1]
#-------------------------------------------------------------------------------------------------


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
while True:
    if os.path.exists(PATH_TO_CSV_INVERTER_AGGREGATED) \
            and os.path.exists(PATH_TO_CSV_INVERTER_1) \
            and os.path.exists(PATH_TO_CSV_INVERTER_2) \
            and os.path.exists(PATH_TO_CSV_INVERTER_3):
        try:
            if count == 0:
                fileObj = open(PATH_TO_CSV_INVERTER_AGGREGATED, 'a')
                print('trying to open file ',PATH_TO_CSV_INVERTER_AGGREGATED)
            else:
                fileObj = open(JOB_SCHEDULE[count - 1][0], 'a')
                print('trying to open file ', JOB_SCHEDULE[count - 1][0])
            if fileObj:
                if count == 0:
                    print('file not locked',PATH_TO_CSV_INVERTER_AGGREGATED)
                    df.to_csv(PATH_TO_CSV_INVERTER_AGGREGATED, mode='a', header=False)
                else:
                    print('file not locked', JOB_SCHEDULE[count - 1][0])
                    JOB_SCHEDULE[count - 1][1].to_csv(JOB_SCHEDULE[count - 1][0], mode='a', header=False, index=False)
        except OSError:
            if count == 0:
                print('file is locked',PATH_TO_CSV_INVERTER_AGGREGATED)
            else:
                print('file is locked',JOB_SCHEDULE[count - 1][0])
        finally:
            if fileObj:
                print('written and closed ', fileObj.name)
                fileObj.close()
                count += 1
                if count > 3:
                    break
    else:
        print('One of path not exist')
        break
    time.sleep(3)
sys.exit(1)