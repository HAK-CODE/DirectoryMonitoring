import json
import pandas as pd
import sys
import time
import os

'''
1 argument is for CSV FILE DEFINED in FILE
2 argument is for JSON FILE
'''

PATH_TO_CSV_METER_AGGREGATED = 'C:/Users/hammad.ali/Desktop/DC DATA/METER/METER_AGGREGATE.csv'
PATH_OF_JSON_FILE = sys.argv[1]
#print(PATH_TO_CSV_METER_AGGREGATED)
#print(PATH_OF_JSON_FILE)

if PATH_OF_JSON_FILE == '':
    print('PATHS NOT DEFINED')
    sys.exit(1)

with open(PATH_OF_JSON_FILE) as data_file:
    data = json.load(data_file)

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

while True:
    if os.path.exists(PATH_TO_CSV_METER_AGGREGATED):
        try:
            fileObj = open(PATH_TO_CSV_METER_AGGREGATED, 'a')
            print('trying to open file ', PATH_TO_CSV_METER_AGGREGATED)
            if fileObj:
                print('file not locked', PATH_TO_CSV_METER_AGGREGATED)
                df.to_csv(PATH_TO_CSV_METER_AGGREGATED, mode='a', header=False)
        except OSError:
            print('file is locked', PATH_TO_CSV_METER_AGGREGATED)
        finally:
            if fileObj:
                fileObj.close()
                break
    time.sleep(3)