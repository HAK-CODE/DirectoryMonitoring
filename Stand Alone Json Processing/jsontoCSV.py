import json
import numpy as np
import pandas as pd
import argparse
import os
import sys
import time

csv_file_path = ""
parser = argparse.ArgumentParser(prog='JSONtoCSV',
                                 formatter_class=argparse.ArgumentDefaultsHelpFormatter,
                                 description='Process all json files in current folder and output to csv file.')
parser.add_argument('--path', '-p', type=str, default='.', help="Specify full path to a directory where JSON files exist.")
args = parser.parse_args()


if not os.path.isdir(args.path):
    print("Directory or file does not exist on defined path.")
    sys.exit(1)

FOLDER_NAME = os.path.basename(os.path.normpath(args.path))

if not os.path.isfile(args.path+'\\'+FOLDER_NAME+'_AGGREGATE.csv'):
    print("File not found "+(FOLDER_NAME+'_AGGREGATE.csv'))
    sys.exit(1)

json_files = []
csv_file_path = args.path+'\\'+FOLDER_NAME+'_AGGREGATE.csv'


def update_csv(PATH_TO_CSV_AGGREGATED, df):
    while True:
        if os.path.exists(PATH_TO_CSV_AGGREGATED):
            try:
                fileObj = open(PATH_TO_CSV_AGGREGATED, 'a')
                print('trying to open file ', PATH_TO_CSV_AGGREGATED)
                if fileObj:
                    print('file not locked', PATH_TO_CSV_AGGREGATED)
                    df.to_csv(PATH_TO_CSV_AGGREGATED, mode='a', header=False)
            except OSError:
                print('file is locked', PATH_TO_CSV_AGGREGATED)
            finally:
                if fileObj:
                    fileObj.close()
                    break
        time.sleep(3)


if FOLDER_NAME == 'INVERTER':
    for filename in os.listdir(args.path):
        if filename.endswith('.json'):
            with open(os.path.join(args.path, filename)) as f:
                data = json.load(f)
            DATA_DICT = {'1': [], '2': [], '3': []}
            keys = ['DAY_ENERGY', 'PAC', 'TOTAL_ENERGY', 'YEAR_ENERGY']
            for items in keys:
                json_data = (data['Body'][items]['Values'])
                for dict_key in ['1', '2', '3']:
                    if dict_key in json_data:
                        DATA_DICT[dict_key].append(json_data[dict_key])
                    else:
                        DATA_DICT[dict_key].append(np.nan)

            for keys in DATA_DICT:
                DATA_DICT[keys].append(data['Head']['Timestamp'])
            df = pd.DataFrame(DATA_DICT)
            df = df.transpose()
            update_csv(csv_file_path, df)

elif FOLDER_NAME == 'METER':
    for filename in os.listdir(args.path):
        if filename.endswith('.json'):
            with open(os.path.join(args.path, filename)) as f:
                data = json.load(f)
            DATA_DICT = {'Code': "", 'Reason': "", 'UserMessage': "", 'Timestamp': ""}

            json_data = (data['Head']['Status'])
            for dict_key in ['Code', 'Reason', 'UserMessage']:
                if dict_key in json_data:
                    DATA_DICT[dict_key] = json_data[dict_key]
                else:
                    DATA_DICT[dict_key] = ""

            DATA_DICT['Timestamp'] = data['Head']['Timestamp']
            df = pd.DataFrame.from_records([DATA_DICT], index='Code')
            df = df[['Reason','UserMessage','Timestamp']]
            update_csv(csv_file_path, df)