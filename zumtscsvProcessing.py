'''
author: HAK
time  : 02:00 AM, 07/11/2017
'''

import json
import numpy as np
import pandas as pd
import sys
import time
import datetime
import os
from Config import ConfigPaths, predixConnection
from colorama import Fore

# from DB import *
'''
This file is used to process inverter file and distribute data to particular
inverter csv. It takes two arguments 
1 argument is for CSV FILE DEFINED in FILE
2 argument is for JSON FILE
'''


def CheckOldData():
    try:
        with open("DefaultDataStore/Default_Store.csv", "r") as file:
            lines = file.readlines()
        for i in lines:
            data = i.split(";")
            predixConnection.timeSeries.queue(data[0], value=data[1], timestamp=data[2].replace('\n', ''))
            predixConnection.timeSeries.send()
            print(data)
        os.remove("DefaultDataStore/Default_Store.csv")
    except Exception:
        print("No Internet :(")
        print("Old Data Not Found! :)")


# -------------------------------------------------------------------------------------------------
PATH_OF_CSV_FILE = sys.argv[1]
# -------------------------------------------------------------------------------------------------


df = pd.read_csv(PATH_OF_CSV_FILE)

print(df)

# ---------------------------------
# new dataframe for timescale DB
'''
df_in1 = pd.DataFrame(rows[0]).transpose()
df_in2 = pd.DataFrame(rows[1]).transpose()
df_in3 = pd.DataFrame(rows[2]).transpose()
df_in1['inverter_id'] = 1
df_in1.columns = ['day_energy', 'pac', 'total_energy', 'year_energy', 'time', 'inverter_id']
df_in2['inverter_id'] = 2
df_in2.columns = ['day_energy', 'pac', 'total_energy', 'year_energy', 'time', 'inverter_id']
df_in3['inverter_id'] = 3
df_in3.columns = ['day_energy', 'pac', 'total_energy', 'year_energy', 'time', 'inverter_id']
df_list = [df_in1, df_in2, df_in3]
df_final = pd.concat(df_list)
df_final.set_index('inverter_id', inplace=True)
df_final['site_id'] = 1
db = DB_CLASS()
db.savetable(df_final, "inverter")

#-------------------------------------------------------------------
#---------------------------------
# data frames list for predix

CheckOldData()
df_1_in_p = pd.DataFrame(rows[0]).transpose()
df_1_in_p.columns=['DAY_ENERGY','PAC','TOTAL_ENERGY','YEAR_ENERGY','Timestamp']
df_1_in_p.set_index('Timestamp',inplace=True)

df_2_in_p = pd.DataFrame(rows[1]).transpose()
df_2_in_p.columns=['DAY_ENERGY','PAC','TOTAL_ENERGY','YEAR_ENERGY','Timestamp']
df_2_in_p.set_index('Timestamp',inplace=True)

df_3_in_p = pd.DataFrame(rows[2]).transpose()
df_3_in_p.columns=['DAY_ENERGY','PAC','TOTAL_ENERGY','YEAR_ENERGY','Timestamp']
df_3_in_p.set_index('Timestamp',inplace=True)

df_for_predix = [df_1_in_p, df_2_in_p, df_3_in_p]
with open("Config/Tags_Inverter.csv", "r") as file:
    tag = file.readlines()

# Loop through dataframes for predix
k=0
for i in df_for_predix:
    timeStamp = str(i.index[0]).replace('T', ' ')
    timeStamp = timeStamp.replace('+05:00', '')
    unixTimeStamp = int(time.mktime(datetime.datetime.strptime(timeStamp, "%Y-%m-%d %H:%M:%S").timetuple()))
    unixTimeStamp=((unixTimeStamp * 1000) - 18000000)
    for j in range(len(list(i))):
        tag[k] = tag[k].replace("\n" , "")
        try:
            predixConnection.timeSeries.queue(tag[k], value=str(i.iloc[0, j]), timestamp=unixTimeStamp , quality=3)
            a = predixConnection.timeSeries.send()
            print(a)
            print(tag[k], str(i.iloc[0, j]))
        except Exception:
            print("No internet")
            with open("DefaultDataStore/Default_Store.csv", "a") as file:
                file.write(tag[k] + ";" + str(i.iloc[0 , j]) + ";" + str(unixTimeStamp * 1000) + "\n")
                print(i.iloc[0, j], tag[k])
        k=k+1

#-------------------------------------------------------------------------------------------------
#df_1.to_csv('./DATA FOR BOKEH/data.csv', header=False)

with open("Config/Tags_Inverter.csv", "r") as file:
    tag = file.readlines()

tag = [t.replace('\n','') for t in tag[-12:]]
df_1.set_index(4)

JOB_SCHEDULE = [[PATH_TO_CSV_INVERTER_AGGREGATED, df],[PATH_TO_CSV_INVERTERS[0], df_1], [PATH_TO_CSV_INVERTERS[1], df_2], [PATH_TO_CSV_INVERTERS[2], df_3]]
fileObj = None
count = 0
if os.path.exists(PATH_TO_CSV_INVERTER_AGGREGATED) \
            and os.path.exists(PATH_TO_CSV_INVERTERS[0]) \
            and os.path.exists(PATH_TO_CSV_INVERTERS[1]) \
            and os.path.exists(PATH_TO_CSV_INVERTERS[2]):
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
       # time.sleep(2)
    #sys.exit(1)
else:
    print(Fore.RED,'One of path not exist', Fore.RESET)
    sys.exit(0)
'''

#for i, time in enumerate(df['TIMESTAMP']):
#    print(str(i)+' index '+str(time))

#df['TIMESTAMP'] = str(df['TIMESTAMP']).replace('/','-').replace('\n1','')
#print(df)

# ------------------------------------------------------------
# for Predix push
#df_for_predix = df.set_index('TIMESTAMP')
#print(df_for_predix)

#CheckOldData()
with open("Config/gentags.csv", "r") as file:
    tag = file.readlines()
k = 0

'''
for i in df_for_predix:
    timeStamp=str(i.index[0]).replace('/','-')
    timeStamp = timeStamp.replace('\n', '')
    unixTimeStamp = int(time.mktime(datetime.datetime.strptime(timeStamp, "%d-%m-%Y %H:%M:%S").timetuple()))
    unixTimeStamp = ((unixTimeStamp * 1000) - 18000000)
    for j in range(len (list(i))):
        try:
            predixConnection.timeSeries.queue(tag[k], value=str(i.iloc[0, j]), timestamp=unixTimeStamp, quality=3)
            a = predixConnection.timeSeries.send()
            print(a)
            print(tag[k], str(df.iloc[0, j]))
        except Exception:
            print("No internet")
            with open("DefaultDataStore/Default_Store.csv", "a") as file:
                file.write(tag[k] + ";" +str(i.iloc[0, j]) + ";" + str(unixTimeStamp * 1000) + "\n")
                print(df.iloc[0, j], tag[k])
        k = k + 1
'''