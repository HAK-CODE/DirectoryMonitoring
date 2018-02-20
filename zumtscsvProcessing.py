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
import ftplib
import traceback  
import glob

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
df.set_index('TIMESTAMP',inplace=True)


CheckOldData()
df1 = df.drop('TRIGGER',1)
df1=df1.drop('INDEX',1)
#CheckOldData()
with open("Config/gentags.csv", "r") as file:
    tag = file.readlines()

df_for_predix=[df1]
#print(df_for_predix)
counter= 0
timeStamp = df1.index.str.replace('/', '-')

for i in range(len(df1.index)):
    timeStamp1= timeStamp[i]#str(i.index[0]).replace('/','-')
    #timeStamp = timeStamp.replace('\n', '')
    unixTimeStamp = int(time.mktime(datetime.datetime.strptime(timeStamp1, "%d-%m-%Y %H:%M:%S").timetuple()))
    unixTimeStamp = ((unixTimeStamp * 1000) - 18000000)
    for j in range(len(df.columns)-2):
        tag[j] = tag[j].replace("\n","")
        try:
            predixConnection.timeSeries.queue(tag[j], value=str(df1.iloc[i, j]), timestamp=unixTimeStamp, quality=3)
            a = predixConnection.timeSeries.send()
            print(a)
            print(tag[j], str(df1.iloc[i, j]))
        except Exception:
            print("No internet")
            with open("DefaultDataStore/Default_Store.csv", "a") as file:
                file.write(tag[j] + ";" +str(df1.iloc[i, j]) + ";" + str(unixTimeStamp * 1000) + "\n")
                print(df1.iloc[i, j], tag[j])
    counter+=1
# for i in range(len(df1.index)):
#     k=0
#     timeStamp1= timeStamp[i]#str(i.index[0]).replace('/','-')
#     #timeStamp = timeStamp.replace('\n', '')
#     unixTimeStamp = int(time.mktime(datetime.datetime.strptime(timeStamp1, "%d-%m-%Y %H:%M:%S").timetuple()))
#     unixTimeStamp = ((unixTimeStamp * 1000) - 18000000)
#     for j in range(len(df.columns)-1):
#         if k < 16:
#             tag[k] = tag[k].replace("\n","")
#             try:
#                 predixConnection.timeSeries.queue(tag[k], value=str(df1.iloc[i, j]), timestamp=unixTimeStamp, quality=3)
#                 a = predixConnection.timeSeries.send()
#                 print(a)
#                 print(tag[k], str(df1.iloc[i, j]))
#             except Exception:
#                 print("No internet")
#                 with open("DefaultDataStore/Default_Store.csv", "a") as file:
#                     file.write(tag[k] + ";" +str(df1.iloc[i, j]) + ";" + str(unixTimeStamp * 1000) + "\n")
#                     print(df1.iloc[i, j], tag[k])
#         k=k+1
#     counter+=1
    
Output_Directory = "/TestDawood"
port = 21
username = "reonenergy"
password = "@Reonenergy92"
print("Logging in...")  
ftp = ftplib.FTP()  
ftp.connect("ftpreonenergy.qosenergy.com", port)
ftp.set_debuglevel(1)
ftp.set_pasv(False)
csv_path = os.path.join(os.getcwd(),PATH_OF_CSV_FILE)
print(csv_path)
print(ftp.getwelcome())  
try:  
    try:  
        ftp.login(username, password)  
        ftp.cwd(Output_Directory)  
        for File2Send in glob.glob(csv_path):
            name = os.path.split(File2Send)[1]  
            f = open(File2Send, "rb")
            ftp.storbinary('STOR ' + name, f)  
            f.close()  
            print("OK")      
            os.remove(File2Send) 
    finally:  
        print("Quitting...")  
        ftp.quit()  
except:  
    traceback.print_exc()