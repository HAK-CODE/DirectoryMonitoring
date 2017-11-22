from SCHEMAS.SCHEMA import SCHEMA_PROVIDER,SENSOR_JSON_META
# from sensorcsvProcessing import send_df
import sys
import time




#
# #df_list=sys.argv[1:sys.argv.index(']')+1]
# if sys.argv[1] == 'SENSOR':
#     print('recieving file from sensor')
#     print('Sensor')
# elif sys.argv[1] == 'INVERTER':
#     print('Inverter')
# else:
#     print('Not valid')
#     # time.sleep(5)
#     # sys.exit(0)


def saif_validate(VDICTIONARY):
    SCHEMA = SCHEMA_PROVIDER(True)
    try:
        if SCHEMA.VALIDATE(VDICTIONARY):
            return True
        else:
            return False
    except Exception as e:
        pass
    return False

def saif_create(df_s1,df_s2,df_s3):

    df_s1_keys=df_s1.keys()
    df_s2_keys = df_s2.keys()
    df_s3_keys = df_s2.keys()

    saif_dict={}
    saif_dict["1"]=df_s1
    saif_dict["2"] = df_s2
    saif_dict["3"] = df_s3


    saif_other=SENSOR_JSON_META




    saif_other["SiteID"] = 12
    saif_other["SiteName"] = "Dawood"

    saif_other["Timestamp"] = list(saif_dict["1"]["Timestamp"].values())[0]
    saif_other["siteSensors"]["solarPotential"] = list(saif_dict["1"]["12"].values())[0]
    saif_other["siteSensors"]["wind"] = list(saif_dict["1"]["14"].values())[0]
    saif_other["siteSensors"]["ambientTemperature"] = list(saif_dict["1"]["11"].values())[0]
    saif_other["inverterSensors"]["InverterSensor_1"]["internalTemperature"] = list(saif_dict["1"]["11"].keys())[0]
    saif_other["inverterSensors"]["InverterSensor_2"]["internalTemperature"] = list(saif_dict["2"]["21"].keys())[0]
    saif_other["inverterSensors"]["InverterSensor_3"]["internalTemperature"] = list(saif_dict["3"]["31"].keys())[0]

    return saif_validate(saif_other)









