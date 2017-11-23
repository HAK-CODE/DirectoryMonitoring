from SCHEMAS.SCHEMA import SCHEMA_PROVIDER
import sys
import time

def saif_validate(VDICTIONARY):
    SCHEMAOBJ = SCHEMA_PROVIDER(True)
    try:
        if SCHEMAOBJ.VALIDATE(VDICTIONARY)!= 'False':
            print("Valid Json file")
            SCHEMA = SCHEMAOBJ.GET_SCHEMA()

        else:
            print("Invalid Format")
    except Exception as e:
        print("Invalid Format")
        pass
    print(VDICTIONARY)

'''
def saif_create(df_s1,df_s2,df_s3):

    main_dict={}
    main_dict["1"] = df_s1
    main_dict["2"] = df_s2
    main_dict["3"] = df_s3


    other_dict=SENSOR_JSON_META

    other_dict["SiteID"] = 12
    other_dict["SiteName"] = "Dawood"

    other_dict["Timestamp"] = list(main_dict["1"]["Timestamp"].values())[0]
    other_dict["siteSensors"]["solarPotential"] = list(main_dict["1"]["12"].values())[0]
    other_dict["siteSensors"]["wind"] = list(main_dict["1"]["14"].values())[0]
    other_dict["siteSensors"]["ambientTemperature"] = list(main_dict["1"]["11"].values())[0]
    other_dict["inverterSensors"]["InverterSensor_1"]["internalTemperature"] = list(main_dict["1"]["11"].keys())[0]
    other_dict["inverterSensors"]["InverterSensor_2"]["internalTemperature"] = list(main_dict["2"]["21"].keys())[0]
    other_dict["inverterSensors"]["InverterSensor_3"]["internalTemperature"] = list(main_dict["3"]["31"].keys())[0]

    return saif_validate(other_dict)
'''








