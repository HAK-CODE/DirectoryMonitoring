from jsonschema import validate

'''
--------------------------------------------------------------------------------------------
SENSOR schema for data fillement and validation
'''
SENSOR_JSON_META = dict({
    "SiteID": None,
    "SiteName": None,
    "Timestamp": None,
    "siteSensors": {
        "solarPotential": None,
        "wind": None,
        "ambientTemperature": None
    },
    "inverterSensors": {
        "InverterSensor_1": {
            "internalTemperature": None
        },
        "InverterSensor_2": {
            "internalTemperature": None
        },
        "InverterSensor_3": {
            "internalTemperature": None
        }
    }
})

SCHEMA_SENSOR = {
    "type": "object",
    "properties": {
        "SiteID": {"type": "number"},
        "SiteName": {"type": "string"},
        "Timestamp": {"type": "string"},
        "siteSensors": {
            "type": "object",
            "properties": {
                "solarPotential": {"type": "number"},
                "wind": {"type": "number"},
                "ambientTemperature": {"type": "number"}
            }
        },
        "inverterSensors": {
            "type": "object",
            "properties": {
                "InverterSensor_1": {
                    "type": "object",
                    "properties": {
                        "internalTemperature": {"type": "number"}
                    }
                },
                "InverterSensor_2": {
                    "type": "object",
                    "properties": {
                        "internalTemperature": {"type": "number"}
                    }
                },
                "InverterSensor_3": {
                    "type": "object",
                    "properties": {
                        "internalTemperature": {"type": "number"}
                    }
                }
            }
        }
    }
}

'''
--------------------------------------------------------------------------------------------
'''


'''
--------------------------------------------------------------------------------------------
INVERTER schema for data fillement and validation
'''

INVERTER_JSON_META = dict({
    "SiteID": None,
    "SiteName": None,
    "Timestamp": None,
    "inverter": {
        "ID": None,
        "DAY_ENERGY": None,
        "PAC": None,
        "TOTAL_ENERGY": None,
        "YEAR_ENERGY": None
    }
})

SCHEMA_INVERTER = {
    "type": "object",
    "properties": {
        "SiteID": {"type": "number"},
        "SiteName": {"type": "string"},
        "Timestamp": {"type": "string"},
        "inverter": {
            "type": "object",
            "properties": {
                "ID": {"type": "number"},
                "DAY_ENERGY": {"type": "number"},
                "PAC": {"type": "number"},
                "TOTAL_ENERGY": {"type": "number"},
                "YEAR_ENERGY": {"type": "number"}
            }
        }
    }
}

'''
--------------------------------------------------------------------------------------------
'''


class SCHEMA_PROVIDER:
    IS_SENSOR = False

    def __init__(self, isSensor):
        self.IS_SENSOR = isSensor

    def GET_SCHEMA(self):
        if self.IS_SENSOR:
            print('Sensor Schema')
            return SENSOR_JSON_META
        else:
            print('Inverter Schema')
            return INVERTER_JSON_META

    def VALIDATE(self, dictionary_obj):
        if self.IS_SENSOR:
            return True if validate(dictionary_obj, SCHEMA_SENSOR) == None else False
        else:
            return True if validate(dictionary_obj, SCHEMA_INVERTER) == None else False
