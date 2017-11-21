from jsonschema import validate

'''
--------------------------------------------------------------------------------------------
'''
SENSOR_JSON_META = dict({
    "SiteID" : None,
    "SiteName" : None,
    "Timestamp" : None,
    "siteSensors" : {
        "solarPotential" : None,
        "for wind" : None,
        "ambientTemperature" : None
    },
    "inverterSensors": {
        "ID": None,
        "internalTemperature": None
    }
})

SCHEMA_SENSOR = {
    "type" : "object",
    "properties" : {
        "SiteID" : {"type" : "number"},
        "SiteName" : {"type" : "string"},
        "Timestamp" : {"type" : "string"},
        "siteSensors" : {
            "type" : "object",
            "properties" : {
                "solarPotential": {"type" : "number"},
                "wind": {"type" : "number"},
                "ambientTemperature": {"type" : "number"}
            }
        },
        "inverterSensors": {
            "type" : "object",
            "properties" : {
                "ID": {"type" : "number"},
                "internalTemperature": {"type" : "number"}
            }
        }
    },
}
'''
--------------------------------------------------------------------------------------------
'''


'''
--------------------------------------------------------------------------------------------
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
    "type" : "object",
    "properties" : {
        "SiteID" : {"type" : "number"},
        "SiteName" : {"type" : "string"},
        "Timestamp" : {"type" : "string"},
        "inverter" : {
            "type" : "object",
            "properties" : {
                "ID" : {"type" : "number"},
                "DAY_ENERGY" : {"type" : "number"},
                "PAC" : {"type" : "number"},
                "TOTAL_ENERGY" : {"type" : "number"},
                "YEAR_ENERGY" : {"type" : "number"}
            }
        }
    }
}

'''
--------------------------------------------------------------------------------------------
'''
