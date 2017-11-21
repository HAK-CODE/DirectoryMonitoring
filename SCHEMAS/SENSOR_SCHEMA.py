from jsonschema import validate

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
