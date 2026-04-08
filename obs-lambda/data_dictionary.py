# variables = {
#     "incoming variable name from data provider": {
#         "vargem": "Synoptic Variable Table Vargem",
#         "VNUM": "Synotic VNUM",
#         "long_name": "Long name description",
#         "incoming_unit": "incoming data unit from data provider",
#         "final_unit": "final unit that POE is expecting (should be metric unit if keeping units in json slug)"
#     }
# }

variables = {
    "W": {
        "vargem": "WLEV",
        "VNUM": "1",
        "long_name": "Water Level",
        "incoming_unit": "cm",
        "final_unit": "m"
    },
    "Q": {
        "vargem": "SFLO",
        "VNUM": "1",
        "long_name": "Discharge",
        "incoming_unit": "m3/s",
        "final_unit": "ft3/s"
    },
    "WT": {
        "vargem": "TLKE",
        "VNUM": "1",
        "long_name": "Water Temperature",
        "incoming_unit": "degC",
        "final_unit": "degC"
    },
    "LT": {
        "vargem": "TMPF",
        "VNUM": "1",
        "long_name": "Air Temperature",
        "incoming_unit": "degC",
        "final_unit": "degC"
    },
    "N": {
        "vargem": "PREC",
        "VNUM": "1",
        "long_name": "Precipitation Accumulation",
        "incoming_unit": "mm",
        "final_unit": "mm"
    },
}