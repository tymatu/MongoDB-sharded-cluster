from pymongo import MongoClient
import pprint
import datetime

# Connect to MongoDB router
client = MongoClient("mongodb://admin:admin@router01:27017/?authSource=admin")
db = client["weatherDB"]

# Function to execute and print query results
def execute_query(query_name, collection_name, pipeline):
    print(f"\n{'=' * 50}")
    print(f"QUERY {query_name}: {collection_name}")
    print(f"{'=' * 50}")
    
    print(f"Pipeline: {pprint.pformat(pipeline)}")
    print("\nResults:")
    
    results = list(db[collection_name].aggregate(pipeline))
    if not results:
        print("No results found.")
    else:
        for result in results[:5]:  # Limit to first 5 results for readability
            pprint.pprint(result)
        
        if len(results) > 5:
            print(f"... and {len(results) - 5} more results.")
        
    print(f"Total results: {len(results)}")
    return results

# QUERY 9: Weather trend analysis with moving averages using $window
# Uses: $match, $sort, $addFields, $window, $project
q9_pipeline = [
    {"$match": {"location": "Prague"}},
    {"$addFields": {
        "date_obj": {"$dateFromString": {"dateString": "$date", "format": "%Y-%m-%d"}}
    }},
    {"$sort": {"date_obj": 1}},
    {"$window": {
        "documents": ["$date_obj", -2, 2],
        "output": {
            "moving_avg_temp": {
                "$avg": "$temperature_c"
            },
            "moving_avg_humidity": {
                "$avg": "$humidity_percent"
            },
            "document_count": {
                "$sum": 1
            }
        }
    }},
    {"$project": {
        "_id": 0,
        "station_id": 1,
        "date": 1,
        "actual_temperature_c": "$temperature_c",
        "moving_avg_temperature_c": {"$round": ["$moving_avg_temp", 1]},
        "actual_humidity_percent": "$humidity_percent",
        "moving_avg_humidity_percent": {"$round": ["$moving_avg_humidity", 1]},
        "points_in_window": "$document_count"
    }},
    {"$match": {"points_in_window": {"$gte": 3}}},
    {"$sort": {"date_obj": 1}}
]
execute_query("9", "weatherHistory", q9_pipeline)

# QUERY 10: Identifying correlated weather patterns across locations using $lookup and $densify
# Uses: $match, $sort, $densify, $lookup, $project
q10_pipeline = [
    {"$match": {"location": "Brno", "event_type": {"$exists": true}}},
    {"$addFields": {
        "date_obj": {"$dateFromString": {"dateString": "$date", "format": "%Y-%m-%d"}}
    }},
    {"$sort": {"date_obj": 1}},
    {"$lookup": {
        "from": "globalClimate",
        "let": {"event_date": "$date", "event_type": "$event_type"},
        "pipeline": [
            {"$match": {
                "$expr": {
                    "$and": [
                        {"$ne": ["$location", "Brno"]},
                        {"$eq": ["$event_type", "$$event_type"]},
                        {"$gte": [{"$dateFromString": {"dateString": "$date", "format": "%Y-%m-%d"}}, 
                                 {"$dateFromString": {"dateString": "$$event_date", "format": "%Y-%m-%d"}}]},
                        {"$lte": [{"$dateFromString": {"dateString": "$date", "format": "%Y-%m-%d"}}, 
                                 {"$dateAdd": {"startDate": {"$dateFromString": {"dateString": "$$event_date", "format": "%Y-%m-%d"}}, "unit": "day", "amount": 7}}]}
                    ]
                }
            }},
            {"$project": {
                "_id": 0,
                "location": 1,
                "date": 1,
                "event_type": 1,
                "temperature_c": 1,
                "precipitation_mm": 1,
                "days_after": {
                    "$dateDiff": {
                        "startDate": {"$dateFromString": {"dateString": "$$event_date", "format": "%Y-%m-%d"}},
                        "endDate": {"$dateFromString": {"dateString": "$date", "format": "%Y-%m-%d"}},
                        "unit": "day"
                    }
                }
            }}
        ],
        "as": "related_events"
    }},
    {"$match": {"related_events": {"$ne": []}}},
    {"$project": {
        "_id": 0,
        "date": 1,
        "event_type": 1,
        "temperature_c": 1,
        "precipitation_mm": 1,
        "location": 1,
        "related_events_count": {"$size": "$related_events"},
        "related_locations": {"$addToSet": "$related_events.location"},
        "related_events": 1
    }},
    {"$sort": {"related_events_count": -1}}
]
execute_query("10", "usWeatherEvents", q10_pipeline)

# QUERY 11: Complex time-series analysis using $densify and $fill
# Uses: $match, $addFields, $densify, $fill, $sort, $project
q11_pipeline = [
    {"$match": {"location": "Ostrava", "date": {"$regex": "^2021"}}},
    {"$addFields": {
        "date_obj": {"$dateFromString": {"dateString": "$date", "format": "%Y-%m-%d"}}
    }},
    {"$sort": {"date_obj": 1}},
    {"$densify": {
        "field": "date_obj",
        "range": {
            "step": 1,
            "unit": "month",
            "bounds": "full"
        }
    }},
    {"$fill": {
        "output": {
            "temperature_c": {"method": "locf"},
            "humidity_percent": {"method": "locf"},
            "location": {"value": "Ostrava"}
        }
    }},
    {"$addFields": {
        "month": {"$month": "$date_obj"},
        "year": {"$year": "$date_obj"},
        "is_interpolated": {"$not": {"$ifNull": ["$station_id", false]}}
    }},
    {"$project": {
        "_id": 0,
        "date": {"$dateToString": {"format": "%Y-%m-%d", "date": "$date_obj"}},
        "temperature_c": 1,
        "humidity_percent": 1,
        "location": 1,
        "year": 1,
        "month": 1,
        "is_interpolated": 1
    }},
    {"$sort": {"date_obj": 1}}
]
execute_query("11", "weatherHistory", q11_pipeline)

# QUERY 12: Advanced data enrichment with $lookup and $merge
# Uses: $lookup, $addFields, $merge, $project, $sort
q12_pipeline = [
    {"$match": {"event_type": {"$in": ["Storm", "Wind"]}}},
    {"$lookup": {
        "from": "weatherHistory",
        "let": {"station_id": "$station_id", "event_date": "$date"},
        "pipeline": [
            {"$match": {
                "$expr": {
                    "$and": [
                        {"$eq": ["$station_id", "$$station_id"]},
                        {"$or": [
                            {"$eq": ["$date", "$$event_date"]},
                            {"$eq": ["$date", {"$dateToString": {
                                "date": {"$dateAdd": {"startDate": {"$dateFromString": {"dateString": "$$event_date", "format": "%Y-%m-%d"}},
                                                     "unit": "day", "amount": -1}},
                                "format": "%Y-%m-%d"
                            }}]},
                            {"$eq": ["$date", {"$dateToString": {
                                "date": {"$dateAdd": {"startDate": {"$dateFromString": {"dateString": "$$event_date", "format": "%Y-%m-%d"}},
                                                     "unit": "day", "amount": -2}},
                                "format": "%Y-%m-%d"
                            }}]}
                        ]}
                    ]
                }
            }},
            {"$sort": {"date": -1}},
            {"$limit": 1},
            {"$project": {
                "_id": 0,
                "previous_temperature_c": "$temperature_c",
                "previous_humidity_percent": "$humidity_percent",
                "previous_date": "$date"
            }}
        ],
        "as": "historical_data"
    }},
    {"$addFields": {
        "historical_data": {"$arrayElemAt": ["$historical_data", 0]},
        "risk_score": {
            "$cond": {
                "if": {"$gt": ["$wind_speed_kmh", 40]},
                "then": {
                    "$add": [
                        10,
                        {"$multiply": [{"$divide": ["$wind_speed_kmh", 10]}, 2]},
                        {"$multiply": [{"$divide": ["$precipitation_mm", 10]}, 1.5]}
                    ]
                },
                "else": {
                    "$add": [
                        5,
                        {"$multiply": [{"$divide": ["$wind_speed_kmh", 10]}, 1]},
                        {"$multiply": [{"$divide": ["$precipitation_mm", 10]}, 1]}
                    ]
                }
            }
        }
    }},
    {"$project": {
        "_id": 0,
        "station_id": 1,
        "date": 1,
        "location": 1,
        "event_type": 1,
        "current_data": {
            "temperature_c": "$temperature_c",
            "humidity_percent": "$humidity_percent",
            "wind_speed_kmh": "$wind_speed_kmh",
            "precipitation_mm": "$precipitation_mm"
        },
        "historical_data": 1,
        "temperature_change": {
            "$cond": {
                "if": {"$ifNull": ["$historical_data.previous_temperature_c", false]},
                "then": {"$subtract": ["$temperature_c", "$historical_data.previous_temperature_c"]},
                "else": null
            }
        },
        "humidity_change": {
            "$cond": {
                "if": {"$ifNull": ["$historical_data.previous_humidity_percent", false]},
                "then": {"$subtract": ["$humidity_percent", "$historical_data.previous_humidity_percent"]},
                "else": null
            }
        },
        "risk_score": {"$round": ["$risk_score", 1]}
    }},
    {"$sort": {"risk_score": -1}}
]
execute_query("12", "globalClimate", q12_pipeline)

# QUERY 13: Advanced pattern recognition using $setWindowFields
# Uses: $addFields, $setWindowFields, $match, $sort, $project
q13_pipeline = [
    {"$addFields": {
        "date_obj": {"$dateFromString": {"dateString": "$date", "format": "%Y-%m-%d"}}
    }},
    {"$sort": {"location": 1, "date_obj": 1}},
    {"$setWindowFields": {
        "partitionBy": "$location",
        "sortBy": {"date_obj": 1},
        "output": {
            "temp_diff": {
                "$derivative": {
                    "input": "$temperature_c",
                    "unit": "day"
                }
            },
            "humidity_diff": {
                "$derivative": {
                    "input": "$humidity_percent",
                    "unit": "day"
                }
            },
            "temp_moving_avg": {
                "$avg": {
                    "input": "$temperature_c",
                    "window": {
                        "documents": [-2, 2]
                    }
                }
            },
            "precip_7day_sum": {
                "$sum": {
                    "input": "$precipitation_mm",
                    "window": {
                        "documents": [-7, 0]
                    }
                }
            }
        }
    }},
    {"$match": {
        "$or": [
            {"temp_diff": {"$gt": 10}},
            {"temp_diff": {"$lt": -10}},
            {"precip_7day_sum": {"$gt": 100}}
        ]
    }},
    {"$project": {
        "_id": 0,
        "station_id": 1,
        "date": 1,
        "location": 1,
        "temperature_c": 1,
        "temperature_change_per_day": {"$round": ["$temp_diff", 2]},
        "humidity_change_per_day": {"$round": ["$humidity_diff", 2]},
        "temperature_7day_avg": {"$round": ["$temp_moving_avg", 1]},
        "precipitation_7day_total": {"$round": ["$precip_7day_sum", 1]},
        "pattern_detected": {
            "$switch": {
                "branches": [
                    {"case": {"$gt": ["$temp_diff", 10]}, "then": "Rapid Warming"},
                    {"case": {"$lt": ["$temp_diff", -10]}, "then": "Rapid Cooling"},
                    {"case": {"$gt": ["$precip_7day_sum", 100]}, "then": "Sustained Precipitation"}
                ],
                "default": "Normal"
            }
        }
    }},
    {"$sort": {"date_obj": 1}}
]
execute_query("13", "usWeatherEvents", q13_pipeline)

# QUERY 14: Analyzing correlation between weather factors with $project, $bucket and $addFields
# Uses: $addFields, $bucket, $project, $sort
q14_pipeline = [
    {"$addFields": {
        "temp_humidity_correlation": {
            "$cond": [
                {"$eq": ["$humidity_percent", 0]},
                null,
                {"$multiply": ["$temperature_c", "$humidity_percent"]}
            ]
        },
        "precipitation_intensity": {
            "$cond": [
                {"$eq": ["$precipitation_mm", 0]},
                "None",
                {"$switch": {
                    "branches": [
                        {"case": {"$lte": ["$precipitation_mm", 5]}, "then": "Very Light"},
                        {"case": {"$lte": ["$precipitation_mm", 20]}, "then": "Light"},
                        {"case": {"$lte": ["$precipitation_mm", 50]}, "then": "Moderate"},
                        {"case": {"$lte": ["$precipitation_mm", 80]}, "then": "Heavy"}
                    ],
                    "default": "Extreme"
                }}
            ]
        },
        "temp_range": {
            "$switch": {
                "branches": [
                    {"case": {"$lt": ["$temperature_c", 0]}, "then": "Below Freezing"},
                    {"case": {"$lt": ["$temperature_c", 10]}, "then": "Cold"},
                    {"case": {"$lt": ["$temperature_c", 20]}, "then": "Mild"},
                    {"case": {"$lt": ["$temperature_c", 30]}, "then": "Warm"}
                ],
                "default": "Hot"
            }
        }
    }},
    {"$group": {
        "_id": {
            "temp_range": "$temp_range",
            "precipitation_intensity": "$precipitation_intensity"
        },
        "count": {"$sum": 1},
        "avg_humidity": {"$avg": "$humidity_percent"},
        "avg_wind": {"$avg": "$wind_speed_kmh"},
        "samples": {"$push": {
            "station_id": "$station_id",
            "date": "$date",
            "location": "$location",
            "temperature_c": "$temperature_c",
            "precipitation_mm": "$precipitation_mm"
        }}
    }},
    {"$addFields": {
        "correlation_observation": {
            "$switch": {
                "branches": [
                    {
                        "case": {
                            "$and": [
                                {"$in": ["$_id.temp_range", ["Warm", "Hot"]]},
                                {"$in": ["$_id.precipitation_intensity", ["Moderate", "Heavy", "Extreme"]]}
                            ]
                        },
                        "then": "High temperature with significant precipitation - tropical pattern"
                    },
                    {
                        "case": {
                            "$and": [
                                {"$in": ["$_id.temp_range", ["Below Freezing", "Cold"]]},
                                {"$in": ["$_id.precipitation_intensity", ["Moderate", "Heavy", "Extreme"]]}
                            ]
                        },
                        "then": "Cold with significant precipitation - winter storm pattern"
                    },
                    {
                        "case": {
                            "$and": [
                                {"$in": ["$_id.temp_range", ["Mild"]]},
                                {"$in": ["$_id.precipitation_intensity", ["Light", "Very Light"]]}
                            ]
                        },
                        "then": "Mild temperature with light precipitation - typical spring/fall pattern"
                    }
                ],
                "default": "No specific correlation pattern"
            }
        }
    }},
    {"$project": {
        "_id": 0,
        "temperature_range": "$_id.temp_range",
        "precipitation_intensity": "$_id.precipitation_intensity",
        "event_count": "$count",
        "avg_humidity_percent": {"$round": ["$avg_humidity", 1]},
        "avg_wind_speed_kmh": {"$round": ["$avg_wind", 1]},
        "correlation_observation": 1,
        "sample_records": {"$slice": ["$samples", 2]}
    }},
    {"$sort": {"event_count": -1}}
]
execute_query("14", "globalClimate", q14_pipeline)

# QUERY 15: Cross-collection weather event sequence analysis with $lookup
# Uses: $match, $lookup, $project, $sort with complex conditions
q15_pipeline = [
    {"$match": {"event_type": "Storm"}},
    {"$addFields": {
        "date_obj": {"$dateFromString": {"dateString": "$date", "format": "%Y-%m-%d"}}
    }},
    {"$lookup": {
        "from": "globalClimate",
        "let": {"storm_location": "$location", "storm_date": "$date_obj"},
        "pipeline": [
            {"$addFields": {
                "date_obj": {"$dateFromString": {"dateString": "$date", "format": "%Y-%m-%d"}}
            }},
            {"$match": {
                "$expr": {
                    "$and": [
                        {"$eq": ["$location", "$$storm_location"]},
                        {"$gt": ["$date_obj", "$$storm_date"]},
                        {"$lt": ["$date_obj", {"$dateAdd": {"startDate": "$$storm_date", "unit": "day", "amount": 14}}]},
                        {"$ne": ["$event_type", "Storm"]}
                    ]
                }
            }},
            {"$sort": {"date_obj": 1}},
            {"$limit": 3},
            {"$project": {
                "_id": 0,
                "event_type": 1,
                "date": 1,
                "temperature_c": 1,
                "precipitation_mm": 1,
                "days_after_storm": {
                    "$dateDiff": {
                        "startDate": "$$storm_date",
                        "endDate": "$date_obj",
                        "unit": "day"
                    }
                }
            }}
        ],
        "as": "subsequent_events"
    }},
    {"$match": {"subsequent_events": {"$ne": []}}},
    {"$project": {
        "_id": 0,
        "station_id": 1,
        "date": 1,
        "location": 1,
        "storm_data": {
            "temperature_c": "$temperature_c",
            "wind_speed_kmh": "$wind_speed_kmh",
            "precipitation_mm": "$precipitation_mm"
        },
        "subsequent_events": 1,
        "has_heatwave_after": {
            "$in": ["Heatwave", "$subsequent_events.event_type"]
        },
        "has_rain_after": {
            "$in": ["Rain", "$subsequent_events.event_type"]
        }
    }},
    {"$sort": {"date_obj": -1}}
]
execute_query("15", "usWeatherEvents", q15_pipeline)

# QUERY 16: Multi-stage data transformation with $set, $unset and conditionals
# Uses: $addFields, $set, $unset, $project, $sort
q16_pipeline = [
    {"$match": {"temperature_c": {"$gte": 0}}},
    {"$addFields": {
        "date_obj": {"$dateFromString": {"dateString": "$date", "format": "%Y-%m-%d"}},
        "temp_fahrenheit": {"$round": [{"$add": [{"$multiply": ["$temperature_c", 1.8]}, 32]}, 1]},
        "wind_speed_mph": {"$round": [{"$multiply": ["$wind_speed_kmh", 0.621371]}, 1]},
        "precipitation_inches": {"$round": [{"$multiply": ["$precipitation_mm", 0.0393701]}, 2]}
    }},
    {"$set": {
        "heat_index": {
            "$cond": {
                "if": {"$and": [{"$gte": ["$temp_fahrenheit", 80]}, {"$gte": ["$humidity_percent", 40]}]},
                "then": {
                    "$round": [
                        {"$subtract": [
                            {"$add": [
                                -42.379,
                                {"$multiply": [2.04901523, "$temp_fahrenheit"]},
                                {"$multiply": [10.14333127, "$humidity_percent"]},
                                {"$multiply": [-0.22475541, "$temp_fahrenheit", "$humidity_percent"]},
                                {"$multiply": [-0.00683783, {"$pow": ["$temp_fahrenheit", 2]}]},
                                {"$multiply": [-0.05481717, {"$pow": ["$humidity_percent", 2]}]},
                                {"$multiply": [0.00122874, {"$pow": ["$temp_fahrenheit", 2]}, "$humidity_percent"]},
                                {"$multiply": [0.00085282, "$temp_fahrenheit", {"$pow": ["$humidity_percent", 2]}]},
                                {"$multiply": [-0.00000199, {"$pow": ["$temp_fahrenheit", 2]}, {"$pow": ["$humidity_percent", 2]}]}
                            ]},
                            0  // Subtract 0 (placeholder to use $subtract)
                        ]},
                        1
                    ]
                },
                "else": null
            }
        },
        "wind_chill": {
            "$cond": {
                "if": {"$and": [{"$lt": ["$temp_fahrenheit", 50]}, {"$gt": ["$wind_speed_mph", 3]}]},
                "then": {
                    "$round": [
                        {"$add": [
                            35.74,
                            {"$multiply": [0.6215, "$temp_fahrenheit"]},
                            {"$subtract": [0, {"$multiply": [35.75, {"$pow": ["$wind_speed_mph", 0.16]}]}]},
                            {"$multiply": [0.4275, "$temp_fahrenheit", {"$pow": ["$wind_speed_mph", 0.16]}]}
                        ]},
                        1
                    ]
                },
                "else": null
            }
        }
    }},
    {"$set": {
        "comfort_index": {
            "$cond": {
                "if": {"$ne": [{"$ifNull": ["$heat_index", null]}, null]},
                "then": {"$subtract": ["$heat_index", "$temp_fahrenheit"]},
                "else": {
                    "$cond": {
                        "if": {"$ne": [{"$ifNull": ["$wind_chill", null]}, null]},
                        "then": {"$subtract": ["$temp_fahrenheit", "$wind_chill"]},
                        "else": 0
                    }
                }
            }
        }
    }},
    {"$project": {
        "_id": 0,
        "station_id": 1,
        "date": 1,
        "location": 1,
        "metrics": {
            "celsius": "$temperature_c",
            "fahrenheit": "$temp_fahrenheit",
            "humidity_percent": "$humidity_percent",
            "wind_kmh": "$wind_speed_kmh",
            "wind_mph": "$wind_speed_mph",
            "precipitation_mm": "$precipitation_mm",
            "precipitation_inches": "$precipitation_inches",
            "heat_index": "$heat_index",
            "wind_chill": "$wind_chill"
        },
        "comfort_level": {
            "$switch": {
                "branches": [
                    {"case": {"$gt": ["$comfort_index", 20]}, "then": "Very Uncomfortable"},
                    {"case": {"$gt": ["$comfort_index", 10]}, "then": "Uncomfortable"},
                    {"case": {"$gt": ["$comfort_index", 5]}, "then": "Slightly Uncomfortable"},
                    {"case": {"$lt": ["$comfort_index", -10]}, "then": "Extremely Cold"},
                    {"case": {"$lt": ["$comfort_index", -5]}, "then": "Very Cold"},
                    {"case": {"$lt": ["$comfort_index", 0]}, "then": "Cold"}
                ],
                "default": "Comfortable"
            }
        },
        "comfort_index": {"$round": ["$comfort_index", 1]}
    }},
    {"$sort": {"comfort_index": -1}}
]
execute_query("16", "weatherHistory", q16_pipeline)

print("\nAll queries in part 2 executed successfully.") 