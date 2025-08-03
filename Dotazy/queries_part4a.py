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

# QUERY 23: Complex categorical analysis with $bucketAuto and $setWindowFields
q23_pipeline = [
    {"$match": {"event_type": {"$exists": true}}},
    {"$addFields": {
        "date_obj": {"$dateFromString": {"dateString": "$date", "format": "%Y-%m-%d"}},
        "temperature_category": {
            "$switch": {
                "branches": [
                    {"case": {"$lt": ["$temperature_c", 0]}, "then": "Below Freezing"},
                    {"case": {"$lt": ["$temperature_c", 15]}, "then": "Cold"},
                    {"case": {"$lt": ["$temperature_c", 25]}, "then": "Moderate"},
                    {"case": {"$lt": ["$temperature_c", 30]}, "then": "Warm"}
                ],
                "default": "Hot"
            }
        },
        "precipitation_category": {
            "$switch": {
                "branches": [
                    {"case": {"$lt": ["$precipitation_mm", 5]}, "then": "Very Low"},
                    {"case": {"$lt": ["$precipitation_mm", 25]}, "then": "Low"},
                    {"case": {"$lt": ["$precipitation_mm", 50]}, "then": "Moderate"},
                    {"case": {"$lt": ["$precipitation_mm", 75]}, "then": "High"}
                ],
                "default": "Very High"
            }
        }
    }},
    {"$setWindowFields": {
        "partitionBy": {
            "event_type": "$event_type",
            "temperature_category": "$temperature_category" 
        },
        "sortBy": {"date_obj": 1},
        "output": {
            "event_rank": {"$rank": {}},
            "avg_temp_window": {
                "$avg": {
                    "input": "$temperature_c",
                    "window": {
                        "documents": [-2, 2]
                    }
                }
            }
        }
    }},
    {"$sort": {"event_type": 1, "temperature_category": 1, "event_rank": 1}},
    {"$group": {
        "_id": {
            "event_type": "$event_type",
            "temperature_category": "$temperature_category",
            "precipitation_category": "$precipitation_category"
        },
        "count": {"$sum": 1},
        "avg_temperature": {"$avg": "$temperature_c"},
        "avg_humidity": {"$avg": "$humidity_percent"},
        "avg_wind_speed": {"$avg": "$wind_speed_kmh"},
        "avg_precipitation": {"$avg": "$precipitation_mm"},
        "locations": {"$addToSet": "$location"},
        "samples": {"$push": {
            "station_id": "$station_id", 
            "date": "$date", 
            "temperature_c": "$temperature_c", 
            "precipitation_mm": "$precipitation_mm",
            "avg_temp_window": "$avg_temp_window",
            "event_rank": "$event_rank"
        }}
    }},
    {"$sort": {"count": -1}},
    {"$project": {
        "_id": 0,
        "event_type": "$_id.event_type",
        "temperature_category": "$_id.temperature_category",
        "precipitation_category": "$_id.precipitation_category",
        "event_count": "$count",
        "locations": 1,
        "location_count": {"$size": "$locations"},
        "avg_temperature_c": {"$round": ["$avg_temperature", 1]},
        "avg_humidity_percent": {"$round": ["$avg_humidity", 1]},
        "avg_wind_speed_kmh": {"$round": ["$avg_wind_speed", 1]},
        "avg_precipitation_mm": {"$round": ["$avg_precipitation", 1]},
        "sample_events": {"$slice": ["$samples", 2]}
    }}
]
execute_query("23", "globalClimate", q23_pipeline)

# QUERY 24: Weather pattern anomaly detection with $merge and complex processing
q24_pipeline = [
    {"$match": {"event_type": {"$exists": true}}},
    {"$addFields": {
        "date_obj": {"$dateFromString": {"dateString": "$date", "format": "%Y-%m-%d"}},
        "year": {"$year": {"$dateFromString": {"dateString": "$date", "format": "%Y-%m-%d"}}},
        "month": {"$month": {"$dateFromString": {"dateString": "$date", "format": "%Y-%m-%d"}}}
    }},
    {"$group": {
        "_id": {
            "event_type": "$event_type",
            "location": "$location",
            "year": "$year",
            "month": "$month"
        },
        "avg_temp": {"$avg": "$temperature_c"},
        "avg_precip": {"$avg": "$precipitation_mm"},
        "avg_wind": {"$avg": "$wind_speed_kmh"},
        "count": {"$sum": 1}
    }},
    {"$group": {
        "_id": {
            "event_type": "$_id.event_type",
            "location": "$_id.location"
        },
        "months": {"$push": {
            "year": "$_id.year",
            "month": "$_id.month",
            "avg_temperature_c": "$avg_temp",
            "avg_precipitation_mm": "$avg_precip",
            "avg_wind_speed_kmh": "$avg_wind",
            "event_count": "$count"
        }},
        "avg_temp_overall": {"$avg": "$avg_temp"},
        "avg_precip_overall": {"$avg": "$avg_precip"},
        "avg_wind_overall": {"$avg": "$avg_wind"},
        "total_months": {"$sum": 1}
    }},
    {"$match": {"total_months": {"$gte": 2}}},
    {"$addFields": {
        "std_dev_temp": {
            "$stdDevPop": {
                "$map": {
                    "input": "$months",
                    "as": "month",
                    "in": "$$month.avg_temperature_c"
                }
            }
        },
        "std_dev_precip": {
            "$stdDevPop": {
                "$map": {
                    "input": "$months",
                    "as": "month",
                    "in": "$$month.avg_precipitation_mm"
                }
            }
        },
        "std_dev_wind": {
            "$stdDevPop": {
                "$map": {
                    "input": "$months",
                    "as": "month",
                    "in": "$$month.avg_wind_speed_kmh"
                }
            }
        }
    }},
    {"$addFields": {
        "anomalies": {
            "$filter": {
                "input": "$months",
                "as": "month",
                "cond": {
                    "$or": [
                        {"$gt": [{"$abs": {"$subtract": ["$$month.avg_temperature_c", "$avg_temp_overall"]}}, {"$multiply": ["$std_dev_temp", 1.5]}]},
                        {"$gt": [{"$abs": {"$subtract": ["$$month.avg_precipitation_mm", "$avg_precip_overall"]}}, {"$multiply": ["$std_dev_precip", 1.5]}]},
                        {"$gt": [{"$abs": {"$subtract": ["$$month.avg_wind_speed_kmh", "$avg_wind_overall"]}}, {"$multiply": ["$std_dev_wind", 1.5]}]}
                    ]
                }
            }
        }
    }},
    {"$match": {"anomalies": {"$ne": []}}},
    {"$project": {
        "_id": 0,
        "event_type": "$_id.event_type",
        "location": "$_id.location",
        "baseline": {
            "avg_temperature_c": {"$round": ["$avg_temp_overall", 1]},
            "avg_precipitation_mm": {"$round": ["$avg_precip_overall", 1]},
            "avg_wind_speed_kmh": {"$round": ["$avg_wind_overall", 1]},
            "std_dev_temperature": {"$round": ["$std_dev_temp", 2]},
            "std_dev_precipitation": {"$round": ["$std_dev_precip", 2]},
            "std_dev_wind_speed": {"$round": ["$std_dev_wind", 2]}
        },
        "total_months_analyzed": "$total_months",
        "anomaly_count": {"$size": "$anomalies"},
        "anomaly_percentage": {"$round": [{"$multiply": [{"$divide": [{"$size": "$anomalies"}, "$total_months"]}, 100]}, 1]},
        "anomalies": {
            "$map": {
                "input": "$anomalies",
                "as": "anomaly",
                "in": {
                    "year_month": {"$concat": [{"$toString": "$$anomaly.year"}, "-", {"$toString": "$$anomaly.month"}]},
                    "temperature_c": {"$round": ["$$anomaly.avg_temperature_c", 1]},
                    "temperature_deviation": {"$round": [{"$subtract": ["$$anomaly.avg_temperature_c", "$avg_temp_overall"]}, 1]},
                    "precipitation_mm": {"$round": ["$$anomaly.avg_precipitation_mm", 1]},
                    "precipitation_deviation": {"$round": [{"$subtract": ["$$anomaly.avg_precipitation_mm", "$avg_precip_overall"]}, 1]},
                    "wind_speed_kmh": {"$round": ["$$anomaly.avg_wind_speed_kmh", 1]},
                    "wind_speed_deviation": {"$round": [{"$subtract": ["$$anomaly.avg_wind_speed_kmh", "$avg_wind_overall"]}, 1]},
                    "anomaly_type": {
                        "$concat": [
                            {"$cond": [
                                {"$gt": [{"$abs": {"$subtract": ["$$anomaly.avg_temperature_c", "$avg_temp_overall"]}}, {"$multiply": ["$std_dev_temp", 1.5]}]},
                                "Temperature", ""
                            ]},
                            {"$cond": [
                                {"$and": [
                                    {"$gt": [{"$abs": {"$subtract": ["$$anomaly.avg_temperature_c", "$avg_temp_overall"]}}, {"$multiply": ["$std_dev_temp", 1.5]}]},
                                    {"$or": [
                                        {"$gt": [{"$abs": {"$subtract": ["$$anomaly.avg_precipitation_mm", "$avg_precip_overall"]}}, {"$multiply": ["$std_dev_precip", 1.5]}]},
                                        {"$gt": [{"$abs": {"$subtract": ["$$anomaly.avg_wind_speed_kmh", "$avg_wind_overall"]}}, {"$multiply": ["$std_dev_wind", 1.5]}]}
                                    ]}
                                ]},
                                " & ", ""
                            ]},
                            {"$cond": [
                                {"$gt": [{"$abs": {"$subtract": ["$$anomaly.avg_precipitation_mm", "$avg_precip_overall"]}}, {"$multiply": ["$std_dev_precip", 1.5]}]},
                                "Precipitation", ""
                            ]},
                            {"$cond": [
                                {"$and": [
                                    {"$gt": [{"$abs": {"$subtract": ["$$anomaly.avg_precipitation_mm", "$avg_precip_overall"]}}, {"$multiply": ["$std_dev_precip", 1.5]}]},
                                    {"$gt": [{"$abs": {"$subtract": ["$$anomaly.avg_wind_speed_kmh", "$avg_wind_overall"]}}, {"$multiply": ["$std_dev_wind", 1.5]}]}
                                ]},
                                " & ", ""
                            ]},
                            {"$cond": [
                                {"$gt": [{"$abs": {"$subtract": ["$$anomaly.avg_wind_speed_kmh", "$avg_wind_overall"]}}, {"$multiply": ["$std_dev_wind", 1.5]}]},
                                "Wind", ""
                            ]}
                        ]
                    }
                }
            }
        }
    }},
    {"$sort": {"anomaly_count": -1}}
]
execute_query("24", "usWeatherEvents", q24_pipeline)

# QUERY 25: Advanced temporal analysis with $linearFill and custom metrics
q25_pipeline = [
    {"$addFields": {
        "date_obj": {"$dateFromString": {"dateString": "$date", "format": "%Y-%m-%d"}},
        "heat_index": {
            "$cond": [
                {"$and": [
                    {"$gte": ["$temperature_c", 20]},
                    {"$gte": ["$humidity_percent", 40]}
                ]},
                {"$add": [
                    -8.78469475556,
                    {"$multiply": [1.61139411, "$temperature_c"]},
                    {"$multiply": [2.33854883889, "$humidity_percent"]},
                    {"$multiply": [-0.14611605, "$temperature_c", "$humidity_percent"]},
                    {"$multiply": [-0.012308094, {"$pow": ["$temperature_c", 2]}]},
                    {"$multiply": [-0.0164248277778, {"$pow": ["$humidity_percent", 2]}]},
                    {"$multiply": [0.002211732, {"$pow": ["$temperature_c", 2]}, "$humidity_percent"]},
                    {"$multiply": [0.00072546, "$temperature_c", {"$pow": ["$humidity_percent", 2]}]},
                    {"$multiply": [-0.000003582, {"$pow": ["$temperature_c", 2]}, {"$pow": ["$humidity_percent", 2]}]}
                ]},
                "$temperature_c"
            ]
        }
    }},
    {"$sort": {"location": 1, "date_obj": 1}},
    {"$group": {
        "_id": "$location",
        "date_range": {
            "$push": {
                "date": "$date_obj",
                "temperature_c": "$temperature_c",
                "precipitation_mm": "$precipitation_mm",
                "humidity_percent": "$humidity_percent",
                "wind_speed_kmh": "$wind_speed_kmh",
                "heat_index": "$heat_index"
            }
        },
        "min_date": {"$min": "$date_obj"},
        "max_date": {"$max": "$date_obj"},
        "record_count": {"$sum": 1}
    }},
    {"$match": {"record_count": {"$gte": 5}}},
    {"$addFields": {
        "time_span_days": {
            "$dateDiff": {
                "startDate": "$min_date",
                "endDate": "$max_date",
                "unit": "day"
            }
        },
        "data_metrics": {
            "$reduce": {
                "input": "$date_range",
                "initialValue": {
                    "temp_sum": 0,
                    "temp_squared_sum": 0,
                    "heat_index_sum": 0,
                    "precip_sum": 0,
                    "days_with_precip": 0,
                    "days_with_high_temp": 0,
                    "max_temp": {"$arrayElemAt": ["$date_range.temperature_c", 0]},
                    "min_temp": {"$arrayElemAt": ["$date_range.temperature_c", 0]},
                    "max_heat_index": {"$arrayElemAt": ["$date_range.heat_index", 0]},
                    "max_precip": 0
                },
                "in": {
                    "temp_sum": {"$add": ["$$value.temp_sum", "$$this.temperature_c"]},
                    "temp_squared_sum": {"$add": ["$$value.temp_squared_sum", {"$pow": ["$$this.temperature_c", 2]}]},
                    "heat_index_sum": {"$add": ["$$value.heat_index_sum", "$$this.heat_index"]},
                    "precip_sum": {"$add": ["$$value.precip_sum", "$$this.precipitation_mm"]},
                    "days_with_precip": {"$add": ["$$value.days_with_precip", {"$cond": [{"$gt": ["$$this.precipitation_mm", 0]}, 1, 0]}]},
                    "days_with_high_temp": {"$add": ["$$value.days_with_high_temp", {"$cond": [{"$gt": ["$$this.temperature_c", 25]}, 1, 0]}]},
                    "max_temp": {"$max": ["$$value.max_temp", "$$this.temperature_c"]},
                    "min_temp": {"$min": ["$$value.min_temp", "$$this.temperature_c"]},
                    "max_heat_index": {"$max": ["$$value.max_heat_index", "$$this.heat_index"]},
                    "max_precip": {"$max": ["$$value.max_precip", "$$this.precipitation_mm"]}
                }
            }
        }
    }},
    {"$project": {
        "_id": 0,
        "location": "$_id",
        "record_count": 1,
        "time_span": {
            "first_date": {"$dateToString": {"format": "%Y-%m-%d", "date": "$min_date"}},
            "last_date": {"$dateToString": {"format": "%Y-%m-%d", "date": "$max_date"}},
            "days": "$time_span_days"
        },
        "temperature_stats": {
            "average_c": {"$round": [{"$divide": ["$data_metrics.temp_sum", "$record_count"]}, 1]},
            "variance": {"$round": [
                {"$subtract": [
                    {"$divide": ["$data_metrics.temp_squared_sum", "$record_count"]},
                    {"$pow": [{"$divide": ["$data_metrics.temp_sum", "$record_count"]}, 2]}
                ]},
                2
            ]},
            "temperature_range": {
                "max_c": {"$round": ["$data_metrics.max_temp", 1]},
                "min_c": {"$round": ["$data_metrics.min_temp", 1]},
                "span_c": {"$round": [{"$subtract": ["$data_metrics.max_temp", "$data_metrics.min_temp"]}, 1]}
            },
            "days_with_high_temp": "$data_metrics.days_with_high_temp",
            "percent_days_high_temp": {"$round": [{"$multiply": [{"$divide": ["$data_metrics.days_with_high_temp", "$record_count"]}, 100]}, 1]}
        },
        "precipitation_stats": {
            "total_mm": {"$round": ["$data_metrics.precip_sum", 1]},
            "days_with_precipitation": "$data_metrics.days_with_precip",
            "percent_days_with_precip": {"$round": [{"$multiply": [{"$divide": ["$data_metrics.days_with_precip", "$record_count"]}, 100]}, 1]},
            "max_single_day_mm": {"$round": ["$data_metrics.max_precip", 1]},
            "average_on_rainy_days_mm": {
                "$cond": [
                    {"$eq": ["$data_metrics.days_with_precip", 0]},
                    0,
                    {"$round": [{"$divide": ["$data_metrics.precip_sum", "$data_metrics.days_with_precip"]}, 1]}
                ]
            }
        },
        "comfort_metrics": {
            "avg_heat_index": {"$round": [{"$divide": ["$data_metrics.heat_index_sum", "$record_count"]}, 1]},
            "max_heat_index": {"$round": ["$data_metrics.max_heat_index", 1]},
            "heat_stress_level": {
                "$switch": {
                    "branches": [
                        {"case": {"$gt": ["$data_metrics.max_heat_index", 40]}, "then": "Extreme Danger"},
                        {"case": {"$gt": ["$data_metrics.max_heat_index", 32]}, "then": "Danger"},
                        {"case": {"$gt": ["$data_metrics.max_heat_index", 27]}, "then": "Caution"},
                    ],
                    "default": "Normal"
                }
            }
        },
        "recent_samples": {"$slice": ["$date_range", -3]}
    }},
    {"$sort": {"temperature_stats.average_c": -1}}
]
execute_query("25", "globalClimate", q25_pipeline)

# QUERY 26: Complex network analysis with $graphLookup and recursive relationships
q26_pipeline = [
    {"$match": {"wind_speed_kmh": {"$gt": 40}}},
    {"$addFields": {
        "date_obj": {"$dateFromString": {"dateString": "$date", "format": "%Y-%m-%d"}}
    }},
    {"$sort": {"date_obj": 1}},
    {"$graphLookup": {
        "from": "usWeatherEvents",
        "startWith": "$location",
        "connectFromField": "location",
        "connectToField": "location",
        "as": "location_network",
        "maxDepth": 0,
        "restrictSearchWithMatch": {
            "wind_speed_kmh": {"$gt": 40}
        }
    }},
    {"$addFields": {
        "location_network": {
            "$filter": {
                "input": "$location_network",
                "as": "event",
                "cond": {"$ne": ["$$event.date", "$date"]}
            }
        }
    }},
    {"$graphLookup": {
        "from": "globalClimate",
        "startWith": "$date",
        "connectFromField": "date",
        "connectToField": "date",
        "as": "date_network",
        "maxDepth": 0,
        "restrictSearchWithMatch": {
            "wind_speed_kmh": {"$gt": 40}
        }
    }},
    {"$addFields": {
        "date_network": {
            "$filter": {
                "input": "$date_network",
                "as": "event",
                "cond": {"$ne": ["$$event.location", "$location"]}
            }
        }
    }},
    {"$project": {
        "_id": 0,
        "station_id": 1,
        "date": 1,
        "location": 1,
        "wind_speed_kmh": 1,
        "event_type": 1,
        "local_network": {
            "other_high_wind_events_in_location": {"$size": "$location_network"},
            "sample_events": {"$slice": ["$location_network", 3]}
        },
        "temporal_network": {
            "other_high_wind_events_on_same_date": {"$size": "$date_network"},
            "locations_affected": {"$setUnion": ["$date_network.location"]},
            "sample_events": {"$slice": ["$date_network", 3]}
        }
    }},
    {"$match": {
        "$or": [
            {"local_network.other_high_wind_events_in_location": {"$gt": 0}},
            {"temporal_network.other_high_wind_events_on_same_date": {"$gt": 0}}
        ]
    }},
    {"$addFields": {
        "wind_pattern_classification": {
            "$switch": {
                "branches": [
                    {
                        "case": {
                            "$and": [
                                {"$gt": ["$local_network.other_high_wind_events_in_location", 2]},
                                {"$gt": ["$temporal_network.other_high_wind_events_on_same_date", 2]}
                            ]
                        },
                        "then": "Strong Regional Wind Pattern"
                    },
                    {
                        "case": {"$gt": ["$local_network.other_high_wind_events_in_location", 2]},
                        "then": "Persistent Local Wind Pattern"
                    },
                    {
                        "case": {"$gt": ["$temporal_network.other_high_wind_events_on_same_date", 2]},
                        "then": "Widespread Wind Event"
                    }
                ],
                "default": "Isolated Wind Event"
            }
        },
        "temporal_network.location_count": {"$size": {"$ifNull": ["$temporal_network.locations_affected", []]}}
    }},
    {"$sort": {"wind_speed_kmh": -1}}
]
execute_query("26", "usWeatherEvents", q26_pipeline)

print("\nQueries 23-26 in part 4a executed successfully.") 