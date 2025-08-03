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

# QUERY 1: Average temperature and precipitation by location and event type with sorting
# Uses: $group, $sort, $project
q1_pipeline = [
    {"$match": {"event_type": {"$exists": True}}},
    {"$group": {
        "_id": {
            "location": "$location",
            "event_type": "$event_type"
        },
        "avg_temp": {"$avg": "$temperature_c"},
        "avg_precip": {"$avg": "$precipitation_mm"},
        "count": {"$sum": 1}
    }},
    {"$match": {"count": {"$gt": 1}}},
    {"$sort": {"avg_temp": -1}},
    {"$project": {
        "_id": 0,
        "location": "$_id.location",
        "event_type": "$_id.event_type",
        "avg_temperature_c": {"$round": ["$avg_temp", 1]},
        "avg_precipitation_mm": {"$round": ["$avg_precip", 1]},
        "event_count": "$count"
    }}
]
execute_query("1", "globalClimate", q1_pipeline)

# QUERY 2: Grouping weather events by season with $addFields and date manipulation
# Uses: $addFields, $group, $project
q2_pipeline = [
    {"$addFields": {
        "date_obj": {"$dateFromString": {"dateString": "$date", "format": "%Y-%m-%d"}},
    }},
    {"$addFields": {
        "month": {"$month": "$date_obj"},
        "year": {"$year": "$date_obj"},
        "season": {
            "$switch": {
                "branches": [
                    {"case": {"$and": [{"$gte": [{"$month": "$date_obj"}, 3]}, {"$lt": [{"$month": "$date_obj"}, 6]}]}, "then": "Spring"},
                    {"case": {"$and": [{"$gte": [{"$month": "$date_obj"}, 6]}, {"$lt": [{"$month": "$date_obj"}, 9]}]}, "then": "Summer"},
                    {"case": {"$and": [{"$gte": [{"$month": "$date_obj"}, 9]}, {"$lt": [{"$month": "$date_obj"}, 12]}]}, "then": "Fall"},
                ],
                "default": "Winter"
            }
        }
    }},
    {"$group": {
        "_id": {
            "season": "$season",
            "event_type": "$event_type"
        },
        "avg_temp": {"$avg": "$temperature_c"},
        "avg_humidity": {"$avg": "$humidity_percent"},
        "count": {"$sum": 1}
    }},
    {"$sort": {"_id.season": 1, "count": -1}},
    {"$project": {
        "_id": 0,
        "season": "$_id.season",
        "event_type": "$_id.event_type",
        "avg_temperature_c": {"$round": ["$avg_temp", 1]},
        "avg_humidity_percent": {"$round": ["$avg_humidity", 1]},
        "event_count": "$count"
    }}
]
execute_query("2", "globalClimate", q2_pipeline)

# QUERY 3: Find extreme weather events with statistical data
# Uses: $match, $group, $project, $sort
q3_pipeline = [
    {"$match": {"$or": [
        {"temperature_c": {"$gt": 25}},
        {"temperature_c": {"$lt": -5}},
        {"wind_speed_kmh": {"$gt": 50}},
        {"precipitation_mm": {"$gt": 80}}
    ]}},
    {"$group": {
        "_id": "$location",
        "extreme_high_temp_events": {"$sum": {"$cond": [{"$gt": ["$temperature_c", 25]}, 1, 0]}},
        "extreme_low_temp_events": {"$sum": {"$cond": [{"$lt": ["$temperature_c", -5]}, 1, 0]}},
        "high_wind_events": {"$sum": {"$cond": [{"$gt": ["$wind_speed_kmh", 50]}, 1, 0]}},
        "heavy_rain_events": {"$sum": {"$cond": [{"$gt": ["$precipitation_mm", 80]}, 1, 0]}},
        "max_temp": {"$max": "$temperature_c"},
        "min_temp": {"$min": "$temperature_c"},
        "max_wind": {"$max": "$wind_speed_kmh"},
        "max_precip": {"$max": "$precipitation_mm"}
    }},
    {"$addFields": {
        "total_extreme_events": {
            "$add": ["$extreme_high_temp_events", "$extreme_low_temp_events", 
                    "$high_wind_events", "$heavy_rain_events"]
        }
    }},
    {"$sort": {"total_extreme_events": -1}},
    {"$project": {
        "_id": 0,
        "location": "$_id",
        "extreme_high_temp_events": 1,
        "extreme_low_temp_events": 1,
        "high_wind_events": 1,
        "heavy_rain_events": 1,
        "total_extreme_events": 1,
        "temperature_range": {
            "max_c": {"$round": ["$max_temp", 1]},
            "min_c": {"$round": ["$min_temp", 1]}
        },
        "max_wind_speed_kmh": {"$round": ["$max_wind", 1]},
        "max_precipitation_mm": {"$round": ["$max_precip", 1]}
    }}
]
execute_query("3", "usWeatherEvents", q3_pipeline)

# QUERY 4: Compare event types across collections with $unionWith and $facet
# Uses: $unionWith, $facet, $group, $sort, $project
q4_pipeline = [
    {"$match": {"event_type": {"$exists": true}}},
    {"$unionWith": {
        "coll": "usWeatherEvents",
        "pipeline": [{"$match": {"event_type": {"$exists": true}}}]
    }},
    {"$facet": {
        "by_event_type": [
            {"$group": {
                "_id": "$event_type",
                "count": {"$sum": 1},
                "avg_temp": {"$avg": "$temperature_c"},
                "avg_precip": {"$avg": "$precipitation_mm"}
            }},
            {"$sort": {"count": -1}},
            {"$project": {
                "_id": 0,
                "event_type": "$_id",
                "count": 1,
                "avg_temperature_c": {"$round": ["$avg_temp", 1]},
                "avg_precipitation_mm": {"$round": ["$avg_precip", 1]}
            }}
        ],
        "by_location": [
            {"$group": {
                "_id": "$location",
                "unique_event_types": {"$addToSet": "$event_type"},
                "count": {"$sum": 1}
            }},
            {"$project": {
                "_id": 0,
                "location": "$_id",
                "unique_event_types": 1,
                "unique_event_type_count": {"$size": "$unique_event_types"},
                "total_events": "$count"
            }},
            {"$sort": {"unique_event_type_count": -1}}
        ]
    }}
]
execute_query("4", "globalClimate", q4_pipeline)

# QUERY 5: Correlate weather metrics with buckets and lookup
# Uses: $bucket, $lookup, $project, $sort
q5_pipeline = [
    {"$bucket": {
        "groupBy": "$temperature_c",
        "boundaries": [-10, 0, 10, 20, 30],
        "default": "Other",
        "output": {
            "count": {"$sum": 1},
            "stations": {"$addToSet": "$station_id"},
            "avg_humidity": {"$avg": "$humidity_percent"},
            "avg_precipitation": {"$avg": "$precipitation_mm"},
            "avg_wind_speed": {"$avg": "$wind_speed_kmh"}
        }
    }},
    {"$lookup": {
        "from": "usWeatherEvents",
        "let": {"temp_range_low": "$_id", "temp_range_high": {"$add": ["$_id", 10]}},
        "pipeline": [
            {"$match": {
                "$expr": {
                    "$and": [
                        {"$gte": ["$temperature_c", "$$temp_range_low"]},
                        {"$lt": ["$temperature_c", "$$temp_range_high"]}
                    ]
                }
            }},
            {"$group": {
                "_id": null,
                "event_types": {"$addToSet": "$event_type"},
                "count": {"$sum": 1}
            }}
        ],
        "as": "corresponding_events"
    }},
    {"$project": {
        "_id": 0,
        "temperature_range": {
            "$cond": [
                {"$eq": ["$_id", "Other"]},
                "Other",
                {"$concat": [
                    {"$toString": "$_id"}, 
                    "°C to ", 
                    {"$toString": {"$add": ["$_id", 10]}},
                    "°C"
                ]}
            ]
        },
        "record_count": "$count",
        "station_count": {"$size": "$stations"},
        "avg_humidity_percent": {"$round": ["$avg_humidity", 1]},
        "avg_precipitation_mm": {"$round": ["$avg_precipitation", 1]},
        "avg_wind_speed_kmh": {"$round": ["$avg_wind_speed", 1]},
        "corresponding_events": {
            "$cond": [
                {"$eq": [{"$size": "$corresponding_events"}, 0]},
                [],
                "$corresponding_events.event_types"
            ]
        }
    }},
    {"$sort": {"temperature_range": 1}}
]
execute_query("5", "weatherHistory", q5_pipeline)

# QUERY 6: Using $unwind and $group to analyze event types by year and location
# Uses: $addFields, $unwind, $group, $sort, $project
q6_pipeline = [
    {"$match": {"event_type": {"$exists": true}}},
    {"$addFields": {
        "date_obj": {"$dateFromString": {"dateString": "$date", "format": "%Y-%m-%d"}},
        "year": {"$substr": ["$date", 0, 4]}
    }},
    {"$group": {
        "_id": {
            "year": "$year",
            "location": "$location"
        },
        "event_types": {"$addToSet": "$event_type"},
        "avg_temp": {"$avg": "$temperature_c"},
        "count": {"$sum": 1}
    }},
    {"$unwind": "$event_types"},
    {"$group": {
        "_id": {
            "year": "$_id.year",
            "event_type": "$event_types"
        },
        "locations": {"$addToSet": "$_id.location"},
        "total_events": {"$sum": "$count"},
        "avg_temp_by_location": {"$avg": "$avg_temp"}
    }},
    {"$sort": {"_id.year": -1, "total_events": -1}},
    {"$project": {
        "_id": 0,
        "year": "$_id.year",
        "event_type": "$_id.event_type",
        "location_count": {"$size": "$locations"},
        "locations": 1,
        "total_events": 1,
        "avg_temperature_c": {"$round": ["$avg_temp_by_location", 1]}
    }}
]
execute_query("6", "globalClimate", q6_pipeline)

# QUERY 7: Analysis of weather patterns with $bucket and complex conditions
# Uses: $match, $addFields, $bucket, $sort, $project with complex expressions
q7_pipeline = [
    {"$addFields": {
        "temp_humidity_ratio": {
            "$cond": [
                {"$eq": ["$humidity_percent", 0]},
                null,
                {"$divide": ["$temperature_c", "$humidity_percent"]}
            ]
        },
        "is_high_wind": {"$gte": ["$wind_speed_kmh", 40]},
        "is_rainy": {"$gte": ["$precipitation_mm", 50]},
        "weather_condition": {
            "$switch": {
                "branches": [
                    {"case": {"$and": [{"$gte": ["$temperature_c", 25]}, {"$lt": ["$humidity_percent", 30]}]}, "then": "Hot and Dry"},
                    {"case": {"$and": [{"$gte": ["$temperature_c", 25]}, {"$gte": ["$humidity_percent", 70]}]}, "then": "Hot and Humid"},
                    {"case": {"$and": [{"$lt": ["$temperature_c", 0]}, {"$gte": ["$precipitation_mm", 20]}]}, "then": "Freezing Precipitation"},
                    {"case": {"$and": [{"$gte": ["$wind_speed_kmh", 50]}, {"$gte": ["$precipitation_mm", 50]}]}, "then": "Stormy"},
                ],
                "default": "Normal"
            }
        }
    }},
    {"$bucket": {
        "groupBy": "$weather_condition",
        "boundaries": ["Freezing Precipitation", "Hot and Dry", "Hot and Humid", "Normal", "Stormy"],
        "default": "Other",
        "output": {
            "count": {"$sum": 1},
            "locations": {"$addToSet": "$location"},
            "avg_temp": {"$avg": "$temperature_c"},
            "avg_humidity": {"$avg": "$humidity_percent"},
            "avg_wind": {"$avg": "$wind_speed_kmh"},
            "avg_precip": {"$avg": "$precipitation_mm"},
            "station_samples": {"$push": {"station": "$station_id", "date": "$date"}}
        }
    }},
    {"$project": {
        "_id": 0,
        "weather_condition": "$_id",
        "event_count": "$count",
        "unique_locations": {"$size": "$locations"},
        "locations": 1,
        "avg_temperature_c": {"$round": ["$avg_temp", 1]},
        "avg_humidity_percent": {"$round": ["$avg_humidity", 1]},
        "avg_wind_speed_kmh": {"$round": ["$avg_wind", 1]},
        "avg_precipitation_mm": {"$round": ["$avg_precip", 1]},
        "sample_stations": {"$slice": ["$station_samples", 3]}
    }},
    {"$sort": {"event_count": -1}}
]
execute_query("7", "globalClimate", q7_pipeline)

# QUERY 8: Complex analysis of temperature anomalies with multiple aggregation stages
# Uses: $lookup, $group, $project, $match, $sort with complex conditions
q8_pipeline = [
    {"$group": {
        "_id": "$location",
        "avg_temp": {"$avg": "$temperature_c"},
        "std_dev_temp": {"$stdDevPop": "$temperature_c"},
        "record_count": {"$sum": 1},
        "data_points": {"$push": {
            "station_id": "$station_id", 
            "date": "$date", 
            "temperature_c": "$temperature_c", 
            "humidity_percent": "$humidity_percent",
            "wind_speed_kmh": "$wind_speed_kmh"
        }}
    }},
    {"$match": {"record_count": {"$gte": 3}}},  # Ensure we have enough data to analyze
    {"$project": {
        "_id": 0,
        "location": "$_id",
        "avg_temperature_c": {"$round": ["$avg_temp", 1]},
        "std_dev_temperature": {"$round": ["$std_dev_temp", 2]},
        "temperature_threshold_high": {"$round": [{"$add": ["$avg_temp", {"$multiply": ["$std_dev_temp", 1.5]}]}, 1]},
        "temperature_threshold_low": {"$round": [{"$subtract": ["$avg_temp", {"$multiply": ["$std_dev_temp", 1.5]}]}, 1]},
        "record_count": 1,
        "data_points": 1
    }},
    {"$addFields": {
        "anomalies": {
            "$filter": {
                "input": "$data_points",
                "as": "point",
                "cond": {
                    "$or": [
                        {"$gt": ["$$point.temperature_c", "$temperature_threshold_high"]},
                        {"$lt": ["$$point.temperature_c", "$temperature_threshold_low"]}
                    ]
                }
            }
        }
    }},
    {"$project": {
        "location": 1,
        "avg_temperature_c": 1,
        "std_dev_temperature": 1,
        "temperature_threshold_high": 1,
        "temperature_threshold_low": 1,
        "record_count": 1,
        "anomaly_count": {"$size": "$anomalies"},
        "anomaly_percentage": {
            "$round": [
                {"$multiply": [
                    {"$divide": [{"$size": "$anomalies"}, "$record_count"]}, 
                    100
                ]}, 
                1
            ]
        },
        "temperature_anomalies": "$anomalies"
    }},
    {"$match": {"anomaly_count": {"$gt": 0}}},
    {"$sort": {"anomaly_percentage": -1}}
]
execute_query("8", "weatherHistory", q8_pipeline)

print("\nAll queries in part 1 executed successfully.") 