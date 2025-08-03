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

# QUERY 17: Analyzing seasonal patterns with $addFields and $group
q17_pipeline = [
    {"$addFields": {
        "date_obj": {"$dateFromString": {"dateString": "$date", "format": "%Y-%m-%d"}},
        "year": {"$year": {"$dateFromString": {"dateString": "$date", "format": "%Y-%m-%d"}}},
        "month": {"$month": {"$dateFromString": {"dateString": "$date", "format": "%Y-%m-%d"}}},
        "season": {
            "$switch": {
                "branches": [
                    {"case": {"$in": ["$month", [12, 1, 2]]}, "then": "Winter"},
                    {"case": {"$in": ["$month", [3, 4, 5]]}, "then": "Spring"},
                    {"case": {"$in": ["$month", [6, 7, 8]]}, "then": "Summer"},
                    {"case": {"$in": ["$month", [9, 10, 11]]}, "then": "Fall"}
                ],
                "default": "Unknown"
            }
        }
    }},
    {"$group": {
        "_id": {
            "location": "$location",
            "season": "$season"
        },
        "avg_temp": {"$avg": "$temperature_c"},
        "avg_humidity": {"$avg": "$humidity_percent"},
        "avg_wind": {"$avg": "$wind_speed_kmh"},
        "avg_precip": {"$avg": "$precipitation_mm"},
        "record_count": {"$sum": 1}
    }},
    {"$match": {"record_count": {"$gte": 2}}},
    {"$sort": {"_id.location": 1, "_id.season": 1}},
    {"$project": {
        "_id": 0,
        "location": "$_id.location",
        "season": "$_id.season",
        "avg_temperature_c": {"$round": ["$avg_temp", 1]},
        "avg_humidity_percent": {"$round": ["$avg_humidity", 1]},
        "avg_wind_speed_kmh": {"$round": ["$avg_wind", 1]},
        "avg_precipitation_mm": {"$round": ["$avg_precip", 1]},
        "record_count": 1
    }}
]
execute_query("17", "globalClimate", q17_pipeline)

# QUERY 18: Geographic clustering with $geoNear and $bucket
q18_pipeline = [
    {"$addFields": {
        "geo_point": {
            "$switch": {
                "branches": [
                    {"case": {"$eq": ["$location", "Prague"]}, "then": {"lat": 50.0755, "lon": 14.4378}},
                    {"case": {"$eq": ["$location", "Brno"]}, "then": {"lat": 49.1951, "lon": 16.6068}},
                    {"case": {"$eq": ["$location", "Ostrava"]}, "then": {"lat": 49.8209, "lon": 18.2625}},
                    {"case": {"$eq": ["$location", "Plzen"]}, "then": {"lat": 49.7384, "lon": 13.3736}},
                    {"case": {"$eq": ["$location", "Liberec"]}, "then": {"lat": 50.7671, "lon": 15.0571}},
                    {"case": {"$eq": ["$location", "Olomouc"]}, "then": {"lat": 49.5894, "lon": 17.2581}},
                    {"case": {"$eq": ["$location", "Hradec Kralove"]}, "then": {"lat": 50.2099, "lon": 15.8325}}
                ],
                "default": {"lat": 0, "lon": 0}
            }
        }
    }},
    {"$match": {"geo_point.lat": {"$ne": 0}}},
    {"$addFields": {
        "distance_from_prague": {
            "$function": {
                "body": `function(lat1, lon1) {
                    const lat2 = 50.0755;
                    const lon2 = 14.4378;
                    const R = 6371; // Earth's radius in km
                    const dLat = (lat2 - lat1) * Math.PI / 180;
                    const dLon = (lon2 - lon1) * Math.PI / 180;
                    const a = Math.sin(dLat/2) * Math.sin(dLat/2) +
                        Math.cos(lat1 * Math.PI / 180) * Math.cos(lat2 * Math.PI / 180) * 
                        Math.sin(dLon/2) * Math.sin(dLon/2);
                    const c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1-a));
                    return R * c;
                }`,
                "args": ["$geo_point.lat", "$geo_point.lon"],
                "lang": "js"
            }
        }
    }},
    {"$bucket": {
        "groupBy": "$distance_from_prague",
        "boundaries": [0, 50, 100, 150, 200],
        "default": "200+",
        "output": {
            "locations": {"$addToSet": "$location"},
            "avg_temp": {"$avg": "$temperature_c"},
            "avg_humidity": {"$avg": "$humidity_percent"},
            "avg_wind": {"$avg": "$wind_speed_kmh"},
            "avg_precipitation": {"$avg": "$precipitation_mm"},
            "count": {"$sum": 1}
        }
    }},
    {"$project": {
        "_id": 0,
        "distance_range_km": {
            "$cond": [
                {"$eq": ["$_id", "200+"]},
                "Over 200 km",
                {"$concat": [
                    {"$toString": "$_id"}, 
                    " to ", 
                    {"$toString": {"$cond": [{"$eq": ["$_id", 150]}, 200, {"$add": ["$_id", 50]}]}},
                    " km"
                ]}
            ]
        },
        "locations": 1,
        "location_count": {"$size": "$locations"},
        "avg_temperature_c": {"$round": ["$avg_temp", 1]},
        "avg_humidity_percent": {"$round": ["$avg_humidity", 1]},
        "avg_wind_speed_kmh": {"$round": ["$avg_wind", 1]},
        "avg_precipitation_mm": {"$round": ["$avg_precip", 1]},
        "record_count": "$count"
    }}
]
execute_query("18", "weatherHistory", q18_pipeline)

# QUERY 19: Complex event correlation using $lookup and $facet
q19_pipeline = [
    {"$match": {"event_type": "Storm"}},
    {"$addFields": {
        "date_obj": {"$dateFromString": {"dateString": "$date", "format": "%Y-%m-%d"}}
    }},
    {"$facet": {
        "by_location": [
            {"$group": {
                "_id": "$location",
                "storm_count": {"$sum": 1},
                "avg_temperature": {"$avg": "$temperature_c"},
                "avg_wind_speed": {"$avg": "$wind_speed_kmh"},
                "avg_precipitation": {"$avg": "$precipitation_mm"}
            }},
            {"$sort": {"storm_count": -1}}
        ],
        "by_year": [
            {"$group": {
                "_id": {"$year": "$date_obj"},
                "storm_count": {"$sum": 1},
                "locations": {"$addToSet": "$location"}
            }},
            {"$sort": {"_id": -1}}
        ],
        "correlation_with_heatwaves": [
            {"$lookup": {
                "from": "globalClimate",
                "let": {"storm_location": "$location", "storm_date": "$date"},
                "pipeline": [
                    {"$match": {
                        "$expr": {
                            "$and": [
                                {"$eq": ["$location", "$$storm_location"]},
                                {"$eq": ["$event_type", "Heatwave"]},
                                {"$lte": [
                                    {"$abs": {
                                        "$subtract": [
                                            {"$dateFromString": {"dateString": "$date", "format": "%Y-%m-%d"}},
                                            {"$dateFromString": {"dateString": "$$storm_date", "format": "%Y-%m-%d"}}
                                        ]
                                    }},
                                    1000 * 60 * 60 * 24 * 30  // 30 days in milliseconds
                                ]}
                            ]
                        }
                    }},
                    {"$project": {
                        "_id": 0,
                        "date": 1,
                        "temperature_c": 1,
                        "days_between": {
                            "$divide": [
                                {"$subtract": [
                                    {"$dateFromString": {"dateString": "$date", "format": "%Y-%m-%d"}},
                                    {"$dateFromString": {"dateString": "$$storm_date", "format": "%Y-%m-%d"}}
                                ]},
                                1000 * 60 * 60 * 24  // Convert ms to days
                            ]
                        }
                    }}
                ],
                "as": "related_heatwaves"
            }},
            {"$match": {"related_heatwaves": {"$ne": []}}},
            {"$project": {
                "_id": 0,
                "location": 1,
                "storm_date": "$date",
                "storm_temperature": "$temperature_c",
                "related_heatwaves": 1
            }}
        ]
    }},
    {"$project": {
        "storm_by_location": "$by_location",
        "storm_by_year": "$by_year",
        "storm_heatwave_correlations": {
            "$slice": ["$correlation_with_heatwaves", 5]
        },
        "correlation_summary": {
            "total_correlated_events": {"$size": "$correlation_with_heatwaves"},
            "locations_with_correlation": {"$size": {"$setUnion": "$correlation_with_heatwaves.location"}}
        }
    }}
]
execute_query("19", "usWeatherEvents", q19_pipeline)

# QUERY 20: Advanced time-series analysis with $setDifference and $reduce
q20_pipeline = [
    {"$match": {"location": {"$in": ["Prague", "Brno"]}}},
    {"$addFields": {
        "date_obj": {"$dateFromString": {"dateString": "$date", "format": "%Y-%m-%d"}},
        "year": {"$year": {"$dateFromString": {"dateString": "$date", "format": "%Y-%m-%d"}}},
        "month": {"$month": {"$dateFromString": {"dateString": "$date", "format": "%Y-%m-%d"}}}
    }},
    {"$sort": {"location": 1, "date_obj": 1}},
    {"$group": {
        "_id": {
            "location": "$location",
            "year": "$year",
            "month": "$month"
        },
        "avg_temp": {"$avg": "$temperature_c"},
        "min_temp": {"$min": "$temperature_c"},
        "max_temp": {"$max": "$temperature_c"},
        "total_precipitation": {"$sum": "$precipitation_mm"},
        "records": {"$push": {
            "date": "$date",
            "temperature_c": "$temperature_c",
            "precipitation_mm": "$precipitation_mm",
            "station_id": "$station_id"
        }}
    }},
    {"$sort": {"_id.location": 1, "_id.year": 1, "_id.month": 1}},
    {"$group": {
        "_id": "$_id.location",
        "monthly_data": {"$push": {
            "year": "$_id.year",
            "month": "$_id.month",
            "avg_temperature_c": {"$round": ["$avg_temp", 1]},
            "min_temperature_c": {"$round": ["$min_temp", 1]},
            "max_temperature_c": {"$round": ["$max_temp", 1]},
            "total_precipitation_mm": {"$round": ["$total_precipitation", 1]},
            "record_count": {"$size": "$records"}
        }}
    }},
    {"$project": {
        "_id": 0,
        "location": "$_id",
        "monthly_data": 1,
        "statistics": {
            "$reduce": {
                "input": "$monthly_data",
                "initialValue": {
                    "total_months": 0,
                    "sum_avg_temp": 0,
                    "months_with_high_temp": 0,
                    "months_with_low_temp": 0,
                    "months_with_high_precip": 0
                },
                "in": {
                    "total_months": {"$add": ["$$value.total_months", 1]},
                    "sum_avg_temp": {"$add": ["$$value.sum_avg_temp", "$$this.avg_temperature_c"]},
                    "months_with_high_temp": {"$add": ["$$value.months_with_high_temp", {"$cond": [{"$gt": ["$$this.max_temperature_c", 25]}, 1, 0]}]},
                    "months_with_low_temp": {"$add": ["$$value.months_with_low_temp", {"$cond": [{"$lt": ["$$this.min_temperature_c", 0]}, 1, 0]}]},
                    "months_with_high_precip": {"$add": ["$$value.months_with_high_precip", {"$cond": [{"$gt": ["$$this.total_precipitation_mm", 100]}, 1, 0]}]}
                }
            }
        }
    }},
    {"$addFields": {
        "statistics.avg_temperature_overall": {"$round": [{"$divide": ["$statistics.sum_avg_temp", "$statistics.total_months"]}, 1]},
        "statistics.percent_months_high_temp": {"$round": [{"$multiply": [{"$divide": ["$statistics.months_with_high_temp", "$statistics.total_months"]}, 100]}, 1]},
        "statistics.percent_months_low_temp": {"$round": [{"$multiply": [{"$divide": ["$statistics.months_with_low_temp", "$statistics.total_months"]}, 100]}, 1]},
        "statistics.percent_months_high_precip": {"$round": [{"$multiply": [{"$divide": ["$statistics.months_with_high_precip", "$statistics.total_months"]}, 100]}, 1]}
    }},
    {"$project": {
        "location": 1,
        "statistics": {
            "total_months_analyzed": "$statistics.total_months",
            "average_temperature_c": "$statistics.avg_temperature_overall",
            "months_with_high_temp": "$statistics.months_with_high_temp",
            "months_with_low_temp": "$statistics.months_with_low_temp",
            "months_with_high_precipitation": "$statistics.months_with_high_precip",
            "percent_months_high_temp": "$statistics.percent_months_high_temp",
            "percent_months_low_temp": "$statistics.percent_months_low_temp",
            "percent_months_high_precip": "$statistics.percent_months_high_precip"
        },
        "monthly_data": {"$slice": ["$monthly_data", 5]}  // Show only first 5 months
    }}
]
execute_query("20", "weatherHistory", q20_pipeline)

# QUERY 21: Multi-dataset analysis with complex joins using $graphLookup
q21_pipeline = [
    {"$match": {"event_type": {"$in": ["Storm", "Wind", "Rain"]}}},
    {"$addFields": {
        "date_obj": {"$dateFromString": {"dateString": "$date", "format": "%Y-%m-%d"}}
    }},
    {"$sort": {"date_obj": 1}},
    {"$graphLookup": {
        "from": "usWeatherEvents",
        "startWith": "$location",
        "connectFromField": "location",
        "connectToField": "location",
        "as": "connected_events",
        "maxDepth": 0,
        "restrictSearchWithMatch": {
            "$and": [
                {"event_type": {"$in": ["Storm", "Wind", "Rain"]}},
                {"date": {"$ne": "$date"}}
            ]
        }
    }},
    {"$addFields": {
        "connected_events": {
            "$filter": {
                "input": "$connected_events",
                "as": "event",
                "cond": {
                    "$and": [
                        {"$ne": ["$$event.date", "$date"]},
                        {"$lte": [
                            {"$abs": {
                                "$subtract": [
                                    {"$dateFromString": {"dateString": "$$event.date", "format": "%Y-%m-%d"}},
                                    "$date_obj"
                                ]
                            }},
                            1000 * 60 * 60 * 24 * 30  // 30 days in milliseconds
                        ]}
                    ]
                }
            }
        }
    }},
    {"$addFields": {
        "event_chain": {
            "$map": {
                "input": "$connected_events",
                "as": "event",
                "in": {
                    "event_type": "$$event.event_type",
                    "date": "$$event.date",
                    "days_apart": {
                        "$ceil": {
                            "$divide": [
                                {"$abs": {
                                    "$subtract": [
                                        {"$dateFromString": {"dateString": "$$event.date", "format": "%Y-%m-%d"}},
                                        "$date_obj"
                                    ]
                                }},
                                1000 * 60 * 60 * 24  // Convert ms to days
                            ]
                        }
                    },
                    "temperature_difference": {"$subtract": ["$$event.temperature_c", "$temperature_c"]},
                    "wind_speed_difference": {"$subtract": ["$$event.wind_speed_kmh", "$wind_speed_kmh"]}
                }
            }
        }
    }},
    {"$match": {"event_chain": {"$ne": []}}},
    {"$addFields": {
        "chain_analysis": {
            "$reduce": {
                "input": "$event_chain",
                "initialValue": {
                    "total_events": 0,
                    "temp_diff_sum": 0,
                    "wind_diff_sum": 0,
                    "days_apart_sum": 0,
                    "event_types": []
                },
                "in": {
                    "total_events": {"$add": ["$$value.total_events", 1]},
                    "temp_diff_sum": {"$add": ["$$value.temp_diff_sum", "$$this.temperature_difference"]},
                    "wind_diff_sum": {"$add": ["$$value.wind_diff_sum", "$$this.wind_speed_difference"]},
                    "days_apart_sum": {"$add": ["$$value.days_apart_sum", "$$this.days_apart"]},
                    "event_types": {"$concatArrays": ["$$value.event_types", ["$$this.event_type"]]}
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
        "temperature_c": 1,
        "chain_summary": {
            "related_event_count": "$chain_analysis.total_events",
            "avg_temperature_change": {"$round": [{"$divide": ["$chain_analysis.temp_diff_sum", "$chain_analysis.total_events"]}, 1]},
            "avg_wind_speed_change": {"$round": [{"$divide": ["$chain_analysis.wind_diff_sum", "$chain_analysis.total_events"]}, 1]},
            "avg_days_between_events": {"$round": [{"$divide": ["$chain_analysis.days_apart_sum", "$chain_analysis.total_events"]}, 1]},
            "event_type_distribution": {"$reduce": {
                "input": "$chain_analysis.event_types",
                "initialValue": {"Storm": 0, "Wind": 0, "Rain": 0},
                "in": {
                    "Storm": {"$add": ["$$value.Storm", {"$cond": [{"$eq": ["$$this", "Storm"]}, 1, 0]}]},
                    "Wind": {"$add": ["$$value.Wind", {"$cond": [{"$eq": ["$$this", "Wind"]}, 1, 0]}]},
                    "Rain": {"$add": ["$$value.Rain", {"$cond": [{"$eq": ["$$this", "Rain"]}, 1, 0]}]}
                }
            }}
        },
        "related_events": {"$slice": ["$event_chain", 5]}
    }},
    {"$sort": {"chain_summary.related_event_count": -1}}
]
execute_query("21", "globalClimate", q21_pipeline)

# QUERY 22: Complex data transformation with $setUnion, $setIntersection, and $setDifference
q22_pipeline = [
    {"$match": {"event_type": {"$exists": true}}},
    {"$group": {
        "_id": "$location",
        "event_types": {"$addToSet": "$event_type"},
        "all_stations": {"$addToSet": "$station_id"},
        "avg_temp": {"$avg": "$temperature_c"},
        "record_count": {"$sum": 1}
    }},
    {"$lookup": {
        "from": "usWeatherEvents",
        "let": {"location": "$_id"},
        "pipeline": [
            {"$match": {
                "$expr": {"$eq": ["$location", "$$location"]}
            }},
            {"$group": {
                "_id": "$$location",
                "us_event_types": {"$addToSet": "$event_type"},
                "us_stations": {"$addToSet": "$station_id"},
                "avg_temp": {"$avg": "$temperature_c"},
                "record_count": {"$sum": 1}
            }}
        ],
        "as": "us_data"
    }},
    {"$addFields": {
        "us_data": {"$arrayElemAt": ["$us_data", 0]}
    }},
    {"$project": {
        "_id": 0,
        "location": "$_id",
        "global_climate_data": {
            "event_types": "$event_types",
            "station_count": {"$size": "$all_stations"},
            "record_count": "$record_count",
            "avg_temperature_c": {"$round": ["$avg_temp", 1]}
        },
        "us_events_data": {
            "event_types": "$us_data.us_event_types",
            "station_count": {"$size": {"$ifNull": ["$us_data.us_stations", []]}},
            "record_count": {"$ifNull": ["$us_data.record_count", 0]},
            "avg_temperature_c": {"$round": [{"$ifNull": ["$us_data.avg_temp", null]}, 1]}
        },
        "event_type_analysis": {
            "common_event_types": {
                "$cond": [
                    {"$eq": [{"$ifNull": ["$us_data.us_event_types", []]}, []]},
                    [],
                    {"$setIntersection": ["$event_types", "$us_data.us_event_types"]}
                ]
            },
            "unique_to_global": {
                "$cond": [
                    {"$eq": [{"$ifNull": ["$us_data.us_event_types", []]}, []]},
                    "$event_types",
                    {"$setDifference": ["$event_types", "$us_data.us_event_types"]}
                ]
            },
            "unique_to_us": {
                "$cond": [
                    {"$eq": [{"$ifNull": ["$us_data.us_event_types", []]}, []]},
                    [],
                    {"$setDifference": ["$us_data.us_event_types", "$event_types"]}
                ]
            },
            "all_event_types": {
                "$cond": [
                    {"$eq": [{"$ifNull": ["$us_data.us_event_types", []]}, []]},
                    "$event_types",
                    {"$setUnion": ["$event_types", "$us_data.us_event_types"]}
                ]
            }
        },
        "temperature_difference": {
            "$cond": [
                {"$eq": [{"$ifNull": ["$us_data.avg_temp", null]}, null]},
                null,
                {"$round": [{"$subtract": ["$avg_temp", "$us_data.avg_temp"]}, 1]}
            ]
        }
    }},
    {"$addFields": {
        "data_completeness": {
            "$cond": [
                {"$eq": [{"$ifNull": ["$us_data", null]}, null]},
                "Only globalClimate data available",
                {
                    "$cond": [
                        {"$gte": ["$global_climate_data.record_count", "$us_events_data.record_count"]},
                        "More data in globalClimate",
                        "More data in usWeatherEvents"
                    ]
                }
            ]
        },
        "event_type_analysis.common_event_count": {"$size": "$event_type_analysis.common_event_types"},
        "event_type_analysis.global_unique_count": {"$size": "$event_type_analysis.unique_to_global"},
        "event_type_analysis.us_unique_count": {"$size": "$event_type_analysis.unique_to_us"},
        "event_type_analysis.total_unique_event_types": {"$size": "$event_type_analysis.all_event_types"}
    }},
    {"$sort": {"event_type_analysis.total_unique_event_types": -1}}
]
execute_query("22", "globalClimate", q22_pipeline)

print("\nQueries 17-22 in part 3 executed successfully.") 