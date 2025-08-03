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

# QUERY 27: Advanced time-series forecasting with exponential smoothing
q27_pipeline = [
    {"$match": {"location": "Prague"}},
    {"$addFields": {
        "date_obj": {"$dateFromString": {"dateString": "$date", "format": "%Y-%m-%d"}},
        "year": {"$year": {"$dateFromString": {"dateString": "$date", "format": "%Y-%m-%d"}}},
        "month": {"$month": {"$dateFromString": {"dateString": "$date", "format": "%Y-%m-%d"}}}
    }},
    {"$sort": {"date_obj": 1}},
    {"$group": {
        "_id": {
            "year": "$year",
            "month": "$month"
        },
        "avg_temp": {"$avg": "$temperature_c"},
        "avg_precip": {"$avg": "$precipitation_mm"},
        "day_count": {"$sum": 1},
        "month_date": {"$min": "$date_obj"}
    }},
    {"$sort": {"month_date": 1}},
    {"$project": {
        "_id": 0,
        "year_month": {"$concat": [{"$toString": "$_id.year"}, "-", {"$toString": "$_id.month"}]},
        "month_date": 1,
        "avg_temperature_c": {"$round": ["$avg_temp", 1]},
        "avg_precipitation_mm": {"$round": ["$avg_precip", 1]},
        "day_count": 1
    }},
    {"$group": {
        "_id": null,
        "month_data": {"$push": "$$ROOT"},
        "month_count": {"$sum": 1}
    }},
    {"$unwind": {
        "path": "$month_data",
        "includeArrayIndex": "index"
    }},
    {"$sort": {"index": 1}},
    {"$project": {
        "_id": 0,
        "month_data": 1,
        "index": 1,
        "alpha": 0.3,  // Smoothing factor
        "total_months": "$month_count"
    }},
    {"$group": {
        "_id": null,
        "all_data": {"$push": "$$ROOT"},
        "total_months": {"$first": "$total_months"}
    }},
    {"$project": {
        "_id": 0,
        "timeseries_data": {
            "$map": {
                "input": {"$range": [0, "$total_months"]},
                "as": "i",
                "in": {
                    "$let": {
                        "vars": {
                            "current": {"$arrayElemAt": ["$all_data", "$$i"]},
                            "previous": {
                                "$cond": [
                                    {"$eq": ["$$i", 0]},
                                    null,
                                    {"$arrayElemAt": ["$all_data", {"$subtract": ["$$i", 1]}]}
                                ]
                            }
                        },
                        "in": {
                            "month": "$$current.month_data.year_month",
                            "actual_temp": "$$current.month_data.avg_temperature_c",
                            "actual_precip": "$$current.month_data.avg_precipitation_mm",
                            "smoothed_temp": {
                                "$cond": [
                                    {"$eq": ["$$i", 0]},
                                    "$$current.month_data.avg_temperature_c",
                                    {"$round": [
                                        {"$add": [
                                            {"$multiply": ["$$current.alpha", "$$current.month_data.avg_temperature_c"]},
                                            {"$multiply": [{"$subtract": [1, "$$current.alpha"]}, "$$previous.smoothed_temp"]}
                                        ]},
                                        1
                                    ]}
                                ]
                            },
                            "smoothed_precip": {
                                "$cond": [
                                    {"$eq": ["$$i", 0]},
                                    "$$current.month_data.avg_precipitation_mm",
                                    {"$round": [
                                        {"$add": [
                                            {"$multiply": ["$$current.alpha", "$$current.month_data.avg_precipitation_mm"]},
                                            {"$multiply": [{"$subtract": [1, "$$current.alpha"]}, "$$previous.smoothed_precip"]}
                                        ]},
                                        1
                                    ]}
                                ]
                            }
                        }
                    }
                }
            }
        }
    }},
    {"$unwind": "$timeseries_data"},
    {"$group": {
        "_id": null,
        "smoothed_data": {"$push": "$timeseries_data"},
        "smoothed_temps": {"$push": "$timeseries_data.smoothed_temp"},
        "actual_temps": {"$push": "$timeseries_data.actual_temp"},
        "last_smoothed_temp": {"$last": "$timeseries_data.smoothed_temp"},
        "last_smoothed_precip": {"$last": "$timeseries_data.smoothed_precip"}
    }},
    {"$project": {
        "_id": 0,
        "location": "Prague",
        "time_series_data": {"$slice": ["$smoothed_data", -12]},  // Last 12 months
        "trend_analysis": {
            "smoothing_alpha": 0.3,
            "temperature_trend": {
                "$cond": [
                    {"$gt": [
                        {"$subtract": [
                            {"$last": "$smoothed_temps"}, 
                            {"$arrayElemAt": ["$smoothed_temps", {"$subtract": [{"$size": "$smoothed_temps"}, 4]}]}
                        ]},
                        0
                    ]},
                    "Rising",
                    "Falling"
                ]
            },
            "forecast_next_month": {
                "temperature_c": {"$round": ["$last_smoothed_temp", 1]},
                "precipitation_mm": {"$round": ["$last_smoothed_precip", 1]}
            },
            "mse_temperature": {
                "$round": [
                    {"$divide": [
                        {"$reduce": {
                            "input": {"$range": [0, {"$size": "$smoothed_temps"}]},
                            "initialValue": 0,
                            "in": {
                                "$add": [
                                    "$$value",
                                    {"$pow": [
                                        {"$subtract": [
                                            {"$arrayElemAt": ["$actual_temps", "$$this"]},
                                            {"$arrayElemAt": ["$smoothed_temps", "$$this"]}
                                        ]},
                                        2
                                    ]}
                                ]
                            }
                        }},
                        {"$size": "$smoothed_temps"}
                    ]},
                    2
                ]
            }
        }
    }}
]
execute_query("27", "weatherHistory", q27_pipeline)

# QUERY 28: Complex correlation analysis with $facet, $lookup, and statistical methods
q28_pipeline = [
    {"$match": {"event_type": {"$exists": true}}},
    {"$facet": {
        "temperature_vs_humidity": [
            {"$project": {
                "_id": 0,
                "event_type": 1,
                "location": 1,
                "temperature_c": 1,
                "humidity_percent": 1
            }},
            {"$group": {
                "_id": {
                    "event_type": "$event_type",
                    "location": "$location"
                },
                "temperature_values": {"$push": "$temperature_c"},
                "humidity_values": {"$push": "$humidity_percent"},
                "count": {"$sum": 1}
            }},
            {"$match": {"count": {"$gte": 5}}},
            {"$project": {
                "_id": 0,
                "event_type": "$_id.event_type",
                "location": "$_id.location",
                "sample_count": "$count",
                "correlation": {
                    "$let": {
                        "vars": {
                            "x_values": "$temperature_values",
                            "y_values": "$humidity_values",
                            "x_mean": {"$avg": "$temperature_values"},
                            "y_mean": {"$avg": "$humidity_values"}
                        },
                        "in": {
                            "$divide": [
                                {"$reduce": {
                                    "input": {"$range": [0, {"$size": "$$x_values"}]},
                                    "initialValue": 0,
                                    "in": {
                                        "$add": [
                                            "$$value",
                                            {"$multiply": [
                                                {"$subtract": [{"$arrayElemAt": ["$$x_values", "$$this"]}, "$$x_mean"]},
                                                {"$subtract": [{"$arrayElemAt": ["$$y_values", "$$this"]}, "$$y_mean"]}
                                            ]}
                                        ]
                                    }
                                }},
                                {"$sqrt": {
                                    "$multiply": [
                                        {"$reduce": {
                                            "input": {"$range": [0, {"$size": "$$x_values"}]},
                                            "initialValue": 0,
                                            "in": {
                                                "$add": [
                                                    "$$value",
                                                    {"$pow": [
                                                        {"$subtract": [{"$arrayElemAt": ["$$x_values", "$$this"]}, "$$x_mean"]},
                                                        2
                                                    ]}
                                                ]
                                            }
                                        }},
                                        {"$reduce": {
                                            "input": {"$range": [0, {"$size": "$$y_values"}]},
                                            "initialValue": 0,
                                            "in": {
                                                "$add": [
                                                    "$$value",
                                                    {"$pow": [
                                                        {"$subtract": [{"$arrayElemAt": ["$$y_values", "$$this"]}, "$$y_mean"]},
                                                        2
                                                    ]}
                                                ]
                                            }
                                        }}
                                    ]
                                }}
                            ]
                        }
                    }
                }
            }},
            {"$addFields": {
                "correlation_strength": {
                    "$switch": {
                        "branches": [
                            {"case": {"$gt": [{"$abs": "$correlation"}, 0.7]}, "then": "Strong"},
                            {"case": {"$gt": [{"$abs": "$correlation"}, 0.5]}, "then": "Moderate"},
                            {"case": {"$gt": [{"$abs": "$correlation"}, 0.3]}, "then": "Weak"}
                        ],
                        "default": "Negligible"
                    }
                },
                "correlation_direction": {
                    "$cond": [
                        {"$gt": ["$correlation", 0]},
                        "Positive",
                        {"$cond": [{"$lt": ["$correlation", 0]}, "Negative", "None"]}
                    ]
                },
                "correlation_coefficient": {"$round": ["$correlation", 2]}
            }},
            {"$sort": {"correlation_coefficient": -1}}
        ],
        "wind_vs_precipitation": [
            {"$project": {
                "_id": 0,
                "event_type": 1,
                "location": 1,
                "wind_speed_kmh": 1,
                "precipitation_mm": 1
            }},
            {"$group": {
                "_id": {
                    "event_type": "$event_type",
                    "location": "$location"
                },
                "wind_values": {"$push": "$wind_speed_kmh"},
                "precipitation_values": {"$push": "$precipitation_mm"},
                "count": {"$sum": 1}
            }},
            {"$match": {"count": {"$gte": 5}}},
            {"$project": {
                "_id": 0,
                "event_type": "$_id.event_type",
                "location": "$_id.location",
                "sample_count": "$count",
                "correlation": {
                    "$let": {
                        "vars": {
                            "x_values": "$wind_values",
                            "y_values": "$precipitation_values",
                            "x_mean": {"$avg": "$wind_values"},
                            "y_mean": {"$avg": "$precipitation_values"}
                        },
                        "in": {
                            "$divide": [
                                {"$reduce": {
                                    "input": {"$range": [0, {"$size": "$$x_values"}]},
                                    "initialValue": 0,
                                    "in": {
                                        "$add": [
                                            "$$value",
                                            {"$multiply": [
                                                {"$subtract": [{"$arrayElemAt": ["$$x_values", "$$this"]}, "$$x_mean"]},
                                                {"$subtract": [{"$arrayElemAt": ["$$y_values", "$$this"]}, "$$y_mean"]}
                                            ]}
                                        ]
                                    }
                                }},
                                {"$sqrt": {
                                    "$multiply": [
                                        {"$reduce": {
                                            "input": {"$range": [0, {"$size": "$$x_values"}]},
                                            "initialValue": 0,
                                            "in": {
                                                "$add": [
                                                    "$$value",
                                                    {"$pow": [
                                                        {"$subtract": [{"$arrayElemAt": ["$$x_values", "$$this"]}, "$$x_mean"]},
                                                        2
                                                    ]}
                                                ]
                                            }
                                        }},
                                        {"$reduce": {
                                            "input": {"$range": [0, {"$size": "$$y_values"}]},
                                            "initialValue": 0,
                                            "in": {
                                                "$add": [
                                                    "$$value",
                                                    {"$pow": [
                                                        {"$subtract": [{"$arrayElemAt": ["$$y_values", "$$this"]}, "$$y_mean"]},
                                                        2
                                                    ]}
                                                ]
                                            }
                                        }}
                                    ]
                                }}
                            ]
                        }
                    }
                }
            }},
            {"$addFields": {
                "correlation_strength": {
                    "$switch": {
                        "branches": [
                            {"case": {"$gt": [{"$abs": "$correlation"}, 0.7]}, "then": "Strong"},
                            {"case": {"$gt": [{"$abs": "$correlation"}, 0.5]}, "then": "Moderate"},
                            {"case": {"$gt": [{"$abs": "$correlation"}, 0.3]}, "then": "Weak"}
                        ],
                        "default": "Negligible"
                    }
                },
                "correlation_direction": {
                    "$cond": [
                        {"$gt": ["$correlation", 0]},
                        "Positive",
                        {"$cond": [{"$lt": ["$correlation", 0]}, "Negative", "None"]}
                    ]
                },
                "correlation_coefficient": {"$round": ["$correlation", 2]}
            }},
            {"$sort": {"correlation_coefficient": -1}}
        ]
    }},
    {"$project": {
        "temperature_humidity_correlations": {"$slice": ["$temperature_vs_humidity", 10]},
        "wind_precipitation_correlations": {"$slice": ["$wind_vs_precipitation", 10]},
        "correlation_summary": {
            "temp_humidity": {
                "strong_positive": {
                    "$size": {
                        "$filter": {
                            "input": "$temperature_vs_humidity",
                            "as": "corr",
                            "cond": {
                                "$and": [
                                    {"$eq": ["$$corr.correlation_strength", "Strong"]},
                                    {"$eq": ["$$corr.correlation_direction", "Positive"]}
                                ]
                            }
                        }
                    }
                },
                "strong_negative": {
                    "$size": {
                        "$filter": {
                            "input": "$temperature_vs_humidity",
                            "as": "corr",
                            "cond": {
                                "$and": [
                                    {"$eq": ["$$corr.correlation_strength", "Strong"]},
                                    {"$eq": ["$$corr.correlation_direction", "Negative"]}
                                ]
                            }
                        }
                    }
                }
            },
            "wind_precipitation": {
                "strong_positive": {
                    "$size": {
                        "$filter": {
                            "input": "$wind_vs_precipitation",
                            "as": "corr",
                            "cond": {
                                "$and": [
                                    {"$eq": ["$$corr.correlation_strength", "Strong"]},
                                    {"$eq": ["$$corr.correlation_direction", "Positive"]}
                                ]
                            }
                        }
                    }
                },
                "strong_negative": {
                    "$size": {
                        "$filter": {
                            "input": "$wind_vs_precipitation",
                            "as": "corr",
                            "cond": {
                                "$and": [
                                    {"$eq": ["$$corr.correlation_strength", "Strong"]},
                                    {"$eq": ["$$corr.correlation_direction", "Negative"]}
                                ]
                            }
                        }
                    }
                }
            }
        }
    }}
]
execute_query("28", "globalClimate", q28_pipeline)

print("\nQueries 27-28 in part 4b executed successfully.") 