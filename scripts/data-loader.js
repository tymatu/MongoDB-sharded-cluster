// data-loader.js - Script to validate and load weather data into MongoDB

// Schema definition for validation - extremely permissive schema
const weatherSchema = {
  bsonType: "object",
  required: ["station_id", "date", "location"],
  properties: {
    station_id: {
      bsonType: "string",
      description: "Station ID must be a string and is required"
    },
    date: {
      bsonType: "string",
      description: "Date must be a string and is required"
    },
    location: {
      bsonType: "string",
      description: "Location must be a string and is required"
    }
    // No validation for other fields to ensure all data can be loaded
  }
};

// Database and collection names
const dbName = "weatherDB";
const collectionsData = [
  { name: "globalClimate", path: "/app/Data/global_climate.json" },
  { name: "usWeatherEvents", path: "/app/Data/us_weather_events.json" },
  { name: "weatherHistory", path: "/app/Data/weather_history.json" }
];

// Verify that data files exist
const fs = require('fs');
collectionsData.forEach(collection => {
  try {
    if (fs.existsSync(collection.path)) {
      console.log(`Data file found: ${collection.path}`);
    } else {
      console.error(`Data file NOT found: ${collection.path}`);
    }
  } catch(err) {
    console.error(`Error checking data file ${collection.path}:`, err);
  }
});

// Function to create collection with validation schema
async function createCollectionWithValidation(db, collectionName) {
  try {
    await db.createCollection(collectionName, {
      validator: {
        $jsonSchema: weatherSchema
      },
      validationLevel: "strict",
      validationAction: "error"
    });
    console.log(`Created collection ${collectionName} with validation schema`);
  } catch (err) {
    if (err.code === 48) {
      console.log(`Collection ${collectionName} already exists, updating validator`);
      await db.command({
        collMod: collectionName,
        validator: {
          $jsonSchema: weatherSchema
        },
        validationLevel: "strict",
        validationAction: "error"
      });
    } else {
      console.error(`Error creating collection ${collectionName}:`, err);
      throw err;
    }
  }
}

// Function to load data from JSON file
async function loadDataFromFile(db, collectionName, filePath) {
  try {
    const fs = require('fs');
    const data = JSON.parse(fs.readFileSync(filePath, 'utf8'));
    
    // Validate and transform data
    const validData = data.map(item => {
      // Convert string numbers to actual numbers for MongoDB
      if (typeof item.temperature_c === 'string') item.temperature_c = parseFloat(item.temperature_c);
      if (typeof item.humidity_percent === 'string') item.humidity_percent = parseInt(item.humidity_percent);
      if (typeof item.wind_speed_kmh === 'string') item.wind_speed_kmh = parseFloat(item.wind_speed_kmh);
      if (typeof item.precipitation_mm === 'string') item.precipitation_mm = parseFloat(item.precipitation_mm);
      return item;
    });
    
    // Get collection
    const collection = db.collection(collectionName);
    
    // Clear existing data
    await collection.deleteMany({});
    
    // Insert data
    if (validData.length > 0) {
      const result = await collection.insertMany(validData);
      console.log(`Inserted ${result.insertedCount} documents into collection ${collectionName}`);
    } else {
      console.log(`No valid data to insert into collection ${collectionName}`);
    }
  } catch (err) {
    console.error(`Error loading data into ${collectionName}:`, err);
    throw err;
  }
}

// Main function to connect to MongoDB and load data
async function loadData() {
  const { MongoClient } = require('mongodb');
  
  // Connect to MongoDB router
  const uri = "mongodb://admin:admin@router01:27017/?authSource=admin";
  
  let client;
  try {
    console.log("Attempting to connect to MongoDB cluster...");
    client = new MongoClient(uri, { 
      serverSelectionTimeoutMS: 5000,
      connectTimeoutMS: 10000
    });
    await client.connect();
    console.log("Connected to MongoDB cluster");
    
    const db = client.db(dbName);
    
    // Enable sharding for the database
    try {
      const adminDb = client.db("admin");
      await adminDb.command({ enableSharding: dbName });
      console.log(`Enabled sharding for database ${dbName}`);
    } catch (err) {
      // Ignore error if sharding is already enabled
      console.log(`Note: ${err.message}`);
    }
    
    // Process each collection
    for (const collection of collectionsData) {
      // Create collection with validation
      await createCollectionWithValidation(db, collection.name);
      
      // Shard the collection
      try {
        const adminDb = client.db("admin");
        await adminDb.command({
          shardCollection: `${dbName}.${collection.name}`,
          key: { station_id: "hashed" }
        });
        console.log(`Sharded collection ${collection.name} by station_id`);
      } catch (err) {
        // Ignore error if collection is already sharded
        console.log(`Note: ${err.message}`);
      }
      
      // Load data
      await loadDataFromFile(db, collection.name, collection.path);
    }
    
    // Create indexes
    for (const collection of collectionsData) {
      const coll = db.collection(collection.name);
      await coll.createIndex({ date: 1 });
      await coll.createIndex({ location: 1 });
      if (collection.name !== "weatherHistory") {
        await coll.createIndex({ event_type: 1 });
      }
      console.log(`Created indexes for collection ${collection.name}`);
    }
    
    console.log("Data loading completed successfully");
  } catch (err) {
    console.error("Error:", err);
  } finally {
    if (client) {
      await client.close();
      console.log("MongoDB connection closed");
    }
  }
}

// Run the data loading process
loadData().catch(console.error); 