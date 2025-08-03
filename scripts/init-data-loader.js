// Initialize replicaset if not already done

// Create admin user if not already done
db = db.getSiblingDB('admin');
if (db.getUser('admin') == null) {
  db.createUser({
    user: 'admin',
    pwd: 'admin',
    roles: [
      { role: 'root', db: 'admin' }
    ]
  });
  print('Admin user created');
} else {
  print('Admin user already exists');
}

// Enable sharding for the weatherDB database
try {
  sh.enableSharding("weatherDB");
  print("Sharding enabled for weatherDB");
} catch (e) {
  print("Note: " + e.message);
}

// Log completion
print("Initialization complete. Ready to load data."); 