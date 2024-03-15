import pymongo
import sqlite3
import time

# Open SQL database
connection = sqlite3.connect(".\\bdd4a.db")
cursor = connection.cursor()

# Open MongoDB
client = pymongo.MongoClient("mongodb://localhost:27017")
db = client["bdd4a"]

# Find tables' names in the sql database
tables_names = cursor.execute("""
        select name from sqlite_master 
        where type='table'
    """).fetchall()

# Start timer
start = time.time()

# Loop through all tables
for table in tables_names:
    # Create/Open MongoDB Collection with name of the table
    collection = db[table[0]]
    # Delete everything in it
    collection.delete_many({})

    # Query to get everything in current table
    query = cursor.execute("select * from " + table[0])
    
    # Get table's column names
    column_names = []
    for column_name in cursor.description:
        column_names.append(column_name[0])

    # Fetch 1000 row from current table
    data = query.fetchmany(1000)    

    while len(data) > 0:
        info_sent = []

        # Get table content
        for row in data:
            # Get infos on row
            cur_info = []
            for i in range(0, len(column_names)):
                cur_info.append((column_names[i], row[i]))
            # Add row to the infos to send
            info_sent.append(dict(cur_info))
        
        # Insert data (1000 rows)
        collection.insert_many(info_sent)

        # Get next data
        data = query.fetchmany(1000)

# End timer
end = time.time()

# Print elapsed time
time_taken = end - start
print("Temps pour importer la base de données :", time_taken)