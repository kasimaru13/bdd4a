import pymongo
import time

# Open MongoDB
client = pymongo.MongoClient("mongodb://localhost:27017")
db = client["bdd4a"]

# Start timer
start = time.time()

# Query 4 --------------------------------------------------------------------------

query = db["characters"].aggregate([{
        "$group": {"_id": {"pid": "$pid", "mid": "$mid"}, "nb_roles": {"$sum": 1}}
    }])

max_roles = 0
result = set()
for person_movie in query:
    if person_movie["nb_roles"] > max_roles:
        max_roles = person_movie["nb_roles"]
        result.clear()
    if person_movie["nb_roles"] == max_roles:
        result.add(person_movie["_id"]["pid"])

# End timer
end = time.time()

# Show query result
print(len(result))



# Print elapsed time
time_taken = end - start
print("Temps pour effectuer la requête :", time_taken)