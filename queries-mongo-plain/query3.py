import pymongo
import time

# Open MongoDB
client = pymongo.MongoClient("mongodb://localhost:27017")
db = client["bdd4a"]

# Start timer
start = time.time()


# Query 3 --------------------------------------------------------------------

# Find movies published in spain
not_spannish = db["titles"].find({"region": "ES"}).distinct("mid")
# Find all writers not in the list of movies published in spain
writers = db["writers"].find({"mid": {"$nin": not_spannish}}).distinct("pid")
# End timer
end = time.time()
print(len(writers))



# Print elapsed time
time_taken = end - start
print("Temps pour effectuer la requête :", time_taken)