import pymongo
import time

# Open MongoDB
client = pymongo.MongoClient("mongodb://localhost:27017")
db = client["bdd4a"]

# Start timer
start = time.time()


# Query 1 -----------------------------
res = []

# Find Jean Reno
jean = db["persons"].find_one({"primaryName": "Jean Reno"})
# Find characters he played
characters = db["characters"].find({"pid": jean["pid"]})
movies_col = db["movies"]
# Find movies id for these characters
for character in characters:
    cur_movie = (movies_col.find({"mid": character["mid"]}, {"_id": 0, "primaryTitle": 1}))
    
    # Print results
    for movie in cur_movie:
        res.append(movie["primaryTitle"])

# End timer
end = time.time()

print(res)



# Print elapsed time
time_taken = end - start
print("Temps pour effectuer la requête :", time_taken)