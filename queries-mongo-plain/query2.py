import pymongo
import time

# Open MongoDB
client = pymongo.MongoClient("mongodb://localhost:27017")
db = client["bdd4a"]

# Start timer
start = time.time()

# Query 2 ------------------------------

# Find Horror movies
horror = db["genres"].find({"genre": "Horror"}, {"_id": 0, "mid": 1})
# Find those from the 2000s
mid_array = []
for movie in horror:
    mid_array.append(db["movies"].find({"mid": movie["mid"], "startYear": {"$gt": 2000, "$lt": 2009}}))
# Find their ratings
movie_list = []
for i in mid_array:
    for movie in i:
        movie_list.append(movie["mid"])

ratings = []
for mid in movie_list:
    ratings.append(db["ratings"].find_one({"mid": mid}, {"_id": 0, "mid": 1, "averageRating": 1}))

# Sort on rating
ratings = sorted(ratings, key=lambda x: x["averageRating"], reverse=True)
# End timer
end = time.time()
# Print 3 bests
for movie in ratings[:3]:
    print(db["movies"].find_one({"mid": movie["mid"]})["primaryTitle"], movie["averageRating"])



# Print elapsed time
time_taken = end - start
print("Temps pour effectuer la requête :", time_taken)