import pymongo
import time

# Open MongoDB
client = pymongo.MongoClient("mongodb://localhost:27017")
db = client["bdd4a"]

# Start timer
start = time.time()


# Query 5 -------------------------------------------------

# Find movies with ratings > 200000
succesful = db["ratings"].find({"numVotes": {"$gt": 200000}}, {"_id": 0, "mid": 1})

# Find which are from before 2009
before = []
after = []
for movie in succesful:
    complete_movie_data = db["movies"].find_one({"mid": movie["mid"]})
    if complete_movie_data["startYear"] < 2009:
        before.append(complete_movie_data["mid"])
    elif complete_movie_data["startYear"] > 2009:
        after.append(complete_movie_data["mid"])

# Find actors in Avatar
avatar = db["movies"].find_one({"primaryTitle": "Avatar"})
actors = db["characters"].find({"mid": avatar["mid"]})

# Check for each actor if they were in a succesful movie before
became_famous = []
for actor in actors:
    # Find movies actor starred in
    movies = list(db["characters"].find({"pid": actor["pid"]}))
    
    stop_checking = False
    # See if a actor was famous before 2009
    for movie in movies:
        if movie["mid"] in before:
            stop_checking = True
            break
    
    # If actor not famous before, see if famous after 2009
    if stop_checking == False:
        for movie in movies:
            if movie["mid"] in after:
                became_famous.append(actor["pid"])
                break

# Find name of actors
res = []
for actor in became_famous:
    res.append(db["persons"].find_one({"pid": actor})["primaryName"])


# End timer
end = time.time()

print(res)



# Print elapsed time
time_taken = end - start
print("Temps pour effectuer la requête :", time_taken)