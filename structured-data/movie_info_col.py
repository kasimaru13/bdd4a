import pymongo
import time


# Open MongoDB
client = pymongo.MongoClient("mongodb://localhost:27017")
db = client["bdd4a"]

# Create/Open new collection
collection = db["movies_info"]

# Delete everything in it
collection.delete_many({})

# Start timer
start = time.time()


movies = db["movies"].find({})

counter = 0
entries = []
for movie in movies:
    # print(counter)
    entry = {}

    # Mid
    mid = movie["mid"]
    entry["mid"] = mid

    # Title
    entry["title"] = movie["primaryTitle"]

    # Other titles
    query = db["titles"].find({"mid": mid}, {"_id": 0, "region": 1, "title": 1})
    titles = []
    for region in query:
        titles.append(region)
    entry["titles"] = titles    

    # Genres
    query = db["genres"].find({"mid": mid})
    entry["genres"] = [element["genre"] for element in query]

    # Find principals
    principals = db["principals"].find({"mid": mid})

    actors = []
    others = []
    for principal in principals:
        person = {}
        pid = principal["pid"]
        person["pid"] = pid

        # Find name
        person["name"] = db["persons"].find_one({"pid": pid}, {"_id": 0, "primaryName": 1})
        
        if principal["category"] == "actor":
            # Find characters played
            query = db["characters"].find({"mid": mid, "pid": pid})
            characters = []
            for character in query:
                characters.append(character["name"])
            
            # Add actor to list
            actors.append(person)

        else:
            person["category"] = principal["category"]

            # Add to others
            others.append(person)

    entry["principals"] = others
    entry["actors"] = actors

    # Rating
    rating = db["ratings"].find_one({"mid": mid})
    entry["averageRating"] = rating["averageRating"]
    entry["numVotes"] = rating["numVotes"]


    entries.append(entry)
    counter += 1

    # Send data by batch of 1000
    if (counter == 1000):
        collection.insert_many(entries)
        entries = []
        counter = 0
        break


# End timer
end = time.time()

# Display execution time
elapsed_time = end-start
print("Temps pour créer et remplir la collection : ", elapsed_time, "s")