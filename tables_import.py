import sqlite3
import os
import time
import csv

# Ce script convertit les fichiers CSV présents dans le dossier .\csv en tables dans la base de 
# données bdd4A.db


# Returns as much interrogation points as there are attributes for the table (separated by comas)
# Used in the query for execute many, to put in the values
# ex: persons -> ?,?,?,?
def getInterogationString(nbCol):
    res = ""
    for i in range(0, nbCol):
        res += "?,"
    return res[:-1]
    

# MAIN ---------------------------------------------------------------------

# Open Database
connection = sqlite3.connect("bdd4a.db")
current = connection.cursor()


# List files in csv directory
db_size = input("Quelle bdd importer ? (tiny, medium, ...)\n")
path = ".\\imdb-" + db_size +"\\"
file_list = os.listdir(path)

# Start time of the importation
start = time.time()

# Put data from each file in .\csv directory in database, in the table with the same name as the file
for file_name in file_list:
    # Open file
    file = open(path + file_name, encoding = "utf8")
    reader = csv.reader(file)
    
    header = next(reader) # first line, used for reference of number of attributes, we get rid of it

    table_name = file_name[:-4] # get db table name

    current.execute("delete from " + table_name + ";")
    
    line = header
    while line is not None:
        data = []

        # Read lines
        i = 0
        while i < 2000000:
            line = next(reader, None)
            if (line is None):
                break

            # Execute sql insert request
            data.append(tuple(line))

            i += 1
        
        statement = "insert into " + table_name + " values(" + getInterogationString(len(header)) + ");"
        current.executemany(statement, data)

        # Commit all insert request
        connection.commit()

    # Close current file
    file.close()


# End time of importation
end = time.time()

# Show time taken to import database
time_taken = end - start
print("Temps pour importer la base de données :", time_taken)