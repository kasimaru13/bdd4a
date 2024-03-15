import sqlite3
import time

# Open Database
connection = sqlite3.connect("bdd4a.db")
current = connection.cursor()

# Mesure le début de la requête
start = time.time()

# Query 1:
query_res = current.execute("""
    select primaryTitle 
    from movies 
    natural join characters
    natural join persons
    where primaryName = 'Jean Reno';
""").fetchall()

# Query 2:
# query_res = current.execute("""
#                             select primaryTitle, averageRating
#                             from ratings 
#                             natural join movies 
#                             natural join genres
#                             where genre = 'Horror' and startYear between 2000 and 2009
#                             order by averageRating desc limit 3;
#                             """).fetchall()

# Query 3:
# query_res = current.execute("""
#                             select count(pid)
#                             from writers
#                             where writers.mid not in (
#                                 select mid 
#                                 from titles
#                                 where region = 'ES'
#                             )
#                             """).fetchall()


# Query 4:
# query_res = current.execute("""
#                             with differentRoles as (
#                             select mid, pid, count(name) as nbRoles
#                             from characters
#                             group by mid, pid
#                             ),
#                             maxRoles as (
#                             select max(nbRoles) from differentRoles limit 1
#                             )
#                             select count(*) from(
#                             select distinct primaryName
#                             from differentRoles
#                             natural join persons
#                             where nbRoles in maxRoles)
#                             """)

# Query 5 (pas fonctionnel)
# query_res = current.execute(""" explain query plan
#                             with before as (select pid
#                                 from movies
#                                 natural join ratings
#                                 natural join principals
#                                 where numVotes > 200000
#                                 and startYear < 2009
#                             ),
#                             after as (select pid
#                                 from movies
#                                 natural join ratings
#                                 natural join principals
#                                 where numVotes > 200000
#                                 and startYear > 2009
#                             ),
#                             avatar_actors as (
#                                 select pid from principals
#                                 natural join movies
#                                 where primaryTitle = 'Avatar'
#                             )

#                             select primaryName from (
#                                 select distinct(pid) from avatar_actors
#                                 natural join after
#                                 except
#                                 select distinct(pid) from avatar_actors
#                                 natural join before
#                             ) natural join persons
#                             """)



# Mesure la fin de la requête
end = time.time()

# Affiche les résultats
for res in query_res:
    print(res)

# Affiche le temps prit pour faire la requête
elapsed_time = end - start
print("Temps de calcul :", elapsed_time, "secondes")