# -*- coding: utf-8 -*-
"""
Created on Tue Nov 25 13:36:16 2014

@author: PoisonTree
"""

import os
import re
import sqlite3


# Open a connection to database
conn = sqlite3.connect("csc455_Final.db")
# Request a cursor from the database
cursor = conn.cursor()

# i.	Find tweets where tweet id_str contains “44” or “77” anywhere in the column
start = time.time()
search1 = str(44)
search2 = str(77)
rows = cursor.execute("SELECT * FROM Tweets Where id_str LIKE ? or id_str LIKE ?",('%'+search1+'%','%'+search2+'%')).fetchall()
print(rows)
print("Number of matches found=", len(rows))
end = time.time()
print("The amount of time it took to query", end-start)

# ii.	Find how many unique values are there in the “in_reply_to_user_id” column
start = time.time()
rows = cursor.execute("SELECT DISTINCT COUNT(in_reply_to_user_id) FROM Tweets").fetchall()
print("Number of uniques values in the “in_reply_to_user_id” column=", rows[0][0])
end = time.time()
print("The amount of time it took to query", end-start)

# iii.	Find the tweet(s) with the longest text message
rows = cursor.execute("SELECT * FROM Tweets WHERE LENGTH(text)= (SELECT max(length(text)) from Tweets)").fetchall()
print("The tweet(s) with the longest text=", len(rows))

# iv.	Find the average longitude and latitude value for each user name.
rows = cursor.execute("SELECT AVG(geo.latitude), AVG(geo.longitude) FROM Tweets JOIN Geo on Geo.geo_id = Tweets.id Group By geo.geo_id").fetchall()

rows = cursor.execute("SELECT User.name From User Join Tweets on user.id = Tweets.id Where Tweets.id =(SELECT Tweets.id, AVG(geo.latitude) FROM Tweets JOIN Geo on Geo.geo_id = Tweets.id Group By geo.geo_id) and Tweets.id = (SELECT AVG(geo.longitude) FROM Tweets JOIN Geo on Geo.geo_id = Tweets.id Group By geo.geo_id)").fetchall()
rows = cursor.execute("SELECT AVG(geo.latitude), AVG(geo.longitude) From Geo WHERE geo.geo_id =(SELECT Tweets.id FROM Tweets JOIN User on User.id = Tweets.id) Group by geo_id").fetchall()
rows = cursor.execute("SELECT AVG(geo.latitude), AVG(geo.longitude) From Geo JOIN Tweets on geo.geo_id = Tweets.id Join User on user.id = Tweets.id group by user.name").fetchall()


# v.	Re-execute the query in part iv) 10 times and 100 times and measure the total runtime (just re-run the same exact query using a for-loop). Does the runtime scale linearly? (i.e., does it take 10X and 100X as much time?)
count = 0
while count < 100:
    rows = cursor.execute("SELECT AVG(geo.latitude), AVG(geo.longitude) FROM Tweets JOIN Geo on Geo.geo_id = Tweets.id Group By geo.geo_id").fetchall()

while count <1000:
    rows = cursor.execute("SELECT AVG(geo.latitude), AVG(geo.longitude) FROM Tweets JOIN Geo on Geo.geo_id = Tweets.id Group By geo.geo_id").fetchall()

conn.commit()
conn.close()



