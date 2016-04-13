# -*- coding: utf-8 -*-
"""
Created on Tue Nov 25 04:27:16 2014

@author: PoisonTree
"""

# -*- coding: utf-8 -*-
"""
Created on Mon Nov 24 18:47:18 2014

@author: SGHAZVIN
"""

import json
import sqlite3
import time

# Part a, create the Geo Table
TweetTable = """
CREATE TABLE Tweets
(
    created_at VARCHAR(40),
    id_str VARCHAR(25),
    text VARCHAR(200),
    source VARCHAR(200),
    in_reply_to_user_id NUMBER,
    in_reply_to_screen_name VARCHAR(30),
    in_reply_to_status_id NUMBER,
    retweet_count NUMBER,
    contributors NUMBER,
    id NUMBER,

    CONSTRAINT Tweets_PK
    PRIMARY KEY (id_str),

    CONSTRAINT Tweets_FK1
    FOREIGN KEY(id)
      REFERENCES User(id),
      
    CONSTRAINT Tweets_FK2
    FOREIGN KEY(id)
      REFERENCES Geo(geo_id)
);
"""
UserTable = """
CREATE TABLE User
(
    id NUMBER,
    name VARCHAR(30),
    screen_name VARCHAR(30),
    description VARCHAR(200),
    friends_count NUMBER,

    CONSTRAINT User_PK
    PRIMARY KEY (id)
);
"""
GeoTable = """
CREATE TABLE Geo
(
    geo_id NUMBER,
    type VARCHAR(10),
    latitude Number,
    longitude Number,

    CONSTRAINT User_PK
    PRIMARY KEY (geo_id)
);
"""
# Open a connection to database
conn = sqlite3.connect("csc455_Final.db")

# Request a cursor from the database
cursor = conn.cursor()

# Get rid of the student table if we already created it
cursor.execute("DROP TABLE IF EXISTS Tweets;")
cursor.execute("DROP TABLE IF EXISTS User;")
cursor.execute("DROP TABLE IF EXISTS Geo;")

cursor.execute(UserTable)
cursor.execute(GeoTable)
cursor.execute(TweetTable)

#Part d, read the tweets from the tweets.txt file, fill in the tables.
start = time.time()

tweets_file= open("C:/Users/PoisonTree/Documents/CDM_455/Final/tweets2.txt", "r",encoding="utf8")
tweet_dict = {}
for i in range(2500):
    each_line = tweets_file.readline().replace("\n","")
    tweet_dict[i] = json.loads(each_line)

    # Collect multiple rows so that we can use "executemany".  We do
    # not want to collect all of the numLines rows because there may
    # not be enough memory for that. So we insert batchRows at a time
batchRows = 50
batchedinserts_tweets = []
batchedinserts_geo = []
batchedinserts_user = []

count = 0

tweetkeys = ['created_at','id_str','text','source','in_reply_to_user_id', 'in_reply_to_screen_name', 'in_reply_to_status_id', 'retweet_count', 'contributors']
geokeys = ['coordinates']
userkeys = ['id','name','screen_name','description','friends_count']
for tweet_obs in tweet_dict:
    newRowtweet = []
    newRowgeo = []    
    newRowuser = []

    newRowgeo.append(tweet_obs)
    if tweet_dict[tweet_obs]['geo'] is None:
        newRowgeo.append(None)
        newRowgeo.append(None)
        newRowgeo.append(None)
    else:
        newRowgeo.append(tweet_dict[tweet_obs]['geo']['coordinates'][0])
        newRowgeo.append(tweet_dict[tweet_obs]['geo']['coordinates'][1])
        newRowgeo.append(tweet_dict[tweet_obs]['geo']['type'])
    for key in userkeys:
        # Treat '', [] and 'null' as NULL
        if tweet_dict[tweet_obs]['user'][key] in ['',[],'null']:
            newRowuser.append(None)
        else:
            newRowuser.append(tweet_dict[tweet_obs]['user'][key])
    for key in tweetkeys:
        # Treat '', [] and 'null' as NULL
        if tweet_dict[tweet_obs][key] in ['',[],'null']:
            newRowtweet.append(None)
        else:
            newRowtweet.append(tweet_dict[tweet_obs][key])
    newRowtweet.append(tweet_obs)

    # Add the new row to the collected batches
    batchedinserts_tweets.append(newRowtweet)
    batchedinserts_geo.append(newRowgeo)
    batchedinserts_user.append(newRowuser)

    # If we have reached # of batchRows, use executemany to insert what we collected
    # so far, and reset the batchedInserts list back to empty
    if (len(batchedinserts_tweets)+len(batchedinserts_geo)+len(batchedinserts_user)) >= batchRows:
        cursor.executemany('INSERT OR IGNORE INTO Geo VALUES (?,?,?,?);', batchedinserts_geo)
        cursor.executemany('INSERT OR IGNORE INTO User VALUES (?,?,?,?,?);', batchedinserts_user)
        cursor.executemany('INSERT OR IGNORE INTO Tweets VALUES (?,?,?,?,?,?,?,?,?,?);', batchedinserts_tweets)
        print("executing")        
        # Reset the batching process
        batchedinserts_tweets = []
        batchedinserts_geo = []
        batchedinserts_user = []
tweets_file.close()

end = time.time()

# Verify that the data has been loaded and report the number of rows for each table
all_table_count = []
for table in ['Tweets', 'User', 'Geo']:
    allRows = cursor.execute("SELECT * FROM %s;" % table).fetchall()
    rowCount = 0
    # For every row, print the results of the query above, separated by a tab
    for eachRow in allRows:
        print("ROW#", str(rowCount))
        rowCount = rowCount+1
        for value in eachRow:
            print(value, "\t")
        print("\nEnd")  # \n is the end of line symbol
    all_table_count.append(rowCount)
print("The number of rows in the Tweets Table is:", all_table_count[0])
print("The number of rows in the User Table is:", all_table_count[1])
print("The number of rows in the Geo Table is:", all_table_count[2])

print("The amount of time it took to fill the tables", end-start)
# Finalize inserts and close the connection to the database
conn.commit()
conn.close()

