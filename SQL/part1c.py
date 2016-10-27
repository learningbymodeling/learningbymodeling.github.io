# -*- coding: utf-8 -*-
"""
Created on Mon Nov 24 00:40:25 2014

@author: PoisonTree
"""

import urllib.request as urllib
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

#Part c, download 500,000 lines worth of tweets, fill in the tables.
response = urllib.urlopen('http://rasinsrv07.cstcis.cti.depaul.edu/CSC455/OneDayOfTweets.txt')
count = 0
tweet_dict = {}

start = time.time()
while count < 2500:
    str_response = response.readline().decode("utf8")
    try:
        tweet_dict[count] = json.loads(str_response)
    except(ValueError):
        pass
    count += 1

#for i in tweet_dict:
#    if tweet_dict[i]['geo'] != None:
#        print(tweet_dict[i]['geo'], "THIS IS GEO")
#        print(tweet_dict[i]['geo']['coordinates'][0], "=latitude")
#        print(tweet_dict[i]['geo']['coordinates'][1], "=longitude")
#        print(tweet_dict[i]['geo']['type'])
#    else:
#        print(tweet_dict[i]['geo'], "THIS IS BLANK GEO")


all_tweets = []
all_users = []
all_geos = []
tweet_obs = 0
for tweet_obs in tweet_dict:
    single_tweet_data = []
    single_user_data = []
    single_geo_data = []

    single_user_data.append(tweet_dict[tweet_obs]['user']['id'])
    single_user_data.append(tweet_dict[tweet_obs]['user']['name'])
    single_user_data.append(tweet_dict[tweet_obs]['user']['screen_name'])
    single_user_data.append(tweet_dict[tweet_obs]['user']['description'])
    single_user_data.append(tweet_dict[tweet_obs]['user']['friends_count'])
    all_users.append(single_user_data)
    
    single_geo_data.append(tweet_obs)
    if tweet_dict[tweet_obs]['geo'] != None:
        single_geo_data.append(tweet_dict[tweet_obs]['geo']['coordinates'][0])
        single_geo_data.append(tweet_dict[tweet_obs]['geo']['coordinates'][1])
        single_geo_data.append(tweet_dict[tweet_obs]['geo']['type'])
    else:
        single_geo_data.append(None)
        single_geo_data.append(None)
        single_geo_data.append(None)
    all_geos.append(single_geo_data)
    
    single_tweet_data.append(tweet_dict[tweet_obs]['created_at'])
    single_tweet_data.append(tweet_dict[tweet_obs]['id_str'])
    single_tweet_data.append(tweet_dict[tweet_obs]['text'])
    single_tweet_data.append(tweet_dict[tweet_obs]['source'])
    single_tweet_data.append(tweet_dict[tweet_obs]['in_reply_to_user_id'])
    single_tweet_data.append(tweet_dict[tweet_obs]['in_reply_to_screen_name'])
    single_tweet_data.append(tweet_dict[tweet_obs]['in_reply_to_status_id'])
    single_tweet_data.append(tweet_dict[tweet_obs]['retweet_count'])
    single_tweet_data.append(tweet_dict[tweet_obs]['contributors'])
    single_tweet_data.append(tweet_obs)
    all_tweets.append(single_tweet_data)

for geo in all_geos:
    cursor.execute("INSERT OR IGNORE INTO Geo VALUES (?,?,?,?);", geo)
for tweet in all_tweets:
    cursor.execute("INSERT OR IGNORE INTO Tweets VALUES (?,?,?,?,?,?,?,?,?,?);", tweet)
for user in all_users:
    cursor.execute("INSERT OR IGNORE INTO User VALUES (?,?,?,?,?);", user)
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
