# -*- coding: utf-8 -*-
"""
Created on Fri Nov 21 21:32:58 2014

"""

import urllib.request as urllib
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

#Part b, download 500,000 lines worth of tweets, record the time it took to save to a text file
response = urllib.urlopen('http://rasinsrv07.cstcis.cti.depaul.edu/CSC455/OneDayOfTweets.txt')
count = 0
tweet_dict = {}

start = time.time()
while count < 2500:
    str_response = response.readline().decode("utf8")
    with open("C:/Users/MASKED_PATH/tweets2.txt", "a", encoding="utf-8") as out_tweet:
        out_tweet.write(str_response)
        out_tweet.close()
    count += 1
end = time.time()
print("The amount of time it took to save the tweets into a file=", end-start)


# Finalize inserts and close the connection to the database
conn.commit()
conn.close()
