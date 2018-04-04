# -*- coding: utf-8 -*-
"""
Created on Tue Nov 25 20:21:59 2014

"""
import re
import json
import time

# Write python code that is going to read the locally saved tweet data
# file from 1-b and perform the equivalent computation for parts 2-i and 2-ii only. 
# How does the runtime compare to the SQL queries?

tweets_file= open("C:/Users/MASKED_PATH/tweets2.txt", "r",encoding="utf8")
tweet_dict = {}

for i in range(2500):
    each_line = tweets_file.readline().replace("\n","")
    tweet_dict[i] = json.loads(each_line)
    
# i.	Find tweets where tweet id_str contains “44” or “77” anywhere in the column
start = time.time()
pattern = re.compile('4{2}|7{2}')
matched_tweets = []
for tweet_obs in tweet_dict:
    match = re.search(pattern,  tweet_dict[tweet_obs]['id_str'])
    if match:
        matched_tweets.append(tweet_dict[tweet_obs])
print(matched_tweets)
end = time.time()
print("The amount of time it took:", end-start)

# ii.	Find how many unique values are there in the “in_reply_to_user_id” column
start = time.time()
values = []
for tweet_obs in tweet_dict:
    value = tweet_dict[tweet_obs]['in_reply_to_user_id']
    values.append(value)
unique_values = set(values)
print("The number of unique values in the 'in_reply_to_user_id' column is:",len(unique_values))
end = time.time()
print("The amount of time it took:", end-start)
