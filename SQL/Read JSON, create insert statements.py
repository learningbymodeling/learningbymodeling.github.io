# -*- coding: utf-8 -*-
"""
Created on Wed Nov 26 06:17:34 2014

"""

# Create a similar collection of INSERT for the User table by 
# reading/parsing data from the local tweet file that you have saved 
# earlier. How do these compare in runtime? Which method was faster?

import json
import sqlite3
import time



alpha ={'1':'a','2':'b','3':'c','4':'d','5':'e','6':'f','7':'g','8':'h','9':'i','0':'j'}
#Part d, read the tweets from the tweets.txt file, fill in the tables.
start = time.time()

tweets_file= open("C:/Users/MASKED_PATH/tweets.txt", "r",encoding="utf8")
tweet_dict = {}
for i in range(60):
    each_line = tweets_file.readline().replace("\n","")
    tweet_dict[i] = json.loads(each_line)

all_users = []
tweet_obs = 0
for tweet_obs in tweet_dict:
    single_user_data = []

    single_user_data.append(tweet_dict[tweet_obs]['user']['id'])
    single_user_data.append(tweet_dict[tweet_obs]['user']['name'])
    single_user_data.append(tweet_dict[tweet_obs]['user']['screen_name'])
    single_user_data.append(tweet_dict[tweet_obs]['user']['description'])
    single_user_data.append(tweet_dict[tweet_obs]['user']['friends_count'])
    all_users.append(single_user_data)
    strings = []
    digits = str(single_user_data[0])
    for digit in digits:
        strings.append(alpha[digit])
    string_id = "".join(strings) #THIS IS THE STRING_ID
    single_user_data.append(string_id)

    statement = 'INSERT OR IGNORE INTO User VALUES ({},{},{},{},{},{});'.format(*single_user_data)+"\n"
    with open('C:/Users/MASKED_PATH/Final/part3b.txt', 'a', encoding='utf-8') as f:
        f.write(statement)
end = time.time()
print("The amount of time it took to fill the tables", end-start)


