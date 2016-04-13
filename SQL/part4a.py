# -*- coding: utf-8 -*-
"""
Created on Wed Nov 26 20:56:47 2014

@author: PoisonTree
"""


# For the Geo table, create a single default entry for the ‘Unknown’ 
# location and round longitude and latitude to a maximum of 4 digits after the decimal.

import sqlite3
import json
import decimal

def generateInsert(tablename):
    for table in tablename:
        allRows = cursor.execute("SELECT * FROM %s;" % table).fetchall()

        rowCount = 0
        # For every row, separate the results of the query above
        for eachRow in allRows:
            print("ROW#", str(rowCount))
            rowCount = rowCount+1
            attributes = []
            # print(eachRow, 'eachRow')
            values = {}           
            if table == 'User':
                #Look at each attribute and evaluate the type
                values["id"]=eachRow[0]
                values["name"]=eachRow[1]
                values["screen_name"]=eachRow[2]
                values["description"]=eachRow[3]
                values["friends_count"]=eachRow[4]
            if table == 'Geo':
                #Look at each attribute and evaluate the type
                values["geo_id"]=eachRow[0]
                values["latitude"]=eachRow[1]
                values["longitude"]=eachRow[2]
                values["type"]=eachRow[3]
            if table == 'Tweets':
                #Look at each attribute and evaluate the type
                values["created_at"]=eachRow[0]
                values["id_str"]=eachRow[1]
                values["text"]=eachRow[2]
                values["source"]=eachRow[3]
                values["in_reply_to_user_id"]=eachRow[4]
                values["in_reply_to_screen_name"]=eachRow[5]
                values["in_reply_to_status_id"]=eachRow[6]
                values["retweet_count"]=eachRow[7]
                values["contributors"]=eachRow[8]
                values["id"]=eachRow[9]

            print(values,'VALUES')
            # print("LENGTH OF",len(values))
            # print(values)
            
            # print("LOOK HERE", values)
            with open('C:/Users/PoisonTree/Documents/CDM_455/Final/all_tables.txt', 'a', encoding='utf-8') as f:
                f.write(str(values))
                f.write("|")
        with open('C:/Users/PoisonTree/Documents/CDM_455/Final/all_tables.txt', 'a', encoding='utf-8') as f:
                f.write("TABLEDELIMITER")
#            with open('C:/Users/PoisonTree/Documents/CDM_455/Final/all_tables.txt', 'a') as f:
#                json.dump(values, f)
            # print("STATEMENT==",statement)
# Open a connection to database
conn = sqlite3.connect("csc455_Final.db")

# Request a cursor from the database
cursor = conn.cursor()

cursor.execute(" ")

generateInsert(['Geo','User','Tweets'])

# Finalize inserts and close the connection to the database
conn.commit()
conn.close()
# f.close()
# Open a file
f = open('C:/Users/PoisonTree/Documents/CDM_455/Final/all_tables.txt', 'r', encoding='utf-8')
for row in f.readlines():
    # Items holds all tweets
    separate_tables= row.split('TABLEDELIMITER')
tables = {}
tables['geo'] = separate_tables[0]
tables['user'] = separate_tables[1]
tables['tweets'] = separate_tables[2]

organized_dictionary = {"geo":{},"user":{},"tweets":{}}

items = []
for obs in tables:
    tables[obs] = tables[obs].split('|')
for obs in tables: 
    del tables[obs][-1]
    for i in range(len(tables[obs])):
        for item in tables[obs]:
            item = item.replace("\"",'')
            item = item.replace('None','null')
            item = item.replace("'",'"')
            print('\n',item)
            organized_dictionary[obs][i] = json.loads(item)
    
# For the Geo table, create a single default entry for the ‘Unknown’ 
# location and round longitude and latitude to a maximum of 4 digits after the decimal.

for i in range(len(organized_dictionary['geo'])): 
    if organized_dictionary['geo'][i]['latitude'] is None and organized_dictionary['geo'][i]['latitude'] is None:
        organized_dictionary['geo'][i].setdefault("unknown",float(format(decimal.Decimal('0'), '.4f')))
    else:
        digitlat = organized_dictionary['geo'][i]['latitude']
        digitlong = organized_dictionary['geo'][i]['longitude']
        digitlat = '%.4f' % round(digitlat, 4)
        digitlong = '%.4f' % round(digitlong, 4)
        organized_dictionary['geo'][i]['latitude'] = format(decimal.Decimal(digitlat), '.4f')
        organized_dictionary['geo'][i]['longitude'] = format(decimal.Decimal(digitlong), '.4f')
print(organized_dictionary['geo'])
with open('C:/Users/PoisonTree/Documents/CDM_455/Final/4a_output.txt', 'a', encoding='utf-8') as f:
    f.write(str(organized_dictionary['geo']))

# For the Tweet table, replace NULLs by a reference to ‘Unknown’ entry 
# (i.e., the foreign key column that references Geo table should refer to the “Unknown” entry
#  you created in part-a. Report how many known/unknown locations there were in total 
# (e.g., 10,000 known, 490,000 unknown,  2% locations are available)

unknown_list = []
known_list = []
for i in range(len(organized_dictionary['tweets'])): 
    if organized_dictionary['geo'][i]['unknown'] == 0.0:
        unknown_list.append(organized_dictionary['tweets'][i]['id'])
        unknown_locations = len(unknown_list)
    else:
        known_list.append(organized_dictionary['tweets'][i]['id'])
        known_locations = len(known_list)
with open('C:/Users/PoisonTree/Documents/CDM_455/Final/4b_output.txt', 'a', encoding='utf-8') as f:
    f.write("Number of unknown locations:" + str(len(unknown_list)) +"\n")
    f.write("Number of known locations:"+ str(len(known_list)) +"\n")
    summ=len(unknown_list)+len(known_list)
    locations_available = "{0:.0f}%".format(len(known_list)/(summ)*100)
    locations_unavailable = "{0:.0f}%".format(len(unknown_list)/(summ)*100)
    f.write(str(locations_available)+ "of the locations are available" +"\n")
    f.write(str(locations_unavailable)+ "of the locations are not known" +"\n")
    f.write("List of geo_id's which have unknown locations:"+str(unknown_list) +"\n")
    f.write("List of geo_id's which have known locations:"+str(known_list) +"\n")

# For the User table file add a column (true/false) 
# that specifies whether “screen_name” or “description” attribute contains 
# within it the “name” attribute of the same user. That is, your output file 
# should contain all of the columns from the User table, plus the new column. 
# You do not have to modify the original User table.
for i in range(len(organized_dictionary['user'])):         
    organized_dictionary['user'][i].setdefault("contains_name","False")
for i in range(len(organized_dictionary['user'])):         
    if organized_dictionary['user'][i]['name'] in organized_dictionary['user'][i]['screen_name']:
        organized_dictionary['user'][i]['contains_name'] = 'True'
    elif organized_dictionary['user'][i]['description'] is not None and organized_dictionary['user'][i]['name'] in organized_dictionary['user'][i]['description']:
        organized_dictionary['user'][i]['contains_name'] = 'True'
with open('C:/Users/PoisonTree/Documents/CDM_455/Final/4c_output.txt', 'a', encoding='utf-8') as f:
    f.write(str(organized_dictionary['user'])+"\n")
    

    