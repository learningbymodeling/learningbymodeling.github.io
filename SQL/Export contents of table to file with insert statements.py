# -*- coding: utf-8 -*-
"""
Created on Tue Nov 25 23:49:25 2014

@author: PoisonTree
"""

# Export the contents of the User table from a SQLite table into a 
# sequence of INSERT statements within a file. This is very similar to 
# what you did in Assignment 4. However, you have to add a unique ID column 
# which has to be a string (you cannot use any numbers). Hint: one possibility 
# is to replace digits with letters, e.g., chr(ord('a')+1) gives you a 'b' 
# and chr(ord('a')+2) returns a 'c'

import sqlite3

alpha ={'1':'a','2':'b','3':'c','4':'d','5':'e','6':'f','7':'g','8':'h','9':'i','0':'j'}

def generateInsert(tablename):
    for table in [tablename]:
        allRows = cursor.execute("SELECT * FROM %s;" % table).fetchall()

        rowCount = 0
        # For every row, separate the results of the query above
        for eachRow in allRows:
            print("ROW#", str(rowCount))
            rowCount = rowCount+1
            attributes = []

            values = []            
            #Look at each attribute and evaluate the type
            for attribute in eachRow:
                if isinstance(attribute, int):
                    values.append(attribute)
                elif attribute is None:
                    values.append(None)
                elif isinstance(attribute, str):
                    #attribute = "'" + attribute + "'"
                    values.append(attribute)
            print("LENGTH OF",len(values))
            print(values)
            #Create an ID that doesn't contain any numbers
            strings = []
            digits = str(values[0])
            for digit in digits:
                strings.append(alpha[digit])
            string_id = "".join(strings) #THIS IS THE STRING_ID
            values.append(string_id)
            statement = 'INSERT OR IGNORE INTO User VALUES ({},{},{},{},{},{});'.format(*values)+"\n"
            with open('C:/Users/PoisonTree/Documents/CDM_455/Final/datainsert.txt', 'a', encoding='utf-8') as f:
                f.write(statement)
            print("STATEMENT==",statement)
            # values = ", ".join(values)
            #newstring = " ".join(statement)

# Open a connection to database
conn = sqlite3.connect("csc455_Final.db")

# Request a cursor from the database
cursor = conn.cursor()


cursor.execute(" ")
start = time.time()
generateInsert('User')
end = time.time()
print("The amount of time it took:", end-start)

# Finalize inserts and close the connection to the database
conn.commit()
conn.close()


    
