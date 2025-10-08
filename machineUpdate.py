#!/usr/bin/env python3

import sqlite3
import csv

# Surface Pro - Use raw string (r'...') for clean Windows paths
db = sqlite3.connect(r'C:\Projects\Musashi\maintenance.db')

# Get a cursor object
cursor = db.cursor()

# Counters to see what equipment was added/ignored
count_ignore = 0
count_add = 0

# The table creation you showed:
# CREATE TABLE machines(EQUIPMENT integer PRIMARY KEY, DESCRIPTION text, PLANT integer, DEPARTMENT text, WORK_CENTER integer)

# Equipment[0], Description[1], Location[2], Cost Center[3], Plant[4]

# Assuming Cost Center [3] is WORK_CENTER and Location [2] is DEPARTMENT
with open(r'C:\Projects\Musashi\machine list.csv') as f:
    csvreader = csv.reader(f)
    next(csvreader) # Skip the header row
    
    # Use INSERT OR IGNORE to handle duplicates directly in SQL
    sql_insert = '''INSERT OR IGNORE INTO machines(EQUIPMENT, DESCRIPTION, PLANT, DEPARTMENT, WORK_CENTER) 
                    VALUES(?,?,?,?,?)'''
    
    for row in csvreader:
        try:
            # Assuming row[3] is WORK_CENTER
            # Mapped: [0]EQUIPMENT, [1]DESCRIPTION, [4]PLANT, [2]DEPARTMENT, [3]WORK_CENTER
            cursor.execute(sql_insert, (row[0], row[1], row[4], row[2], row[3]))
            
            # Check if a row was actually inserted (optional but robust)
            if cursor.rowcount > 0:
                count_add += 1
            else:
                count_ignore += 1
                
        # Catch any other potential SQLite errors, but not IntegrityError if using INSERT OR IGNORE
        except sqlite3.Error as e:
            print(f"Error processing row {row[0]}: {e}")
            count_ignore += 1 

db.commit()

print(f'{count_add} machines were added.')
print(f'{count_ignore} machines were ignored (already existed or had an error).')

db.close()
