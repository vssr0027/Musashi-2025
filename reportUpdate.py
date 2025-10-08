import sqlite3
import csv

# --- Configuration ---
# Choose the desired database path (use raw string r'...' for Windows paths)
DB_PATH = r'C:\Projects\Musashi\maintenance.db'
CSV_PATH = r'C:\Projects\Musashi\reports.csv'

# --- Main Execution ---
try:
    # Use 'with' statement for safe and automatic closing of the connection
    with sqlite3.connect(DB_PATH) as db:
        cursor = db.cursor()

        # 1. Read all report data from the CSV file
        report_data = []
        with open(CSV_PATH, 'r', newline='', encoding='utf-8') as f:
            csvreader = csv.reader(f)
            next(csvreader) # Skip the header row
            
            for row in csvreader:
                # Map the CSV columns to the required table order.
                # [0]NOTIFICATION, [1]DATE, [2]DESCRIPTION, [3]PLANT, [4]DEPARTMENT, 
                # [5]WORK_CENTER, [6]EQUIPMENT, [7]BREAKDOWN, [8]DOWNTIME, 
                # [10]REPORTED, [11]START_DATE, [12]START_TIME, [13]FINISH_DATE, [14]FINISH_TIME
                # Note: We skip row[9] in the CSV list as per your indexing (REPORTED is [10])
                report_record = (
                    row[0], row[1], row[2], row[3], row[4], row[5],
                    row[6], row[7], row[8], row[10], row[11], row[12],
                    row[13], row[14]
                )
                report_data.append(report_record)

        # 2. SQL Statement: Use INSERT OR IGNORE and parameter placeholders
        sql_insert = '''
            INSERT OR IGNORE INTO reports(
                NOTIFICATION, DATE, DESCRIPTION, PLANT, DEPARTMENT, WORK_CENTER, 
                EQUIPMENT, BREAKDOWN, DOWNTIME, REPORTED, START_DATE, START_TIME, 
                FINISH_DATE, FINISH_TIME
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        '''

        # 3. Execute bulk insert for massive performance improvement
        cursor.executemany(sql_insert, report_data)
        
        # Calculate final counts based on the total number of records processed
        total_records = len(report_data)
        rows_added = cursor.rowcount
        rows_ignored = total_records - rows_added

        # Commit all changes at once
        db.commit()

        print(f'{rows_added} Reports added with {rows_ignored} reports ignored (already existed).')

except sqlite3.Error as e:
    # Catch and report specific database errors
    print(f"\n[ERROR] A database error occurred: {e}")
except FileNotFoundError:
    # Catch file path errors
    print(f"\n[ERROR] CSV file not found at path: {CSV_PATH}")
except Exception as e:
    # Catch any other unexpected errors
    print(f"\n[FATAL ERROR] An unexpected error occurred: {e}")

# The connection is automatically closed by the 'with' statement
