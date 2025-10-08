import sqlite3
import csv
from datetime import datetime
from math import ceil

# --- Configuration ---
# Choose the desired database path by uncommenting one line:
# DB_PATH = r'H:\Projects\maintenance.db'
DB_PATH = r'C:\Projects\Musashi\maintenance.db'

# --- Helper Functions ---
def get_quarter(month):
    """Calculates the quarter (1-4) from a given month (1-12)."""
    return ceil(month / 3)

def generate_quarter_tables(start_year, start_month):
    """Generates a list of quarterly table names from a start date up to the current quarter."""
    today = datetime.now()
    tables = []

    # Calculate the current quarter's start date
    current_year = today.year
    current_quarter = get_quarter(today.month)
    
    # Set the loop start date
    current_date = datetime(start_year, start_month, 1)

    while True:
        year = current_date.year
        quarter = get_quarter(current_date.month)
        table_name = f"Q{quarter}_{year}"
        tables.append(table_name)
        
        # Stop condition: reached the current quarter
        if year == current_year and quarter == current_quarter:
            break

        # Move to the start of the next quarter
        if quarter == 4:
            next_month = 1
            next_year = year + 1
        else:
            next_month = (quarter * 3) + 1
            next_year = year

        current_date = datetime(next_year, next_month, 1)

    return tables


# --- Main Execution ---
count_add = 0
count_ignore = 0
total_machines = 0

# Set your starting point (e.g., Q1 2016)
START_YEAR = 2016
START_MONTH = 1

try:
    # Use 'with' statement for safe and automatic closing of the connection
    with sqlite3.connect(DB_PATH) as db:
        cursor = db.cursor()

        # 1. Get the list of machines (Primary Keys)
        cursor.execute("SELECT EQUIPMENT FROM machines")
        # Ensure the results are tuples, which is what executemany expects
        machines_rows = cursor.fetchall()
        total_machines = len(machines_rows)

        # 2. Get the list of quarterly tables dynamically
        databases = generate_quarter_tables(START_YEAR, START_MONTH)

        # 3. Insert machine IDs into all quarterly tables
        for table_name in databases:
            # Use INSERT OR IGNORE to handle duplicates directly in SQL
            sql_insert = f'''INSERT OR IGNORE INTO {table_name}(EQUIPMENT) VALUES(?)'''
            
            # Use executemany for high efficiency
            cursor.executemany(sql_insert, machines_rows)
            
            # Count added/ignored rows
            rows_affected = cursor.rowcount
            # total_machines - rows_affected = number of rows ignored/already existing
            count_add += rows_affected
            count_ignore += (total_machines - rows_affected)

        # Commit all changes after all insertions are complete
        db.commit() 
        
        print(f"Update complete for {len(databases)} quarterly tables:")
        print(f'{count_add} machine entries were added.')
        print(f'{count_ignore} machine entries were ignored (already existed).')

except sqlite3.Error as e:
    print(f"\n[ERROR] A database error occurred: {e}")
    # Rollback any pending changes on error
    if 'db' in locals():
        db.rollback()
