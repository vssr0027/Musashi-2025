import sqlite3
from datetime import datetime
from math import ceil

# Work Laptop
# db = sqlite3.connect('H:\\Projects\\maintenance.db')

# Surface Pro
db = sqlite3.connect('C:\\Projects\\Musashi\\maintenance.db')
cursor = db.cursor()

def get_quarter(month):
    """Calculates the quarter (1-4) from a given month (1-12)."""
    return ceil(month / 3)

def generate_quarter_tables(start_year, start_month):
    """Generates a list of quarterly table names from a start date up to the current date."""
    today = datetime.now()
    tables = []

    current_date = datetime(start_year, start_month, 1)

    while current_date <= today:
        year = current_date.year
        quarter = get_quarter(current_date.month)
        table_name = f"Q{quarter}_{year}"
        tables.append(table_name)

        # Move to the start of the next quarter
        if quarter == 4:
            next_month = 1
            next_year = year + 1
        else:
            next_month = (quarter * 3) + 1
            next_year = year

        current_date = datetime(next_year, next_month, 1)

    # Remove duplicates in case the loop logic adds a month multiple times
    return sorted(list(set(tables)))


# Set your starting point for data collection (e.g., Q1 2016)
START_YEAR = 2016
START_MONTH = 1  # January is the start of Q1

databases = generate_quarter_tables(START_YEAR, START_MONTH)
# print(databases) # Uncomment this to see the generated list

# For making the tables
for i in databases:
	try:
		# Use 'IF NOT EXISTS' to prevent errors if the table is already there
		cursor.execute(f"""CREATE TABLE IF NOT EXISTS {i}(EQUIPMENT integer PRIMARY KEY,
	MTBR integer, MTTR integer, DT integer, COUNT integer)""")
	except Exception as e:
		print(f'Something went wrong creating table {i}: {e}')


# The rest of your table creation logic...
# # For making the KPI table
# cursor.execute("""CREATE TABLE kpi(EQUIPMENT integer PRIMARY KEY,
# 	MTBR_ALL integer, MTBR_PREVIOUS integer, MTBR_YTD integer, MTBR_MONTH integer,
# 	MTTR_ALL integer, MTTR_PREVIOUS integer, MTTR_YTD integer, MTTR_MONTH integer,
# 	DT_ALL integer, DT_PREVIOUS integer, DT_YTD integer, DT_MONTH integer)""")

# # For making machines table
# cursor.execute("""CREATE TABLE machines(EQUIPMENT integer PRIMARY KEY, DESCRIPTION text, PLANT integer,	DEPARTMENT text, WORK_CENTER integer)""")

# # For making reports table
# cursor.execute("""CREATE TABLE reports(NOTIFICATION integer PRIMARY KEY,
# 	DATE text, DESCRIPTION text, PLANT integer, DEPARTMENT text,
# 	WORK_CENTER integer, EQUIPMENT integer, BREAKDOWN text, DOWNTIME integer,
# 	REPORTED text, START_DATE text, START_TIME text, FINISH_DATE text,
# 	FINISH_TIME integer)""")


db.commit()

db.close()
