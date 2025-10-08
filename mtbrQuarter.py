import sqlite3
import time
from datetime import datetime, timedelta
from math import ceil

# --- Configuration ---
# Choose the desired database path
DB_PATH = r'C:\Projects\Musashi\maintenance.db'
START_YEAR = 2016 # Define the starting year for your quarterly tables

# --- Helper Function: Quarter Generation ---
def get_quarter(month):
    """Calculates the quarter (1-4) from a given month (1-12)."""
    return ceil(month / 3)

def generate_quarter_tables(start_year):
    """Generates a list of quarterly table names and their date ranges up to the current quarter."""
    NOW = datetime.now()
    tables = []
    
    # Start on Q1 of the start year
    current_date = datetime(start_year, 1, 1)

    while True:
        year = current_date.year
        quarter = get_quarter(current_date.month)
        table_name = f"Q{quarter}_{year}"
        
        # Calculate the start date and end date of the quarter
        q_start = current_date
        
        # Determine the start of the next quarter
        if quarter == 4:
            next_month = 1
            next_year = year + 1
        else:
            next_month = (quarter * 3) + 1
            next_year = year
        
        next_quarter_start = datetime(next_year, next_month, 1)
        # End date is one second before the next quarter starts
        q_end = next_quarter_start - timedelta(seconds=1) 
        
        # Do not process future quarters
        if q_start > NOW:
             break
        
        # Cap the end date at the current time if it's the current quarter
        if q_end > NOW:
            q_end = NOW
        
        tables.append({
            'name': table_name, 
            'start': q_start, 
            'end': q_end
        })

        # Advance to the next quarter
        current_date = next_quarter_start
        
        # Break condition if we've processed up to the current quarter
        if year == NOW.year and quarter == get_quarter(NOW.month):
            break

    return tables

# --- Helper Function: Time Difference (from previous script) ---
def time_difference(start_dt_str, finish_dt_str):
    """Calculates the time difference in hours between two datetime strings."""
    datetime_format = '%m/%d/%Y %H:%M:%S'
    
    def safe_parse(dt_str):
        if '24:00:00' in dt_str:
            dt_str = dt_str.replace('24:00:00', '23:59:59')
        return datetime.strptime(dt_str, datetime_format)
        
    try:
        time1 = safe_parse(start_dt_str)
        time2 = safe_parse(finish_dt_str)
    except ValueError:
        return 0.0 # Handle case where date/time format is invalid

    delta_seconds = abs((time1 - time2).total_seconds())
    return delta_seconds / 3600.0

# --- Core Logic Function: KPI Calculation for a Single Machine/Period ---
def calculate_kpis_for_quarter(cursor, machine_id, q_start, q_end):
    """
    Calculates DT, MTTR, MTBR, and COUNT for a specific machine within a quarter.
    """
    
    # 1. Prepare Date Strings and SQL
    # SQL date format must match what's stored in REPORTS (MM/DD/YYYY)
    start_date_str = q_start.strftime('%m/%d/%Y')
    end_date_str = q_end.strftime('%m/%d/%Y')
    
    # SQL to fetch relevant reports (ordered for MTBR calculation)
    sql_query = '''
        SELECT DOWNTIME, START_DATE, START_TIME, FINISH_DATE, FINISH_TIME
        FROM REPORTS
        WHERE BREAKDOWN = 'X' AND EQUIPMENT = ?
        AND START_DATE BETWEEN ? AND ? 
        ORDER BY START_DATE, START_TIME ASC
    '''
    cursor.execute(sql_query, (machine_id, start_date_str, end_date_str))
    results = cursor.fetchall()
    
    # 2. Process results
    total_downtime = 0.0
    failure_count = 0
    total_operational_time = 0.0
    last_finish_dt_str = None
    
    period_start_dt_str = f"{q_start.strftime('%m/%d/%Y')} 00:00:00"
    period_end_dt_str = f"{q_end.strftime('%m/%d/%Y %H:%M:%S')}"
    
    if results:
        # Time from quarter start to first failure
        first_report = results[0]
        first_start_dt_str = f"{first_report[1]} {first_report[2]}"
        time_to_first = time_difference(period_start_dt_str, first_start_dt_str)
        total_operational_time += time_to_first
        
        # Calculate time between failures and total downtime/count
        for row in results:
            downtime_minutes = row[0] if row[0] else 0
            start_date, start_time, finish_date, finish_time = row[1], row[2], row[3], row[4]
            
            # Get the report's end time
            current_finish_date = finish_date if finish_date else start_date
            current_finish_time = finish_time if finish_time else start_time
            current_finish_dt_str = f"{current_finish_date} {current_finish_time}"
            
            # DT and COUNT Calculation
            total_downtime += downtime_minutes / 60.0 # Assuming DOWNTIME is in minutes
            failure_count += 1
            
            # MTBR Calculation (Time between failure finish and next failure start)
            if last_finish_dt_str:
                time_between_failures = time_difference(last_finish_dt_str, f"{start_date} {start_time}")
                total_operational_time += time_between_failures
            
            last_finish_dt_str = current_finish_dt_str

        # Time from last failure to quarter end
        if last_finish_dt_str:
            time_from_last = time_difference(last_finish_dt_str, period_end_dt_str)
            total_operational_time += time_from_last
    
    # 3. Final KPI Calculation
    mttr_avg = total_downtime / failure_count if failure_count > 0 else 0.0
    mtbr_avg = total_operational_time / failure_count if failure_count > 0 else 0.0

    return (round(total_downtime, 2), round(mttr_avg, 2), 
            round(mtbr_avg, 2), failure_count)


# --- Main Execution ---
script_start = time.time()
total_updates = 0

try:
    with sqlite3.connect(DB_PATH) as db:
        cursor = db.cursor()

        # Get list of all machine EQUIPMENT IDs from any table (e.g., Q1_2016 for full set)
        cursor.execute("SELECT EQUIPMENT FROM Q1_2016") 
        machines_to_update = [row[0] for row in cursor.fetchall()]

        # Generate all quarter names and date ranges
        quarterly_periods = generate_quarter_tables(START_YEAR)

        print(f"Processing {len(quarterly_periods)} quarterly periods for {len(machines_to_update)} machines.")

        for period in quarterly_periods:
            table_name = period['name']
            q_start = period['start']
            q_end = period['end']

            for machine in machines_to_update:
                # Calculate KPIs for the current machine and quarter
                dt, mttr, mtbr, count = calculate_kpis_for_quarter(cursor, machine, q_start, q_end)

                # Update the quarterly table
                sql_update = f'''
                    UPDATE {table_name} 
                    SET DT = ?, MTTR = ?, MTBR = ?, COUNT = ? 
                    WHERE EQUIPMENT = ?
                '''
                cursor.execute(sql_update, (dt, mttr, mtbr, count, machine))
                total_updates += 1

        db.commit()
        print(f"Successfully processed and updated {total_updates} records.")

except sqlite3.Error as e:
    print(f"\n[ERROR] A database error occurred: {e}")
    
except Exception as e:
    print(f"\n[FATAL ERROR] An unexpected error occurred: {e}")


# Script timer
script_end = time.time()
print(f"Script finished in {round(script_end - script_start, 2)} seconds.")
