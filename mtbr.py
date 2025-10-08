import sqlite3
import time
from datetime import datetime, timedelta
from math import floor

# --- Configuration ---
# Choose the desired database path by uncommenting one line:
# DB_PATH = r'H:\Projects\maintenance.db'
DB_PATH = r'C:\Projects\Musashi\maintenance.db'

# Define start and end dates for the time periods
NOW = datetime.now()
THIS_YEAR_START = NOW.replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0)
PREV_YEAR_START = THIS_YEAR_START - timedelta(days=365) # Approx start
PREV_YEAR_START = PREV_YEAR_START.replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0)
PREV_YEAR_END = THIS_YEAR_START - timedelta(seconds=1) # Dec 31st of previous year

# --- Helper Function: Time Difference ---
def time_difference(start_dt_str, finish_dt_str):
    """Calculates the time difference in hours between two datetime strings."""
    datetime_format = '%m/%d/%Y %H:%M:%S'
    
    # Handle the '24:00:00' legacy issue by replacing with end of day
    def safe_parse(dt_str):
        if '24:00:00' in dt_str:
            dt_str = dt_str.replace('24:00:00', '23:59:59')
        return datetime.strptime(dt_str, datetime_format)
        
    time1 = safe_parse(start_dt_str)
    time2 = safe_parse(finish_dt_str)
    
    # Return absolute difference in hours
    delta_seconds = abs((time1 - time2).total_seconds())
    return delta_seconds / 3600.0


# --- Core Logic Function: KPI Calculation ---
def calculate_kpis(cursor, machine_id, start_date_obj=None, end_date_obj=NOW):
    """
    Calculates DT, MTTR, and MTBR for a specific machine within a date range.
    
    start_date_obj: datetime object representing the start of the period.
    end_date_obj: datetime object representing the end of the period (inclusive).
    """
    
    # 1. Build the SQL Query with date filtering
    sql_base = '''
        SELECT DOWNTIME, START_DATE, START_TIME, FINISH_DATE, FINISH_TIME
        FROM REPORTS
        WHERE BREAKDOWN = 'X' AND EQUIPMENT = ?
    '''
    params = [machine_id]
    
    if start_date_obj:
        # Convert objects to required string format for filtering in SQL
        start_date_str = start_date_obj.strftime('%m/%d/%Y')
        end_date_str = end_date_obj.strftime('%m/%d/%Y')

        # NOTE: Your reports table DATE column is likely stored as text ('MM/DD/YYYY').
        # We must filter by the start date of the report (START_DATE) to accurately 
        # determine which failures belong to the period.
        sql_base += " AND START_DATE BETWEEN ? AND ? ORDER BY START_DATE, START_TIME ASC"
        params.extend([start_date_str, end_date_str])
    else:
        # For 'All Time'
        sql_base += " ORDER BY START_DATE, START_TIME ASC"


    # 2. Execute the query
    cursor.execute(sql_base, tuple(params))
    results = cursor.fetchall()

    
    # 3. Process results
    total_downtime = 0.0
    failure_count = 0
    total_operational_time = 0.0
    
    # Time of the last reported failure (used for MTBR calculation)
    last_finish_dt_str = ''

    # Get the period start time for MTBR calculation
    period_start_dt_str = f"{start_date_obj.strftime('%m/%d/%Y')} 00:00:00" if start_date_obj else None
    
    # If there are reports
    if results:
        # Time from period start to first failure (or 'All Time' start to first failure)
        first_report = results[0]
        first_start_dt_str = f"{first_report[1]} {first_report[2]}"

        if period_start_dt_str:
            # Add time from start of period to start of first failure
            time_to_first = time_difference(period_start_dt_str, first_start_dt_str)
            total_operational_time += time_to_first
        
        # Calculate time between failures and total downtime/count
        for row in results:
            downtime_minutes = row[0] if row[0] else 0
            start_date, start_time, finish_date, finish_time = row[1], row[2], row[3], row[4]
            
            # Use FINISH_DATE/TIME if available, otherwise use START_DATE/TIME
            current_finish_date = finish_date if finish_date else start_date
            current_finish_time = finish_time if finish_time else start_time
            current_finish_dt_str = f"{current_finish_date} {current_finish_time}"
            
            # MTTR and DT Calculation
            total_downtime += downtime_minutes / 60.0 # Assuming DOWNTIME is in minutes
            failure_count += 1
            
            # MTBR Calculation (Time between failure finish and next failure start)
            if last_finish_dt_str:
                time_between_failures = time_difference(last_finish_dt_str, f"{start_date} {start_time}")
                total_operational_time += time_between_failures
            
            # Update the last finish time for the next iteration
            last_finish_dt_str = current_finish_dt_str

        # Time from last failure to period end (MTBR completion)
        if last_finish_dt_str:
            period_end_dt_str = f"{end_date_obj.strftime('%m/%d/%Y %H:%M:%S')}"
            time_from_last = time_difference(last_finish_dt_str, period_end_dt_str)
            total_operational_time += time_from_last
    
    # 4. Final KPI Calculation
    
    # MTTR (Mean Time To Repair) = Total Downtime / Failure Count
    mttr_avg = total_downtime / failure_count if failure_count > 0 else 0.0
    
    # MTBR (Mean Time Between Repair) = Total Operational Time / Failure Count
    mtbr_avg = total_operational_time / failure_count if failure_count > 0 else 0.0

    return round(total_downtime, 2), round(mttr_avg, 2), round(mtbr_avg, 2)


# --- Main Execution ---
start = time.time()
print(f"Starting KPI calculation at {NOW.strftime('%Y-%m-%d %H:%M:%S')}")

try:
    # Use 'with' statement for connection safety
    with sqlite3.connect(DB_PATH) as db:
        cursor = db.cursor()

        # Get list of all equipment for the reports
        cursor.execute('''SELECT EQUIPMENT from KPI''')
        machines_to_update = [row[0] for row in cursor.fetchall()]

        # Loop through each machine and calculate KPIs for the three timeframes
        for machine in machines_to_update:
            
            # 1. ALL TIME Report (start_date=None)
            dt_all, mttr_all, mtbr_all = calculate_kpis(cursor, machine, None, NOW)
            
            # 2. PREVIOUS YEAR Report
            dt_prev, mttr_prev, mtbr_prev = calculate_kpis(cursor, machine, PREV_YEAR_START, PREV_YEAR_END)
            
            # 3. YEAR-TO-DATE (YTD) Report
            dt_ytd, mttr_ytd, mtbr_ytd = calculate_kpis(cursor, machine, THIS_YEAR_START, NOW)

            # 4. Update the KPI Table
            sql_update = '''
                UPDATE kpi SET 
                DT_ALL = ?, MTTR_ALL = ?, MTBR_ALL = ?,
                DT_PREVIOUS = ?, MTTR_PREVIOUS = ?, MTBR_PREVIOUS = ?,
                DT_YTD = ?, MTTR_YTD = ?, MTBR_YTD = ?
                WHERE EQUIPMENT = ?
            '''
            cursor.execute(sql_update, (
                dt_all, mttr_all, mtbr_all,
                dt_prev, mttr_prev, mtbr_prev,
                dt_ytd, mttr_ytd, mtbr_ytd,
                machine
            ))
            
        # Commit all updates after the loop finishes successfully
        db.commit()
        print(f"Successfully updated KPIs for {len(machines_to_update)} machines.")

except sqlite3.Error as e:
    print(f"\n[ERROR] A database error occurred: {e}")
    
# Script timer
end = time.time()
print(f"Script finished in {round(end - start, 2)} seconds.")
