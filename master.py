import sqlite3

# --- Configuration ---
# Choose the desired database path by uncommenting one line:
# DB_PATH = 'H:\\Projects\\maintenance.db'
DB_PATH = 'C:\\Projects\\Musashi\\maintenance.db' 

# --- Database Connection and Query ---
try:
    # Using 'with' ensures the connection is closed automatically
    with sqlite3.connect(DB_PATH) as db:
        cursor = db.cursor()

        # Get a list of all table names
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        # Extract table names into a clean list for validation
        tables = [t[0] for t in cursor.fetchall()]
        
        print(f"Tables found in {DB_PATH}:")
        print(tables)
        print('-' * 30)
        print('Enter the name of a table to see its structure, or type "q" to quit.')

        # --- User Interaction Loop ---
        while True:
            answer = input('Table name: ').strip() # .strip() removes leading/trailing spaces
            
            if answer.lower() == 'q':
                break
                
            if not answer:
                continue

            # 1. Validation: Check if the table name exists in the database
            if answer not in tables:
                print(f"Error: Table '{answer}' not found. Please check the list above.")
                print('-' * 30)
                continue

            # 2. Safe Execution: Execute the PRAGMA command with the validated name
            try:
                # PRAGMA is used to get the table information (column details)
                print(f"\n--- Schema for table: {answer} ---")
                
                # We use an f-string here after validating 'answer' is a known table name
                cursor.execute(f"PRAGMA table_info('{answer}')")
                
                # PRAGMA returns: (cid, name, type, notnull, dflt_value, pk)
                for row in cursor.fetchall():
                    # Format the output for readability
                    print(f"  ID: {row[0]}, Name: {row[1]:<15}, Type: {row[2]:<8}, Primary Key: {bool(row[5])}")
                
                print('-' * 30)

            except sqlite3.Error as e:
                # Catch any unexpected database errors during PRAGMA execution
                print(f"An unexpected database error occurred: {e}")
                print('-' * 30)
                
# Catch errors that occur before or during connection
except sqlite3.Error as e:
    print(f"\n[FATAL ERROR] Could not connect to the database or retrieve data: {e}")
except Exception as e:
    print(f"\n[FATAL ERROR] An unexpected error occurred: {e}")
