import os
import pyodbc
from datetime import datetime
from config import synapse_config


def execute_sql_scripts(script_directories, log_path):
    # Connection string
    db_config = synapse_config
    connection_str = f'DRIVER=ODBC Driver 17 for SQL Server;SERVER={db_config['server']};DATABASE={db_config['database']};UID={db_config['username']};PWD={db_config['password']}'

    # Create a connection
    connection = pyodbc.connect(connection_str, autocommit=True)
    cursor = connection.cursor()

    # Iterate through script directories in the specified order
    for script_directory in script_directories:
        # Iterate through SQL script files in the current directory
        for filename in os.listdir(script_directory):
            if filename.endswith(".sql"):
                file_path = os.path.join(script_directory, filename)
                script_name = os.path.splitext(filename)[0]

                # Read the SQL script content
                with open(file_path, 'r') as file:
                    sql_script = file.read()

                try:
                    # Execute the SQL script
                    cursor.execute(sql_script)
                    print(f"Script '{script_name}' executed successfully.")
                except Exception as e:
                    # Log the error to a file
                    log_error(log_path, script_name, str(e))
                    print(f"Error executing script '{script_name}': {str(e)}")

    # Close the connection
    connection.close()

def log_error(log_path, script_name, error_message):
    current_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(log_path, 'a') as log_file:
        log_file.write(f"Date and Time: {current_datetime}\nScript: {script_name}\nError: {error_message}\n\n")


# Example usage
temp_tables_directory = 'path/to/temp_tables'
tables_directory = 'path/to/tables'
stored_procs_directory = 'path/to/stored_procs'
log_file_path = 'path/to/error.log'

# Specify the order of script directories
script_directories = [temp_tables_directory, tables_directory, stored_procs_directory]

execute_sql_scripts(server_name, database_name, username, password, script_directories, log_file_path)
