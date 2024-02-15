import oracledb
import csv
from config import oracle_config

oracledb.init_oracle_client(lib_dir=r"C:\Users\aa185524\Downloads\instantclient_21_13")

def get_table_info(table_names, schema_name):
    # Get database configuration
    db_config = oracle_config

    # Construct the connection string
    connection_str = f"{db_config['username']}/{db_config['password']}@{db_config['hostname']}:{db_config['port']}/{db_config['service_name']}"

    # Connect to the Oracle database
    connection = oracledb.connect(connection_str)

    try:
        # Create a cursor
        cursor = connection.cursor()

        # Write the output to a CSV file
        csv_filename = 'table_info.csv'
        with open(csv_filename, 'w', newline='') as csvfile:
            csv_writer = csv.writer(csvfile)
            csv_writer.writerow(["Table Name", "Column Name", "Data Type", "Data Length", "Nullable"])

            # Iterate through the list of table names
            for table_name in table_names:
                # Execute the query to retrieve table information
                query = """
                    SELECT table_name, column_name, data_type, data_length, nullable
                    FROM all_tab_columns
                    WHERE table_name = :table_name
                    AND owner = :schema_name
                    """
                cursor.execute(query, table_name=table_name, schema_name=schema_name)

                # Fetch all rows for the current table
                rows = cursor.fetchall()

                # Write the rows to the CSV file
                for row in rows:
                    csv_writer.writerow(row)

        print(f"Table information has been written to {csv_filename}")

    finally:
        # Close the cursor and connection
        cursor.close()
        connection.close()

# Example usage with a list of table names
table_names_input = input("Enter multiple table names separated by commas: ")
schema_name_input = input("Enter the schema name: ")

# Convert the input string into a list of table names
table_names = [table.strip() for table in table_names_input.split(',')]

get_table_info(table_names, schema_name_input)
