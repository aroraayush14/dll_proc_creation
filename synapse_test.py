import os
from connection import connect

def get_table_sql_query(schema_name):
    # ... (your existing implementation remains unchanged)

def create_table_ddl_files(schema_name, cursor):
    table_sql_query = get_table_sql_query(schema_name)
    cursor.execute(table_sql_query)
    table_rows = cursor.fetchall()

    ddl_directory = f"{schema_name}_ExternalTables_DDL"
    os.makedirs(ddl_directory, exist_ok=True)

    for row in table_rows:
        table_name, column_name, data_type, char_max_length, derived_column, collation_name, location = row
        data_source = "hub" if schema_name[:3].lower() == "hub" else "pub"

        ddl = f"CREATE EXTERNAL TABLE {schema_name}.{table_name} (\n"
        ddl += f"    {derived_column},\n"
        ddl += f"    DATA_SOURCE = {data_source}\n"
        ddl += f"    LOCATION = '{location}'\n"
        ddl += "    FILE_FORMAT = [SynapseParquetFormat]\n"
        ddl += ");"

        file_path = os.path.join(ddl_directory, f"{table_name}.sql")

        with open(file_path, "w") as ddl_file:
            ddl_file.write(ddl)

# Take input for schema
schema_name = input('Enter Schema Name: ')

# Get table SQL query and execute
cursor = connect.cursor()
create_table_ddl_files(schema_name, cursor)
