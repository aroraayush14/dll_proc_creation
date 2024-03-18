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



-------------------------------------------


def execute_sql_scripts(script_base_directory, log_path):
    sep_line = '-' * 100
    cursor = connect_atmco.cursor()

    for schema_dir in os.listdir(script_base_directory):
        schema_dir_path = os.path.join(script_base_directory, schema_dir)
        if os.path.isdir(schema_dir_path):
            for filename in os.listdir(schema_dir_path):
                if filename.endswith(".sql"):
                    file_path = os.path.join(schema_dir_path, filename)
                    script_name = os.path.splitext(filename)[0]
                    with open(file_path, 'r', encoding='utf-8') as file:
                        sql_script = file.read()
                        try:
                            cursor.execute(sql_script)
                            print(f"Script '{script_name}' executed successfully.")
                        except Exception as e:
                            log_error(log_path, sep_line, script_name, str(e), file_path)
                            print(f"Error executing script '{script_name}': {str(e)}")

    connect_atmco.commit()
    connect_atmco.close()


external_table_dir = 'serverless1'  # Assuming this is the base directory containing schema directories
log_file_path = 'error.log'

execute_sql_scripts(external_table_dir, log_file_path)

