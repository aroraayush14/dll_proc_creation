import os
from connection import connect

def get_table_sql_query(schema_name):
    table_sql_query = f"""SELECT 
    c.table_name,c.column_name,
    c.data_type,
    c.character_maximum_length,
    CASE 
    WHEN UPPER(c.data_type) = 'DECIMAL' THEN '['+c.column_name+']'+ ' ' + '['+c.data_type+']' + ' (' + COALESCE(CAST(c.numeric_precision AS VARCHAR(20)),'') +','+COALESCE(CAST(c.numeric_scale AS VARCHAR(20)),'')+')'
    WHEN UPPER(c.data_type) = 'VARCHAR' or UPPER(c.data_type) = 'NVARCHAR' or UPPER(c.data_type) = 'CHAR' THEN '['+c.column_name+']'+ ' ' + '['+c.data_type+']'+'('+CAST(c.character_maximum_length AS VARCHAR(20))+')'
    WHEN UPPER(c.data_type) = 'DATETIME2'  THEN '['+c.column_name+']'+ ' ' + '['+c.data_type+']'+'(7)'
    ELSE '['+c.column_name+']'+ ' ' + '['+c.data_type+']' END as derivedcolumn,
    CASE WHEN c.collation_name IS NOT NULL THEN 'COLLATE '+c.collation_name ELSE c.collation_name END AS collation_name,
    et.location
    FROM INFORMATION_SCHEMA.COLUMNS c
    LEFT JOIN sys.external_tables et
    ON TRIM(c.table_name) = TRIM(et.name)
    WHERE c.table_schema = '{schema_name}'
    ORDER BY c.table_name, c.ORDINAL_POSITION asc"""
    return table_sql_query


def export_query_result_to_csv(schema_name, cursor):
    table_sql_query = get_table_sql_query(schema_name)
    cursor.execute(table_sql_query)
    table_rows = cursor.fetchall()

    csv_directory = f"{schema_name}_CSV_Export"
    os.makedirs(csv_directory, exist_ok=True)

    csv_file_path = os.path.join(csv_directory, f"{schema_name}_query_result.csv")

    with open(csv_file_path, mode='w', newline='') as csv_file:
        csv_writer = csv.writer(csv_file)

        # Write header
        header = ['table_name', 'column_name', 'data_type', 'character_maximum_length', 'derivedcolumn', 'collation_name', 'location']
        csv_writer.writerow(header)

        # Write data
        csv_writer.writerows(table_rows)

# Take input for schema
schema_name = input('Enter Schema Name: ')

# Get table SQL query and execute
cursor = connect.cursor()
export_query_result_to_csv(schema_name, cursor)

def create_table_ddl_files(schema_name, cursor):
    table_sql_query = get_table_sql_query(schema_name)
    cursor.execute(table_sql_query)
    table_rows = cursor.fetchall()
    ddl_directory = f"{schema_name}_ExternalTables_DDL"
    os.makedirs(ddl_directory, exist_ok=True)

    for row in table_rows:
        table_name, _, _, _, derived_column, _, location = row
        data_source = "hub" if schema_name[:3].lower() == "hub" else "pub"
        
        ddl_columns = [f"    {derived_column}"]
        ddl_columns.append(f"    DATA_SOURCE = {data_source}")
        ddl_columns.append(f"    LOCATION = '{location}'")
        ddl_columns.append("    FILE_FORMAT = [SynapseParquetFormat]")

        ddl = f"CREATE EXTERNAL TABLE {schema_name}.{table_name} (\n"
        ddl += ",\n".join(ddl_columns)
        ddl += "\n);"

        file_path = os.path.join(ddl_directory, f"{table_name}.sql")
        with open(file_path, "w") as ddl_file:
            ddl_file.write(ddl)

def create_table_ddl_files_from_csv(csv_file_path):
    with open(csv_file_path, mode='r') as csv_file:
        csv_reader = csv.DictReader(csv_file)

        current_table = None
        current_columns = []

        for row in csv_reader:
            table_name = row['table_name']
            column_name = row['column_name']
            data_type = row['data_type']
            character_length = row['character_maximum_length']
            derived_column = row['derivedcolumn']
            collation_name = row['collation_name']
            location = row['location']

            if current_table is None:
                current_table = table_name
                current_columns = []

            if table_name != current_table:
                # Process the previous table and create DDL
                create_table_ddl(current_table, current_columns)

                # Reset for the new table
                current_table = table_name
                current_columns = []

            current_columns.append((column_name, data_type, character_length, derived_column, collation_name, location))

        # Process the last table
        create_table_ddl(current_table, current_columns)

def create_table_ddl(table_name, columns):
    ddl_directory = f"{table_name}_DDL"
    os.makedirs(ddl_directory, exist_ok=True)

    ddl = f"CREATE TABLE {table_name} (\n"
    for column in columns:
        ddl += f"    {column[3]},\n"
    ddl = ddl.rstrip(',\n')  # Remove the trailing comma and newline
    ddl += "\n);\n"

    ddl_file_path = os.path.join(ddl_directory, f"{table_name}_DDL.sql")
    with open(ddl_file_path, "w") as ddl_file:
        ddl_file.write(ddl)

# Specify the path to the CSV file
csv_file_path = input('Enter the path to the CSV file: ')

# Create table DDL files from CSV
create_table_ddl_files_from_csv(csv_file_path)

# Take input for schema
schema_name = input('Enter Schema Name: ')

# Get table SQL query and execute
cursor = connect.cursor()
create_table_ddl_files(schema_name, cursor)
