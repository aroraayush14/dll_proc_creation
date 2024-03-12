import os
import csv
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

import os
import csv
from connection import connect

def get_table_sql_query(schema_name):
    # ... (unchanged)

def export_query_result_to_csv(schema_name, cursor):
    table_sql_query = get_table_sql_query(schema_name)
    cursor.execute(table_sql_query)
    table_rows = cursor.fetchall()

    csv_file_path = f"{schema_name}_query_result.csv"

    with open(csv_file_path, mode='w', newline='') as csv_file:
        csv_writer = csv.writer(csv_file)

        # Write header
        header = ['table_name', 'column_name', 'data_type', 'character_maximum_length', 'derivedcolumn', 'collation_name', 'location']
        csv_writer.writerow(header)

        # Write data
        csv_writer.writerows(table_rows)

def create_table_ddl_files_from_csv(csv_file_path):
    with open(csv_file_path, mode='r') as csv_file:
        csv_reader = csv.DictReader(csv_file)

        current_table = None
        current_columns = []

        for row in csv_reader:
            table_name = row['table_name']
            derived_column = row['derivedcolumn']

            if current_table is None:
                current_table = table_name
                current_columns = []

            if table_name != current_table:
                # Process the previous table and create DDL
                create_table_ddl(current_table, current_columns)

                # Reset for the new table
                current_table = table_name
                current_columns = []

            current_columns.append(derived_column)

        # Process the last table
        create_table_ddl(current_table, current_columns)

def create_table_ddl(table_name, derived_columns):
    ddl = f"CREATE TABLE {table_name} (\n"
    for column in derived_columns:
        ddl += f"    {column},\n"
    ddl = ddl.rstrip(',\n')  # Remove the trailing comma and newline
    ddl += "\n);\n"

    ddl_file_path = os.path.join(f"{table_name}_DDL.sql")
    with open(ddl_file_path, "w") as ddl_file:
        ddl_file.write(ddl)

# Take input for schema
schema_name = input('Enter Schema Name: ')
# Get table SQL query and execute
cursor = connect.cursor()
export_query_result_to_csv(schema_name, cursor)
csv_file_path = f"{schema_name}_query_result.csv"
create_table_ddl_files_from_csv(csv_file_path)
