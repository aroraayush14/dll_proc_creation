import os
from connection import connect

def get_table_ddl(schema_name, table_name, cursor):
    ddl_query = f"""SELECT 
        c.column_name,
        c.data_type,
        c.character_maximum_length,
        c.numeric_precision,
        c.numeric_scale,
        c.collation_name
    FROM INFORMATION_SCHEMA.COLUMNS c
    WHERE c.table_schema = '{schema_name}' AND c.table_name = '{table_name}'
    ORDER BY c.ORDINAL_POSITION asc"""

    cursor.execute(ddl_query)
    rows = cursor.fetchall()

    ddl = f"CREATE TABLE {schema_name}.{table_name} (\n"
    for row in rows:
        column_name, data_type, char_max_length, numeric_precision, numeric_scale, collation_name = row
        column_definition = f"    {column_name} {data_type}"
        if char_max_length is not None:
            column_definition += f"({char_max_length})"
        elif numeric_precision is not None:
            column_definition += f"({numeric_precision},{numeric_scale})"
        if collation_name is not None:
            column_definition += f" COLLATE {collation_name}"
        ddl += column_definition + ",\n"

    ddl = ddl.rstrip(",\n") + "\n);"
    return ddl

def create_table_ddl_files(schema_name, cursor):
    table_sql_query = f"""SELECT DISTINCT c.table_name
        FROM INFORMATION_SCHEMA.COLUMNS c
        WHERE c.table_schema = '{schema_name}'"""

    cursor.execute(table_sql_query)
    table_names = [row[0] for row in cursor.fetchall()]

    ddl_directory = f"{schema_name}_DDL"
    os.makedirs(ddl_directory, exist_ok=True)

    for table_name in table_names:
        ddl = get_table_ddl(schema_name, table_name, cursor)
        file_path = os.path.join(ddl_directory, f"{table_name}_DDL.sql")

        with open(file_path, "w") as ddl_file:
            ddl_file.write(ddl)

# Take input for schema
schema_name = input('Enter Schema Name: ')

# Get table SQL query and execute
table_sql_query = get_table_sql_query(schema_name)
cursor = connect.cursor()
cursor.execute(table_sql_query)

# Create table DDL files
create_table_ddl_files(schema_name, cursor)
