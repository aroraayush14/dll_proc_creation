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

def get_view_sql_query(schema_name):
    view_sql_query = f"""SELECT * FROM INFORMATION_SCHEMA.VIEWS
    WHERE table_schema = '{schema_name}' 
    ORDER BY table_name asc"""
    return view_sql_query

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
    table_sql_query = get_table_sql_query(schema_name)
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
cursor = connect.cursor()
create_table_ddl_files(schema_name, cursor)
