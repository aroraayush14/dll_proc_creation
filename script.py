import csv
import re

def extract_table_name_and_columns(file_path):
    with open(file_path, 'r') as file:
        reader = csv.DictReader(file)
        current_table_name = None
        columns = []

        for row in reader:
            table_name = row['Table Name']
            if table_name != current_table_name:
                if current_table_name:
                    yield current_table_name, columns
                current_table_name = table_name
                columns = []

            column_name = row['Column Name']
            data_type = row['Data Type']
            nullable = row['Nullable']

            

            columns.append((column_name, data_type, nullable == 'Y'))

        if current_table_name and columns:
            yield current_table_name, columns

def replace_data_types(data_type):
    # Apply data type replacements for non-temp tables
    data_type = re.sub(r'\bNUMBER\b', 'decimal(38, 0)', data_type)
    data_type = re.sub(r'\bVARCHAR2\b', 'varchar', data_type)
    data_type = re.sub(r'\bDATE\b', 'varchar(35)', data_type)
    return data_type

def generate_create_temp_table_sql(table_name, columns):
    schema_name = 'product'
    sql = f"CREATE TABLE {schema_name}.{table_name}_temp (\n"

    
    sql += '  [infa_operation_time] [varchar](35) NULL,\n'
    sql += '  [infa_operation_type] [varchar](1) NULL,\n'

    # Sort the columns alphabetically
    sorted_columns = sorted(columns, key=lambda x: x[0])

    for column_name, data_type, is_not_null in sorted_columns:
        sql += f"  [{column_name}] {replace_data_types(data_type)}"
        sql += ",\n"  # New line for each column

    # Append the additional code
    sql += '  [ingest_partition] [varchar](100) NULL,\n'
    sql += '  [ingest_channel] [varchar](100) NULL,\n'
    sql += '  [file_path] [varchar](100) NULL,\n'
    sql += '  [root_path] [varchar](100) NULL\n'
    sql += ')\n'
    sql += 'WITH\n'
    sql += '(\n'
    sql += '  DISTRIBUTION = ROUND_ROBIN,\n'
    sql += '  HEAP\n'
    sql += ')\n'
    sql += 'GO\n'

    return sql

def generate_create_table_sql(table_name, columns):
    schema_name = 'product'
    sql = f"CREATE TABLE {schema_name}.tolower({table_name}) (\n"

    # Sort the columns alphabetically
    sorted_columns = sorted(columns, key=lambda x: x[0])

    for column_name, data_type, is_not_null in sorted_columns:
        sql += f"  [{column_name}] {replace_data_types(data_type)}"
        sql += ",\n"  # New line for each column

    # Append the additional code
    sql += '  [ingest_partition] [varchar](100) NULL,\n'
    sql += '  [ingest_channel] [varchar](100) NULL,\n'
    sql += '  [file_path] [varchar](100) NULL,\n'
    sql += '  [root_path] [varchar](100) NULL,\n'
    sql += '  [pipeline_name] [varchar](100) NULL,\n'
    sql += '  [pipeline_run_id] [varchar](100) NULL,\n'
    sql += '  [pipeline_trigger_name] [varchar](100) NULL,\n'
    sql += '  [pipeline_trigger_id] [varchar](100) NULL,\n'
    sql += '  [pipeline_trigger_type] [varchar](100) NULL,\n'
    sql += '  [pipeline_trigger_date_time_utc] [datetime2](7 NULL,\n'
    sql += '  [trans_load_date_time_utc] [datetime2](7 NULL,\n'
    sql += '  [adle_transaction_code] [char](1 NULL,\n'
    sql += '  [hash_key] [varbinary](32) NULL\n'
    sql += ')\n'
    sql += 'WITH\n'
    sql += '(\n'
    sql += '  DISTRIBUTION = HASH([hash_key]),\n'
    sql += '  CLUSTERED COLUMNSTORE INDEX\n'
    sql += ')\n'
    sql += 'GO\n'

    return sql

def generate_create_proc_sql(table_name, columns):
    schema_name = 'product'
    sql = f"CREATE PROC {schema_name}.{table_name}_proc (\n"

    # Sort the columns alphabetically
    sorted_columns = sorted(columns, key=lambda x: x[0])

    for column_name, data_type, is_not_null in sorted_columns:
        sql += f"  [{column_name}] {replace_data_types(data_type)}"
        if is_not_null:
            sql += " NOT NULL"
        sql += ",\n"  # New line for each column

    # Append the additional code
    sql += '@pipeline_name [VARCHAR](100),@pipeline_run_id [VARCHAR](100),@pipeline_trigger_name [VARCHAR](100),@pipeline_trigger_id [VARCHAR](100),@pipeline_trigger_type [VARCHAR](100),@pipeline_trigger_date_time_utc [DATETIME2] AS\n'
    sql += '  [ingest_partition] [varchar](100) NULL,\n'
    sql += '  [ingest_channel] [varchar](100) NULL,\n'
    sql += '  [file_path] [varchar](100) NULL,\n'
    sql += '  [root_path] [varchar](100 NULL,\n'
    sql += '  [pipeline_name] [varchar](100 NULL,\n'
    sql += '  [pipeline_run_id] [varchar](100 NULL,\n'
    sql += '  [pipeline_trigger_name] [varchar](100 NULL,\n'
    sql += '  [pipeline_trigger_id] [varchar](100 NULL,\n'
    sql += '  [pipeline_trigger_type] [varchar](100 NULL,\n'
    sql += '  [pipeline_trigger_date_time_utc] [datetime2](7 NULL,\n'
    sql += '  [trans_load_date_time_utc] [datetime2](7 NULL,\n'
    sql += '  [adle_transaction_code] [char](1 NULL,\n'
    sql += '  [hash_key] [varbinary](32) NULL\n'
    sql += ')\n'
    sql += 'WITH\n'
    sql += '(\n'
    sql += '  DISTRIBUTION = HASH([hash_key]),\n'
    sql += '  CLUSTERED COLUMNSTORE INDEX\n'
    sql += ')\n'
    sql += 'GO\n'

    return sql

file_path = 'table_info.csv'

# Extract table name and columns
for table_name, columns in extract_table_name_and_columns(file_path):
    # Generate SQL statements and save them to files
    sql_statements_temp = generate_create_temp_table_sql(table_name, columns)
    with open(f"Temp_tables/{table_name}_temp.sql", 'w') as output_file:
        output_file.write(sql_statements_temp)

    sql_statements = generate_create_table_sql(table_name, columns)
    with open(f"Tables/{table_name}.sql", 'w') as output_file:
        output_file.write(sql_statements)

    # Generate SQL statements using generate_create_proc_sql and save them to files
    sql_statements_proc = generate_create_proc_sql(table_name, columns)
    with open(f"Stored_Procs/{table_name}_proc.sql", 'w') as output_file:
        output_file.write(sql_statements_proc)
