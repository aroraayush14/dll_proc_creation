import csv
import json

def extract_table_name_and_columns(file_path):
    with open(file_path, 'r') as file:
        reader = csv.DictReader(file)
        current_table_name = None
        columns = []

        for row in reader:
            table_name = row['Table Name'].lower()
            if table_name != current_table_name:
                if current_table_name:
                    yield current_table_name, columns
                current_table_name = table_name
                columns = []

            column_name = row['Column Name'].lower()
            data_type = row['Data Type']
            data_length = row['Data Length']
            nullable = row['Nullable']

            

            columns.append((column_name, data_type, data_length))

        if current_table_name and columns:
            yield current_table_name, columns

def replace_data_types_temp_tables(data_type, data_length):
    # Apply data type replacements for main tables
    if data_type == 'VARCHAR2':
        return f'[varchar]({data_length})'
    elif data_type == 'NUMBER':
        return '[decimal](38, 0)'
    elif data_type == 'DATE':
        return '[varchar](35)'
    else:
        return data_type

def replace_data_types(column_name, data_type, data_length):
    # Check column name for keywords
    if '_rate' in column_name or '_amount' in column_name or '_weight' in column_name or '_height' in column_name or '_length' in column_name or '_width' in column_name:
        return f'[decimal](38, 5)'
    
    # Apply data type replacements for main tables
    if data_type == 'VARCHAR2':
        return f'[varchar]({data_length})'
    elif data_type == 'NUMBER':
        return '[decimal](38, 0)'
    elif data_type == 'DATE':
        return '[varchar](35)'
    else:
        return data_type

def generate_create_temp_table_sql(table_name, columns):
    schema_name = 'trans_product_gsdb_gsdb'
    sql = f"CREATE TABLE {schema_name}.{table_name}_temp (\n"

    sql += '  [infa_operation_time] [varchar](35) NULL,\n'
    sql += '  [infa_operation_type] [varchar](1) NULL,\n'

    # Sort the columns alphabetically
    sorted_columns = sorted(columns, key=lambda x: x[0])

    for column in sorted_columns:
        column_name, data_type, data_length = column
        sql += f"  [{column_name}] {replace_data_types_temp_tables( data_type, data_length)},\n"

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
    #sql += 'GO'

    return sql


def generate_create_table_sql(table_name, columns):
    schema_name = 'trans_product_gsdb_gsdb'
    sql = f"CREATE TABLE {schema_name}.{table_name} (\n"

    for column_name, data_type, data_length in columns:
        sql += f"  [{column_name}] {replace_data_types(column_name, data_type, data_length)}"
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
    sql += '  [pipeline_trigger_date_time_utc] [datetime2](7) NULL,\n'
    sql += '  [trans_load_date_time_utc] [datetime2](7) NULL,\n'
    sql += '  [adle_transaction_code] [char](1) NULL,\n'
    sql += '  [hash_key] [varbinary](32) NULL\n'
    sql += ')\n'
    sql += 'WITH\n'
    sql += '(\n'
    sql += '  DISTRIBUTION = HASH([hash_key]),\n'
    sql += '  CLUSTERED COLUMNSTORE INDEX\n'
    sql += ')\n'
    sql += 'GO;\n'

    return sql

def extract_primary_key_info(pk_file_path, table_name):
    with open(pk_file_path, 'r') as json_file:
        data = json.load(json_file)
        if table_name in data:
            return data[table_name].get("Primary Key Information", [])
        else:
            return []
        
def generate_sql(primary_keys):
    sql = "CAST(hashbytes('sha2_256', CONCAT("
    
    for i, key_info in enumerate(primary_keys):
        column_name = key_info.get("Primary Key", "")
        sql += f"UPPER(COALESCE([{column_name}], ''))"
        if i < len(primary_keys) - 1:
            sql += ",'||',"
    
    sql += ")) AS VARBINARY(32)) AS [hash_key]"
    return sql

def generate_create_proc_sql(table_name, columns, primary_keys):
    schema_name = 'product'
    sql = f"CREATE PROCEDURE [{schema_name}].[{table_name}_proc] \n"
        # Append the additional code
    sql += '@pipeline_name [VARCHAR](100),\n'
    sql += '@pipeline_run_id [VARCHAR](100),\n'
    sql += '@pipeline_trigger_name [VARCHAR](100),\n'
    sql += '@pipeline_trigger_id [VARCHAR](100),\n'
    sql += '@pipeline_trigger_type [VARCHAR](100),\n'
    sql += '@pipeline_trigger_date_time_utc [DATETIME2],\n'
    sql += 'AS\n'
    sql += 'BEGIN TRY\n'
    sql += '--LOAD-TYPE: Incremental temp2trans\n'
    sql += 'WITH gen_hashkey as (\n'
    sql += '    SELECT\n'

    for column_info in columns:
        column_name, data_type, data_length = column_info
        sql += f"      CAST([{column_name}] AS {replace_data_types(column_name, data_type, data_length)}) AS [{column_name}],\n"
    sql += f'    FROM [trans_product_gsdb_gsdb].[{table_name}], \n'
    sql += '),\n'
    primary_key_sql = generate_sql(primary_keys)
    sql += primary_key_sql

    sql += 'rn as (\n'
    sql += '    SELECT  *, ROW_NUMBER() OVER (PARTITION BY hash_key ORDER BY \n'
    #sql += '                 last_update_timestamp DESC,\n'
    sql += '				  infa_operation_time DESC,\n'
    sql += '                infa_sortable_sequence  DESC\n'
    sql += '        ) as _ELT_ROWNUMBERED\n'
    sql += '    FROM    gen_hashkey\n'
    sql += '),\n'
    sql += 'data as (\n'
    sql += '    SELECT  *\n'
    sql += '    FROM    rn\n'
    sql += '    WHERE _ELT_ROWNUMBERED = 1\n'
    sql += ')\n'
    sql += f'MERGE INTO    [trans_product_gsdb_gsdb].[{table_name}] tgt\n'
    sql += 'USING (\n'
    sql += '    SELECT  *\n'
    sql += '    FROM    data\n'
    sql += ') src\n'
    sql += 'ON ( src.[hash_key] = tgt.[hash_key] )\n'
    sql += 'WHEN MATCHED THEN \n'
    sql += 'UPDATE SET\n'
    for column_info in columns:
        column_name, data_type, data_length = column_info
        sql += f"    [tgt].[{column_name}] = [src].[{column_name}],\n"
    sql += '    [tgt].[ingest_partition] = [src].[ingest_partition],\n'
    sql += '    [tgt].[ingest_channel] = [src].[ingest_channel],\n'
    sql += '    [tgt].[file_path] = [src].[file_path],\n'
    sql += '    [tgt].[root_path] = [src].[root_path],\n'
    sql += '    [tgt].[trans_load_date_time_utc] = GETDATE(),\n'
    sql += '    [tgt].[adle_transaction_code] = [src].[infa_operation_type],\n'
    sql += '    [tgt].[infa_operation_time]=[src].[infa_operation_time],\n'
    sql += '	[tgt].[infa_sortable_sequence]=[src].[infa_sortable_sequence],\n'
    sql += '    [tgt].[pipeline_name] = @pipeline_name,\n'
    sql += '    [tgt].[pipeline_run_id] = @pipeline_run_id,\n'
    sql += '    [tgt].[pipeline_trigger_name] = @pipeline_trigger_name,\n'
    sql += '    [tgt].[pipeline_trigger_id] = @pipeline_trigger_id,\n'
    sql += '    [tgt].[pipeline_trigger_type] = @pipeline_trigger_type,\n'
    sql += '    [tgt].[pipeline_trigger_date_time_utc] = @pipeline_trigger_date_time_utc\n'
    sql += 'WHEN NOT MATCHED THEN \n'
    sql += '    INSERT (\n'
    for column_info in columns:
        column_name, data_type, data_length = column_info
        sql += f"    [{column_name}],\n"
    sql += '    [ingest_partition], \n'
    sql += '    [ingest_channel], \n'
    sql += '    [file_path], \n'
    sql += '    [root_path], \n'       
    sql += '    [pipeline_name], \n'
    sql += '    [pipeline_run_id], \n'
    sql += '    [pipeline_trigger_name], \n'
    sql += '    [pipeline_trigger_id], \n'
    sql += '    [pipeline_trigger_type], \n'
    sql += '    [pipeline_trigger_date_time_utc], \n'
    sql += '    [trans_load_date_time_utc], \n'
    sql += '    [adle_transaction_code], \n'
    sql += '    [infa_operation_time], \n'
    sql += '    [infa_sortable_sequence], \n'
    sql += '    [hash_key] \n'
    sql += ')'
    sql += 'VALUES(\n'
    for column_info in columns:
        column_name, data_type, data_length = column_info
        sql += f"    [src].[{column_name}],\n" 
    sql += '    [src].[ingest_partition], \n'
    sql += '    [src].[ingest_channel], \n'
    sql += '    [src].[file_path], \n'
    sql += '    [src].[root_path], \n'        
    sql += '    @pipeline_name, \n'
    sql += '    @pipeline_run_id, \n'
    sql += '    @pipeline_trigger_name, \n'
    sql += '    @pipeline_trigger_id, \n'
    sql += '    @pipeline_trigger_type, \n'
    sql += '    @pipeline_trigger_date_time_utc, \n'
    sql += '    GETDATE(), \n'
    sql += '    [src].[infa_operation_type], \n'
    sql += '    [src].[infa_operation_time], \n'
    sql += '    [src].[infa_sortable_sequence], \n'
    sql += '    [src].[hash_key]\n'
    sql += '    );\n'
    sql += 'END TRY\n'
    sql += 'BEGIN CATCH\n'
    sql += '    DECLARE @db_name VARCHAR(200),\n'
    sql += '        @schema_name VARCHAR(200),\n'
    sql += '        @error_nbr INT,\n'
    sql += '        @error_severity INT,\n'
    sql += '        @error_state INT,\n'
    sql += '        @stored_proc_name VARCHAR(200),\n'
    sql += '        @error_message VARCHAR(8000),\n'
    sql += '        @created_date_time DATETIME2\n\n'
    
    sql += '    SET @db_name=DB_NAME()\n'
    sql += "    SET @schema_name=SUBSTRING (@pipeline_name, CHARINDEX('2', @pipeline_name) + 1, LEN(@pipeline_name) - CHARINDEX('2', @pipeline_name) - 3 )\n"
    sql += '    SET @error_nbr=ERROR_NUMBER()\n'
    sql += '    SET @error_severity=ERROR_SEVERITY()\n'
    sql += '    SET @error_state=ERROR_STATE()\n'
    sql += '    SET @stored_proc_name=ERROR_PROCEDURE()\n'
    sql += '    SET @error_message=ERROR_MESSAGE()\n'
    sql += '    SET @created_date_time=GETDATE()\n\n'
    
    sql += '    EXECUTE [adle_platform_orchestration].[elt_error_log_proc]\n'
    sql += '        @db_name,\n'
    sql += "        'ERROR',\n"
    sql += '        @schema_name,\n'
    sql += '        @error_nbr,\n'
    sql += '        @error_severity,\n'
    sql += '        @error_state,\n'
    sql += '        @stored_proc_name,\n'
    sql += "        'PROC',\n"
    sql += '        @error_message,\n'
    sql += '        @created_date_time,\n'
    sql += '        @pipeline_name,\n'
    sql += '        @pipeline_run_id,\n'
    sql += '        @pipeline_trigger_name,\n'
    sql += '        @pipeline_trigger_id,\n'
    sql += '        @pipeline_trigger_type,\n'
    sql += '        @pipeline_trigger_date_time_utc\n'
    sql += '        ;\n'
    sql += '    THROW;\n'
    sql += 'END CATCH\n'
    sql += ';\n'
    sql += 'GO\n'
   
    return sql


file_path = 'table_info.csv'

# Extract table name and columns
for table_name, columns in extract_table_name_and_columns(file_path):
    primary_keys = extract_primary_key_info('pk_info.json', table_name)
    # Generate SQL statements and save them to files
    sql_statements_temp = generate_create_temp_table_sql(table_name, columns)
    with open(f"Temp_tables/{table_name}_temp.sql", 'w') as output_file:
        output_file.write(sql_statements_temp)

    sql_statements = generate_create_table_sql(table_name, columns)
    with open(f"Tables/{table_name}.sql", 'w') as output_file:
        output_file.write(sql_statements)

    # Generate SQL statements using generate_create_proc_sql and save them to files
    sql_statements_proc = generate_create_proc_sql(table_name, columns, primary_keys)
    with open(f"Stored_Procs/{table_name}_proc.sql", 'w') as output_file:
        output_file.write(sql_statements_proc)
