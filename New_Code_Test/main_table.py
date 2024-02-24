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