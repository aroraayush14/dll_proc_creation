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
