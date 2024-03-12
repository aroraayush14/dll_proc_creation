import json

def generate_sql(primary_keys):
    sql_template = "CAST(hashbytes('sha2_256', concat({})) as VARBINARY(32)) AS [hash_key]"

    key_list = []
    for key_info in primary_keys:
        key_list.append(f"upper(coalesce([{key_info['Primary Key']}], ''))")

    concat_expression = ",'||',".join(key_list)
    return sql_template.format(concat_expression)

def main():
    with open('pk_info.json') as json_file:
        data = json.load(json_file)

        table_name = list(data.keys())[0]
        primary_keys = data[table_name]["Primary Key Information"]

        sql_statement = generate_sql(primary_keys)

        print(f"Table: {table_name}")
        print(f"Generated SQL Statement: {sql_statement}")

if __name__ == "__main__":
    main()


#json_file = 'pk_info.json'
