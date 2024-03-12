from connection import connect

schema_name = input('Enter Schema Name: ')

table_sql_query = """SELECT 
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
from INFORMATION_SCHEMA.COLUMNS c
LEFT JOIN sys.external_tables et
ON TRIM(c.table_name) = TRIM(et.name)
where c.table_schema = 'hub_product'
-- AND (c.table_schema like 'hub_%' or c.table_schema like 'pub_%') and c.table_name is not null 
order by c.table_name, c.ORDINAL_POSITION asc"""

view_sql_query = """select * from INFORMATION_SCHEMA.VIEWS
WHERE table_schema = 'hub_product_v' 
ORDER BY table_name asc"""

cursor = connect.cursor()
cursor.execute(table_sql_query) 
row = cursor.fetchone() 
while row:
    print (row) 
    row = cursor.fetchone()
