import pyodbc 
#cnxn = pyodbc.connect('DRIVER={ODBC Driver 18 for SQL Server};Server=atmco-adle-dev-syn-ws01.sql.azuresynapse.net;Database=adledevsynsp05;User ID=aa251149@ncr.com;Password=Iamsourpunk@2023')
connect = pyodbc.connect(Trusted_Connection= 'No',UID='aa251149@ncr.com',PWD='Iamsourpunk@2023',Authentication='ActiveDirectoryPassword',
          DATABASE='adledevsynsp05',Driver='{ODBC Driver 18 for SQL Server}',Server='atmco-adle-dev-syn-ws01.sql.azuresynapse.net')


cursor = connect.cursor()
cursor.execute("SELECT TOP 1 * FROM hub_product.dim_pid;") 
row = cursor.fetchone() 
while row:
    print (row) 
    row = cursor.fetchone()