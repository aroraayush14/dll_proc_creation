#This file contains the configs for Source and Target Databases
# Oracle database configuration
oracle_config = {
    'username': 'gsdb',
    'password': 'Thisis4gsdb2use**yr**23**06***',
    'hostname': 'rpc2114.daytonoh.ncr.com',
    'port': '1521',
    'service_name': 'GSDBES22'
}


# Azure Synapse Analytics configuration
synapse_config = {
    'server': 'database.windows.net',
    'database': 'database',
    'username': 'username',
    'password': 'password',
    'driver': '{ODBC Driver 18 for SQL Server}',
}
