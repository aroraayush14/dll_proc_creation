https://www.oracle.com/database/technologies/instant-client/winx64-64-downloads.html

# Extract Table Information from Oracle Database

## Introduction
This Python script connects to an Oracle database and retrieves information about specified tables, including column names, data types, data length, and whether columns are nullable. The script then writes this information to a CSV file.

## Prerequisites
- Oracle Database
- Oracle Instant Client installed
- `config.py` with Oracle database configuration (`oracle_config`)
- Python environment with `oracledb` library

## Setup
1. Install Oracle Instant Client.
2. Update the `config.py` file with Oracle database configuration.
3. Install the `oracledb` library.
   ```bash
   pip install oracledb
```