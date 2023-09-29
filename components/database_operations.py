import pyodbc

# SQL Server connection settings
server = '102.221.36.221,9555'
database = 'CHRISTIAN_TEST'
username = 'christian'
password = 'c852456!'
driver = '{ODBC Driver 17 for SQL Server}'

def ensure_table_exists(table_name, data):
    with pyodbc.connect('DRIVER=' + driver +
                        ';SERVER=' + server +
                        ';DATABASE=' + database +
                        ';UID=' + username +
                        ';PWD=' + password) as conn:
        cursor = conn.cursor()

        # Check if table exists
        cursor.execute(
            f"SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{table_name}'")
        table_exists = cursor.fetchone()[0]

        if not table_exists:
            # Dynamically construct the CREATE TABLE statement
            column_definitions = ", ".join(
                [f"[{key}] NVARCHAR(MAX)" for key in data.keys()])
            query = f"CREATE TABLE {table_name} ({column_definitions})"
            cursor.execute(query)
            conn.commit()

def insert_into_db(table_name, data):
    ensure_table_exists(table_name, data)
    with pyodbc.connect('DRIVER=' + driver +
                        ';SERVER=' + server +
                        ';DATABASE=' + database +
                        ';UID=' + username +
                        ';PWD=' + password) as conn:
        cursor = conn.cursor()

        columns = ", ".join([f"[{key}]" for key in data.keys()])
        placeholders = ", ".join(["?"] * len(data))
        query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"

        cursor.execute(query, list(data.values()))
        conn.commit()
