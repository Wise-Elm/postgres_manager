
# Author:
    Graham Steeds

# Context:
    Provide a simplistic tool for interfacing with a Postgres database.

# Description:
    Class DBManager allows the execution of CREATE TABLE, INSERT INTO, and SELECT sql
    statements. More powerful statements such as CREATE DATABASE or DROP TABLE are not
    supported on purpose. Class provides simple checking for table_sql, insert_sql, and
    select_sql key/values to catch common mistakes, but is not intended to stop
    malicious intent.
    DBManager arguments:
    connection_info(dict): DB connection specs and sql statement info.
        example:
            'database': 'test_db',  # database name
            'user': 'superman',  # username
            'password': '1234567',  # password
            'host': 'localhost',  # host name
            'port': '5432',  # port number
            'table_sql': None,  # sql statement for table creation
            'insert_sql': None,  # sql statement for insert
            'select_sql': None  # sql statement for select
        table_sql, insert_sql, and select_sql fields must only contain their
        related sql statements. Ex. select_sql should only contain a
        SELECT statement.

# Attributes:
    MAX_ATTEMPTS: Default number of attempts when trying to connect with database.
    CON_SLEEP: Seconds between database connection attempts.

# Composition Attributes:
    Line length = 88 characters.
