
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
    
# Usage Example:
    from postgres_manager import DBManager

    select = "SELECT * FROM employee"
    insert = "INSERT INTO employee(name, state) VALUES('Shayla', 'Great')"

    x = {
        'database': 'just_test',  # database name
        'user': 'me',  # username
        'password': '1234567',  # password
        'host': 'localhost',  # host name
        'port': '5432',  # port number
        'table_sql': None,  # sql statement for table creation
        'insert_sql': insert,  # sql statement for insert
        'select_sql': select  # sql statement for select
    }
    
    y = DBManager(x)

## Prints to terminal:
    Connection to database (just_test) established on attempt 1.
    Queue for Insert SQL successful.
    Queue for Select SQL successful.

    Displaying select statement results...
    [(1, 'graham', 'Amazing'), (2, 'Shayla', 'Great')]

    Attempting commit to database...
    Commit successful.
    Connection to database (just_test) terminated.
    DBManager took 0.015147s to run.
    
## Returns a list of tuples accessable in the instance.select_return attribute:
    print(y.select_return)
    
    [(1, 'graham', 'Amazing'), (2, 'Shayla', 'Great')]
