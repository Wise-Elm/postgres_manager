
# Author:
    Graham Steeds

# Context:
    Provide a simplistic tool for interfacing with a Postgres database.

# Description:
    Class DBManager allows the execution of sql statements. Class provides
    simple checking for sql queries to catch common mistakes, but is not
    intended to stop malicious intent.

# Composition Attributes:
    Line length = 88 characters.
    
# Python Development Version:
    3.10
    Probably works with any Python 3 version.
    
# Usage Example:
    from postgres_manager import DBManager

    insert1 = "INSERT INTO employee(name, state) VALUES('Dan', 'Okay')"
    insert2 = "INSERT INTO employee(name, state) VALUES('Steve', 'Meh')"
    table = "CREATE TABLE employee(name VARCHAR(20), state VARCHAR(20))"
    select = "SELECT * FROM employee"

    connection_info = {
        'database': 'just_test',  # database name
        'user': 'me',  # username
        'password': '1234567',  # password
        'host': 'localhost',  # host name
        'port': '5432',  # port number
    }
    
    y = DBManager(connection_info, verbose=True)
    y.connect()  # Connect to database.
    y.create(table)
    y.insert(insert1)
    y.insert(insert2)
    y.commit()  # Commit changes to database.
    return = y.select(select)  # Returns a list of tuples containing the select statement results.
    y.disconnect()  # Disconnect from database.
    print(return)

## Prints to terminal:
    Connection to database (just_test) established on attempt 1: Tue Sep  6 14:13:04 2022.
    Queue for CREATE SQL (CREATE TABLE employee(name VARCHAR(20), state VARCHAR(20))) successful. Don't forget to commit.
    Queue for INSERT SQL (INSERT INTO employee(name, state) VALUES('Dan', 'Okay')) successful. Don't forget to commit.
    Queue for INSERT SQL (INSERT INTO employee(name, state) VALUES('Steve', 'Meh')) successful. Don't forget to commit.
    Queue for SELECT SQL (SELECT * FROM employee) successful. Don't forget to commit.
    Commit successful.
    Connection to database (just_test) terminated: Tue Sep  6 14:14:40 2022.
    [('Dan', 'Okay'), ('Steve', 'Meh')]
    
## Example usage causing error when database does not exist.

    connection_info = {
        'database': 'just_bad',  # bad database name
        'user': 'me',  # username
        'password': '1234567',  # password
        'host': 'localhost',  # host name
        'port': '5432',  # port number
    }

    select = "SELECT * FROM employee"

    y = DBManager(connection_info, verbose=True)
    y.connect()
    lst = y.select(select)
    y.disconnect()

## Prints to terminal:
    Error connecting to database (just_bad) on attempt 1.
    Error connecting to database (just_bad) on attempt 2.
    Error connecting to database (just_bad) on attempt 3.
    Failed to connect with database (just_bad). Maximum attempts reached (4).
    connection to server at "localhost" (127.0.0.1), port 5432 failed: FATAL:  database "just_bad" does not exist

    Connection to database (just_bad) needs to be established before SELECT statement.
    No database cursor to disconnect from.

## Example output for external log file from successful use case:
	[2022-09-06 23:41:25,099] - eb1b850c-9944-49ac-8e1e-fefbc3e09622 - INFO - [root:344] - Connection to database (just_test) established on attempt 1: Tue Sep  6 23:41:25 2022.
	[2022-09-06 23:41:25,107] - eb1b850c-9944-49ac-8e1e-fefbc3e09622 - INFO - [root:410] - Queue for INSERT SQL (INSERT INTO employee(name, state) VALUES('Dan', 'Okay')) successful.
	[2022-09-06 23:41:25,108] - eb1b850c-9944-49ac-8e1e-fefbc3e09622 - INFO - [root:410] - Queue for INSERT SQL (INSERT INTO employee(name, state) VALUES('Steve', 'Meh')) successful.
	[2022-09-06 23:41:25,109] - eb1b850c-9944-49ac-8e1e-fefbc3e09622 - INFO - [root:379] - Commit successful.
	[2022-09-06 23:41:25,111] - eb1b850c-9944-49ac-8e1e-fefbc3e09622 - INFO - [root:410] - Queue for SELECT SQL (SELECT * FROM employee) successful.
	[2022-09-06 23:41:25,111] - eb1b850c-9944-49ac-8e1e-fefbc3e09622 - INFO - [root:370] - Connection to database (just_test) terminated: Tue Sep  6 23:41:25 2022.
