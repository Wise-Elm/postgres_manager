#! /usr/bin/python3.10
# -*- coding: utf-8 -*-

"""
Author:
    Graham Steeds

Context:
    Provide a simplistic tool for interfacing with a Postgres database.

Description:
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

Attributes:
    CON_SLEEP: Seconds between database connection attempts.
    MAX_ATTEMPTS: Default number of attempts when trying to connect with database.
    SELECT_RESULTS: Number of db tables rows to print to screen with select statement.

Composition Attributes:
    Line length = 88 characters.
    
Python Development Version:
    3.10
    Probably works with any Python 3 version.
"""

import time
import psycopg2
from functools import wraps
from time import perf_counter

CON_SLEEP = 2  # Seconds between connection attempts.
MAX_ATTEMPTS = 4  # Max attempts when connecting to database.
SELECT_RESULTS = 5  # Number of db tables rows to print to screen with select statement.


def _timed(fn):
    """Function timer.

    Args:
        fn(function): Function to time.

    Returns:
        inner(function): Result from method called by inner function.

    Prints:
        Elapsed function time message.
    """

    @wraps(fn)
    def inner(*args, **kwargs):
        start = perf_counter()
        result = fn(*args, **kwargs)
        end = perf_counter()
        elapsed = end - start

        print('DBManager took {0:.6f}s to instantiate .'.format(elapsed))

        return result

    return inner


class DBManagerError(RuntimeError):
    """Base class for exceptions arising from DBManager."""


class DBManager:
    """Handler for Postgres database connections.

    Allows the execution of CREATE TABLE, INSERT INTO, and SELECT sql statements. More
    powerful statements such as CREATE DATABASE or DROP TABLE are not supported on
    purpose. Class provides simple checking for table_sql, insert_sql, and select_sql
    key/values to catch common mistakes, but is not intended to stop malicious intent.

    Args:
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

    Example:
        DBManager(connection_info=connection_info)

    Public Attributes:
        select_return: Returns a list of tuples containing the select statement results.
        usage: Returns a message describing the usage of this class.
    """

    @_timed  # Time instantiation of instance.
    def __init__(self, connection_info=None):
        """instantiate instance."""

        self._connection_info = connection_info
        if self._connection_info:
            self._create_tables_sql = True if self._connection_info['table_sql'] else None
            self._insert_sql = True if self._connection_info['insert_sql'] else None
            self._select_sql = True if self._connection_info['select_sql'] else None
        self._cursor = None  # Database cursor.
        self._connection = None  # Database connection.
        self.select_return = None
        self._automation_manager()  # Call to method that automates instance.

    def _automation_manager(self):
        """Main automation method for this class.

        Args:
            None

        Returns:
            select_result(str) or True: Result for any provided select statement or
            True.
        """

        # Display usage message.
        if self._connection_info is None:
            self.usage()
            return

        # Should a commit to db be made.
        # True = Yes (no errors).
        # False = No (error found).
        # None = nothing to commit.
        commit = None

        # Database cursor.
        cursor_result = self._connect(self._connection_info)
        if isinstance(cursor_result, DBManagerError):
            print('Creation of database cursor failed.')
            print(cursor_result)
            return
        else:
            self._cursor = cursor_result

        # Create tables.
        if self._create_tables_sql:
            tables_result = self._make_tables()
            if isinstance(tables_result, DBManagerError):
                print('Creating tables failed.')
                commit = False
                print(tables_result)
            else:
                commit = True

        # Insert.
        if self._insert_sql:
            insert_result = self._insert()
            if isinstance(insert_result, DBManagerError):
                print('Insert statement failed.')
                commit = False
                print(insert_result)
            else:
                commit = True

        # Select.
        if self._select_sql:
            select_result = self._select()
            if isinstance(select_result, DBManagerError):
                print('Insert statement failed.')
                commit = False
                print(select_result)
            else:
                self.select_return = select_result

        # Commit to database.
        if commit:
            result = self._commit()
            if isinstance(result, DBManagerError):
                print('Commit to database failed.')
                print(result)
            elif result is True:
                print('Commit successful.')
        elif commit is False:
            print('Database commit canceled. No changes made.')
        else:
            print('Nothing to commit.')

        # Disconnect from database.
        self._disconnect()

        return select_result or True

    def _commit(self):
        """Commit to database.

        Commit SQL statements stored in self.connection.

        Args:
            None

        Returns:
            result (DBManagerError or True):
                DBManagerError when error.
                True when commit successful.
        """

        result = None

        try:
            print('Attempting commit to database...')
            self._connection.commit()
            result = True
        except Exception as exc:
            result = DBManagerError(exc)
        finally:
            return result

    def _connect(self, con_info):
        """Establish database connection.

        Args:
            con_info(dict): Database connection attributes.

        Returns:
            cur(obj): Database connection object.
        """

        if con_info is None:
            msg = f'Missing connection information.'
            return DBManagerError(msg)

        cur = None  # Database cursor.
        con_attempt = 1  # Track connection attempts.
        while cur is None and con_attempt <= MAX_ATTEMPTS:

            con_exc = None  # Capture Exceptions, if any.

            try:
                self._connection = psycopg2.connect(
                    database=con_info['database'],
                    user=con_info['user'],
                    password=con_info['password'],
                    host=con_info['host'],
                    port=con_info['port']
                )
                cur = self._connection.cursor()
                msg = f"Connection to database ({con_info['database']}) established " \
                      f"on attempt {con_attempt}."
                print(msg)
            except psycopg2.OperationalError as exc:
                con_exc = exc
                msg = f"Error connecting to database ({con_info['database']}) on " \
                      f"attempt {con_attempt}."
                print(msg)
                con_attempt += 1
                time.sleep(CON_SLEEP)
            finally:
                if con_attempt == MAX_ATTEMPTS:
                    msg = f"Failed to connect with database " \
                          f"({con_info['database']}). " \
                          f"Maximum attempts reached ({con_attempt})."
                    print(msg)
                    return DBManagerError(msg, con_exc)

        return cur

    def _make_tables(self):
        """Create database tables.

        Args:
            None

        Returns:
            err(DBManagerError) or None
        """

        table_sql = self._connection_info['table_sql']
        upper = table_sql.upper()

        if 'INSERT INTO' in upper:
            msg = f"SQL for creating a table(s) must not contain an 'INSERT INTO' " \
                  f"statement."
            err = DBManagerError(msg)
            return err
        elif 'CREATE TABLE' not in upper:
            msg = f"SQL for creating a table(s) must contain a 'CREATE TABLE' " \
                  f"statement."
            err = DBManagerError(msg)
            return err

        try:
            self._cursor.execute(table_sql)
            msg = 'Queue for Table creation SQL successful.'
            print(msg)
        except BaseException as exc:
            msg = f'Error creating tables. Ref: {exc}.'
            err = DBManagerError(msg)
            return err

        return None

    def _insert(self):
        """Execute insert sql.

        Args:
            None

        Returns:
            err(DBManagerError) or None
        """

        insert_sql = self._connection_info['insert_sql']
        upper = insert_sql.upper()

        if 'SELECT' in upper:
            msg = f"SQL for insert must not contain a 'SELECT' statement."
            err = DBManagerError(msg)
            return err
        elif 'INSERT INTO' not in upper:
            msg = f"SQL for insert statement must contain a 'INSERT INTO'."
            err = DBManagerError(msg)
            return err

        try:
            self._cursor.execute(insert_sql)
            msg = 'Queue for Insert SQL successful.'
            print(msg)
        except BaseException as exc:
            msg = f"Error with 'INSERT' statement. Ref: {exc}."
            err = DBManagerError(msg)
            return err

        return None

    def _select(self):
        """Execute select statement.

        Args:
            None

        Returns:
            err(DBManagerError) or result(List(tuples)) containing the desired db rows.
        """

        select_sql = self._connection_info['select_sql']
        upper = select_sql.upper()

        if 'CREATE' in upper:
            msg = f"SQL for insert must not contain a 'CREATE' statement."
            err = DBManagerError(msg)
            return err
        elif 'SELECT' not in upper:
            msg = f"SQL for insert statement must contain a 'SELECT'."
            err = DBManagerError(msg)
            return err
        elif 'INSERT INTO' in upper:
            msg = f"SQL for insert statement must not contain an 'INSERT INTO' " \
                  f"statement."
            err = DBManagerError(msg)
            return err

        try:
            self._cursor.execute(select_sql)
            print('Queue for Select SQL successful.')
            result = self._cursor.fetchall()
            print(f'\nDisplaying first {SELECT_RESULTS} select statement results...')
            print(str(result[:SELECT_RESULTS + 1]) + '\n')
        except BaseException as exc:
            msg = f"Error with 'SELECT' statement. Ref: {exc}."
            err = DBManagerError(msg)
            return err

        return result

    def _disconnect(self):
        """Disconnect from database."""

        if self._cursor:
            self._cursor.close()
            msg = f"Connection to database ({self._connection_info['database']}) " \
                  f"terminated."
            print(msg)

    def usage(self):
        """Display usage message."""

        print(self.__class__.__doc__)


def main():
    """Run DBManager class."""

    db = DBManager()
    print(db.usage())


if __name__ == '__main__':

    main()
