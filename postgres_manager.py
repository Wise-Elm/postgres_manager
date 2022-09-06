#! /usr/bin/env python
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

import logging
import sys
import uuid
from logging import handlers
from functools import wraps
import time
from time import perf_counter

import psycopg2


CON_SLEEP = 2  # Seconds between connection attempts.
MAX_ATTEMPTS = 4  # Max attempts when connecting to database.
SELECT_RESULTS = 5  # Number of db tables rows to print to screen with select statement.
DEFAULT_LOG_FILENAME = 'postgres_manager.log'
DEFAULT_LOG_LEVEL = logging.INFO
RUNTIME_ID = uuid.uuid4()
STATEMENTS = (
    'INSERT',
    'DROP',
    'CREATE',
    'SELECT',
    'UPDATE',
    'DELETE',
    'TRUNCATE',
    'ALTER'
)

# Configure logging.
log = logging.getLogger()
logging.basicConfig(
    format='%(message)s',
    level=DEFAULT_LOG_LEVEL,
    stream=sys.stdout
)


def _timed(fn):
    """Time the enclosed fn and print the result on fn exit.

    Args:
        fn(function): Function to time.

    Returns:
        wrapper(function): Result from method called by wrapper function.

    Prints:
        Elapsed function time message.
    """

    @wraps(fn)
    def wrapper(*args, **kwargs):
        start = perf_counter()
        result = fn(*args, **kwargs)
        end = perf_counter()
        elapsed = end - start

        print('DBManager took {0:.6f}s to instantiate.'.format(elapsed))

        return result

    return wrapper


def usage():
    """Display usage message."""

    print(DBManager.__doc__)


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
        
    Public Methods:
        usage: Returns a message describing the usage of this class.
    """

    def __init__(self, connection_info=None):

        self.connection_info = connection_info
        self.statements = STATEMENTS
        self._cursor = None  # Database cursor.
        self._connection = None  # Database connection.

    def create(self, create_sql):
        """Create database tables.

        Args:
            create_sql(str):

        Returns:
            err(DBManagerError) or None
        """

        allowed_statement = 'CREATE'

        check = self._sql_checker(create_sql, allowed_statement)
        if not check:
            log.error(check)
            return False

        try:
            self._cursor.execute(create_sql)
            msg = f"Queue for Create SQL ({create_sql}) successful. Don't forget to " \
                  f"commit."
            log.info(msg)
        except AttributeError as exc:
            msg = f"Connection to database ({self.connection_info['database']}) " \
                  f"needs to be established before {allowed_statement} statement."
            err = DBManagerError(msg)
            log.error(err)
            return False
        except BaseException as exc:
            msg = f'Error creating tables. Ref: {exc}.'
            err = DBManagerError(msg)
            log.error(err)
            return False

        return True

    def insert(self, insert_sql):
        """Insert into database tables.

        Args:
            insert_sql(str):

        Returns:
            err(DBManagerError) or None
        """

        allowed_statement = 'INSERT'

        check = self._sql_checker(insert_sql, allowed_statement)
        if not check:
            log.error(check)
            return False

        try:
            self._cursor.execute(insert_sql)
            msg = f"Queue for Insert SQL ({insert_sql}) successful. Don't forget to " \
                  f"commit."
            log.info(msg)
        except AttributeError as exc:
            msg = f"Connection to database ({self.connection_info['database']}) " \
                  f"needs to be established before {allowed_statement} statement."
            err = DBManagerError(msg)
            log.error(err)
            return False
        except BaseException as exc:
            msg = f"Error with 'INSERT' statement. Ref: {exc}."
            err = DBManagerError(msg)
            log.error(err)
            return False

        return None

    def select(self, select_sql):
        """Select SQL statement.

        Args:
            select_sql(str):

        Returns:
            err(DBManagerError) or None
        """

        allowed_statement = 'SELECT'

        check = self._sql_checker(select_sql, allowed_statement)
        if not check:
            log.error(check)
            return False

        try:
            self._cursor.execute(select_sql)
            log.info(f'SELECT statement ({select_sql}) successful.')
            result = self._cursor.fetchall()
        except AttributeError as exc:
            msg = f"Connection to database ({self.connection_info['database']}) " \
                  f"needs to be established before {allowed_statement} statement."
            err = DBManagerError(msg)
            log.error(err)
            return False
        except BaseException as exc:
            msg = f"Error with 'SELECT' statement. Ref: {exc}."
            err = DBManagerError(msg)
            log.error(err)
            return False

        return result

    def connect(self):

        if self.connection_info is None:
            msg = f'Missing connection information.'
            log.error(DBManagerError(msg))
            return False

        self._cursor = None  # Database cursor.
        con_attempt = 1  # Track connection attempts.
        while self._cursor is None and con_attempt <= MAX_ATTEMPTS:

            con_exc = None  # Capture Exceptions, if any.

            try:
                self._connection = psycopg2.connect(
                    database=self.connection_info['database'],
                    user=self.connection_info['user'],
                    password=self.connection_info['password'],
                    host=self.connection_info['host'],
                    port=self.connection_info['port']
                )
                self._cursor = self._connection.cursor()
                msg = f"Connection to database ({self.connection_info['database']}) " \
                      f"established on attempt {con_attempt}: {time.asctime()}."
                log.info(msg)
            except psycopg2.OperationalError as exc:
                con_exc = exc
                msg = f"Error connecting to database " \
                      f"({self.connection_info['database']}) on attempt {con_attempt}."
                log.error(msg)
                con_attempt += 1
                time.sleep(CON_SLEEP)
            finally:
                if con_attempt == MAX_ATTEMPTS:
                    msg = f"Failed to connect with database " \
                          f"({self.connection_info['database']}). " \
                          f"Maximum attempts reached ({con_attempt})."
                    log.error(msg)
                    log.error(DBManagerError(con_exc))
                    return False

        return True

    def commit(self):
        """Commit to database.

        Commit SQL statements stored in self.connection.

        Args:
            None

        Returns:
            result (DBManagerError or True):
                DBManagerError when error.
                True when commit successful.
        """

        try:
            log.info('Attempting commit to database...')
            self._connection.commit()
            log.info('Commit successful.')
        except Exception as exc:
            err = DBManagerError(exc)
            log.error(err)
            log.error('Commit to database failed.')
        finally:
            return

    def _sql_checker(self, sql, allowed_statement):
        """Check for illegal sql statements.

        Args:
            sql(str): SQL code.

        Returns:
            bool(): True if no illegal statements found.

        Raises:
            Exception(DBManagerError): When illegal statement found.
        """

        if sql is not type(str):
            msg = f"{allowed_statement} statement argument must be a string."
            return DBManagerError(msg)

        sql = sql.upper()

        for statement in self.statements:
            if statement in sql and statement != allowed_statement:
                msg = f"{allowed_statement} failed. {statement} not allowed to be " \
                      f"used in same statement."

                log.error(msg)
                return DBManagerError(msg)

        return True

    def disconnect(self):
        """Disconnect from database."""

        if self._cursor:
            self._cursor.close()
            msg = f"Connection to database ({self.connection_info['database']}) " \
                  f"terminated: {time.asctime()}."
            log.info(msg)
        else:
            log.error('No database cursor to disconnect from.')


def main():

    # Configure Rotating Log.
    rotating_handler = handlers.RotatingFileHandler(
        filename=DEFAULT_LOG_FILENAME,
        maxBytes=100**3,  # 0.953674 Megabytes.
        backupCount=1
    )

    formatter = logging.Formatter(
        f'[%(asctime)s] - {RUNTIME_ID} - %(levelname)s - [%(name)s:%(lineno)s] - '
        f'%(message)s'
    )
    rotating_handler.setFormatter(formatter)

    log.addHandler(rotating_handler)
    log.setLevel(DEFAULT_LOG_LEVEL)


if __name__ == '__main__':
    main()
