#! /usr/bin/env python
# -*- coding: utf-8 -*-

"""
Author:
    Graham Steeds

Context:
    Provide a simplistic tool for interfacing with a Postgres database.

Description:
    Class DBManager allows the execution of sql statements. Class provides
    simple checking for sql queries to catch common mistakes, but is not
    intended to stop malicious intent.

Attributes:
    BASIC_STATEMENTS: SQL statements allowed when DBManager is instantiated
        with advanced_statements=False
    CON_SLEEP: Seconds between database connection attempts.
    MAX_ATTEMPTS: Default number of attempts when trying to connect with
        database.
    DEFAULT_LOG_FILENAME: Filename for log file.
    DEFAULT_LOG_LEVEL: Default console log level.
    MAX_ATTEMPTS: Max attempts when connecting to database.
    RUNTIME_ID: Uniquely generated ID for external log file.
    STATEMENTS: Tuple of common recognized SQL statements.
    VERBOSE: Log level when DBManager is instantiated in verbose mode.

Public Functions:
    usage

Public Methods for DBManager:
    alter
    connect
    commit
    create
    delete
    disconnect
    drop_database
    drop_table
    insert
    select
    truncate
    update

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


BASIC_STATEMENTS = (  # SQL statements allowed at 'basic' level.
    'INSERT',
    'CREATE',
    'SELECT'
)
CON_SLEEP = 2  # Seconds between connection attempts.
DEFAULT_LOG_FILENAME = 'postgres_manager.log'
DEFAULT_LOG_LEVEL = logging.WARNING
MAX_ATTEMPTS = 4  # Max attempts when connecting to database.
RUNTIME_ID = uuid.uuid4()
STATEMENTS = (  # All recognized SQL statements.
    'INSERT',
    'DROP TABLE',
    'DROP DATABASE',
    'CREATE',
    'SELECT',
    'UPDATE',
    'DELETE',
    'TRUNCATE',
    'ALTER'
)
VERBOSE = logging.INFO  # Log level when DBManager is instantiated in verbose mode.

# Configure logging.
log = logging.getLogger()


def _set_logging(verbose):
    """Set log level of console and setup log file.

    Args:
        verbose(bool): Log to console if True.
    """

    if verbose:
        # Setup console logging.
        logging.basicConfig(
            format='%(message)s',
            level=VERBOSE,
            stream=sys.stdout
        )

    # Configure Rotating logging to external log.
    rotating_handler = handlers.RotatingFileHandler(
        filename=DEFAULT_LOG_FILENAME,
        maxBytes=100 ** 3,  # 0.953674 Megabytes.
        backupCount=1
    )
    formatter = logging.Formatter(
        f'[%(asctime)s] - {RUNTIME_ID} - %(levelname)s - [%(name)s:%(lineno)s] - '
        f'%(message)s'
    )
    rotating_handler.setFormatter(formatter)
    log.addHandler(rotating_handler)
    log.setLevel(VERBOSE)


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

    Allows the execution of sql statements on a PostgreSQL database. Class
    provides simple checking of SQL statements to catch common mistakes, but
    is not intended to stop malicious intent.

    Args:
        connection_info(dict): DB connection specs and sql statement info.
            example:
                'database': 'test_db',  # database name
                'user': 'superman',  # username
                'password': '1234567',  # password
                'host': 'localhost',  # host name
                'port': '5432',  # port number
        verbose(Bool): OPTIONAL. Defaults to False. Select verbosity for console.
        advanced_statements(Bool). OPTIONAL. Defaults to False, only allowing
            some SQL statements to be used.
    """

    def __init__(self, connection_info, verbose=False, advanced_statements=False):

        _set_logging(verbose)  # Setup console logging.

        self._advanced_statements = advanced_statements
        self._all_statements = STATEMENTS
        self._connection_info = connection_info
        self._cursor = None  # Database cursor.
        self._connection = None  # Database connection.

    def create(self, sql):
        """Create database tables.

        Args:
            sql(str):

        Returns:
            result(Bool): True if successful, False otherwise.
        """

        allowed_statement = 'CREATE'
        result = self._attempt_sql(sql, allowed_statement)
        return result

    def insert(self, sql):
        """Insert into database tables.

        Args:
            sql(str):

        Returns:
            result(Bool): True if successful, False otherwise.
        """

        allowed_statement = 'INSERT'
        result = self._attempt_sql(sql, allowed_statement)
        return result

    def select(self, sql):
        """Select SQL statement.

        Args:
            sql(str):

        Returns:
            err(DBManagerError) or None
        """

        allowed_statement = 'SELECT'
        result = self._attempt_sql(sql, allowed_statement, return_result=True)
        return result

    def update(self, sql):
        """Update SQL statement.

        Args:
            sql(str):

        Returns:
            result(Bool): True if successful, False otherwise.
        """

        allowed_statement = 'UPDATE'
        result = self._attempt_sql(sql, allowed_statement)
        return result

    def delete(self, sql):
        """Delete SQL statement.

        Args:
            sql(str):

        Returns:
            result(Bool): True if successful, False otherwise.
        """

        allowed_statement = 'DELETE'
        result = self._attempt_sql(sql, allowed_statement)
        return result

    def truncate(self, sql):
        """Delete SQL statement.

        Args:
            sql(str):

        Returns:
            result(Bool): True if successful, False otherwise.
        """

        allowed_statement = 'TRUNCATE'
        result = self._attempt_sql(sql, allowed_statement)
        return result

    def alter(self, sql):
        """Alter SQL statement.

        Args:
            sql(str):

        Returns:
            result(Bool): True if successful, False otherwise.
        """

        allowed_statement = 'ALTER'
        result = self._attempt_sql(sql, allowed_statement)
        return result

    def drop_table(self, sql):
        """Drop table SQL statement.

        Args:
            sql(str):

        Returns:
            result(Bool): True if successful, False otherwise.
        """

        allowed_statement = 'DROP TABLE'
        result = self._attempt_sql(sql, allowed_statement)
        return result

    def drop_database(self, sql):
        """Drop database SQL statement.

        Args:
            sql(str):

        Returns:
            result(Bool): True if successful, False otherwise.
        """

        allowed_statement = 'DROP DATABASE'
        result = self._attempt_sql(sql, allowed_statement)
        return result

    def connect(self):
        """Connect to database.

        Returns:
            Bool: True if successful, False otherwise.
        """

        if self._connection_info is None:
            msg = f'Missing connection information.'
            log.error(DBManagerError(msg))
            return False

        self._cursor = None  # Database cursor.
        con_attempt = 1  # Track connection attempts.
        while self._cursor is None and con_attempt <= MAX_ATTEMPTS:

            con_exc = None  # Capture Exceptions, if any.

            try:
                self._connection = psycopg2.connect(
                    database=self._connection_info['database'],
                    user=self._connection_info['user'],
                    password=self._connection_info['password'],
                    host=self._connection_info['host'],
                    port=self._connection_info['port']
                )
                self._cursor = self._connection.cursor()
                msg = f"Connection to database ({self._connection_info['database']}) " \
                      f"established on attempt {con_attempt}: {time.asctime()}."
                log.info(msg)
            except psycopg2.OperationalError as exc:
                con_exc = exc
                msg = f"Error connecting to database " \
                      f"({self._connection_info['database']}) on attempt {con_attempt}."
                log.error(msg)
                con_attempt += 1
                time.sleep(CON_SLEEP)
            finally:
                if con_attempt == MAX_ATTEMPTS:
                    msg = f"Failed to connect with database " \
                          f"({self._connection_info['database']}). " \
                          f"Maximum attempts reached ({con_attempt})."
                    log.error(msg)
                    log.error(DBManagerError(con_exc))
                    return False

        return True

    def disconnect(self):
        """Disconnect from database."""

        if self._cursor:
            self._cursor.close()
            msg = f"Connection to database ({self._connection_info['database']}) " \
                  f"terminated: {time.asctime()}."
            log.info(msg)
        else:
            log.error('No database cursor to disconnect from.')

    def commit(self):
        """Commit changes to database."""

        try:
            self._connection.commit()
            log.info('Commit successful.')
        except Exception as exc:
            err = DBManagerError(exc)
            log.error(err)
            log.error('Commit to database failed.')
        finally:
            return

    def _attempt_sql(self, sql, check_statement, return_result=False):
        """Attempt SQL.

        Args:
            sql(str): SQL query.
            check_statement(str): Allowed SQL statement.
            return_result(bool): If True method returns SQL query result. Used for
                'SQL SELECT' statements.

        Returns:
            result or Bool: result if return_result argument is True, True if SQL
                query was successful, False otherwise.
        """

        result = None

        check = self._check_sql(sql, check_statement)
        if check != True:
            return False

        try:
            x = self._cursor.execute(sql)
            msg = f"Queue for {check_statement} SQL ({sql}) successful."
            log.info(msg)
            if return_result:  # Return the results of a SELECT statement.
                result = self._cursor.fetchall()
        except AttributeError as exc:
            msg = f"Connection to database ({self._connection_info['database']}) " \
                  f"needs to be established before {check_statement} statement."
            err = DBManagerError(msg)
            log.error(err)
            return False
        except BaseException as exc:
            msg = f'Error with {check_statement} ({sql}). Ref: {exc}.'
            err = DBManagerError(msg)
            log.error(err)
            return False

        return result or True

    def _check_sql(self, sql, check_statement):
        """Check validity of SQL.

        Args:
            sql(str): SQL query.
            check_statement(str): Allowed SQL statement.

        Returns:
            bool(): True if no illegal statements found.

        Raises:
            Exception(DBManagerError): When illegal statement found.
        """

        # Check sql statement type.
        if type(sql) is not str:
            msg = f"{check_statement} statement argument must be a string."
            return DBManagerError(msg)

        # Check if sql statement allowed in current mode; advanced or basic.
        elif not self._advanced_statements:
            if check_statement not in BASIC_STATEMENTS:
                msg = f"Error: {check_statement} statement can only be used if " \
                      f"DBManager is instantiated with advanced_statements as True."
                log.error(msg)
                return DBManagerError(msg)

        sql = sql.upper()

        # Check if statements other than argument check_statement found in sql.
        for statement in self._all_statements:
            if statement in sql and statement != check_statement:
                msg = f"{check_statement} failed. {statement} not allowed to be " \
                      f"used in same statement."
                log.error(msg)
                return DBManagerError(msg)

        return True


def main():

    usage()


if __name__ == '__main__':
    main()
