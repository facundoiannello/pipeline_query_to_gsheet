"""
Handles every communication with any Postgres based DB

"""
import os
import sys
import logging
import pandas
import psycopg2

from psycopg2.extras import execute_values
from psycopg2.extras import DictCursor

logger = logging.getLogger(__name__)


class PostgresConnectionService:

    def __init__(self):
        logger.info('Initializing DWH service..')
        self.credentials = None
        self.connection = None

    def get_credentials(self):
        self.credentials = {
            "user": os.environ.get('redshift_user', None),
            "password": os.environ.get('redshift_pass', None),
            "host": os.environ.get('redshift_host', None),
            "port": os.environ.get('redshift_port', None),
            "database": os.environ.get('redshift_db_name', None)
        }
        return self.credentials

    def make_connection(self):
        if self.credentials is None:
            self.get_credentials()

        try:
            conn = psycopg2.connect(user=self.credentials['user'],
                                    password=self.credentials['password'],
                                    host=self.credentials['host'],
                                    port=self.credentials['port'],
                                    database=self.credentials['database'])

            self.connection = conn
        except ConnectionError as ce:
            logger.info('Credentials used => User = {}, Host = {}, Database = {}'.format(
                self.credentials['user'], self.credentials['host'], self.credentials['database']))
            logger.info('Error in making connection. Error is =>')
            logger.error(ce)
            raise Exception()

    def get_connection(self):
        if self.connection is None:
            self.make_connection()
        return self.connection

    def get_cursor(self, dict_cursor=False):
        if dict_cursor:
            return self.get_connection().cursor(cursor_factory=DictCursor)
        return self.get_connection().cursor()

    def execute_select(self, query, values=None, log=True, dict_cursor=False):
        logger.info(query) if log else None
        try:
            cursor = self.get_cursor(dict_cursor)
            cursor.execute(query, values)
            return cursor
        except Exception as e:
            logger.error(f'Error in executing select query: {e}')
            self.print_psycopg2_exception(e)
            raise
    
    def print_psycopg2_exception(self, err):
        # get details about the exception
        err_type, err_obj, traceback = sys.exc_info()

        # get the line number when exception occured
        line_num = traceback.tb_lineno

        # print the connect() error
        print ("\npsycopg2 ERROR:", err, "on line number:", line_num)
        print ("psycopg2 traceback:", traceback, "-- type:", err_type)

        # psycopg2 extensions.Diagnostics object attribute
        print ("\nextensions.Diagnostics:", err.diag)

        # print the pgcode and pgerror exceptions
        print ("pgerror:", err.pgerror)
        print ("pgcode:", err.pgcode, "\n")

    def execute_insert(self, query, values=None, log=True, commit=True):
        ''' Inserts a single row and returns results of the insertion
        :returns: dict -- with keys: row_count and message
        '''
        logger.info(query) if log else None
        try:
            cursor = self.get_cursor()
            if values is None:
                cursor.execute(query)
            else:
                cursor.execute(query, values)
            self.connection.commit() if commit else None
            result = {
                'row_count': cursor.rowcount,
                'message': cursor.statusmessage
            }
            cursor.close()
            return result
        except Exception as e:
            logger.error(f'Error in executing insert query :{e}')
            raise

    def execute_bulk_insert(self, query, values, template, log=True, commit=True, page_size=1000):
        ''' Inserts a collection of values and returns results of the insertion
        :returns: dict -- with keys: row_count, message
        '''
        logger.info(query) if log else None
        try:
            cursor = self.get_cursor()
            execute_values(cursor, query, values,
                           template=template, page_size=page_size)
            self.connection.commit() if commit else None
            cursor.close()
            result = {
                'row_count': cursor.rowcount,
                'message': cursor.statusmessage
            }
            return result
        except Exception as e:
            logger.error(f'Error in executing bulk insert query: {e}')
            raise

    def execute_query(self, query, log=True, commit=True, close_cur=True):
        aws_access_key = os.environ.get('aws_access_key')
        aws_access_secret_key = os.environ.get('aws_access_secret_key')
        if aws_access_key and aws_access_secret_key:
            disp_query = query.replace(
                aws_access_key, '********').replace(aws_access_secret_key, '********')
        else:
            disp_query = query
        logger.info(disp_query) if log else None
        try:
            cursor = self.get_cursor()
            cursor.execute(query)
            self.connection.commit() if commit else None
            cursor.close() if close_cur else None
            return cursor
        except Exception as e:
            logger.error(f'Error in executing query: {e}')
            raise
    
    def close(self):
        if self.connection is not None:
            try:
                self.connection.close()
                self.connection = None
            except Exception as e:
                logger.debug('Unable to close connection')

