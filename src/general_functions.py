import csv
import sys
import json
import psycopg2
import pandas as pd
from time import time
from io import StringIO
from os import environ as e
from datetime import datetime

class GeneralFunctions(object):

    def __init__(self, verbose=0):
        self.connection = json.loads(open('src/connection.json').read())
        self.verbose = verbose
        self.connect_to_db(self.connection)

    def verboseprint(self, *args):
        if self.verbose:
            for arg in args:
                print(arg)

    def connect_to_db(self, connection):
        try:
            self.conn = psycopg2.connect(**connection)
            self.cursor = self.conn.cursor()
            self.verboseprint('Connected!')
        except Exception as e:
            print('Not Connected, error: {0}'.format(e))

    def to_table(self, df, table):
        ''' Adds a dataframe in its entirety to a table in the db

        Parameters:
            df (DataFrame): Pandas dataframe to add to the table
            table (string): Table name to add data to
        '''

        # self.connect_to_db(self.connection)
        cols = df.columns
        for idx in range(df.shape[0]):
            df_out = df[idx:idx+1]
            output = StringIO()
            df_out.to_csv(output, index=False, header=False, sep='\t')
            output.seek(0)
            contents = output.getvalue()
            try:
                self.cursor.copy_from(output, table, null="", columns=cols)
                self.conn.commit()
            except Exception as e:
                self.verboseprint(e)
                self.conn.rollback()
        # self.conn.close()

    def execute_query(self, query):
        try:
            self.cursor.execute(query)
            self.conn.commit()
            self.verboseprint(query)
        except Exception as e:
            self.verboseprint(e)
            self.conn.rollback()

    def _format_query(self, query_input, replacements={}):
        ''' Takes in a string or .sql file and optional 'replacements' dictionary.

        Returns a string containing the formatted sql query and replaces the
        keys in the replacements dictionary with their values.
        '''

        # checks if input is a file or query
        if query_input.split('.')[-1] == 'sql':
            f = open(query_input, 'r')
            query = f.read().replace('Â', '')
            f.close()
        else:
            query = query_input
        if replacements:
            for key, value in replacements.items():
                query = query.replace(key, str(value))
        return query

    def _get(self, get_type, query_input, symbol_id, replacements):
        ''' Method used by other functions to generate and execute a query'''

        replacements['{symbol_id}'] = symbol_id
        query_input = f'queries/{get_type}/{query_input}.sql'
        query = self._format_query(query_input, replacements)
        self.cursor.execute(query)

    def get_value(self, query_input, symbol_id=None, replacements={}):
        '''Returns a value from the database

        Parameters:
            query_input (str): Name of the file located in ./queries/get_value
            symbol_id (int): If a symbol_id is needed, put it here
            replacements (dict): Replace key with value in the query
        '''
        self._get('get_value', query_input, symbol_id, replacements)
        return self.cursor.fetchone()[0]

    def get_list(self, query_input, symbol_id=None, replacements={}):
        '''Returns a list from the database

        Parameters:
            query_input (str): Name of the file located in ./queries/get_list
            symbol_id (int): If a symbol_id is needed, put it here
            replacements (dict): Replace key with value in the query
        '''
        self._get('get_list', query_input, symbol_id, replacements)
        return [x[0] for x in self.cursor.fetchall()]

    def get_dict(self, query_input, symbol_id=None, replacements={}):
        '''Returns a dict from the database. Specify two columns in the query

        Parameters:
        query_input (str): Name of the file located in ./queries/get_dict
        symbol_id (int): If a symbol_id is needed, put it here
        replacements (dict): Replace key with value in the query
        '''
        self._get('get_dict', query_input, symbol_id, replacements)
        return dict(self.cursor.fetchall())

    def get_df(self, query_input, symbol_id=None, replacements={}):
        '''
        Takes in a string containing either a correctly formatted SQL
        query, or filepath directed to a .sql file. Returns a pandas DataFrame
        of the executed query.
        '''

        replacements['{symbol_id}'] = symbol_id
        if query_input in ['combined_data','get_combined_data','get_point',
            'model_data','model_point']:
            query_input = 'queries/get_df/{0}.sql'.format(query_input)
        query = self._format_query(query_input,replacements)
        return pd.read_sql(query,self.conn)

    def list_tables(self):
        '''Print a list of user-created tables within the db'''

        self.cursor.execute("""SELECT CONCAT(table_schema,'.',table_name) AS tables
            FROM information_schema.tables
            WHERE table_schema != 'pg_catalog'
            AND table_schema != 'information_schema'
            ORDER BY 1""")
        for table in self.cursor.fetchall():
            print(table[0])

    def status_update(self, num, total):
        '''Prints an update every minute on the progress'''

        if not total:
            return
        if time() > self.interval_time + 60:
            elapsed_time = (time() - self.original_time) / 60
            percent = round(num / total * 100, 2)
            if not percent:
                percent = num / total
            a = int(percent / 2) * '='
            b = (50 - int(percent / 2)) * '-'
            min_rem = str(int((elapsed_time) / percent * (100 - percent)))
            print('\r', f'|{a}{b}| {num}/{total} ({percent:0.2f}%) - {min_rem} minute(s) remaining', end = '')
            sys.stdout.flush()
            self.interval_time = time()

if __name__ == '__main__':
    gf = GeneralFunctions(verbose=1)
    # gf.list_tables()
    gf.interval_time, gf.original_time = time(), time()
    tot = range(1000000000000)
    for i in tot:
        gf.status_update(i,len(tot))
