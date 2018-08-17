import csv
import json
import psycopg2
import pandas as pd
from time import time
from io import StringIO
from os import environ as e
from datetime import datetime

class GeneralFunctions(object):

    def __init__(self, local=False,verbose=0,load_model_params=False):
        super().__init__()
        self.local = local
        json_file = open('src/connection.json')
        json_str = json_file.read()
        self.connection = json.loads(json_str)
        self.verbose = verbose
        self.conn = psycopg2.connect(**self.connection)
        print('Connected!')
        self.cursor = self.conn.cursor()
        if load_model_params:
            self._load_model_parameters()

    def _load_model_parameters(self):
        param_path = 'model/parameters.json'
        with open(param_path) as f:
            self.model_params = json.load(f)


    def to_table(self,df,table):

        # df = self._reorder_columns(df)
        cols = df.columns
        for idx in range(df.shape[0]):
            df_out = df[idx:idx+1]
            output = StringIO()
            df_out.to_csv(output,index=False,header=False,sep='\t')
            output.seek(0)
            contents = output.getvalue()
            try:
                self.cursor.copy_from(output,table,null="",columns=cols)
                self.conn.commit()
            except Exception as e :
                self.conn.rollback()

    def _format_query(self,query_input,replacements={}):
        '''
        Takes in a string or .sql file and optional 'replacements' dictionary.

        Returns a string containing the formatted sql query and replaces the
        keys in the replacements dictionary with their values.
        '''

        # checks if input is a file or query
        if query_input.split('.')[-1] == 'sql':
            # print('Reading .sql File')
            f = open(query_input,'r')
            # reading files with a guillemet », add an uncessary Â to the string
            query = f.read().replace('Â','')
            f.close()
        else:
            query = query_input
        if replacements:
            for key,value in replacements.items():
                query = query.replace(key,str(value))
        return query

    def get_value(self,query_input,symbol_id=None,replacements={}):
        replacements['{symbol_id}'] = symbol_id
        query_input = 'queries/get_value/{0}.sql'.format(query_input)
        query = self._format_query(query_input,replacements)
        self.cursor.execute(query)
        return self.cursor.fetchone()[0]

    def get_list(self,query_input,symbol_id=None,replacements={}):
        replacements['{symbol_id}'] = symbol_id
        query_input = 'queries/get_list/{0}.sql'.format(query_input)
        query = self._format_query(query_input,replacements)
        self.cursor.execute(query)
        output = self.cursor.fetchall()
        return [x[0] for x in output]

    def get_dict(self,query_input,symbol_id=None,replacements={}):
        replacements['{symbol_id}'] = symbol_id
        query_input = 'queries/get_dict/{0}.sql'.format(query_input)
        query = self._format_query(query_input,replacements)
        self.cursor.execute(query)
        return dict(self.cursor.fetchall())

    def get_df(self,query_input,symbol_id=None,replacements={}):
        '''
        Takes in a string containing either a correctly formatted SQL
        query, or filepath directed to a .sql file. Returns a pandas DataFrame
        of the executed query.
        '''

        replacements['{symbol_id}'] = symbol_id
        query_input = 'queries/get_df/{0}.sql'.format(query_input)
        query = self._format_query(query_input,replacements)
        if self.verbose:
            print ('Executing Query:\n\n',format(query,reindent=True,keyword_case='upper'))
        return pd.read_sql(query,self.conn)

    def list_tables(self):
        self.cursor.execute("""SELECT CONCAT(table_schema,'.',table_name) AS tables
            FROM information_schema.tables
            WHERE table_schema != 'pg_catalog'
            AND table_schema != 'information_schema'
            ORDER BY 1""")
        for table in self.cursor.fetchall():
            print(table[0])

    def status_update(self,percent):
        '''
        Provides an update every minute on the progress of a given function
        '''
        # Display update ever 60 seconds
        if time() > self.interval_time + 60:
            time_elapsed = time() - self.original_time
            a = int(percent/2)
            b = 50-a
            if percent == 0:
                percent = 0.5
            min_rem = int(time_elapsed/percent*(100-percent)/60)
            print ('|{0}{1}| {2}% - {3} minute(s) remaining'.format(a*'=',b*'-',str(percent),str(min_rem)))
            self.interval_time = time()

if __name__ == '__main__':
    gf = GeneralFunctions()
