import numpy as np
import pandas as pd
from sys import argv
from time import time
from random import sample
from sklearn.preprocessing import Normalizer
from sklearn.model_selection import train_test_split
from model.model_base_class import ModelBaseClass


class TrainingData(ModelBaseClass):

    def __init__(self,verbose=False):
        super().__init__(verbose=verbose)

    def _drop_and_recreate_table(self,df,table):

        table = 'model.{0}'.format(table)
        self.execute_query('DROP TABLE {0};'.format(table))

        create_table = pd.io.sql.get_schema(df,table,con=self.conn).replace('''"''',"")
        if self.verbose:
            print(create_table)
        self.execute_query(create_table)

    def get_idxs(self,category):
        replacements = {'{category}':category}
        return self.get_list('model_idxs',replacements=replacements)

    def reset_split(self,column,symbol_id=None):
        insert_query = '''
        UPDATE model.combined_data
        SET {0} = FALSE, modified_date = '{1}'
        WHERE {0} = TRUE
        '''.format(column,pd.Timestamp.now())
        if symbol_id:
            insert_query += ' AND symbol_id = {0}'.format(symbol_id)
        try:
            self.cursor.execute(insert_query)
            self.conn.commit()
        except:
            self.conn.rollback()

    def set_split(self,column,chunk,value_to_set='TRUE'):

        idx_lst = ', '.join(str(num) for num in chunk)
        insert_query = '''
        UPDATE model.combined_data
        SET {0} = {1}, modified_date = '{2}'
        WHERE idx IN ({3})
        '''.format(column,value_to_set,pd.Timestamp.now(),idx_lst)
        self.execute_query(insert_query)

    def insert_splits(self,splits,symbol_id=None,chunksize=1000):

        self.interval_time, self.original_time = time(), time()

        for column,idxs in splits.items():
            print('reseting {0}'.format(column))
            self.reset_split(column,symbol_id=symbol_id)
            print('adding splits for {0}'.format(column))
            total = len(idxs)
            chunks = np.array_split(np.array(idxs),chunksize)

            for num,chunk in enumerate(chunks):
                self.set_split(column,chunk)
                self.status_update(num*chunksize,total)

    def working_split(self,symbol_id):
        '''
        Parameters: symbol_id (int)

        Function splits the full available set of training data for a given
        symbol into a train test set. Will reset what was previously selected.
        '''
        df = self.get_df('get_combined_data',symbol_id=symbol_id)
        print('Got data for symbol_id: {0}. df shape of {1}'.format(symbol_id,str(df.shape)))

        # only include points after a certain number of days that the
        # stock has been on the market

        all_idxs = set(df.idx)
        df = df[self.model_params['num_days']:].dropna(subset=['defined_target'])
        incomplete_points = all_idxs - set(df.idx)
        print ('Dropping {0} incomplete points'.format(len(incomplete_points)))
        self.set_split('pred_eligible',incomplete_points,value_to_set='FALSE')

        train,validation = train_test_split(df.idx.tolist())
        self.insert_splits({'working_train':train,
            'working_validation':validation},symbol_id)

    def model_development_split(self):
        '''
        Takes the points selected for the working_train set and splits them
        further into model development train and test
        '''

        idxs = self.get_idxs('working_train')
        print('got idxs')
        train,test = train_test_split(idxs)

        self.insert_splits({'model_development_train':train,
            'model_development_test':test})

    def create_training_data(self,column):

        idxs = self.get_idxs(column)
        first = True
        num_days = self.model_params['num_days']
        total = len(idxs)

        self.interval_time, self.original_time = time(), time()
        for num,idx in enumerate(idxs):
            replacements = {'{idx}':idx,'{num_days}':num_days}
            df = self.get_df('model_point',replacements=replacements)
            df = df.sort_values('date')

            data_point = self.transform(df)
            data_point['idx'] = df.idx
            data_point['target'] = df.defined_target

            self.status_update(num,total)

            if first:
                self._drop_and_recreate_table(data_point,column)
                first = False

            self.to_table(data_point,'model.'+column)

if __name__ == '__main__':


    td = TrainingData(verbose=True)
    td.model_development_split()
    td.create_training_data('model_development_train')
    td.create_training_data('model_development_test')
    # td.working_train_validation()
    # df = td.pull_and_transform_point(1689)


    # symbol_ids = td.get_list('symbol_ids')
    # grp1 = [x for x in symbol_ids if not x%4]
    # grp2 = [x for x in symbol_ids if not (x+1)%4]
    # grp3 = [x for x in symbol_ids if not (x+2)%4]
    # grp4 = [x for x in symbol_ids if not (x+3)%4]
    # for symbol_id in eval(argv[1]):
    # for symbol_id in symbol_ids:
        # print(symbol_id)
        # td.working_split(symbol_id)
    # td.generate_training_data()



# FEATURES

    # X number of days before prediction
    # num_days = 100

    #MESSAGE BOARD POSTS
        # Normalize weekly data
        # Significance Factor

    #DOLLAR VOLUME
        # Normalize weekly data
        # Significance Factor

    #BREAKOUT BOARDS
        #Coming as soon as data is available

    #MOST READ BOARDS
        #Coming as soon as data is available
