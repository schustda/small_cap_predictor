import numpy as np
import pandas as pd
from sys import argv
from time import time
from random import sample,choices
from sklearn.preprocessing import Normalizer
from sklearn.model_selection import train_test_split
from src.general_functions import GeneralFunctions


class TrainingData(GeneralFunctions):

    def __init__(self):
        super().__init__(load_model_params=True)

    def _transform_set( self , name , data , scale = Normalizer()):
        data = data.values.astype(float).reshape(1,-1)
        data_scaled = scale.fit_transform(data)
        columns = ['{0}_minus_{1}_days'.format(name,len(data_scaled[0])-i) for i in range(len(data_scaled[0]))]
        return pd.DataFrame(data_scaled,columns=columns)


    def transform(self,df):

        data_point = self._transform_set('ohlc',df.ohlc_average)
        data_point = pd.concat([data_point,self._transform_set('dollar_volume',df.dollar_volume)],axis=1)
        data_point = pd.concat([data_point,self._transform_set('num_posts',df.num_posts)],axis=1)
        return data_point

    def pull_and_transform_point(self,idx):
        replacements = {'{idx}':idx,'{num_days}':self.model_params['num_days']}
        df = self.get_df('model_point',replacements=replacements)
        return df
        df = df.sort_values('date')
        return self.transform(df)
        data_point['idx'] = df.idx

    def _drop_and_recreate_table(self):
        pass

    def working_train_validation(self,symbol_id,reset=False):

        df = self.get_df('get_combined_data',symbol_id=symbol_id)
        df = df[self.model_params['buffer_days']:].dropna(subset=['defined_target'])
        train,validation = train_test_split(df.idx.tolist())
        splits = {'working_train':train,'working_validation':validation}
        for name,idxs in splits.items():
            if reset:
                insert_query = '''
                UPDATE model.combined_data
                SET {0} = FALSE, modified_date = '{1}'
                WHERE {0} = TRUE
                AND symbol_id = {2}
                '''.format(name,pd.Timestamp.now(),symbol_id)
                try:
                    self.cursor.execute(insert_query)
                    self.conn.commit()
                except:
                    self.conn.rollback()

            for idx in idxs:
                insert_query = '''
                    UPDATE model.combined_data
                    SET {0} = TRUE, modified_date = '{1}'
                    WHERE idx = {2}
                    '''.format(name,pd.Timestamp.now(),idx)
                try:
                    self.cursor.execute(insert_query)
                    self.conn.commit()
                except:
                    self.conn.rollback()


if __name__ == '__main__':


    td = TrainingData()
    # td.working_train_validation()
    # df = td.pull_and_transform_point(1689)


    symbol_ids = td.get_list('symbol_ids')
    # grp1 = [x for x in symbol_ids if not x%4]
    # grp2 = [x for x in symbol_ids if not (x+1)%4]
    # grp3 = [x for x in symbol_ids if not (x+2)%4]
    # grp4 = [x for x in symbol_ids if not (x+3)%4]
    # for symbol_id in eval(argv[1]):
    for symbol_id in symbol_ids:
        print(symbol_id)
        td.working_train_validation(symbol_id,reset=True)
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
