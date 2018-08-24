from src.general_functions import GeneralFunctions
from sklearn.preprocessing import Normalizer
import json
import pandas as pd


class ModelBaseClass(GeneralFunctions):

    def __init__(self,verbose=False):
        super().__init__(verbose=verbose)
        self._load_model_parameters()

    def _load_model_parameters(self):
        param_path = 'model/parameters.json'
        with open(param_path) as f:
            self.model_params = json.load(f)

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

    def load_train_test(self):
        train = self.get_df('model_data',replacements={'{table}':'model_development_train'})
        self.X_train = train.drop(['idx','target'],axis=1).values
        self.y_train = train['target'].values

        test = self.get_df('model_data',replacements={'{table}':'model_development_test'})
        self.X_test = test.drop(['idx','target'],axis=1).values
        self.y_test = test['target'].values
