import numpy as np
import pandas as pd
from time import time
import tensorflow as tf
from random import sample
from tensorflow import keras
import matplotlib.pyplot as plt
from model.model_base_class import ModelBaseClass


class TFModel(ModelBaseClass):
    def __init__(self,load_data=False):
        super().__init__(verbose=1)
        if load_data:
            self.load_train_test()
        self.compile_params = {'loss' : 'mse',
                    'optimizer' : tf.train.RMSPropOptimizer(0.001),
                    'metrics' : ['accuracy']}
        self.model_json_path = 'model/model.json'
        self.model_weights_path = 'model/model.h5'

    def add_predictions_to_db(self,method,chunksize=1000):

        if method == 'append':
            option = 'AND model_development_prediction IS NULL'
        elif method == 'overwrite':
            option = ''
        replacements = {'{option}': option}
        idxs = self.get_list('idxs_to_predict',replacements=replacements)
        shape = self.get_value('number_of_features')
        num_days = self.model_params['num_days']
        self.interval_time, self.original_time = time(), time()
        idx_processed = 0
        total = len(idxs)
        idx_chunks = [idxs[x:x+chunksize] for x in range(0,total,chunksize)]

        for idx_chunk in idx_chunks:
            pred_data = np.empty((chunksize,shape))
            for num,idx in enumerate(idx_chunk):
                replacements = {'{idx}':idx,'{num_days}':num_days}
                df = self.get_df('model_point',replacements=replacements)
                df = df.sort_values('date')
                pred_data[num] = self.transform(df)
                idx_processed += 1

            preds = self.model.predict_proba(pred_data)**(1/self.model_params['target_power'])
            update_query = ''
            for idx,pred in zip(idx_chunk,preds):
                update_query += '''
                UPDATE model.combined_data
                SET model_development_prediction = {0}, modified_date = '{1}'
                WHERE idx = {2};
                '''.format(pred[0],pd.Timestamp.now(),idx)
            self.execute_query(update_query)
            self.status_update(idx_processed,total)

    def random_undersampling(self,x,y):
        # there is a high number of points that have
        zero_percentage = self.model_params['zero_target_percentage']
        less_than_5_percentage = self.model_params['less_than_5_percentage']

        zeroes = set([num for num,value in enumerate(y) if value==0])
        idxs_to_remove = set(sample(zeroes,int(len(zeroes)*(1-zero_percentage))))

        zero_to_five = set([num for num,value in enumerate(y) if (value > 0 and value < 0.05)])
        idxs_to_remove = idxs_to_remove.union(set(sample(zero_to_five,int(len(zero_to_five)*(1-less_than_5_percentage)))))

        mask = [num not in idxs_to_remove for num in range(len(y))]

        return x[mask], y[mask]

    def build_model(self):

        self.model = keras.Sequential([
            keras.layers.Dense(64, activation=tf.nn.relu,
                       input_shape=(self.X_train.shape[1],)),
            keras.layers.Dense(64, activation=tf.nn.relu),
            keras.layers.Dense(1, activation=tf.nn.tanh)
            ])

        self.model.compile(**self.compile_params)

    def fit_model(self):

        self.y_train_modified = self.y_train**(self.model_params['target_power'])
        self.model.fit(self.X_train, self.y_train_modified, epochs=20, verbose=1)

    def save_model(self):
        # serialize model to JSON
        model_json = self.model.to_json()
        with open(self.model_json_path,"w") as json_file:
            json_file.write(model_json)
        # serialize weights to HDF5
        self.model.save_weights(self.model_weights_path)
        print("Saved model to disk")

    def load_model(self):
        print('Loading model from disk')
        json_file = open(self.model_json_path, 'r')
        loaded_model_json = json_file.read()
        json_file.close()
        self.model = tf.keras.models.model_from_json(loaded_model_json)
        # load weights into new model
        self.model.load_weights(self.model_weights_path)
        print("Loaded model from disk")
        self.model.compile(**self.compile_params)



def plot_target_hist(data):
    pd.DataFrame(data).hist()
    plt.show()

if __name__ == '__main__':

    # training a model
    # tfm = TFModel(load_data=True)
    # tfm.build_model()
    # tfm.fit_model()
    # tfm.save_model()

    # using a model
    tfm = TFModel()
    tfm.load_model()
    tfm.add_predictions_to_db(method='overwrite',chunksize=1000)
    # tfm.add_predictions_to_db(method='append',chunksize=1000)


    # X_train = tfm.X_train
    # y_train = tfm.y_train
    # train_idx = tfm.train_idx
    #
    # # x_resampled,y_resampled = tfm.random_undersampling(X_train,y_train)
    # # print("Training set: {}".format(X_train.shape))  # 404 examples, 13 features
    #
    # # plot_target_hist(y_resampled)
    # # plot_target_hist(y_train)
    #
    # model_path = 'model/model.h5'
    # # # model = tf.keras.models.load_model(model_path)
    # model = tfm.build_model(X_train)
    # y_train = y_train**(tfm.model_params['target_power'])
    # model.fit(X_train, y_train, epochs=20, verbose=1)
    #
    # y_test = tfm.y_test
    # X_test = tfm.X_test
    # test_idx = tfm.test_idx
    # print('loss --- acc')
    # print(model.evaluate(X_test, y_test**(tfm.model_params['target_power'])))
    #
    # # serialize model to JSON
    # model_json = model.to_json()
    # with open("model.json","w") as json_file:
    #     json_file.write(model_json)
    # # serialize weights to HDF5
    # model.save_weights("model.h5")
    # print("Saved model to disk")



    # test_saved_model
    # load json and create model
    # print('Loading model from disk')
    # json_file = open('model.json', 'r')
    # loaded_model_json = json_file.read()
    # json_file.close()
    # loaded_model = tf.keras.models.model_from_json(loaded_model_json)
    # # load weights into new model
    # loaded_model.load_weights("model.h5")
    # print("Loaded model from disk")
    #
    # optimizer = tf.train.RMSPropOptimizer(0.001)
    #
    # loaded_model.compile(loss='mse',
    #             optimizer=optimizer,
    #             metrics=['accuracy'])
    #
    # df = tfm.add_predictions(loaded_model,tfm.X_train.shape[1],chunksize=1000)
