import numpy as np
import pandas as pd
from time import time
import tensorflow as tf
from random import sample
from tensorflow import keras
import matplotlib.pyplot as plt
from model.model_base_class import ModelBaseClass


class TFModel(ModelBaseClass):
    def __init__(self,load_data=True):
        super().__init__(verbose=1)
        if load_data:
            self.load_train_test()

    def add_predictions(self,model,shape,chunksize=1000):

        idxs = self.get_list('idxs_to_predict')
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

            preds = model.predict_proba(pred_data)**(1/self.model_params['target_power'])
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


    def build_model(self,train_data):
        model = keras.Sequential([
            keras.layers.Dense(64, activation=tf.nn.relu,
                       input_shape=(train_data.shape[1],)),
            keras.layers.Dense(64, activation=tf.nn.relu),
            keras.layers.Dense(1, activation=tf.nn.tanh)
            ])

        optimizer = tf.train.RMSPropOptimizer(0.001)

        model.compile(loss='mse',
                    optimizer=optimizer,
                    metrics=['accuracy'])
        return model

def plot_target_hist(data):
    pd.DataFrame(data).hist()
    plt.show()

if __name__ == '__main__':

    tfm = TFModel()
    x_train = tfm.X_train
    y_train = tfm.y_train
    train_idx = tfm.train_idx

    # x_resampled,y_resampled = tfm.random_undersampling(x_train,y_train)
    # print("Training set: {}".format(x_train.shape))  # 404 examples, 13 features

    # plot_target_hist(y_resampled)
    # plot_target_hist(y_train)

    model_path = 'model/scp_model.h5'
    # # model = tf.keras.models.load_model(model_path)
    model = tfm.build_model(x_train)
    y_train = y_train**(tfm.model_params['target_power'])
    model.fit(x_train, y_train, epochs=20, verbose=1)

    y_test = tfm.y_test
    x_test = tfm.X_test
    test_idx = tfm.test_idx
    print('loss --- acc')
    print(model.evaluate(x_test, y_test**2))
    model.save('model/scp_model.h5')

    tfm.add_predictions(model,x_train.shape[1],chunksize=1000)
