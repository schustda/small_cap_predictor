from tensorflow import keras
import tensorflow as tf
from random import sample
from model.model_base_class import ModelBaseClass
from time import time
import pandas as pd
import matplotlib.pyplot as plt


class TFModel(ModelBaseClass):
    def __init__(self,load_data=True):
        super().__init__(verbose=1)
        if load_data:
            self.load_train_test()

    def add_predictions(self,model):

        mbc = ModelBaseClass(verbose=1)
        idxs = mbc.get_list('idxs_to_predict')
        num_days = mbc.model_params['num_days']
        mbc.interval_time, mbc.original_time = time(), time()
        total = len(idxs)
        for num,idx in enumerate(idxs):

            replacements = {'{idx}':idx,'{num_days}':num_days}
            df = mbc.get_df('model_point',replacements=replacements)
            df = df.sort_values('date')
            data_point = mbc.transform(df)
            pred = model.predict_proba(data_point)[0][0]**(1/2)

            update_query = '''
            UPDATE model.combined_data
            SET model_development_prediction = {0}
            WHERE idx = {1}
            '''.format(pred,idx)
            mbc.execute_query(update_query)
            mbc.status_update(num,total)

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
            keras.layers.Dense(1)
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
    print("Training set: {}".format(x_train.shape))  # 404 examples, 13 features

    # plot_target_hist(y_resampled)
    # plot_target_hist(y_train)

    # model_path = 'model/scp_model.h5'
    # # model = tf.keras.models.load_model(model_path)
    model = tfm.build_model(x_train)
    y_train = y_train**2
    model.fit(x_train, y_train, epochs=20, verbose=1)
    #
    y_test = tfm.y_test
    x_test = tfm.X_test
    test_idx = tfm.test_idx
    print('loss --- acc')
    print(model.evaluate(x_test, y_test**2))
    model.save('model/scp_model.h5')

    tfm.add_predictions(model)
