from tensorflow import keras
import tensorflow as tf
from random import sample
from model.model_base_class import ModelBaseClass
from time import time


class TFModel(ModelBaseClass):
    def __init__(self):
        super().__init__(verbose=1)

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
            pred = model.predict_proba(data_point)[0][0]

            update_query = '''
            UPDATE model.combined_data
            SET model_development_prediction = {0}
            WHERE idx = {1}
            '''.format(pred,idx)
            mbc.execute_query(update_query)
            mbc.status_update(num,total)

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
                    metrics=['mae'])
        return model

if __name__ == '__main__':

    tfm = TFModel()
    tfm.load_train_test()

    x_train = tfm.X_train
    y_train = tfm.y_train
    train_idx = tfm.train_idx

    # there is a high number of points that have
    zeroes = set([num for num,value in enumerate(y_train) if value<0.05])
    idxs_to_remove = set(sample(zeroes,int(len(zeroes)*(1-tfm.model_params['zero_target_percentage']))))
    mask = [num not in idxs_to_remove for num in range(len(y_train))]
    x_train = x_train[mask]
    y_train = y_train[mask]

    print("Training set: {}".format(x_train.shape))  # 404 examples, 13 features
    # print("Testing set:  {}".format(x_test.shape))   # 102 examples, 13 features

    model_path = 'model/scp_model.h5'
    # model = tf.keras.models.load_model(model_path)
    model = tfm.build_model(x_train)
    model.fit(x_train, y_train, epochs=2, verbose=1)

    y_test = tfm.y_test
    x_test = tfm.X_test
    test_idx = tfm.test_idx
    model.evaluate(x_test, y_test)
    model.save('model/scp_model.h5')

    # add_predictions(model)
