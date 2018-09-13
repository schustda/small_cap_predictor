import tensorflow as tf
from random import sample
from model.model_base_class import ModelBaseClass
from time import time

def add_predictions(model):

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
        pred = model.predict

        update_query = '''
        UPDATE model.combined_data
        SET model_development_prediction = {0}
        WHERE idx = {1}
        '''.format(idx,pred)
        mbc.execute_query(update_query)
        mbc.status_update(num,total)


if __name__ == '__main__':

    mbc = ModelBaseClass()
    mbc.load_train_test()

    x_train = mbc.X_train
    y_train = mbc.y_train
    train_idx = mbc.train_idx
    y_test = mbc.y_test
    x_test = mbc.X_test
    test_idx = mbc.test_idx

    # there is a high number of points that have
    zeroes = set([num for num,value in enumerate(y_train) if value==0])
    idxs_to_remove = set(sample(zeroes,int(len(zeroes)*(1-mbc.model_params['zero_target_percentage']))))
    mask = [num not in idxs_to_remove for num in range(len(y_train))]
    x_train = x_train[mask]
    y_train = y_train[mask]

    model_path = 'model/scp_model.h5'
    # model = tf.keras.models.load_model(model_path)

    model = tf.keras.models.Sequential([
      tf.keras.layers.Flatten(),
      tf.keras.layers.Dense(512, activation=tf.nn.relu),
      tf.keras.layers.Dropout(0.2),
      tf.keras.layers.Dense(1, activation=tf.nn.softmax)
    ])
    model.compile(optimizer='adam',
                  loss='mean_squared_error',
                  metrics=['accuracy'])
    model.fit(x_train, y_train, epochs=10, verbose=1)
    model.evaluate(x_test, y_test)
    model.save('model/scp_model.h5')

    add_predictions(model)
