import tensorflow as tf
from model.model_base_class import ModelBaseClass


if __name__ == '__main__':

    mbc = ModelBaseClass()
    mbc.load_train_test()

    x_train = mbc.X_train
    y_train = mbc.y_train
    x_test = mbc.X_test
    y_test = mbc.y_test

    model = tf.keras.models.Sequential([
      tf.keras.layers.Flatten(),
      tf.keras.layers.Dense(512, activation=tf.nn.relu),
      tf.keras.layers.Dropout(0.2),
      tf.keras.layers.Dense(10, activation=tf.nn.softmax)
    ])
    model.compile(optimizer='adam',
                  loss='sparse_categorical_crossentropy',
                  metrics=['accuracy'])

    model.fit(x_train, y_train, epochs=5, verbose=1)
    model.evaluate(x_test, y_test)
