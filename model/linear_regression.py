from model.model_base_class import ModelBaseClass
from sklearn.linear_model import LinearRegression

# class LinearRegression(ModelBaseClass)
#
#     def __init__(self,verbose=False):
#         super().__init__(verbose=verbose)


if __name__ == '__main__':
    mbc = ModelBaseClass()

    train = mbc.get_df('model_data',replacements={'{table}':'model_development_train'})
    X_train = train.drop(['idx','target'],axis=1).values
    y_train = train['target'].values

    test = mbc.get_df('model_data',replacements={'{table}':'model_development_test'})
    X_test = test.drop(['idx','target'],axis=1).values
    y_test = test['target'].values

    lr = LinearRegression()
    lr.fit(X_train,y_train)
    print(lr.score(X_test,y_test))

    train_pred = lr.predict(X_train)
    train_idx = train.idx.values
    for pred,idx in zip(train_pred,train_idx):
        mbc.execute_query('''UPDATE model.combined_data
        SET model_development_prediction = {0}
        WHERE idx = {1}'''.format(pred,idx))

    test_pred = lr.predict(X_test)
    test_idx = test.idx.values
    for pred,idx in zip(test_pred,test_idx):
        mbc.execute_query('''UPDATE model.combined_data
        SET model_development_prediction = {0}
        WHERE idx = {1}'''.format(pred,idx))
