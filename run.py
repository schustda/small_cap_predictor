from datetime import datetime as dt
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from getpass import getpass
import numpy as np
import pandas as pd
from sklearn.externals import joblib
from src.data.ihub_data import IhubData
from src.data.stock_data import StockData
from src.data.combine_data import CombineData
from src.data.training_data import TrainingData
from src.general_functions import GeneralFunctions
import smtplib
import subprocess
from sys import argv
from time import time,sleep, gmtime
import xgboost as xgb
import seaborn as sns
import matplotlib.pyplot as plt

class DailyPrediction(TrainingData):

    def __init__(self,num_days,days_avg,threshold,email_address,password):
        super().__init__(num_days=num_days,days_avg=days_avg,predict=True)
        self.num_days = num_days
        self.days_avg = days_avg
        self.threshold = threshold
        self.predict = True
        self.email_address = email_address
        self.password = password

    def _update_log(self, buy):
        log = self.import_from_s3('prediction_log','prediction')
        for symbol in buy:
            log.loc[log.index.max()+1] =[str(dt.today().date()),symbol]
        self.save_to_s3(log,'prediction_log')

    def plot_pred_percentage(self,predictions):
        symbols, percent = zip(*predictions)

        plt.close('all')
        fig, ax = plt.subplots()
        edgecolor = ['black']*len(symbols)
        sns.set_color_codes("pastel")
        sns.barplot(x=list(range(len(symbols))), y=[1]*len(symbols),
                    color='r',
                   edgecolor=edgecolor)

        sns.set_color_codes("muted")
        sns.barplot(x=list(range(len(symbols))), y=percent,
        color='r',
        edgecolor=edgecolor)

        fig.suptitle('Top 5 Highest Percentage')
        ax.set_xticklabels(symbols)
        plt.savefig('images/daily_update.png')

    def _make_predictions(self):
        # Load current model, symbols, and most recent dataset
        model = joblib.load('data/model/model.pkl')
        ticker_symbols = self.import_from_s3('ticker_symbols','key')
        combined_data = self.import_from_s3('combined_data')

        buy = []
        chance = []
        for _,stock in ticker_symbols.iterrows():
            symbol = stock['symbol']
            data = combined_data[combined_data.symbol == symbol]

            # some of the symbols do not qualify as they do not have enough
            # price history required to make a prediction
            if data.shape[0] <= self.num_days:
                continue
            else:
                x_pred = self._get_data_point(data.shape[0],data).reshape(1,-1)
                train_pred_proba = model.predict(xgb.DMatrix(x_pred))
                train_pred = train_pred_proba.copy()
                train_mask = train_pred > threshold
                train_pred[train_mask] = 1
                train_pred[np.invert(train_mask)] = 0
                if train_pred == 1:
                    buy.append(symbol)
                chance.append((symbol,float(train_pred_proba)))
                print(symbol, bool(train_pred))
        chance = sorted(chance,key = lambda x: x[1],reverse=True)
        return buy, chance

    def update_and_predict(self):
        # can start program at any time, but will only run between 1-2am MST
        while gmtime().tm_hour != 7:
            sleep(3600)
        while True:
            try:
                interval_time = time()
                if gmtime().tm_wday in [5,6]:
                    pass
                else:
                    buy,chances = self._make_predictions()
                    top5 = chances[0:5]
                    self.plot_pred_percentage(top5)
                    self.save_image_to_s3('daily_update.png')
                    self.send_email('prediction',buy)
                    self._update_log(buy)
            except Exception as e:
                self.send_email('error',str(e))
            sleep(60*60*24-(time()-interval_time))

if __name__ == '__main__':

    username = input('Gmail Username: ')
    password = getpass(prompt='Gmail Password: ')
    if username.count('@') < 1:
        username += '@gmail.com'

    num_days = 1200
    days_avg = 12
    threshold = 0.2
    p = DailyPrediction(num_days = num_days,days_avg=days_avg,threshold=threshold,
    email_address = username, password = password)
    p.update_and_predict()
