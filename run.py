import smtplib
import subprocess
import numpy as np
import pandas as pd
import xgboost as xgb
from sys import argv
from time import time,sleep, gmtime
from datetime import datetime as dt
from email.mime.text import MIMEText
from sklearn.externals import joblib
from src.data.ihub_data import IhubData
from src.data.stock_data import StockData
from src.data.combine_data import CombineData
from email.mime.multipart import MIMEMultipart
from src.data.training_data import TrainingData

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
        log = pd.read_csv('log/prediction_log.csv',index_col='prediction')
        for symbol in buy:
            log.loc[log.index.max()+1] =[str(dt.today().date()),symbol]
        log.to_csv('log/prediction_log.csv')

    def _update(self):
        mbp = IhubData(verbose=1)
        mbp.pull_posts()

        sp = StockData()
        sp.update_stock_data()

        cd = CombineData()
        cd.compile_data()

    def _make_predictions(self):
        model = joblib.load('data/model/model.pkl')

        ticker_symbols = pd.read_csv('data/tables/ticker_symbols.csv',
            index_col='key')

        combined_data = pd.read_csv('data/tables/combined_data.csv')

        buy = []
        for _, stock in ticker_symbols.iterrows():
            symbol = stock['symbol']
            data = combined_data[combined_data.symbol == symbol]

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
                print(symbol, bool(train_pred))

        return buy

    def _email_results(self,stock_lst):
        fromaddr = self.email_address
        toaddr = self.email_address
        msg = MIMEMultipart()
        msg['From'] = fromaddr
        msg['To'] = toaddr
        msg['Subject'] = "BUY SIGNAL ALERT"

        body = "Stocks indicating buy signal: {0}".format(", ".join(stock_lst).upper())
        msg.attach(MIMEText(body, 'plain'))

        server = smtplib.SMTP('smtp.gmail.com', 587)
        server.starttls()
        server.login(fromaddr, self.password)
        text = msg.as_string()
        server.sendmail(fromaddr, toaddr, text)
        server.quit()

    def update_and_predict(self):
        # can start program at any time, but will only run between 12-1am
        while gmtime().tm_hour != 6:
            # print (gmtime().tm_hour)
            sleep(3600)
        while True:
            interval_time = time()
            if gmtime().tm_wday in [5,6]:
                pass
            else:
                rc = subprocess.call('scripts/git_pull.sh',shell=True)
                self._update()
                buy = self._make_predictions()
                if len(buy) > 0:
                    self._email_results(buy)
                    self._update_log(buy)

                rc = subprocess.call('scripts/git_add_data.sh',shell=True)
            sleep(60*60*24-(time()-interval_time))

if __name__ == '__main__':

    num_days = 1200
    days_avg = 12
    threshold = 0.2
    p  = DailyPrediction(num_days = num_days,days_avg=days_avg,threshold=threshold,
    email_address = argv[1], password = argv[2])
    p.update_and_predict()
