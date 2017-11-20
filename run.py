import numpy as np
import pandas as pd
import xgboost as xgb
import seaborn as sns
import matplotlib.pyplot as plt
from emails.send_emails import Email
from src.training_data import TrainingData
from sklearn.externals import joblib
from time import time,sleep,gmtime
from datetime import datetime as dt

class DailyPrediction(TrainingData,Email):

    def __init__(self,threshold=0.2,now=False):
        TrainingData.__init__(self,num_days=1200,days_avg=12,predict=True)
        Email.__init__(self)
        self.predict = True
        self.threshold = threshold
        self.now = now

    def _update_log(self, buy):
        log = self.import_from_s3('prediction_log')
        for symbol in buy:
            log.loc[log.index.max()+1] =[str(dt.today().date()),symbol]
        self.save_to_s3(log,'prediction_log')

    def plot_pred_percentage(self,predictions):
        symbols, percent = zip(*predictions)
        plt.close('all')
        fig, ax = plt.subplots(figsize=(4,4))

        # Barplot of top stocks
        plot_params = {'color':'b','edgecolor':['black']*len(symbols)}
        sns.set_color_codes("pastel")
        sns.barplot(x=list(range(len(symbols))), y=[1]*len(symbols),
            alpha = 0.3,**plot_params)
        sns.set_color_codes("muted")
        sns.barplot(x=list(range(len(symbols))), y=percent,**plot_params)

        # Add labels to the plot
        text_params = {'ha':'center','va':'center'}
        for i in range(len(symbols)):
            label = '{0}%'.format(int(percent[i]*100))
            ax.text(i,percent[i]+0.04,label,size=12,weight='light',**text_params)
            ax.text(i,-0.05,symbols[i].upper(),size=15,**text_params)
        ax.axis('off')
        plt.tight_layout(rect = [-0.07,0,1,1.05])
        # plt.savefig('images/daily_update.png')
        self.save_image_to_s3('daily_update.png')

    def _make_predictions(self):
        # Load current model, symbols, and most recent dataset
        model = joblib.load('model/data/model.pkl')
        ticker_symbols = self.import_from_s3('ticker_symbols')
        combined_data = self.import_from_s3('combined_data')

        buy, chance = [], []
        for _,stock in ticker_symbols.iterrows():
            symbol = stock['symbol']
            data = combined_data[combined_data.symbol == symbol]

            # some of the symbols do not qualify as they do not have enough
            # price history required to make a prediction
            if data.shape[0] <= self.num_days:
                continue
            else:
                x_pred = self._get_data_point(data.shape[0],data).reshape(1,-1)
                y_pred = model.predict(xgb.DMatrix(x_pred))[0]
                y_bool = y_pred > self.threshold
                if y_bool:
                    buy.append(symbol)
                chance.append((symbol,y_pred))
                print(symbol, y_pred, y_bool)
        chance = sorted(chance,key = lambda x: x[1],reverse=True)
        return buy, chance

    def update_and_predict(self):
        # can start program at any time, but will only run between 1-2am MST
        if not self.now:
            while gmtime().tm_hour != 7:
                sleep(3600)
        while True:
            try:
                interval_time = time()
                if gmtime().tm_wday in [5,6]:
                    pass
                else:
                    buy,chances = self._make_predictions()
                    self.plot_pred_percentage(chances[0:5])
                    sleep(30)
                    self.send_email('prediction',buy)
                    self._update_log(buy)
            except Exception as e:
                self.send_email('error',str(e))
            sleep(60*60*24-(time()-interval_time))

if __name__ == '__main__':
    DailyPrediction().update_and_predict()
