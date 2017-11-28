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
        '''
        The prediction log is the file that contains all predictions made by
            the model and date the prediction was made.

        The function will take every element within the input list 'buy', add it
            to the log along with the current date and then save.
        '''

        log = self.load_file('prediction_log')
        for symbol in buy:
            log.loc[log.index.max()+1] =[str(dt.today().date()),symbol]
        self.save_file(log,'prediction_log')

    def plot_pred_percentage(self,predictions):
        '''
        Informative visualization that shows the stocks that have the current
            chance of 'buy' signal as predicted by the model.
        '''

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
        '''
        This function goes through each symbol in the database and
            makes a prediction based on the most recent data. If any prediction
            percentages are above the given threshold, they will be added to the
            'buy' list and treated as a positive prediction. The function also
            returns a list of tuples with each symbol and their prediction
            percentage
        '''

        # Load current model, symbols, and most recent dataset
        model = joblib.load('model/data/model.pkl')
        ticker_symbols = self.load_file('ticker_symbols')
        combined_data = self.load_file('combined_data')

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

    def daily_prediction(self):
        '''
        Function will make model predictions on a daily basis. Intended to be
            running continuously on a cloud server.

        Performs the following (in order):
            * Make predictions
            * Plot visualizations of top stocks
            * Send emails to distribution list with predictions
            * Update log with any 'buy' signals
        '''

        # can start script at any time, but will only run between 1-2am MST
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
            if not self.now:
                sleep(60*60*24-(time()-interval_time))
            else:
                break

if __name__ == '__main__':
    DailyPrediction().daily_prediction()
