import pandas as pd
import datetime as dt
from src.general_functions import GeneralFunctions
from emails.send_emails import Email


class StockData(Email,GeneralFunctions):

    def __init__(self, update_single=[]):

        super().__init__()
        self.update_single = update_single
        if self.update_single != []:
            self.ticker_symbols = pd.DataFrame(update_single).T
            self.ticker_symbols.columns = ['symbol','url']
        else:
            self.ticker_symbols = self.load_file('ticker_symbols')

    def _add_zero_days(self, df):
        '''
        Parameters
        ----------
        df: pandas dataframe, raw stock price history

        Output
        ------
        df: pandas dataframe, original data with 0 volume days added in

        Some tickers will experience no volume in a given day. When importing,
        these days are not included at all. This function adds in the missing
        days with zero volume.
        '''

        # Convert index to datetime
        df.index = pd.to_datetime(df.index)

        # Start is the first day that stock price is recorded
        start = df.index[0]

        # If market is closed, last stock day is current day. Otherwise
        # it is previous day
        end = max([pd.Timestamp(dt.date.today()-dt.timedelta(1)),df.index[-1]])

        # Create datetime index with all business days (no weekends).
        # Holidays are not excluded, but are removed in CombineData
        all_days = pd.DatetimeIndex(start = start, end = end, freq = 'B')

        # Reset the index with the full set, and fill nulls (days that are
        # empty are days with 0 stock volume)
        df = df.reindex(all_days)
        df.index.name = 'Date'
        df.fillna({'Volume': 0},inplace=True)
        return df.fillna(method = 'ffill')

    def update_stock_data(self):

        first = True
        print ('\n','Getting stock price history for:')
        if self.update_single != []:
            # self.stock_data = pd.read_csv('data/tables/stock_prices.csv',index_col='Date')
            self.stock_data = self.load_file('stock_prices')
            first = False
        for _, stock in self.ticker_symbols.iterrows():
            symbol = stock['symbol']
            print (symbol)
            self.fid_url = 'https://screener.fidelity.com/ftgw/etf/downloadCSV.jhtml?symbol='
            url = self.fid_url + symbol.upper()
            try:
                df = pd.read_csv(url,index_col = 'Date').iloc[:-18]
                df = self._add_zero_days(df)
                df['symbol'] = symbol
            except Exception as e:
                df = self.stock_data[self.stock_data.symbol == symbol]
                self.send_email('error','Problem with {0}'.format(symbol))
                print ('Error for {0}'.format(symbol))

            if first:
                self.stock_data = df
                first = False
            else:
                self.stock_data = pd.concat([self.stock_data,df])

        self.save_file(self.stock_data,'stock_prices')

if __name__ == '__main__':
    StockData().update_stock_data()
