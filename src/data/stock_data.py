import pandas as pd
import datetime as dt

class StockData(object):

    def __init__(self, update_single=[]):
        self.update_single = update_single
        if self.update_single != []:
            self.ticker_symbols = pd.DataFrame(update_single).T
            self.ticker_symbols.columns = ['symbol','url']
        else:
            self.ticker_symbols = pd.read_csv('data/tables/ticker_symbols.csv',
                index_col='key')

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

    def _replace_bad_link(self, symbol):
        idx = self.ticker_symbols[self.ticker_symbols.symbol == symbol].index[0]
        df = pd.DataFrame(columns=['Open','High','Low','Close','Volume','symbol'])
        try:
            url = self.fid_url + symbol.upper() + 'D'
            df = pd.read_csv(url,index_col = 'Date').iloc[:-18]
            df = self._add_zero_days(df)
            df['symbol'] = symbol+'d'
            self.ticker_symbols.loc[idx].symbol = symbol + 'd'
            self.ticker_symbols.to_csv('data/tables/ticker_symbols.csv')
            print ('Added "D" to symbol')
        except:
            print ('Split (D) not working')

            try:
                url = self.fid_url + symbol.upper()[:-1]
                df = pd.read_csv(url,index_col = 'Date').iloc[:-18]
                df = self._add_zero_days(df)
                df['symbol'] = symbol[:-1]
                self.ticker_symbols.loc[idx].symbol = symbol[:-1]
                self.ticker_symbols.to_csv('data/tables/ticker_symbols.csv')

            except:
                print ('Link broken for {0}'.format(symbol))
        return df

    def update_stock_data(self):

        first = True
        print ('Getting stock price history for:')
        if self.update_single != []:
            self.stock_data = pd.read_csv('data/tables/stock_prices.csv',index_col='Date')
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
            except:
                print ('Error for {0}'.format(symbol))
                df = self._replace_bad_link(symbol)

            if first:
                self.stock_data = df
                first = False
            else:
                self.stock_data = pd.concat([self.stock_data,df])

        self.stock_data.to_csv('data/tables/stock_prices.csv')

if __name__ == '__main__':

    cbyi = StockData()
    cbyi.update_stock_data()
