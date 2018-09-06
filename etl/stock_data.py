import pandas as pd
import datetime as dt
from src.general_functions import GeneralFunctions
# from emails.send_emails import Email

# class StockData(Email,GeneralFunctions):
class StockData(GeneralFunctions):

    def __init__(self):
        super().__init__()
        self.fid_url = 'https://screener.fidelity.com/ftgw/etf/downloadCSV.jhtml?symbol='

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
        df.index = pd.to_datetime(df.date)

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
        df = df.fillna(method = 'ffill')
        df.date = df.index
        return df

    def pull_data(self,symbol):

        url = self.fid_url + symbol.upper()
        # try:
        df = pd.read_csv(url,index_col = 'Date').iloc[:-18]
        # except Exception as e:
        #     self.send_email('error','Problem with {0}'.format(symbol))
        #     print ('Error for {0}'.format(symbol))
        df['Date'] = df.index
        df = df.reset_index(drop=True)
        df.columns = map(lambda x: x.lower(), df.columns)
        df['volume'] = df['volume'].astype(int)
        return df

    def update_stock_data(self, symbol_id):

        symbol = self.get_value('symbol',symbol_id=symbol_id)
        df = self.pull_data(symbol)
        df['symbol_id'] = symbol_id
        df['date'] = pd.to_datetime(df.date)
        df = self._add_zero_days(df)
        dates_already_added = set(self.get_list('price_history_dates',symbol_id=symbol_id))
        if dates_already_added:
            df = df[~df.date.isin(set(dates_already_added))]
        print('Adding {0} day(s) of price history for {1} ({2})'.format(df.shape[0],symbol,symbol_id))
        self.to_table(df,'market.price_history')

if __name__ == '__main__':
    sd = StockData()
    symbol_ids = sd.get_list('symbol_ids')
    for symbol_id in symbol_ids:
        sd.update_stock_data(symbol_id)
