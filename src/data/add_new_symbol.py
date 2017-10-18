import subprocess
import pandas as pd
from src.data.ihub_data import IhubData
from src.data.stock_data import StockData

if __name__ == '__main__':

    # symbol = input('What is the symbol?  ')
    # url = input('What is the ihub message board url?  ')

    symbol = 'frfs'
    url = 'Firefish-Inc-FRFS-24668'

    rc = subprocess.call('scripts/git_pull.sh',shell=True)

    ihub = IhubData(verbose=1,update_single=[symbol,url])
    ihub.pull_posts()

    price = StockData(update_single=[symbol,url])
    price.update_stock_data()

    ticker_symbols = pd.read_csv('data/tables/ticker_symbols.csv',
        index_col='key')
    ticker_symbols.loc[ticker_symbols.index.max()+1] = [symbol,url]
    ticker_symbols.to_csv('data/tables/ticker_symbols.csv')

    rc = subprocess.call('scripts/git_add_data.sh',shell=True)
