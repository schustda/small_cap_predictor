from src.ihub_data import IhubData
from src.stock_data import StockData
from src.general_functions import GeneralFunctions


def add_new_symbol():
    '''
    To reduce errors, any given stock will be added to the database one at a
        time. The user can specify which stock is added. The only input needed
        is the symbol, and the ihub link to that stock's message board
    '''
    symbol = input('What is the symbol?  ')
    url = input('What is the ihub message board url?  ')

    ihub = IhubData(verbose=1,update_single=[symbol,url])
    ihub.pull_posts()

    sd = StockData(update_single=[symbol,url])
    sd.update_stock_data()

    gf = GeneralFunctions()
    ticker_symbols = gf.import_from_s3('ticker_symbols','key')
    ticker_symbols.loc[ticker_symbols.index.max()+1] = [symbol,url]
    gf.save_to_s3(ticker_symbols,'ticker_symbols')



if __name__ == '__main__':
