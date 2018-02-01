from src.ihub_data import IhubData
from src.stock_data import StockData
from src.general_functions import GeneralFunctions
from time import sleep


def add_new_symbol():
    '''
    To reduce errors, any given stock will be added to the database one at a
        time. The user can specify which stock is added. The only input needed
        is the symbol, and the ihub link to that stock's message board
    '''
    symbol = input('What is the symbol?  ')
    url = input('What is the ihub message board url?  ')

    ticker_symbols = GeneralFunctions().load_file('ticker_symbols')

    # make sure the symbol is not already in the database
    if symbol in ticker_symbols.symbol.tolist():
        print ('Symbol already in database')
        return

    ticker_symbols.loc[ticker_symbols.index.max()+1] = [symbol,url,0]
    GeneralFunctions().save_file(ticker_symbols,'ticker_symbols')
    sleep(30)

    ihub = IhubData(verbose=1,update_single=True)
    ihub.pull_posts()

    sd = StockData(update_single=[symbol,url])
    sd.update_stock_data()




if __name__ == '__main__':
    add_new_symbol()
