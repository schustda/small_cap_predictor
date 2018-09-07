from etl.ihub_data import IhubData
from etl.stock_data import StockData
from model.combine_data import CombineData
from model.define_target import DefineTarget


if __name__ == '__main__':

    ihub = IhubData(verbose = 1,delay=False)
    symbol_ids = ihub.get_list('symbol_ids')

    for symbol_id in symbol_ids:
        ihub.update_posts(symbol_id)
    del ihub

    sd = StockData()
    for symbol_id in symbol_ids:
        sd.update_stock_data(symbol_id)
    del sd

    cd = CombineData()
    for symbol_id in symbol_ids:
        cd.compile_data(symbol_id)
    del sd

    dt = DefineTarget()
    for symbol_id in symbol_ids:
        dt.add_target(symbol_id)
    del sd
