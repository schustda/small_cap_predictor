from etl.ihub_data import IhubData
from etl.stock_data import StockData
from model.combine_data import CombineData
from model.define_target import DefineTarget
from sys import argv


def update_data(section):
    ihub = IhubData(verbose = 1,delay=True)
    symbol_ids = ihub.get_list('symbol_ids')
    id_groups = {}
    id_groups['grp1'] = [x for x in symbol_ids if not x%4]
    id_groups['grp2'] = [x for x in symbol_ids if not (x+1)%4]
    id_groups['grp3'] = [x for x in symbol_ids if not (x+2)%4]
    id_groups['grp4'] = [x for x in symbol_ids if not (x+3)%4]
    id_groups['all'] = symbol_ids

    for symbol_id in id_groups[section]:
        ihub.update_posts(symbol_id)
    del ihub

    sd = StockData()
    for symbol_id in id_groups[section]:
        sd.update_stock_data(symbol_id)
    del sd

    cd = CombineData()
    for symbol_id in id_groups[section]:
        cd.compile_data(symbol_id)
    del cd

    dt = DefineTarget()
    for symbol_id in id_groups[section]:
        print ('adding target for {0}'.format(symbol_id))
        dt.add_target(symbol_id)
    del dt



if __name__ == '__main__':
    update_data(argv[1])
