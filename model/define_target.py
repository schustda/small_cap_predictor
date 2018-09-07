import pandas as pd
from sys import argv
import numpy as np
from model.model_base_class import ModelBaseClass


class DefineTarget(ModelBaseClass):

    def __init__(self,verbose=False):
        super().__init__(verbose=verbose)

    def add_target(self,symbol_id,reset=False):
        '''
        Define target given a data point. Current critera are as follows:
            * Average price for the following market week is double what the
                current price is
            * Average price for the following two market weeks is one and a half
                times what the current price is
            * The stock price is not currently zero
            * Average price for the following two market weeks is more than
                $0.00015
            * The stock has been around for at least 60 days
                * Normally at inception funky things happen with the price
                    that are difficult to capture
            * The dollar volume for during following two weeks is more than
                $500 per day.
        '''

        data = self.get_df('get_combined_data',symbol_id=symbol_id)

        print('generating target...')
        for i in range(data.shape[0]-11):
            if not reset:
                if not np.isnan(data.iloc[i].defined_target):
                    continue
            idx = data.iloc[i].idx
            ohlc_average = data.iloc[i].ohlc_average
            wk_avg1 = data.iloc[i+1:i+6].ohlc_average.mean()
            wk_avg2 = data.iloc[i+1:i+11].ohlc_average.mean()
            wk_avg_vol = data.iloc[i-1:i+11].dollar_volume.mean()
            final_score = 1

            # Factors
            if all([ohlc_average!=0.0,i>60,wk_avg2>0.00015,wk_avg_vol>500]):

                wk_1_score = 5
                wk_2_score = 3

                if wk_avg1 > ohlc_average * wk_1_score:
                    score_1 = 1
                elif wk_avg1 < ohlc_average:
                    score_1 = 0
                else:
                    score_1 = (wk_avg1-ohlc_average)/(ohlc_average*(wk_1_score-1))

                if wk_avg2 > ohlc_average * wk_2_score:
                    score_2 = 1
                elif wk_avg2 < ohlc_average:
                    score_2 = 0
                else:
                    score_2 = (wk_avg2-ohlc_average)/(ohlc_average*(wk_2_score-1))
                target = (score_1+score_2)/2
            else:
                target = 0

            update_query = '''
UPDATE model.combined_data
SET defined_target = {0}, modified_date = '{1}'
WHERE idx = {2};
            '''.format(target,pd.Timestamp.now(),idx)
            self.cursor.execute(update_query)
            self.conn.commit()

if __name__ == '__main__':
    dt = DefineTarget()
    # dt.add_target(89)

    symbol_ids = dt.get_list('symbol_ids')
    grp1 = [x for x in symbol_ids if not x%4]
    grp2 = [x for x in symbol_ids if not (x+1)%4]
    grp3 = [x for x in symbol_ids if not (x+2)%4]
    grp4 = [x for x in symbol_ids if not (x+3)%4]
    for symbol_id in eval(argv[1]):
        print(symbol_id)
        dt.add_target(symbol_id)
