import pandas as pd


class DefineTarget(object):

    def __init__(self,data):
        self.data = data
        self.target = self.add_target()

    def add_target(self):
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

        target = []
        print('generating target...')
        for i in range(self.data.shape[0]-11):
            ohlc = self.data.iloc[i].ohlc
            wk_avg1 = self.data.iloc[i+1:i+6].ohlc.mean()
            wk_avg2 = self.data.iloc[i+1:i+11].ohlc.mean()
            wk_avg_vol = self.data.iloc[i-1:i+11].dollar_volume.mean()

            final_score = 1

            # Factors
            if all([ohlc!=0.0,i>60,wk_avg2>0.00015,wk_avg_vol>500]):

                wk_1_score = 5
                wk_2_score = 3

                if wk_avg1 > ohlc * wk_1_score:
                    score_1 = 1
                elif wk_avg1 < ohlc:
                    score_1 = 0
                else:
                    score_1 = (wk_avg1-ohlc)/(ohlc*(wk_1_score-1))

                if wk_avg2 > ohlc * wk_2_score:
                    score_2 = 1
                elif wk_avg2 < ohlc:
                    score_2 = 0
                else:
                    score_2 = (wk_avg2-ohlc)/(ohlc*(wk_2_score-1))
                target.append((score_1+score_2)/2)
            else:
                target.append(0)

        # Last two weeks treated as N/A since future week prices cannot be obtained
        na = [None]*11
        target.extend(na)

        self.data['target'] = target
        return target
