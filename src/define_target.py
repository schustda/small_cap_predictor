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

            a1 = wk_avg1 > ohlc * 2
            a2 = wk_avg2 > ohlc * 1.5
            a3 = ohlc != 0.0
            a4 = wk_avg2 > 0.00015
            a5 = i > 60
            a6 = wk_avg_vol > 500
            buy = all([a1,a2,a3,a4,a5,a6])

            if buy :
                target.append(1)
            else:
                target.append(0)

        # Last two weeks treated as N/A since future week prices cannot be obtained
        na = [None]*11
        target.extend(na)

        self.data['target'] = target
        return target
