import pandas as pd
from math import ceil
from time import time


class GeneralFunctions(object):

    def __init__(self, breaks=10):
        self.breaks = breaks
        self.index_col = {'message_board_posts': 0,
                            'stock_prices': 'Date'}

    def load_file(self,f):
        for i in range(self.breaks):
            df_load = pd.read_csv('data/tables/{0}{1}.csv'.format(f,i),
                index_col=self.index_col[f])
            if i == 0:
                df = df_load
            else:
                df = pd.concat([df,df_load])
        return df

    def save_file(self,f,df):

        break_lst = list(range(0,df.shape[0],ceil(df.shape[0]/self.breaks)))
        break_lst.append(df.shape[0])
        for i in range(len(break_lst)-1):
            df[break_lst[i]:break_lst[i+1]].to_csv('data/tables/{0}{1}.csv'.format(f,i))

    def status_update(self,percent):
        '''
        Method displays progress of retriving message board posts
        '''
        # Display update ever 60 seconds
        if time() > self.interval_time + 60:
            time_elapsed = time() - self.original_time
            a = int(percent/2)
            b = 50-a
            if percent == 0:
                percent = 0.5
            min_rem = int(time_elapsed/percent*(100-percent)/60)
            print ('|{0}{1}| {2}% - {3} minute(s) remaining'.format(a*'=',b*'-',str(percent),str(min_rem)))
            self.interval_time = time()

if __name__ == '__main__':
    sf = SplitFile()
    mbp = sf.load_file('message_board_posts')
    # sf.save_file('message_board_posts',mbp)
