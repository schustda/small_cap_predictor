import pandas as pd
from math import ceil


class SplitFile(object):
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

if __name__ == '__main__':
    sf = SplitFile()
    mbp = sf.load_file('message_board_posts')
    # sf.save_file('message_board_posts',mbp)
