import requests
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
from time import time

class IhubData(object):

    def __init__(self, verbose = 0, update_single=[]):
        self.verbose = verbose
        self.update_single = update_single
        try:
            self.post_data = pd.read_csv('data/tables/message_board_posts.csv',index_col=0)
        except:
            self.post_data = pd.DataFrame(columns=['post_number','date','symbol'])
        if self.update_single != []:
            self.ticker_symbols = pd.DataFrame(update_single).T
            self.ticker_symbols.columns = ['symbol','url']
        else:
            self.ticker_symbols = pd.read_csv('data/tables/ticker_symbols.csv',
                index_col='key')

    def _total_and_num_pinned(self,url):
        '''
        Output
        ------
        num_pinned: int, shows how many posts are 'pinned' on the top of the
            board.
        num_posts: int, shows, to-date, how many messages have been posted on
            the specific board
        '''

        # Retrieve the first page on the board
        df, _ = self._get_page(url,most_recent=True,sort = False)

        # Number of pinned posts determined by the number of posts that are not
        # in 'numerical' order at the top of the page
        post_list = df.post_number.tolist()
        for i in range(len(post_list)):
            if post_list[i] == post_list[i+1]+1:
                return i, post_list[i]

    def _clean_table(self, table, sort):
        '''
        Parameters
        ----------
        df: pandas dataframe,
        sort: boolean, the message board posts are displayed in descending
            order. Sort=True sorts them

        Output
        ------
        df: pandas dataframe, message board table
        '''

        df = pd.DataFrame(table)
        # At this time, not using username and post subject
        df.drop([1,2],axis=1,inplace=True)
        df = df.applymap(lambda x: x.text)
        df.columns = ['post_number','date']
        df.post_number = df['post_number'].map(lambda x: x.strip('-#\n\r').replace('\n', "").replace('\r',''))
        df.post_number = df.post_number.astype(int)
        df['date'] = pd.to_datetime(df['date']).dt.date
        if sort:
            df.sort_values('post_number',inplace = True)
        return df

    def _get_page(self, url, num_pinned = 0, post_number = 1,
            most_recent = False, sort = True, error_list = []):
        '''
        Parameters
        ----------
        post_number: int, specific post number of page to be returned
        most_recent: boolean, returns the currently displayed page if True
        sort: boolean, as displayed on the webpage, the message board posts are
            displayed in descending order. Sort sorts them

        Output
        ------
        df: pandas dataframe, pulled from the webpage, parsed, and cleaned
        '''
        URL = "https://investorshub.advfn.com/"+str(url)
        if not most_recent:
            URL += "/?NextStart="+str(post_number)
        content = requests.get(URL).content
        soup = BeautifulSoup(content, "lxml")
        rows = list(soup.find('table', id="ctl00_CP1_gv"))
        table = []
        try:
            for row in rows[(2+num_pinned):-2]:
                cell_lst = [cell for cell in list(row)[1:5]]
                table.append(cell_lst)
            return self._clean_table(table,sort), error_list

        except Exception as e:
            print ('{0} ERROR ON PAGE: {1}'.format(e, str(post_number)))
            error_list.append(post_number)
            return pd.DataFrame(), error_list

    def _verbose(self,num,number_of_pages):
        '''
        Method displays progress of retriving message board posts
        '''

        # Display update ever 60 seconds
        if time() > self.interval_time + 60:
            percent = int(num/number_of_pages*100)
            time_elapsed = time() - self.original_time
            a = int(percent/2)
            b = 50-a
            if percent == 0:
                percent = 0.5
            min_rem = int(time_elapsed/percent*(100-percent)/60)
            print ('|{0}{1}| {2}% - {3} minute(s) remaining'.format(a*'=',b*'-',str(percent),str(min_rem)))
            self.interval_time = time()

    def _replace_bad_link(self, symbol, url):
        _,error_list = self._get_page(url,most_recent=True)
        idx = self.ticker_symbols[self.ticker_symbols.symbol == symbol].index[0]
        if len(error_list) != 0:
            url_lst = url.split('-')
            symbol_idx = url_lst.index(symbol.upper())
            url_lst[symbol_idx] += 'D'
            url_new = "-".join(url_lst)
            _,error_list = self._get_page(url_new,most_recent=True)
            if len(error_list) != 0:
                url_lst[symbol_idx] = url_lst[symbol_idx][:-2]
                url_new = "-".join(url_lst)
                _,error_list = self._get_page(url_new,most_recent=True)
                if len(error_list) != 0:
                    print ('LINK BROKEN')
                else:
                    # stock no longer has a D at the end
                    self.ticker_symbols.loc[idx].symbol = symbol[:-1]
                    self.ticker_symbols.loc[idx].url = url_new
                    self.ticker_symbols.to_csv('data/tables/ticker_symbols.csv')
            else:
                # stock has a split and not needs a D at the end
                self.ticker_symbols.loc[idx].symbol = symbol + 'd'
                self.ticker_symbols.loc[idx].url = url_new
                self.ticker_symbols.to_csv('data/tables/ticker_symbols.csv')
        else:
            print ('LINK WORKING')
            #revise stock list


    def _add_deleted_posts(self, df, start, end):
        '''
        Parameters
        ----------
        df: pandas dataframe, full message board data

        Output
        ------
        df: pandas dataframe, original data with deleted posts added in

        Moderators of a message board forum may remove a post if it violates
        the ihub policy. While the content of these posts is unknown the actual
        post is important when suming the posts per a given day.
        '''

        #get missing post numbers
        deleted_post_set = set(range(start,end+1)).difference(set(df.post_number))
        if len(deleted_post_set) == 0:
            pass
        else:
            #create df with deleted posts
            df_deleted = pd.DataFrame(np.nan, index=range(len(deleted_post_set)), columns=df.columns.tolist())
            df_deleted.post_number = deleted_post_set

            #add to original dataframe
            df = pd.concat([df,df_deleted])

            #sort df
            df.sort_values('post_number',inplace=True)

            # The dates from the deleted posts will be interpreted as from the
            # day from the previous post
            df.date.fillna(method = 'ffill',inplace = True)
        return df

    def pull_posts(self):
        for _, stock in self.ticker_symbols.iterrows():
            self.interval_time, self.original_time = time(), time()
            symbol, ihub_url = stock['symbol'],stock['url']
            num_pinned, num_posts = self._total_and_num_pinned(ihub_url)
            df = self.post_data[self.post_data.symbol == symbol]
            if len(df) == 0:
                start_number = 0
            else:
                start_number = df.post_number.max()

            if num_posts == df.post_number.max():
                print('No posts added for {0}'.format(symbol))
                continue

            pages_to_add = list(range(start_number,num_posts,50))
            pages_to_add.append(num_posts)
            number_of_pages = len(pages_to_add)

            error_list, first = [], True

            for num,page in enumerate(pages_to_add):
                page_df, error_list = self._get_page(ihub_url,post_number=page,
                    num_pinned=num_pinned,error_list = error_list)
                if first:
                    new_posts, first = page_df, False
                else:
                    new_posts = pd.concat([new_posts,page_df])
                if self.verbose:
                    self._verbose(num,number_of_pages)

            final_error_list = []
            shallow_error_list = list(error_list)
            for page in shallow_error_list:
                page_df, final_error_list = self._get_page(page,num_pinned=num_pinned,error_list = final_error_list)
                new_posts = pd.concat([new_posts,page_df])
            if final_error_list != []:
                self._replace_bad_link(symbol,url)

            new_posts.drop_duplicates(inplace = True)
            new_posts = self._add_deleted_posts(new_posts,start_number,num_posts)
            new_posts['symbol'] = symbol

        # clean and save the new dataframe
            print ('{0} complete, {1} posts added'.format(symbol,
                new_posts.post_number.max()-start_number))
            # df.to_csv('data/raw_data/ihub/message_boards/'+self.symbol+'.csv')
            self.post_data = pd.concat([self.post_data,new_posts])

        self.post_data.sort_values(['symbol','post_number'],inplace=True)
        self.post_data.reset_index(inplace=True,drop=True)
        self.post_data.to_csv('data/tables/message_board_posts.csv')

if __name__ == '__main__':
    data = IhubData(verbose = 1)
    data.pull_posts()
