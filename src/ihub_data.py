import requests
import numpy as np
import pandas as pd
from time import time
from time import sleep
from bs4 import BeautifulSoup
from src.general_functions import GeneralFunctions
from emails.send_emails import Email

class IhubData(Email,GeneralFunctions):

    def __init__(self, verbose=0, update_single=[],delay=False):
        super().__init__()
        self.verbose = verbose
        self.update_single = update_single
        # self.post_data = self.load_file('message_board_posts')
        self.post_data = self.load_file('message_board_posts')
        self.ticker_symbols = self.load_file('ticker_symbols')
        if self.update_single:
            self.indexer = self.ticker_symbols.index.max()
        else:
            self.indexer = 0
        self.delay = delay

    def _check_link_integrity(self,link):
        '''
        If a stock has updated it's symbol, the investorshub website will have
            a new link for the message board forum. This function is a failsafe
            to make sure the symbol is correct.
        '''

        URL = "https://investorshub.advfn.com/"+str(link)
        content = requests.get(URL).content
        soup = BeautifulSoup(content, "lxml")
        tag = soup.find_all('a', id="ctl00_CP1_btop_hlBoard")[0]['href'][1:-1]
        return tag

    def _update_link(self,symbol,ihub_url,tag):
        '''
        If the symbol has been updated, this function will update the databases
            for the messsage boards, stock prices, ticker_symbols.

        The function will also send an email update to notify the user
        '''

        new_symbol = tag.split('-')[-2].lower()
        self.send_email('update_symbol',[symbol,new_symbol])

        # update ticker symbol database and save
        idx = self.ticker_symbols[self.ticker_symbols.symbol == symbol].index[0]
        self.ticker_symbols.loc[idx]['url'] = tag
        self.ticker_symbols.loc[idx]['symbol'] = new_symbol
        self.ticker_symbols.to_csv('data/tables/ticker_symbols.csv')

        # update message board data
        idx = self.post_data[self.post_data.symbol == symbol].index
        self.post_data.loc[idx,'symbol'] = new_symbol

        # update stock price data
        df = self.load_file('stock_prices')
        idx = df[df.symbol == symbol].index
        df.loc[idx,'symbol'] = new_symbol
        self.save_file(df,'stock_prices')

        return new_symbol, tag

    def _total_and_num_pinned(self,url):
        '''
        Output
        ------
        num_pinned: int, shows how many posts are 'pinned' on the top of the
            board.
        num_posts: int, shows, to-date, how many messages have been posted on
            the specific board
        '''

        try:
            # Retrieve the first page on the board
            df, _ = self._get_page(url,most_recent=True,sort = False)

            # Number of pinned posts determined by the number of posts that are not
            # in 'numerical' order at the top of the page
            post_list = df.post_number.tolist()
            for i in range(len(post_list)):
                if post_list[i] == post_list[i+1]+1:
                    return i, post_list[i]
        except:
            return 0,0

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
        try:
            content = requests.get(URL).content
            soup = BeautifulSoup(content, "lxml")
            rows = list(soup.find('table', id="ctl00_CP1_gv"))
            table = []
            for row in rows[(2+num_pinned):-2]:
                cell_lst = [cell for cell in list(row)[1:5]]
                table.append(cell_lst)
            return self._clean_table(table,sort), error_list
        except Exception as e:
            print ('{0} ERROR ON PAGE: {1} for {2}'.format(e, str(post_number),url))
            error_list.append(post_number)
            return pd.DataFrame(), error_list

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
        if len(deleted_post_set) != 0:
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
        for _, stock in self.ticker_symbols.loc[self.indexer:].iterrows():
            self.interval_time, self.original_time = time(), time()
            idx = stock.name
            symbol, ihub_url, last_post = stock['symbol'],stock['url'],stock['last_post']
            tag = self._check_link_integrity(ihub_url)
            if tag != ihub_url:
                print ('{0} changing to {1}'.format(ihub_url,tag))
                symbol, ihub_url = self._update_link(symbol,ihub_url,tag)

            df = self.post_data[self.post_data.symbol == symbol]
            num_pinned, num_posts = self._total_and_num_pinned(ihub_url)

            if len(df) == 0:
                start_number = 0
            else:
                start_number = last_post

            if num_posts in [0,last_post]:
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
                    percent = int(num/number_of_pages*100)
                    self.status_update(percent)
                if self.delay:
                    sleep(10)

            final_error_list = []
            shallow_error_list = list(error_list)
            for page in shallow_error_list:
                page_df, final_error_list = self._get_page(ihub_url,post_number=page,
                    num_pinned=num_pinned,error_list = final_error_list)
                new_posts = pd.concat([new_posts,page_df])

            new_posts.drop_duplicates(inplace = True)
            new_posts = self._add_deleted_posts(new_posts,start_number,num_posts)
            new_posts['symbol'] = symbol

        # clean and save the new dataframe
            print ('{0} complete, {1} posts added'.format(symbol,
                new_posts.post_number.max()-start_number))

            new_posts = new_posts.groupby(['date','symbol'],as_index=False).count()
            new_posts.date = new_posts.date.astype(str)

            last_date = df.index.max()
            if df.loc[last_date,'date'] == new_posts.loc[0,'date']:
                self.post_data.loc[last_date,'post_number'] += new_posts.loc[0,'post_number']
                new_posts = new_posts.loc[1:]
            self.post_data = pd.concat([self.post_data,new_posts])
            self.ticker_symbols.loc[idx,'last_post'] = num_posts

        self.post_data.sort_values(['symbol','date'],inplace=True)
        self.post_data.reset_index(inplace=True,drop=True)
        self.save_file(self.ticker_symbols,'ticker_symbols')
        self.save_file(self.post_data,'message_board_posts')

if __name__ == '__main__':
    data = IhubData(verbose = 1)
    df_old = data.post_data
    data.pull_posts_test()
    df_new = data.post_data
    # df1 = data.new_posts
    # df2 = data.df

    # abhi = df[df.symbol=='abhi']
