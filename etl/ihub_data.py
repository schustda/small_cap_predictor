from time import time, sleep
from random import randint
import requests
import numpy as np
import pandas as pd
from bs4 import BeautifulSoup
from emails.send_emails import Email
from src.general_functions import GeneralFunctions

class IhubData(Email, GeneralFunctions):

    def __init__(self, verbose=0, delay=True):
        super().__init__()
        self.verbose = verbose
        self.delay = delay

    def check_link_integrity(self, symbol_id):
        '''
        Input: symbol_id (int)

        If a stock has updated it's symbol, the investorshub website will have
            a new link for the message board forum. This function is a failsafe
            to make sure the link is up to date.
        '''

        ihub_code = self.get_value('ihub_code', symbol_id=symbol_id)
        url = "https://investorshub.advfn.com/"+str(ihub_code)
        content = requests.get(URL).content
        soup = BeautifulSoup(content, "lxml")

        # This location in the website will contain the current link
        tag = soup.find_all('a', id="ctl00_CP1_btop_hlBoard")[0]['href'][1:-1]

        # If there is in fact a new link, then this will update the database
        # with the new link and symbol
        if ihub_code != tag:
            new_symbol = tag.split('-')[-2].lower()
            self._update_link(symbol_id, tag, new_symbol)
            print(f'NEW SYMBOL {symbol} changed to {new_symbol}')
        else:
            print(f'No change in symbol')

    def _update_link(self, symbol_id, tag, new_symbol):
        '''
        If the symbol has been updated, this function will update the databases
            for the messsage boards, stock prices, ticker_symbols.

        The function will also send an email update to notify the user
        '''

        # First pull the existing symbol
        old_symbol = self.get_value('symbol', symbol_id=symbol_id)

        # First append the changed symbol table
        df = pd.DataFrame(columns=['symbol_id', 'changed_from', 'changed_to', 'date_modified'])
        df.loc[0] = [symbol_id, old_symbol, new_symbol, pd.Timestamp.now()]
        self.to_table(df, 'items.changed_symbol')

        # Then modify the symbols table
        update_query = '''
            UPDATE items.symbol
            SET symbol = '{0}', ihub_code = '{1}', modified_date = '{2}'
            WHERE symbol_id = {3};
            '''.format(new_symbol, tag, pd.Timestamp.now(), symbol_id)
        self.cursor.execute(update_query)
        self.conn.commit()
        self.send_email('update_symbol', ['', new_symbol])

    def _total_and_num_pinned(self, url):
        '''
        Function call gets the most recent page for the message board and
            extracts both num_pinned, and num_posts (desc below)

        Output
        ------
        num_pinned: int, shows how many posts are 'pinned' on the top of the
            board.
        num_posts: int, shows, to-date, how many messages have been posted on
            the specific board
        '''

        try:
            # Retrieve the first page on the board
            df, _ = self._get_page(url, most_recent=True, sort=False)

            # Number of pinned posts determined by the number of posts that are not
            # in 'numerical' order at the top of the page
            post_list = df.post_number.tolist()
            for i in range(len(post_list)):
                if post_list[i] == post_list[i+1]+1:
                    return i, post_list[i]
        except:
            return 0, 0

    def _clean_table(self, table, sort):
        '''
        Function takes the raw dataframe and formats it

        Parameters
        ----------
        df: pandas dataframe,
        sort: boolean, the message board posts are displayed in descending
            order. Sort=True sorts them

        Output
        ------
        df: pandas dataframe, message board table
        '''

        # 0 - post_number
        # 1 - subject
        # 2 - username
        # 3 - post_time

        df = pd.DataFrame(table)
        df = df.applymap(lambda x: x.text)
        df.columns = ['post_number', 'subject', 'username', 'post_time']
        df[['subject', 'username']] = df[['subject', 'username']].applymap(
            lambda x: x.strip('-#\n\r').replace('\n', "").replace(
                '\r', '').replace('\t', '').replace('\\', ''))
        df.post_number = df['post_number'].map(lambda x: x.strip(
            '-#\n\r').replace(' ', '').replace('\n', "").replace(
                '\r', '').split('\xa0')[0])
        df.post_number = df.post_number.astype(float)
        df.post_number = df.post_number.astype(int)
        df['post_time'] = pd.to_datetime(df['post_time'])
        if sort:
            df.sort_values('post_number', inplace=True)
        return df

    def _get_page(self, url, num_pinned=0, post_number=1,
                  most_recent=False, sort=True, error_list=[]):
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
        url = "https://investorshub.advfn.com/"+str(url)
        if not most_recent:
            url += "/?NextStart="+str(post_number)
        content = requests.get(URL).content
        soup = BeautifulSoup(content, "lxml")
        rows = list(soup.find('table', id="ctl00_CP1_gv"))
        table = []
        for row in rows[(2+num_pinned):-2]:
            cell_lst = [cell for cell in list(row)[1:5]]
            table.append(cell_lst)
        return self._clean_table(table, sort), error_list

    def _add_deleted_posts(self, page_df, post_number):
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

        should_be_on_page = set(range(min(page_df.post_number), post_number+1))
        should_be_on_page.add(post_number)
        deleted_post_numbers = should_be_on_page - set(page_df.post_number)
        del_df = pd.DataFrame(columns=page_df.columns)
        for num, post_num in enumerate(deleted_post_numbers):
            del_df.loc[num] = [post_num, '<del>', '<del>', np.nan]
        return pd.concat([page_df, del_df])

    def update_posts(self, symbol_id):

        # first, pull the necessary symbol info
        symbol = self.get_value('symbol', symbol_id=symbol_id)
        ihub_code = self.get_value('ihub_code', symbol_id=symbol_id)
        num_pinned, num_posts = self._total_and_num_pinned(ihub_code)

        self.interval_time, self.original_time = time(), time()

        # calculate which post numbers are missing from the database
        posts_to_add = set(range(1, num_posts+1))
        already_added = set(self.get_list('existing_posts', symbol_id=symbol_id))
        posts_to_add -= already_added

        error_list = []
        total_posts_to_add = len(posts_to_add)
        print(f"Adding {total_posts_to_add} post(s) for {symbol} ({symbol_id})")
        while len(posts_to_add) > 0:
            post_number = max(posts_to_add)
            page = post_number
            while True:
                try:
                    page_df, error_list = self._get_page(ihub_code, post_number=page,
                                                         num_pinned=num_pinned,
                                                         error_list=error_list)
                    break

                # if the number one post is deleted and you're calling it, it will fail
                except ValueError:
                    page += 1

                except Exception as e:
                    print(f'{e} ERROR ON PAGE: {str(post_number)} for {ihub_code}')
                    error_list.append(post_number)
                    page_df = pd.DataFrame()
                    break

            page_df = self._add_deleted_posts(page_df, post_number)
            page_df['symbol_id'] = symbol_id
            self.to_table(page_df, 'ihub.message_board')
            posts_to_add -= set(page_df.post_number)
            if self.verbose:
                num = (total_posts_to_add-len(posts_to_add))
                total = total_posts_to_add
                self.status_update(num, total)
            if self.delay:
                sleep(randint(2, 15))
