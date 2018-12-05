import requests
import pandas as pd
from bs4 import BeautifulSoup
from textblob import TextBlob
from src.general_functions import GeneralFunctions
from time import time, sleep
from random import randint, sample
import numpy
from multiprocessing import Process, Pool


class IhubSentiment(GeneralFunctions):

    def __init__(self, verbose = 0):
        super().__init__(verbose)
        self.total = 144699616
        self.bad_page_count = 0

    def get_message_page(self, message_id):
        ''' Web scrapes post information from the specified message_id

        Parameters:
            message_id (int): The message number to extract data from
        '''

        url = f"https://investorshub.advfn.com/boards/read_msg.aspx?message_id={message_id}"
        r = requests.get(url)
        content = r.content
        soup = BeautifulSoup(content, "lxml")
        response_code = soup.find_all(id="ctl00_CP1_L1")
        columns = ['status','message_id','ihub_code','post_number',
                   'sentiment_polarity','sentiment_subjectivity','message_date']
        df = pd.DataFrame(columns=columns)

        if response_code:
            self.verboseprint(response_code)
            self.bad_page_count += 1
            df.loc[0,'status'] = 'Error'
            df.loc[0,'message_id'] = message_id
            return df

        tag = soup.find_all(id="ctl00_CP1_mbdy_dv")
        if tag:
            try:
                body = tag[0].text.strip()
                tb = TextBlob(body)
                sp, ss = tb.sentiment
                ihub_code = soup.find_all(id="ctl00_CP1_bbc1_hlBoard")[0]['href'].replace('/','')
                # post_number = soup.find_all(id="ctl00_CP1_mh1_hlReply")[0].text.split(' ')[-1]
                post_number = int(soup.find_all(id="ctl00_CP1_mh1_tbPost")[0].get_attribute_list('value')[0])
                message_date = pd.to_datetime(soup.find_all(id="ctl00_CP1_mh1_lblDate")[0].text)
                df.loc[0] = ['Active',message_id,ihub_code,post_number,sp,ss,message_date]
            except Exception as e:
                self.verboseprint(message_id, e)
                self.bad_page_count += 1
                df.loc[0,'status'] = 'Error'
                df.loc[0,'message_id'] = message_id

        else:
            # self.verboseprint(message_id, e)
            self.bad_page_count += 1
            df.loc[0,'status'] = 'Error'
            df.loc[0,'message_id'] = message_id
        return df

    def get_queue(self,chunksize=100000):
        already_added = set(self.get_list('sentiment_posts_added'))
        return set(sample(set(range(self.total)) - already_added,chunksize))

    def add_messages(self):

        self.interval_time, self.original_time = time(), time()
        records_processed, records_added = 0, 0
        while records_processed < self.total:
            message_id = randint(1,self.total)
            if not self.get_value('message_id_exists',replacements={'{message_id}':message_id}):
                self.message_to_db(message_id)
            records_processed += 1
            self.status_update(records_processed, self.total)

    def message_to_db(self, message_id):
        try:
            df = self.get_message_page(message_id)
            self.to_table(df,'ihub.message_sentiment')
            sleep(randint(1, 2))
        except Exception as e:
            print(e)
            sleep(60)


    def add_messages_mp(self, chunksize=10000):

        # https://www.journaldev.com/15631/python-multiprocessing-example
        self.interval_time, self.original_time = time(), time()
        iterations = 0
        num_threads = 4

        # total = len(queue)
        while True:
            procs = []
            message_ids = sample(range(self.total),chunksize)
            with Pool(num_threads) as pool:
                results = pool.map(self.message_to_db,message_ids)

            # for message_id in message_ids:
            #     proc = Process(target=self.message_to_db, args=(message_id,))
            #     procs.append(proc)
            # [x.start() for x in procs]

            iterations += 1
            self.status_update(iterations*chunksize, self.total)


            #     proc.start()
            # for proc in procs:
            #     proc.join()

if __name__ == '__main__':

    # deleted post
    # message_id = 144689607
    # good post
    # message_id = 144689616
    # non-existant-post
    # message_id=154689616
    # other board
    # message_id = 144802665
    # message_id = 77864629

    # message_ids = [77864629,144802665,154689616,144689616,144689607]

    # url = f"https://investorshub.advfn.com/boards/read_msg.aspx?message_id={message_id}"
    # r = requests.get(url)
    # content = r.content
    # soup = BeautifulSoup(content, "lxml")
    # d = pd.to_datetime(soup.find_all(id="ctl00_CP1_mh1_lblDate")[0].text)

    s = IhubSentiment()
    s.add_messages()
    # s.add_messages_mp(chunksize=100)
    # x = s.get_queue()
    # s.add_messages()
    # for message_id in message_ids:
    #     df = s.get_message_page(message_id)
    #     print(df)
        # url = "https://investorshub.advfn.com/boards/read_msg.aspx?message_id=144689607"
        # url = "https://investorshub.advfn.com/boards/read_msg.aspx?message_id=144689616"
