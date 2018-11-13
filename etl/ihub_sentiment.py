import requests
import pandas as pd
from bs4 import BeautifulSoup
from textblob import TextBlob
from src.general_functions import GeneralFunctions
from time import time, sleep
from random import randint, sample
import numpy


class IhubSentiment(GeneralFunctions):

    def __init__(self):
        super().__init__()
        self.bad_page_count = 0

    def get_message_page(self, message_id):

        url = f"https://investorshub.advfn.com/boards/read_msg.aspx?message_id={message_id}"

        r = requests.get(url)
        content = r.content
        soup = BeautifulSoup(content, "lxml")

        response_code = soup.find_all(id="ctl00_CP1_L1")

        columns = ['status','message_id','ihub_code','post_number',
                   'sentiment_polarity','sentiment_subjectivity','message_date']
        df = pd.DataFrame(columns=columns)

        if response_code:
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
                self.bad_page_count += 1
                df.loc[0,'status'] = 'Error'
                df.loc[0,'message_id'] = message_id

        else:
            self.bad_page_count += 1
            df.loc[0,'status'] = 'Error'
            df.loc[0,'message_id'] = message_id
        return df

    def get_queue(self):
        self.total = 144699616
        already_added = set(self.get_list('sentiment_posts_added'))
        return set(range(self.total)) - already_added


    def add_messages(self):

        self.interval_time, self.original_time = time(), time()
        queue = self.get_queue()
        print('Loaded Queue')
        records_processed = 0
        total = len(queue)
        while len(queue):
            message_id = sample(queue,1)[0]
            try:
                df = self.get_message_page(message_id)
                self.to_table(df,'ihub.message_sentiment')
                self.status_update(records_processed, total)
                queue -= set([message_id])
                records_processed += 1
                sleep(randint(2, 8))
            except Exception as e:
                print(e)
                sleep(60)
            # page += 1

        # while bad_page_count < 100:
            # pass

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
    # x = s.get_queue()
    s.add_messages()
    # for message_id in message_ids:
    #     df = s.get_message_page(message_id)
    #     print(df)
        # url = "https://investorshub.advfn.com/boards/read_msg.aspx?message_id=144689607"
        # url = "https://investorshub.advfn.com/boards/read_msg.aspx?message_id=144689616"
