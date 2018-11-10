import requests
import pandas as pd
from bs4 import BeautifulSoup
from textblob import TextBlob
from src.general_functions import GeneralFunctions
from time import time

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
        columns = ['message_id','ihub_code','post_number','sentiment_polarity','sentiment_subjectivity']
        df = pd.DataFrame(columns=columns)
        if response_code:
            self.bad_page_count += 1
            return df
        tag = soup.find_all(id="ctl00_CP1_mbdy_dv")
        if tag:
            body = tag[0].text.strip()
            tb = TextBlob(body)
            sp, ss = tb.sentiment
            ihub_code = soup.find_all(id="ctl00_CP1_bbc1_hlBoard")[0]['href'].replace('/','')
            post_number = soup.find_all(id="ctl00_CP1_mh1_hlReply")[0].text.split(' ')[-1]
            df.loc[0] = [message_id,ihub_code,post_number,sp,ss]
        else:
            self.bad_page_count += 1
        return df

    def add_messages(self):

        self.interval_time, self.original_time = time(), time()

        message_id = 0
        start_page = 0
        total = 144699616

        for i in range(total):
            df = self.get_message_page(i)
            self.to_table(df,'ihub.message_sentiment')
            self.status_update(i, total)
            sleep(randint(2, 8))

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

    s = IhubSentiment()
    s.add_messages()
    # df = s.get_message_page(message_id)

        # url = "https://investorshub.advfn.com/boards/read_msg.aspx?message_id=144689607"
        # url = "https://investorshub.advfn.com/boards/read_msg.aspx?message_id=144689616"
