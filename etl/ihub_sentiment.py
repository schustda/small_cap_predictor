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
    def __init__(self, verbose=0):
        super().__init__(verbose)
        self.total = 153009616
        self.bad_page_count = 0
        self.headers = {
            "User-Agent": 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/35.0.1916.47 Safari/537.36'
        }


    def get_message_page(self, message_id):
        """ Web scrapes post information from the specified message_id

        Parameters:
            message_id (int): The message number to extract data from
        """

        headers = {
            "User-Agent": 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/35.0.1916.47 Safari/537.36'
        }
        url = f"https://investorshub.advfn.com/boards/read_msg.aspx?message_id={message_id}"
        r = requests.get(url, timeout=100, headers=headers)
        content = r.content
        soup = BeautifulSoup(content, "lxml")
        response_code = soup.find_all(id="ctl00_CP1_L1")
        columns = [
            "status",
            "message_id",
            "ihub_code",
            "post_number",
            "sentiment_polarity",
            "sentiment_subjectivity",
            "message_date",
        ]
        df = pd.DataFrame(columns=columns)

        if response_code:
            self.verboseprint(response_code)
            self.bad_page_count += 1
            df.loc[0, "status"] = "Error"
            df.loc[0, "message_id"] = message_id
            return df

        tag = soup.find_all(id="ctl00_CP1_mbdy_dv")
        if tag:
            try:
                body = tag[0].text.strip()
                tb = TextBlob(body)
                sp, ss = tb.sentiment
                ihub_code = soup.find_all(id="ctl00_CP1_bbc1_hlBoard")[0][
                    "href"
                ].replace("/", "")
                # post_number = soup.find_all(id="ctl00_CP1_mh1_hlReply")[0].text.split(' ')[-1]
                post_number = int(
                    soup.find_all(id="ctl00_CP1_mh1_tbPost")[0].get_attribute_list(
                        "value"
                    )[0]
                )
                message_date = pd.to_datetime(
                    soup.find_all(id="ctl00_CP1_mh1_lblDate")[0].text
                )
                df.loc[0] = [
                    "Active",
                    message_id,
                    ihub_code,
                    post_number,
                    sp,
                    ss,
                    message_date,
                ]
            except Exception as e:
                self.verboseprint(message_id, e)
                self.bad_page_count += 1
                df.loc[0, "status"] = "Error"
                df.loc[0, "message_id"] = message_id

        else:
            # self.verboseprint(message_id, e)
            self.bad_page_count += 1
            df.loc[0, "status"] = "Error"
            df.loc[0, "message_id"] = message_id
        return df

    def get_queue(self, chunksize=1000):
        start = randint(1,self.total)
        end = start + chunksize
        replacements = {'[START]': start, '[END]': end}
        already_added = set(self.get_list("sentiment_posts_added", replacements = replacements))
        queue = set(range(start, end+1)) - already_added
        self.verboseprint(f'Adding {len(queue)} posts between {start} and {end}') 
        return set(range(start, end+1)) - already_added

    def page_response(self, message_id, timeout = 10):
        '''
        Use the requests and beautifulsoup modules to make the request to the website
        '''
        payload = {'message_id' : message_id}
        url = f"https://investorshub.advfn.com/boards/read_msg.aspx"
        r = requests.get(url=url, timeout=timeout, headers=self.headers, params = payload)
        return BeautifulSoup(r.content, "lxml")


    def get_message_data(self, message_id):
        """ Web scrapes post information from the specified message_id

        Parameters: message_id (int): The message number to extract data from
        """

        site_id_codes = {
            "active": { 
                # This is where the data from the post will originate from
                'message' : "ctl00_CP1_mbdy_dv"
            
                # Where to extract the post number for the specific board the message is on
                ,'post_number' : "ctl00_CP1_mh1_tbPost"
            
                # Finds the date the message was posted
                ,'message_date' : "ctl00_CP1_mh1_lblDate"
            
                # extracting the code of the specific ticker or board
                ,'ihub_code' : "ctl00_CP1_bbc1_hlBoard"
            }
            ,"error": { 
                # If this id has any data, it means the post is erroneous
                'missing_post' : "ctl00_CP1_L1"

                # If a message was deleted, the message will appear here
                ,'deleted_post' : 'ctl00_CP1_na'
            }
        }

        try:
            soup = self.page_response(message_id)
        except requests.exceptions.Timeout:
            return {}

        output = {'message_id': message_id, 'status': 'Active'}


        for id_type, id_code in site_id_codes['error'].items():
            data = soup.find(id=id_code)
            if data:
                if id_type == 'missing_post':
                    output['status'] = 'Error'
                    output['error_message'] = data.text.strip()
                    return output
                if id_type == 'deleted_post':
                    output['status'] = 'Error'
                    output['error_message'] = data.text.strip()
                    return output

        for id_type, id_code in site_id_codes['active'].items():
            data = soup.find(id=id_code)
            if data:
                if id_type == 'message':
                    body = data.text.strip()
                    tb = TextBlob(body)
                    output['sentiment_polarity'], output['sentiment_subjectivity'] = tb.sentiment
                if id_type == 'post_number':
                    # Number from string, from: https://tinyurl.com/unazhvl
                    output['post_number'] = int(data.get('value',0))
                if id_type == 'message_date':
                    output['message_date'] = pd.to_datetime(data.text)
                if id_type == 'ihub_code':
                    output['ihub_code'] = data["href"].replace("/", "")
        return output



    def add_messages(self):

        self.interval_time, self.original_time = time(), time()
        records_processed, records_added = 0, 0
        while records_processed < self.total:
            for message_id in self.get_queue():
                try:
                    self.verboseprint(message_id)
                    self.message_to_db(message_id)
                    sleep(randint(2,4))
                except Exception as e:
                    print(e)
                    sleep(60)
            records_processed += 1
            self.status_update(records_processed, self.total)

    def message_to_db(self, message_id):
        data = self.get_message_data(message_id)
        self.to_table(pd.DataFrame([data]), "ihub.message_sentiment")

if __name__ == "__main__":

    # deleted post
    # message_id = 144689607
    # good post
    # message_id = 144689616
    # non-existant-post
    # message_id = 254689616
    # other board
    # message_id = 144802665
    # message_id = 77864629
    # message_id = 153009616

    # message_ids = [77864629,144802665,154689616,144689616,144689607]

    # url = f"https://investorshub.advfn.com/boards/read_msg.aspx?message_id={message_id}"
    # r = requests.get(url)
    # content = r.content
    # soup = BeautifulSoup(content, "lxml")
    # d = pd.to_datetime(soup.find_all(id="ctl00_CP1_mh1_lblDate")[0].text)

    s = IhubSentiment(verbose = 1)
    # data = s.get_message_data(message_id)
    # s.message_to_db(message_id)
    # s.add_messages()
    # s.add_messages_mp(chunksize=100)
    # x = s.get_queue()
    # s.add_messages()
    # for message_id in message_ids:
    #     df = s.get_message_page(message_id)
    #     print(df)
    # url = "https://investorshub.advfn.com/boards/read_msg.aspx?message_id=144689607"
    # url = "https://investorshub.advfn.com/boards/read_msg.aspx?message_id=144689616"
