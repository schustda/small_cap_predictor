from src.data.ihub_data import IhubData
from src.data.stock_data import StockData
from src.data.combine_data import CombineData
from src.general_functions import GeneralFunctions
import subprocess
from getpass import getpass
from time import gmtime,sleep,time

class Update(IhubData,StockData,CombineData):

    def __init__(self,email_address,password):
        super().__init__()
        self.email_address = email_address
        self.password = password

    def _update(self):

        ihub = IhubData(
            # delay=True,
            # verbose=1
            )
        ihub.pull_posts()
        del ihub

        sd = StockData()
        sd.update_stock_data()
        del sd

        cd = CombineData()
        cd.compile_data()
        del cd

    def daily_update(self):
        # can start program at any time, but will only run between 1-2am MST
        while gmtime().tm_hour != 6:
            sleep(3600)
        while True:
            try:
                interval_time = time()
                self._update()
            except Exception as e:
                self.send_email('error',str(e))
            sleep(60*60*24-(time()-interval_time))


if __name__ == '__main__':

    username = input('Gmail Username: ')
    password = getpass(prompt='Gmail Password: ')
    if username.count('@') < 1:
        username += '@gmail.com'

    Update(username,password).daily_update()
