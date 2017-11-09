from src.data.ihub_data import IhubData
from src.data.stock_data import StockData
from src.data.combine_data import CombineData
from src.general_functions import GeneralFunctions
import subprocess
from getpass import getpass
from time import gmtime,sleep,time

class Update(GeneralFunctions):

    def __init__(self,email_address,password):
        self.email_address = email_address
        self.password = password

    def _update(self):
        IhubData(verbose=1,email=self.email_address,
        password=self.password,delay=True).pull_posts()
        StockData().update_stock_data()
        CombineData().compile_data()

    def daily_update(self):
        # can start program at any time, but will only run between 1-2am MST
        while gmtime().tm_hour != 6:
            sleep(3600)
        while True:
            try:
                interval_time = time()
                rc = subprocess.call('scripts/git_pull.sh',shell=True)
                self._update()
                rc = subprocess.call('scripts/git_add_data.sh',shell=True)
            except Exception as e:
                self.send_email('error',str(e))
            sleep(60*60*24-(time()-interval_time))


if __name__ == '__main__':

    username = input('Gmail Username: ')
    password = getpass(prompt='Gmail Password: ')
    if username.count('@') < 1:
        username += '@gmail.com'

    Update(username,password).daily_update()
