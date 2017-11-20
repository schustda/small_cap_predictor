from src.ihub_data import IhubData
from src.stock_data import StockData
from src.combine_data import CombineData
from emails.send_emails import Email
from time import gmtime,sleep,time

class Update(Email):

    def __init__(self,now=False,delay=True):
        super().__init__()
        self.now = now
        self.delay = delay

    def _update(self):

        ihub = IhubData(delay=self.delay,verbose=1)
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
        if not self.now:
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

    UpdateData().daily_update()
