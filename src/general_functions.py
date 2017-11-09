import smtplib
import pandas as pd
from math import ceil
from time import time
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart



class GeneralFunctions(object):

    def __init__(self, breaks=10):
        self.breaks = breaks
        self.index_col = {'message_board_posts': 0,
                            'stock_prices': 'Date'}

    def load_file(self,f):
        for i in range(self.breaks):
            df_load = pd.read_csv('data/tables/{0}/{0}{1}.csv'.format(f,i),
                index_col=self.index_col[f])
            if i == 0:
                df = df_load
            else:
                df = pd.concat([df,df_load])
        return df

    def save_file(self,f,df):

        break_lst = list(range(0,df.shape[0],ceil(df.shape[0]/self.breaks)))
        break_lst.append(df.shape[0])
        for i in range(len(break_lst)-1):
            df[break_lst[i]:break_lst[i+1]].to_csv('data/tables/{0}/{0}{1}.csv'.format(f,i))

    def status_update(self,percent):
        '''
        Method displays progress of retriving message board posts
        '''
        # Display update ever 60 seconds
        if time() > self.interval_time + 60:
            time_elapsed = time() - self.original_time
            a = int(percent/2)
            b = 50-a
            if percent == 0:
                percent = 0.5
            min_rem = int(time_elapsed/percent*(100-percent)/60)
            print ('|{0}{1}| {2}% - {3} minute(s) remaining'.format(a*'=',b*'-',str(percent),str(min_rem)))
            self.interval_time = time()

    def send_email(self,topic,content):
        msg = MIMEMultipart()
        if topic == 'update_symbol':
            msg['Subject'] = "Symbol updated"
            body = 'Ticker symbol {0} changed to {1}'.format(content[0],content[1])
        elif topic == 'prediction':
            msg['Subject'] = "BUY SIGNAL ALERT"
            body = "Stocks indicating buy signal: {0}".format(", ".join(content).upper())
        elif topic == 'error':
            body = content

        fromaddr = self.email_address
        toaddr = self.email_address
        msg['From'] = fromaddr
        msg['To'] = toaddr

        msg.attach(MIMEText(body, 'plain'))

        server = smtplib.SMTP('smtp.gmail.com', 587)
        server.starttls()
        server.login(fromaddr, self.password)
        text = msg.as_string()
        server.sendmail(fromaddr, toaddr, text)
        server.quit()


if __name__ == '__main__':
    sf = GeneralFunctions()
    df = sf.load_file('message_board_posts')
    sp = sf.load_file('stock_prices')
