import smtplib
import pandas as pd
from math import ceil
from time import time
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from os import environ
import boto3
from io import StringIO, BytesIO
from string import Template
import codecs
from PIL import Image
from datetime import datetime

class GeneralFunctions(object):

    def __init__(self, breaks=10):
        self.breaks = breaks
        self.s3_url = 'https://s3.amazonaws.com/small-cap-predictor/'
        self.bucket = 'small-cap-predictor'
        self.s3_resource = boto3.resource('s3',
                aws_access_key_id=environ['AWS_ACCESS_KEY'],
                aws_secret_access_key=environ['AWS_SECRET_ACCESS_KEY'])
        self.s3_client = boto3.client('s3',
                aws_access_key_id=environ['AWS_ACCESS_KEY'],
                aws_secret_access_key=environ['AWS_SECRET_ACCESS_KEY'])
        self.index_col = {'message_board_posts': 0,
                            'stock_prices': 'Date',
                            'ticker_symbols': 'key'}

    def import_from_s3(self,filename,index_col=None):
        obj = self.s3_client.get_object(Bucket=self.bucket,Key=filename+'.csv')
        body = obj['Body']
        csv_string = body.read().decode('utf-8')
        return pd.read_csv(StringIO(csv_string),index_col=index_col)

    def save_to_s3(self,f,filename):
        csv_buffer = StringIO()
        f.to_csv(csv_buffer)
        self.s3_resource.Object('small-cap-predictor',filename+'.csv').put(Body=csv_buffer.getvalue())

    def load_file(self,f):
        for i in range(self.breaks):
            df_load = self.import_from_s3('{0}/{0}{1}'.format(f,i),self.index_col[f])
            if i == 0:
                df = df_load
            else:
                df = pd.concat([df,df_load])
        return df

    def save_file(self,f,df):

        break_lst = list(range(0,df.shape[0],ceil(df.shape[0]/self.breaks)))
        break_lst.append(df.shape[0])
        for i in range(len(break_lst)-1):
            self.save_to_s3(df[break_lst[i]:break_lst[i+1]],'{0}/{0}{1}'.format(f,i))

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

    def _get_html(self,stocks_to_buy):
        f = codecs.open('scripts/daily_update.html','r')
        html = f.read()
        return Template(html).safe_substitute(predictions=stocks_to_buy)

    def save_image_to_s3(self,filepath):
        '''
        filepath within the images folder
        '''
        filepath = 'images/'+filepath
        out_img = BytesIO()
        img = Image.open(filepath)
        filepath = filepath.replace('.jpg','.png')
        img.save(out_img,'PNG')
        out_img.seek(0)
        self.s3_resource.Bucket('small-cap-predictor').put_object(Key=filepath,Body=out_img,ACL='public-read')


    def send_email(self,topic,content):
        msg = MIMEMultipart()
        distribution_list = self.import_from_s3('distribution_list',0)
        distribution_list = distribution_list.email_address.tolist()
        if topic == 'update_symbol':
            subject = "Symbol updated"
            body = 'Ticker symbol {0} changed to {1}'.format(content[0],content[1])
            distribution_list = distribution_list[0]
        elif topic == 'prediction':
            subject = "Daily Small Cap Predictor Summary for "
            body = self._get_html(", ".join(content).upper())
            # body = "Stocks indicating buy signal: {0}".format(", ".join(content).upper())
        elif topic == 'error':
            subject = 'ERROR ALERT'
            body = content
            distribution_list = distribution_list[0]

        subject += str(datetime.today().date())
        msg['Subject'] = subject

        for to_email in distribution_list:
            fromaddr = self.email_address
            toaddr = to_email
            msg['From'] = fromaddr
            msg['To'] = toaddr

            if topic == 'prediction':
                msg.attach(MIMEText(body, 'html'))
            else:
                msg.attach(MIMEText(body, 'plain'))

            server = smtplib.SMTP('smtp.gmail.com', 587)
            server.starttls()
            server.login(fromaddr, self.password)
            text = msg.as_string()
            server.sendmail(fromaddr, toaddr, text)
            server.quit()


if __name__ == '__main__':
    gf = GeneralFunctions()
    gf.send_email('prediction',['asdf'])
