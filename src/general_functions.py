import boto3
import pandas as pd
import matplotlib.pyplot as plt
from math import ceil
from time import time
from os import environ as e
from io import StringIO, BytesIO

class GeneralFunctions(object):

    def __init__(self, breaks=10):
        self.breaks = breaks
        self.s3_url = 'https://s3.amazonaws.com/small-cap-predictor/'
        self.bucket = 'small-cap-predictor'
        self.s3_resource = boto3.resource('s3',
                aws_access_key_id=e['AWS_ACCESS_KEY'],
                aws_secret_access_key=e['AWS_SECRET_ACCESS_KEY'])
        self.s3_client = boto3.client('s3',
                aws_access_key_id=e['AWS_ACCESS_KEY'],
                aws_secret_access_key=e['AWS_SECRET_ACCESS_KEY'])
        self.index_col = {'message_board_posts': 0,
                            'stock_prices': 'Date',
                            'ticker_symbols': 'key',
                            'prediction_log':'prediction'}

    def import_from_s3(self,filename):
        '''
        Downloads the 'filename' file that is stored on S3 bucket
        '''

        obj = self.s3_client.get_object(Bucket=self.bucket,Key=filename+'.csv')
        body = obj['Body']
        csv_string = body.read().decode('utf-8')
        index_col = self.index_col.get(filename,None)
        return pd.read_csv(StringIO(csv_string),index_col=index_col)

    def save_to_s3(self,f,filename):
        '''
        Saves the file 'f', as 'filename' on the S3 bucket
        ** csv files only ***
        '''

        csv_buffer = StringIO()
        f.to_csv(csv_buffer)
        self.s3_resource.Object('small-cap-predictor',filename+'.csv').put(Body=csv_buffer.getvalue())

    def load_file(self,f):
        '''
        Special function needed to load 'message_board_posts' and 'stock_prices'
            files. These have been partitioned into separate chuncks to save
            space.
        '''

        if f in ['message_board_posts','stock_prices']:
            for i in range(self.breaks):
                df_load = self.import_from_s3('{0}/{0}{1}'.format(f,i))
                if i == 0:
                    df = df_load
                else:
                    df = pd.concat([df,df_load])
            return df

        else:
            return self.import_from_s3(f)

    def save_file(self,f,df):
        '''
        Special function needed to save 'message_board_posts' and 'stock_prices'
        files. These have been partitioned into separate chuncks to save
        space.
        '''

        break_lst = list(range(0,df.shape[0],ceil(df.shape[0]/self.breaks)))
        break_lst.append(df.shape[0])
        for i in range(len(break_lst)-1):
            self.save_to_s3(df[break_lst[i]:break_lst[i+1]],'{0}/{0}{1}'.format(f,i))

    def status_update(self,percent):
        '''
        Provides an update every minute on the progress of a given function
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

    def save_image_to_s3(self,filepath):
        '''
        Saves a given image file to the S3 Bucket
        '''

        filepath = 'images/'+filepath
        img_data = BytesIO()
        plt.savefig(img_data, format='png',transparent = True)
        img_data.seek(0)
        self.s3_resource.Bucket(self.bucket).put_object(Key=filepath,
            Body=img_data,ACL='public-read')
        plt.close('all')
