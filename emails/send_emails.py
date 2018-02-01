from os import environ as e
from string import Template
from datetime import datetime
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from src.general_functions import GeneralFunctions
import codecs
import smtplib

class Email(GeneralFunctions):

    def __init__(self):
        super().__init__()
        self.email_address = 'smallcappredictor@gmail.com'
        self.email_password = e['email_password']
        self.distribution_list = self.load_file('distribution_list').email_address.tolist()
        self.msg_type = {'update_symbol':'plain','error':'plain','prediction':'html'}

    def _load_html(self,buy):
        '''
        The content of the 'prediction' email is generated through an html file.
            This function loads the file.
        '''

        f = codecs.open('emails/html/daily_update.html','r')
        if buy == None:
            buy = ''
        html = f.read()
        return Template(html).safe_substitute(predictions=buy)

    def _email_specifics(self,topic,content):
        '''
        Returns the subject, body, and recipients of the specified email
        '''

        if topic == 'update_symbol':
            subject = "Symbol updated"
            body = 'Ticker symbol {0} changed to {1}'.format(content[0],content[1])
            to_lst = [self.distribution_list[0]]
        elif topic == 'prediction':
            subject = "Daily Small Cap Predictor Summary for {0}".format(str(datetime.today().date()))
            body = self._load_html(", ".join(content).upper())
            to_lst = self.distribution_list
        elif topic == 'error':
            subject = 'ERROR ALERT'
            body = content
            to_lst = [self.distribution_list[0]]

        return subject, body, to_lst

    def send_email(self,topic,content):
        '''
        send email currenty supports three different 'topics' that each require
            a different type of content. The email will come from the designated
            email address for the project 'smallcappredictor@gmail.com'

        * topic: update_symbol
            * companies or organizations may change their ticker symbol for any
                number of reasons. This email will update the user when this occurs
            * content: list of two elements. First element is former symbol,
                second element is the new symbol
        * topic: prediction
            * This email topic sends an email update to everyone in the
                distribution list that contains information on predictions
                made by the model
            * Content: list of n-length. Each element in the list is a 'buy'
                signal as indicated by the model
        * topic: error
            * This is to notify the user of any errors that occur. The intent is
                for the user to know that an a script running on AWS is no
                longer running
            Content: string, the error message
        '''

        subject, body, distribution_list = self._email_specifics(topic,content)
        msg = MIMEMultipart()
        msg['Subject'] = subject
        msg['From'] = self.email_address

        for to_email in distribution_list:
            msg['To'] = to_email
            msg.attach(MIMEText(body, self.msg_type[topic]))
            server = smtplib.SMTP('smtp.gmail.com', 587)
            server.starttls()
            server.login(self.email_address, self.email_password)
            text = msg.as_string()
            server.sendmail(self.email_address, to_email, text)
            server.quit()
