from os import environ as e
from src.general_functions import GeneralFunctions
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import codecs
from string import Template
from datetime import datetime
import smtplib




class Email(GeneralFunctions):

    def __init__(self):
        super().__init__()
        self.email_address = 'smallcappredictor@gmail.com'
        self.email_password = e['email_password']
        self.distribution_list = self.import_from_s3('distribution_list').email_address.tolist()
        self.msg_type = {'update_symbol':'plain','error':'plain','prediction':'html'}

    def _load_html(self,buy):

        # f = codecs.open('emails/html/daily_update.html','r')
        f = codecs.open('local/htmltemplate.html','r')
        if buy == None:
            buy = ''
        html = f.read()
        return Template(html).safe_substitute(predictions=buy)

    def _email_specifics(self,topic,content):

        if topic == 'update_symbol ':
            subject = "Symbol updated"
            body = 'Ticker symbol {0} changed to {1}'.format(content[0],content[1])
            to_lst = self.distribution_list[0]
        elif topic == 'prediction':
            subject = "Daily Small Cap Predictor Summary for {0}".format(str(datetime.today().date()))
            body = self._load_html(", ".join(content).upper())
            to_lst = self.distribution_list
        elif topic == 'error':
            subject = 'ERROR ALERT'
            body = content
            to_lst = self.distribution_list[0]

        return subject, body, to_lst

    def send_email(self,topic,content):

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
