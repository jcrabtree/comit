'''
comit - automatic webscrapper for COMIT hydro data (http://www.comithydro.niwa.co.nz)
Copyright (C) 2013 David Hume, Electricty Authority, New Zealand.

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.

-----------------------------------------------------------------------
This is the comit class.  It is used to connect, login, download, 
convert and save daily hydro inflows and storage in New Zelaand.   

Used with the following crontab:
   
5 7 * * * /usr/bin/python /home/dave/python/comit/comit.py --comit_pass='password' >> /home/dave/python/comit/comit_CRON.log 2>&1
'''

from pandas import *
from pandas.util.testing import set_trace as st
from datetime import datetime
import numpy as np
from bs4 import BeautifulSoup
import mechanize
import cookielib
import os,sys
from datetime import date, datetime, time, timedelta
import calendar
from io import StringIO
import logging
import logging.handlers
import argparse
import EAtools as ea

#############################################################################################################################################################################        
#Setup command line option and argument parsing
#############################################################################################################################################################################        

parser = argparse.ArgumentParser(add_help=False)
parser.add_argument('--comit_host', action="store",dest='comit_host',default='http://www.comithydro.niwa.co.nz')
parser.add_argument('--comit_user', action="store",dest='comit_user',default='ecomhyd12')
parser.add_argument('--comit_pass', action="store",dest='comit_pass')
parser.add_argument('--comit_path', action="store",dest='comit_path',default='/home/dw/comit/')

IPy_notebook = False
if IPy_notebook == False:
    cmd_line = parser.parse_args()
if IPy_notebook == True:
    ea.set_options()
    class cmd_line():
        def __init__(self,comit_host,comit_user,comit_pass,comit_path):
            self.comit_host = comit_host
            self.comit_user = comit_user
            self.comit_pass = comit_pass
            self.comit_path = comit_path
    cmd_line=cmd_line('http://www.comithydro.niwa.co.nz','ecomhyd12','password','/home/humed/python/comit/')

#############################################################################################################################################################################        
#Setup logging
#############################################################################################################################################################################        

formatter = logging.Formatter('|%(asctime)-6s|%(message)s|','%Y-%m-%d %H:%M:%S')
consoleLogger = logging.StreamHandler()
consoleLogger.setLevel(logging.INFO)
consoleLogger.setFormatter(formatter)
logging.getLogger('').addHandler(consoleLogger)
fileLogger = logging.handlers.RotatingFileHandler(filename=cmd_line.comit_path + 'comit.log',maxBytes = 1024*1024, backupCount = 9)
fileLogger.setLevel(logging.ERROR)
fileLogger.setFormatter(formatter)
logging.getLogger('').addHandler(fileLogger)
logger = logging.getLogger('comit Hydro ')
logger.setLevel(logging.INFO)

class comit_scraper():
   
    def __init__(self,comit_host,comit_user,comit_pass,comit_path):
        self.comit_host = comit_host
        self.comit_user = comit_user
        self.comit_pass = comit_pass
        self.comit_path = comit_path
        self.comit_site = self.comit_host + '''/comitweb/request_template.html'''
        self.inflows_names = {'csv':'inflows.csv','pickle':'inflows.pickle'}
        self.storage_names = {'csv':'storage.csv','pickle':'storage.pickle'}
        self.locations = range(1,15)
        self.df_stored = None
        self.inflows = None
        self.br = None
        
    ##############################################################################################################################        
    def date_parser(self,x):       #Date parser for comit data
    ##############################################################################################################################        

        x=x.split(' ')[0]
        return datetime.date(datetime(int(x.split('/')[2]),int(x.split('/')[1]),int(x.split('/')[0])))

    ##############################################################################################################################        
    def get_data(self,location,storage_inflows):       #Data scraper, hard coded form filler and parser for comit data
    ############################################################################################################################## 

        self.enter_comit()  #enter comit for each data pass
        self.br.select_form('test')     #select form 
        self.br['loca'] = [location]  #
        self.br['dura'] = ['365.25']  #
        self.br['dury'] = "1000"
        self.br['mfiy'] = "1932"  
        self.br['mlay'] = "2013"
        self.br['efiy'] = "1932"  
        self.br['elay'] = "2013"
        self.br['todo'] = ["3"]
        #br['quan'] = "3"  #!!This took ages to work out.  Because the name "quan" is used twice we can't set the form as above
        if storage_inflows=='storage':
            controls = self.br.form.controls #Set QUANTITIES, either {"1":INFLOW,"2":CUSUM (GWh),"3":STORED (GWh)} 
            controls[7].value = ["3"]  #STORED (GWh)
        if storage_inflows=='inflows':
            controls = self.br.form.controls #Set QUANTITIES, either {"1":INFLOW,"2":CUSUM (GWh),"3":STORED (GWh)} 
            controls[7].value = ["1"]  #INFLOW
        self.br.set_all_readonly(False)
        self.br['Submit']='Display'
        response = self.br.submit() 
        link = [l for l in self.br.links()][-1]
        self.br.click_link(link)
        response = self.br.follow_link(link).read()
        name = response.split(',')[0]
        bufferIO = StringIO()    #Open a string buffer object, write the POCP database to this then read_csv the data...
        bufferIO.write(unicode(response)) 
        bufferIO.seek(0)
        data = read_csv(bufferIO,skiprows=[0,1,2,3]).rename(columns={'Unnamed: 0':'date'})
        del data['Unnamed: 3']
        data['date']=data.date.map(lambda x: self.date_parser(x))
        data=data.set_index('date')
        #print data
        if storage_inflows=='inflows':
            return data['2013 inflow'],name
        if storage_inflows=='storage':
            return data['2013 stored'],name

    ##############################################################################################################################        
    def enter_comit(self):
    ##############################################################################################################################        

        try:
            self.br = mechanize.Browser()    # Browser
            cj = cookielib.LWPCookieJar()    # Cookie Jar
            self.br.set_cookiejar(cj)        # Browser options
            self.br.set_handle_equiv(True)
            self.br.set_handle_gzip(False)
            self.br.set_handle_redirect(True)
            self.br.set_handle_referer(True)
            self.br.set_handle_robots(False)
            self.br.set_handle_refresh(mechanize._http.HTTPRefreshProcessor(), max_time=1) # Follows refresh 0 but not hangs on refresh > 0
            self.br.addheaders = [('User-agent', 'Mozilla/5.0 (X11; U; Linux i686; en-US; rv:1.9.0.1) Gecko/2008071615 Fedora/3.0.1-1.fc9 Firefox/3.0.1')]
            self.br.add_password(self.comit_host,self.comit_user,self.comit_pass)
            r=self.br.open(self.comit_host)
            r=self.br.open(self.comit_site)
        except:
            error_text = "Unable to logged into comit Hydro"
            logger.error(error_text.center(msg_len,'*'))

    ##############################################################################################################################        
    def get_all_data(self):
    ##############################################################################################################################        

        self.storage = {}
        self.inflows = {}
        start_text = 'Scraping comit Hydro @ ' + self.comit_site
        logger.info(start_text.center(msg_len,' '))
        for loci in self.locations:
            data_storage,name_storage = self.get_data(str(loci),'storage')
            data_inflows,name_inflows = self.get_data(str(loci),'inflows')
            self.storage[name_storage] = data_storage
            self.inflows[name_inflows] = data_inflows

    ##############################################################################################################################        
    def df_the_data(self):
    ##############################################################################################################################        

        df_storage = DataFrame(self.storage)
        df_inflows = DataFrame(self.inflows)
        df_storage = df_storage.rename(columns = dict(zip(df_storage.columns,df_storage.columns.map(lambda x: x.split(' ')[0]))))
        df_inflows = df_inflows.rename(columns = dict(zip(df_inflows.columns,df_inflows.columns.map(lambda x: x.split(' ')[0]))))
        self.df_storage = df_storage.shift(-1).ix[:-1,:]   #shifting time stamp to previous day
        self.df_inflows = df_inflows.shift(-1).ix[:-1,:]

    ##############################################################################################################################        
    def to_pickle_and_csv(self):
    ##############################################################################################################################        

        self.df_storage.to_pickle(self.comit_path + 'data/' + self.storage_names['pickle'])
        self.df_inflows.to_pickle(self.comit_path + 'data/' + self.inflows_names['pickle'])
        self.df_storage.to_csv(self.comit_path + 'data/' + self.storage_names['csv'])
        self.df_inflows.to_csv(self.comit_path + 'data/' + self.inflows_names['csv'])
        done_text = 'GOT comit Hydro data, saved to ' + self.comit_path + 'data/'
        logger.info(done_text.center(msg_len,' '))

##############################################################################################################################        
#Start the programme
##############################################################################################################################        
msg_len = 88

if __name__ == '__main__':
    cs = comit_scraper(cmd_line.comit_host,cmd_line.comit_user,cmd_line.comit_pass,cmd_line.comit_path) #run instance
    cs.get_all_data() #get all the data!
    cs.df_the_data()  #data frame the data
    cs.to_pickle_and_csv()



