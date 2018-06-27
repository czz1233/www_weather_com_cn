# -*- coding: utf-8 -*-
import requests
from bs4 import BeautifulSoup
import time
import re
import pandas as pd
import numpy as np
from sqlalchemy import create_engine

class Weather:

    def __init__(self):
        self.h2={'Accept': 'application/javascript, */*; q=0.8',
                 'Accept-Encoding': 'gzip, deflate',
                 'Accept-Language': 'zh-CN',
                 'Connection': 'Keep-Alive',
#                 'Cookie': 'Hm_lpvt_080dabacb001ad3dc8b9b9049b36d43b=1529975999; Wa_lpvt_1=1529975999; Hm_lvt_080dabacb001ad3dc8b9b9049b36d43b=1529973448; vjuids=40424efab.16439861e21.0.7c32b03667798; vjlast=1529973448.1529973448.30; UM_distinctid=16439861ea0af0-0eee6bf2b1a7a9-20d1644-115bc0-16439861ea1b27; f_city=%E5%8C%97%E4%BA%AC%7C101010100%7C; Wa_lvt_1=1529973448',
                 'Host': 'd1.weather.com.cn',
                 'Referer': 'http://www.weather.com.cn/weather1d/101010100.shtml',
                 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/46.0.2486.0 Safari/537.36 Edge/13.10586'
                 }
        self.weather={}
        self.warning={}
        self.bs='zhanwei'
        self.city_list = []
        self.weather_csv = pd.DataFrame()
        self.df_city_pro = '1'
        self.beijing = '1'



    def get_city_list(self):
        f = open(r"C:\Users\马超\Desktop\2.txt",'r') 
        city_list_xml = f.read()
        f.close()
        bs_city_list = BeautifulSoup(city_list_xml,'lxml')
        dic_pro={}
        dic_city={}
        for i in bs_city_list.find_all('province'):
            for j in i.find_all('city'):
                for k in j.find_all('county'):
                    dic_pro[k.get('weathercode')] = i.get('name')
                    dic_city[k.get('weathercode')] = j.get('name')
        df_city = pd.DataFrame.from_dict(dic_city, orient='index')
        df_city.reset_index(inplace=True)
        df_pro = pd.DataFrame.from_dict(dic_pro, orient='index')
        df_pro.reset_index(inplace=True)
        self.df_city_pro = pd.merge(left = df_city,
                                    right = df_pro,
                                    on='index',
                                    how='inner' )
        self.df_city_pro.columns=[['city', 'cit', 'pro']]

        for i in bs_city_list.find_all('county'):
            self.city_list.append(i.get('weathercode'))
        return self.city_list



    def connect_weather(self,city_num):
        url = 'http://www.weather.com.cn/sk_2d/'+str(city_num)+'.html'
        try:
            r=requests.get(url, timeout=5, headers=self.h2)
            r.encoding='utf8'
            self.bs = eval(r.text.replace('var dataSK = ',''))
        except requests.exceptions.RequestException as e:
            time.sleep(3)
            self.connect_weather(city_num)



    def connect_warning(self,city_num):
        url = 'http://www.weather.com.cn/dingzhi/'+str(city_num)+'.html'
        try:
            r=requests.get(url, timeout=5, headers=self.h2)
            r.encoding='utf8'
            self.warning = r.text
            re.findall(re.compile('(var city.*?=)'),self.warning)[0]
#            re.findall(re.compile('(alarm.*?=)'),self.warning)[0]
        except (IndexError,requests.exceptions.RequestException):
            time.sleep(2)
            self.connect_warning(city_num)


    def get_data(self):
        self.weather={}
        self.weather = self.bs
        try:
            a1 = re.findall(re.compile('(var city.*?=)'),self.warning)[0]
            a2 = re.findall(re.compile('(alarm.*?=)'),self.warning)[0]
            self.warning = self.warning.replace(a1,'').replace(a2,'')
            self.warning = self.warning.split(';var ')   
            self.weather['cityname'] = eval(self.warning[0])['weatherinfo']['cityname']
            self.weather['temp_day'] = eval(self.warning[0])['weatherinfo']['temp']
            self.weather['temp_night'] = eval(self.warning[0])['weatherinfo']['tempn']
            self.weather['wd'] = eval(self.warning[0])['weatherinfo']['wd']
            self.weather['ws'] = eval(self.warning[0])['weatherinfo']['ws']    
            if eval(self.warning[1])['w'] != []:
                self.weather['warning'] = eval(self.warning[1])['w'][0]['w13']
                for i in eval(self.warning[1])['w'][1:]:
                    self.weather['warning'] = self.weather['warning']+','+i['w13']
            else:
                self.weather['warning'] = np.NaN
    
            weather_csv_1 = pd.DataFrame.from_dict(self.weather, orient='index').T
            self.weather_csv = self.weather_csv.append(weather_csv_1)
        except IndexError:
            
            self.get_data()


    def get_df(self):          
        self.weather_csv = pd.merge(  left = self.weather_csv,
                                      right = self.df_city_pro,
                                      on='city',
                                      how='left'  )

    def get_csv(self):
        self.weather_csv.to_csv('1.csv',index=False)

    
    def get_sql(self):
        sql = 'postgresql+pg8000://postgres:dd1314@localhost:5432/weatherdata'
        conn = create_engine(sql,encoding='utf8')
        pd.io.sql.to_sql(self.weather_csv,'weather_com_cn',conn,if_exists='append',
                         index=False, chunksize = 2000)



if __name__ == '__main__':

    def run():
        w = Weather()
        l = w.get_city_list()
        t1=time.time()     
        for i in l:
            w.connect_weather(i)
            w.connect_warning(i)
            w.get_data()
            print(i)
        w.get_df()
        w.get_sql()
        t2=time.time()
        print(t2-t1)

    
    while True:
        print(time.strftime('%H:%M:%S',time.localtime(time.time()))+' -- 开始')
        run()
        print(time.strftime('%H:%M:%S',time.localtime(time.time()))+' -- 结束\n\n')
        time.sleep(3600)
#    w.get_csv()
#    df = w.weather_csv












