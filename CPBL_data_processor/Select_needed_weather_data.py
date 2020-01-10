# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import datetime 
import time 

import Data_uploader

import sys

#處理空值時，找前後兩天的非空值資料然後算平均填補空值
def find_all_not_null_date_and_data(weather_data_df,col_name,station,is_null_date,deltadays,dates,datas,date0_found=False):
    date = is_null_date + datetime.timedelta(days= deltadays)
    try:
        data = weather_data_df.loc[(str(date),station),col_name].tolist()[0]
    except:
        data = None
    if(not data == None):
        # print(type(data))
        # print(data)
        dates.append(date)
        datas.append(data)
        if(date0_found):
            pass
        else:
            find_all_not_null_date_and_data(weather_data_df,col_name,station,date,deltadays,dates,datas,date0_found=True) 
    else:
        pass
        # find_all_not_null_date_and_data(weather_data_df,col_name,station,date,deltadays,date0_found,dates,datas)

error = []

# MySQL的使用者：root, 密碼:147369, 埠：3306,資料庫：mydb
engine = create_engine('mysql+pymysql://root:Lc-20332895-@localhost:3306/'+"cpbl_whole_data")
conn = engine.connect()
# 查詢語句，選出employee表中的所有資料
sql = '''
select * from weather_bureau_data;
'''
# read_sql_query的兩個引數: sql語句， 資料庫連線
weather_table = pd.read_sql_query(sql, engine)
sql = '''
select * from weather_bureau_data_append;
'''
# read_sql_query的兩個引數: sql語句， 資料庫連線，將兩個天氣資料表格合併
weather_table = pd.concat([weather_table,pd.read_sql_query(sql, engine)],axis=0).reset_index().set_index("STATION")
weather_table["DATE"]=pd.to_datetime(weather_table["DATE"])
weather_table = weather_table[["DATE", "STADIUM",  "氣溫(℃)", "相對溼度(%)", "降水量(mm)"]]
print(weather_table)
#將含有非數值資料的列刪掉
weather_table = weather_table[~weather_table[["DATE", "STADIUM",  "氣溫(℃)", "相對溼度(%)", "降水量(mm)"]].isin(['/\xa0','X\xa0','T\xa0'])]
print(weather_table)
weather_table[["氣溫(℃)", "相對溼度(%)", "降水量(mm)"]] = weather_table[["氣溫(℃)", "相對溼度(%)", "降水量(mm)"]].astype("float64")
print(weather_table.dtypes)
# input("continue")

#read 新各球場對應觀測站.xlsx to dataframe
stadium_station_df = pd.read_excel("新各球場對應觀測站.xlsx")
print(stadium_station_df)

weather_data_df = None
is_first_time = True
for station_data_index in range(stadium_station_df.shape[0]):
    station_data = stadium_station_df.iloc[station_data_index,:]
    station = station_data.loc["station"]
    start_date = station_data.loc["start_time"]
    end_date = station_data.loc["end_time"]
    print(type(end_date))
    weather_station_df = weather_table.loc[station,:]
    # print(weather_station_df)
    # print(weather_station_df["DATE"])
    needed_weather_data = weather_station_df[(weather_station_df["DATE"]<=end_date) & (weather_station_df["DATE"]>=start_date)]
    needed_weather_data = needed_weather_data.groupby("DATE").mean().round(2)
    needed_weather_data.insert(0,"STATION",station)
    print(needed_weather_data)
    if(is_first_time):
        weather_data_df = needed_weather_data
        is_first_time = False
    else:
        weather_data_df = pd.concat([weather_data_df,needed_weather_data],axis = 0)
    # input("continue")
print(weather_data_df)

#處理空值:將空值填入前兩天數值的平均
weather_data_df.set_index("STATION",drop=False,append=True,inplace=True)
# is_null_table = weather_data_df.isnull()
is_null_table = pd.concat([weather_data_df["STATION"],weather_data_df.loc[:,["氣溫(℃)", "相對溼度(%)", "降水量(mm)"]].isnull()],axis=1)
col_list = list(is_null_table.columns)
# print(col_list)
is_null_columns = list(weather_data_df.isnull().any())
print(is_null_columns)
#將同一天各觀測站的資料排在一起以利後續查找資料
weather_data_df.sort_index(level=0,inplace=True)
print(weather_data_df)
print(weather_data_df.index)
print(is_null_table)

for col_index in range(len(is_null_columns)):
    #判斷有null值欄位
    if(is_null_columns[col_index]):
        col_name = col_list[col_index] 
        print("正在處理空值的欄位:",col_name)
        null_data = is_null_table[is_null_table[col_name].isin([True])]
        print(null_data)
        input("continue")
        date_list = list(null_data.index.get_level_values(0))
        for date_index in range(null_data.shape[0]):
            station = null_data.iloc[date_index]["STATION"]
            print("處理空值的觀測站:",station)

            is_null_date = datetime.datetime.strptime(str(date_list[date_index]),"%Y-%m-%d %H:%M:%S")
            print("有空值的日期:",is_null_date)

            dates=[]
            datas=[]
            #分往前和往後找非Null的值(利用deltadays=-1 or +1)
            for deltadays in [-1,1]:
                #先找離is_null_date近的再找遠的
                find_all_not_null_date_and_data(weather_data_df,col_name,station,is_null_date,deltadays,dates,datas,date0_found=False)

            try:
                print("上上一次無空值的日期",dates[1],"數值",datas[1],"\n")
                print("上一次無空值的日期",dates[0],"數值",datas[0],"\n")
                print("下一次無空值的日期",dates[2],"數值",datas[2],"\n")
                print("下下一次無空值的日期",dates[3],"數值",datas[3],"\n")
            except Exception as e:
                error.append(e)

            # the_day_before_lastdate = date + datetime.timedelta(days=-2)
            # lastdate = date + datetime.timedelta(days=-1)
            # nextdate = date + datetime.timedelta(days=1)
            # the_day_next_nextdate = date + datetime.timedelta(days=2)

            # print("上上一次無空值的日期",the_day_before_lastdate,"數值")
            # print("上一次無空值的日期",lastdate,"數值")
            # pr
            # int("下一次無空值的日期",lastdate,"數值")
            # print("下下一次無空值的日期",the_day_before_lastdate,"數值")

            # the_day_before_lastdate_data = weather_data_df.loc[(str(the_day_before_lastdate),station),col_name]
            # print(the_day_before_lastdate_data) 
            # lastdate_data = weather_data_df.loc[(str(lastdate),station),col_name]
            # print(lastdate_data) 
            # nextdate_data = weather_data_df.loc[(str(lastdate),station),col_name]
            # print(nextdate_data)
            # the_day_before_nextdate_data = weather_data_df.loc[(str(the_day_before_lastdate),station),col_name]
            # print(the_day_before_nextdate_data)  
            #防呆設計:如果兩個df抓到的資料不同則退出程序，拋出異常
            # if(weather_data_df.iloc[date_index]["STATION"] == station):
            #     print("hello")
            #     try:
            #         sys.exit(0)
            #     except:
            #         print("two data not match")
            # the_day_before_yesterday_col = weather
            # yesterday_col
        input("continue")
    else:
        continue

for e in error:
    print(e,"\n")
# for col in null_table.columns:
    
# for data_index in range(weather_data_df.shape[0]):
    

# weather_data_df.to_excel("需要的天氣資料.xlsx")
