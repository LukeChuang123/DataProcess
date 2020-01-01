# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import datetime

import Data_uploader

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
input("continue")

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

weather_data_df.to_excel("需要的天氣資料.xlsx")
