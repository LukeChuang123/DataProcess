# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import datetime

import Data_uploader

#read temp file to dataframe
temp_df = pd.read_excel("temp.xlsx")
print(temp_df)

#read 新各球場對應觀測站.xlsx to dataframe
stadium_station_df = pd.read_excel("新各球場對應觀測站.xlsx").set_index("stadium")
stadium_station_df["start_time"] = pd.to_datetime(stadium_station_df["start_time"])
stadium_station_df["end_time"] = pd.to_datetime(stadium_station_df["end_time"])
print(stadium_station_df)
input("continue")

#read 需要的天氣資料.xlsx to dataframe
weather_data = pd.read_excel("需要的天氣資料.xlsx").set_index(["DATE","STATION"])
# print(weather_data.columns)
# input("continue")
print(weather_data)

#加入星期的dummy coding
day_dummy_df = pd.get_dummies(temp_df["DAY"]).reset_index()
temp_df = pd.concat([temp_df.drop("DAY",axis=1),day_dummy_df.drop("index",axis=1)],axis=1)
print(temp_df)

#將天氣資料併入temp_df
weather_data_df = None
is_first_time = True
temp_df.replace("斗六","雲林",inplace = True)
for data_index in range(temp_df.shape[0]):
    data = temp_df.iloc[data_index,:]
    date = data.loc["DATE"]
    stadium = data.loc["STADIUM"]
    station_df = stadium_station_df.loc[stadium]
    try:
        #如果那場比賽的球場對應多個觀測站
        for station_index in range(station_df.shape[0]):
            station = station_df.iloc[station_index].loc["station"]
            print("station",station)
            start_date  = station_df.iloc[station_index].loc["start_time"] 
            end_date  = station_df.iloc[station_index].loc["end_time"] 
            if(start_date <= date and date <= end_date):
                if(is_first_time):
                    weather_data_df = weather_data.loc[(date,station)]
                    is_first_time = False
                else:
                    weather_data_df = pd.concat([weather_data_df,weather_data.loc[(date,station)]],axis = 1)
                break
    except Exception as e:
        #如果那場比賽的球場指對應一個觀測站
        print(e)
        station = station_df.loc["station"]
        print("station",station)
        start_date  = station_df.loc["start_time"] 
        end_date  = station_df.loc["end_time"] 
        # print(weather_data.loc[(date,station)])
        if(is_first_time):
            weather_data_df = weather_data.loc[(date,station)]
            is_first_time = False
        else:
            weather_data_df = pd.concat([weather_data_df,weather_data.loc[(date,station)]],axis = 1)
    # input("continue")
# weather_data_df = weather_data_df.reset_index()
new_cols = [i for i in range(temp_df.shape[0])]
weather_data_df.columns = new_cols
weather_data_df = weather_data_df.transpose()
print(weather_data_df)
weather_data_df.to_excel("每場天氣資料-temp.xlsx")

temp_df = pd.concat([temp_df,weather_data_df],axis=1)
print(temp_df)
temp_df.to_excel("temp2.xlsx")
