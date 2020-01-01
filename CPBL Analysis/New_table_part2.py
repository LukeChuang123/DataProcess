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
print(stadium_station_df)

#read 需要的天氣資料.xlsx to dataframe
weather_data = pd.read_excel("需要的天氣資料.xlsx").set_index("DATE")
print(weather_data)

#加入星期的dummy coding
day_dummy_df = pd.get_dummies(temp_df["DAY"]).reset_index()
temp_df = pd.concat([temp_df.drop("DAY",axis=1),day_dummy_df.drop("index",axis=1)],axis=1)
print(temp_df)

#將天氣資料併入temp_df
date_list = list(temp_df["DATE"])
stadium_list = list(temp_df["STADIUM"])

print(date_list)
# weather_data_df = weather_data.loc[]
print(weather_data_df)



weather_data_df = None
is_first_time = True
for data_index in range(temp_df.shape[0]):
    data = temp_df.iloc[data_index,:]
    stadium = data.loc["STADIUM"]
    stations = stadium_station_df.loc[stadium]
    for station in stations:
    date = data.loc["DATE"]
    print(date)