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
# weather_table = pd.read_sql_query(sql, engine)
sql = '''
select * from weather_bureau_data_append;
'''
# read_sql_query的兩個引數: sql語句， 資料庫連線，將兩個天氣資料表格合併
# weather_table = pd.concat([weather_table,pd.read_sql_query(sql, engine)],axis=0)

#read temp file to dataframe
temp_df = pd.read_excel("temp.xlsx")
print(temp_df)

#read 新各球場對應觀測站.xlsx to dataframe
stadium_station_df = pd.read_excel("新各球場對應觀測站.xlsx")
print(stadium_station_df)

#加入星期的dummy coding
day_dummy_df = pd.get_dummies(temp_df["DAY"]).reset_index()
temp_df = pd.concat([temp_df.drop("DAY",axis=1),day_dummy_df.drop("index",axis=1)],axis=1)
print(temp_df)

#將天氣資料併入temp_df
for data_index in range(temp_df.shape[0]):
    data = temp_df.iloc[data_index,:]
    stadium = data.loc["STADIUM"]
    date = data.loc["DATE"]
    
    print(type(date))