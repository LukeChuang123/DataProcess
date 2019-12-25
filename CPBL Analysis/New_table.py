# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np
from sqlalchemy import create_engine

import re

import Data_uploader

# MySQL的使用者：root, 密碼:147369, 埠：3306,資料庫：mydb
engine = create_engine('mysql+pymysql://root:Lc-20332895-@localhost:3306/'+"cpbl_whole_data")
conn = engine.connect()
# 查詢語句，選出employee表中的所有資料
sql = '''
select * from each_game_data;
'''
# read_sql_query的兩個引數: sql語句， 資料庫連線
final_table = pd.read_sql_query(sql, engine)

#先將資料依照日期順序排好
new_date_list = [date.replace('/','-') for date in list(final_table["DATE"])]
final_table["DATE"] = new_date_list
final_table["DATE"] = pd.to_datetime(final_table["DATE"])
final_table = final_table.sort_values("DATE")
final_table = final_table.reset_index(drop=True) 
final_table.replace("Lamigo桃猿","LAMIGO桃猿",inplace = True)
# print(final_table)

#選擇要哪一天哪一場開始的資料
start_index = final_table[(final_table.DATE=="2017/3/25")&(final_table.GAME_NO =='001')].index.tolist()[0]
final_table = final_table[start_index:]

game_date_list = list(final_table["DATE"])

#將打擊成績和投球成績讀進來
batting_df = None
pitching_df = None
i = 0
for table in ["打擊成績","投球成績"]:
    sql = "select * from "+table
    grade_df = pd.read_sql_query(sql, engine)
    grade_df.set_index(["DATE","GAME_NO","TEAM"],inplace = True)
    # print(grade_df.iloc[:,2:])
    if(i == 0):
        grade_df.drop(['RBI', 'R', 'AVG', 'TP','HR'],axis = 1,inplace = True)
        grade_df = grade_df.convert_objects(convert_numeric=True)
        # print("type",grade_df.dtypes)
        # print(grade_df)
        batting_df = grade_df
        i += 1
    else:
        grade_df.drop(['PITCHER', 'DEC', 'H', 'HR', 'SO', 'R', 'BB', 'IBB', 'HBP', 'ERA', 'NP_2', 'SB', 'CG', 'SHO'],axis = 1,inplace = True)
        grade_df = grade_df.convert_objects(convert_numeric=True)
        # grade_df.apply(np.sqrt)
        pitching_df = grade_df      

#加入各隊及球場dummy_coding欄位
host_name_dummy = pd.get_dummies(final_table["HOST"]).add_prefix("HOST_")
client_name_dummy = pd.get_dummies(final_table["CLIENT"]).add_prefix("CLIENT_")
stadium_name_dummy = pd.get_dummies(final_table["STADIUM"])
print(host_name_dummy)
print(client_name_dummy)
print(stadium_name_dummy)
team_stadium_dummy_df = pd.concat([host_name_dummy,client_name_dummy,stadium_name_dummy],axis=1).reset_index()
print(team_stadium_dummy_df)

# input("stop")
final_df = None
first_date = True
for date_index in range(len(game_date_list)):
    # date_index = 7
    game_date = game_date_list[date_index]
    #確定比賽編號和主客場
    host = final_table.iloc[date_index]["HOST"]
    client = final_table.iloc[date_index]["CLIENT"]
    #將each_game_data的GAME_NO的三位表示法變成一位表示法
    not_zero_index = 0
    for s in final_table.iloc[date_index]["GAME_NO"]:
        if(s != '0'):
            break
        else:
            not_zero_index += 1
    game_no = final_table.iloc[date_index]["GAME_NO"][not_zero_index:]

    batting_data_uploader = Data_uploader.BattingDataUploader()
    batting_data_uploader.set_input(batting_df.iloc[:,2:],str(game_date)[0:10].replace('-','/'),game_no)
    batting_data = batting_data_uploader.process_data(final_df,date_index,host,client)

    pitching_data_uploader = Data_uploader.PitchingDataUploader()
    pitching_data_uploader.set_input(pitching_df.iloc[:,3:-1],str(game_date)[0:10].replace('-','/'),game_no)
    pitching_data = pitching_data_uploader.process_data(final_df,date_index,host,client)

    if(first_date):
        final_df = pd.concat([batting_data,pitching_data],axis=1)
        first_date = False
    else:
        final_df = pd.concat([final_df,pd.concat([batting_data,pitching_data],axis=1)],axis=0)

    print("final")
    print(final_df)
    
#將final_table,final_df,team_stadium_dummy_df橫向合併        
final_table = final_table.reset_index()
print(final_table)
final_df = final_df.reset_index().drop("index",axis=1)
print(final_df)
final_table = final_table.join(final_df)
final_table = pd.concat([final_table,team_stadium_dummy_df],axis=1).drop(["HOST","CLIENT","STADIUM"],axis=1)
print(final_table)


final_table.to_excel('temp.xlsx',sheet_name='biubiu')

    












