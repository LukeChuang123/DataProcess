# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np
from sqlalchemy import create_engine

import re

import Data_uploader

# def get_need_col_dict(columns):
#     col_dict = {}
#     for col in columns:
#         col_dict[col] = "sum"
#     return col_dict

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
# print(final_table)

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

#加入各隊dummy_coding欄位
# for host_or_client in ["host_","client_"]:
#     for team in ["中信兄弟","Lamigo桃猿","統一7-ELEVEn獅","富邦悍將"]:
#         final_table[host_or_client+team] = 0
# for stadium in ["天母","新莊","桃園","新竹","台灣國立體育","洲際","雲林","嘉義市","台南","澄清湖","屏東","羅東","花蓮"]:
#     final_table[stadium] = 0

final_df = None
first_date = True
for date_index in range(len(game_date_list[700:])):
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
    
            
# print("wrong",Wrong)


# game_date = game_date_list[0]
# #確定比賽編號和主客場
# host = "富邦"
# client = "中信"
# #將each_game_data的GAME_NO的三位表示法變成一位表示法
# game_no = '1'
# print(game_no,host,client)

# sql = "select * from 打擊成績\
#         where date = "+"'"+str(game_date)[0:11].replace('-','/')+"' \
#         && GAME_NO = "+"'"+game_no+"'"+" && (team like "+"'"+host[0]+'%%'+"' || team like "+"'"+client[0]+'%%'+"');"
 
# batting_df = pd.read_sql_query(sql, engine)
# # print(batting_df)

# host = batting_df[batting_df["TEAM"].str.contains(host[0])]
# client = batting_df[batting_df["TEAM"].str.contains(client[0])]

# # batting_df.set_index("T",inplace=True)
# print(host)
# for col in host.columns[5:]:
#     print(col)
#     col_data = sum([float(s) for s in host[col]])
#     final_table.insert(final_table.shape[1],col,col_data)

# final_table = pd.concat([final_table,final_df],axis=1)
# print(final_table.reset_index())
# final_table = pd.concat([final_table.iloc[700,:],final_df],axis=1)
final_table = final_table.iloc[700:,:].reset_index()
print(final_table)
final_df = final_df.reset_index().drop("index",axis=1)
print(final_df)
final_table = final_table.join(final_df)
print(final_table)

final_table.to_excel('temp.xlsx',sheet_name='biubiu')

    












