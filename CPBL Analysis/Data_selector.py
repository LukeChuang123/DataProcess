# -*- coding: utf-8 -*-
import pandas as pd
from sqlalchemy import create_engine

import re

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
print(final_table)

#選擇要哪一天哪一場開始的資料
start_index = final_table[(final_table.DATE=="2017/3/25")&(final_table.GAME_NO =='001')].index.tolist()[0]
final_table = final_table[start_index:]
print(final_table)

game_date_list = list(final_table["DATE"])

final_table.set_index("DATE",inplace = True)
first_time = [True for i in range(5,24)]
# wrong = False
# team_name = {"義大犀牛":"義大","兄弟象":"中信兄弟","統一7-ELEVEn獅":"統一7-ELEVEn獅","Lamigo桃猿":"LAMIGO桃猿"}
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


    sql = "select * from 打擊成績\
           where date = "+"'"+str(game_date)[0:11].replace('-','/')+"' \
           && GAME_NO = "+"'"+game_no+"'"+" && (team like "+"'"+host[0]+'%%'+"' || team like "+"'"+client[0]+'%%'+"');"
 
    batting_df = pd.read_sql_query(sql, engine)
    # print(batting_df)
    print("yo",host,client)
    host = batting_df[batting_df["TEAM"].str.contains(host[0])]
    client = batting_df[batting_df["TEAM"].str.contains(client[0])]

    for col in host.columns[5:24]:
        # print(col)
        if(col in ['RBI', 'R', 'AVG']):
            continue

        #如果是第一次添加該欄位則先創立每一列都是None的欄位
        if(first_time[list(host.columns[5:24]).index(col)] == True):
            final_table["host_"+col] = None
            final_table["client_"+col] = None
            # final_table["whole_"+col] = None
            first_time[list(host.columns[5:24]).index(col)] = False

        host_cell_data ,client_cell_data = sum([float(s) for s in host[col]]) ,sum([float(s) for s in client[col]])
        print(host_cell_data,client_cell_data)
        final_table.iloc[date_index,list(final_table.columns).index("host_"+col)] = host_cell_data
        final_table.iloc[date_index,list(final_table.columns).index("client_"+col)] = client_cell_data
        # final_table.iloc[date_index,list(final_table.columns).index("whole_"+col)] = host_cell_data + client_cell_data
        # print("cell",final_table.iloc[date_index,list(final_table.columns).index(col)])
        print(final_table)
    print("date",game_date_list[date_index])
    print("game_no",game_no)
    # input("continue")
        # first_time = False
            
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
print(final_table)

final_table.to_excel('temp.xlsx',sheet_name='biubiu')

    















# df.index = df["DATE"]
# del df["DATE"]
# df.sort_index()
# print(df.head())
# df = df.sort_values(by = 'DATE')
# print(df)