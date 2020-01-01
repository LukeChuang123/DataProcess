# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import datetime

import Data_uploader

def get_station_start_and_end_dates(station):
    station_start_date = datetime.datetime.strptime(str(station["資料起始日期"][0]), "%Y-%m-%d").date() 
    if(str(station["撤站日期"][0]) == 'nan'):
        return station_start_date,None
    else:
        # print(type(station["撤站日期"][0]))
        station_end_date = datetime.datetime.strptime(str(station["撤站日期"][0]), "%Y-%m-%d").date() 
        return station_start_date,station_end_date
def decide_start_and_end_dates(station):
    global start_date
    global end_date
    station_start_date,station_end_date = get_station_start_and_end_dates(station)
    if(str(station["撤站日期"][0]) == 'nan'):
        if(station_start_date > start_date):
            end_date = station_start_date
        else:
            return
    else:
        if(station_start_date > start_date and station_end_date >= end_date):
            end_date = station_start_date
        elif(station_start_date <= start_date and station_end_date < end_date):
            start_date = station_end_date

#read temp file to dataframe
temp_df = pd.read_excel("temp.xlsx")
print(temp_df)

#read 各球場對應觀測站 file to dataframe
stadium_and_station = pd.read_excel("各球場對應觀測站.xlsx").set_index("球場")

start_date = datetime.datetime.strptime("2013-03-23", "%Y-%m-%d").date()
end_date = datetime.datetime.strptime("2019-10-17", "%Y-%m-%d").date()
is_first_time = True
previous_stadium = None
for stadium in stadium_and_station.index:
    print("old_start_date:",start_date)
    print("old_end_date:",end_date)
    if(stadium == previous_stadium):
        continue
    else:
        previous_stadium = stadium
        # stadium_station_df = None

        if(stadium_and_station.loc[stadium,:].shape[0] == 5):
            stadium_df = pd.DataFrame([stadium_and_station.loc[stadium,:]])
        else:
            stadium_df = stadium_and_station.loc[stadium,:] 
        # print(stadium_df.shape[0])
        # input("continue")
        print("shape[0]",stadium_df.shape[0])
        stations = []
        start_dates = []
        end_dates = []
        for station_index in range(stadium_df.shape[0]):
            print("start_date:",start_date)
            print("end_date:",end_date)
            station = pd.DataFrame([stadium_df.iloc[station_index,:]])
            print(station)
            # input("continue")
            stations.append(list(station["觀測站"])[0])
            station_start_date,station_end_date = get_station_start_and_end_dates(station)
            # print(station_start_date)
            if(station_start_date <= start_date and (str(station["撤站日期"][0]) == 'nan' or station_end_date >= end_date)):
                start_dates.append(start_date)
                end_dates.append(end_date)
            elif(station_start_date > start_date):
                start_dates.append(station_start_date)
                if(str(station["撤站日期"][0]) == 'nan' or station_end_date >= end_date):
                    end_dates.append(end_date)
                else:
                    end_dates.append(station_end_date)
            else:
                start_dates.append(start_date)
                end_dates.append(station_end_date)
            decide_start_and_end_dates(station)
        data = {"stadium":stadium,"station":stations,"start_time":start_dates,"end_time":end_dates}
        if(is_first_time):
            stadium_station_df = pd.DataFrame(data)
            is_first_time = False
        else:
            stadium_station_df = pd.concat([stadium_station_df,pd.DataFrame(data)],axis=0)
        start_date = datetime.datetime.strptime("2013-03-23", "%Y-%m-%d").date()
        end_date = datetime.datetime.strptime("2019-10-17", "%Y-%m-%d").date()
print(stadium_station_df)
stadium_station_df.to_excel("temp2.xlsx")