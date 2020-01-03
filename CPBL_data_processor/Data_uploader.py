import pandas as pd
import numpy as np

class DataUploader:
    def set_input(self,grade_df,game_date,game_no):
        # print("grade_df",grade_df)
        self.grade_df = grade_df.loc[(game_date,game_no),:]
    def process_data(self,final_df,date_index,host,client):
        batting_df = self.grade_df.groupby("TEAM").aggregate([sum]).droplevel(1,axis=1)
        host_df = pd.DataFrame([batting_df.loc[host]])
        client_df = pd.DataFrame([batting_df.loc[client]])
        host_df = host_df.add_prefix("HOST_").reset_index()
        client_df = client_df.add_prefix("CLIENT_").reset_index()
        print(host_df)
        print(client_df)
        temp_df = pd.concat([host_df,client_df],axis=1,).drop("index",axis=1)
        print("temp")
        print(temp_df) 
        # print("temp index",temp_df.index)
        # temp_df.to_excel('temp.xlsx',sheet_name='biubiu')
        # input("continue")
        return temp_df

class BattingDataUploader(DataUploader):
    def set_input(self,grade_df,game_date,game_no):
        super(BattingDataUploader,self).set_input(grade_df,game_date,game_no)

class PitchingDataUploader(DataUploader):
    def set_input(self,grade_df,game_date,game_no):
        super(PitchingDataUploader,self).set_input(grade_df,game_date,game_no)