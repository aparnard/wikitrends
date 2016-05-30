import pandas as pd
from datetime import datetime

graphlab_df=pd.read_csv("E:\Coursework\Spring 2016\INFX 575\Project\Data\All_Wiki_2010_15.csv")
api_df=pd.read_csv("E:\Coursework\Spring 2016\INFX 575\Project\Data\WikiData_Jul15_16.csv")

graphlab_df["Date"]=graphlab_df["Date"].str[:-1]
df1=graphlab_df[['Date','Page','Sum of PageViews']]
df2=api_df[['time2','article','views']]
df2.columns=['Date','Page','Sum of PageViews']

df=df1.append(df2)
df['Date']=pd.to_datetime(df["Date"],format="%Y%m%d")
df.index=df['Date']

biweekly_df=df[["Page","Sum of PageViews"]][df["Date"]>=datetime.strptime("2015-07-01", "%Y-%m-%d")].groupby("Page").apply(lambda page: page.resample("W",how=sum)).reset_index()
biweekly_df.to_csv("biweekly.csv",index=False)
monthly_df=df[["Page","Sum of PageViews"]].groupby("Page").apply(lambda page: page.resample("M",how=sum)).reset_index()
monthly_df.to_csv("monthly.csv",index=False)