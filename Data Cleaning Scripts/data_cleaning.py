import pandas as pd

df=pd.read_csv("output_2007.csv")
df.columns=["date","topic","count"]
df["date"]=df["date"].str[:-3]
df["topic"]=df["topic"].str.replace("_"," ")

print df
