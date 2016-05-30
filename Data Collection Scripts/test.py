from urllib2 import urlopen
from json import load 
import pandas as pd
import datetime

#buliding URL:

#article for which page views need to be extracted
#look_up="Donald_Trump"
look_up=[ "Donald_Trump","Bernie_Sanders", "Hillary_Clinton","Ted_Cruz","John_Kasich"]

#dates between which page views need to be extracted in the format YYYYMMDD
startdate="20130101"
enddate="20160430"
i=0
for name in look_up:
#request URL : https://wikimedia.org/api/rest_v1/?doc
	url='https://wikimedia.org/api/rest_v1/metrics/pageviews/per-article/en.wikipedia.org/all-access/all-agents/'+name+'/daily/'+startdate+'/'+enddate

	response = urlopen(url) 

#parses the json response and creates a dictionary with key "list" and value of a  list of dictionaries where each dictionary is a day's page view data
	json_obj = load(response) 

#extacting just the list of dictionaries and creating a data frame
	if i==0:
		data=pd.DataFrame(json_obj['items'])
	else:	
		data=pd.concat([data,pd.DataFrame(json_obj['items'])])
	i=i+1
#data cleaning begins

#step 1 : manipulate the timestamp to create a date attribute
data["time2"]=data["timestamp"].str.rstrip('00')
data['date']=pd.to_datetime(data["time2"],format="%Y%m%d")


data.to_csv('data.csv')
