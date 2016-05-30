import graphlab as gl
import numpy as np 
import time

download_links_np = np.genfromtxt("download_links.txt", dtype=str, delimiter=',')
lines_per_file = []

candidates=['Donald_Trump','Bernie_Sanders','Hillary_Clinton','Ted_Cruz','John_Kasich',
			'String_theory','Megan_Fox' ,'Shah_Rukh_Khan','Sundar_Pichai','Deep_Learning' ,
			'Apache_Spark',"Apache_Hadoop",'Independence_Day_(United_States)',
			'Sam_Anderson_(Tamil_actor)','Jack_Ma','Ryan_Gosling'] 

def process_links(download_link):
	
	read_data = gl.SFrame.read_csv(download_link, delimiter = " ", header = False)
	lines_per_file.append(read_data.num_rows())
	filtered_data = read_data.filter_by(candidates,'X2')
	# grouped_data = filtered_data.groupby("X2",{"X3":gl.aggregate.SUM("X3")})
	filtered_data = filtered_data.filter_by('en','X1')
	filtered_data.rename({"X1":"Proj","X2":"Page", "X3":"PageViews"})
	filtered_data.remove_column("X4")
	dates = np.empty(filtered_data.num_rows(), dtype=str)
	dates.fill(download_link[73:82])
	dates_gl = gl.SArray(dates, dtype=str)
	filtered_data.add_column(dates_gl, name="Date")
	dest = "./wiki_grouped_rawdata/" + download_link[73:88] + ".csv"
	filtered_data.save(dest)




process_links(download_links_np[14])
# for link in download_links_np:
# 	start_time = time.time()
# 	print start_time
# 	process_links(link)
# 	print "-"*50
# 	print "All Files processed"
# 	print "Total Lines Processed", sum(lines_per_file)
# 	print "Total Time Taken:", time.time() - start_time


