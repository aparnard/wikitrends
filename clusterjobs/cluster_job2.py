import graphlab as gl
import numpy as np 
import time


def process_links(download_link):

	candidates=['Donald_Trump','Bernie_Sanders','Hillary_Clinton','Ted_Cruz','John_Kasich',
			'String_theory','Megan_Fox' ,'Shah_Rukh_Khan','Sundar_Pichai','Deep_Learning' ,
			'Apache_Spark',"Apache_Hadoop",'Independence_Day_(United_States)',
			'Sam_Anderson_(Tamil_actor)','Jack_Ma','Ryan_Gosling'] 
	
	read_data = gl.SFrame.read_csv(download_link, delimiter = " ", header = False)
	filtered_data = read_data.filter_by(candidates,'X2')
	# grouped_data = filtered_data.groupby("X2",{"X3":gl.aggregate.SUM("X3")})
	filtered_data = filtered_data.filter_by('en','X1')
	filtered_data.rename({"X1":"Proj","X2":"Page", "X3":"PageViews"})
	filtered_data.remove_column("X4")
	# dates = np.empty(filtered_data.num_rows(), dtype=str)
	# dates.fill()
	dates = [str(download_link[73:82])]*filtered_data.num_rows()
	dates_gl = gl.SArray(dates, dtype=str)
	filtered_data.add_column(dates_gl, name="Date")
	dest = "s3://wikitrends-gl-map/raw_data/" + download_link[73:88] + ".csv"
	filtered_data.save(dest)

	return filtered_data

download_links_np = np.genfromtxt("Links_2.csv", dtype=str, delimiter=',')
links = []
for link in download_links_np:
	download_link = {}
	download_link['download_link'] = link
	links.append(download_link)


gl.aws.set_credentials('AKIAJ77JMQ4RKXTZIJFA', '1Jp8AWsWwCt6vr4rPmhLfI11paAbS+0JRUppDx7g')

ec2config = gl.deploy.Ec2Config(region='us-west-2',
                                instance_type='c3.xlarge',
                                aws_access_key_id='AKIAJ77JMQ4RKXTZIJFA',
                                aws_secret_access_key='1Jp8AWsWwCt6vr4rPmhLfI11paAbS+0JRUppDx7g')
# Define your EC2 cluster to use 3 hosts (instances)
ec2 = gl.deploy.ec2_cluster.create(name='wikitrends_cluster_2',
                                   s3_path='s3://wikitrends-gl-map',
                                   ec2_config=ec2config,
                                   num_hosts=4)
# Execute a map_job.
job = gl.deploy.map_job.create(process_links, links,
                               environment=ec2)
# Get a list of results.
print job.get_map_results()
