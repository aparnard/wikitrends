!/usr/bin/env python
import urllib
import sys
import gzip
import re
import os
candidates=['Donald_Trump','Bernie_Sanders','Hillary_Clinton','Ted_Cruz','John_Kasich','String_theory','Megan_Fox','Shah_Rukh_Khan','Sundar_Pichai','Deep_Learning','MapReduce','Independence_Day_(United_States)','Sam_Anderson_(Tamil_actor)','Jack_Ma']
urlopener_obj = urllib.URLopener()
for line in sys.stdin:
	urls=line.split(',')
	for url in urls:
		filename=str(re.search('-(\d)+-',url).group())[1:-1]+".gz"
		urlopener_obj.retrieve(url, filename)
		with gzip.open(filename,'rb') as f:
			file_content=f.read().split('\n')
			for row in file_content:
				try:
   					project,topic, count, _=row.split(' ')
   					if any(candidate ==topic for candidate in candidates) and project =="en":
						print '%s\t%s' % (','.join([filename,topic]),count)
				except ValueError:
   					continue
		os.remove(filename)
