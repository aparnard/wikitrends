import urllib2
import re
from bs4 import BeautifulSoup

def retrieveUrls(url):
   page=urllib2.urlopen(url)
   soup = BeautifulSoup(page.read(),"lxml")
   newlink = [url+'/'+a['href'] for a in soup.find_all('a') if '.gz' in a['href'] and '../' not in a['href']] #find just the first one
   return newlink

def getLinks(url):
    d_links=[]
    print "SCraping " + url + " for links"
    page=urllib2.urlopen(url)
    soup = BeautifulSoup(page.read())
    links = [url+a['href'] for a in soup.find_all('a')]
    for link in links:
        d_links=d_links+retrieveUrls(link)
    return d_links

page_urls=['https://dumps.wikimedia.org/other/pagecounts-raw/2007/','https://dumps.wikimedia.org/other/pagecounts-raw/2008/','https://dumps.wikimedia.org/other/pagecounts-raw/2009/',
'https://dumps.wikimedia.org/other/pagecounts-raw/2010/','https://dumps.wikimedia.org/other/pagecounts-raw/2011/','https://dumps.wikimedia.org/other/pagecounts-raw/2012/',
'https://dumps.wikimedia.org/other/pagecounts-raw/2013/','https://dumps.wikimedia.org/other/pagecounts-raw/2014/','https://dumps.wikimedia.org/other/pagecounts-raw/2015/']
links=[]
for page in page_urls:
  links=links+getLinks(page)
print("Collected ")+str(len(links))+" links to be downloaded"
f=file("download_links.txt","w")
f.write(",".join(links))