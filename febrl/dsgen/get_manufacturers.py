import pandas as pd
import numpy as np
import re
import unicodedata
df = pd.read_csv("all_manufacturers.csv")
manufacturers = df[df.columns[0]].to_numpy()

import requests
import sys
S = requests.Session()
original_stdout = sys.stdout # Save a reference to the original standard output

URL = "https://en.wikipedia.org/w/api.php"


def get_substring(s, start, end):
	try:
		substring = s.split(start)[1].split(end)[0]
		return substring
	except:
		return ""

def cleanhtml(raw_html):
	cleanr = re.compile('<.*?>')
	cleantext = re.sub(cleanr, '', raw_html)
	return cleantext

df = pd.DataFrame(columns=["manufacturer", "foundation", "founder", "location_city", "industry", "owner"])
for manufacturer in manufacturers:
	PARAMS = {
    	"action": "query",
    	"format": "json",
    	"list": "search",
    	"srsearch": manufacturer,
    	"srlimit": 1,
    	"rawcontinue":True
	}
	R = S.get(url=URL, params=PARAMS)
	DATA = R.json()
	page_id =  DATA.get("query").get("search")
	if(len(page_id)!=0):
		page_id = page_id[0].get("pageid")
	else:
		continue;
	PARAMS = {
    	"action": "query",
    	"format": "json",
    	"prop":"revisions",
    	"rvprop":"content",
    	"pageids":page_id,
    	"rvsection":0
	}
	R = S.get(url=URL, params=PARAMS)
	DATA = R.json()
	s = DATA.get("query").get("pages").get(str(page_id)).get("revisions")[0].get("*")
	infobox = get_substring(s, '{{Infobox company', ']]}}')
	if(infobox==""): continue;		
	foundation = get_substring(infobox, "| foundation", '| ').replace('sUnbulletedlist', "").replace('Unbulletedlist', "").replace(" ", "").replace("|", "").replace("\n","").replace("=", "").replace("[","").replace("]","").replace("{","").replace("}","").rstrip()
	foundation = re.sub("[^0-9]", "", foundation)
	founder = get_substring(infobox, "| founder", '| ').replace('sUnbulletedlist', "").replace('Unbulletedlist', "").replace(" ", "").replace("|", "").replace("\n","").replace("=", "").replace("[","").replace("]","").replace("{","").replace("}","").rstrip()
	location_city = get_substring(infobox, "| location_city", '| ').replace('sUnbulletedlist', "").replace('Unbulletedlist', "").replace(" ", "").replace("|", "").replace("\n","").replace("=", "").replace("[","").replace("]","").replace("{","").replace("}","").rstrip()
	industry = cleanhtml(get_substring(infobox, "| industry", '| ').replace('sUnbulletedlist', "").replace('Unbulletedlist', "").replace(" ", "").replace("|", "").replace("\n","").replace("=", "").replace("[","").replace("]","").replace("{","").replace("}","").rstrip())
	owner = get_substring(infobox, "| owner", '| ').replace(" ", "").replace('sUnbulletedlist', "").replace('Unbulletedlist', "").replace("|", "").replace("\n","").replace("=", "").replace("[","").replace("]","").replace("{","").replace("}","").rstrip()
	if(foundation == "" and founder == "" and location_city == "" and industry == "" and owner == ""):
		continue
	new_row = {"manufacturer": manufacturer, "foundation":foundation, "founder":founder, "location_city":location_city, "industry":industry, "owner":owner}
	df = df.append(new_row, ignore_index=True)

df.to_csv("manufacturers_all.csv",encoding='utf-8-sig')		

# 	| name          = Corel Corporation
# | logo          = Corel logo.svg
# | image         = Corelheadquarters.jpg
# | image_size    = 250px
# | image_caption = Corel headquarters in Ottawa
# | type   = [[Private company|Private]]
# | owner  = [[Kohlberg Kravis Roberts]]
# | industry      = [[Software]]<br />[[computer programming|Programming]]
# | foundation    = {{start date and age|1985}}
# | founder       = [[Michael Cowpland]]
# | location_city = [[Ottawa]], [[Ontario]], Canada