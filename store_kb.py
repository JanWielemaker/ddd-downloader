#!/usr/bin/env python

# Copyright 2011-2014, University of Amsterdam. This program is free software:
# you can redistribute it and/or modify it under the terms of the GNU Lesser 
# General Public License as published by the Free Software Foundation, either 
# version 3 of the License, or (at your option) any later version.
# 
# This program is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or 
# FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License 
# for more details.
# 
# You should have received a copy of the GNU Lesser General Public License 
# along with this program. If not, see <http://www.gnu.org/licenses/>.

import xml.etree.cElementTree as ElementTree
import zipfile, gzip
from cStringIO import StringIO
from datetime import datetime
import time
import urllib, urllib2

DC_NS = "{http://purl.org/dc/elements/1.1/}"
DCX_NS = "{http://krait.kb.nl/coop/tel/handbook/telterms.html}"
OAI_NS = "{http://www.openarchives.org/OAI/2.0/}"
SRW_NS = "{info:srw/schema/1/dc-v1.1}"
DIDL_NS = "{urn:mpeg:mpeg21:2002:02-DIDL-NS}"
DDD_NS = "{http://www.kb.nl/namespaces/ddd}"

file_size = 0
file_count = 0
paper_count = 0
article_count = 0

ocrfile = zipfile.ZipFile("ocr.zip", "r", allowZip64=True)
ocrfiles = ocrfile.namelist()
ocrfile.close()
print "Loaded %d files in first zipfile." % len(ocrfiles)
#print "Last one is: " + ocrfiles[-1]

#max_id = int(ocrfiles[-1].split(':')[1])
# max_id = 10560628
# print "Max id: %d" % max_id

ocrfile = zipfile.ZipFile("ocr2.zip", "a", allowZip64=True)

import os
from os.path import getsize
for filename in os.listdir('.'):
	if not filename.startswith("DDD"): continue
	if not filename.endswith(".xml.gz"): continue
	file_size += getsize(filename)
 	file_count += 1
   	
   	try:
		with gzip.GzipFile(filename) as file:
			for event, elem in ElementTree.iterparse(file):
				if elem.tag != OAI_NS + "record": continue

				paper_dcx = elem.findall( "./" + OAI_NS  + "metadata" +
									 "/" + DIDL_NS + "DIDL" +
									 "/" + DIDL_NS + "Item"
									 "/" + DIDL_NS + "Component" +
									 "/" + DIDL_NS + "Resource" +
									 "/" + SRW_NS  + "dcx")
				
				assert len(paper_dcx) == 1				 

				paper_id = paper_dcx[0].find(DCX_NS + "recordIdentifier").text
				paper_id_num = int(paper_id.split(':')[1])

# 				if paper_id_num < max_id:
# 					print "Skipping paper: " + paper_id 
# 					continue
				
				#print ElementTree.dump(paper_dcx[0])
		
				items = elem.findall("./" + OAI_NS + "metadata" +
							 	"/" + DIDL_NS + "DIDL" +
							 	"/" + DIDL_NS + "Item" +
							 	"/" + DIDL_NS + "Item")
							 
				for item in items:
					dcx = item.findall("./" + DIDL_NS + "Component" +
									 "/" + DIDL_NS + "Resource" +
									 "/" + SRW_NS + "dcx")
			
					for e in dcx:
						if e.find(DDD_NS + "OCRConfidencelevel") != None:
							paper_count += 1
							continue
						
						assert len(dcx) == 1	
						
						identifier = e.find(DCX_NS + "recordIdentifier").text
						last_part = identifier.split(':')[-1]
		
						assert last_part[0] == "a"
						#print last_part,
			
						article_parts = item.findall("./" + DIDL_NS + "Component" +
												"/" + DIDL_NS + "Resource" +
												"/" + DCX_NS + "zoning" +
												"/" + DCX_NS + "article-part")
						
						article_count += 1


						filename = "%s:ocr.xml" % identifier
						if filename in ocrfiles: 
							print "v",
							continue
						try:
							ocrfile.getinfo(filename)
							print "v",
						except KeyError as error:
							url = e.find(DC_NS + "identifier").text + ":ocr"
							try:
								try:
									data = urllib2.urlopen(url).read()
								except urllib2.URLError:
									# Retry one
									data = urllib.urlopen(url).read()
								ocrfile.writestr(filename, data)
								print ".",
							except (urllib2.HTTPError,urllib2.URLError) as error:
								print
								print error, "for", url

				print
				elem.clear()
	except:
		print "Error in", filename
		raise

	print "Total size (Gb):\t%.3f" % (float(file_size+getsize("ocr.zip"))/(1024**3))
	print "Number of files:\t%d" % file_count
	print "Number of papers:\t%d" % paper_count
	print "Number of articles:\t%d" % article_count
	print "Number downloaded:\t%d" % len(ocrfile.namelist())
	print
	
