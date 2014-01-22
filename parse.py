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
import gzip

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
import os
from os.path import getsize
for filename in os.listdir('.'):
	if not filename.startswith("DDD"): continue
	if not filename.endswith(".xml.gz"): continue
	file_size += getsize(filename)
 	file_count += 1
   	
   	try:
		file = gzip.GzipFile(filename)
		for event, elem in ElementTree.iterparse(file):
			if elem.tag == OAI_NS + "record":
				paper_dcx = elem.findall( "./" + OAI_NS  + "metadata" +
									 "/" + DIDL_NS + "DIDL" +
									 "/" + DIDL_NS + "Item"
									 "/" + DIDL_NS + "Component" +
									 "/" + DIDL_NS + "Resource" +
									 "/" + SRW_NS  + "dcx")
				
				assert len(paper_dcx) == 1				 
				#print paper_dcx[0].find(DCX_NS + "recordIdentifier").text
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
		#					print "Paper"
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
		#				if last_part == "a0001":
		#					print ElementTree.dump(e)
		#					print [article_part.attrib["pageid"] for article_part in article_parts]
		#		print
				elem.clear()
				
		#		break
	except:
		print "Error in", filename
		raise

	print "Total size (Gb):\t%.3f" % (float(file_size)/(1024**3))
	print "Number of files:\t%d" % file_count
	print "Number of papers:\t%d" % paper_count
	print "Number of articles:\t%d" % article_count
	print
	
