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
import os
from os.path import getsize, exists

from multiprocessing import Process, Pool

DC_NS = "{http://purl.org/dc/elements/1.1/}"
DCX_NS = "{http://krait.kb.nl/coop/tel/handbook/telterms.html}"
OAI_NS = "{http://www.openarchives.org/OAI/2.0/}"
SRW_NS = "{info:srw/schema/1/dc-v1.1}"
DIDL_NS = "{urn:mpeg:mpeg21:2002:02-DIDL-NS}"
DDD_NS = "{http://www.kb.nl/namespaces/ddd}"

def process_file(zipfilename):
    paper_count = 0
    article_count = 0
    downloaded_count = 0
    already_done = 0
    downloaded = 0
    errors = 0
                
    try:
        with gzip.GzipFile(zipfilename) as file:
            ocrfiles = {}
            if exists(zipfilename.replace(".xml.gz", ".ocr.zip")): 
                with zipfile.ZipFile(zipfilename.replace(".xml.gz", ".ocr.zip"), "r", allowZip64=True) as ocrfile:
                    for filename in ocrfile.namelist():
                        ocrfiles[filename] = True
            for event, elem in ElementTree.iterparse(file):
                if elem.tag != OAI_NS + "record": continue

                paper_dcx = elem.findall( "./" + OAI_NS  + "metadata" +
                                     "/" + DIDL_NS + "DIDL" +
                                     "/" + DIDL_NS + "Item"
                                     "/" + DIDL_NS + "Component" +
                                     "/" + DIDL_NS + "Resource" +
                                     "/" + SRW_NS  + "dcx")
                
                # There are probably deleted papers in this record
                if len(paper_dcx) == 0: continue
                assert len(paper_dcx) == 1, "len(paper_dcx) = " + str(len(paper_dcx)) + " != 1"

                paper_id = paper_dcx[0].find(DCX_NS + "recordIdentifier").text
                paper_id_num = int(paper_id.split(':')[1])

#               if paper_id_num < max_id:
#                   print "Skipping paper: " + paper_id 
#                   continue
                
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
                        
                        if "%s:ocr.xml" % identifier in ocrfiles:
                            downloaded_count += 1
    
                    elem.clear()
                
        return (paper_count, article_count, downloaded_count)
    except Exception as error:
        print type(error), "in", zipfilename + ":", error
        import traceback
        traceback.print_exc()
#        raise
        return (0, 0, 0)
        
file_count = 0

pool = Pool(processes=46)
results = []

import sys

for filename in os.listdir('.'):
    if not filename.startswith("DDD"): continue
    if not filename.endswith(".xml.gz"): continue
    if len(sys.argv) > 1: 
    	if not filename in sys.argv[1:]: continue
#    file_size += getsize(filename)
    file_count += 1

#     process_file(filename)
    result = pool.apply_async(process_file, [filename])
    results.append(result)

#print "Total size (Gb):\t%.3f" % (float(file_size+getsize("ocr3.zip"))/(1024**3))
print "Number of files:\t%d" % file_count

articles = 0
papers = 0
downloaded = 0
done = 0
    
pool.close()
for index, result in enumerate(results):
    (p, a, d) = result.get()
    articles += a
    papers += p
    downloaded += d
    if a == d: done += 1
    if a != d and d > 0: print "Incomplete datafile for index", index
    if index == (len(results)-1) or index % 46 == 0:
        print "[%d/%d][%d]" % (index+1, len(results), done),
        print "Number of articles: %d" % articles,
        print "Number downloaded: %d" % downloaded,
        print "Number of papers: %d" % papers
pool.join()

#print "Total size (Gb):\t%.3f" % (float(file_size+getsize("ocr3.zip"))/(1024**3))
