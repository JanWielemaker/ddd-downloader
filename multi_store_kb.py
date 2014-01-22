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

ocrfile = zipfile.ZipFile("ocr4.zip", "a", allowZip64=True)

def process_file(zipfilename):
    new_ocrfiles = {}     
    try:
        if exists(zipfilename.replace(".xml.gz", ".ocr.zip")): 
            return new_ocrfiles
        if exists(zipfilename.replace(".xml.gz", ".done")): 
            return new_ocrfiles
        with gzip.GzipFile(zipfilename) as file:
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
                
                assert len(paper_dcx) == 1               

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
                             
                paper_count = 0
                article_count = 0
                already_done = 0
                downloaded = 0
                errors = 0
                
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
                        try:
                            ocrfile.getinfo(filename)
                            already_done += 1
                            new_ocrfiles[filename] = ocrfile.read(filename)
                        except KeyError as error:
                            url = e.find(DC_NS + "identifier").text + ":ocr"
                            try:
                                try:
                                    data = urllib2.urlopen(url).read()
                                except urllib2.URLError:
                                    # Retry once
                                    try:
                                        data = urllib2.urlopen(url).read()
                                    except urllib2.URLError:
                                        # Retry twice
                                        data = urllib.urlopen(url).read()
                                new_ocrfiles[filename] = data
                                downloaded += 1
                                # Do not rush the KB...
                                time.sleep(0.5 if time.localtime()[3] > 5 else 0.01)
                            except (urllib2.HTTPError,urllib2.URLError) as error:
                                print error, "for", url
                                errors += 1
                                return {}

                print "Finished %s. %d papers\t %d articles\t had %d already\t %d downloaded\t %d errors" % \
                        (paper_id, paper_count, article_count, already_done, downloaded, errors)
                elem.clear()
                
        print "Finished with", zipfilename, "downloaded %d files in total." % len(new_ocrfiles)
        with zipfile.ZipFile(zipfilename.replace(".xml.gz", ".ocr.zip"), "w", allowZip64=True) as out:
            for filename in new_ocrfiles:
                out.writestr(filename, new_ocrfiles[filename])
        return {}
    except Exception as error:
        print type(error), "in", zipfilename + ":", error
#        raise
        

file_count = 0

pool = Pool(processes=24)
results = []

for filename in os.listdir('.'):
    if not filename.startswith("DDD"): continue
    if not filename.endswith(".xml.gz"): continue
    file_count += 1

#     process_file(filename)
    result = pool.apply_async(process_file, [filename])
    results.append(result)

print "Number of files:\t%d" % file_count
    
pool.close()
for result in results:
    new_ocrfiles = result.get()
    if new_ocrfiles == None:
        print "Error in getting result"
        continue
#    for filename, data in new_ocrfiles.iteritems():
#        ocrfile.writestr(filename, data)
    del result
pool.join()

#print "Total size (Gb):\t%.3f" % (float(file_size+getsize("ocr3.zip"))/(1024**3))
