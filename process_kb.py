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
DC_TERMS = "{http://purl.org/dc/terms/}"

global outputdir
outputdir = '../KBdoc'

def process_file(zipfilename):
    paper_count = 0
    article_count = 0
    downloaded_count = 0
    already_done = 0
    downloaded = 0
    errors = 0

    papers = []
                
    try:
        # Open metadata XML (.zip) file
        with gzip.GzipFile(zipfilename) as file:
            ocrfiles = {}
            
            if exists(zipfilename.replace(".xml.gz", ".ocr.zip")): 
                # Get all the files in the corrensponding ocr.zip file
                with zipfile.ZipFile(zipfilename.replace(".xml.gz", ".ocr.zip"), "r", allowZip64=True) as ocrfile:
                    for filename in ocrfile.namelist():
                        #print filename

                        ocrfiles[filename] = True
            
                    # Parse the XML file
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

                        paper_e = paper_dcx[0]
                        paper_id = paper_dcx[0].find(DCX_NS + "recordIdentifier").text
                        paper_id_num = int(paper_id.split(':')[1])

                        # PAPER-LEVEL METADATA
                        paper_obj = {}

                        paper_obj['dc_title'] = paper_e.find(DC_NS + "title").text
                        paper_obj['dc_identifier'] = paper_e.find(DC_NS + "identifier").text
                        
                        #paper_obj['dcterms_alternative'] = paper_e.find(DC_TERMS + "alternative").text
                        if paper_e.find(DC_TERMS + "alternative") is None:
                            paper_obj['dcterms_alternative'] = '-'
                        else:
                            paper_obj['dcterms_alternative'] = paper_e.find(DC_TERMS + "alternative").text

                        #paper_obj['dcterms_isVersionOf'] = paper_e.find(DC_TERMS + "isVersionOf").text
                        if paper_e.find(DC_TERMS + "isVersionOf") is None:
                            paper_obj['dcterms_isVersionOf'] = '-'
                        else:
                            paper_obj['dcterms_isVersionOf'] = paper_e.find(DC_TERMS + "isVersionOf").text

                        paper_obj['dc_date'] = paper_e.find(DC_NS + "date").text
                        paper_obj['dcterms_temporal'] = paper_e.find(DC_TERMS + "temporal").text
                        paper_obj['dcx_recordRights'] = paper_e.find(DCX_NS + "recordRights").text
                        paper_obj['dc_publisher'] = paper_e.find(DC_NS + "publisher").text
                        paper_obj['dcterms_spatial'] = paper_e.find(DC_TERMS + "spatial").text
                        paper_obj['dc_source'] = paper_e.find(DC_NS + "source").text
                        
                        #paper_obj['dcx_volume'] = paper_e.find(DCX_NS + "volume").text
                        if paper_e.find(DCX_NS + "volume") is None:
                            paper_obj['dcx_volume'] = '-'
                        else:
                            paper_obj['dcx_volume'] = paper_e.find(DCX_NS + "volume").text 

                        #paper_obj['dcx_issuenumber'] = paper_e.find(DCX_NS + "issuenumber").text
                        if paper_e.find(DCX_NS + "issuenumber") is None:
                            paper_obj['dcx_issuenumber'] = '-'
                        else:
                            paper_obj['dcx_issuenumber'] = paper_e.find(DCX_NS + "issuenumber").text

                        paper_obj['dcx_recordIdentifier'] = paper_e.find(DCX_NS + "recordIdentifier").text
                        paper_obj['dc_identifier_resolver'] = paper_e.findall(DC_NS + "identifier")[1].text
                        paper_obj['dc_language'] = paper_e.find(DC_NS + "language").text
                        paper_obj['dcterms_isPartOf'] = paper_e.find(DC_TERMS + "isPartOf").text
                        paper_obj['ddd_yearsDigitized'] = paper_e.find(DDD_NS + "yearsDigitized").text
                        paper_obj['dcterms_spatial_creation'] = paper_e.findall(DC_TERMS + "spatial")[1].text
                        paper_obj['dcterms_issued'] = paper_e.find(DC_TERMS + "issued").text

                        # Get indivual articles metadata
                        items = elem.findall("./" + OAI_NS + "metadata" +
                                        "/" + DIDL_NS + "DIDL" +
                                        "/" + DIDL_NS + "Item" +
                                        "/" + DIDL_NS + "Item")
                        articles = []     
                        for item in items:
                            dcx = item.findall("./" + DIDL_NS + "Component" +
                                             "/" + DIDL_NS + "Resource" +
                                             "/" + SRW_NS + "dcx")
                    
                            for e in dcx:
                                if e.find(DDD_NS + "OCRConfidencelevel") != None:
                                    paper_count += 1
                                    continue
                                
                                assert len(dcx) == 1    
                                
                                article_obj = {}
                                identifier = e.find(DCX_NS + "recordIdentifier").text
                                last_part = identifier.split(':')[-1]
                    
                                assert last_part[0] == "a"

                                # ARTICLE-LEVEL METADATA
                                article_obj['dc_identifier'] = e.find(DC_NS + "identifier").text
                                article_obj['dc_subject'] = e.find(DC_NS + "subject").text
                                
                                #article_obj['dc_title'] = e.find(DC_NS + "title").text
                                if e.find(DC_NS + "title") is None:
                                    article_obj['dc_title'] = '-'
                                else:
                                    if e.find(DC_NS + "title").text is None:
                                        article_obj['dc_title'] = '-'
                                    else:
                                        article_obj['dc_title'] = e.find(DC_NS + "title").text

                                article_obj['dcterms_accessRights'] = e.find(DC_TERMS + "accessRights").text
                                article_obj['dcx_recordIdentifier'] = e.find(DCX_NS + "recordIdentifier").text
                                article_obj['dc_identifier_resolver'] = e.find(DC_NS + "identifier").text

                                # Content extraction, parse the XML file
                                # WARNING: malformed XML could cause problems.
                                
                                try: 
                                    tree = ElementTree.parse(ocrfile.open("%s:ocr.xml"%identifier,'r'))
                                    root = tree.getroot()
                                    content = ElementTree.tostring(root, 'utf-8')
                                    article_obj['content'] = content
                                except:
                                    article_obj['content'] = '-'
                                
                                articles.append(article_obj)

                                # What's this one for?
                                article_parts = item.findall("./" + DIDL_NS + "Component" +
                                                        "/" + DIDL_NS + "Resource" +
                                                        "/" + DCX_NS + "zoning" +
                                                        "/" + DCX_NS + "article-part")
                                
                                article_count += 1

                                if "%s:ocr.xml" % identifier in ocrfiles:
                                    downloaded_count += 1
            
                            elem.clear()
                        paper_obj['articles'] = articles
                        papers.append(paper_obj)

        # Write to files
        zipfilename = zipfilename.split("/")[-1]
        if papers:
            with gzip.GzipFile(outputdir+zipfilename.replace(".xml.gz",'.data.gz'),'wb') as fh:
                fh.write('<?xml version="1.0" encoding="UTF-8"?>\n')
                fh.write('<add>\n')
                for paper in papers:
                    #fh.write(u"<paper>\n")
                    for article in paper['articles']:
                        fh.write('<doc>\n')
                        
                        fh.write("<field name=\"%s\">%s</field>\n" % ('dc_title',paper['dc_title'].encode("utf-8")))
                        fh.write("<field name=\"%s\">%s</field>\n" % ('dc_identifier',paper['dc_identifier'].encode("utf-8")))
                        fh.write("<field name=\"%s\">%s</field>\n" % ('dcterms_alternative',paper['dcterms_alternative'].encode("utf-8")))
                        fh.write("<field name=\"%s\">%s</field>\n" % ('dcterms_isVersionOf',paper['dcterms_isVersionOf'].encode("utf-8")))
                        fh.write("<field name=\"%s\">%s</field>\n" % ('dc_date',paper['dc_date'].encode("utf-8")))
                        fh.write("<field name=\"%s\">%s</field>\n" % ('dcterms_temporal',paper['dcterms_temporal'].encode("utf-8")))
                        fh.write("<field name=\"%s\">%s</field>\n" % ('dcx_recordRights',paper['dcx_recordRights'].encode("utf-8")))
                        fh.write("<field name=\"%s\">%s</field>\n" % ('dc_publisher',paper['dc_publisher'].encode("utf-8")))
                        fh.write("<field name=\"%s\">%s</field>\n" % ('dcterms_spatial',paper['dcterms_spatial'].encode("utf-8")))
                        fh.write("<field name=\"%s\">%s</field>\n" % ('dc_source',paper['dc_source'].encode("utf-8")))
                        fh.write("<field name=\"%s\">%s</field>\n" % ('dcx_volume',paper['dcx_volume'].encode("utf-8")))
                        fh.write("<field name=\"%s\">%s</field>\n" % ('dcx_issuenumber',paper['dcx_issuenumber'].encode("utf-8")))
                        fh.write("<field name=\"%s\">%s</field>\n" % ('dcx_recordIdentifier',paper['dcx_recordIdentifier'].encode("utf-8")))
                        fh.write("<field name=\"%s\">%s</field>\n" % ('dc_identifier_resolver',paper['dc_identifier_resolver'].encode("utf-8")))
                        fh.write("<field name=\"%s\">%s</field>\n" % ('dc_language',paper['dc_language'].encode("utf-8")))
                        fh.write("<field name=\"%s\">%s</field>\n" % ('dcterms_isPartOf',paper['dcterms_isPartOf'].encode("utf-8")))
                        fh.write("<field name=\"%s\">%s</field>\n" % ('ddd_yearsDigitized',paper['ddd_yearsDigitized'].encode("utf-8")))
                        fh.write("<field name=\"%s\">%s</field>\n" % ('dcterms_spatial_creation',paper['dcterms_spatial_creation'].encode("utf-8")))
                        fh.write("<field name=\"%s\">%s</field>\n" % ('dcterms_issued',paper['dcterms_issued'].encode("utf-8")))

                        fh.write("\t<field name=\"%s\">%s</field>\n" % ('dc_subject',article['dc_subject'].encode("utf-8")))
                        fh.write("\t<field name=\"%s\">%s</field>\n" % ('dc_title',article['dc_title'].encode("utf-8")))
                        fh.write("\t<field name=\"%s\">%s</field>\n" % ('dcterms_accessRights',article['dcterms_accessRights'].encode("utf-8")))
                        fh.write("\t<field name=\"%s\">%s</field>\n" % ('dcx_recordIdentifier',article['dcx_recordIdentifier'].encode("utf-8")))
                        fh.write("\t<field name=\"%s\">%s</field>\n" % ('dc_identifier_resolver',article['dc_identifier_resolver'].encode("utf-8")))
                        fh.write("\t<field name=\"%s\">\n%s\n\t</field>\n" % ('content',article['content']))

                        fh.write('</doc>\n') 
                    #fh.write(u"</paper>\n")
                fh.write('</add>\n')
                
        return (paper_count, article_count, downloaded_count)
    except Exception as error:
        print type(error), "in", zipfilename + ":", error
        import traceback
        traceback.print_exc()
#        raise
        return (0, 0, 0)

if __name__ == '__main__':
    file_count = 0

    pool = Pool(processes=24)
    results = []

    import sys
    
    INPUT_DIR = '.'
    for filename in os.listdir(INPUT_DIR):
        if not filename.startswith("DDD"): continue
        if not filename.endswith(".xml.gz"): continue
        if len(sys.argv) > 1: 
            if not filename in sys.argv[1:]: continue
        # file_size += getsize(filename)
        file_count += 1

        #print filename
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
