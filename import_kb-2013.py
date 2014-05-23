import xml.etree.cElementTree as ElementTree
import zipfile, gzip
import os
import sys
import pyelasticsearch
import HTMLParser
import datetime
from multiprocessing import Process, Pool

global input_dir
input_dir = '.'

global es
es = pyelasticsearch.ElasticSearch('http://zookst18:8009/',timeout=600,max_retries=10)

INDEX_NAME = 'kb'

def index_document(doc_obj, _id):
	es.index(INDEX_NAME,"doc",doc_obj, id=_id)

def index_zipfile(zipfile_obj, _id):
	es.index(INDEX_NAME,"zipfile", zipfile_obj, id=_id)

def write_progress(logfile):
	date_str = str(datetime.datetime.now())

	f = open(logfile, 'w')
	f.write(date_str)
	f.close()

def process_file(zipfilename):
	print "Processing:",zipfilename

	filename = zipfilename.split("/")[-1]
	logfile = 'progress/' + filename.replace('.gz', '.log')
	print "Checking progress file %s" % logfile
	if os.path.exists(logfile):
		print "[%s] File already imported." % logfile
		return 1

	article_count = 0
	try:
		with gzip.GzipFile(zipfilename) as file:
			txt = file.read('utf-8')
			txt = txt.replace('&','&amp;') # quick and dirty solution to encoding entities
			etree = ElementTree.fromstring(txt)
			docs = etree.findall('doc')
			#for event, elem in ElementTree.iterparse(file):
			for elem in docs:
				if elem.tag == 'doc':
					doc_el = elem

					doc_obj = {}
					# paper metadata
					doc_obj['paper_dc_title'] = doc_el.find("field[@name='dc_title']").text
					doc_obj['paper_dc_identifier'] = doc_el.find("field[@name='dc_identifier']").text
					doc_obj['paper_dcterms_alternative'] = doc_el.find("field[@name='dcterms_alternative']").text
					doc_obj['paper_dcterms_isVersionOf'] = doc_el.find("field[@name='dcterms_isVersionOf']").text
					doc_obj['paper_dc_date'] = doc_el.find("field[@name='dc_date']").text
					doc_obj['paper_dcterms_temporal'] = doc_el.find("field[@name='dcterms_temporal']").text
					doc_obj['paper_dcx_recordRights'] = doc_el.find("field[@name='dcx_recordRights']").text	
					doc_obj['paper_dc_publisher'] = doc_el.find("field[@name='dc_publisher']").text
					doc_obj['paper_dcterms_spatial'] = doc_el.find("field[@name='dcterms_spatial']").text
					doc_obj['paper_dc_source'] = doc_el.find("field[@name='dc_source']").text
					doc_obj['paper_dcx_volume'] = doc_el.find("field[@name='dcx_volume']").text
					doc_obj['paper_dcx_issuenumber'] = doc_el.find("field[@name='dcx_issuenumber']").text
					doc_obj['paper_dcx_recordIdentifier'] = doc_el.find("field[@name='dcx_recordIdentifier']").text
					doc_obj['paper_dc_identifier_resolver'] = doc_el.find("field[@name='dc_identifier_resolver']").text
					doc_obj['paper_dc_language'] = doc_el.find("field[@name='dc_language']").text
					doc_obj['paper_dcterms_isPartOf'] = doc_el.find("field[@name='dcterms_isPartOf']").text
					doc_obj['paper_ddd_yearsDigitized'] = doc_el.find("field[@name='ddd_yearsDigitized']").text
					doc_obj['paper_dcterms_spatial_creation'] = doc_el.find("field[@name='dcterms_spatial_creation']").text
					doc_obj['paper_dcterms_issued'] = doc_el.find("field[@name='dcterms_issued']").text

					# article metadata
					doc_obj['article_dc_subject'] = doc_el.find("field[@name='dc_subject']").text
					doc_obj['article_dc_title'] = doc_el.findall("field[@name='dc_title']")[1].text
					doc_obj['article_dcterms_accessRights'] = doc_el.find("field[@name='dcterms_accessRights']").text
					doc_obj['article_dcx_recordIdentifier'] = doc_el.findall("field[@name='dcx_recordIdentifier']")[1].text
					doc_obj['article_dc_identifier_resolver'] = doc_el.findall("field[@name='dc_identifier_resolver']")[1].text
					
					# text content
					content_el = doc_el.find("field[@name='content']")
					text_el = content_el.find("text") #sometimes no text_el found (invalid ocr xml)
					if text_el is None:
						doc_obj['text_content'] = ''
					else:
						print text_el 
						print text_el.text
						text_content = ElementTree.tostring(text_el, 'utf-8')
						print text_content
						assert False

						# Unescape HTML entities
						text_content = text_content.replace("&amp;amp;", '&')
						text_content = text_content.replace("&amp;quot;", '"')
						text_content = text_content.replace("&amp;gt;", '>')
						text_content = text_content.replace("&amp;lt;", '<')
						text_content = text_content.replace("&amp;apos;", "'")
						
						#if doc_obj['article_dcx_recordIdentifier'] == 'ddd:000013812:mpeg21:a0010':
						#	print text_content
												
						doc_obj['text_content'] = text_content
					# upload_document
					doc_obj['zipfilename'] = zipfilename
					doc_obj['identifier'] = doc_obj['article_dcx_recordIdentifier']
					index_document(doc_obj, doc_obj['identifier'])

					article_count += 1
		
		#zipfile_obj = {}
		#zipfile_obj['zipfilename'] = zipfilename
		#ipfile_obj['article_count'] = article_count
		
		#logname_arr = zipfilename.split("/")
		#index_zipfile(zipfile_obj, logname_arr[4])
		write_progress(logfile)
		return (article_count)	
	except Exception as error:
	    print type(error), "in", zipfilename + ":", error
	    import traceback
	    traceback.print_exc()
	    return (0)

if __name__  == '__main__':
	pool = Pool(processes=1)
	results = []
	
	for filename in os.listdir(input_dir):
		# print filename
		# Do not forget to run async
		result = pool.apply_async(process_file, [input_dir + filename])
		results.append(result)
	pool.close()
	pool.join()
		
