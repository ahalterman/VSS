from __future__ import print_function
from bs4 import BeautifulSoup
import re
import dateutil.parser
import os
import multiprocessing
import pika
import json
from bson import json_util
from pymongo import MongoClient
import datetime

import logging
import sys

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

ch = logging.StreamHandler(sys.stdout)
ch.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(funcName)s -  %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)

connection = MongoClient()
db = connection.lexisnexis
collection = db['gigaword']

def extract_xml(doc):
    doc_id = doc['id']
    try:
        article_body = doc.find("text").text.strip()
    except AttributeError as e:
        logger.warning("No article body for {}".format(doc_id))
        #print("No article body for {}".format(doc_id))
        article_body = ""
    word_count = len(article_body.split())
    language = re.findall("_([A-Z]{3})_", doc_id)[0]
    news_source = re.findall("^([A-Z]{3})_", doc_id)[0]
    date_string = re.findall("_(\d{8})", doc_id)[0]
    parsed_date = dateutil.parser.parse(date_string)
    try:
        doc_type = doc['type']
    except Exception as e:
        #logger.info("Error getting story_type: {}".format(e))
        print("Error getting story_type: {}".format(e))
        doc_type = ""
    try:
        dateline = doc.find("dateline").text.strip()
    except AttributeError:
        dateline = ""
        #logger.info("No dateline for doc {}".format(doc_id))
        #print("No dateline for doc {}".format(doc_id))
    doc_dict = {
        'article_title' : doc.find("headline").text.strip(),
        'dateline' : dateline,
        'article_body' : article_body,
        'doc_id' : doc_id,
        'publication_date' : parsed_date,
        'news_source' : news_source,
        'language' : language,
        'doc_type' : doc_type
        }
    sys.stdout.flush()
    return doc_dict

def get_file_list(root_dir):
    files = [] #Will have list of all the files parsed through
    for dname, subdirlist, flist in os.walk(root_dir):
        for fname in flist:
            files.append(os.path.join(dname, fname))
    return files

def write_to_mongo(collection, doc):
    lang = "english"
    toInsert = {"news_source": doc['news_source'],
                "article_title": doc['article_title'],
                #"publication_date_raw": doc['publication_date_raw'],
                "date_added": datetime.datetime.utcnow(),
                "article_body": doc['article_body'],
                "stanford": 0,
                "language": doc['language'],
                "doc_id" : doc['doc_id'],
                'word_count' : doc['word_count'],
                'dateline' : doc['dateline']
                }
    object_id = collection.insert(toInsert)
    return object_id


def process_file(fi):
    #print("Processing file {}".format(fi))
    logger.info("Processing file {}".format(fi))
    with open(fi,'r') as f:
        docs = f.read()
    soup = BeautifulSoup(docs)
    for dd in soup.find("body").find_all("doc"):
        #logger.debug("found doc")
        try:
            ex = extract_xml(dd)
            if ex:
                try:
                    pass
                    #logger.debug(ex['article_title'])
                    #write_to_mongo(collection, ex)
                except Exception as e:
                    #logger.error("Mongo error: {0}".format(e))
                    print("Mongo error: {0}".format(e))
            else:
                logger.error("No extracted xml")
                #print("No extracted xml")
        except IndexError as e:
            logger.error(e, exc_info=True)
            #print(e, exc_info=True)
            #print dd['filename']
            #print dd['xml']
        except AttributeError as e:
            logger.error(e, exc_info=True)
            #print(e, exc_info=True)
        except Exception as e:
            logger.error("Some other error in extracting XML: ".format(e), exc_info=True)
            #print("Some other error in extracting XML: ".format(e), exc_info=True)
    sys.stdout.flush()


if __name__ == "__main__":
    print("Traversing directory to get files...")
    ln_files = get_file_list("ltw_eng/")
    print("Found {0} documents".format(len(ln_files)))
    short_files = ln_files[0:20] #[0:100]
    print("Extracting documents from {0} documents".format(len(short_files)))
    #pool_size = 4
    #print("Using {} workers".format(pool_size))
    #pool = multiprocessing.Pool(pool_size)
    print("ETLing...")
    #processed = [process_file(f) for f in short_files]
    #multiprocessing.log_to_stderr()
    #logger = multiprocessing.get_logger()
    #logger.setLevel(logging.INFO)
    #processed = [pool.map(process_file, short_files)]
    processed = [process_file(f) for f in short_files]
    print("Complete")


