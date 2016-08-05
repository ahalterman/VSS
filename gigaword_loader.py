from bs4 import BeautifulSoup
import re
import dateutil.parser
import os
from multiprocessing import Pool
import pika
import json
from bson import json_util
from pymongo import MongoClient
import datetime

connection = MongoClient()
db = connection.lexisnexis
collection = db['gigaword']

def extract_xml(doc):
    article_body = doc.find("text").text.strip()
    word_count = len(article_body.split())
    doc_id = doc['id']
    language = re.findall("_([A-Z]{3})_", doc_id)[0]
    news_source = re.findall("^([A-Z]{3})_", doc_id)[0]
    date_string = re.findall("_(\d{8})", doc_id)[0]
    parsed_date = dateutil.parser.parse(date_string)
    try:
        dateline = doc.find("dateline").text.strip()
    except AttributeError:
        dateline = ""
        print "No dateline for doc {}".format(doc_id)
    doc_dict = {
        'article_title' : doc.find("headline").text.strip(),
        'dateline' : dateline,
        'article_body' : article_body,
        'doc_id' : doc_id,
        'publication_date' : parsed_date,
        'news_source' : news_source,
        'language' : language
        }
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

