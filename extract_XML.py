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
collection = db['disk_stories_full']

RABBIT_QUEUE = "disk_process"

def parse_date(raw_date):
    date_string = re.findall(r"(.+\d{4})", raw_date)
    parsed_date = dateutil.parser.parse(date_string[0])
    return parsed_date

def extract_xml(text):
    if text[0:79] == '<?xml version="1.0" encoding="utf-8"?><feed xmlns="http://wwww.w3.org/2005/Atom':
        #print "RSS junk"
        return
    try:
        text = re.sub("</p>", " ", text)
        soup = BeautifulSoup(text)
    except Exception as e:
        print "Error in BS conversion: ",
        print e
        print text
    try:
        doc = soup.contents[1]
    except IndexError:
        doc = soup
    classes = [i.text for i in doc.findAll("classname")]
    if 'PHOTO(S) ONLY' in classes:
        #print "Photo only"
        return
    if 'PHOTO(S) LAYOUT' in classes:
        #print "Photo only"
        return
    try:
        news_source = doc.find("metadata").find("publicationname").text.strip()
    except Exception as e:
        print "Problem getting news source",
        print e
        news_source = ""
    publication_date_raw = doc.find("metadata").find("datetext").text.strip()
    publication_date = parse_date(publication_date_raw)

    try:
        word_count = doc.find("wordcount").attrs['number']
    except AttributeError:
        word_count = "NA"

    position_section = ""
    try:
        position_section = doc.find("positionsection").text.strip()
    except AttributeError:
        position_section = ""

    try:
        article_title = doc.find("nitf:hl1").text.strip()
    except AttributeError:
        if position_section == "EDITORIAL":
            article_title = position_section
        else:
            article_title = ""
            print "No title found."
    if article_title == u"READERS' SUNSHOTS":
        #print "B-Sun shots"
        return # very specific Baltimore Sun problem
    try:
        doc_id = doc.find("dc:identifier", {"identifierscheme":"DOC-ID"}).text
        id_type = "DOC-ID"
    except AttributeError:
        try:
            doc_id = doc.find("dc:identifier", {"identifierscheme":"PGUID"}).text
            id_type = "PGUID"
        except Exception as e:
            print "Some other error in getting doc_id ",
            print e
            id_type = "NA"

    try:
        article_body = doc.find("bodytext").text.strip()
    except AttributeError:
        #print "No body"
        return

    cities = []
    states = []
    countries = []
    city_results = doc.findAll("classification", {"classificationscheme":"city"})
    if city_results:
        cities = [c.find("classname").text for c in city_results]
    state_results = doc.findAll("classification", {"classificationscheme": "state"})
    if state_results:
        states = [c.find("classname").text for c in state_results]
    country_results = doc.findAll("classification", {"classificationscheme":"country"})
    if country_results:
        countries = [c.find("classname").text for c in country_results]

    output = {
        "news_source" : news_source,
        "article_body" : article_body,
        "article_title" : article_title,
        "position_section" : position_section,
        "word_count" : word_count,
        "publication_date_raw" : publication_date_raw,
        "publication_date" : publication_date,
        "cities" : cities,
        "states" : states,
        "countries" : countries,
        "doc_id" : doc_id,
        "id_type" : id_type
    }
    return output

def get_file_list(root_dir):
    files = [] #Will have list of all the files parsed through
    for dname, subdirlist, flist in os.walk(root_dir):
        for fname in flist:
            files.append(os.path.join(dname, fname))
    return files

def setup_rabbitmq():
    credentials = pika.PlainCredentials('guest', 'guest')
    parameters = pika.ConnectionParameters('localhost', 5672, '/', credentials)
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()
    channel.queue_declare(queue='disk_process', durable=True)
    channel.confirm_delivery()

def write_to_queue(doc, queue):
    message = json.dumps(doc, default=json_util.default)
    channel.basic_publish(exchange='',
                      routing_key=queue,
                      body=message,
                      properties=pika.BasicProperties(
                         delivery_mode = 2,
            ))

def write_to_mongo(collection, doc):
    lang = "english"
    toInsert = {"news_source": doc['news_source'],
                "article_title": doc['article_title'],
                "publication_date_raw": doc['publication_date_raw'],
                "date_added": datetime.datetime.utcnow(),
                "article_body": doc['article_body'],
                "stanford": 0,
                "language": lang,
                "doc_id" : doc['doc_id'],
                # disk-specific fields follow
                'position_section' : doc['position_section'],
                'cities' : doc['cities'],
                'countries' : doc['countries'],
                'states' : doc['states'],
                'word_count' : doc['word_count'],
                'id_type' : doc['id_type']
                }
    object_id = collection.insert(toInsert)
    return object_id

#def callback(ch, method, properties, body):
#    print(" [x] Received %r" % body)
#    print body
#    print(" [x] Done")
#    ch.basic_ack(delivery_tag = method.delivery_tag)


# serial implementation
def process_file_list(files):
    docs = []
    for fi in files:
        with open(fi,'r') as f:
            for row_num, row in enumerate(f):
                if row[0:5] == "<?xml":
                    d = {"filename" : fi,
                         "row_num" : row_num,
                         "xml" : row}
                    docs.append(d)

    processed = []
    for i, dd in enumerate(docs):
        try:
            ex = extract_xml(dd['xml'])
            if ex:
                write_to_queue(ex, "disk_process")
        except IndexError:
            print i,
            print dd
            #print dd['filename']
            #print dd['xml']
        except AttributeError as e:
            print i,
            print dd
            print e
            #print dd['filename']
            #print dd['xml']
    #return processed

# parallel implementation
def process_file(fi):
    docs = []
    with open(fi,'r') as f:
        for row_num, row in enumerate(f):
            if row[0:5] == "<?xml":
                d = {"filename" : fi,
                     "row_num" : row_num,
                     "xml" : row}
                docs.append(d)

    #processed = []
    for i, dd in enumerate(docs):
        try:
            ex = extract_xml(dd['xml'])
            if ex:
                try:
                    #write_to_queue(ex, "disk_process")
                    write_to_mongo(collection, ex)
                except Exception as e:
                    print "Mongo error: {0}".format(e)
        except IndexError:
            print i,
            #print dd['filename']
            #print dd['xml']
        except AttributeError as e:
            print i,
            print e
    except Exception as e:
            print "Some other error in extracting XML: ",
            print e


if __name__ == "__main__":
    print "Setting up RabbitMQ..."
    #setup_rabbitmq()
    #credentials = pika.PlainCredentials('guest', 'guest')
    #parameters = pika.ConnectionParameters('localhost', 5672, '/', credentials)
    #connection = pika.BlockingConnection(parameters)
    #channel = connection.channel()
    #channel.queue_declare(queue='disk_process', durable=True)
    #channel.confirm_delivery()
    print "Traversing directory to get files..."
    ln_files = get_file_list("/phani_event_data")
    print "Extracting documents from {0} documents".format(len(ln_files))
    short_files = ln_files #[0:100]
    pool_size = 14 
    pool = Pool(pool_size)
    print "ETLing..."
    processed = [pool.map(process_file, short_files)]
    #processed = process_file_list(short_files)
    #print len(processed)
    print "Complete"
