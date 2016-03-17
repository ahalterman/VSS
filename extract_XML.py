from bs4 import BeautifulSoup
import re
import dateutil.parser
import os
from multiprocessing import Pool

def parse_date(raw_date):
    date_string = re.findall(r"(.+\d{4})", raw_date)
    parsed_date = dateutil.parser.parse(date_string[0])
    return parsed_date

def extract_xml(text):
    text = re.sub("</p>", " ", text)
    soup = BeautifulSoup(text)
    doc = soup.contents[1]
    news_source = doc.find("metadata").find("publicationname").text.strip()
    publication_date_raw = doc.find("metadata").find("datetext").text.strip()
    publication_date = parse_date(publication_date_raw)
    article_body = doc.find("bodytext").text.strip()
    article_title = doc.find("nitf:hl1").text.strip()
    word_count = doc.find("wordcount").attrs['number']
    position_section = ""
    position_section = doc.find("positionsection").text.strip()
    doc_id = doc.find("dc:identifier", {"identifierscheme":"DOC-ID"}).text

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
        "doc_id" : doc_id
    }
    return output


def get_file_list(root_dir):
    files = [] #Will have list of all the files parsed through
    for dname, subdirlist, flist in os.walk(root_dir):
        for fname in flist:
            files.append(os.path.join(dname, fname))
    return files

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
            processed.append(extract_xml(dd['xml']))
        except IndexError:
            print i,
            #print dd['filename']
            #print dd['xml']
        except AttributeError as e:
            print i,
            print e
            #print dd['filename']
            #print dd['xml']
    return processed

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

    processed = []
    for i, dd in enumerate(docs):
        try:
            processed.append(extract_xml(dd['xml']))
        except IndexError:
            print i,
            #print dd['filename']
            #print dd['xml']
        except AttributeError as e:
            print i,
            print e
            #print dd['filename']
            #print dd['xml']
    return processed

if __name__ == "__main__":
    ln_files = get_file_list("/phani_event_data")
    short_files = ln_files[0:100]
    pool_size = 12
    pool = Pool(pool_size)
    #processed = [pool.apply_async(process_file(sf)) for sf in short_files]
    processed = process_file_list(short_files)
    print len(processed)
