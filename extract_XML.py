from bs4 import BeautifulSoup
import re
import dateutil.parser

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

    city_results = doc.findAll("classification", {"classificationscheme":"city"})
    if city_results:
        cities = [c.find("classname").text for c in city_results]
    country_results = doc.findAll("classification", {"classificationscheme":"country"})
    if country_results:
        countries = [c.find("classname").text for c in country_results]

    output = {
        "news_source" : news_source,
        "article_body" : article_body,
        "article_title" : article_title,
        "word_count" : word_count,
        "publication_date_raw" : publication_date_raw,
        "publication_date" : publication_date,
        "cities" : cities,
        "countries" : countries
    }
    return output
