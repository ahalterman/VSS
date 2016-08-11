# VSS

Extract, transform, and load news stories from LexisNexis Bulk API dumps and
from the LDC Gigaword corpora.

"Your [XML] seems less removed than *hurled* from you, and hurled with a
velocity that lets you feel as through the [XML] is going to end up someplace
so far away from you that it will have become an abstraction...a kind of
existential-level [XML] treatment."

- `extract_xml` takes in one XML-format news article from the LexisNexis dump
  and returns a JSON object that looks like this:

```
    {'article_body': u'',
     'article_title': u'',
     'cities': [],
     'countries': [],
     'doc_id': u'',
     'news_source': u'',
     'position_section': u'',
     'publication_date': datetime.datetime(2015, 5, 18, 0, 0),
     'publication_date_raw': u'',
     'states': [],
     'word_count': u'',
     'id_type' : '',
    }
```

- `gigaword_loader.py` traverses a directory of LDC Gigaword documents in XML,
  transforms them, and loads them into a Mongo database. It takes as an
  argument the gigaword directory's path. The resulting documents look like this:

```
      { 'article_title' : article_title,
        'dateline' : dateline,
        'article_body' : article_body,
        'doc_id' : doc_id,
        'publication_date' : parsed_date,
        'news_source' : news_source,
        'language' : language,
        'doc_type' : doc_type,
        'word_count' : word_count
        }
```

Note: Python's built-in tools for logging and for multiprocessing don't work
well together. I've defaulted to better logging because that saved more time in
debugging than multiprocessing added, but all of the code for multiprocessing
is built in and commented out. Switching to multiprocessing wouldn't be hard.
