# VSS

Extract, transform, and load news stories from LexisNexis Bulk API dumps.

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

