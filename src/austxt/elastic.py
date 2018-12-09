import os
import urllib3
from multiprocessing import Pool, current_process
from functools import partial
from collections import deque

import pandas as pd
from elasticsearch import Elasticsearch

from random import randint

DOC_TYPE = 'doc'
TEXT_FIELD = 'text'


def create_elastic():
    elastic_address = os.getenv('AUSTXT_ELASTIC_ADDRESS', 'localhost:9200')
    elastic = Elasticsearch(elastic_address, timeout=300, verify_certs=False,
                  use_ssl=True)
    return elastic


def global_elastic():
    global elastic
    elastic = create_elastic()

    
def index_speech(index, speech_type, row_tuple):
    i, row = row_tuple
    elastic.index(
        id=row["speech_id"], 
        body={
            'text':row['text'],
            'exact_text':row['text'],
            'speech_type':speech_type
        },
        index=index,
        doc_type=DOC_TYPE,
    )
    if (i + 1) % 1000 == 0:
        print(i + 1)


def index_speeches(speech_type, path, index_name, limit, workers):
    """Index an extracted CSV file of speeches"""
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    speech_df = pd.read_csv(path, nrows=limit)
    func = partial(index_speech, index_name, speech_type)
    rows = ((index, row) for index, row in speech_df.iterrows())
    if workers == 1:
        global_elastic()
        speeches = map(func, rows)
        deque(speeches, maxlen=0)
    else:
        with Pool(workers, global_elastic) as pool:
            speeches = pool.imap(func, rows)
            deque(speeches, maxlen=0)


def do_query(query, index_name, size, exact=False, operator="and", return_fields=None,
             elastic=None):
    if return_fields is None:
        return_fields = []

    if elastic is None:
        elastic = create_elastic()

    query_type = "match_phrase" if exact else "match"
    
    result = elastic.search(
        index=index_name,
        doc_type=DOC_TYPE,
        body={
            "explain": True,
            "query": {
                query_type: {query_field: TEXT_FIELD}
            },
            "operator": operator,
        },
        stored_fields=return_fields,
        size=size,
        sort=DOC_TYPE,
    )
    return result


def do_get(identifier, index_name, elastic=None):
    if elastic is None:
        elastic = create_elastic()
    return elastic.get(id=identifier, index=index_name, doc_type=DOC_TYPE)
    
