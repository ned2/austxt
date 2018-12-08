import os
import urllib3
from multiprocessing import Pool
from functools import partial
from collections import deque

import pandas as pd
from elasticsearch import Elasticsearch


DOC_TYPE = 'speech'
TEXT_FIELD = 'text'
EXACT_FIELD = 'exact_text'


def create_elastic():
    elastic_address = os.getenv('AUSTXT_ELASTIC_ADDRESS', 'localhost:9200')
    elastic = Elasticsearch(elastic_address, timeout=300, verify_certs=False,
                  use_ssl=True)
    return elastic


def global_elastic():
    global elastic
    elastic = create_elastic()

    
def index_speech(index, speech_type, id_prefix, row_tuple):
    i, row = row_tuple
    elastic.index(
        id=f"{id_prefix}_{row['speech_id']}", 
        body={'text':row['text'], 'speech_type':speech_type},
        index=index,
        doc_type=DOC_TYPE,
    )
    if (i + 1) % 1000 == 0:
        print(i + 1)

        
def index_speeches(speech_type, path, index_name, limit, workers):
    """Index an extracted CSV file of speeches"""
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    speech_df = pd.read_csv(path, nrows=limit)
    id_prefix = "sen" if speech_type == "senate" else "rep"
    func = partial(index_speech, index_name, speech_type, id_prefix)
    rows = ((index, row) for index, row in speech_df.iterrows())
    
    if workers == 1:
        global_elastic()
        speeches = map(func, rows)
        deque(speeches, maxlen=0)
    else:
        with Pool(workers, global_elastic) as pool:
            speeches = pool.imap(func, rows)
            deque(speeches, maxlen=0)


def do_query(query, index_name, exact, size, return_fields=None, elastic=None):
    if return_fields is None:
        return_fields = []

    if elastic is None:
        elastic = create_elastic()

    if exact:
        query_type = "term"
        query_field = EXACT_FIELD
    else:
        query_type = "match"
        query_field = TEXT_FIELD

    result = elastic.search(
        index=index_name,
        doc_type=DOC_TYPE,
        body={
            "explain": True,
            "query": {query_type: {query_field: query}}
        },
        stored_fields=return_fields,
        size=size,
        sort=["_doc"],
    )
    return result


def do_get(identifier, index_name, elastic=None):
    if elastic is None:
        elastic = create_elastic()
    return elastic.get(id=identifier, index=index_name, doc_type=DOC_TYPE)
    
