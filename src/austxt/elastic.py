import os
import sys
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

    
def index_speech(index, row_tuple):
    i, row = row_tuple
    try:
        elastic.index(
            id=row["speech_id"],
            body={'text':row['text']},
            index=index,
            doc_type=DOC_TYPE,
        )
    except urllib3.exceptions.ProtocolError:
        print(f"Error indexing: {row['speech_id']}", file=sys.stderr)
    if (i + 1) % 1000 == 0:
        print(i + 1)


def index_speeches(path, index_name, limit, workers):
    """Index an extracted CSV file of speeches"""
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    speech_df = pd.read_csv(path, nrows=limit)
    func = partial(index_speech, index_name)
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
                query_type: {
                    TEXT_FIELD :{ 
                        "query" : query,
                        "operator": operator,
                    }
                },
            }
        },
        stored_fields=return_fields,
        size=size,
        # sort by doc rather than query score, faster because
        # elasticsearch will not have to do any scoring
        sort='_doc',
    )
    return result


def do_get(identifier, index_name, elastic=None):
    if elastic is None:
        elastic = create_elastic()
    return elastic.get(id=identifier, index=index_name, doc_type=DOC_TYPE)
    
