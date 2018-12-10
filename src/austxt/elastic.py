import os
import sys
import urllib3
from multiprocessing import Pool, current_process
from functools import partial
from collections import deque

import pandas as pd
from elasticsearch import Elasticsearch

from . import config


DOC_TYPE = 'doc'
TEXT_FIELD = 'text'
ELASTIC = None

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def create_elastic():
    return Elasticsearch(config.ELASTIC_ADDRESS, timeout=300,
                         verify_certs=False, use_ssl=True)


def global_elastic():
    global ELASTIC
    if ELASTIC is None:
        ELASTIC = create_elastic()

    
def index_speech(index, row_tuple):
    i, row = row_tuple
    try:
        ELASTIC.index(
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


def do_query(query, index_name, size, query_type):
    global_elastic()
    if query_type == "exact":
        body= {
            "explain": True,
            "query": {
                "match_phrase": {
                    TEXT_FIELD : query
                }
            }
        }
    else:
        body = {
            "explain": True,
            "query": {
                "match": {
                    TEXT_FIELD :{ 
                        "query" : query,
                        "operator": query_type,
                    }
                },
            }
        }
        
    result = ELASTIC.search(
        index=index_name,
        doc_type=DOC_TYPE,
        body=body,
        stored_fields=[],
        size=size,
        # sort by doc rather than query score, faster because
        # elasticsearch will not have to do any scoring
        sort='_doc',
    )
    return result


def do_get(identifier, index_name):
    global_elastic()
    return ELASTIC.get(id=identifier, index=index_name, doc_type=DOC_TYPE)
    
