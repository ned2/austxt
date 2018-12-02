import os
import sys
import click
import logging
import traceback
import urllib3
from itertools import chain
from pathlib import Path
from multiprocessing import Pool
from functools import partial
from collections import deque

import pandas as pd
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk, streaming_bulk

from .process import speeches_from_xml, members_from_xml
from .models import Speech, Member
from .utils import add_members_columns

logging.basicConfig()
logger = logging.getLogger(__package__)


class CatchCliExceptions(click.Group):
    """Allows catching specific exceptions at the CLI top level"""
    def __call__(self, *args, **kwargs):
        try:
            return self.main(*args, **kwargs)
        except Exception as error:
            print(traceback.format_exc(), file=sys.stderr)
            import ipdb; ipdb.post_mortem()


@click.group(cls=CatchCliExceptions)
@click.option('--log', default='WARNING',
              type=click.Choice(logging._levelToName.values()))
def cli(log):
    level = logging.getLevelName(log)
    logger.setLevel(level)


@cli.command(name='process-debates')
@click.argument('path', type=click.Path(exists=True, file_okay=False,
                                        readable=True))
@click.option('--members-path', type=click.Path(exists=True))
@click.option('--output', default='debates.csv',
              help="Output file to write CSV data to.")
@click.option('--clean/--no-clean', default=False)
@click.option('--limit', default=None, type=int,
              help="Limit the processing to some number of files.")
@click.option('--files', default=None, type=str,
              help="Limit the processing to these comma separated file names.")
@click.option('--workers', default=1)
def process_debates(path, output, members_path, clean, limit, files, workers):
    xml_paths = sorted(path for path in Path(path).glob('*.xml'))

    if files is not None:
        filter_files = set(files.split(','))
        xml_paths = [path for path in xml_paths if path.name in filter_files]
    
    if limit is not None:
        xml_paths = xml_paths[:limit]

    xml_path_strs = [str(path) for path in xml_paths]
    func = partial(speeches_from_xml, clean=clean)

    if workers == 1:
        speeches = chain.from_iterable(map(func, xml_path_strs))
    else:
        with Pool(workers) as pool:
            speeches = chain.from_iterable(pool.imap(func, xml_path_strs))

    speeches_df = Speech.to_dataframe(speeches)

    if not clean:
        speeches_df = speeches_df.drop('cleaned_text', axis=1)

    if members_path is not None:
        speeches_df = add_members_columns(speeches_df, members_path, ['division'])

    speeches_df.to_csv(output, index=False)

            
@cli.command(name='get-members')
@click.argument('path', nargs=-1, type=click.Path(exists=True))
@click.option('--output', default='members.csv')
def get_members(path, output):
    """Process one or more members XML files"""
    members = chain.from_iterable(members_from_xml(p) for p in path)
    members_df = Member.to_dataframe(members)
    members_df.to_csv(output, index=False)


def get_speech_index_actions(speech_df, id_prefix):
    for index, row in speech_df.iterrows():
        yield {
            '_id': f"{id_prefix}_{row['speech_id']}", 
            'text': row['text']
        }

def index_speech(index, doc_type, id_prefix, row):
    elastic.index(
        id=f"{id_prefix}_{row['speech_id']}", 
        body={'text':row['text']},
        index=index,
        doc_type=doc_type,
    )
    
    
@cli.command(name='index-speeches')
@click.argument('doc-type', type=click.Choice(['senate', 'representatives']))
@click.argument('path', type=click.Path(exists=True))
@click.option('--index-name', default='austxt')
@click.option('--limit', default=None, type=int)
@click.option('--workers', default=1, type=int)
def index_speeches(doc_type, path, index_name, limit, workers):
    """Index an extracted CSV file of speeches"""
    global elastic
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    elastic_address = os.getenv('AUSTXT_ELASTIC_ADDRESS', 'localhost:9200')
    elastic = Elasticsearch(elastic_address, timeout=300, verify_certs=False,
                                   use_ssl=True)
    speech_df = pd.read_csv(path, nrows=limit)
    id_prefix = 'sen' if doc_type == "senate" else 'rep'
    index_stream = streaming_bulk(elastic, get_speech_index_actions(speech_df, id_prefix),
                                  index=index_name, doc_type=doc_type, chunk_size=500)
    # for i, (ok, result) in enumerate(index_stream):
    #     action, result = result.popitem()
    #     doc = f"{result['_id']} ({doc_type})"
    #     if not ok:
    #         print(f"Failed to {action} document {doc}: {result}")
    #     if i % 10 == 0:           
    #         print(f"completed {i} docs")
    func = partial(index_speech, index_name, doc_type, id_prefix)
    rows = (row for _index, row in speech_df.iterrows())
    
    if workers == 1:
        speeches = map(func, rows)
        deque(speeches, maxlen=0)
    else:
        with Pool(workers) as pool:
            speeches = pool.imap(func, rows)
            deque(speeches, maxlen=0)
