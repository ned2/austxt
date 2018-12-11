import sys
import click
import logging
import traceback
from json import dumps
from pathlib import Path

import pandas as pd

from .process import process_speeches, get_members
from .elastic import index_speeches, do_query, do_get
from .utils import process_query_result, make_dataset, query_to_column_name
from . import config


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


@cli.command(name='process-speeches')
@click.argument('speech-type', type=click.Choice(['senate', 'representatives']))
@click.argument('path', type=click.Path(exists=True, file_okay=False,
                                        readable=True))
@click.option('--members-path', type=click.Path(exists=True))
@click.option('--clean/--no-clean', default=False)
@click.option('--limit', default=None, type=int,
              help="Limit the processing to some number of files.")
@click.option('--files', default=None, type=str,
              help="Limit the processing to these comma separated file names.")
@click.option('--workers', default=1)
@click.option('--output-path', default='.', type=click.Path(file_okay=False),
              help="Output file to write CSV data to.")
def run_process_speeches(speech_type, path, members_path, clean, limit, files,
                         workers, output_path):
    """Process a directory of speech XML files."""
    output_name = f"{speech_type}_speeches"
    speeches_df = process_speeches(path, speech_type, members_path, clean,
                                   limit, files, workers)
    speeches_df.to_csv(f"{output_name}_full.csv", index=False)
    speeches_df.drop(['text', 'cleaned_text'], errors='ignore',
                     axis=1).to_csv(f"{output_name}_notext.csv", index=False)

    
@cli.command(name='get-members')
@click.argument('path', nargs=-1, type=click.Path(exists=True))
@click.option('--output', default='members.csv')
def run_get_members(path, output):
    """Process one or more members XML files"""
    members_df = get_members(path)
    members_df.to_csv(output, index=False)

    
@cli.command(name='index-speeches')
@click.argument('path', type=click.Path(exists=True))
@click.option('--index-name', default=config.DEFAULT_INDEX)
@click.option('--limit', default=None, type=int)
@click.option('--workers', default=1, type=int)
def run_index_speeches(**kwargs):
    """Index a CSV of exracted speeches"""
    index_speeches(**kwargs)

       
@cli.command(name='get-speech')
@click.argument('identifier', )
@click.option('--index-name', default=config.DEFAULT_INDEX)
def run_get(**kwargs):
    """Get a speech from Elasticsearch using its ID"""
    result = do_get(**kwargs)
    print(dumps(result))

    
@cli.command(name='query')
@click.argument('query', )
@click.option('--index-name', default=config.DEFAULT_INDEX)
@click.option('--size', default=10,
              type=click.IntRange(1, config.ELASTIC_MAX_RESULTS))
@click.option('--query_type', default='and',
              type=click.Choice(["and", "or", "exact"]))
@click.option('--json/--no-json', default=False)
def run_query(query, index_name, size, query_type, json):
    """Query Elasticsearch index of speeches"""
    result = do_query(query, index_name, size, query_type)
    if json :
        print(dumps(result))
    else:
        docs = process_query_result(result)
        for doc, tf in docs:
            print(f"{doc:21} {tf:2}")


@cli.command(name='make-dataset')
@click.argument('input-path', type=click.Path(exists=True, dir_okay=False))
@click.argument('query')
@click.option('--index-name', default=config.DEFAULT_INDEX)
@click.option('--size', type=click.IntRange(1, config.ELASTIC_MAX_RESULTS),
              default=config.ELASTIC_MAX_RESULTS)
@click.option('--query-type', default='and',
              type=click.Choice(["and", "or", "exact"]))
@click.option('--output-path', default='.', type=click.Path(file_okay=False))
def run_make_dataset(input_path, query, index_name, size, query_type, output_path):
    """Create a copy of Austxt dataset with results of a query."""
    column_name = query_to_column_name(query, query_type)
    output_path =  Path(output_path) / f"{Path(input_path).stem}_{column_name}.csv"
    base_dataset_df = pd.read_csv(input_path)
    new_dataset_df = make_dataset(base_dataset_df, query, index_name, size,
                                  query_type)
    new_dataset_df.to_csv(output_path)
