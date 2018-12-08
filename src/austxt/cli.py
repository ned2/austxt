import sys
import click
import logging
import traceback
from json import dumps
from pathlib import Path

import pandas as pd

from .process import process_debates, get_members
from .elastic import index_speeches, do_query, do_get
from .utils import add_results_to_dataframe, process_query_result


DEFAULT_INDEX = 'austxt'


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
def run_process_debates(**kwargs):
    process_debates(**kwargs)

            
@cli.command(name='get-members')
@click.argument('path', nargs=-1, type=click.Path(exists=True))
@click.option('--output', default='members.csv')
def run_get_members(**kwargs):
    """Process one or more members XML files"""
    get_members(**kwargs)


@cli.command(name='index-speeches')
@click.argument('speech-type', type=click.Choice(['senate', 'representatives']))
@click.argument('path', type=click.Path(exists=True))
@click.option('--index-name', default=DEFAULT_INDEX)
@click.option('--limit', default=None, type=int)
@click.option('--workers', default=1, type=int)
def run_index_speeches(**kwargs):
    index_speeches(**kwargs)


       
@cli.command(name='get')
@click.argument('identifier', )
@click.option('--index-name', default=DEFAULT_INDEX)
def run_get(**kwargs):
    result = do_get(**kwargs)
    print(dumps(result))

    
@cli.command(name='query')
@click.argument('query', )
@click.option('--index-name', default=DEFAULT_INDEX)
@click.option('--exact/--no-exact', default=False)
@click.option('--size', type=click.IntRange(1, 500000), default=10)
@click.option('--return-fields', default='')
@click.option('--json/--no-json', default=False)
def run_query(query, index_name, exact, size, return_fields, json):
    result = do_query(query, index_name, exact, size, return_fields.split(','))
    if json :
        print(dumps(result))
    else:
        docs = process_query_result(result)
        for doc, tf in docs:
            print(f"{doc:21} {tf:2}")


@cli.command(name='make-dataset')
@click.argument('input-path', type=click.Path(exists=True, dir_okay=False))
@click.argument('query')
@click.option('--index-name', default=DEFAULT_INDEX)
@click.option('--exact/--no-exact', default=False)
@click.option('--size', type=click.IntRange(1, 500000), default=10)
@click.option('--output-path', type=click.Path())
def make_dataset(input_path, query, index_name, exact, size, output_path):
    df = pd.read_csv(input_path)
    result = do_query(query, index_name, exact, size)
    parsed_results = process_query_result(result)
    column_name = "_".join(query.split())
    df = add_results_to_dataframe(parsed_results, df, column_name)

    if output_path is None:
        output_path =  f"{Path(input_path).stem}_{column_name}.csv"

    df.to_csv(output_path)
