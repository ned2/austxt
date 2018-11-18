import sys
import click
import logging
from itertools import chain
from pathlib import Path
from dataclasses import asdict
from multiprocessing import Pool

from .process import speeches_from_xml
from .models import Speech, Member


logging.basicConfig()
logger = logging.getLogger(__package__)


class CatchCliExceptions(click.Group):
    """Allows catching specific exceptions at the CLI top level"""
    def __call__(self, *args, **kwargs):
        try:
            return self.main(*args, **kwargs)
        except Exception as error:
            print(error, file=sys.stderr)
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
@click.option('--output', default='debates.csv',
              help="Output file to write CSV data to.")
@click.option('--limit', default=None, type=int,
              help="Limit the processing to some number of files.")
@click.option('--files', default=None, type=str,
              help="Limit the processing to these comma separated file names.")
@click.option('--workers', default=1)
def process_debates(path, output, limit, files, workers):
    xml_paths = sorted(path for path in Path(path).glob('*.xml'))

    if files is not None:
        filter_files = set(files.split(','))
        xml_paths = [path for path in xml_paths if path.name in filter_files]
    
    if limit is not None:
        xml_paths = xml_paths[:limit]

    xml_path_strs = [str(path) for path in xml_paths]
    func = speeches_from_xml

    if workers == 1:
        speeches = chain.from_iterable(map(func, xml_path_strs))
    else:
        with Pool(workers) as pool:
            speeches = chain.from_iterable(pool.map(func, xml_path_strs))

    speeches_df = Speech.to_dataframe(speeches)
    speeches_df.to_csv(output)

            
@cli.command(name='get-members')
@click.argument('path', nargs=-1, type=click.Path(exists=True))
@click.option('--output', default='members.csv')
def get_members(path, output):
    members = chain.from_iterable(members_from_xml(p) for p in path)
    members_df = pd.DataFrame.from_records(m.asdict() for m in members)
    members_df.to_csv(output)
