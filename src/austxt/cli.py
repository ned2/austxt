import click
from itertools import chain
from pathlib import Path
from dataclasses import asdict
from multiprocessing import Pool

import pandas as pd

from .process import speeches_from_xml


class CatchCliExceptions(click.Group):
    """Allows catching specific exceptions at the CLI top level"""
    def __call__(self, *args, **kwargs):
        try:
            return self.main(*args, **kwargs)
        except Exception as error:
            print(error)
            import ipdb; ipdb.post_mortem()


@click.group(cls=CatchCliExceptions)
def cli():
    pass


@cli.command(name='process-debates')
@click.argument('path')
@click.option('--output', default='debates.csv')
@click.option('--workers', default=1)
def process_debates(path, output, workers):
    func = speeches_from_xml
    xml_paths = [str(path) for path in Path(path).glob('*.xml')]
    
    if workers == 1:
        speeches = chain.from_iterable(map(func, xml_paths))
    else:
        with Pool(workers) as pool:
            speeches = chain.from_iterable(pool.map(func, xml_paths))

    speeches_df = pd.DataFrame.from_records(s.asdict() for s in speeches)
    speeches_df.to_csv(output)

            
@cli.command(name='get-members')
@click.argument('path', nargs=-1)
@click.option('--output', default='members.csv')
def get_members(path, output):
    members = chain.from_iterable(members_from_xml(p) for p in path)
    members_df = pd.DataFrame.from_records(m.asdict() for m in members)
    members_df.to_csv(output)
