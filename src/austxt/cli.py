import click
from pathlib import Path
from dataclasses import asdict

import pandas as pd

from .process import speeches_from_xml_paths, members_from_xml_paths 


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
def process_debates(path, output):
    xml_paths = [str(path) for path in Path(path).glob('*.xml')]
    speeches = speeches_from_xml_paths(xml_paths)
    speeches_df = pd.DataFrame.from_records(s.asdict() for s in speeches)
    speeches_df.to_csv(output)

            
@cli.command(name='get-members')
@click.argument('path', nargs=-1)
@click.option('--output', default='members.csv')
def get_members(path, output):
    members = members_from_xml_paths(path)
    members_df = pd.DataFrame.from_records(m.asdict() for m in members)
    members_df.to_csv(output)


