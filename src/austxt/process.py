import logging
from pathlib import Path
from datetime import datetime
from functools import partial
from multiprocessing import Pool
from itertools import chain

from lxml import etree
from unidecode import unidecode

from .models import Speech, Member
from .text import clean_speeches
from .utils import add_members_columns


logger = logging.getLogger(__package__)


def members_from_xml(xml_path):
    logger.info(f"processing {xml_path}")

    tree = etree.parse(xml_path)
    members = []

    for element in tree.iter(tag=etree.Element):
        if element.tag != 'member':
            continue

        first_name = element.get('firstname')
        last_name = element.get('lastname')

        members.append(Member(
            member_id=int(element.get('id').split('/')[-1]),
            first_name=first_name,
            last_name=last_name,
            full_name=f'{first_name} {last_name}',
            house=element.get('house'),
            division=element.get('division'),
            party=element.get('party'),
            from_date=element.get('fromdate'),
            from_why=element.get('fromwhy'),
            to_date=element.get('todate'),
            to_why=element.get('towhy'),
        ))
    return members


def speeches_from_xml(speech_type, xml_path, clean=False):
    logger.info(f'processing {xml_path}')
    tree = etree.parse(xml_path)
    speeches = []

    for element in tree.iter(tag=etree.Element):
        if element.tag != 'speech':
            continue
        if not element.get('speakername'):
            # ignore entries without speaker names
            continue
        if element.get('speakerid') == 'unknown':
            # skip over speeches from 'The Clerk' and 'Honorable Senators' etc 
            continue
        
        paragraphs = []
        etree.strip_tags(element, 'i', 'b')
        for i, tag in enumerate(element.iter()): 
            if tag.text is None or tag.text == '\n':
                continue
            paragraphs.append(tag.text.strip())

        if not paragraphs:
            continue

        id_prefix = "sen" if speech_type.startswith("sen") else "rep"
        all_text = unidecode('\n\n'.join(paragraphs))
        date_str = Path(xml_path).stem
        speech = Speech(
            speech_id=f"{id_prefix}_{element.get('id').split('/')[-1]}",
            speaker_id=int(element.get('speakerid').split('/')[-1]),
            speaker=element.get('speakername'),
            duration=element.get('approximate_duration'),
            date=date_str,
            time=element.get('time'),
            day=datetime.strptime(date_str, '%Y-%m-%d').strftime('%A'),
            text=all_text,
            num_tokens=len(all_text.split())
        )
        speeches.append(speech)

    if clean:
        clean_speeches(speeches)
        
    return speeches


def process_speeches(path, speech_type, members_path, clean, limit, files,
                     workers):
    xml_paths = sorted(path for path in Path(path).glob('*.xml'))

    if files is not None:
        filter_files = set(files.split(','))
        xml_paths = [path for path in xml_paths if path.name in filter_files]
    
    if limit is not None:
        xml_paths = xml_paths[:limit]

    xml_path_strs = [str(path) for path in xml_paths]
    func = partial(speeches_from_xml, speech_type, clean=clean)

    if workers == 1:
        speeches = chain.from_iterable(map(func, xml_path_strs))
        speeches_df = Speech.to_dataframe(speeches)
    else:
        with Pool(workers) as pool:
            speeches = chain.from_iterable(pool.imap(func, xml_path_strs))
            speeches_df = Speech.to_dataframe(speeches)

    if not clean:
        speeches_df = speeches_df.drop('cleaned_text', axis=1)

    if members_path is not None:
        speeches_df = add_members_columns(speeches_df, members_path,
                                          ['gender', 'division', 'party'])
        
    return speeches_df    

    
def get_members(path):
    """Process one or more members XML files"""
    members = chain.from_iterable(members_from_xml(p) for p in path)
    return Member.to_dataframe(members)

