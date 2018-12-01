import logging
from pathlib import Path
from datetime import datetime

from lxml import etree
from unidecode import unidecode

from .models import Speech, Member
from .text import clean_speeches


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


def speeches_from_xml(xml_path, clean=True):
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
            # skip over speeches from 'The Clerk' and 'Honorable Semators' etc 
            continue
        
        paragraphs = []
        etree.strip_tags(element, 'i', 'b')
        for i, tag in enumerate(element.iter()): 
            if tag.text is None or tag.text == '\n':
                continue
            paragraphs.append(tag.text.strip())

        if not paragraphs:
            continue
        
        all_text = unidecode('\n\n'.join(paragraphs))
        date_str = Path(xml_path).stem
        speech = Speech(
            speech_id=element.get('id').split('/')[-1],
            speaker_id=int(element.get('speakerid').split('/')[-1]),
            speaker=element.get('speakername'),
            duration=element.get('approximate_duration'),
            date=date_str,
            time=element.get('time'),
            day=datetime.strptime(date_str, '%Y-%m-%d').strftime('%A'),
            text=all_text,
        )
        speeches.append(speech)

    if clean:
        clean_speeches(speeches)
        
    return speeches
