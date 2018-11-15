from pathlib import Path
from dataclasses import dataclass, asdict

from lxml import etree
from unidecode import unidecode


@dataclass
class Speech:
    speaker: str
    speaker_id: int
    duration: int
    date: str
    time: str
    text: str

    def index(self):
        pass

    def asdict(self):
        return asdict(self)

    
@dataclass
class Member:
    id: int
    first_name: str
    last_name: str
    full_name: str
    division: str
    house: str
    party: str
    from_date: str
    from_why: str
    to_date: str
    to_why: str

    def asdict(self):
        return asdict(self)

    
def members_from_xml_paths(xml_paths):
    for xml_path in xml_paths:
        yield from members_from_xml(xml_path)

    
def members_from_xml(xml_path):
    tree = etree.parse(xml_path)

    for element in tree.iter(tag=etree.Element):
        if element.tag != 'member':
            continue

        first_name = element.get('firstname')
        last_name = element.get('lastname')

        yield Member(
            id=int(element.get('id').split('/')[-1]),
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
        )


def speeches_from_xml_paths(xml_paths):
    for xml_path in xml_paths:
        print(f'processing {xml_path}')
        yield from speeches_from_xml(xml_path)


def speeches_from_xml(xml_path):
    tree = etree.parse(xml_path)
    date = Path(xml_path).stem
    
    for element in tree.iter(tag=etree.Element):
        if element.tag != 'speech':
            continue
        if not element.get('speakername'):
            # ignore entries without speaker names
            continue

        paragraphs = []
        for p_tag in element.findall('p'): 
            if p_tag.text is None or p_tag.text == '\n':
                continue
            etree.strip_tags(p_tag, '*')
            paragraphs.append(p_tag.text)

        all_text = unidecode('\n\n'.join(paragraphs))

        speakerid = element.get('speakerid')
        if speakerid == 'unknown':
            speaker_id = 0
        else:
            speaker_id = int(speakerid.split('/')[-1])

        yield Speech(
            speaker_id=speaker_id,
            speaker=element.get('speakername'),
            duration=element.get('approximate_duration'),
            time=element.get('time'),
            text=all_text,
            date=date,
        )
