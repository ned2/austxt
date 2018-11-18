from dataclasses import dataclass, asdict

import pandas as pd


class DataclassRecord:
    
    def asdict(self):
        return asdict(self)

    @classmethod
    def to_dataframe(cls, instances):
        records = (instance.asdict() for instance in instances)
        return pd.DataFrame.from_records(
            records,
            columns=cls.__annotations__.keys()
        )


@dataclass
class Speech(DataclassRecord):
    speaker: str
    speaker_id: int
    date: str
    time: str
    day: str
    duration: int
    text: str
    cleaned_text: str


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
    
