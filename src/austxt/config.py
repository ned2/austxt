import os
from pathlib import Path


DEFAULT_INDEX = 'austxt'
ELASTIC_MAX_RESULTS = 500000
ELASTIC_ADDRESS = os.getenv('AUSTXT_ELASTIC_ADDRESS', 'localhost:9200')
DATA_PATH = Path(os.getenv("AUSTXT_DATA_PATH", "."))
DOWNLOAD_PATH = DATA_PATH / 'download'
SENATES_CSV_PATH = DATA_PATH / "senate_speeches_notext.csv"
REPRESENTATIVES_CSV_PATH = DATA_PATH / "representatives_speeches_notext.csv"

