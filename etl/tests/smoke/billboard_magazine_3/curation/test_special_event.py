from etl.schemas.billboard_magazine_3.curation.special_event import *
from etl.utils.utils import load_dimension_tables, load_event_keywords
import pytest
from config.paths import EVENT_KEYWORDS_PATH

dimension_tables = load_dimension_tables()
dim_special_events = dimension_tables['special_events']
event_keywords = load_event_keywords(EVENT_KEYWORDS_PATH)
strong_keywords = event_keywords["strong"]
weak_keywords = event_keywords["weak"]
tag_keywords = event_keywords["tags"]