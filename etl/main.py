from etl.schemas.billboard_magazine_3.curation import curate as curator
from etl.schemas.billboard_magazine_3.curation.artists import parse_artist_names
from etl.schemas.billboard_magazine_3.curation.special_event import calc_special_event_score
from etl.schemas.billboard_magazine_3.processing.process import extract_to_csv
from utils.utils import *
import slugify

if __name__ == "__main__":
    #extract_to_csv()
    curator.curate_events()