from etl.schemas.billboard_magazine_3.curation import curate as curator
from etl.schemas.billboard_magazine_3.curation import location
from etl.schemas.billboard_magazine_3.curation import dates

if __name__ == "__main__":
    #extract_to_csv()
    curator.curate_events()
