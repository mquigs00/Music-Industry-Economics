from etl.schemas.billboard_magazine_3.curation import curate as curator
from etl.schemas.billboard_magazine_3.curation import location
from etl.schemas.billboard_magazine_3.curation import dates
from etl import orchestrator
from etl.preload_reference_tables import preload_dimension_tables, preload_corrections_table
from etl.schemas.billboard_magazine_3.processing.process import extract_to_csv

if __name__ == '__main__':
    extract_to_csv()