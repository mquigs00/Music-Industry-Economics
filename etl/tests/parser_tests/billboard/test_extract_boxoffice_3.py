from etl.parsers.magazines.billboard import extract_boxoffice_3 as parser

def test_parse_additional_artists_basic():
    tour_data = {"artists": []}
    rest_of_tour = "SHELIA E. Detroit Mz 517.50 sevan sellouts"
    components = rest_of_tour.split()
    it = iter(components)
    next_item = next(it)

    parser.parse_additional_artist(tour_data, next_item, it)

    assert tour_data["artists"] == ["SHELIA E."]

