import json
from config.paths import LOCATION_ALIASES_PATH

def build_reverse_map(regular_map):
    """

    :param regular_map: a dictionary of str --> list
    :return: reverse_map: dict, each list value as the keys mapped to their original key
    """
    reverse_map = {}
    try:
        for key, value in regular_map.items():
            if isinstance(value, list):
                for subvalue in regular_map[key]:
                    reverse_map[subvalue] = key
            else:
                reverse_map[key] = value

        return reverse_map

    except FileNotFoundError:
        print("File not found for build_reverse_venue_map")