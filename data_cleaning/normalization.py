import json
from config.paths import VENUE_PATTERNS_PATH
from config.paths import STATE_ALIASES_PATH

def build_reverse_map(path):
    reverse_map = {}
    try:
        with open(path, "r") as f:
            data = json.load(f)

        for key, value in data.items():
            if isinstance(value, list):
                for subvalue in data[key]:
                    reverse_map[subvalue] = key
            else:
                reverse_map[key] = value

        return reverse_map

    except FileNotFoundError:
        print("File not found for build_reverse_venue_map")