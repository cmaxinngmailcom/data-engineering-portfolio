# Examining JSON Data
# Downloading Earthquake Data

import json

# Explore the structure of the data.

filename = 'Chap16_DownloadingData/Data/eq_data_1_day_m1.json'

with open(filename) as f:
    all_eq_data = json.load(f)

readable_file = 'Chap16_DownloadingData/Data/readable_eq_data.json'
with open(readable_file, 'w') as f:
    json.dump(all_eq_data, f, indent=4)
