# Parsing the CSV File Headers
import csv

filename = 'Chap16_DownloadingData/Data/sitka_weather_07-2018_simple.csv'

with open(filename) as f:
    reader = csv.reader(f)
    header_row = next(reader)
    print(header_row)
