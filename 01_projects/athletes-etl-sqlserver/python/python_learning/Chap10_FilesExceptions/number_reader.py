
import json

filename = 'Chap10_FilesExceptions/numbers.json'

with open(filename) as f:
    numbers = json.load(f)
print(numbers)