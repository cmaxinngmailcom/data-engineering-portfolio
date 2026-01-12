import json 

filename = 'Chap10_FilesExceptions/username.json'

with open(filename) as f:
    username = json.load(f)
    print(f"Welcome back, {username}!")