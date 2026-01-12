# https://www.youtube.com/watch?v=ZvU7lupoXQE

import requests

response = requests.get("https://jsonplaceholder.typicode.com/posts/1")


data = response.json()

print(data)
