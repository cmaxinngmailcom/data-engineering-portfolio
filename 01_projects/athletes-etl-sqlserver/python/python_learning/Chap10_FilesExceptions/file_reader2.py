# Relative Path
import os
print("Current working directory:", os.getcwd())

with open('pi_digits2.txt') as file_object:
    contents = file_object.read()
print(contents.rstrip())