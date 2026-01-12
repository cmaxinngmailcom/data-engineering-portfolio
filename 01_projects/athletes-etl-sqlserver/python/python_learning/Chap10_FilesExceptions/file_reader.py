import os
print("Current working directory:", os.getcwd())

with open('Chap10_FilesExceptions/pi_digits.txt') as file_object:
    contents = file_object.read()
print(contents.rstrip())