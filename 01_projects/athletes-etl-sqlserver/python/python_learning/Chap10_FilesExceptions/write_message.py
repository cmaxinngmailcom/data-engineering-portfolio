# Writing to a File
# Writing to an Empty File

filename = 'Chap10_FilesExceptions/programming.txt'

with open(filename, 'w') as file_object:
    file_object.write("I love programming.")
