
filename = 'Chap10_FilesExceptions/pi_digits.txt'

with open(filename) as file_object:
  #  for line in file_object:
  #      print(line.rstrip()) # rstrip() eliminates extra blank lines

# Making a List of Lines from a File

    lines = file_object.readlines()
    
for line in lines:
    print(line.rstrip())