# Working with Multiple Files
# To make a program fail silently, you
# write a try block as usual, but you explicitly tell Python to do nothing in the
# except block. Python has a pass statement that tells it to do nothing in a block:

def count_words(filename):
    """Count the approximate number of words in a file."""
    try:
        with open(filename, encoding='utf-8') as f:
            contents = f.read()
    except FileNotFoundError:
        pass
    else:
        words = contents.split()
        num_words = len(words)
        print(f"The file {filename} has about {num_words} words.")


filenames = ['Chap10_FilesExceptions/alice.txt', 'Chap10_FilesExceptions/siddhartha.txt', 'Chap10_FilesExceptions/moby_dick.txt', 'Chap10_FilesExceptions/little_women.txt']

for filename in filenames:
    count_words(filename)