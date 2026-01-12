
squares = []
for value in range(1, 11):
	square = value ** 2
	squares.append(square)

print(squares)


squares = []
for value in range(1,11):
	squares.append(value**2)
print(squares)

# Simple Statistics with a List of Numbers

digits = [1, 2, 3, 4, 5, 6, 7, 8, 9, 0]
min(digits)

max(digits)

sum(digits)


# List Comprehensions

# The approach described earlier for generating the list squares consisted of using three or four lines of code. A list comprehension allows you to generate
# this same list in just one line of code.

squares = [value**2 for value in range(1, 11)]
print(squares)
