# Handling the ZeroDivisionError Exception
# print(5/0)

# Using try-except Blocks

try:
    print(5/0)
except ZeroDivisionError:
    print("You can't divide by zero!")
