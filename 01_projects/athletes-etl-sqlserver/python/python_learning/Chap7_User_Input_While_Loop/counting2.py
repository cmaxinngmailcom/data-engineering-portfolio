# First we set current_number to 0. Because it’s less than 10, Python
# enters the while loop. Once inside the loop, we increment the count by 1
# at u, so current_number is 1. The if statement then checks the modulo of
# current_number and 2. If the modulo is 0 (which means current_number is
# divisible by 2), the continue statement tells Python to ignore the rest of
# the loop and return to the beginning. If the current number is not divisible
# by 2, the rest of the loop is executed and Python prints the current number:

current_number = 0
while current_number < 10:
    current_number += 1
    if current_number % 2 == 0:
        continue

    print(current_number)