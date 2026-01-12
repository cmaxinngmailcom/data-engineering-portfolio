

# The if-elif-else Chain

# if-elif-else syntax

age = 12

if age < 4:
	print("Your admission cost is $0.")
elif age < 18:
	print("Your admission cost is $25.")
else:
	print("Your admission cost is $40.")


# Using Multiple elif Blocks


age = 65
if age < 4:
	price = 0
elif age < 18:
	price = 25
elif age < 65:
	price = 40
else:
	price = 20
print(f"Your admission cost is ${price}.")


age = 65
if age < 4:
	price = 0
elif age < 18:
	price = 25
elif age < 65:
	price = 40
elif age >= 65:
	price = 20
print(f"Your admission cost is ${price}.")
