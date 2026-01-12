# Indenting Unnecessarily
message = "Hello Python world!"
#	print(message)

# Indenting Unnecessarily After the Loop

# If you accidentally indent code that should run after a loop has finished, that
# code will be repeated once for each item in the list. 

magicians = ['alice', 'david', 'carolina']
for magician in magicians:
	print(f"{magician.title()}, that was a great trick!")
	print(f"I can't wait to see your next trick, {magician.title()}.\n")
	print("Thank you everyone, that was a great magic show!")
