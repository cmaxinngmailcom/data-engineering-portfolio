# Looping Through an Entire List
magicians = ['alice', 'david', 'carolina']
for magician in magicians:
	print(magician)

magicians = ['alice', 'david', 'carolina']
for magician in magicians:
	print(f"{magician.title()}, that was a great trick!")
	print(f"I can't wait to see your next trick, {magician.title()}.\n")
print("Thank you, everyone. That was a great magic show!")

# Forgetting to Indent
#magicians = ['alice', 'david', 'carolina']
#for magician in magicians:
#print(magician)

# Forgetting to Indent Additional Lines

magicians = ['alice', 'david', 'carolina']
for magician in magicians:
	print(f"{magician.title()}, that was a great trick!")
print(f"I can't wait to see your next trick, {magician.title()}.\n")
