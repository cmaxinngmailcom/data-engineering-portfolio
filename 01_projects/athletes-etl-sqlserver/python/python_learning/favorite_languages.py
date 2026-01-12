

favorite_languages = {
'jen': 'python',
'sarah': 'c',
'edward': 'ruby',
'phil': 'python',
}
for name, language in favorite_languages.items():
	print(f"{name.title()}'s favorite language is {language.title()}.")

# Looping Through All the Keys in a Dictionary

favorite_languages = {
'jen': 'python',
'sarah': 'c',
'edward': 'ruby',
'phil': 'python',
}
for name in favorite_languages.keys():
	print(name.title())
 
#############

favorite_languages = {
'jen': 'python',
'sarah': 'c',
'edward': 'ruby',
'phil': 'python',
}

friends = ['phil', 'sarah']
for name in favorite_languages.keys():
	print(name.title())
	
	if name in friends:
		language = favorite_languages[name].title()
		print(f"\t{name.title()}, I see you love {language}!")
		

#############

favorite_languages = {
'jen': 'python',
'sarah': 'c',
'edward': 'ruby',
'phil': 'python',
}
if 'erin' not in favorite_languages.keys():
	print("Erin, please take our poll!")
		


# Looping Through a Dictionary’s Keys in a Particular Order

favorite_languages = {
'jen': 'python',
'sarah': 'c',
'edward': 'ruby',
'phil': 'python',
}

print("The following languages have been mentioned:")

for language in favorite_languages.values():
	print(language.title())


#############

favorite_languages = {
'jen': 'python',
'sarah': 'c',
'edward': 'ruby',
'phil': 'python',
}

print("The following languages have been mentioned:")

for language in set(favorite_languages.values()):
	print(language.title())











