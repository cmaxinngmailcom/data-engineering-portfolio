dimensions = (200, 50)
print(dimensions[0])
print(dimensions[1])


# will generate error since you can't change a Tuple
# dimensions = (200, 50)
# dimensions[0] = 250

# Looping Through All Values in a Tuple
dimensions = (200, 50)
for dimension in dimensions:
	print(dimension)
	
# Writing over a Tuple

dimensions = (200, 50)
print("Original dimensions:")
for dimension in dimensions:
	print(dimension)
	
dimensions = (400, 100)
print("\nModified dimensions:")
for dimension in dimensions:
	print(dimension)
