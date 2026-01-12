bicycles = ['trek', 'cannondale', 'redline', 'specialized']
print(bicycles)

print(bicycles[0])


print(bicycles[0].title())
print(bicycles[1])
print(bicycles[3])

print(bicycles[-1])
print(bicycles[-2])
print(bicycles[-3])

message = f"My first bicycle was a {bicycles[0].title()}."
print(message)

cars = ['BMW','Mercedez','Ferrari','McLaren','Benetton']
print(cars)
print(cars[0])
print(cars[0].title())
print(cars[1])
print(cars[4])

print(cars[-1])
print(cars[-3])
print(cars[-2])

#Modifying Elements in a List

motorcycles = ['honda', 'yamaha', 'suzuki']
print(motorcycles)
motorcycles[0] = 'ducati'
print(motorcycles)

#Adding Elements to a List

#Appending Elements to the End of a List

motorcycles = ['honda', 'yamaha', 'suzuki']
print(motorcycles)

motorcycles.append('ducati')
print(motorcycles)

motorcycles = []
motorcycles.append('honda')
motorcycles.append('yamaha')
motorcycles.append('suzuki')
print(motorcycles)


#Inserting Elements into a List

motorcycles = ['honda', 'yamaha', 'suzuki']
motorcycles.insert(0, 'ducati')
print(motorcycles)

#Removing Elements from a List

# Removing an Item Using the del Statement

motorcycles = ['honda', 'yamaha', 'suzuki']
print(motorcycles)


del motorcycles[0]
print(motorcycles)


del motorcycles[1]
print(motorcycles)




# Removing an Item Using the pop() Method
#########################################

motorcycles = ['honda', 'yamaha', 'suzuki']
print(motorcycles)
popped_motorcycle = motorcycles.pop()
print(motorcycles)
print(popped_motorcycle)


motorcycles = ['honda', 'yamaha', 'suzuki']
last_owned = motorcycles.pop()
print(f"The last motorcycle I owned was a {last_owned.title()}.")


# Popping Items from any Position in a List
###########################################

motorcycles = ['honda', 'yamaha', 'suzuki']
print(motorcycles)
first_owned = motorcycles.pop(0)
print(f"The first motorcycle I owned was a {first_owned.title()}.")


# Removing an Item by Value
###########################

motorcycles = ['honda', 'yamaha', 'suzuki', 'ducati']
print(motorcycles)

motorcycles.remove('ducati')
print(motorcycles)









