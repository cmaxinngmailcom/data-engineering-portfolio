import time
import random

from concurrent.futures import ThreadPoolExecutor

tables =  ["orders","products","customers","reviews","cancels"]
   
def my_func(i):

    wait = random.randint(1,10)
    time.sleep(wait)
    print (f"I am {i} . I took {wait}  seconds")

# for i in tables:
#    my_func(i)
# Exec Results
# Runs Sequentially ****************************************
#I am orders . I took 6  seconds
#I am products . I took 10  seconds
#I am customers . I took 1  seconds
#I am reviews . I took 6  seconds
#I am cancels . I took 9  seconds

with ThreadPoolExecutor(max_workers=len(tables)) as executor:
# above workers are being 
# Map Method all tables are executed in Paralell

    futures = executor.map(my_func,tables)

# Exec Results
# Runs Parallelly ****************************************
#I am customers . I took 1  seconds
#I am orders . I took 3  seconds
#I am products . I took 3  seconds
#I am reviews . I took 6  seconds
#I am cancels . I took 6  seconds