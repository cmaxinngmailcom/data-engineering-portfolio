import time
import random

from concurrent.futures import ThreadPoolExecutor

tables =  ["orders","products","customers","reviews","cancels"]
   
def my_func(i):

    wait = random.randint(1,10)
    time.sleep(wait)
    print (f"I am {i} . I took {wait}  seconds")

with ThreadPoolExecutor(max_workers=len(tables)) as executor:
# above workers are being created

# Still runs in paralell
# i assigns table value but doesn't wait for it to complete, it goes to next table value
# This is using Submit Method instead of Map Method

    for i in tables:
        future = executor.submit(my_func,i)

# Exec Results
# Runs Parallelly ****************************************
#I am customers . I took 1  seconds
#I am orders . I took 3  seconds
#I am products . I took 3  seconds
#I am reviews . I took 6  seconds
#I am cancels . I took 6  seconds