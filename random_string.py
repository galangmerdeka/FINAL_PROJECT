import random

MAX_LIMIT = 255
random_string = ''
def getRandomString():
    for i in range(10):
            random_int = random.randint(0, MAX_LIMIT)
            random_string += str(random_int)
    return random_string