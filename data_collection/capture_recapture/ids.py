import random
from numpy.random import randint
import sys
import radar
from datetime import datetime
import time

sys.path.append('snowflake2time/python')
from snowflake import utc2snowflake
# Functionality to generate 32 and 64 bit random twitter ids



def gen_64(time_start, time_stop):
    '''
    Generate random 64 bit Twitter id
    
    Arguments:
    seed seed for random number generator

    Returns:
    int64 id
    int   seed
    '''
    time_stamp = gen_timestamp(time_start, time_stop)
    data_center = randint(0, 31) # max 5 bit
    machine = randint(0, 31) 
    sequence = randint(0, 5) # max 12 bit

    print(time_stamp, data_center, machine, sequence)
    out = time_stamp | data_center | machine | sequence
    return out


def gen_timestamp(start='2016-02-02T00:00:00', stop=datetime.now()):

    random_time = radar.random_datetime(start=start, stop=stop)
    utc_timestamp = time.mktime(random_time.timetuple())
    out = utc2snowflake(utc_timestamp)
    return out


def gen_32():
    return random.getrandbits(32)

def gen_id():
    type64 = bool(randint(0, 1))
    if type64:
        return gen_64
    else:
        return gen_32

print(gen_timestamp())
    
