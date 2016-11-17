from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from functools import wraps
from pymongo import MongoClient
import io
import json
import time
import errno
import os
import signal
import logging
import re
import sys
from stop_words import get_stop_words



class TimeoutError(Exception):
        pass

def check_time(start, seconds=100): 
    if time.time() - start >= seconds:
        raise TimeoutError
    else:
        pass
    return None


def timeout(seconds=10, error_message=os.strerror(errno.ETIME)):
    def decorator(func):
        def _handle_timeout(signum, frame):
            raise TimeoutError(error_message)

        def wrapper(*args, **kwargs):
            signal.signal(signal.SIGALRM, _handle_timeout)
            signal.alarm(seconds)
            try:
                result = func(*args, **kwargs)
            finally:
                signal.alarm(0)
            return result

        return wraps(func)(wrapper)

    return decorator



#This is a basic listener that just prints received tweets to stdout.
class Listener(StreamListener):
    '''
    db_connection: mongo connection to collection to dump tweets into
    '''

    def __init__(self, db_connection=None, out_file=None):
        super(Listener, self).__init__()
        self.start = time.time()
        self.db_connection = db_connection
        self.out_file = out_file
        self.count_valid = 0
        self.count_total = 0
    
    def on_data(self, data):
        self.count_total += 1
        doc = json.loads(data.strip('\n'))
        if 'limit' in doc:
            print(doc)
        elif 'delete' in doc:
            return True
        else:
            # Write data to db
            if self.db_connection is not None and self.out_file is None:
                self.db_connection.update_one({'id': doc['id']}, {'$set': doc}, 
                        upsert = True)
            elif self.out_file is not None and self.db_connection is None:
                with io.open(self.out_file, 'a+', encoding='utf-8') as out_file:
                    out_file.write(json.dumps(doc))
                    out_file.write('\n')
            elif self.out_file is None and self.db_connection is None:
                print(self.count_valid)

            self.count_valid += 1
        return True

    def on_error(self, status):
        print('Error. Code: {}'.format(status))
        if status == 420:
            raise ValueError("Rate Limited")
        return False
       

class TimedStream(Stream):

    @timeout(3600)
    def timed_filter(self, kwargs):
        self.filter(**kwargs)

    @timeout(3600)
    def timed_sample(self, kwargs):
        self.sample(**kwargs)


def load_credentials(fname, account):
    # Load twitter credentials
    with io.open(fname, 'r') as credfile:
        creds = json.loads(credfile.read())
    creds = creds[account]
    at = creds['access_token']
    ats = creds['access_token_secret']
    ck = creds['consumer_key']
    cks = creds['consumer_secret']
    return at, ats, ck, cks
