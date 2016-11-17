from __future__ import division, print_function, unicode_literals

import io
import tweepy
import sys
import json
import numpy as np
import pandas as pd
import cPickle as pickle
import time
import os

from pprint import pprint
from itertools import count, islice

sys.path.append('../')
sys.path.append('../../database/')
import twitter_credentials as tc
from db import make_session



class idGenerator(object):
    '''
    Provides an iterator that provides random 32 bit integer ids
    without replacement
    '''
    
    def __init__(self, seed=31222):
        self.sampled_ids = set()
        self.n_samples = 0
        self.seed = seed
    def __iter__(self):
        np.random.seed(self.seed)

        for i in count():
            prop = np.random.randint(0, 4924990722) # Weirdly the youngest user in pablos sample has a 33 bit id
            if i <= self.n_samples:
                continue
            self.n_samples += 1
            if prop in self.sampled_ids:
                continue
            else:
                self.sampled_ids.update([prop])
                yield prop
                
                
def get_valid_users(results):
    '''
    Get valid users, i.e. from Germany according to following criteria
    - Last status in German
    - User interface in German
    - 
    '''
    out = []
    for r in results:
        try:
            if r['lang'] == 'de' or r['status']['lang'] == 'de':
                out.append(r)
            else:
                continue
        except KeyError:
            continue
    
    return(out)

def write_results(res, id_batch, track, outfile_name):
          
    valid = get_valid_users(res)

    queried = set(id_batch)
    hits = set([u['id'] for u in res])
    hits_de = set([u['id'] for u in valid])
    recaptured = sample_1.intersection(hits)
    
    with io.open(outfile_name, 'a') as outfile:

        for id_ in queried:
            id_ = int(id_)
            hit = hit_de = recap = 0
            if id_ in hits:
                hit = 1
            if id_ in hits_de:
                hit_de = 1
            if id_ in recaptured:
                recap = 1
            outfile.write(outline.format(id_, hit, hit_de, recap))


    track['queried'].update(queried)
    track['hits'].update(hits)
    track['hits_de'].update(hits_de)
    track['recaptured'].update(recaptured)

    return track

def file_len(f):
    for i, l in enumerate(f):
        pass
    return i + 1


if __name__ == '__main__':

    # Set params
    OUTFILE = 'crc_results.csv'
    TRACKFILE = 'crc_track.p'
    goal_n_recaptured = 10

    # Pick up where stopped last time
    id_generator = idGenerator()
    outline = '{},{},{},{}\n'
    # If running for first time
    if not os.path.isfile(OUTFILE) or not os.path.isfile(TRACKFILE):
        with io.open(OUTFILE, 'w') as outfile:
            outfile.write(outline.format('id', 'hit', 'hit_de', 'recaptured'))
        track = {"queried": set(), "hits": set(), "hits_de": set(),
                 "recaptured": set()}
    else:
        with io.open(OUTFILE, 'r') as outfile:
            id_generator.n_samples = file_len(outfile) - 1
        with io.open(TRACKFILE, 'rb') as trackfile:
            track = pickle.load(trackfile)

    
    # Set up authentication and api
    acc = tc.credentials['coll_1']
    auth = tweepy.OAuthHandler(acc['consumer_key'], acc['consumer_secret'])
    auth.set_access_token(acc['access_token'], acc['access_token_secret'])
    api = tweepy.API(auth, parser=tweepy.parsers.JSONParser())
    
    # Get all ids from pablos sample

    ## Connect to sql engine
    _, engine = make_session('../../database/db_credentials')
    query = "SELECT id FROM users WHERE data_group = 'de_panel';"
    captured_1 = pd.read_sql_query(sql=query, con=engine)
    sample_1 = set(captured_1.id)
    
    limits = api.rate_limit_status()
    remaining = limits['resources']['users']['/users/lookup']['remaining']

    for i in count():

        # Stopping condition
        if len(track['recaptured']) >= goal_n_recaptured:
            break

        # Sleep random time (Expectation 1 second == 900 requests per 15 minutes)
        time.sleep(1 + np.random.exponential(scale=0.1))

        # Generate a batch of unique random ids
        id_batch = [str(a) for a in islice(id_generator, 100)]
       
        # Wait if no requests left
        if remaining == 0:
            reset_time = limits['resources']['users']['/users/lookup']['reset']
            time_to_reset = reset_time - time.time() + 10
            msg = 'Rate limited. Waiting {} minutes...'
            print(msg.format((float(time_to_reset) + 10.0) / 60.0))
            time.sleep(time_to_reset)
            limits = api.rate_limit_status()
            remaining = limits['resources']['users']['/users/lookup']['remaining']
           
        # Query
        try:
            res = api.lookup_users(user_ids=id_batch)
            remaining -= 1
            track = write_results(res, id_batch, track, OUTFILE)
            pickle.dump(track, open(TRACKFILE, 'wb'))

        except tweepy.TweepError as e:
            error_code = e.api_code
            # Code 17 is 'no results'
            if error_code == 17:
                continue
            # Rate limited
            elif error_code == 88:
                print('Rate limited waiting 15 minutes...')
                time.sleep(60 * 15)
            else:
                raise
