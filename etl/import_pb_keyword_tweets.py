import json
import io 
import sys 
import tarfile
from pprint import pprint 
import time
import gzip
import sqlalchemy
import os
from sqlalchemy.sql import exists
import psycopg2

# Custom Imports 
sys.path.append('../database/')
from db import make_session, make_sql, User, Tweet

def keyword_in(tweet):
    text = tweet['text']
    kws = ['fluchtling', 'flüchtling', 'fluechtling', 'asyl'] 
    for kw in kws:
        if kw in text:
            return True
    return False

def is_valid(tweet):
    keys = ['text', 'id_str', 'created_at']
    for k in keys:
        try:
            tweet[k]
        except KeyError:
            return False
    return True

def read_tweet(line):
    '''
    Read line from infile, check if valid json, check if valid tweet (has all
    required fields) and check if relevant tweet (has relevant keywords)
    '''
    
    # Read in line, if decoder can't recognize json exit
    try:
        tweet = json.loads(line)
    except json.decoder.JSONDecodeError:
        return None

    # Check if all necessary fields are present
    if not is_valid(tweet):
        return None

    # Check if tweet has any of the keywords 
    if not keyword_in(tweet):
        return None

    # If all checks pass return the tweet as dictionary
    return tweet
if __name__ == "__main__":


    # Parameters 
    INFILE = '../../data/pablos_stuff/keyword_tweets/pb_keyword_tweets.json'

    # Set up sqlalchemy session
    session = make_session()
        
    with io.open(INFILE, 'r') as infile:
            
        invalid_lines = 0
        ts = 0
        us = 0
        s = time.time()
        for ln, line in enumerate(infile):

            tweet = read_tweet(line)
            if tweet is None:
                invalid_lines += 1
            else:
                u, t, uid, tid = make_sql(tweet, 'pb_keyword') # user, tweet, userid, tweetid

                ## Insert them to database
                user_exists = session.query(exists().where(User.id==uid)).scalar()
                tweet_exists = session.query(exists().where(Tweet.id==tid)).scalar()
                
                if not user_exists:
                    session.add(u)
                    us += 1
                        
                if not tweet_exists:
                    session.add(t)
                    ts += 1

            if ln % 10000 == 0:
                print("Processed {} lines. {} relevant tweets, "
                      "{} tweets inserted, {} users inserted.".format(
                          ln, ln - invalid_lines, ts, us))
                session.commit()
                print('This chunk took: {}s'.format(round(time.time() - s, 2)))
                s = time.time()
                

