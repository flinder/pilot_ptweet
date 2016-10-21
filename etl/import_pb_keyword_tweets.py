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

# Custom Imports 
sys.path.append('../database/')
from db import make_session, Tweet, User

def keyword_in(tweet):
    text = tweet['text']
    kws = ['fluchtling', 'flüchtling', 'fluechtling', 'refugee', 'asyl'] 
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

def make_sql(tweet):
    '''
    Transforms json tweet into sqlalchemy User and Tweet objects as defined
    in /database/db.py
    '''
    
    tt = tweet.get('created_at', None)
    if tt is not None:
        tt = time.strftime('%Y-%m-%d %H:%M:%S', time.strptime(
            tt, '%a %b %d %H:%M:%S +0000 %Y'))
 
    user = tweet['user']
    uid = int(user['id_str'])
    ca = user.get('created_at', None)
    if ca is not None:
        ca = time.strftime('%Y-%m-%d %H:%M:%S', time.strptime(
            ca,'%a %b %d %H:%M:%S +0000 %Y'))

    u = User(id=uid,
             contributors=user.get('contributors_enabled', None),
             created_at=ca,
             description=user.get('description', None),
             followers_count=user.get('followers_count', None),
             friends_count=user.get('friends_count', None),
             geo_enabled=user.get('geo_enabled', None),
             lang=user.get('lang', None),
             listed_count=user.get('listed_count', None),
             location=user.get('location', None),
             name=user.get('name', None),
             screen_name=user.get('screen_name', None),
             statuses_count=user.get('statuses_count', None),
             time_zone=user.get('time_zone', None),
             verified=user.get('verified', None),
             data_group='pb_keyword',
             profile_date=tt # From tweet
             )

    # Handle all tweet information
    id_ = int(tweet['id_str'])
    
    ## Handle Optional tweet fields

    if tweet['coordinates'] is None:
        lon = None
        lat = None
    else:
        lon=tweet['coordinates']['coordinates'][0],
        lat=tweet['coordinates']['coordinates'][1],

    ## Make mapper object
    t = Tweet(id=id_,
              is_quote_status=tweet.get('is_quote_status', None),
              in_reply_to_status_id=tweet.get('in_reply_to_status_id', None),
              in_reply_to_user_id=tweet.get('in_reply_to_user_id', None),
              quoted_status_id=tweet.get('quoted_status_id', None),
              source=tweet.get('source', None),
              longitude=lon,
              latitude=lat,
              lang=tweet.get('lang', None),
              created_at=tt,
              retweet_count=tweet.get('retweet_count', None),
              favorite_count=tweet.get('favorite_count', None),
              text=tweet.get('text', None), 
              user_id=uid,
              data_group='pb_keyword')

    return u, t, uid, id_


if __name__ == "__main__":


    # Parameters 
    INFILE = '../../data/pablos_stuff/keyword_tweets/pb_keyword_tweets.json'

    # Set up sqlalchemy session
    session = make_session()
        
    with io.open(INFILE, 'r') as infile:
            
        invalid_lines = 0
        to_commit = []
        ts = 0
        us = 0
        for ln, line in enumerate(infile):

            tweet = read_tweet(line)
            if tweet is None:
                invalid_lines += 1
                continue

            u, t, uid, tid = make_sql(tweet) # user, tweet, userid, tweetid

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
                      "{} tweets inserted, {} users inserted."
                      "Committing to db...".format(ln, ln - invalid_lines, ts, 
                                                   us))
                #session.add_all(to_commit)
                session.commit()
                

