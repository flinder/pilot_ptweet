import json
import io 
import sys
import tarfile
from pprint import pprint 
import time
import gzip
import sqlalchemy
import os

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
    if not keyword_in(tweet):
        return False
    return True

# Parameters 
ARCHIVES = ['../../data/pablos_stuff/keyword_tweets/germany-2013-03.tar',
            '../../data/pablos_stuff/keyword_tweets/germany-2013-04.tar',
            '../../data/pablos_stuff/keyword_tweets/germany-2013-05.tar',
            '../../data/pablos_stuff/keyword_tweets/germany-2016-06.tar',
            '../../data/pablos_stuff/keyword_tweets/germany-2016-07.tar',
            '../../data/pablos_stuff/keyword_tweets/germany-2016-08.tar',
            '../../data/pablos_stuff/keyword_tweets/germany-2016-09.tar',
            ]

# Set up sqlalchemy session
session = make_session()

for archive in ARCHIVES:
    
    print('Processing {}...'.format(os.path.basename(archive)))
    with tarfile.open(archive, 'r') as archive:

        decode_err = 0
        tweet_count = 0
        for i, entry in enumerate(archive):
            
            # Skip folders
            if entry.isdir():
                continue

            # Get file out of tar archive
            extracted = archive.extractfile(entry)

            # Decompress
            with gzip.open(extracted, 'rb') as infile:
                
                # Process lines (each line a tweet)
                for ln, line in enumerate(infile):
                    try:
                        tweet = json.loads(line.decode("utf-8"))
                        tweet_count += 1
                    except json.decoder.JSONDecodeError as e:
                        print(ln, e)
                        decode_err += 1
                        continue
                    
                    # Check if tweet is relevant and valid
                    if not is_valid(tweet):
                        continue

                    # Handle all tweet information
                    id_ = int(tweet['id_str'])
                    
                    ## Handle Optional tweet fields
                    try:
                        qsid = tweet['quoted_status_id']
                    except KeyError:
                        qsid = None

                    if tweet['coordinates'] is None:
                        lon = None
                        lat = None
                    else:
                        lon=tweet['coordinates']['coordinates'][0],
                        lat=tweet['coordinates']['coordinates'][1],
                   
                    ts = time.strftime('%Y-%m-%d %H:%M:%S', 
                                       time.strptime(tweet['created_at'],
                                                     '%a %b %d %H:%M:%S +0000 %Y'))
                    ## Make mapper object
                    t = Tweet(id=int(tweet['id_str']),
                              is_quote_status=tweet['is_quote_status'],
                              in_reply_to_status_id=tweet['in_reply_to_status_id'],
                              in_reply_to_user_id=tweet['in_reply_to_user_id'],
                              quoted_status_id=qsid,
                              source=tweet['source'],
                              longitude=lon,
                              latitude=lat,
                              lang=tweet['lang'],
                              created_at=ts,
                              retweet_count=tweet['retweet_count'],
                              favorite_count=tweet['favorite_count'],
                              text=tweet['text'], 
                              user_id=tweet['user']['id'],
                              data_group='pb_keyword')
                    ## Add to db session 
                    try:
                        session.add(t)
                        session.commit()
                    except sqlalchemy.exc.IntegrityError as e:
                        session.rollback()



                    # Handle user information

                    ## Check if user is already in db
                    user = tweet['user']
                    uid = int(user['id_str'])
                    
                    uts = time.strftime('%Y-%m-%d %H:%M:%S', 
                                       time.strptime(user['created_at'],
                                                     '%a %b %d %H:%M:%S +0000 %Y'))
                    u = User(id=uid,
                             contributors=user['contributors_enabled'],
                             created_at=uts,
                             description=user['description'],
                             followers_count=user['followers_count'],
                             friends_count=user['friends_count'],
                             geo_enabled=user['geo_enabled'],
                             lang=user['lang'],
                             listed_count=user['listed_count'],
                             location=user['location'],
                             name=user['name'],
                             screen_name=user['screen_name'],
                             statuses_count=user['statuses_count'],
                             time_zone=user['time_zone'],
                             verified=user['verified'],
                             data_group='pb_keyword',
                             profile_date=ts # From tweet
                             )
                    try:
                        session.add(u)
                        session.commit()
                    except sqlalchemy.exc.IntegrityError as e:
                        session.rollback()

        print("Processed {} tweets.".format(tweet_count))
