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

# Parameters 
INFILE = '../../data/pablos_stuff/rnd_usr_sample/users-2016-10-19.json'

# Set up sqlalchemy session
session = make_session()

with io.open(INFILE, 'r') as infile:

    tweet_count = 0
    duplicates = 0
    for i, line in enumerate(infile):
       
        try: 
            user = json.loads(line)
            tweet_count += 1
        except json.decoder.JSONDecodeError as e:
            print(e)
            continue

        uid = int(user['id_str'][0])
        
        uts = time.strftime('%Y-%m-%d %H:%M:%S', 
                           time.strptime(user['created_at'][0],
                                         '%a %b %d %H:%M:%S +0000 %Y'))
        try:
            ts = time.strftime('%Y-%m-%d %H:%M:%S', 
                               time.strptime(user['status']['created_at'][0],
                                             '%a %b %d %H:%M:%S +0000 %Y'))
        except KeyError:
            print(i, 'No status')
            ts = None
        tz = None

        u = User(id=uid,
                 contributors=user['contributors_enabled'][0],
                 created_at=uts,
                 description=user['description'][0],
                 followers_count=user['followers_count'][0],
                 friends_count=user['friends_count'][0],
                 geo_enabled=user['geo_enabled'][0],
                 lang=user['lang'][0],
                 listed_count=user['listed_count'][0],
                 location=user['location'][0],
                 name=user['name'][0],
                 screen_name=user['screen_name'][0],
                 statuses_count=user['statuses_count'][0],
                 time_zone=tz,
                 verified=user['verified'][0],
                 data_group='de_panel',
                 profile_date=ts # From tweet
                 )
        try:
            session.add(u)
            session.commit()
        except sqlalchemy.exc.IntegrityError as e:
            duplicates += 1
            print('{} duplicates'.format(duplicates))
            session.rollback()

    print("Processed {} tweets.".format(tweet_count))
