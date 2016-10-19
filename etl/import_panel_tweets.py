import json
import io 
import sys
import tarfile
from pprint import pprint
import time

# Custom Imports 
sys.path.append('../database/')
from db import make_session, Tweet


# Parameters 
ARCHIVE = '../../data/pablos_stuff/rnd_usr_sample/germany-panel-tweets.tar.gz'
# Set up sqlalchemy session
session = make_session()

imported = set()
with tarfile.open(ARCHIVE, 'r') as archive:

    decode_err = 0
    tweet_count = 0
    for i, entry in enumerate(archive):
        
        # Skip folders
        if entry.isdir():
            continue

        with archive.extractfile(entry) as infile:
            for line in infile:
                try:
                    tweet = json.loads(line.decode("utf-8"))
                    tweet_count += 1
                except json.decoder.JSONDecodeError:
                    decode_err += 1
                    continue
                
                id_ = int(tweet['id_str'][0])

                if id_  in imported:
                    continue

                imported.update([id_])

                # Handle optional tweet fields
                try:
                    qsid = tweet['quoted_status_id'][0]
                except KeyError:
                    qsid = None

                if tweet['coordinates'] is None:
                    lon = None
                    lat = None
                else:
                    lon=tweet['coordinates']['coordinates'][0][0],
                    lat=tweet['coordinates']['coordinates'][1][0],
               
                ts = time.strftime('%Y-%m-%d %H:%M:%S', 
                                   time.strptime(tweet['created_at'][0],
                                                 '%a %b %d %H:%M:%S +0000 %Y'))

                t = Tweet(id=int(tweet['id_str'][0]),
                          is_quote_status=tweet['is_quote_status'][0],
                          in_reply_to_status_id=tweet['in_reply_to_status_id'],
                          in_reply_to_user_id=tweet['in_reply_to_user_id'],
                          quoted_status_id=qsid,
                          source=tweet['source'][0],
                          longitude=lon,
                          latitude=lat,
                          lang=tweet['lang'][0],
                          created_at=ts,
                          retweet_count=tweet['retweet_count'][0],
                          favorite_count=tweet['favorite_count'][0],
                          text=tweet['text'], user_id=tweet['user']['id'][0],
                          group='de_panel')
             
                session.add(t)
                
                nchunk = 10000
                if tweet_count % nchunk == 0:
                    print("Processed {} tweets".format(tweet_count))
                    print("Committing {} to database...".format(
                        nchunk))
                    session.commit()
                    print("Done")
                
    print("Processed {} tweets".format(tweet_count))
