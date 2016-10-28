from pymongo import MongoClient
import json
import stream_utils
from stop_words import get_stop_words
import time
from http.client import IncompleteRead
from requests.packages.urllib3.exceptions import ProtocolError
from tweepy import Stream

# Set parameters
CRED_FILE = 'twitter_credentials.json'
TRACK_ARGS = {'languages': ['de'], 'track': get_stop_words('de')}
OUTFILE = '../../data/de_stream.json'

# Load twitter credentials
at, ats, ck, cks = stream_utils.load_credentials(CRED_FILE, 'coll_1')

#This handles Twitter authetification and the connection to Twitter Streaming 
#API
l = stream_utils.Listener(out_file=OUTFILE)
auth = stream_utils.OAuthHandler(ck, cks)
auth.set_access_token(at, ats)
stream = Stream(auth, l)

# Stream data
running_smoothly = True
while True:
    try:
        running_smoothly = stream.filter(**TRACK_ARGS)
    except KeyboardInterrupt:
        # Or however you want to exit this loop
        stream.disconnect()
        break
    except Exception as e:
        print(e)
        time.sleep(60)
        
    if not running_smoothly:
        time.sleep(60*15)
