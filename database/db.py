import json
import sys
import time

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import (
        create_engine,
        Column, 
        Integer, 
        String, 
        BigInteger, 
        Boolean, 
        Sequence, 
        Float,
        DateTime
)
from sqlalchemy.orm import sessionmaker

Base = declarative_base()

class Tweet(Base):
    __tablename__ = 'tweets'

    id = Column(BigInteger, primary_key=True)
    is_quote_status = Column(Boolean)
    in_reply_to_status_id = Column(String)
    in_reply_to_user_id = Column(String)
    quoted_status_id = Column(BigInteger)
    source = Column(String)
    longitude = Column(Float)
    latitude = Column(Float)
    lang = Column(String)
    created_at = Column(DateTime(timezone=True))
    retweet_count = Column(Integer)
    favorite_count = Column(Integer)
    text = Column(String)
    user_id = Column(BigInteger)
    data_group = Column(String)
    


class User(Base):
    
    __tablename__ = 'users'

    id = Column(BigInteger, primary_key=True)
    contributors = Column(Boolean)
    created_at = Column(DateTime(timezone=True))
    description = Column(String)
    followers_count = Column(Integer)
    friends_count = Column(Integer)
    geo_enabled = Column(Boolean)
    lang = Column(String)    
    listed_count = Column(Integer)
    location = Column(String)
    name = Column(String)
    screen_name = Column(String)
    statuses_count = Column(Integer)
    time_zone = Column(String)
    verified = Column(Boolean)
    profile_date = Column(DateTime(timezone=True))
    data_group = Column(String)

#class Hashtag(Base):
#    pass
#
#class HashtagMention(Base):
#    pass
#
#class UserMention(Base):
#    pass



def make_session(credential_file='../database/db_credentials'):

    with open(credential_file, 'r') as credfile:
        db_credentials = json.load(credfile)

    engine_name = 'postgresql://{user}:{password}@localhost/{db}'.format(
            user=db_credentials['user'],
            password=db_credentials['password'],
            db='dissertation'
            )

    engine = create_engine(engine_name)

    Base.metadata.create_all(engine)
    
    Session = sessionmaker(bind=engine)
    session = Session()

    return session

def get_str_as_int(d, key):
    string = d.get(key, None)
    if isinstance(string, dict):
        string = string['$numberLong']
    if string:
        return int(string)
    else:
        return None

def make_sql(tweet, data_identifier):
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
             data_group=data_identifier,
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
    
    # TODO: Find a cleaner solution for this 

    t = Tweet(id=id_,
              is_quote_status=tweet.get('is_quote_status', None),
              in_reply_to_status_id=get_str_as_int(tweet,
                                                   'in_reply_to_status_id_str'),
              in_reply_to_user_id=get_str_as_int(tweet, 'in_reply_to_user_id_str'),
              quoted_status_id=get_str_as_int(tweet, 'quoted_status_id'),
              source=tweet.get('source', None),
              longitude=lon,
              latitude=lat,
              lang=tweet.get('lang', None),
              created_at=tt,
              retweet_count=tweet.get('retweet_count', None),
              favorite_count=tweet.get('favorite_count', None),
              text=tweet.get('text', None), 
              user_id=uid,
              data_group=data_identifier)

    return u, t, uid, id_


if __name__ == "__main__":

    make_session()
