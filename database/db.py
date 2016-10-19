import json
import sys
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


if __name__ == "__main__":

    make_session()
