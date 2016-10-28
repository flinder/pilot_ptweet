import sys
import spacy
import numpy as np
import re
import pickle
import time
import os
import pandas as pd

from ggplot import *
from operator import itemgetter
from sqlalchemy import and_, or_
from copy import copy
from gensim import corpora
from gensim.matutils import corpus2csc

# Custom imports
sys.path.append('../database')
from db import make_session, Tweet, data_frame
sys.path.append('/home/flinder/Dropbox/current_projects/text_utils')
from text_utils import Cleaner, n_grams, url_regex

# ---------------------------------------------
# Data preprocessing
# ---------------------------------------------


# Database connection
_, engine = make_session()


# Get number of tweets by language
query = ("SELECT count(*), lang " 
         "FROM tweets "
         "WHERE data_group = 'de_panel' "
         "AND created_at >= '2015-01-01' "
         "AND created_at < '2016-01-01' " 
         "GROUP BY lang "
         "ORDER BY count DESC; ")
df_language = pd.read_sql(sql=query, con=engine)
print('Most common languages:')
print(df_language.head())
print('Number of languages:')
print(df_language.shape)

# Get the tweet dataset 
query = ("SELECT user_id, id, text, created_at, lang "
         "FROM tweets "
         "WHERE data_group = 'de_panel' "
         "AND created_at >= '2015-01-01' "
         "AND created_at < '2016-01-01' ")
df = pd.read_sql(sql=query, con=engine)

df['created_at'] = pd.to_datetime(df['created_at'], 
				  format='%Y-%m-%d %H', utc=True)
print('Main dataframe:')
print(df.head())

# Identify tweets about refugees
keyword_regex = ('([^\s]*fluechtling[^\s]*|'
                 '[^\s]*flüchtling[^\s]*|'
                 '[^\s]*refugee[^\s]*|'
                 '[^\s]*asyl[^\s]*|'
                 '[^\s]*migration[^\s]*|'
                 '[^\s]*migrant[^\s]*|'
                 '[^\s]*pegida[^\s]*)')
re_kw = re.compile(keyword_regex, re.IGNORECASE)

def is_refugee_tweet(text):
    if re_kw.search(text):
        return 1
    else:
        return 0

df['is_refugee_tweet'] = [1 if re_kw.search(t) else 0 for t in df.text]

###  Look at some examples
#for i,t in enumerate(df[df['is_refugee_tweet'] == 1].text):
#    print(t)
#    if i == 100:
#        break


# ---------------------------------------------
# Analysis
# ---------------------------------------------

def get_ci(dat):
    '''
    Calculate standard error and 95% confidence intervals
        for proportion
    '''
    n = dat.shape[0]
    prop = sum(dat.is_refugee_tweet) / n
    se = np.sqrt(prop * (1 - prop) / n)
    return {'low': prop - 1.96*se, 
            'high': prop + 1.96*se,
            'prop': prop, 
            'se': se}


# Tweet level analysis
#--------------------------------------------------------------------------------


## Overall proportion refugee related
print(get_ci(df))

## Overall proportion for german language tweets
df_german = df[df['lang'] == 'de']
print(get_ci(df_german))

## Plot daily proportions
times = pd.DatetimeIndex(df['created_at'])
def prop_refugee_related(group):
    n = group.shape[0]
    prop = sum(group.is_refugee_tweet) / n
    se = np.sqrt(prop * (1 - prop) / n)
    low = prop - 1.96 * se
    high = prop + 1.96 * se
    return pd.Series({'prop': prop, 'low': low,
                      'high': high, 'n': n})
grouped = df.groupby(times.dayofyear)

props = grouped.apply(prop_refugee_related)

props['day'] = props.index

## Plots proportions
p = ggplot(aes(x='day', y='prop'), data=props) +\
        geom_line() +\
        geom_ribbon(aes(ymin='low',ymax='high'), 
                    alpha=0.3) +\
        ylab('Proportion refugee related') +\
        xlab('Day of the year (2015)') +\
        theme_bw()
p.save('/home/flinder/Dropbox/current_projects/dissertation/proposal/figures/prop_refugee_related.png')

## Plots daily sample size
p = ggplot(aes(x='day', y='n'), data=props) +\
        geom_line() +\
        ylab('Sample size (Tweets)') +\
        xlab('Day of the year (2015)') +\
        theme_bw()
p.save('/home/flinder/Dropbox/current_projects/dissertation/proposal/figures/daily_sample_size.png')


# User level analysis
#--------------------------------------------------------------------------------------------

def user_stats(user_df):
    n_tweets = user_df.shape[0]
    n_refugee_related = user_df['is_refugee_tweet'].sum()
    prop_refugee_related = n_refugee_related / n_tweets
    return pd.Series({'n_tweets': n_tweets,
                      'n_refugee_related': n_refugee_related,
                      'prop_refugee_related': prop_refugee_related})
users = df.groupby('user_id').apply(user_stats)
users.head()

n_users = users.shape[0]

def cumu_prop():
    for n in np.arange(1,users.n_refugee_related.max()):
        m = users[users.n_refugee_related >= n].shape[0]
        prop = m / n_users
        se = np.sqrt(prop * (1 - prop) / n_users)
        low = prop - 1.96*se
        high = prop + 1.96*se
        yield {'m': m, 'low': low, 'prop': prop, 'high': high, 'n': n}

cumu = pd.DataFrame(cumu_prop())

cumu[cumu.n == 100]

p = ggplot(aes(x='n', y='prop'), data=cumu) +\
        geom_line() +\
        geom_ribbon(aes(ymin='low',ymax='high'), 
                        alpha=0.3) +\
        scale_x_log() +\
        ylab('P(n > X)') +\
        xlab('X') +\
        theme_bw()
p.save('/home/flinder/Dropbox/current_projects/dissertation/proposal/figures/prop_users_n_tweets.png')

# Plot the probability of having a proportion of refugee related tweets
def cumu_prop():
    for n in np.arange(0.0001,1,0.01):
        m = users[users.prop_refugee_related >= n].shape[0]
        prop = m / n_users
        se = np.sqrt(prop * (1 - prop) / n_users)
        low = prop - 1.96*se
        high = prop + 1.96*se
        yield {'m': m, 'low': low, 'prop': prop, 'high': high, 'n': n}

cumu_p = pd.DataFrame(cumu_prop())

cumu_p[cumu_p.n > 0.1].head()

p = ggplot(aes(x='n', y='prop'), data=cumu_p) +\
        geom_line() +\
        geom_ribbon(aes(ymin='low',ymax='high'), 
                        alpha=0.3) +\
        ylab('P(n > X)') +\
        xlab('X') +\
        theme_bw()
p.save('/home/flinder/Dropbox/current_projects/dissertation/proposal/figures/p_users_prop_tweets.png')
