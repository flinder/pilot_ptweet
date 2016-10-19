## Pilot study: Refugee related tweets in Germany


### ETL

All etl scripts are in `/etl/`



### Database Schema

#### tweets

```
db_name             type            twitter_field_name
tweet_id            string          id
is_quote_status     bool            is_quote_status
reply_to_status     string          in_reply_to_status_id
reply_to_user       string          in_reply_to_user_id
quoted_status       string          quoted_status_id_str 
favorite_count      integer         favorite_count
source              string          source
coords_lon          float           coordinates['coordinates'][0]
coords_lat          float           coordinates['coordinates'][1]
language            string          lang 
created_at          datetime        created_at
retweet_count       integer         retweet_count
favorite_count      integer         favorite_count
text                string          text
hashtags            string          entities['hashtags']
user_mentions       string          entities['user_mentions']
user_id             string          user['id_str']
data_group               string

```


#### users

```
db_name         type        twitter_user_field_name
contributors    bool        contributors_enabled
created_at      datetime    created_at 
description     string      description
followers_count integer     followers_count
friends_count   integer     friends_count
geo_enabled     boolean     geo_enabled
id              string      id_str
lang            string      lang
listed_count    integer     listed_count
location        string      location
name            string      name
screen_name     string      screen_name
statuses_count  integer     statuses_count
time_zone       string      time_zone
verified        boolean     verified
data_group
```

### hashtags

```
name    string
```

### hashtag_mentions

```
tweet_id    string
hashtag     string
```

### user_mentions
```
tweet_id    string
user_id     string
```

### data_group names
```
de_panel    Pablo Barbera's panel of 25k users
pb_keyword  Pablo Barbera's keyword stream 
```
