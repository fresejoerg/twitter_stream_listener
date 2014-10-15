# -*- coding: utf-8 -*-
"""
Created on Fri May 02 09:01:10 2014

@author: jfrese
"""

import tweepy
import sys
import pymongo
import pandas as pd

consumer_key="..."
consumer_secret="..."

access_token="..."
access_token_secret="..."

auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
api = tweepy.API(auth)

class CustomStreamListener(tweepy.StreamListener):
    def __init__(self, api):
        self.api = api
        super(tweepy.StreamListener, self).__init__()

        self.db = pymongo.MongoClient().emopos_es_sad

    def on_status(self, status):
        print status.text , "\n"

        data ={}
        data['text'] = status.text
        data['created_at'] = status.created_at
        data['geo'] = status.geo
        data['source'] = status.source
        data['language'] = status.lang
        data['user_language'] = status.user.lang
        data['author'] = status.user.screen_name

        self.db.Tweets.insert(data)

    def on_error(self, status_code):
        print >> sys.stderr, 'Encountered error with status code:', status_code
        return True # Don't kill the stream

    def on_timeout(self):
        print >> sys.stderr, 'Timeout...'
        return True # Don't kill the stream

sapi = tweepy.streaming.Stream(auth, CustomStreamListener(api))
#sapi.filter(track=[':-)',':)',':->',':D',':-D','=)',';)','^_^','^-^',':(',':-(',':((','-.-','>-:(','D:',':/'], languages=["de"])

sapi.filter(track=[':(',':-(',':((','-.-','>-:(','D:',':/'], languages=["es"])

def mongo_to_df(db,collection,query={}):
    """
    example:
    mongo_to_df('emopos','Tweets')
    """
    client=pymongo.MongoClient()
    db=client[db]
    coll=db[collection]
    cursor=coll.find(query)
    df=pd.DataFrame(list(cursor))
    return df
    
df = mongo_to_df('emopos_es_sad','Tweets')    

df['text_uni'] = df.apply(lambda row: unicode(row['text'].replace("\n","")), axis=1)
df['text_uni'].to_csv("X:\Joerg\\twitter_multilanguage_sentiment\emopos_es_sad.csv", sep=",", encoding='utf-8', index=False)


happy = [':-)',':)',':->',':D',':-D','=)',';)','^_^','^-^']
l=[]
import re
for i in happy:
    x=re.escape(i)
    l.append(x)
    
pattern='|'.join(l)

df['happy']=0
df['happy'][df['text_ascii'].str.contains(pattern)]=1

from sklearn.cross_validation import train_test_split
from sklearn.naive_bayes import GaussianNB
from sklearn.feature_extraction.text import TfidfVectorizer

X, y = df['text_ascii'], df['happy']
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.25, random_state=33)

vectorizer = TfidfVectorizer(sublinear_tf=True, max_df=0.5, stop_words='english')
X_train_v = vectorizer.fit_transform(X_train).toarray()
X_test_v = vectorizer.fit_transform(X_test)
gnb = GaussianNB()
gnb.fit(X_train_v,y_train)

