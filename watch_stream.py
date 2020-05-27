import bson
import json
import os

from pymongo import MongoClient
import pymongo
import twitter

TWITTER_API_KEY = os.environ["TWITTER_API_KEY"]
TWITTER_SECRET_KEY = os.environ["TWITTER_SECRET_KEY"]
TWITTER_TOKEN = os.environ["TWITTER_TOKEN"]
TWITTER_TOKEN_SECRET = os.environ["TWITTER_TOKEN_SECRET"]

LANGUAGES = ["en"]

with open('tagwatch.txt','r') as tag_file:
    tags = [s.strip() for s in tag_file.readlines()]

print(tags)

api = twitter.Api(TWITTER_API_KEY, TWITTER_SECRET_KEY, TWITTER_TOKEN, TWITTER_TOKEN_SECRET)
client = MongoClient("mongodb://root:password@localhost:27017")

db = client['maggot-tracker']
tweet_collection = db.tweets
user_collection = db.users

def make_id_from_num(num):
    s = str(num)
    s = '0' * (24 - len(s)) + s
    return bson.ObjectId(s)

def breakdown_tweet(tweet):
    tweets = []
    users = []
    if 'retweeted_status' in tweet:
        result = breakdown_tweet(tweet['retweeted_status'])
        tweets += result['tweets']
        users += result['users']        
        #tweet['retweeted_status'] = result['tweets'][0]['id']
        tweet["retweeted_status"] = tweet['retweeted_status']["id"]
    if 'quoted_status' in tweet:
        result = breakdown_tweet(tweet['quoted_status'])
        tweets += result['tweets']
        users += result['users']
        tweet["quoted_status"] = tweet['quoted_status']["id"]
        
    
    user = tweet['user']    
    user["_id"] = make_id_from_num(user["id"])
    tweet['user'] = user['id']
    
    tweet["_id"] = make_id_from_num(tweet['id'])
    users.append(user)
    tweets.append(tweet)
    return {'tweets': tweets, 'users': users}

def main():
    for line in api.GetStreamFilter(track=tags, languages=LANGUAGES):
        result = breakdown_tweet(line)
        try:
            user_collection.insert_many(result['users'], ordered=False)
        except pymongo.errors.BulkWriteError as e:
            panic = list(filter(lambda x: x['code'] != 11000, e.details['writeErrors']))
            if len(panic) > 0:
                raise
        try:
            tweet_collection.insert_many(result['tweets'], ordered=False)
        except pymongo.errors.BulkWriteError as e:
            panic = list(filter(lambda x: x['code'] != 11000, e.details['writeErrors']))
            if len(panic) > 0:
                raise
        #collection.insert_one(line)
        #print(tweeter)
        #break

if __name__ == "__main__":
    main()