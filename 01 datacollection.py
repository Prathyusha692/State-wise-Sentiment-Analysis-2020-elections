# Databricks notebook source
import sys
import json
import pymongo #a native Python driver for MongoDB allows interaction with the MongoDB database through Python.
import tweepy #library for accessing the Twitter API
from datetime import datetime
from tweepy.streaming import StreamListener 
from tweepy import OAuthHandler #class for authentication we stored in file associated with twitter app
from tweepy import Stream
from pymongo import MongoClient
import yaml
#pip install pymongo[srv], pip install pymongo, pip install tweepy

#Twitter Developer Account Authorization (to access twitter_creds.yml file saved in same directory)
with open("twitter_creds.yml", 'r') as ymlfile:
    creds = yaml.safe_load(ymlfile)
CONSUMER_KEY = creds['CONSUMER_KEY']
CONSUMER_SECRET = creds['CONSUMER_SECRET']
ACCESS_TOKEN_KEY = creds['ACCESS_TOKEN']
ACCESS_TOKEN_SECRET = creds['ACCESS_TOKEN_SECRET']

# complete authorization and initialize API endpoint
auth = OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
auth.set_access_token(ACCESS_TOKEN_KEY, ACCESS_TOKEN_SECRET)

# insert your mongoDB access key
client = MongoClient('mongodb+srv://admin:admin@prathyushawcu.jkrrw.mongodb.net/test_db?retryWrites=true&w=majority') #Mongoclient connecting url
db = client.tweets1012

try:
    db.command("serverStatus")
except Exception as e:
    print(e)
else:
    print("You are connected! to MongoDB Server!!")

# COMMAND ----------


# StreamListener class inherits from tweepy.StreamListener and overrides methods.
class myStreamListener(StreamListener):
    def on_connect(self):
        """Called when the connection is made"""
        
        print("Connected to the Twitter Streaming API....")
        
    def on_data(self, data):
        full_tweet = json.loads(data)
 
        if 'extended_tweet' in full_tweet:
            tweet_text = full_tweet.get('full_text') #API automatically truncates the text attribute of tweets longer than 140 characters. If we want to access the full text,we need to access the                                                       “extended_tweet” attribute of the tweet object
        else:
            tweet_text = full_tweet.get('text')
        
        tweet_id = full_tweet.get('id')
        tweet_time = full_tweet.get('created_at')
        tweet_lang = full_tweet.get('lang')
        place = full_tweet.get('place')
             
        if tweet_lang != None and tweet_lang == 'en' and tweet_text != None and 'RT @' not in tweet_text:
            if place != None:
                tweetObject = {
                "id": tweet_id,
                "text": tweet_text,
                "time": tweet_time,
                "city_state": full_tweet['place']['full_name'],
                "country_code": full_tweet['place']['country_code'],
                "coordinates" : full_tweet['coordinates']
                    
                }
#                 print(tweetObject)
                db.geo_true.insert_one(tweetObject)
            else:
                tweetObject = {
                "id": tweet_id,  
                "text": tweet_text,
                "time": tweet_time,
                "lang": tweet_lang,
                "location" : full_tweet['user']['location']
                }
#                 print(tweetObject)
                db.geo_false.insert_one(tweetObject)        
         
     
    def on_error(self, status_code):
        if status_code == 420:          # Returning False on_data method in case rate limit occurs
            return False
        else:                          #continue listening if other errors occur
            print ('An Error has occurred: ' + repr(status_code))
            return True     

track = ['#Donald Trump', 'donald trump', '#Biden', 'biden','realDonaldTrump','JoeBiden',
         '#VicePresidentialDebate', 'KamalaHarris','SenKamalaHarris','sen kamala harris','Vice President Mike Pence','Mike Pence'] #list of keywords

while True:
    try:
        stream_listener = myStreamListener(api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True))
        stream = tweepy.Stream(auth=auth, listener=stream_listener)
        stream.filter(track=track)
    except KeyboardInterrupt:
#         s.close()
        stream.disconnect()
        break

# COMMAND ----------


