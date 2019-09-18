
import tweepy
from tweepy import OAuthHandler
from tweepy import Stream
from tweepy.streaming import StreamListener
import socket
import json
from Twitter_credentials import *
from Oracle_DB_Connection import dbcon

class listener(StreamListener):
     
    def on_data(self,data):
     '''
     This Function Returns Live Tweets and stores in Oracle Database.
     '''
            db = dbcon()
            tweets = json.loads(data)
            all_tweets = tweets["text"]
            username = tweets["user"]["screen_name"]
            all_tweets = all_tweets.encode('utf-8')
            username = username.encode('utf-8')
            values = (username,all_tweets)    
            db.ex.execute("""insert into TWEET_FEED_3 (user_name,Tweet ) VALUES (:1,:2 )""",values)
            db.conn.commit()
            return True    
        
    def on_error(self,status):
            print(status)

def sendData():
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_secret)
    
    twitterStream = Stream(auth, listener())
    twitterStream.filter(track=["FILTER_WORDS"])

if __name__ == "__main__":
    
    sendData()
