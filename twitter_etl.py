import tweepy
import pandas as pd
import json
from datetime import datetime
import s3fs


def run_twitter_etl():
    access_key = "UX6GUQHGx2OMzPcg7Lf5H9xEa"
    access_secret = "CYKHfLc7p0qD8oKmyE2S8aFF3lEHegL1EUmCh6EB9Xw6f07V9b"
    consumer_key = "1594298248294010883-v7mGfrDHu7kf1PRvDOMv82djrhjHbK"
    consumer_secret = "q1hDim2K3Bd308kM4YzwN6z2Na2gwrQUqAkaVGp3sU1ad"

    # Twitter authentication
    auth = tweepy.OAuthHandler(access_key, access_secret)
    auth.set_access_token(consumer_key, consumer_secret)

    # # # Creating an API object
    api = tweepy.API(auth)
    tweets = api.user_timeline(screen_name='@elonmusk',
                               # 200 is the maximum allowed count
                               count=200,
                               include_rts=False,
                               # Necessary to keep full_text
                               # otherwise only the first 140 words are extracted
                               tweet_mode='extended'
                               )

    list = []
    for tweet in tweets:
        text = tweet._json["full_text"]

        refined_tweet = {"user": tweet.user.screen_name,
                         'text': text,
                         'favorite_count': tweet.favorite_count,
                         'retweet_count': tweet.retweet_count,
                         'created_at': tweet.created_at}

        list.append(refined_tweet)

    df = pd.DataFrame(list)
    df.to_csv('refined_tweets.csv')