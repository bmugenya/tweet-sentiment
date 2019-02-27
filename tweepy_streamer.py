from tweepy import API
from tweepy import Cursor
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream

import twitter_credentials
import numpy as np
import pandas as pd


class TwitterClient():

    def __init__(self, twitter_user=None):
        self.auth = TwitterAuthenticator().authenticate_twitter_app()
        self.twitter_client = API(self.auth)

        self.twitter_user = twitter_user

    def get_twitter_client_api(self):
        return self.twitter_client

    def get_user_timeline_tweets(self, num_tweets):
        tweets = []
        for tweet in Cursor(self.twitter_client.user_timeline, id=self.twitter_user).items(num_tweets):
            tweets.append(tweet)
        return tweets

    def get_home_timeline_tweets(self, num_tweets):
        home_timeline_tweets = []
        for tweet in Cursor(self.twitter_client.home_timeline, id=self.twitter_user).items(num_tweets):
            home_timeline_tweets.append(tweet)
        return home_timeline_tweets


class TwitterAuthenticator():

    def authenticate_twitter_app(self):
        auth = OAuthHandler(twitter_credentials.CONSUMER_KEY, twitter_credentials.CONSUMER_SECRET)
        auth.set_access_token(twitter_credentials.ACCESS_TOKEN, twitter_credentials.ACCESS_TOKEN_SECRET)
        return auth


class TwitterStreamer():
    """
    A class for streaming and processing live tweets
    """

    def __init__(self):
        self.twitter_authenticator = TwitterAuthenticator()

    def stream_tweets(self, fetched_tweets_filename, hash_tag):
        # This handles Twitter authentication and the connection to the Twitter Streamin API.
        listener = TwitterListerner(fetched_tweets_filename)
        auth = self.twitter_authenticator.authenticate_twitter_app()

        stream = Stream(auth, listener)

        stream.filter(track=hash_tag)


class TwitterListerner(StreamListener):
    """
    This is a basic listener class that just prints recieved tweets to stdout
    """

    def __init__(self, fetched_tweets_filename):
        self.fetched_tweets_filename = fetched_tweets_filename

    def on_data(self, data):
        try:
            print(data)
            with open(self.fetched_tweets_filename, 'a') as tf:
                tf.write(data)
            return True
        except BaseException as e:
            print("Error on_data: %s " % str(e))
        return True

    def on_error(self, status):
        if status == 420:
            # returning fasle on_data method in case rate limit occurs
            return False
        print(status)


class TweetAnalzer():
    """
    Functionality for analyzing and categorizing content from tweets.
    """

    def tweets_to_data_frame(self, tweets):
        df = pd.DataFrame(data=[tweet.text for tweet in tweets], columns=['tweets'])
        df['id'] = np.array([tweet.id for tweet in tweets])
        df['len'] = np.array([len(tweet.text) for tweet in tweets])
        df['date'] = np.array([tweet.created_at for tweet in tweets])
        df['source'] = np.array([tweet.source for tweet in tweets])
        df['likes'] = np.array([tweet.favorite_count for tweet in tweets])
        df['retweets'] = np.array([tweet.retweet_count for tweet in tweets])
        return df


if __name__ == "__main__":
    twitter_client = TwitterClient()
    tweet_analayzer = TweetAnalzer()

    api = twitter_client.get_twitter_client_api()

    tweets = api.user_timeline(screen_name='100xcode', count=40)
    df = tweet_analayzer.tweets_to_data_frame(tweets)

    # print(dir(tweets[0]))
    # print(df.head(100))

    # Average length over all tweets
    print(np.mean(df['len']))

    # Number of likes for the most liked twee
    print(np.max(df['likes']))

    # Number of retweets for the most retweeted tweet
    print(np.max(df['retweets']))

    # Time series
    time_likes = pd.Series(data=df['likes'].values, index=df['date'])
