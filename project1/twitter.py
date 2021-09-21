'''
@author: Souvik Das
Institute: University at Buffalo
'''

import tweepy


class Twitter:
    def __init__(self):
        self.auth = tweepy.OAuthHandler("FxRHehioJ4sbBbTwdIEPXHKZY",
                                        "qQZoJi70gvwgvNyFB3US1YiKcA7D66rBjAdB2TEV7NWjUE2CkR")
        self.auth.set_access_token("1432842215294967814-Nm1cS4f7qxrDs6XY3EYpx1OYnnQ2ks",
                                   "lmIkbR5TmX9oKNhBwd9uXDgCdFzB15UzblChUN7XZRFtb")
        self.api = tweepy.API(self.auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)

    def _meet_basic_tweet_requirements(self):
        '''
        Add basic tweet requirements logic, like language, country, covid type etc.
        :return: boolean
        '''
        raise NotImplementedError

    def get_tweets_by_poi_screen_name(self, screen_name, count):
        '''
        Use user_timeline api to fetch POI related tweets, some postprocessing may be required.
        :return: List
        '''
        #tweets = self.api.user_timeline(screen_name = screen_name, count = count, include_rts=True)
        tweets = []

        for status in tweepy.Cursor(self.api.user_timeline, screen_name= screen_name, tweet_mode="extended").items(count):
            tweets.append(status)

        # public_tweets = self.api.search(q = 'covid', count=10, lang = "english")

        print("Total tweet scraped ====    ",len(tweets))
        #for tweet in tweets:
        #   print(tweet.id)

        return tweets;

        raise NotImplementedError

    def get_tweets_by_lang_and_keyword(self, name, count, lang, country):
        '''
        Use search api to fetch keywords and language related tweets, use tweepy Cursor.
        :return: List
        '''

        #tweets = api.search(q='covid', count=10, lang="en")
        tweets = []

        for status in tweepy.Cursor(self.api.search, q=name, lang = lang, tweet_mode="extended").items(count):
            tweets.append(status)

        print("Total tweet scraped ====    ", len(tweets),  name)

        return tweets
        raise NotImplementedError

    def get_replies(self):
        '''
        Get replies for a particular tweet_id, use max_id and since_id.
        For more info: https://developer.twitter.com/en/docs/twitter-api/v1/tweets/timelines/guides/working-with-timelines
        :return: List
        '''
        raise NotImplementedError
