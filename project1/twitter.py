'''
@author: Souvik Das
Institute: University at Buffalo
'''

import tweepy


class Twitter:
    def __init__(self):
        self.auth = tweepy.OAuthHandler("PI4E6fSPT3N9IpIhL8zULRfAL",
                                        "HWr7ic4K0J6C0SwK4UowIyXlPAhlms9D8rF0kBGXXVRo1WrUm8")
        self.auth.set_access_token("1432842215294967814-ZOaowZCPpFIRoIPtE3jlX6U95aXFQf",
                                   "CXTZYbXMuSKweopmdGlW3dtPqyRnbVvZYT1ahAMZ4VhJ1")
        self.api = tweepy.API(self.auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)

    def _meet_basic_tweet_requirements(self):
        '''
        Add basic tweet requirements logic, like language, country, covid type etc.
        :return: boolean
        '''
        raise NotImplementedError

    def get_tweets_by_poi_screen_name(self):
        '''
        Use user_timeline api to fetch POI related tweets, some postprocessing may be required.
        :return: List
        '''
        tweets = self.api.user_timeline(screen_name="ABdeVilliers17", count=10, include_rts=True)

        # public_tweets = self.api.search(q = 'covid', count=10, lang = "english")

        print(len(tweets))
        # for tweet in tweets:
        #   print(tweet.text)

        return 'OK';

        raise NotImplementedError

    def get_tweets_by_lang_and_keyword(self):
        '''
        Use search api to fetch keywords and language related tweets, use tweepy Cursor.
        :return: List
        '''
        raise NotImplementedError

    def get_replies(self):
        '''
        Get replies for a particular tweet_id, use max_id and since_id.
        For more info: https://developer.twitter.com/en/docs/twitter-api/v1/tweets/timelines/guides/working-with-timelines
        :return: List
        '''
        raise NotImplementedError
