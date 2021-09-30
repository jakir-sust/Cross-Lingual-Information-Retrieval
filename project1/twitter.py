
import tweepy
import datetime
import pickle
from langdetect import detect

covid_keyword = ["testing", "wearmask", "death", "second wave", "covid-19", "cases", "staty home","work from home",
                         "hospitalización", "cases", "oxygencrisis", "self-isolation", "community spread", "infección",
                         "सुरक्षित रहें ", "covid19 ", "face mask ", "covidsecondwaveinindia ", "flattenthecurve ",
                         "casos ", "कोविड मृत्यु ", "स्वयं चुना एकांत ", "covid symptoms ", "brote ", "encierro ",
                         "flatten the curve ", "oxígeno ", "desinfectante ", "ventilador ", "coronawarriors ",
                         "quedate en casa ", "mascara facial ", "trabajar desde casa ", "संगरोध ", "immunity ",
                         "स्वयं संगरोध ", "डेल्टा संस्करण ", "health ", "cilindro de oxígeno ", "covid ",
                         "yomequedoencasa ", "doctor ", "एंटीबॉडी ", "दूसरी लहर ", "मुखौटा ", "covid test ", "covid deaths ", "कोविड19 ",
                         "संक्रमित", "ऑक्सीजन ", "कोरोनावायरस", "महामारी", "स्वास्थ्य सेवा", "लॉकडाउन", "máscara",
                         "cuarentena", "distanciamiento", "distanciasocial ", "cubrebocas", "muerte", "transmisión"]

vaccine_keyword = ["vaccinate", "vaccines", "moderna","covid vaccine","efficacy","dose", "side effects", "pfizer", "vaccine efficacy", "fully vaccinate",
                   "vaccineshortage ","covishield ","booster shot ","sinopharm ","immunity ","injection ","herd immunity ",
                   "ivermectin ","getvaccinatednow ","first dose ","booster shots ","firstdose ","fully vaccinated ","covidshield ",
                   "fullyvaccinated ","anticuerpos ","dosis de vacuna ","campaña de vacunación ","efectos secundarios de la vacuna ",
                   "inyección de refuerzo ","vacunacovid19 ","inmunización ","vaccine dose ","vacunado ","efecto secundario ","astra zeneca ",
                   "yomevacunoseguro ","cdc ","estrategiadevacunación ","seconddose ","vacunación ","efectos secundarios ","eficacia ","anticuerpo ",
                   "pasaporte de vacuna ","la inmunidad de grupo ","second dose ","vacuna para el covid-19 ","completamente vacunado ","कोविशील्ड ","टीके ",
                   "वैक्सीनेशन ","वैक्सीन पासपोर्ट ","दूसरी खुराक ","टीकाकरण अभियान ","एंटीबॉडी ","वैक्सीन के साइड इफेक्ट ","वैक्सीन जनादेश ","कोवेक्सिन ",
                   "वाइरस ","कोविड का टीका ","खराब असर ","कोवैक्सिन ","फाइजर ","कोवैक्सीन ","वैक्सीन ","प्रभाव ","लसीकरण ","वैक्‍सीन ","दुष्प्रभाव ","टीका लगवाएं ","एमआरएनए वैक्सीन ",
                   "एस्ट्राजेनेका ","कोविड टीका"]

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

    def get_tweets_by_poi_screen_name(self, screen_name, count, max_id = None):
        '''
        Use user_timeline api to fetch POI related tweets, some postprocessing may be required.
        :return: List
        '''
        #tweets = self.api.user_timeline(screen_name = screen_name, count = count, include_rts=True)
        tweets = []
        covid_word = ["testing", "wearmask", "death", "second wave", "covid-19", "cases", "staty home", "work from home",
                      "hospitalización", "cases", "oxygencrisis", "self-isolation", "community spread", "infección", "testing",
                      "distanciasocial", "संक्रमित", "ऑक्सीजन ","कोरोनावायरस","महामारी", "स्वास्थ्य सेवा", "लॉकडाउन",
                      "máscara", "cuarentena", "distanciamiento", "distanciasocial ","cubrebocas", "muerte", "transmisión"]
        with open('crowdsourced_keywords.pickle', 'rb') as handle:
            b = pickle.load(handle)
            for cov in b['covid']:
                lan = detect(cov)
                #print(cov, lan)
                if lan == 'hi' or lan == 'mr' or lan == 'en' or lan == 'es':
                    covid_word.append(cov)
                    print("\"" + cov, "\",", end="")
                    if len(covid_word)>= 50:
                        break
                    #print(cov)
        #print(len(covid_word))
        #return
        for status in tweepy.Cursor(self.api.user_timeline, screen_name= screen_name, tweet_mode="extended").items(1000):
            tweets.append(status)
            continue
            for cov in covid_word:
               if status.full_text.find(cov) != -1:
                   tweets.append(status)
                   print(cov, status.created_at)
                   break


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
        start_date = datetime.datetime(2020, 6, 19, 12, 00, 00)
        end_date = datetime.datetime(2021, 12, 19, 13, 00, 00)

        for status in tweepy.Cursor(self.api.search, q=name, lang=lang, tweet_mode="extended").items(count):
            tweets.append(status)
            #tweets.append(status)

        print("Total tweet scraped ====    ", len(tweets),  name)

        return tweets
        raise NotImplementedError


    def non_POI(self, name, tweet_id, count, poi_dict):
        return_tweets = []
        cnt = 0
        for full_tweets in tweepy.Cursor(self.api.user_timeline, screen_name=name, max_id = 1250699202700677120, timeout=999999,
                                         tweet_mode="extended").items(1000):

            found = 0
            for keyword in vaccine_keyword:
                if full_tweets.full_text.find(keyword) != -1:
                    found = 1
                    break

            if found ==1:
                return_tweets.append(full_tweets)

        print("Total tweet scraped ====    ", len(return_tweets))
        return return_tweets


    def new_replies_implementation(self, name, tweet_id, count, poi_dict):
        return_tweets = []
        #name = "DonCheadle"
        # non_bmp_map = dict.fromkeys(range(0x10000, sys.maxunicode + 1), 0xfffd)
        cnt = 0
        print("-----------------------------------------     >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>    ", name)
        for full_tweets in tweepy.Cursor(self.api.user_timeline, screen_name=name, max_id = 1250699202700677120, timeout=999999,
                                         tweet_mode="extended").items(1):
            found = 0

            print("INside   ")

            #for word in covid_keyword:
            #    if full_tweets.full_text.find(word) != -1:
                     #found = 1
                    #print(word, "  -----------  ", full_tweets.full_text)
               #     break
            #if found == 0:
            #    continue

            replies = []
            for tweet in tweepy.Cursor(self.api.search, q='to:' + name, since=full_tweets.id,
                                       tweet_mode="extended", timeout=999999).items(300):
                #replies.append(tweet)
                #continue
                if hasattr(tweet, 'in_reply_to_status_id_str') and tweet.in_reply_to_status_id != None:
                    if tweet.in_reply_to_status_id_str != full_tweets.id_str:
                        #print("Id  ====   ", tweet.in_reply_to_status_id)
                        reply_parent = self.api.get_status(tweet.in_reply_to_status_id, tweet_mode="extended")
                        replies.append(reply_parent)
                        replies.append(tweet)
                        #replies.append(tweet)
                        # if len(replies)> 11:
                        #   break
            # print("Tweet :",full_tweets.text.translate(non_bmp_map))
            #print(len(replies))
            if len(replies)>= 1:
                #return_tweets.append(full_tweets)
                for reply in replies:
                   return_tweets.append(reply)
            #for elements in replies:
             #   print("Replies :", elements)

        print("Total tweet scraped ====    ", len(return_tweets))
        return return_tweets

    def get_replies(self, name, tweet_id, count, poi_dict):
        '''
        Get replies for a particular tweet_id, use max_id and since_id.
        For more info: https://developer.twitter.com/en/docs/twitter-api/v1/tweets/timelines/guides/working-with-timelines
        :return: List
        '''
        tweets = self.new_replies_implementation(name, tweet_id, count, poi_dict)
        #tweets = self.non_POI(name, tweet_id, count, poi_dict)
        return tweets

        tweets = []
        #tweet_id =
        reply_dict = {}
        reply_tweets = []
        mx = 0
        for status in tweepy.Cursor(self.api.search, q='to:{}'.format(name), since = tweet_id, result_type='recent',
                                    timeout=999999, tweet_mode="extended").items(count):

            if not hasattr(status, 'in_reply_to_status_id') or status.in_reply_to_status_id == None:
                continue
            reply_tweets.append(status)
           # print(status.id, status.in_reply_to_status_id, tweet_id)

            #print(status.text)

            #if status.in_reply_to_status_id in poi_dict:
            #print("OKKKK")
            if status.in_reply_to_status_id not in reply_dict:
                reply_dict[status.in_reply_to_status_id] = 1
            else:
                reply_dict[status.in_reply_to_status_id] = reply_dict[status.in_reply_to_status_id] + 1
                mx = max(mx, reply_dict[status.in_reply_to_status_id])

            #if reply_dict[status.in_reply_to_status_id] >= 10:

        #return reply_tweets

        for reply in reply_tweets:
            if reply_dict[reply.in_reply_to_status_id] >= 1:
                tweets.append(reply)

        cnt = 0
        for reply in reply_tweets:
            if reply_dict[reply.in_reply_to_status_id] >= 10:
                #print("OKKKKKK    ",reply.in_reply_to_status_id)
                tweet = self.api.get_status(reply.in_reply_to_status_id, tweet_mode="extended")
                #print(tweet)
                tweets.append(tweet)
                cnt += 1
                reply_dict[reply.in_reply_to_status_id] = -1

        print("Total tweet scraped ====    ", len(tweets), cnt)
        return tweets
        raise NotImplementedError
