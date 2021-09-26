'''
@author: Souvik Das
Institute: University at Buffalo
'''

import json
import datetime
import pandas as pd
from twitter import Twitter
from tweet_preprocessor import TWPreprocessor
from indexer import Indexer
import pickle
import pysolr
from textblob import TextBlob
from langdetect import detect
from solr_search import SolrSearch
import solr_search


reply_collection_knob = False


def read_config():
    with open("config.json") as json_file:
        data = json.load(json_file)

    return data


def write_config(data):
    with open("config.json", 'w') as json_file:
        json.dump(data, json_file)


def save_file(data, filename):
    df = pd.DataFrame(data)
    df.to_pickle("data/" + filename)


def read_file(type, id):
    return pd.read_pickle(f"data/keywords/{type}_{id}.pkl")

def reset_pois(pois, keywords):
    for i in range(len(pois)):
        pois[i]["finished"] = 0
        pois[i]["collected"] = 0
        pois[i]["count"] = 500
        write_config({
            "pois": pois, "keywords": keywords
        })
def reset_keyword(pois, keywords):
    for i in range(len(keywords)):
        if keywords[i]["lang"] == 'hi':
            keywords[i]["finished"] = 0
            keywords[i]["collected"] = 0
            keywords[i]["count"] = 500
        write_config({
            "pois": pois, "keywords": keywords
        })

def read_keyword(pois, keywords):
    with open('crowdsourced_keywords.pickle', 'rb') as handle:
        b = pickle.load(handle)
        print(len(b['covid']), len(b['vaccine']))

        my_keyword = {}
        my_keyword['en'], my_keyword['es'], my_keyword['hi'] = [], [], []
        for cov in b['vaccine']:
            lan = detect(cov)
            #print(cov, lan)
            if lan == 'hi' or lan == 'mr':
                my_keyword['hi'].append(cov)
            elif lan == 'es':
                my_keyword['es'].append(cov)
            elif lan == 'en':
                my_keyword['en'].append(cov)
            else:
                continue
                print(cov, lan)

        print(len(my_keyword['en']), len(my_keyword['es']), len(my_keyword['hi'] ))

        cnt = 200
        for key in my_keyword.keys():
            #print(key, my_keyword[key])
            country = 'USA'
            if key == 'hi':
                country = 'India'
            elif key == 'es':
                country = 'Mexico'

            for word in my_keyword[key]:
                print("\"" + word, "\",", end="")
                key_field = {}
                key_field['id'] = cnt
                key_field['name'] = word
                key_field['count'] = 500
                key_field['lang'] = key
                key_field['country'] = country
                key_field['finished'] = 0
                keywords.append(key_field)
                cnt += 1
        #for i in range(cnt):
        #    print(i, keywords[i])
        #return

       # write_config({
        #    "pois": pois, "keywords": keywords
        #})
def write_new_keyword_in_json():
    word_list = solr_search.open_crowd_keyword_file()

def get_10_health_reply(raw_tweets):
    tweets = []
    all_keywords = solr_search.get_all_keyword()
    #print(all_keywords)
    for tweet in raw_tweets:
        found = 0
        for word in all_keywords:
            if tweet.full_text.find(word) != -1:
                found = 1
                break

        if found==1:
            #print("OK")
            tweets.append(tweet)

    print('new tweets to be added    ',len(tweets))
    return tweets

def get_non_poi_tweets(pois, keywords, config, indexer, sol_search, twitter):
    #all_pois = solr_search.get_all_poi()
    #print(all_pois)

    person_name  = ['sachin_rt', 'harbhajan_singh', 'NeymarStats', 'BeingSalmanKhan', 'akshaykumar', 'SrBachchan', 'iamsrk',
     'KapilSharmaK9', 'SAfridiOfficial', 'iamAhmadshahzad', 'MHafeez22', 'realshoaibmalik', 'simadwasim',
                        'iamamirofficial', 'realrossnoble', ]
    #person_name = [ 'daraobriain', 'jimmycarr',
     #              'RobBrydon', 'stephenfry', 'wossy']

    person_name = ['Berci', 'kevinmd', 'DrOz', 'DrLeslieSaxon', 'ZDoggMD', 'drmikesevilla', 'drval', 'JuliekWoodMD', 'GrStream', 'DrJosephKim',
                    'CLflickMD', 'MrsBrull', 'kennylinafp', 'TroyTXfamilyDoc', 'BlackwelderMD', 'RichmondDoc', 'apjonas',
                    'symtym', 'consultdoc', 'healthewoman', 'jayparkinson',
                    'TheRealJackDee']

    person_name = ["AnthonyMackie", "ChrisEvans", "MarkRuffalo", "Marvel", "MarvelStudios"]

    for person in (person_name):

            print("---------- collecting tweets for poi: ", person)

            poi_dict = solr_search.get_poi_twitter_id_in_solr(sol_search.connection, person)

            print("total tweet by user ", len(poi_dict))
            min_id = None

            raw_tweets = twitter.get_replies(person, min_id, 100, poi_dict)  # pass args as needed

            #print(len(raw_tweets))
            #continue


            processed_tweets = []
            for tw in raw_tweets:
                processed_tweets.append(TWPreprocessor.preprocess(tw, 'non-poi'))

            indexer.create_documents(processed_tweets)

            #save_file(processed_tweets, f"poi_reply_{pois[i]['id']}.pkl")
            print("------------ process complete -----------------------------------")


def get_reply_from_poi(pois, keywords, config, indexer, sol_search, twitter):
    #all_pois = solr_search.get_all_poi()
    #print(all_pois)

    for i in range(len(pois)):
        min_id = None
        if pois[i]['id'] != 0:
            continue
        if pois[i]["finished"] == 0:
            min_id = solr_search.search_by_poi(sol_search.connection, pois[i]['screen_name'])
            print(f"---------- collecting tweets for poi: {pois[i]['screen_name']}")

            poi_dict = solr_search.get_poi_twitter_id_in_solr(sol_search.connection, pois[i]['screen_name'])

            print("total tweet by user ", len(poi_dict))

            raw_tweets = twitter.get_replies(pois[i]['screen_name'], min_id, pois[i]['count'], poi_dict)  # pass args as needed

            #print(len(raw_tweets))
            #continue


            processed_tweets = []
            for tw in raw_tweets:
                if tw.user.screen_name == pois[i]['screen_name']:
                    processed_tweets.append(TWPreprocessor.preprocess(tw, 'poi'))
                else:
                    processed_tweets.append(TWPreprocessor.preprocess(tw, 'non-poi'))

            indexer.create_documents(processed_tweets)

            pois[i]["finished"] = 1
            pois[i]["collected"] = len(processed_tweets)

            write_config({
                "pois": pois, "keywords": keywords
            })

            save_file(processed_tweets, f"poi_reply_{pois[i]['id']}.pkl")
            print("------------ process complete -----------------------------------")



def main():
    config = read_config()
    indexer = Indexer()
    twitter = Twitter()
    sol_search = SolrSearch()

    pois = config["pois"]
    keywords = config["keywords"]

    #read_keyword(pois, keywords)
    #reset_keyword(pois, keywords)
    #reset_pois(pois, keywords)
    #return
   # indexer.solr_search()
    #get_reply_from_poi(pois, keywords, config, indexer, sol_search, twitter)
    get_non_poi_tweets(pois, keywords, config, indexer, sol_search, twitter)
    return

    for i in range(len(pois)):
        min_id = None
        #break
        #if pois[i]['id'] <= 100:
        #    break
        if pois[i]["finished"] == 0:
            min_id = solr_search.search_by_poi(sol_search.connection, pois[i]['screen_name'])
            print(min_id)

            print(f"---------- collecting tweets for poi: {pois[i]['screen_name']}")

            #raw_tweets = twitter.get_tweets_by_poi_screen_name(pois[i]['screen_name'], pois[i]['count'], max_id = min_id)  # pass args as needed

            #raw_tweets = get_10_health_reply(raw_tweets)
            #raw_tweets = get_reply_from_poi(raw_tweets)

            #continue


            processed_tweets = []
            for tw in raw_tweets:
                processed_tweets.append(TWPreprocessor.preprocess(tw, 'poi'))


            #print(processed_tweets[0], raw_tweets[0].created_at)
            #return
            print("Preprocess done")

            indexer.create_documents(processed_tweets)

            pois[i]["finished"] = 1
            pois[i]["collected"] = len(processed_tweets)

            write_config({
                "pois": pois, "keywords": keywords
            })

            save_file(processed_tweets, f"poi_{pois[i]['id']}.pkl")
            print("------------ process complete -----------------------------------")
            #print("OK")



    return
    for i in range(len(keywords)):
        if keywords[i]["finished"] == 0 and keywords[i]["id"] > 190:
            print(f"---------- collecting tweets for keyword: {keywords[i]['name']}")

            raw_tweets = twitter.get_tweets_by_lang_and_keyword(keywords[i]['name'], keywords[i]['count'], keywords[i]['lang'], keywords[i]['country'])  # pass args as needed

            #return

            processed_tweets = []
            for tw in raw_tweets:
                processed_tweets.append(TWPreprocessor.preprocess(tw, 'keyword'))

            indexer.create_documents(processed_tweets)

            keywords[i]["finished"] = 1
            keywords[i]["collected"] = len(processed_tweets)

            write_config({
                "pois": pois, "keywords": keywords
            })

            save_file(processed_tweets, f"keywords_{keywords[i]['id']}.pkl")

            print("------------ process complete -----------------------------------")
            #return

    if reply_collection_knob:
        # Write a driver logic for reply collection, use the tweets from the data files for which the replies are to collected.

        raise NotImplementedError


if __name__ == "__main__":
    main()
