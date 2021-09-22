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

def reset_keyword(pois, keywords):
    for i in range(len(keywords)):
        if keywords[i]["lang"] == 'es':
            keywords[i]["finished"] = 0
            keywords[i]["collected"] = 0
        write_config({
            "pois": pois, "keywords": keywords
        })

def read_keyword(pois, keywords):
    with open('crowdsourced_keywords.pickle', 'rb') as handle:
        b = pickle.load(handle)
        print(len(b['covid']), len(b['vaccine']))

        my_keyword = {}
        my_keyword['en'], my_keyword['es'], my_keyword['hi'] = [], [], []
        for cov in b['covid']:
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

        cnt = 0
        for key in my_keyword.keys():
            print(key, my_keyword[key])
            country = 'USA'
            if key == 'hi':
                country = 'India'
            elif key == 'es':
                country = 'Mexico'

            for word in my_keyword[key]:
                key_field = {}
                key_field['id'] = cnt
                key_field['name'] = word
                key_field['count'] = 100
                key_field['lang'] = key
                key_field['country'] = country
                key_field['finished'] = 0
                keywords.append(key_field)
                cnt += 1
        for i in range(cnt):
            print(i, keywords[i])

        write_config({
            "pois": pois, "keywords": keywords
        })

def main():
    config = read_config()
    indexer = Indexer()
    twitter = Twitter()

    pois = config["pois"]
    keywords = config["keywords"]

    #read_keyword(pois, keywords)
    #reset_keyword(pois, keywords)
    indexer.solr_search()
    return

    for i in range(len(pois)):
        break
        if pois[i]["finished"] == 0:
            print(f"---------- collecting tweets for poi: {pois[i]['screen_name']}")

            raw_tweets = twitter.get_tweets_by_poi_screen_name(pois[i]['screen_name'], pois[i]['count'])  # pass args as needed



            processed_tweets = []
            for tw in raw_tweets:
                processed_tweets.append(TWPreprocessor.preprocess(tw, 'poi'))


            #print(processed_tweets[0], raw_tweets[0].created_at)
            #return

            indexer.create_documents(processed_tweets)

            pois[i]["finished"] = 1
            pois[i]["collected"] = len(processed_tweets)

            write_config({
                "pois": pois, "keywords": keywords
            })

            save_file(processed_tweets, f"poi_{pois[i]['id']}.pkl")
            print("------------ process complete -----------------------------------")
            #print("OK")




    for i in range(len(keywords)):
        if keywords[i]["finished"] == 0:
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
