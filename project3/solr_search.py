from replace_bm25 import Indexer
import json
import emoji
import pickle
import pysolr

import simplejson
import pprint

import json_to_trec

import urllib.request as urllib2
from urllib.parse import quote

import pandas as pd

import collections
from nltk.stem import PorterStemmer
import re
import json_to_trec
from nltk.corpus import stopwords
import nltk
nltk.download('stopwords')

import re
from urllib.parse import urlparse
from urllib.parse import urlunparse

def urlEncodeNonAscii(b):
    return re.sub('[\x80-\xFF]', lambda c: '%%%02x' % ord(c.group(0)), b)

def iriToUri(iri):
    parts= urlparse(iri)
    return urlunparse(
        part.encode('idna') if parti==1 else urlEncodeNonAscii(part.encode('utf-8'))
        for parti, part in enumerate(parts)
    )



def save_file(data, filename):
    df = pd.DataFrame(data)
    df.to_pickle("data/all_data/" + filename)

def remove_emoji(text):
    return emoji.get_emoji_regexp().sub(u'', text)

def open_crowd_keyword_file():
    with open('crowdsourced_keywords.pickle', 'rb') as f:
        all_word = pickle.load(f)

        word_list = []
        for vaccine_word in all_word['vaccine']:
            word_list.append(vaccine_word)
            print("\"" + vaccine_word, "\",", end ="")
            if len(word_list)>= 100:
                break

        return word_list

def solr_search_query(connection, query, rows=0):
    results = connection.search(q=query, rows=rows)
    print("Saw {0} result(s).".format(len(results)))
    return results


class SolrSearch:
    def __init__(self):
        indexer = Indexer()
        self.connection = indexer.connection


def tokenizer(text):
    """ Implement logic to pre-process & tokenize document text.
        Write the code in such a way that it can be re-used for processing the user's query.
        To be implemented."""

    # print("Original ---->>>>  ", text)
    stop_words = set(stopwords.words('english'))

    text = text.lower()
    text = text.replace("'", "")
    text = text.replace("’", "")
    text = text.replace("#", "%23")
    #text = re.sub(r'[-]+', ' ', text)
    #text = re.sub(r'[^A-Za-z0-9 ]+', ' ', text)
    text = re.sub('[,]', ' ', text)
    text = re.sub(' +', ' ', text)


    tokenized_text = text.split()

    #print(text)
    #print(tokenized_text)

    ps = PorterStemmer()
    tokenized_text = [word for word in tokenized_text if word not in stop_words]
    #tokenized_text = [ps.stem(word) for word in tokenized_text]

    # print(tokenized_text)

    return tokenized_text

def get_from_solr(core_name, text, query_id):
    host = "localhost"
    port = "8983"
    collection = core_name
    qt = "select"
    url = "http://" + host + ":" + port + "/solr/" + collection + "/" + qt + "?"

    #text = text[:-1]
    query_string = "text_en%3A" + text + "text_ru%3A" + text + "text_de%3A" + text

   # query_string = "\"" + query_string + "\"~3"
    print("Query------>>>>>  ", query_string)
    q = "defType=edismax&df=text_en&df=text_ru&df=text_de&q=" + query_string
    q = "defType=edismax&q=" + query_string
    q += "&qf=tweet_hashtags%5E1.5%20text_en%5E1.5%20text_de%5E1.5%20text_ru%5E1.5"

    fl = "q.op=OR&fl=id%2Cscore"
    fq = "fq="
    rows = "rows=20"
    wt = "wt=json"
    # wt        = "wt=python"
    params = [q, fl, fq, wt, rows]
    p = "&".join(params)

    #print(url, p)
    actual_url = url + (p)

    #actual_url = urllib2.unquote(actual_url)
    key = "абс"
    #quoted = urllib.parse.quote(key)
    #actual_url = actual_url + quoted

    #actual_url = quote(actual_url)
    print(actual_url)


    connection = urllib2.urlopen(actual_url)

    if wt == "wt=json":
        response = simplejson.load(connection)
    else:
        response = eval(connection.read())

    print("Number of hits: " + str(response['response']['numFound']))
    pprint.pprint(response['response']['docs'][0])

    json_to_trec.create_trec_input(actual_url, core_name.lower(), query_id)


def main(sol_search):
    connection = sol_search.connection
    lang = 'Mexico'
    search_query = 'country:' + lang

    #get_from_solr('VSM')
    #return

    file_name = 'queries.txt'
    file_name = 'test_queries.txt'
    core_name = 'vsm'

    with open(file_name) as f:
        lines = f.readlines()

        for line in lines:
            line = line.split()

            mark = 0
            if line[0] <= "006" or line[0] == "012":
                mark = 1

            if mark == 0 and file_name == 'queries.txt':
                continue
            '''
            if (line[0] < "015"):
                continue
            actual_url= "http://18.189.38.246:8983/solr/vsm/select?defType=edismax&fl=id%2C%20score&indent=true&q.op=OR&q=text_en%3A%20%22anti-terrorist%22~2%20text_de%3APM%20Medvedev%E2%80%99s%20delegation%20to%20coordinate%20anti-terrorist%20actions&qf=tweet_hashtags%5E1.5%20text_en%5E1.5%20text_de%5E1.5%20text_ru%5E0.9&rows=20"


            json_to_trec.create_trec_input(actual_url, core_name.lower(), line[0])
            continue
            #'''
            if(file_name == 'queries.txt'):
                if(line[0] == "004"):
                    #actual_url = 'http://18.189.38.246:8983/solr/BM25/select?indent=true&q.op=OR&q=text_ru%3A%D0%91%D0%B8%D0%BB%D1%8C%D0%B4.%20%D0%92%D0%BD%D1%83%D1%82%D1%80%D0%B5%D0%BD%D0%BD%D0%B8%D0%B9%20%D0%B4%D0%BE%D0%BA%D1%83%D0%BC%D0%B5%D0%BD%D1%82%20%D0%B3%D0%BE%D0%B2%D0%BE%D1%80%D0%B8%D1%82%2C%20%D1%87%D1%82%D0%BE%20%D0%93%D0%B5%D1%80%D0%BC%D0%B0%D0%BD%D0%B8%D1%8F%20%D0%BF%D1%80%D0%B8%D0%BC%D0%B5%D1%82%201%2C5%20%D0%BC%D0%BB%D0%BD%20%D0%B1%D0%B5%D0%B6%D0%B5%D0%BD%D1%86%D0%B5%D0%B2%20%D0%B2%20%D1%8D%D1%82%D0%BE%D0%BC%20%D0%B3%D0%BE%D0%B4%D1%83%0Atext_en%3A%D0%91%D0%B8%D0%BB%D1%8C%D0%B4.%20%D0%92%D0%BD%D1%83%D1%82%D1%80%D0%B5%D0%BD%D0%BD%D0%B8%D0%B9%20%D0%B4%D0%BE%D0%BA%D1%83%D0%BC%D0%B5%D0%BD%D1%82%20%D0%B3%D0%BE%D0%B2%D0%BE%D1%80%D0%B8%D1%82%2C%20%D1%87%D1%82%D0%BE%20%D0%93%D0%B5%D1%80%D0%BC%D0%B0%D0%BD%D0%B8%D1%8F%20%D0%BF%D1%80%D0%B8%D0%BC%D0%B5%D1%82%201%2C5%20%D0%BC%D0%BB%D0%BD%20%D0%B1%D0%B5%D0%B6%D0%B5%D0%BD%D1%86%D0%B5%D0%B2%20%D0%B2%20%D1%8D%D1%82%D0%BE%D0%BC%20%D0%B3%D0%BE%D0%B4%D1%83%20%0Atext_de%3A%D0%91%D0%B8%D0%BB%D1%8C%D0%B4.%20%D0%92%D0%BD%D1%83%D1%82%D1%80%D0%B5%D0%BD%D0%BD%D0%B8%D0%B9%20%D0%B4%D0%BE%D0%BA%D1%83%D0%BC%D0%B5%D0%BD%D1%82%20%D0%B3%D0%BE%D0%B2%D0%BE%D1%80%D0%B8%D1%82%2C%20%D1%87%D1%82%D0%BE%20%D0%93%D0%B5%D1%80%D0%BC%D0%B0%D0%BD%D0%B8%D1%8F%20%D0%BF%D1%80%D0%B8%D0%BC%D0%B5%D1%82%201%2C5%20%D0%BC%D0%BB%D0%BD%20%D0%B1%D0%B5%D0%B6%D0%B5%D0%BD%D1%86%D0%B5%D0%B2%20%D0%B2%20%D1%8D%D1%82%D0%BE%D0%BC%20%D0%B3%D0%BE%D0%B4%D1%83'
                    actual_url = "http://18.189.38.246:8983/solr/" + core_name +"/select?defType=dismax&df=text_de&df=text_en&fl=id%2C%20score&indent=true&q.op=OR&q=text_en%3AWegen%20Fl%C3%BCchtlingskrise%3A%20Angela%20Merkel%20st%C3%BCrzt%20in%20Umfragen%20text_ru%3AWegen%20Fl%C3%BCchtlingskrise%3A%20Angela%20Merkel%20st%C3%BCrzt%20in%20Umfragen%20text_de%3AWegen%20Fl%C3%BCchtlingskrise%3A%20Angela%20Merkel%20st%C3%BCrzt%20in%20Umfragen&rows=20"
                    json_to_trec.create_trec_input(actual_url, core_name.lower(), line[0])
                    continue
                if (line[0] == "005"):
                    actual_url = 'http://18.189.38.246:8983/solr/' + core_name +"/select?defType=dismax&df=text_ru&df=text_en&fl=id%2C%20score&indent=true&q.op=OR&q=text_en%3A%D0%A0%D0%A4%20%D0%B2%20%D0%A1%D0%B8%D1%80%D0%B8%D0%B8%20%D0%B2%D1%8B%D0%BD%D1%83%D0%B4%D0%B8%D0%BB%D0%B8%20250%20%D1%82%D1%83%D0%BD%D0%B8%D1%81%D1%81%D0%BA%D0%B8%D1%85%20%D0%B1%D0%BE%D0%B5%D0%B2%D0%B8%D0%BA%D0%BE%D0%B2%20%D0%B1%D0%B5%D0%B6%D0%B0%D1%82%D1%8C%20text_ru%3A%D0%A0%D0%A4%20%D0%B2%20%D0%A1%D0%B8%D1%80%D0%B8%D0%B8%20%D0%B2%D1%8B%D0%BD%D1%83%D0%B4%D0%B8%D0%BB%D0%B8%20250%20%D1%82%D1%83%D0%BD%D0%B8%D1%81%D1%81%D0%BA%D0%B8%D1%85%20%D0%B1%D0%BE%D0%B5%D0%B2%D0%B8%D0%BA%D0%BE%D0%B2%20%D0%B1%D0%B5%D0%B6%D0%B0%D1%82%D1%8C%20text_de%3A%D0%A0%D0%A4%20%D0%B2%20%D0%A1%D0%B8%D1%80%D0%B8%D0%B8%20%D0%B2%D1%8B%D0%BD%D1%83%D0%B4%D0%B8%D0%BB%D0%B8%20250%20%D1%82%D1%83%D0%BD%D0%B8%D1%81%D1%81%D0%BA%D0%B8%D1%85%20%D0%B1%D0%BE%D0%B5%D0%B2%D0%B8%D0%BA%D0%BE%D0%B2%20%D0%B1%D0%B5%D0%B6%D0%B0%D1%82%D1%8C&rows=20"
                    json_to_trec.create_trec_input(actual_url, core_name.lower(), line[0])

                    continue
                if (line[0] == "012"):
                    actual_url = 'http://18.189.38.246:8983/solr/' + core_name +'/select?fl=id%2C%20score&indent=true&q.op=OR&q=text_ru%3AASYL-FL%C3%9CCHTLING%20bedankt%20sich%20per%20Video-Botschaft%20bei%20Til%20Schweiger%20text_en%3AASYL-FL%C3%9CCHTLING%20bedankt%20sich%20per%20Video-Botschaft%20bei%20Til%20Schweiger%20text_de%3AASYL-FL%C3%9CCHTLING%20bedankt%20sich%20per%20Video-Botschaft%20bei%20Til%20Schweiger&rows=20'
                    json_to_trec.create_trec_input(actual_url, core_name.lower(), line[0])
                    continue

            if (file_name == 'test_queries.txt'):
                #continue
                if (line[0] == "005"):
                    actual_url = 'http://18.189.38.246:8983/solr/' + core_name +'/select?defType=edismax&fl=id%2C%20score&indent=true&q.op=OR&q=text_ru%3A%D0%91%D0%B8%D0%BB%D1%8C%D0%B4.%20%D0%92%D0%BD%D1%83%D1%82%D1%80%D0%B5%D0%BD%D0%BD%D0%B8%D0%B9%20%D0%B4%D0%BE%D0%BA%D1%83%D0%BC%D0%B5%D0%BD%D1%82%20%D0%B3%D0%BE%D0%B2%D0%BE%D1%80%D0%B8%D1%82%2C%20%D1%87%D1%82%D0%BE%20%D0%93%D0%B5%D1%80%D0%BC%D0%B0%D0%BD%D0%B8%D1%8F%20%D0%BF%D1%80%D0%B8%D0%BC%D0%B5%D1%82%201%2C5%20%D0%BC%D0%BB%D0%BD%20%D0%B1%D0%B5%D0%B6%D0%B5%D0%BD%D1%86%D0%B5%D0%B2%20%D0%B2%20%D1%8D%D1%82%D0%BE%D0%BC%20%D0%B3%D0%BE%D0%B4%D1%83&qf=tweet_hashtags%5E1.5%20text_en%5E1.5%20text_de%5E1.5%20text_ru%5E1.5&rows=20'


                    json_to_trec.create_trec_input(actual_url, core_name.lower(), line[0])
                    continue

                if (line[0] == "003"):
                    actual_url = 'http://18.189.38.246:8983/solr/' + core_name + "/select?defType=edismax&fl=id%2C%20score%2C%20text_de%2C%20text_en%2C%20text_ru&indent=true&q.op=OR&q=text_en%3AAssad%20und%20ISIS%20auf%20dem%20Vormarsch&qf=tweet_hashtags%5E1.5%20text_en%5E1.5%20text_de%5E1.5%20text_ru%5E1.5&rows=20"

                    json_to_trec.create_trec_input(actual_url, core_name.lower(), line[0])
                    continue
            #print("Collecting data  ", line[0], line[1::])
            search_query = ""
            word_sen = ""
            for word in line[1::]:
                word_sen += word + " "
            tokenized_text = tokenizer(word_sen)
            print(tokenized_text)
            #continue
            for word in tokenized_text:
                #if (word.find('-') == -1):
                search_query += word + "%20"
                continue
                if (word.find('-') != -1):
                    cur = "\"" + word +"\"" + "%5E4"
                    search_query += cur + "%20"
                    continue



            print("Search query==", search_query)

            #search_query = re.sub(r'[^\x00-\x7F]+',' ', search_query)


            print("query id   core name and query  _______>>>>>>>>>    ", line[0], core_name, search_query)

            get_from_solr(core_name, (search_query), line[0])
            #break

            #sr_query = "http://18.189.38.246:8983/solr/VSM/select?q=*%3A*&fl=id%2Cscore&wt=json&indent=true&rows=20"
    return

if __name__ == "__main__":
    sol_search = SolrSearch()
    main(sol_search)