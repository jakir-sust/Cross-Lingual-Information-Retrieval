# -*- coding: utf-8 -*-


import json
# if you are using python 3, you should
import urllib.request

import urllib.request as urllib2
import urllib.parse

import requests


#def create_trec_input(query_url, model_name, query_id):
# change the url according to your own corename and query
ext_url = "&q=text_en:Russia%20intervention%20in%20Syria&q.op=OR&defType=dismax&indent=true&fl=id,%20score&qf=tweet_hashtags%5E1.5%20text_en%5E1.5%20text_de%5E0.5%20text_ru%5E0.9&rows=20"
inurl = 'http://localhost:8983/solr/corename/select?q=*%3A*&fl=id%2Cscore&wt=json&indent=true&rows=20'

#in_url = 'http://localhost:8983/solr/corename/select?' + ext_url
#in_url = 'http://localhost:8983/solr/BM25/select?' + ext_url

#in_url = "http://18.189.38.246:8983/solr/BM25/select?&q=text_en:Russia%20intervention%20in%20Syria&q.op=OR&defType=dismax&indent=true&fl=id,%20score&wt=json&qf=tweet_hashtags%5E1.5%20text_en%5E1.5%20text_de%5E0.5%20text_ru%5E0.9&rows=20"
in_url = "http://18.189.38.246:8983/solr/#/BM25/select?defType=dismax&q=text_en:Russia%20intervention%20in%20Syria&q.op=OR&rows=20&fl=id,%20score&qf=tweet_hashtags%5E1.5%20text_en%5E1.5%20text_de%5E0.5%20text_ru%5E0.9&wt=json"
print(in_url)

#inurl = query_url
#outfn = model_name + '_trec_input.txt'
outfn = '1' + '_trec_input.txt'

#print(query_url)


# change query id and IRModel name accordingly
#qid = query_id
qid = '001'
#IRModel= model_name #either bm25 or vsm
IR_Model = 'bm25'
outf = open(outfn, 'a+')

print(in_url)
data = urllib2.urlopen(inurl)
#data = requests.get(inurl)

#print(data)
#data = urllib.request.urlopen(inurl).read()
# if you're using python 3, you should use
# data = urllib.request.urlopen(inurl)

docs = json.load(data)['response']['docs']
# the ranking should start from 1 and increase
rank = 1
for doc in docs:
    outf.write(qid + ' ' + 'Q0' + ' ' + str(doc['id']) + ' ' + str(rank) + ' ' + str(doc['score']) + ' ' + IRModel + '\n')
    rank += 1
outf.close()
