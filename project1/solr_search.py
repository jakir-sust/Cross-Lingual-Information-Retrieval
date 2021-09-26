from indexer import Indexer
import json
import emoji
import pickle

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

def open_keyword_file():
    all_lang = {}
    all_lang['hi'], all_lang['es'], all_lang['en'] = [], [], []

    with open('data/hi_id.pkl', 'rb') as f:
        all_lang['hi'] = pickle.load(f)

    with open('data/en_id.pkl', 'rb') as f:
        all_lang['en'] = pickle.load(f)

    with open('data/es_id.pkl', 'rb') as f:
        all_lang['es'] = pickle.load(f)

    print(all_lang['hi'][0][0])

    cnt = 0
    for a in all_lang['es'][0]:
        for b in all_lang['en'][0]:
            if a == b:
                cnt += 1
                # break
                print("matched")
    print('Total matched     ', cnt)

def modify_by_lang_country(connection, results):
    # all_lang = []
    # for result in results:
    #    all_lang.append(result['id'])

    # df = pd.DataFrame(all_lang)
    # df.to_pickle("data/" + search_lang + "_id.pkl")

    for result in results:
        change = 0
        if result['tweet_lang'] == 'hi':
            result['country'] = 'India'
            result['text_hi'] = remove_emoji(result['tweet_text'])
            #print(result)
            change = 1

        if change == 1:
            connection.add(result)

def modify_poi_name(connection, results):
    with open("config.json") as json_file:
        data = json.load(json_file)
    #print(data['pois'])

    poi_vis={}
    for poi in data['pois']:
        #print(poi['screen_name'])
        poi_vis[poi['screen_name']] = 1

    #print(poi_vis)
    #return
    for result in results:
        #print(result)
        change = 0
        if 'poi_name' in result:
            poi_name = result['poi_name']
            if poi_name not in poi_vis:
                result['poi_name'] = None
                result['poi_id'] = None
                change = 1

        if change == 1:
            print("cnt +++")
            connection.add(result)
def get_all_poi():
    with open("config.json") as json_file:
        data = json.load(json_file)
    poi_name = []
    for poi in data['pois']:
        poi_name.append(poi['screen_name'])
    return poi_name

def get_all_keyword():
    with open("config.json") as json_file:
        data = json.load(json_file)
    keyword_name = []
    for poi in data['keywords']:
        keyword_name.append(poi['name'])
    return keyword_name


def solr_unique_check(connection, results):
    x = set()
    cnt = 0
    for result in results:
        #if 'text_hi' in result:
        cnt +=1
        x.add(hash(result['tweet_text']))

    print(len(x), cnt)
def solr_remove_duplicate(connection, results, poi_names):
    text_dict = {}
    cnt = 0
    for result in results:
        change = 0
        hash_text = hash(result['tweet_text'])
        if result['tweet_text'].find("RT @") != -1:
        #if hash_text in text_dict:
            change = 1
        '''
            for poi in poi_names:
                if 'poi_name' in result and poi == result['poi_name']:
                    change = 1
                    break
                    '''

        text_dict[hash_text] = 1
        #if hash_text in text_dict:
        if change == 1:
            cnt += 1
            #print("Deleted")
            connection.delete(id = result['id'])
            #return

    print(cnt)

def solr_search_query(connection, query, rows=0):
    results = connection.search(q=query, rows=rows)
    print("Saw {0} result(s).".format(len(results)))
    return results

def get_poi_twitter_id_in_solr(connection, poi_name):
    search_query = 'poi_name:' + poi_name
    results = solr_search_query(connection, query=search_query, rows=100000)

    poi_dict = {}
    for result in results:
        poi_dict[result['id']] = 1
    return poi_dict

def search_by_poi(connection, poi_name):
    search_query = 'poi_name:' + poi_name
    results = solr_search_query(connection, query=search_query, rows=100000)
    #print(len(results))

    min_id = 1439607728016609286
    for res in results:
        min_id = min(min_id, int(res['id']))
    #print(min_id)

    return min_id
class SolrSearch:
    def __init__(self):
        indexer = Indexer()
        self.connection = indexer.connection_object()

def main(sol_search):
    connection = sol_search.connection
    search_query = 'tweet_lang:es'
    results = solr_search_query(connection, query=search_query, rows=100000)

    #open_crowd_keyword_file()
    #solr_unique_check(connection, results)
    poi_names = get_all_poi()
    solr_remove_duplicate(connection, results, poi_names)
    #modify_poi_name(connection, results)
    #modify_by_lang_country(connection,results)

    #print(poi_names)

    #min_id = search_by_poi(connection, poi_names[0])



    return

if __name__ == "__main__":
    sol_search = SolrSearch()
    main(sol_search)