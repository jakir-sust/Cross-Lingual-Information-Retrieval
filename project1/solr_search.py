from indexer import Indexer
import json

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
            change = 1

        if result['tweet_lang'] == 'es':
            result['country'] = 'Mexico'
            change = 1
        if result['country'] == 'México':
            result['country'] = 'Mexico'
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

def solr_search_query(connection, query, rows=0):
    results = connection.search(q=query, rows=rows)
    print("Saw {0} result(s).".format(len(results)))
    return results

def main():
    indexer = Indexer()
    connection = indexer.connection_object()
    search_query = 'tweet_lang:en'
    results = solr_search_query(connection, query=search_query, rows=100000)

    modify_poi_name(connection, results)

    return

if __name__ == "__main__":
    main()