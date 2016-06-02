#encoding=utf-8
#Elastic Search class

import time
import json
import random
import requests

#def put(i):
#    data = {
#        'a': random.randint(0, 10000),
#        'b': random.randint(0, 10000),
#        'c': random.randint(0, 10000),
#        'd': random.randint(0, 10000),
#    }
#    url = 'http://9200/zhen/test/%d' % (i)
#    r = requests.put(url, json=data)
#    if i % 100 == 0:
#        _show(r, None)
#
#def mput(i):
#    data = []
#    for _ in range(0, 10000):
#        header = { 'create':  { '_index': 'zhen', '_type': 'test'}}
#        data.append(json.dumps(header))
#        body = {
#            'a': random.randint(0, 10000),
#            'b': random.randint(0, 1000000),
#            'c': random.randint(0, 100000000),
#            'd': random.randint(0, 1000000000),
#            'uuid': str(uuid.uuid4()),
#        }
#        data.append(json.dumps(body))
#    data = '\n'.join(data) + '\n'
#    url = 'http://loclahost:9200/_bulk'
#    r = requests.post(url, data=data)

def get(query):
    #data = {
    #    'from': 0,
    #    'size': 10,
    #    'query': {
    #        'bool': {
    #            'must': [
    #                {'range': {'a': {'lt': 1000}}},
    #                {'range': {'b': {'lt': 1000}}},
    #                {'range': {'c': {'lt': 1000}}},
    #            ],
    #        },
    #    },
    #    'sort': [
    #        {'b': 'desc'},
    #    ],
    #}
    url = 'http://localhost:9200/zhen/test/_search'
    r = requests.post(url, json=query)
    _show(r)

def sql_get(query):
    url = 'http://localhost:9200/_sql?sql='
    url += query
    print url
    r = requests.get(url, timeout=10)
    #print r.status_code,r.text
    return json.dumps(r.json(), indent = 4)

def test_put():
    input_fp = open('./poj_itemCF_rank', 'rb')
    output_fp = open('./test_poj_itemCF_json', 'wb')
    start_time = time.time()
    get_sql()
    end_time = time.time()
    print '>>> get'
    print end_time - start_time
    return

def test_sql():
    sql = 'select p1, p2 from recommend_sys where (p1 in (1000, 1001, 1002) and p2 not in (1000, 1001, 1002)) or (p2 in (1000, 1001, 1002) and p1 not in (1000, 1001, 1002)) order by score desc limit 10'
    pb_list = [1000, 1001, 1002]
    result = sql_get(sql)
    print type(result)
    print result
    data = json.loads(result)
    data = data['hits']['hits']
    data = map((lambda _: _['_source']['p1'] if _['_source']['p1'] not in pb_list else _['_source']['p2']), data)
    data =  reduce(lambda x,y:x if y in x else x + [y], [[]] + data)
    #data = reduce(lambda x, y: x + y, data)
    #print data
    print data


if __name__ == '__main__':
    test_sql()
