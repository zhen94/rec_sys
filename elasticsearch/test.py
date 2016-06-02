#encoding=utf-8
#test Elastic Search class

import time
import json
import random
import requests
import Elasticsearch


#def _show(_, indent=4):
#    try:
#        print json.dumps(_.json(), indent=indent)
#    except:
#        print _.content
#
#def put():
#    data = {
#        'p2': 3685, 
#        'p1': 1905, 
#        'score': 1.0806772405365401e-05
#    }
#    url = 'http://localhost:9200/zhen/test'
#    r = requests.put(url, json=data)
#    print r
#    #if i % 100 == 0:
#    #    _show(r, None)
#
#def mput(data_index, data_type, src):
#    header = { 'create':  { '_index': data_index, '_type': data_type}}
#    for i in range(0, len(src), 10000):
#        data = src[i: i + 9999]
#        result = list()
#        for _ in data:
#            result.append(json.dumps(header))
#            result.append(json.dumps(_))
#        #print result
#        result = '\n'.join(result) + '\n'
#        url = 'http://localhost:9200/_bulk'
#        r = requests.post(url, data=result)
#        print 'part-%d done\n' % int(i / 10000 + 1)
#
#def get():
#    data = {
#        'from': 0,
#        'size': 10,
#        'query': {
#            'bool': {
#                'should': [
#                    {'match': {'p1':  1003}},
#                    {'match': {'p3':  1003}},
#                ],
#            },
#        },
#        'sort': [
#            {'score': 'desc'},
#        ],
#    }
#    url = 'http://localhost:9200/recommend_sys/poj_itemCF/_search'
#    r = requests.post(url, json=data)
#    _show(r)
#
#def build_dataset():
#    src = list()
#    in_fp = open('/Users/zhen/tmp/modifyRank/poj_itemCF_rank', 'rb')
#    #in_fp = open('/Users/zhen/tmp/modifyRank/test_poj_itemCF_rank', 'rb')
#    for _ in in_fp.readlines():
#        line = eval(_)
#        if line['p1'] < 1000 or line['p2'] < 1000:
#            continue
#        src.append(line)
#    print len(src)
#    start_time = time.time()
#    mput('recommend_sys', 'poj_itemCF', src)
#    #mput('zhen', 'test', src)
#    end_time = time.time()
#    print end_time - start_time
#    return

def search():
    from Elasticsearch import Elasticsearch
    a = {'a': 100}
    Elasticsearch._show(json.dumps(a))
    #Elasticsearch.get()

def main():
    pass

if __name__ == '__main__':
    #build_dataset()
    search()
    #main()
