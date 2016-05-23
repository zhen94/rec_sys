#
# Elastic Search Demo
# qinsiyuan@meituan.com
#

import time
import json
import uuid
import random
import requests


def _show(_, indent=4):
    try:
        print json.dumps(_.json(), indent=indent)
    except:
        print _.content

def put(i):
    data = {
        'a': random.randint(0, 10000),
        'b': random.randint(0, 10000),
        'c': random.randint(0, 10000),
        'd': random.randint(0, 10000),
    }
    url = 'http://rz-giant-mining03.rz.sankuai.com:9200/qinsiyuan/test/%d' % (i)
    r = requests.put(url, json=data)
    if i % 100 == 0:
        _show(r, None)

def mput(i):
    data = []
    for _ in range(0, 10000):
        header = { 'create':  { '_index': 'qinsiyuan', '_type': 'test'}}
        data.append(json.dumps(header))
        body = {
            'a': random.randint(0, 10000),
            'b': random.randint(0, 1000000),
            'c': random.randint(0, 100000000),
            'd': random.randint(0, 1000000000),
            'uuid': str(uuid.uuid4()),
        }
        data.append(json.dumps(body))
    data = '\n'.join(data) + '\n'
    url = 'http://rz-giant-mining03.rz.sankuai.com:9200/_bulk'
    r = requests.post(url, data=data)

def get():
    data = {
        'from': 0,
        'size': 10,
        'query': {
            'bool': {
                'must': [
                    {'range': {'a': {'lt': 1000}}},
                    {'range': {'b': {'lt': 1000}}},
                    {'range': {'c': {'lt': 1000}}},
                ],
            },
        },
        'sort': [
            {'b': 'desc'},
        ],
    }
    url = 'http://rz-giant-mining03.rz.sankuai.com:9200/qinsiyuan/test/_search'
    r = requests.post(url, json=data)
    _show(r)

def get_sql():
    url = 'http://rz-giant-mining03.rz.sankuai.com:9200/_sql?sql=select * from qinsiyuan limit 10'
    url = 'http://rz-giant-mining03.rz.sankuai.com:9200/_sql?sql=select * from qinsiyuan where a<1000 limit 10'
    url = 'http://rz-giant-mining03.rz.sankuai.com:9200/_sql?sql=select * from qinsiyuan where a<1000 order by d limit 10'
    url = 'http://rz-giant-mining03.rz.sankuai.com:9200/_sql?sql=select a, b, count(1) from qinsiyuan where a<10 and b<10 group by a, b limit 10'
    url = 'http://rz-giant-mining03.rz.sankuai.com:9200/_sql?sql=select a, count(1) from qinsiyuan where a<10000 group by a order by a desc limit 10'
    url = 'http://rz-giant-mining03.rz.sankuai.com:9200/_sql?sql=select a, count(1) c1 from qinsiyuan where a<1000 and b<100 and c>9000 and d>9900 group by a order by c1 limit 10'
    url = 'http://rz-giant-mining03.rz.sankuai.com:9200/_sql?sql=select a, b, c, d, uuid from qinsiyuan where a<1000 and b<100 and c>9000 and uuid > "a" order by a limit 10'
    url = 'http://rz-giant-mining03.rz.sankuai.com:9200/_sql?sql=select a, b, c, d, uuid from qinsiyuan where a<1000 and b<100 and c>9000 and uuid=\'fed2fd1e-5a02-4626-9d68-2b6424cb1b95\' order by a limit 10'
    url = 'http://rz-giant-mining03.rz.sankuai.com:9200/_sql?format=csv&sql=select a, b, c, d, uuid from qinsiyuan where b=1000 and uuid like "%%" order by a limit 10'
    url = 'http://rz-giant-mining03.rz.sankuai.com:9200/_sql?&sql=select a, b, c, d, uuid from qinsiyuan where b=1000 and uuid like "%%b44a%%" order by a limit 10'
    url = 'http://rz-giant-mining03.rz.sankuai.com:9200/_sql?&sql=select a, b, c, d, uuid from qinsiyuan where b=1000 and uuid like "%%b44%%" order by a limit 10'
    url = 'http://rz-giant-mining03.rz.sankuai.com:9200/_sql?&sql=select a, b, c, d, uuid from qinsiyuan where b=1000 and uuid like "%%b7b5%%" order by a limit 10'
    url = 'http://rz-giant-mining03.rz.sankuai.com:9200/_sql?&sql=select a, b, c, d, uuid from qinsiyuan where b=1000 and uuid like "%%81-9%%" order by a limit 10'
    print url
    r = requests.get(url, timeout=10)
    print r.status_code
    _show(r)


def main():
    #for _ in range(100000, 200000):
    #    put(_)

    #### MPut
    #start_time = time.time()
    #for _ in range(0, 85000000, 10000):
    #    mput(_)
    #    if _ % 100000 == 0:
    #        print _
    #end_time = time.time()
    #print '>>> mput'
    #print end_time - start_time

    #### Get
    #start_time = time.time()
    #get()
    #end_time = time.time()
    #print '>>> get'
    #print end_time - start_time

    ### SqlGet
    start_time = time.time()
    get_sql()
    end_time = time.time()
    print '>>> get'
    print end_time - start_time
    return


if __name__ == '__main__':
    main()
