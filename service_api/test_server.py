#encoding=utf-8

import json
import requests

def test_http():
    data = {
            'problems': [10012, 10023],
    }
    url = 'http://localhost:1024'
    print 'start'
    #r = requests.post(url, data = json.dumps(data), timeout = 1)
    r = requests.post(url, data = json.dumps(data))
    print 'end'
    print r
    print r.headers
    print r.json
    print r.text

def test_grl():
    from service_api import get_recommend_list
    test_case = {
        'problem_list': [1000, 1001, 1002],
        'filter_list': [1000, 1001, 1002],
        'return_num': 10,
    }
    result = get_recommend_list(json.dumps(test_case))
    print result    

def test_service():
    url = 'http://localhost:1024'
    test_case = {
        'problem_list': [1000, 1001, 1002],
        'filter_list': [1000, 1001, 1002],
        'return_num': 10,
    }
    r = requests.post(url, data = json.dumps(test_case))
    print r
    print r.text

if __name__ == '__main__':
    #test_grl()
    test_service()
