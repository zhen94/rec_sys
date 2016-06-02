#!/usr/bin/python
#coding=utf8
import sys
import time
import json
import requests
from BaseHTTPServer import BaseHTTPRequestHandler,HTTPServer

SYS_PATH = [
    '/Users/zhen/recommend_sys/elasticsearch/',
]
sys.path.extend(SYS_PATH)

def parser_se_query():
    pass

def get_recommend_list(input_json):
    from Elasticsearch import sql_get
    input_dict = json.loads(input_json)
    seed_list = map(str, input_dict['problem_list'])
    filter_list = map(str, input_dict['filter_list'])
    return_num = input_dict['return_num']
    search_num = len(set(seed_list + filter_list)) * return_num
    sql = 'select p1, p2 from recommend_sys where (p1 in (%s) and p2 not in (%s)) or (p2 in (%s) and p1 not in (%s)) order by score desc limit %d' % (','.join(seed_list), ','.join(filter_list), ','.join(seed_list), ','.join(filter_list), search_num)
    print sql
    result = sql_get(sql)
    data = json.loads(result)
    data = data['hits']['hits']
    data = map((lambda _: _['_source']['p1'] if str(_['_source']['p1']) not in seed_list else _['_source']['p2']), data)
    data =  reduce(lambda x,y:x if y in x else x + [y], [[]] + data)[:return_num +1]
    return data
  
class myHandler(BaseHTTPRequestHandler):

    def do_POST(self):
        print 'start'
        try:
            body = self.rfile.read(
                int(self.headers['content-length'])
            )
            data = get_recommend_list(body)
            data_json = json.dumps({'data': data})
            self.send_response(200)
            self.end_headers()
            self.wfile.write(data_json)
        except:  
            self.send_error(400,'Not supported: %s' % self.path)  

def run(IP, PORT_NUMBER):
    try:  
        server = HTTPServer((IP, int(PORT_NUMBER)), myHandler)  
        print 'Started httpserver on port' , PORT_NUMBER  
        server.serve_forever()  
    except KeyboardInterrupt:  
        print '^C received, shutting down the web server'  
        server.socket.close()  

def test():
    test_case = {'problem_list': [1000, 1001, 1002, 1003], 'filter_list': [1000, 1001, 1002, 1003], 'return_num': 10}
    result = get_recommend_list(test_case)
    print result

if __name__ == '__main__':
    IP = 'localhost'
    #IP = '0.0.0.0'
    PORT_NUMBER = 1024
    run(IP, PORT_NUMBER)
