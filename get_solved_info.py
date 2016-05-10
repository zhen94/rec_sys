#encoding=utf-8
#get_solved_info.py
import re
import os
import bs4
import json
import time
import multiprocessing
from get_user import get_date
from get_user import set_logger
from get_user import write_log
from get_user import request

def get_info(user):
    userid = user.get('user_id', '')
    url = 'http://poj.org/userstatus?user_id=%s' % userid
    #print url
    for i in range(0, 3):
        time.sleep((i + 1) * 1)
        text = request(url)['text']
        items = re.findall('p\((\d*?)\)', text)
        user['items'] = map(lambda _: int(_), items)
        soup = bs4.BeautifulSoup(text)
        #print userid
        #print type(userid)
        total_num = soup.find(
            'a',
            attrs = {
                'href': re.compile('status\?result=0&user_id=%s' % userid ),
            },
        )
        try:
            total_num = int(total_num.string)
        except:
            total_num = 0
        user['total_num'] = total_num
        if len(items) == int(total_num):
            print user
            return user
    write_log('userid=%s\n%s' %(userid, url))
    user['items'] = []
    user['total_num'] = 0
    print user
    return user

def main():
    set_logger('lost_info')
    input_path = os.path.abspath('.') + '/user_list/' + '20160425'
    data = open(input_path, 'rb+').read()
    data = data.split('\n')[:-1]
    pool = multiprocessing.Pool(processes = 8)
    data = pool.map(json.loads, data)
    data = pool.map(get_info, data)
    #print data
    output_path = os.path.abspath('.') + '/user_info/'
    if os.path.isdir(output_path) == False:
        os.mkdir(output_path)
    out_fp = open(output_path + get_date(), 'wb')
    out_fp.write(json.dumps(data))

def test():
    set_logger('lost_info')
    user = {'user_id': 'hdmnba'}
    get_info(user)

if __name__ == '__main__':
    #test()
    main()
