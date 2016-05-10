#encoding=utf-8
#get_user.py
import re
import os
import bs4
import json
import time
import datetime
import requests
import logging
import multiprocessing

def get_date(delta=0):
    import datetime
    date = datetime.datetime.now()
    date += datetime.timedelta(days = delta)
    dt = date.strftime('%Y%m%d')
    return dt

def set_logger(log_name):
    log_path = os.path.abspath('.') + '/%s/' % log_name
    if os.path.exists(log_path) == False:
        os.mkdir(log_path)
    logging.basicConfig(
        level = logging.ERROR,
        format = '%(asctime)s %(message)s',
        datefmt = '%H:%M:%S',
        filename = '%s/%s' % (log_path, get_date()),
        filemode = 'wb'
    )

def write_log(log):
    logger = logging.getLogger()
    logger.error(log)

def request(url):
    for i in range(0, 3):
        time.sleep((i + 1) * 1)
        try:
            r = requests.get(url, timeout = 5.0)
            state = r.status_code
            text = r.text
            if r.status_code == 200:
                result = {'state': state, 'text': text}
                break
        except:
            result = {'state': 0, 'text': 'timeout'}
    return result

def id_parser(st_num, width = 10):
    url = 'http://poj.org/userlist?start=%d&size=%d&of1=solved&od1=desc&of2=submit&od2=asc' % (st_num, width)
    for i in range(0, 3):
        time.sleep((i + 1) * 1)
        text = request(url)['text']
        soup = bs4.BeautifulSoup(text)
        ids = soup.find_all(
            'a',
            attrs = {
                'href': re.compile('userstatus\?user_id='),
            },
        )
        ids = map(lambda _: str(_.string), ids)
        if len(ids) > width / 10:
            print url
            print ids
            return ids
    print url
    print ids
    write_log('lost users: No%d~%d width: %d\nurl: %s\ntext: %s' %
        (st_num, st_num + width - 1, width, url, text)
    )
    return []

def cron_user_list(end_page, width):
    set_logger('lost_log')
    user_list = [_ * width for _ in range(0, end_page)]
    pool = multiprocessing.Pool(processes = 4)
    user_list = pool.map(id_parser, user_list)
    user_list = [user for sub_list in user_list for user in sub_list]
    return user_list

def save_user_list_as_json(user_list):
    base_path = os.path.abspath('.') + '/user_list/'
    if os.path.isdir(base_path) == False:
        os.mkdir(base_path)
    path = base_path + get_date()
    fp = open(path, 'wb')
    user_list = '\n'.join(map(lambda _: json.dumps({'user_id': _}), user_list))
    fp.write(user_list)

def test():
    #print id_parser(0, 10)
    #cron_user_list(435, 500)
    #user_list =  cron_user_list(280, 500)
    user_list =  cron_user_list(5, 10)
    save_user_list_as_json(user_list)
    #print id_parser(0, 10)
    pass

if __name__ == '__main__':
    test()

