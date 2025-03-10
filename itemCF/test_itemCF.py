# encoding=utf8

#########################################
# 运行依赖
#########################################

import os
import sys
import json
import random
import unittest

DIR_THIS = os.path.dirname(os.path.abspath(__file__))
global APP_NAME


#########################################
# 参数配置
# APP_NAME: 程序执行名称
# OWNER: 开发者misId
# PY_FILES: 运行依赖的.py .egg .zip .ZIP
#########################################

APP_NAME = 'test_itemCF'
OWNER = 'lijiazhen'
PY_FILES = [
    DIR_THIS + '/itemCF.py'
]


########################################
# 单元测试
# sc: 默认全局变量, SparkContext
# hc: 默认全局变量, HiveContext
########################################

class test(unittest.TestCase):

    def setUp(self):
        init()
        pass
    
    def tearDown(self):
        sc.stop()
        pass

    def _test_in(self):
        input_path = '../poj_data/paser_loger/data'
        data = open(input_path, 'rb').read().split('\n')
        data = map(json.loads, data)
        print len(data)
        print type(data[0])
        print data[0]
        data = filter(
            lambda _: _['total_num'] < 10 and _['total_num'] > 5
        ,data)
        print data[0:5]

    def _test_mapper(self):
        from itemCF import mapper
        data = {u'items': [1001, 1004, 1005, 1008, 1017, 1326, 1657, 2136, 2301], u'user_id': u'cpp050600448026', u'total_num': 9}
        data = mapper(data)
        print data

    def _test_reducer(self):
        from itemCF import mapper
        from itemCF import reducer
        data = [
            {u'rank': {
                (1001, 1003): 0.1,
                (1001, 1004): 0.1,
                (1003, 1004): 0.1,
                },
                u'prob_ucnt': {1001: 1, 1003: 1, 1004: 1}
            },
            {u'rank': {
                (1001, 1002): 0.1,
                (1001, 1003): 0.1,
                (1002, 1003): 0.1,
                },
                u'prob_ucnt': {1001: 1, 1002: 1, 1003: 1}
            },
        ]
        data = sc.parallelize(data)
        data = data.reduce(reducer)
        print data

    def _test_process_map(self):
        from itemCF import mapper
        input_path = 'lijiazhen/tmp/poj_data'
        poj_ac_record = sc.textFile(input_path).sample(False, 0.01).map(eval).repartition(200)
        item2item_rank = poj_ac_record.map(mapper)
        from bftlib.hadoop import Hadoop
        h = Hadoop()
        h.rmr('lijiazhen/tmp/poj_itemCF')
        item2item_rank.repartition(20).saveAsTextFile('lijiazhen/tmp/poj_itemCF')

    def test_process_reduce(self):
        from itemCF import reducer
        input_path = 'lijiazhen/tmp/poj_itemCF'
        data = sc.textFile(input_path).map(eval)
        data = data.reduce(reducer)
        data = str(data)
        fp = open('./tmp', 'wb+')
        fp.write(data)
        #data = data.reduceByKey(reducer)
        #from bftlib.hadoop import Hadoop
        #h = Hadoop()
        #h.rmr('lijiazhen/tmp/poj_itemCF_res')
        #data.saveAsTextFile('lijiazhen/tmp/poj_itemCF_res')



#########################################
# 初始化
#########################################

def init():
    import socket
    import pyspark
    global sc, hc, pyspark
    HOST_NAME = socket.getfqdn(socket.gethostname())
    app_name = '(traffic) (%s) (%s@meituan.com) (%s:%s)' % (
        APP_NAME, OWNER, HOST_NAME, DIR_THIS
    )
    conf = pyspark.SparkConf().setAppName(app_name).setMaster('local')
    sc = pyspark.SparkContext(conf=conf)
    for py_file in PY_FILES:
        sc.addPyFile(py_file)
    hc = pyspark.sql.HiveContext(sc)


if __name__ == '__main__':
    unittest.main()

