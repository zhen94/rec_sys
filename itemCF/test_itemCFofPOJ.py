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
    DIR_THIS + '/itemCFofPOJ.py'
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
        from itemCFofPOJ import mapper
        data0 = {u'items': [1001, 1004, 1005, 1008], u'user_id': u'cpp050600448026', u'total_num': 4}
        data1 = {u'items': [], u'user_id': u'cpp050600448026', u'total_num': 0}
        data2 = {u'items': [1001], u'user_id': u'cpp050600448026', u'total_num': 1}
        print mapper(data0)
        print mapper(data1)
        print mapper(data2)

    def _test_reducer(self):
        from itemCF import mapper
        from itemCF import reducer
        data = [
            {u'rank': [
                [0, 0.7, 0.3],
                [0.7, 0, 0.5],
                [0.3, 0.5, 0]],u'prob_ucnt': [2, 3, 4]
            },
            {u'rank': [
                [0, 0.2, 0.1],
                [0.2, 0, 0.4],
                [0.1, 0.4, 0]],u'prob_ucnt': [1, 2, 3]
            }
        ]
        data = sc.parallelize(data)
        data = data.reduce(reducer)
        print data

    def _test_process_map(self):
        from itemCFofPOJ import mapper
        input_path = 'lijiazhen/tmp/poj_data'
        data = sc.textFile(input_path).map(json.loads).sample(False, 0.0001)
        data = data.flatMap(mapper)
        #.map(json.dumps)
        data = data.repartition(100)
        from bftlib.hadoop import Hadoop
        h = Hadoop()
        h.rmr('lijiazhen/tmp/poj_itemCF')
        data.saveAsTextFile('lijiazhen/tmp/poj_itemCF')

    def _test_process_reduce(self):
        from random import randint
        from operator import add
        input_path = 'lijiazhen/tmp/poj_itemCF/part-00001'
        item2item = sc.textFile(input_path).sample(False, 0.01).map(eval)
        item2item = item2item.repartition(200)
        item2item = item2item.map(lambda _: ((_[0], randint(1, 100)), _[1]))
        item2item = item2item.reduceByKey(add)
        item2item = item2item.map(lambda _: (_[0][0], _[1]))
        item2item = item2item.reduceByKey(add)
        from bftlib.hadoop import Hadoop
        h = Hadoop()
        h.rmr('lijiazhen/tmp/poj_itemCF_res')
        item2item.coalesce(10).saveAsTextFile('lijiazhen/tmp/poj_itemCF_res')

    def test_result(self):
        input_path = 'lijiazhen/tmp/poj_itemCF_rank'
        data = sc.textFile(input_path).sample(False, 0.001).map(eval)
        data = data.keyBy(lambda _: _['score'])
        data = data.sortByKey(ascending = False)
        print data.take(100)


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

