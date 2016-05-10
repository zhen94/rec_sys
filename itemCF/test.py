# encoding=utf8
"""
    启动方式
    spark test.py
    or /opt/meituan/spark-1.4/bin/spark-submit test.py
"""

#########################################
# 运行依赖
#########################################

import os
import sys
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

APP_NAME = 'pyspark_unittest_demo'
OWNER = 'qinsiyuan'
PY_FILES = []


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

    def testSparkDataframe(self):
        lines = hc.sql('select deal_name from mart_traffic.apideal limit 2')
        self.assertEqual(len(lines.take(10)), 2)


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

