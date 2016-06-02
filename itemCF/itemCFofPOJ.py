# encoding=utf8

#########################################
# 运行依赖
#########################################

import os
import sys
import json
import math
import random
DIR_THIS = os.path.dirname(os.path.abspath(__file__))
DIR_DIST = 'hdfs://hadoop-meituan/user/hadoop-traffic/qinsiyuan/dist'
SPARK_SUBMIT = '/opt/meituan/versions/spark-1.5/bin/spark-submit'
global SPARK_CONF, YARN_CONF, APP_NAME


#########################################
# 参数配置
# APP_NAME: 程序执行名称
# OWNER: 开发者misId
# SPARK_CONF: spark相关参数配置
# YARN_CONF: yarn相关参数配置
# PY_FILES: 运行依赖的.py .egg .zip .ZIP
#########################################

APP_NAME = 'itemCFofPOJ'
OWNER = 'lijiazhen'
SPARK_CONF = {
    'spark.rdd.compress': 'true',
    'spark.akka.frameSize': '100',
    'spark.default.parallelism': '100',
    'spark.storage.memoryFraction': '0.8',
    'spark.sql.shuffle.partitions': '100',
    'spark.yarn.executor.memoryOverhead': '4096',
    'spark.sql.inMemoryStorage.compressed': 'true',
    'spark.core.connection.ack.wait.timeout': '100',
}
YARN_CONF = {
    'num-executors': '25', 
    'executor-cores': '4',
    'master': 'yarn-client',
    'executor-memory': '8g',
    'conf': 'spark.ui.port=0',
    'queue': 'root.hadoop-traffic.test01',
}
PY_FILES = [
    DIR_DIST + '/medusa.egg',
]


########################################
# 主函数
# sc: 默认全局变量, SparkContext
# hc: 默认全局变量, HiveContext
########################################

def rank_flat(line):
    result = list()
    if len(line['items']) < 2:
        return result
    score = 1 / (math.log(1 + len(line['items']) * 1.0))
    for i in range(0, len(line['items'])):
        x = line['items'][i]
        if x >= 10000 or x < 1000:
            continue
        for j in range(0, i):
            y = line['items'][j]
            if y > 10000:
                continue
            result.append(((min(x, y), max(x, y)), score))
    return result

def ucnt_flat(line):
    result = list()
    if len(line['items']) < 2:
        return result
    for _ in line['items']:
        result.append((_, 1))
    return result

#def reducer(line_a, line_b):
#    if len(line_a['rank']) < len(line_b['rank']):
#        line_a, line_b = line_b, line_a
#    for i in range(0, len(line_b['rank'])):
#        line_a['prob_ucnt'][i] += line_b['prob_ucnt'][i]
#        for j in range(0, len(line_b['rank'])):
#            line_a['rank'][i][j] += line_b['rank'][i][j]
#    return line_a

def main():
    pass

def test():
    def process_map():
        #input_path = '../poj_data/paser_loger/data'
        #data = open(input_path, 'rb').read().split('\n')
        #data = map(json.loads, data)
        #data = sc.parallelize(data).repartition(50)
        input_path = 'lijiazhen/tmp/poj_data'
        data = sc.textFile(input_path).map(eval).repartition(200)
        data = data.flatMap(rank_flat)
        data = data.repartition(100)
        from bftlib.hadoop import Hadoop
        h = Hadoop()
        h.rmr('lijiazhen/tmp/poj_itemCF')
        data.saveAsTextFile('lijiazhen/tmp/poj_itemCF')

    def process_reduce():
        from random import randint
        from operator import add
        input_path = 'lijiazhen/tmp/poj_itemCF'
        item2item = sc.textFile(input_path).map(eval).repartition(200)
        item2item = item2item.map(lambda _: ((_[0], randint(1, 100)), _[1]))
        item2item = item2item.reduceByKey(add)
        item2item = item2item.map(lambda _: (_[0][0], _[1]))
        item2item = item2item.reduceByKey(add)
        from bftlib.hadoop import Hadoop
        h = Hadoop()
        h.rmr('lijiazhen/tmp/poj_itemCF_res')
        item2item.coalesce(100).saveAsTextFile('lijiazhen/tmp/poj_itemCF_res')

    def get_ucnt():
        from operator import add
        input_path = 'lijiazhen/tmp/poj_data'
        data = sc.textFile(input_path).map(eval).repartition(200)
        data = data.flatMap(ucnt_flat)
        data = data.reduceByKey(add).collect()
        result = dict()
        for _ in data:
            result[_[0]] = _[1]
        return result
        #from bftlib.hadoop import Hadoop
        #h = Hadoop()
        #h.rmr('lijiazhen/tmp/poj_prob_ucnt')
        #data.saveAsTextFile('lijiazhen/tmp/poj_prob_ucnt')

    def test_result():
        rank_path = 'lijiazhen/tmp/poj_itemCF_res'
        ucnt_path = 'lijiazhen/tmp/poj_prob_ucnt'
        rank = sc.textFile(rank_path).map(eval)
        #ucnt = sc.textFile(ucnt_path).map(eval).collect()
        ucnt = get_ucnt()
        ucnt = sc.broadcast(ucnt)
        rank = rank.map(
                lambda _: {'p1': _[0][0], 'p2': _[0][1], 'score': _[1] / math.sqrt(ucnt.value[_[0][0]] * ucnt.value[_[0][1]])}
        )
        #rank = rank.keyBy(lambda _: _[0][0]).join(ucnt.value)
        #rank = rank.map(lambda _: (_[1][0][0], _[1][0][1] / _[1][1]))
        #rank = rank.keyBy(lambda _: _[0][1]).join(ucnt.value)
        #rank = rank.map(lambda _: (_[1][0][0], _[1][0][1] / _[1][1]))
        from bftlib.hadoop import Hadoop
        h = Hadoop()
        h.rmr('lijiazhen/tmp/poj_itemCF_rank')
        rank.saveAsTextFile('lijiazhen/tmp/poj_itemCF_rank')

    #process_map()
    #process_reduce()
    #get_ucnt()
    test_result()



#########################################
# 初始化
#########################################

def init():
    import json
    import socket
    import pyspark
    global sc, hc, pyspark
    HOST_NAME = socket.getfqdn(socket.gethostname())
    app_name = {
        'team': 'traffic',
        'queue': YARN_CONF['queue'],
        'email': OWNER + '@meituan.com',
        'host': HOST_NAME,
        'path': sys.argv[0],
        'app': APP_NAME,
        'timeout': 3600,
    }
    conf = pyspark.SparkConf().setAppName(json.dumps(app_name)).setAll(SPARK_CONF.items())
    sc = pyspark.SparkContext(conf=conf)
    for py_file in PY_FILES:
        print sc.addPyFile(py_file)
    hc = pyspark.sql.HiveContext(sc)


#########################################
# 提交到集群执行
# stdout: 当前路径下"log.{APP_NAME}"文件
#########################################

def run():
    cmd = ' --'.join(
        [SPARK_SUBMIT] + map(lambda _: ' '.join(_), YARN_CONF.items())
    ) + ' %s/%s run' % (DIR_THIS, sys.argv[0])
    from tempfile import TemporaryFile
    from subprocess import call
    buf = TemporaryFile()
    print cmd
    exit_status = call(cmd, stdout=buf, shell=True)
    buf.seek(0)
    stdout = buf.read()
    file('log.' + APP_NAME, 'wb').write(stdout)
    if exit_status:
        raise Exception(stdout)
    print stdout

if __name__ == '__main__':
    if len(sys.argv) == 1:
        run()
    else:
        init()
        #main()
        test()

