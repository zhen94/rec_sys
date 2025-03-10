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
    'spark.driver.maxResultSize': '1096',
}
YARN_CONF = {
    'num-executors': '25', 
    'executor-cores': '2',
    'master': 'yarn-client',
    'executor-memory': '8g',
    'driver-memory': '4g',
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

def mapper(line):
    result = dict()
    #limit = max(line['items']) + 1
    result['rank'] = dict()
    result['prob_ucnt'] = dict()
    if len(line['items']) < 2:
        return result
    score = 1 / (math.log(1 + len(line['items']), 2))
    for i in range(0, len(line['items'])):
        x = line['items'][i]
        if x > 10000:
            continue
        result['prob_ucnt'][x] = 1
        for j in range(0, i):
            y = line['items'][j]
            if y > 10000:
                continue
            result['rank'][(min(x, y), max(x, y))] = score
    return result

def reducer(line_a, line_b):
    for k, v in line_b['rank'].items():
        if k in line_a['rank']:
            line_a['rank'][k] += v
        else:
            line_a['rank'][k] = v
    for k, v in line_b['prob_ucnt'].items():
        if k in line_a['prob_ucnt']:
            line_a['prob_ucnt'][k] += v
        else:
            line_a['prob_ucnt'][k] = v
    return line_a

def main():
        input_path = 'lijiazhen/tmp/poj_data'
        poj_ac_record = sc.textFile(input_path).map(eval).repartition(1000)
        item2item_rank = poj_ac_record.map(mapper)
        item2item_rank.cache()
        item2item_rank = item2item_rank.reduce(reducer)
        data = str(item2item_rank)
        fp = open('./result', 'wb+')
        fp.write(data)

def test():
    def process_map():
        input_path = 'lijiazhen/tmp/poj_data'
        poj_ac_record = sc.textFile(input_path).map(eval).repartition(200)
        poj_ac_record = poj_ac_record.filter(lambda _: len(_['items']) < 1000 and len(_['items']) > 1)
        item2item_rank = poj_ac_record.map(mapper)
        from bftlib.hadoop import Hadoop
        h = Hadoop()
        h.rmr('lijiazhen/tmp/poj_itemCF')
        item2item_rank.saveAsTextFile('lijiazhen/tmp/poj_itemCF')

    def process_reducer():
        input_path = 'lijiazhen/tmp/poj_itemCF'
        poj_ac_record = sc.textFile(input_path).map(eval).repartition(200)
        item2item_rank = poj_ac_record.reduce(reducer)
        data = str(item2item_rank)
        fp = open('./result', 'wb+')
        fp.write(data)

    #process_map()
    process_reducer()


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

