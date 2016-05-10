# encoding=utf8

#########################################
# 运行依赖
#########################################

import os
import sys
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

APP_NAME = 'itemCF'
OWNER = 'lijiazhen'
SPARK_CONF = {
    'spark.rdd.compress': 'true',
    'spark.akka.frameSize': '100',
    'spark.default.parallelism': '100',
    'spark.storage.memoryFraction': '0.8',
    'spark.sql.shuffle.partitions': '100',
    'spark.yarn.executor.memoryOverhead': '1024',
    'spark.sql.inMemoryStorage.compressed': 'true',
    'spark.core.connection.ack.wait.timeout': '100',
}
YARN_CONF = {
    'num-executors': '1', 
    'executor-cores': '4',
    'master': 'yarn-client',
    'executor-memory': '2g',
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

def main():
    def test_func(_):
        from medusalib.city_info.city_info_sogou import city_name
        return city_name.keys()[0]
    lines = sc.parallelize(range(0, 10)).map(test_func)
    print lines.take(10)


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
        main()

