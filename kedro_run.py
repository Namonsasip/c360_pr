# import subprocess
# import shlex
# import os, sys
# 
# pipeline_to_run = os.environ["PIPELINE_TO_RUN"]
# env_to_use = os.environ["CONF"]
# 
# 
# kedro_run_cmd = "kedro run --pipeline={} --env={}".format(pipeline_to_run, env_to_use)
# 
# print(kedro_run_cmd)
# 
# def run_command(command):
#     process = subprocess.Popen(shlex.split(command), stdout=subprocess.PIPE,
#                                cwd='/home/cdsw/customer360/', encoding='utf8', bufsize=-1)
#     while True:
#         output = process.stdout.readline()
#         if output == '' and process.poll() is not None:
#             break
#         if output:
#             print(output.strip())
#     rc = process.poll()
#     if rc:
#         print("Return Code {}".format(rc))
#     return rc
# 
# 
# return_flag = run_command(kedro_run_cmd)
# if return_flag:
#     if return_flag != 0:
#         sys.exit(2)
# 

import os
import sys
import logging
import subprocess
import subprocess,pyspark,uuid
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark import SQLContext
from pyspark.sql import functions as F

pipeline_to_run = os.environ["PIPELINE_TO_RUN"]
env_to_use = os.environ["CONF"]

####### Set Pipeline #######
pipeline_domains = pipeline_to_run.split(",")

#######################################################

p_appName = "app-C360-"+str(uuid.uuid4().hex[:8])
conf = pyspark.SparkConf()
spark = SparkSession.builder.master('yarn')\
            .appName(p_appName)\
            .config("spark.yarn.queue", "pr_c360")\
            .config("spark.executor.memory","256g")\
            .config("spark.driver.memory", "256g")\
            .config("spark.executor.cores","8")\
            .config("spark.sql.session.timeZone", "UTC")\
            .config("spark.submit.deployMode", "client")\
            .config("spark.dynamicAllocation.enabled", "false")\
            .config("spark.sql.sources.partitionOverwriteMode", "dynamic")\
            .config("spark.sql.parquet.binaryAsString", "true")\
            .config("spark.sql.shuffle.partitions", 1500)\
            .config("spark.sql.files.maxPartitionBytes", 1073741824)\
            .config("spark.debug.maxToStringFields", 100)\
            .config("spark.port.maxRetries", 100)\
            .config("spark.executor.instances", 150)\
            .config("spark.sql.autoBroadcastJoinThreshold",-1)\
            .config("spark.sql.parquet.mergeSchema", "false")\
            .config("spark.sql.legacy.timeParserPolicy","LEGACY")\
            .config("spark.databricks.repl.allowedLanguages","sql,python,r")\
            .config("spark.databricks.cluster.profile","serverless")\
            .config("spark.databricks.delta.preview.enabled","true")\
            .config("spark.ui.showConsoleProgress", "false")\
            .getOrCreate()
spark.conf.set("spark.sql.parquet.binaryAsString", "true")
spark.sparkContext.setLogLevel('WARN')


os.chdir("/home/cdsw/customer360/")
# NOTE:
# pull code & check out manual on terminal

sys.path.append(os.path.abspath("/home/cdsw/customer360/src"))
sys.path.append(os.path.abspath("/home/cdsw/customer360/"))

import customer360.run as run
import importlib
import pandas as pd
import pytz
import time
from datetime import datetime

run = importlib.reload(run)
def func(pipeline_name):
    time_start = pytz.utc.localize(datetime.now()).astimezone(pytz.timezone("Asia/Bangkok"))
    print(pipeline_name," | Start:",time_start)
    run.run_package(pipelines =[pipeline_name])
    time_end = pytz.utc.localize(datetime.now()).astimezone(pytz.timezone("Asia/Bangkok"))
    print(pipeline_name," | Success:",time_end, time_end-time_start)
    return pipeline_name,str(time_start),str(time_end),str(time_end-time_start)

runTime_lists = []
for pipeline in pipeline_domains:
    runTime_lists.append([func(pipeline)])

print('*'*30)
print('*'*30)
#print(runTime_lists)
for i in runTime_lists:
    print(i)