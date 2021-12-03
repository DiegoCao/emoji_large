import pandas
from pyspark import SparkConf, SparkContext
import json
import re
import io
from collections import namedtuple, Counter
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, countDistinct
from pyspark.sql.types import IntegerType, FloatType

def udffilter(x):
    
    maxval = -1
    emoji_set = set()
    for i in x:
        for j in i:
            emoji_set.add(j)
    
    return len(emoji_set)

def getSetlen(lis):
    eset = set()

    for arr in lis:
        for emoj in arr:
            eset.add(emoj)
        
    return len(eset)
    
import numpy as np
def getSetVar(lis):
    edict = dict()
    for arr in lis:
        for emoj in arr:
            if emoj not in edict:
                edict[emoj] = 0
            edict[emoj] += 1
    
    return np.var(list(edict.values()))

import pyspark.sql.functions as func
from pyspark.sql import Window
import operator


if __name__ == "__main__":
    sc_name = "Conversation Issue"
    sc = SparkContext(conf=SparkConf().setAppName(sc_name))
    sc.addFile("./emoji-test.txt")
    sc.setLogLevel('ERROR')
    spark = SparkSession(sc)
    spark.sparkContext.setLogLevel('WARN')
    raw_root = "/user/hangrui/"
    df_old = spark.read.parquet("/user/hangrui/2018_parquet_v3.parquet")
    df = spark.read.parquet("/user/hangrui/2018_parquet_v3.parquet")
    print('the df head is ', df.head())
    print('the original number of rows: ', df.count())


    res = df.select('commentissueid', 'rid').distinct()


    issueemoji = df.groupby('issueid').agg(func.collect_list('emojis').alias("issueemoji"))
    commentemoji = df.groupby('commentid').agg(func.collect_list('emojis').alias("commentemoji"))

    issueemoji.show()

