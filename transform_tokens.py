from pyspark import SparkConf, SparkContext
import json
import pandas
import re
import langdetect
import io
from operator import add
import nltk
from nltk.corpus import stopwords

from collections import namedtuple, Counter
from langdetect import detect

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, countDistinct
from pyspark.sql.types import IntegerType, FloatType

import pyspark.sql.functions as func
import pickle
import networkx as nx

from pyspark.ml.feature import HashingTF, IDF, Tokenizer
from nltk.tokenize import word_tokenize
import nltk

nltk.download('stopwords')

from nltk.corpus import stopwords
STOPWORDS = set(stopwords.words('english'))
# import enchant
from buildgraph import emoji_entries_construction, construct_regex

def listtores(row):
    ans = []

    if len(row['commenttokens']) == 1:
        return []

    for i in range(len(['commenttokens']) - 1):
        ans.append((tokens['commenttokens'][i][0], tokens['commenttokens'][i][1]))
    
    return ans

def listtotry(tokens):
    tokens = [2]
    return tokens

def getTokens():
    emoji_entries = emoji_entries_construction()
    all_emoji_regex, emoji_dict = construct_regex(emoji_entries)
    sc_name = "Build Graph"
    sc = SparkContext(conf=SparkConf().setAppName(sc_name))
    sc.addFile("./emoji-test.txt")
    sc.setLogLevel('ERROR')
    spark = SparkSession(sc)
    spark.sparkContext.setLogLevel('WARN')
    raw_root = "/user/hangrui/"
    comment = spark.read.parquet("/user/hangrui/commentokens.parquet")
    issue = spark.read.parquet("/user/hangrui/issuetokens.parquet")
    issue.show()

    comment.show()
    commentrdd = comment.rdd
    print(commentrdd.take(5))

    res1 = commentrdd.flatMap(listtotry).reduceByKey(add)
    print(res1.take(5))
    # dfres1 = res1.toDF()
    # res2 = issue.flatMap(listtores).reduceByKey(add)
    # res1.show()
    # res2.show()

if __name__ == "__main__":
    getTokens()