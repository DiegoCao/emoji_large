from pyspark import SparkConf, SparkContext
import json
import pandas
import re
import langdetect
import io

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

def listtores(tokens):
    ans = []
    if len(tokens['commenttokens']) == 1:
        return []
    else:
        for i in range(len(tokens['commenttokens']) - 1):
            ans.append((tokens['commenttokens'][0], tokens['commenttokens'][1]))


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
    res1 = comment.rdd.flatMap(listtores).reduceByKey(add)

    # res2 = issue.flatMap(listtores).reduceByKey(add)
    res1.show()
    # res2.show()

if __name__ == "__main__":
    getTokens()