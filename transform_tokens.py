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


