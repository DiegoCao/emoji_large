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
    df_old = spark.read.parquet("/user/hangrui/2018_month_new.parquet")
    df = spark.read.parquet("/user/hangrui/2018_month_new.parquet")
    print('the df head is ', df.head())
    print('the original number of rows: ', df.count())


    res = df.select('commentissueid', 'rid').distinct()


    issueemoji = df.groupby('issueid').agg(func.collect_list('emojis').alias("issueemoji"))
    commentemoji = df.groupby('commentid').agg(func.collect_list('emojis').alias("commentemoji"))

    myudf = func.udf(getSetlen)
    issueemoji = issueemoji.select("issueid", myudf("issueemoji").alias("issueemojicnt"))
    issueemoji.show()
    # issueemoji = issueemoji.combineb

    commentemoji = commentemoji.select("commentid", myudf("commentemoji").alias("commentemojicnt"))
    commentemoji.show()
    # issueemoji.write.format("csv").option("header", "true").save("/user/hangrui/new/issuecntemoji")
    # commentemoji.write.format("csv").option("header", "true").save("/user/hangrui/new/commentcntemoji")



    # # df.show()
    # issueemoji.show()
    # commentemoji.show()
    
    # df = df.select('rid', 'issueid', 'commentid').distinct()

    # df = df.alias('a').join(issueemoji.alias('b'), issueemoji.issueid == df.issueid, 'outer')\
    #         .join(commentemoji.alias('c'), commentemoji.commentid==df.commentid, how='outer')\
    #             .select('a.rid', 'a.commentid', 'a.issueid', 'b.issueemojicnt', 'c.commentemojicnt')
    
    # df.write.format("csv").option("header", "true").save("/user/hangrui/new/conversation_new")
    # print('the row number is :', df.count())




    # df = df.filter(df.commentid.isNotNull()&df.commentissueid.isNotNull())
    # dfissue = df_old.filter(df_old.issueid.isNotNull())

    
    # def sorter(l):
    #     res = sorted(l, key=operator.itemgetter(0))
    #     return [item[1] for item in res], [item[2] for item in res]

    # sort_udf = func.udf(sorter)

    # dfi = dfissue.groupby('issueid')\
    #     .agg(func.collect_list(func.struct("created_time", "has_emoji", "msg"))\
    #     .alias("templist"))

    # # # w = Window.partitionby()
    # # dfi = dfi.groupby('issueid')\
    # #     .agg(func.collect_list(func.struct("created_time", "has_emoji"))).withColumnRenamed("issueid", "templist")
    
    
    # dfi.show()
    # dfi = dfi.select("issueid", sort_udf("templist").alias("comment_list", "issuemsg")) 
        
    # # dfi.show()

    # dfci = df.groupby('commentissueid')\
    #         .agg(func.collect_list(func.struct("created_time", "has_emoji","msg"))\
    #         .alias("templist"))
    
    # dfci = dfci.select("commentissueid", sort_udf("templist").alias("comment_lis", "commentmsg"))

    # dfnew = dfci.join(dfi, dfi.issueid==dfci.commentissueid, 'outer')
    # par(dfnew.head())
    # dfnew.write.format("csv").option("header", "true").save("/user/hangrui/new/conversation_msg")

    # # ??

    # # df.show(