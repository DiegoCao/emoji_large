import pandas
from pyspark import SparkConf, SparkContext
import json
import re
import io
from collections import namedtuple, Counter
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
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
        
    return len(eset)/len(lis)



if __name__ == "__main__":
    sc_name = "process"
    sc = SparkContext(conf=SparkConf().setAppName(sc_name))
    sc.addFile("./emoji-test.txt")
    sc.setLogLevel('WARN')
    spark = SparkSession(sc)
    spark.sparkContext.setLogLevel('WARN')
    raw_root = "/user/hangrui/"
    df_old = spark.read.parquet("/user/hangrui/2018_year_pid_v2.parquet")
    df = spark.read.parquet("/user/hangrui/2018_year_pid_v2.parquet")

    df = df.filter(df.has_emoji == True)
    df_comment = df.filter(df.commentid.isNotNull())
    df_issue = df.filter(df.issueid.isNotNull()&df.commentid.isNull())
    df_pr = df.filter(df.prid.isNotNull())
    
    udf_ = udf(getSetlen, IntegerType())

    commentdf = df_comment.withColumn("commentemojicnt", udf_("comment_emojis"))
    selected_comment = commentdf.select('commentid', 'commentemojicnt')
    prdf = df_pr.withColumn("premojicnt", udf_("pr_emojis"))
    
    selected_pr = prdf.select("prid", 'premojicnt')

    issuedf = df_issue.withColumn("issueemojicnt", udf_("issue_emojis"))
    selected_issue = issuedf("issueid", "issueemojicnt")

    dfmap = df.select("rid", "aid", "prid", "issueid", "commentid")

    dfmap.createOrReplaceTempView("DFMAP")
    selected_issue.createOrReplaceTempView("SISSUE")
    selected_pr.createOrReplaceTempView("SPR")
    selected_comment.createOrReplaceTempView("SCOMMENT")
    res = spark.sql("""select * from DFMAP d, SISSUE i, SPR p, SCOMMENT c
                where (d.issueid == i.issueid and d.commentid == null ) and 
                d.prid == p.prid and 
                d.commentid == c.commentid
            """)    
    res.show()

    

    # comment_emoji = df.