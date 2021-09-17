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
        
    return len(eset)/len(lis)

import pyspark.sql.functions as func

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

    df_event_cnt = df_old.groupby('rid').count().withColumnRenamed('count(rid)', 'repoeventcnt')
    df_event_cnt.show()
    df_ads = df_old.groupby('rid').agg(countDistinct("aid").alias("repouserscnt"), countDistinct('prid').alias('repopids'), countDistinct('issueid').alias('repoissueids'), countDistinct('commentid').alias('repocommentids'))


    df_ads.show()



    df = df.filter(df.has_emoji == True)
    df_comment = df.filter(df.commentid.isNotNull()).groupby("commentid").agg(func.collect_list('emojis').alias('comment_emojis'))
    df_issue = df.filter(df.issueid.isNotNull()&df.commentid.isNull()).groupby("issueid").agg(func.collect_list('emojis').alias('issue_emojis'))
    df_pr = df.filter(df.prid.isNotNull()).groupby("prid").agg(func.collect_list('emojis').alias('pr_emojis'))

    udf_ = udf(udffilter, IntegerType())

    commentdf = df_comment.withColumn("commentemojicnt", udf_("comment_emojis"))
    selected_comment = commentdf.select('commentid', 'commentemojicnt')
    prdf = df_pr.withColumn("premojicnt", udf_("pr_emojis"))
    selected_pr = prdf.select("prid", 'premojicnt')
    issuedf = df_issue.withColumn("issueemojicnt", udf_("issue_emojis"))
    selected_issue = issuedf.select("issueid", "issueemojicnt")


    dfmap = df.select("rid", "aid", "prid", "issueid", "commentid").distinct()

    dfmap.createOrReplaceTempView("DFMAP")
    selected_issue.createOrReplaceTempView("SISSUE")
    selected_pr.createOrReplaceTempView("SPR")
    selected_comment.createOrReplaceTempView("SCOMMENT")
    selected_comment.show()
    res = spark.sql("""select * from DFMAP d, SISSUE i, SPR p, SCOMMENT c 
                    left join 
                # where (d.issueid == i.issueid and d.commentid == null ) and 
                # d.prid == p.prid and 
                # d.commentid == c.commentid
            """)    
    res = dfmap.join(selected_pr, selected_pr.prid==dfmap.prid, 'outer')\
                .join(selected_comment, selected_comment.commentid==dfmap.commentid, 'outer')\
                    .join(selected_issue, selected_issue.issueid==dfmap.issueid, 'outer')

    res.show()
    res.createOrReplaceTempView("RES")



    dfcount = res.groupby("rid").agg(sum("issueemojicnt").alias("issueemojitotal"), sum("commentemojicnt").alias("commentemojitotal"), sum("premojicnt").alias("premojitotal"))




    res.show()

    

    # comment_emoji = df.