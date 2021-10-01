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
    # df_event_cnt.write.format("csv").option("header", "true").save("/user/hangrui/new/dfeventcnt")
    dff1 = df_old.filter((df_old.type=="PushEvent") | (df_old.type== "IssuesEvent") | (df_old.type=="IssueCommentEvent") | (df_old.type=="PullRequestReviewCommentEvent") | (df_old.type=="PullRequestEvent")｜(df_old.type=="CommitCommentEvent"))
    
    # dfusers = df_old.groupby('rid').a
    dfusers = dff1.groupby('rid').agg(countDistinct("aid").alias("repouserscnt"))
    dfusers.write.format("csv").option("header", "true").save("/user/hangrui/new/dfuserswork")

    dfall = df_old.groupby('rid').agg(countDistinct("prid").alias("repoprcnt"), countDistinct("issueid").alias("repoissuecnt"), countDistinct("commentid").alias("repocommentcnt"))
    dfall = dfall.groupby('rid').agg(func.sum(dfall.repoprcnt+dfall.repoissuecnt + dfall.repocommentcnt).alias("allposts"))

    # dfissues = df_old.groupby('rid').agg(countDistinct("issueid").alias("repoissuecnt"))
    # dfcomment = df_old.groupby('rid').agg(countDistinct("commentid").alias("repocommentcnt"))

    # dfgen = spark.sql("""select * from DFMAP d
    #                 left outer join SISSUE i on i.issueid == d.issueid and
    #                 left outer join SPR p on p.prid == d.prid and
    #                 left outer join SCOMMENT c on c.commentid == d.commentid
    #                 """)

    # dfpids.write.format("csv").option("header", "true").save("/user/hangrui/new/repoprcnt")
    # dfissues.write.format("csv").option("header", "true").save("/user/hangrui/new/repoissuescnt")
    # dfcomment.write.format("csv").option("header", "true").save("/user/hangrui/new/repocommentcnt")


    # df_ads.show()



    df = df.filter(df.has_emoji == True)
    
    dffilter = df.groupby('rid').agg(countDistinct("prid").alias("repoprcnt"), countDistinct("issueid").alias("repoissuecnt"), countDistinct("commentid").alias("repocommentcnt"))
    dffilter = dffilter.groupby('rid').agg(func.sum(dffilter.repoprcnt+dffilter.repoissuecnt + dffilter.repocommentcnt).alias("filterposts"))
    # dffilter.write.format("csv").option("header", "true").save("/user/hangrui/new/repofilterposts")
    # dfall.write.format("csv").option("header", "true").save("/user/hangrui/new/repoallposts")


    df_comment = df.filter(df.commentid.isNotNull()).groupby("commentid").agg(func.collect_list('emojis').alias('comment_emojis'))
    df_issue = df.filter(df.issueid.isNotNull()&df.commentid.isNull()).groupby("issueid").agg(func.collect_list('emojis').alias('issue_emojis'))
    df_pr = df.filter(df.prid.isNotNull()).groupby("prid").agg(func.collect_list('emojis').alias('pr_emojis'))



    udf_ = udf(getSetlen, IntegerType())

    

    commentdf = df_comment.withColumn("commentemojitype", udf_("comment_emojis"))
    selected_comment = commentdf.select('commentid', 'commentemojitype')
    prdf = df_pr.withColumn("premojitype", udf_("pr_emojis"))
    selected_pr = prdf.select("prid", 'premojitype')
    issuedf = df_issue.withColumn("issueemojitype", udf_("issue_emojis"))
    selected_issue = issuedf.select("issueid", "issueemojitype")




    dfmap = df.select("rid", "aid", "prid", "issueid", "commentid").distinct()

    dfmap.createOrReplaceTempView("DFMAP")
    selected_issue.createOrReplaceTempView("SISSUE")
    selected_pr.createOrReplaceTempView("SPR")
    selected_comment.createOrReplaceTempView("SCOMMENT")
    # selected_comment.show()

   
    res = dfmap.alias('a').join(selected_pr.alias('b'), selected_pr["prid"]== dfmap["prid"], 'outer')\
                .join(selected_comment.alias('c'), selected_comment["commentid"]==dfmap["commentid"], 'outer')\
                    .join(selected_issue.alias('d'), selected_issue["issueid"]==dfmap["issueid"], 'outer')\
                        .select('a.rid', 'a.aid', 'a.prid', 'a.issueid', 'a.commentid', 'b.premojitype', 'c.commentemojitype', 'd.issueemojitype')
                        # .select($'selected_pr')

    res.show()
    res.createOrReplaceTempView("RES")



    # dfcount = res.na.fill(0).groupby("rid").agg(func.sum(res.commentemojicnt+res.premojicnt+res.issueemojicnt).alias('totalcnt'))

    res.write.format("csv").option("header", "true").save("/user/hangrui/new/resvarnew")
    # dfcount = dfcount.join(df_event_cnt, df_event_cnt['rid']==dfcount['rid'])
    # dfcount = dfcount.join(dfusers, dfusers['rid']==dfcount['rid'])

    # # dfcount
    # #  
    # dfcount.show()

    # res = spark.sql("""select * from DFMAP d
    #                 left outer join SISSUE i on i.issueid == d.issueid and
    #                 left outer join SPR p on p.prid == d.prid and
    #                 left outer join SCOMMENT c on c.commentid == d.commentid
    #                 """)
    # res.show()
    # dfcount.

    

    # comment_emoji = df.