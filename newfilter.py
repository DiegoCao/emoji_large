from pyspark import SparkConf, SparkContext
import json
import re
import io
from collections import namedtuple, Counter
from pyspark.sql import SparkSession


def get_ranges(nums):
    """Reduce a list of integers to tuples of local maximums and minimums.

    :param nums: List of integers.
    :return ranges: List of tuples showing local minimums and maximums
    """
    nums = sorted(nums)
    lows = [nums[0]]
    highs = []
    if nums[1] - nums[0] > 1:
        highs.append(nums[0])
    for i in range(1, len(nums) - 1):
        if (nums[i] - nums[i - 1]) > 1:
            lows.append(nums[i])
        if (nums[i + 1] - nums[i]) > 1:
            highs.append(nums[i])
    highs.append(nums[-1])
    if len(highs) > len(lows):
        lows.append(highs[-1])
    return [(l, h) for l, h in zip(lows, highs)]
	

def emoji_entries_construction():
    with io.open('emoji-test.txt', 'rt', encoding="utf8") as file:
        emoji_raw = file.read()
    EmojiEntry = namedtuple('EmojiEntry', ['codepoint', 'status', 'emoji', 'name', 'group', 'sub_group'])
    emoji_entries = []

    # The following code goes through lines one by one,
    # extracting the information that is needed,
    # and appending each entry to emoji_entries which will be a list containing all of them.
    # I have annotated the code with some comments, and below elaborated a little more to clarify.

    for line in emoji_raw.splitlines()[32:]:  # skip the explanation lines
        if line == '# Status Counts':  # the last line in the document
            break
        if 'subtotal:' in line:  # these are lines showing statistics about each group, not needed
            continue
        if not line:  # if it's a blank line
            continue
        if line.startswith('#'):  # these lines contain group and/or sub-group names
            if '# group:' in line:
                group = line.split(':')[-1].strip()
            if '# subgroup:' in line:
                subgroup = line.split(':')[-1].strip()
        if group == 'Component':  # skin tones, and hair types, skip, as mentioned above
            continue
        if re.search('^[0-9A-F]{3,}', line):  # if the line starts with a hexadecimal number (an emoji code point)
            # here we define all the elements that will go into emoji entries
            codepoint = line.split(';')[0].strip()  # in some cases it is one and in others multiple code points
            status = line.split(';')[-1].split()[0].strip()  # status: fully-qualified, minimally-qualified, unqualified
            if line[-1] == '#':
                # The special case where the emoji is actually the hash sign "#". In this case manually assign the emoji
                if 'fully-qualified' in line:
                    emoji = '#️⃣'
                else:
                    emoji = '#⃣'  # they look the same, but are actually different
            else:  # the default case
                emoji = line.split('#')[-1].split()[0].strip()  # the emoji character itself
            if line[-1] == '#':  # (the special case)
                name = '#'
            else:  # extract the emoji name
                name = '_'.join(line.split('#')[-1][1:].split()[1:]).replace('_', ' ')
            templine = EmojiEntry(codepoint=codepoint,
                                  status=status,
                                  emoji=emoji,
                                  name=name,
                                  group=group,
                                  sub_group=subgroup)
            emoji_entries.append(templine)

    return emoji_entries


def construct_regex(emoji_entries):
    multi_codepoint_emoji = []

    for code in [c.codepoint.split() for c in emoji_entries]:
        if len(code) > 1:
            # turn to a hexadecimal number filled to 8 zeros e.g: '\U0001F44D'
            hexified_codes = [r'\U' + x.zfill(8) for x in code]
            hexified_codes = ''.join(hexified_codes)  # join all hexadecimal components
            multi_codepoint_emoji.append(hexified_codes)

    # sorting by length in decreasing order is extremely important as demonstrated above
    multi_codepoint_emoji_sorted = sorted(multi_codepoint_emoji, key=len, reverse=True)
    # join with a "|" to function as an "or" in the regex
    multi_codepoint_emoji_joined = '|'.join(multi_codepoint_emoji_sorted)

    single_codepoint_emoji = []

    for code in [c.codepoint.split() for c in emoji_entries]:
        if len(code) == 1:
            single_codepoint_emoji.append(code[0])

    single_codepoint_emoji_int = [int(x, base=16) for x in single_codepoint_emoji]
    single_codepoint_emoji_ranges = get_ranges(single_codepoint_emoji_int)
    single_codepoint_emoji_raw = r''  # start with an empty raw string
    for code in single_codepoint_emoji_ranges:
        if code[0] == code[1]:  # in this case make it a single hexadecimal character
            temp_regex = r'\U' + hex(code[0])[2:].zfill(8)
            single_codepoint_emoji_raw += temp_regex
        else:
            # otherwise create a character range, joined by '-'
            temp_regex = '-'.join([r'\U' + hex(code[0])[2:].zfill(8), r'\U' + hex(code[1])[2:].zfill(8)])
            single_codepoint_emoji_raw += temp_regex
    all_emoji_regex = re.compile(multi_codepoint_emoji_joined + '|' + r'[' + single_codepoint_emoji_raw + r']')
    emoji_dict = {x.emoji: x for x in emoji_entries}
    return all_emoji_regex, emoji_dict



# def line2jsons(line):
#     """
#     This function translate each line (which may contain more than one tweets into a list of json objects.
#     """

    
#     jsons = json.loads(line)
#     return jsons


def keyword_text_filter(t_json):
    """
    Filtering tweets based on regular expression and hashtags.
    """
    # if 'type' in t_json:
    #     return True
    # return False
    if len(t_json["emojis"]) == 0:
        return False
    return True

def extractMsg(pay_load, dtype, user_emoji_map, regex, aid):
    msg = ""
    if dtype == "PushEvent":
        msg = ""
        for commit in pay_load['commits']:
            msg += commit['message']
    elif dtype == "CreateEvent":
        msg = pay_load['description']
    elif dtype == "IssuesEvent":
                # json_formatted_str = json.dumps(data, indent=2)
                # print(json_formatted_str)
        msg = pay_load['issue']['title']
        msg = pay_load['issue']['body']
    elif dtype == "IssueCommentEvent":
        msg = pay_load['comment']['body']
    elif dtype == "PullRequestEvent":
        title = pay_load['pull_request']['title']
        body = pay_load['pull_request']['body']
        if title == None:
            title = ""
        elif body == None:
            body = ""
            msg = title + body
        elif dtype == "PullRequestReviewEvent":
            msg = pay_load['review']['body']
        elif dtype == "PullRequestReviewCommentEvent":
            msg = pay_load['comment']['body']
        elif dtype == "CommitCommentEvent":
            msg = pay_load['comment']['body']
        elif dtype == "ReleaseEvent":
            msg = pay_load['release']['body']
        elif dtype == "ForkEvent":
            pass
        emojis = re.findall(regex, msg)
        if len(emojis) > 0:
            print('found!')
        if aid not in user_emoji_map:
            user_emoji_map[aid] = set()
        for e in emojis:
            user_emoji_map[aid].add(e)
    
    return msg


def extract_emoji_hashtag(emoji_json, regex):
    # dic = {}
    # dic['emoji'] = re.findall(regex, twitter_json['text'])
    # dic['id'] = twitter_json['id']
    # dic['screen_name'] = twitter_json['user']['screen_name']
    print('THE TYPE IS ', type(emoji_json))
    dic = {}
    emoji_json = json.loads(emoji_json)
    dic['aid'] = emoji_json['actor']['id']
    dic['id'] = emoji_json['id']
    # dic['payload'] = emoji_json['payload']
    pay_load = emoji_json['payload']
    dic['type'] = emoji_json['type']
    dic['public'] = emoji_json['public']
    dic['rid'] = emoji_json['repo']['id']
    dic['created_time'] = emoji_json["created_at"]
    
    dtype = emoji_json['type']
    
    msg = ""
    
    if dtype == "PushEvent":
        msg = ""
        for commit in pay_load['commits']:
            msg += commit['message']
        
    elif dtype == "CreateEvent":
        msg = pay_load['description']
    elif dtype == "IssuesEvent":
                # json_formatted_str = json.dumps(data, indent=2)
                # print(json_formatted_str)
        msg = pay_load['issue']['body']
        dic["issueid"] = pay_load['issue']['id']
    elif dtype == "IssueCommentEvent":
        msg = pay_load['comment']['body']
        # dic["issueid"] = pay_load['issue']['id']
        dic["commentid"]=pay_load['comment']['id']

    elif dtype == "PullRequestEvent":
        title = pay_load['pull_request']['title']
        body = pay_load['pull_request']['body']
        dic['prid'] = pay_load['pull_request']['id']
        if title == None:
            title = ""
        elif body == None:
            body = ""
            msg = title + body
    elif dtype == "PullRequestReviewEvent":
        msg = pay_load['review']['body']
        
    elif dtype == "PullRequestReviewCommentEvent":
        msg = pay_load['comment']['body']
        dic['commentid']=pay_load['comment']['id']
    elif dtype == "CommitCommentEvent":
        msg = pay_load['comment']['body']
        dic["commentid"]=pay_load['comment']['id']
    elif dtype == "ReleaseEvent":
        msg = pay_load['release']['body']
        
    elif dtype == "ForkEvent":
        pass
    if msg is None:
        emojis = []
        msg = ""
    else:
        emojis = re.findall(regex, msg)
        
    dic['has_emoji'] = False
    if len(emojis) > 0:
        dic['has_emoji'] = True
        
    dic['msg'] = msg
    dic['emojis'] = emojis
    
    return dic

import os

def main():
    
    raw_root = "/user/hangrui/data/"
    output = "/user/hangrui/extracted_emoji_2018_year_v2"

    sc_name = "Filter New into Parquet"
    sc = SparkContext(conf=SparkConf().setAppName(sc_name))
    sc.addFile("emoji-test.txt")
    spark = SparkSession(sc)
    emoji_entries = emoji_entries_construction()
    regex, emoji_dict = construct_regex(emoji_entries)
	
 
    files = sc.textFile(raw_root + "2018-*.json.gz")
    text = files.map(lambda line: extract_emoji_hashtag(line, regex))
    df = text.toDF()
    df.write.save('2018_year_pid_v2.parquet')
    print('sucessfully saved')

    sc.stop()


def line2json(line):
    # str_ = line.replace("\'", "\"")
    str_ = line
    res = {}
    if str_ is None:
        return res
    else:
        res = json.loads(str_, strict=False)
    return res

def filter2(line):
    if line is None:
        return False
    return True
def testsmall():
    
    print('the test of small')

    sc_name = "Small Test"
    sc = SparkContext(conf=SparkConf().setAppName(sc_name))
    sc.addFile("emoji-test.txt")
    spark = SparkSession(sc)
    emoji_entries = emoji_entries_construction()
    regex, emoji_dict = construct_regex(emoji_entries)
	
    raw_root = "/user/hangrui/data/"
    output = "/user/hangrui/"    
    files = sc.textFile(raw_root + "2018-01-01*.json.gz")
    text = files.map(lambda line: extract_emoji_hashtag(line, regex))
    df = text.toDF()
    df.write.save('testday_v2.parquette')
    print('sucessfully saved')

    sc.stop()

def printHelper(x):
    print(x)
def analysis():
    
    sc_name = "process"
    sc = SparkContext(conf=SparkConf().setAppName(sc_name))
    
    sc.addFile("emoji-test.txt")
    spark = SparkSession(sc)
    raw_root = "/user/hangrui/"
    # files = sc.textFile("/user/hangrui/testday.parquet").filter(lambda t:filter2(t)).map(lambda t:line2json(t))
    files = spark.read.parquet("/user/hangrui/testday.parquet")
    
    files.show()

from pyspark.sql.functions import udf
import pyspark.sql.functions as func
from pyspark.sql.types import IntegerType, FloatType
    
def calLen(lis):
    return len(lis)

def getSetlen(lis):
    eset = set()

    for arr in lis:
        for emoj in arr:
            eset.add(emoj)
        
    return len(eset)/len(lis)

import numpy as np
def getSetvar(lis):
    edict = dict()
    for arr in lis:
        for emoj in arr:
            if emoj not in edict:
                edict[emoj] = 0
            
            edict[emoj] += 1
    res = []
    for val in edict.values():
        res.append(int(val))
    array = np.array(res, dtype=np.int32)
    return np.var(array)

def getgroupJoin(df1, df2, dname, newname):
    res = df1.select('rid', 'aid').distinct
    joindf = df1.join(df2, df1.aid == df2.aid)
    joindf = joindf.groupby('rid').agg(func.mean(dname).alias(newname))
    return joindf

def getWorkratio(lis):
    edict = dict()
    for arr in lis:
        for event in arr:
            if event not in edict:
                edict[event] = 0
            
            edict[event] += 1
    res = []
    for val in edict.values():
        res.append(int(val))
    array = np.array(res, dtype=np.int32)
    return np.var(array)


    
def udffilter(x):
    
    maxval = -1
    emoji_set = set()
    for i in x:
        for j in i:
            emoji_set.add(j)
    
    return len(emoji_set)

from pyspark.sql.functions import countDistinct

def analysis_DF():
    
    sc_name = "process"
    sc = SparkContext(conf=SparkConf().setAppName(sc_name))

    sc.addFile("./emoji-test.txt")
    sc.setLogLevel('WARN')
    spark = SparkSession(sc)
    spark.sparkContext.setLogLevel('WARN')
    raw_root = "/user/hangrui/"
    df = spark.read.parquet("/user/hangrui/2018_year_pid.parquet")

    # groupdf = df.groupby('rid').agg(countDistinct('prid'))
    

    # piddf = df.select('prid', 'has_emoji').distinct()
    # final_df = df.groupby('rid').agg(countDistinct('prid'), countDistinct('issueid'), countDistinct('commentid'))
    
    # groupdf = piddf.join(groupdf,groupdf.prid==piddf.prid, 'outer')
    # groupdf.show()

    # final_df.write.format("csv").option("header", "true").save("/user/hangrui/new/rid_all_distinct")
    
    # df.show()
    # df.write.format("csv").option("header", "true").save("/user/hangrui/new/rid_post_distinct")

    df_old= df.filter(df.has_emoji == True)
    
    df = df_old.filter(df.commentid.isNull()&df.issueid.isNotNull())

    # df.show()
    # # # df.write.save("/user/hangrui/comment_emoji.parquet")
    commentdf = df.groupby('issueid').agg(func.collect_list('emojis').alias('issue_emojis'))


    udf_ = udf(udffilter, IntegerType())

    commentdf = commentdf.withColumn("emojicnt", udf_("issue_emojis"))
    # commentdf.show()
    selected_comment = commentdf.select('issueid', 'emojicnt')

    # df = df_old.filter(df.prid.isNotNull())
    # df.show()
    # prdf = df.groupby('prid').agg(func.collect_list('emojis').alias('pr_emojis'))
    # prdf = prdf.withColumn("emojicnt", udf_("pr_emojis"))
    # prdf.show()
    # selected_pr = prdf.select('emojicnt', 'prid')

    selected_comment.write.format("csv").option("header", "true").save("/user/hangrui/new/issuecnt_csv/")
    # selected_pr.write.format("csv").option("header", "true").save("/user/hangrui/new/pr_cnt")
    
    # selectdf = df_old.select('rid', 'aid', 'commentid', 'prid')
    # selectdf.write.format("csv").option("header", "true").save("/user/hangrui/new/idmap")


    # # df = df.withColumn("num_emojis", udf_("emojis"))
    # # groupdf = df.groupby('id').agg(func.collect_list('emojis').alias('repo_emojis'))
    # # udf3 = udf(getSetvar, FloatType())
    

    # # groupdf = groupdf.withColumn("repo_emoji_var", udf3("repo_emojis"))
    # groupdf.drop("repo_emojis")
    # groupdf.show()
    # groupdf.write.format("csv").option("header", "true").save("/user/hangrui/repo_event_types_var_fix2")
    
    # udf2 = udf(getSetlen, IntegerType())
    # groupdf = groupdf.withColumn("emoji_cnt", udf2("user_emojis"))
    # groupdf = groupdf.drop("user_emojis")
    # joindf = getgroupJoin(df, groupdf, 'emoji_cnt', 'repoavguser_emojis_fix')
    # # # # joindf = joindf.groupBy('rid').agg(func.mean('count').alias('repoavguser_events_fix'))
    # joindf.write.format("csv").option("header", "true").save("/user/hangrui/repoavguser_emojis_fix")
        
    # df2 = groupdf.select('aid','user_avg_emoji')
    # joindf = df1.join(df2, df1.aid == df2.aid, 'outer')
    
    
    # # groupdf.show()
    
    # cnt_aid = df.groupby('rid').agg(func.countDistinct('aid').alias('repo_aids_cnt'))
    
    # event_cnt = df.groupby('rid').count()
    # # cnt_aid.write.format("csv").option("header", "true").save("/user/hangrui/repo_aids_cnt")
    
    # event_cnt.write.format("csv").option("header", "true").save("/user/hangrui/repo_event_cnt")

    
    
# def analysis_large():
    
    
#     sc_name = "process 2"
#     sc = SparkContext(conf=SparkConf().setAppName(sc_name))
    
#     sc.addFile("emoji-test.txt")
#     spark = SparkSession(sc)
#     raw_root = "/user/hangrui/"
#     # files = sc.textFile("/user/hangrui/testday.parquet").filter(lambda t:filter2(t)).map(lambda t:line2json(t))
#     files = spark.read.parquet("/user/hangrui/testday.parquet")
    
#     df = df.groupBy('rid').agg({''})
    sc.stop()
    




    
if __name__ == '__main__':
    analysis_DF()