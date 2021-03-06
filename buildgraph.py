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
from emoji import UNICODE_EMOJI, EMOJI_UNICODE

def filterChinese(msg):
    language = None
    try:
        language = detect(msg)
    except:
        language = "error"
        return True 
    if language in ['zh-cn','ja','ko','fr','vi']:
        return False
        
    return True

def is_emoji(s):
    if s in UNICODE_EMOJI['en']:
        return True
    if s in EMOJI_UNICODE['en']:
        return True
    return False

def removeEmail(text):
    e = "\S*@\S*\s?"
    pattern = re.compile(e)

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
                    emoji = '#??????'
                else:
                    emoji = '#???'  # they look the same, but are actually different
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
    print(type(multi_codepoint_emoji_sorted))
    
    regexword = r'\w+'
    
    multi_codepoint_emoji_sorted.append(regexword)
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

WINSIZE = 2
def buildG(tokenslist, regex, G, emojicntdict):
    """
        Only consider the one within emoji usage
    """
    # nx.set_node_attributes
    for tokens in tokenslist:
        msg = tokens[1:-1]
        if filterChinese(msg) == False:
            continue
        tokens = re.findall(regex, msg)
        tokens = [t.lower() for t in tokens]
        for idx, token in enumerate(tokens):
            if is_emoji(token):
                window = tokens[max(0, idx-WINSIZE):min(len(tokens)-1,idx+WINSIZE)]
                idx = 0
                if token not in G.nodes():
                    G.add_node(token)
                    G.nodes[token]['cnt'] = 1
                else:
                    G.nodes[token]['cnt'] += 1
                for id_, w in enumerate(window):
                    # if (idx == WINSIZE):
                    if id_ == idx:
                        continue
                    # idx += 1
                    if filterChinese(w) == False or w in STOPWORDS:
                        continue
                    if w not in G.nodes():
                        G.add_node(w)
                        G.nodes[w]['cnt'] = 1
                    else:
                        G.nodes[w]['cnt'] += 1
                    if G.has_edge(w, token):
                        # data = G.get_edge_data()
                        G[w][token]["weight"] += 1
                    else:
                        G.add_edge(w,token, weight = 1)


def tokenfunc_frequency(msg):
    tokens = re.findall(all_emoji_regex, msg)
    # print(tokens)
    freqdict = dict()
    for t in tokens:
        if t not in freqdict:
            freqdict[t] = 0
        freqdict[t] + 1
    
    res = []
    for key, val in freqdict.items():
        res.append((key, val))

    return res


def countTokens(commenttokens, issuetokens):
    tokendict = dict()
    for tokens in commenttokens:
        tokens = tokens[2:-2].split()


def getPair(lis):
    pairs = []
    for idx in range(0, len(lis) - 1):
        pairs.append(lis[idx], lis[idx+1])
    
    return pairs




if __name__ == "__main__":

    emoji_entries = emoji_entries_construction()
    all_emoji_regex, emoji_dict = construct_regex(emoji_entries)
    sc_name = "Build Graph"
    sc = SparkContext(conf=SparkConf().setAppName(sc_name))
    sc.addFile("./emoji-test.txt")
    sc.setLogLevel('ERROR')
    spark = SparkSession(sc)
    spark.sparkContext.setLogLevel('WARN')
    raw_root = "/user/hangrui/"
    df_old = spark.read.parquet("/user/hangrui/2018_month_new.parquet")
    df = spark.read.parquet("/user/hangrui/2018_month_new.parquet")
    # df_old = spark.read.parquet("/user/hangrui/2018_day_new.parquet")
    # df = spark.read.parquet("/user/hangrui/2018_day_new.parquet")
    print('the df head is ', df.head())
    print('the original number of rows: ', df.count())


    res = df.select('commentissueid', 'rid').distinct()
    df = df.filter(df.has_emoji==True)
    
    comment = df.filter(df.commentid.isNotNull()&df.commentissueid.isNotNull())
    issue = df.filter(df.issueid.isNotNull())
    comment = comment.select("commentid", "commentissueid", "msg")
    issue = issue.select("issueid", "msg")
    comment = comment.groupby("commentid")\
                        .agg(func.collect_list(func.struct("commentissueid", "msg")).alias("commentmsglist"))
                        
    def getMsg(msgs):
        return msgs[len(msgs)-1]
    def getMsg2(msgstruct):
        return msgstruct[len(msgstruct)-1]["msg"]
    def getid(msgstruct):
        return msgstruct[len(msgstruct)-1]["commentissueid"]

    myudf = func.udf(getMsg)
    myudf2 = func.udf(getMsg2)
    myudf3 = func.udf(getid)


    issue = issue.groupby("issueid").agg(func.collect_list("msg").alias("issuemsglist"))
    issue = issue.select("issueid", myudf("issuemsglist").alias("msg"))
    issue.show()


    comment = comment.select("commentid", myudf2("commentmsglist").alias("msg"))
    comment.show()


    tokenizer = Tokenizer(inputCol="msg", outputCol="tokens")
    comment = tokenizer.transform(comment)
    comment.show()

    tokenizer = Tokenizer(inputCol="msg", outputCol="tokens")
    issue = tokenizer.transform(issue)
    issue.show()
    
    comment.withColumnRenamed("getMsg2(commentmsglist)","msg")
    

    def tokenfunc(msg):
        # if filterChinese(msg) == False:
        #     return []
        
    
        tokens = re.findall(all_emoji_regex, msg)
        # print(msg)
        # print(tokens)
        return tokens

    

    tokenudf = func.udf(tokenfunc)
    
    issue = issue.select("issueid",tokenudf("msg").alias("issuetokens"))
    # issue.withColumnRenamed("issueid","issuetokens")
    comment = comment.select("commentid", tokenudf("msg").alias("commenttokens"))
    comment.withColumnRenamed("commentid","commenttokens")


    
    # commenttokens = comment["commenttokens"]   iss
    commenttokens = list(comment.select('commenttokens').toPandas()['commenttokens'])
    issuetokens = list(issue.select("issuetokens").toPandas()['issuetokens'])
    # comment.write.save("commentokens_.parquet")
    # issue.write.save("issuetokens_.parquet")
    
    emojitokencnt = dict()
    for token in issuetokens:
        msg = token[1:-1]
        token = re.findall(all_emoji_regex, msg)
        for t in token:
            # print(t)
            t = t.lower()
            
            # if is_emoji(t):
            if t not in emojitokencnt:
                emojitokencnt[t] = 0
            emojitokencnt[t] +=1 
    
    for token in commenttokens:
        # print(token)
        # print(type(token))
        msg = token[1:-1]
        token = re.findall(all_emoji_regex, msg)
        
        for t in token:
            # print(t)
            t = t.lower()
            # if is_emoji(t):
            if t not in emojitokencnt:
                emojitokencnt[t] = 0
            emojitokencnt[t] += 1 
    UDK = 0     
    
    pickle.dump(emojitokencnt, open("all_freq_month1.pck", "wb"))
    pickle.dump(commenttokens, open("commenttokensmonth1.pck","wb"))
    pickle.dump(issuetokens, open("issuetokensmonth1.pck", "wb"))

    G = nx.Graph()
    
    buildG(commenttokens, all_emoji_regex, G, emojitokencnt)
    buildG(issuetokens,all_emoji_regex, G, emojitokencnt)
    pickle.dump(G, open("token_graph_month1_include_words.pck", "wb"))
    print(G.nodes)

    

    # calculate the token pair frequency first, get the appearance, based on this calculate graph, 
    # issue.write.format("csv").option("header", "true").save("/user/hangrui/conversation/issuemsg")
    # comment.write.format("csv").option("header", "true").save("/user/hangrui/conversation/commentmsg")


