data_dir = "./data/"
import os
import pyspark
import emoji


from collections import namedtuple, Counter
import io
import re
from pyspark import SparkConf, SparkContext
import pickle
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType,BooleanType,DoubleType


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
    with io.open('./emoji/emoji-test.txt', 'rt', encoding="utf8") as file:
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



def line2jsons(line):
    """
    This function translate each line (which may contain more than one tweets into a list of json objects.
    """
    pat = r'\s*(\{"created_at".*?\})\s*(?=\s*\{"created_at")'
    lst = re.split(pat, line)
    jsons = []
    for json_string in lst:
        try:
            j = json.loads(json_string.strip())
            jsons.append(j)
        except ValueError:
            continue
    return jsons



def keyword_text_filter(twitter_json):
    """
    Filtering tweets based on regular expression and hashtags.
    """
    if 'text' in twitter_json:
        return True
    return False


def extract_emoji_hashtag(twitter_json, regex):
    dic = {}
    dic['emoji'] = re.findall(regex, twitter_json['text'])
    dic['id'] = twitter_json['id']
    dic['screen_name'] = twitter_json['user']['screen_name']
    return dic

def extractNewMsg(pay_load, dtype, user_emoji_map, regex, aid):
    
    msg = ""
    if pay_load['commit']['message'] is not None:
        for commit in pay_load['commits']:
            msg += commit['message']
    
    if pay_load['comment']['body'] is not None:
        msg += commit['message']
          
            
    
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
    
    print('the msg is ', msg)
    return msg


def getrows(df, rownums=None):
    return df.rdd.zipWithIndex().filter(lambda x: x[1] in rownums).map(lambda x: x[0])
    

def setRidMap(pay_load, repo_Com, dtype, aid, ridd):
    if dtype == "PullRequestEvent" or dtype == "PullRequestReviewCommentEvent":
        bid = pay_load['pull_request']['base']['user']['id']
        if rid not in repo_Com:
            repo_Com[rid] = set()
        repo_Com[rid].add(aid)
        repo_Com[rid].add(bid)

import logging

if __name__ == "__main__":
    import sys
    sc = SparkContext(conf=SparkConf().setAppName("Test Emoji"))
    sc.addFile("./emoji/emoji-test.txt")
    spark = SparkSession.builder \
        .appName("SparkByExamples") \
        .getOrCreate()
        
    emoji_entries = emoji_entries_construction()
    regex, emoji_dict = construct_regex(emoji_entries)
	
    # spark.addFile("./emoji/emoji-test.txt")
    # spark.setLogLevel('WARN')
    sample_file = ["./data/2018-01-01-0.json.gz", "./data/2018-01-01-1.json.gz",  "./data/2018-01-01-2.json.gz"]
    all_file = "./data/2018-*.json.gz"

    df = spark.read.json(all_file)
    spark.sparkContext.setLogLevel("ERROR")
    df.printSchema()
    
    length = df.count()
    print(length)
    all_ids = set()
    user_emoji_map = dict()
    repo_id_map = dict()
    dtype_map = dict()
    
    ITERNUM =  200000
    iter = 0
    while iter*ITERNUM < length:
        print('Processing iter ', iter)
        rownums = []
        for i in range(0, ITERNUM):
            rownums.append(i + iter*ITERNUM)
            if (i + iter*ITERNUM) >= length - 1:
                break
            
        dfrows = getrows(df, rownums=rownums).collect()
            
        for dfrow in dfrows:
            # print(dfrow)
            # print(type(dfrow))
            # print(dfrow['actor'])
            aid = dfrow['actor']['id']
            rid = dfrow['repo']['id']
            dtype = dfrow['type']
            if dtype not in dtype_map:
                dtype_map[dtype] = 1
            else:
                dtype_map[dtype] += 1
            all_ids.add(aid)
            pay_load = dfrow['payload']
            setRidMap(pay_load, repo_id_map, dtype, aid, rid)
            extractMsg(pay_load, dtype, user_emoji_map, regex, aid)
                # dtype = atypes[i][0]
                # ('the dtype is, ', dtype)
    pickle.dump(repo_id_map, open('./repo_actoridmap_v2.txt', 'wb'))
    pickle.dump(user_emoji_map, open('./user_emoji_map.txt', 'wb'))
    sc.stop()
    
