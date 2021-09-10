
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



def line2jsons(line):
    """
    This function translate each line (which may contain more than one tweets into a list of json objects.
    """

    
    jsons = json.loads(line)
    print('THE OTHER TYPE IS:', type(jsons))
    print('the return res', jsons)
    return jsons
def emojiFilter(ejson):
    if ejson["emojiscnt"] == 0:
        return False
    return True

def keyword_text_filter(t_json):
    """
    Filtering tweets based on regular expression and hashtags.
    """
    # if 'type' in t_json:
    #     return True
    # return False
    print('the fileter in ', type(t_json))
    print('t_json', t_json)
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
    print(emoji_json)
    dic = {}
    emoji_json = json.loads(emoji_json)
    dic['aid'] = emoji_json['actor']['id']
    # dic['id'] = emoji_json['id']
    # dic['payload'] = emoji_json['payload']
    pay_load = emoji_json['payload']
    # dic['type'] = emoji_json['type']
    
    dic['rid'] = emoji_json['repo']['id']
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
    if msg == None:
        emojis = []
    else:
        emojis = re.findall(regex, msg)

    dic['emojis'] = emojis
    dic['event'] = dtype
    dic["msg"] = msg
    dic['emojiscnt'] = len(emojis)
    
    return dic

def printFunc(x):
    print(x)
    print('the type of x', type(x))

def applyFunc1(x):
    emojilis = x.emojis
    cnt = 0
    for i in emojilis:
        cnt += len(emojilis)
    return cnt
    

def main():
    

    sc_name = "Filter Test"
    sc = SparkContext(conf=SparkConf().setAppName(sc_name))
    output = "/user/hangrui/"
    
    sc.addFile("emoji-test.txt")
    spark = SparkSession(sc)
    emoji_entries = emoji_entries_construction()
    regex, emoji_dict = construct_regex(emoji_entries)
	
    
    files = sc.textFile("data/2018*.json.gz")
    text = files.map(lambda line: extract_emoji_hashtag(line, regex)).filter(lambda t: emojiFilter(t))
    text.saveAsTextFile("2018_data_cnt")
    
    # text.foreach(printFunc)
    # text = files.flatMap(line2jsons).filter(lambda t: keyword_text_filter(t)).map(lambda line: extract_emoji_hashtag(line, regex))
    df = text.toDF()
    
    df.show()
    
    newdf = df.groupBy("rid").avg("emojiscnt")
    # print('The cnt of ')
    newdf.show()
    newdf.saveAsTextFile("2018_emojis_cnt_1")
    
    
    # df.show()
    # print(type(text))
    
    # print('the type is')
    # newtext = text.map(json.dumps)
    # print(type(newtext))
    # for r in newtext.collect():
    #     print(r)
    # print('the text is', text)
    sc.stop()

    
if __name__ == '__main__':
    main()


