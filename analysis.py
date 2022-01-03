import re
import pickle
import networkx as nx
import io
from collections import namedtuple, Counter

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



WINSIZE = 2
def buildG(tokenslist, regex, G):
    """
        Only consider the one within emoji usage
    """
    
    for tokens in tokenslist:
        for idx, token in enumerate(tokens):
            # if is_emoji(token):
            window = tokens[max(0, idx-WINSIZE):min(len(tokens)-1,idx+WINSIZE)]
            idx = 0
            if token not in G.nodes():
                G.add_node(token)
            for w in window:
                if (idx == WINSIZE):
                    continue
                idx += 1
                if w not in G.nodes():
                    G.add_node(w)
                G.add_edge(w,token)


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

def plotFreq():
    emoji_freq = None
    with open("emoji_freq.pck", "rb") as fp:
        emoji_freq = pickle.load(fp)
    
    print(emoji_freq)


import langdetect
from langdetect import detect
if __name__ == "__main__":

    # emoji_entries = emoji_entries_construction()
    # all_emoji_regex, emoji_dict = construct_regex(emoji_entries)

    plotFreq()
    # def tokenfunc(msg):
    #     tokens = re.findall(all_emoji_regex, msg)
    #     return tokens

    

    # msg = "真的😄"
    # print(detect(msg))
    
    # tokenfunc(msg)
    # print(tokenfunc(msg))
    # issue.write.format("csv").option("header", "true").save("/user/hangrui/conversation/issuemsg")
    # comment.write.format("csv").option("header", "true").save("/user/hangrui/conversation/commentmsg")



