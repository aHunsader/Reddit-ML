#!/usr/bin/env python3

"""Clean comment text for easier parsing."""

from __future__ import print_function

import re
import string
import argparse
import sys
import bz2


__author__ = ""
__email__ = ""

# Some useful data.
_CONTRACTIONS = {
    "tis": "'tis",
    "aint": "ain't",
    "amnt": "amn't",
    "arent": "aren't",
    "cant": "can't",
    "couldve": "could've",
    "couldnt": "couldn't",
    "didnt": "didn't",
    "doesnt": "doesn't",
    "dont": "don't",
    "hadnt": "hadn't",
    "hasnt": "hasn't",
    "havent": "haven't",
    "hed": "he'd",
    "hell": "he'll",
    "hes": "he's",
    "howd": "how'd",
    "howll": "how'll",
    "hows": "how's",
    "id": "i'd",
    "ill": "i'll",
    "im": "i'm",
    "ive": "i've",
    "isnt": "isn't",
    "itd": "it'd",
    "itll": "it'll",
    "its": "it's",
    "mightnt": "mightn't",
    "mightve": "might've",
    "mustnt": "mustn't",
    "mustve": "must've",
    "neednt": "needn't",
    "oclock": "o'clock",
    "ol": "'ol",
    "oughtnt": "oughtn't",
    "shant": "shan't",
    "shed": "she'd",
    "shell": "she'll",
    "shes": "she's",
    "shouldve": "should've",
    "shouldnt": "shouldn't",
    "somebodys": "somebody's",
    "someones": "someone's",
    "somethings": "something's",
    "thatll": "that'll",
    "thats": "that's",
    "thatd": "that'd",
    "thered": "there'd",
    "therere": "there're",
    "theres": "there's",
    "theyd": "they'd",
    "theyll": "they'll",
    "theyre": "they're",
    "theyve": "they've",
    "wasnt": "wasn't",
    "wed": "we'd",
    "wedve": "wed've",
    "well": "we'll",
    "were": "we're",
    "weve": "we've",
    "werent": "weren't",
    "whatd": "what'd",
    "whatll": "what'll",
    "whatre": "what're",
    "whats": "what's",
    "whatve": "what've",
    "whens": "when's",
    "whered": "where'd",
    "wheres": "where's",
    "whereve": "where've",
    "whod": "who'd",
    "whodve": "whod've",
    "wholl": "who'll",
    "whore": "who're",
    "whos": "who's",
    "whove": "who've",
    "whyd": "why'd",
    "whyre": "why're",
    "whys": "why's",
    "wont": "won't",
    "wouldve": "would've",
    "wouldnt": "wouldn't",
    "yall": "y'all",
    "youd": "you'd",
    "youll": "you'll",
    "youre": "you're",
    "youve": "you've"
}

# You may need to write regular expressions.

def sanitize(text):
    """Do parse the text in variable "text" according to the spec, and return
    a LIST containing FOUR strings 
    1. The parsed text.
    2. The unigrams
    3. The bigrams
    4. The trigrams
    """

    # YOUR CODE GOES BELOW:

    text = text.lower()

    i = 0
    for c in text:
        if c == '\t' or c == '\n':
            text = text[:i] + ' ' + text[i+1:]
        i += 1


    pattern4 = re.compile("^(.*)\[(.*)\]\(https?\:\/\/\w+.*\.\w+.*\)(.*)")

    while pattern4.search(text):
        n = pattern4.search(text)
        text = n.group(1) + n.group(2) + n.group(3)


    tokens = text.split(' ')
    tokens = [value for value in tokens if value != ' ']

    pattern0 = re.compile("https?[:][/][/]\w+.*[.]\w+")
    pattern2 = re.compile("^([^A-Za-z0-9]*)(.*?)([^A-Za-z0-9]*)$")
    pattern5 = re.compile("^www.\w+.\w+")
    pattern6 = re.compile("^[:?!.,;\w]$")

    i = 0
    while i < len(tokens): 
        if len(tokens[i]) == 1 or len(tokens[i]) == 0:
            i += 1
            continue
        n = pattern2.search(tokens[i])
        if n:
            tokens.pop(i)
            for c in n.group(1):
                tokens.insert(i, c)
                i += 1
            tokens.insert(i, n.group(2))
            i += 1
            for c in n.group(3):
                tokens.insert(i, c)
                i += 1
            i -= 1
        i += 1

    tokens = [value for value in tokens if value != ""]
    tokens = [value for value in tokens if pattern6.search(value[0])]
    tokens = [_CONTRACTIONS[value] if value in _CONTRACTIONS else value for value in tokens]
    tokens = [value for value in tokens if not pattern0.search(value)]
    tokens = [value for value in tokens if not pattern5.search(value)]


    parsed_text = " ".join(tokens)
    unigrams = [x for x in tokens if not (x == "," or x == "." or x == ";" or x == ":" or x == "?" or x == "!")]

    clauses = []
    clause = []
    for x in tokens:
        if x == "," or x == "." or x == ";" or x == ":" or x == "?" or x == "!":
            clauses.append(clause)
            clause = []
        else:
            clause.append(x)
    if clause != []:
        clauses.append(clause)

    bigrams = []
    trigrams = []

    for c in clauses:
        for i in range(0, len(c)):
            if i < len(c)-1:
                bigrams.append("{0}_{1}".format(c[i], c[i+1]))
            if i < len(c)-2:
                trigrams.append("{0}_{1}_{2}".format(c[i], c[i+1], c[i+2]))

    unigrams = " ".join(unigrams)
    bigrams = " ".join(bigrams)
    trigrams = " ".join(trigrams)

    return [parsed_text, unigrams, bigrams, trigrams]

if __name__ == "__main__":

    if (len(sys.argv)!= 2):
        print("Usage: python cleantext.py <filename>")
        exit(1)

    filename = sys.argv[1]



    if (filename.endswith('.bz2')):
        try: 
            file = bz2.BZ2File(filename, 'r')
        except IOError:
            print("Error: unable to open bz2 file")
            exit(1)
    else:
        try:
            file = open(filename)
        except IOError:
            print("Error: unable to open file")
            exit(1)

    import json


    for line in file:
        jdict = json.loads(line)
        if "body" in jdict.keys():
            sanitized_data = str(sanitize(jdict["body"]))
            print(sanitized_data)
        jvals = jdict.values()

    file.close()

