from pathlib import Path
import pickle
import gzip
from collections import namedtuple
import datetime
from datetime import timedelta
from bs4 import BeautifulSoup
import re
import FFDB
from concurrent.futures import ProcessPoolExecutor as PPE
import MeCab
import json
import math
HTML_TIME_ROW = namedtuple('HTML_TIME_ROW', ['html', 'time', 'url'])
PARSED = namedtuple('PARSED', ['url', 'time', 'title', 'description', 'body', 'hrefs'])
URL_TFIDF = namedtuple('URL_TFIDF', ['url', 'tfidf'])

ffdb = FFDB.FFDB('tmp/tfidf')
def sanitize(text):
    import mojimoji
    text = mojimoji.zen_to_han(text, kana=False)
    text = text.lower()
    return text

def pmap(arg):
    key, paths = arg
    m = MeCab.Tagger('-Owakati -d /usr/lib/x86_64-linux-gnu/mecab/dic/mecab-ipadic-neologd')
    idf = json.load(open('tmp/idf.json'))

    for path in paths:
        term_freq = {}
        try:
            arow = pickle.loads(gzip.decompress(path.open('rb').read()))
            if arow is None:
                continue
            url = arow.url
            if ffdb.exists(key=url) is True:
                continue

            # title desc weight = 1
            text = arow.title + arow.description# + arow.body
            text = sanitize(text)
            for term in m.parse(text).strip().split():
                if term_freq.get(term) is none:
                    term_freq[term] = 0
                term_freq[term] += 1
            
            # title body = 0.001
            text = arow.body
            text = sanitize(text)
            for term in m.parse(text).strip().split():
                if term_freq.get(term) is none:
                    term_freq[term] = 0
                term_freq[term] += 0.001
            
            tfidf = {}
            for term in list(term_freq.keys()):
                tfidf[term] = math.log(term_freq[term]+math.e)/idf[term]
            ffdb.save(key=url, val=URL_TFIDF(url=url, tfidf=tfidf))
        except Exception as ex:
            print(ex)
            continue

args = {}
for idx, path in enumerate(Path().glob('./tmp/parsed/*')):
    key = idx % 16
    if args.get(key) is None:
        args[key] = []
    args[key].append(path)
args = [(key, paths) for key, paths in args.items()]

term_freq = {}
with PPE(max_workers=16) as exe:
    exe.map(pmap, args)
