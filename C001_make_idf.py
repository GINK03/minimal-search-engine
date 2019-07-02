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
HTML_TIME_ROW = namedtuple('HTML_TIME_ROW', ['html', 'time', 'url'])
PARSED = namedtuple('PARSED', ['url', 'time', 'title', 'description', 'body', 'hrefs'])

def sanitize(text):
    import mojimoji
    text = mojimoji.zen_to_han(text, kana=False)
    text = text.lower()
    return text

def pmap(arg):
    key, paths = arg
    m = MeCab.Tagger('-Owakati -d /usr/lib/x86_64-linux-gnu/mecab/dic/mecab-ipadic-neologd')

    term_freq = {} # this is per doc
    for path in paths:
        #pickle.loads(gzip.decompress(path.open('rb').read()))
        try:
            arow = pickle.loads(gzip.decompress(path.open('rb').read()))
            if arow is None:
                continue
            text = arow.title + arow.description + arow.body
            text = sanitize(text)
            for term in set(m.parse(text).strip().split()):
                if term_freq.get(term) is None:
                    term_freq[term] = 0
                term_freq[term] += 1
            print(path)
        except EOFError as ex:
            path.unlink()
            continue
        except Exception as ex:
            print(ex)
            continue
    return term_freq

args = {}
for idx, path in enumerate(Path().glob('./tmp/parsed/*')):
    key = idx % 16
    if args.get(key) is None:
        args[key] = []
    args[key].append(path)
args = [(key, paths) for key, paths in args.items()]
#[pmap(args[0])]

term_freq = {}
with PPE(max_workers=16) as exe:
    for _term_freq in exe.map(pmap, args):
        for term, freq in _term_freq.items():
            if term_freq.get(term) is None:
                term_freq[term] = 0
            term_freq[term] += freq
json.dump(term_freq, open('tmp/idf.json', 'w'), indent=2, ensure_ascii=False)
