
import FFDB
from collections import namedtuple
from hashlib import sha256
import json
from pathlib import Path
import pickle
import gzip
idf = json.load(open('tmp/idf.json'))
href_refnum = json.load(open('tmp/href_refnum.json'))


def hashing(x):
    return sha256(bytes(x, 'utf8')).hexdigest()[:16]


HTML_TIME_ROW = namedtuple('HTML_TIME_ROW', ['html', 'time', 'url'])
PARSED = namedtuple(
    'PARSED', ['url', 'time', 'title', 'description', 'body', 'hrefs'])
URL_TFIDF = namedtuple('URL_TFIDF', ['url', 'tfidf'])

#URL_W = namedtuple('URL_W', ['url', 'w'])
Path('tmp/inverted-index/').mkdir(exist_ok=True)

def pmap(arg):
    key,paths = arg
    
    for idx, path in enumerate(paths):
        try:
            a = pickle.loads(gzip.decompress(path.open('rb').read()))
            url = a.url
            print(idx, url)
            tfidf = a.tfidf

            urlhash = hashing(url)
            print(url, href_refnum.get(url))
            refnum = href_refnum.get(url) if href_refnum.get(url) else 0
            # 小さいデータでテスト
            #if refnum == 0:
            #    continue
            for t, w in tfidf.items():
                if idf.get(t) <= 100:
                    continue
                thashing = hashing(t)
                try:
                    with gzip.open(f'tmp/inverted-index/{thashing}', 'at') as fp:
                        fp.write(f'{urlhash}\t{w}\t{refnum}\t{url}\n')
                except Exception as ex:
                    print(ex)
                    ...
        except Exception as exe:
            print(exe)
args = {}
for idx, path in enumerate(Path().glob('tmp/tfidf/*')):
    key = idx%16
    if args.get(key) is None:
        args[key] = []
    args[key].append(path)
args = [(key,paths) for key,paths in args.items()]
from concurrent.futures import ProcessPoolExecutor as PPE
with PPE(max_workers=16) as exe:
    exe.map(pmap, args)
