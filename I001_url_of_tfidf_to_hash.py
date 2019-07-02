
from concurrent.futures import ProcessPoolExecutor as PPE
import FFDB
from collections import namedtuple
from hashlib import sha256
import json
from pathlib import Path
import glob
import pickle
import gzip

def hashing(x):
    return sha256(bytes(x, 'utf8')).hexdigest()[:16]

HTML_TIME_ROW = namedtuple('HTML_TIME_ROW', ['html', 'time', 'url'])
PARSED = namedtuple(
    'PARSED', ['url', 'time', 'title', 'description', 'body', 'hrefs'])
URL_TFIDF = namedtuple('URL_TFIDF', ['url', 'tfidf'])

# URL_W = namedtuple('URL_W', ['url', 'w'])
Path('tmp/inverted-index/').mkdir(exist_ok=True)

Path('tmp/hash_url').mkdir(exist_ok=True, parents=True)


def pmap(arg):
    key, paths = arg
    hash_url = {}
    for idx, path in enumerate(paths):
        path = Path(path)
        try:
            a = pickle.loads(gzip.decompress(path.open('rb').read()))
            url = a.url
            print(idx, url)
            tfidf = a.tfidf

            urlhash = hashing(url)
            hash_url[urlhash] = url
            print(urlhash, url)
        except Exception as exe:
            print(exe)
    json.dump(hash_url, fp=open(f'tmp/hash_url/{key:04d}.json', 'w'), indent=2)


args = {}
for idx, path in enumerate(glob.glob('tmp/tfidf/*')):
    key = idx % 16
    if args.get(key) is None:
        args[key] = []
    args[key].append(path)
args = [(key, paths) for key, paths in args.items()]
#[pmap(args[0])]
with PPE(max_workers=16) as exe:
    exe.map(pmap, args)
