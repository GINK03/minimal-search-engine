
from pathlib import Path
from hashlib import sha256
import gzip
import pickle
from collections import namedtuple
from concurrent.futures import ProcessPoolExecutor as PPE

import FFDB
HTML_TIME_ROW = namedtuple('HTML_TIME_ROW', ['html', 'time', 'url'])
PARSED = namedtuple('PARSED', ['url', 'time', 'title', 'description', 'body', 'hrefs'])
URL_TFIDF = namedtuple('URL_TFIDF', ['url', 'tfidf'])
URL_HASH = namedtuple('URL_HASH', ['url', 'hash'])
ffdb = FFDB.FFDB(tar_path='tmp/url_hash')
url_hash = {}
def encode_url(url):
    if ffdb.exists(url):
        return
    hash = sha256(bytes(url, 'utf8')).hexdigest()[:16]
    ffdb.save(key=url, val=URL_HASH(url=url, hash=hash))

def pmap(path):
    try:
        a:PARSED  = pickle.loads(gzip.decompress(path.open('rb').read()))
        if a is None:
            return
        print(a)
    except Exception as ex:
        print(ex)
    try:
        encode_url(a.url)
        [encode_url(url) for url in a.hrefs]
    except Exception as ex:
        print(ex)

paths = [path for path in Path().glob('tmp/parsed/*')]
with PPE(max_workers=16) as exe:
    exe.map(pmap, paths)
