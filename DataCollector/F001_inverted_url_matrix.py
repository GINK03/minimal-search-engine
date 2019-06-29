
from pathlib import Path
from hashlib import sha256
import gzip
import pickle
from collections import namedtuple
from concurrent.futures import ProcessPoolExecutor as PPE
import urllib.parse 
import FFDB
HTML_TIME_ROW = namedtuple('HTML_TIME_ROW', ['html', 'time', 'url'])
PARSED = namedtuple(
    'PARSED', ['url', 'time', 'title', 'description', 'body', 'hrefs'])
URL_TFIDF = namedtuple('URL_TFIDF', ['url', 'tfidf'])
URL_HASH = namedtuple('URL_HASH', ['url', 'hash'])

Path('tmp/inverted').mkdir(exist_ok=True, parents=True)
def pmap(arg):
    key,paths = arg
    href_url = {}
    for path in paths:
        try:
            a: PARSED = pickle.loads(gzip.decompress(path.open('rb').read()))
            if a is None:
                continue
        except Exception as ex:
            if ex.args == ('Ran out of input',):
                path.unlink()
                continue
            else:
                print(ex.args)
                continue
        url = a.url
        netloc_parent = urllib.parse.urlparse(url).netloc
        for href in a.hrefs:
            netloc_href = urllib.parse.urlparse(href).netloc
            if netloc_parent == netloc_href:
                continue
            if href_url.get(href) is None:
                href_url[href] = set()
            href_url[href].add(netloc_parent)
    open(f'tmp/inverted/{key:010d}', 'wb').write( pickle.dumps(href_url) )
    print('finish', key)
    #print(href_url)
args = {}
for idx, path in enumerate(Path().glob('tmp/parsed/*')):
    key = idx%100
    if args.get(key) is None:
        args[key] = []
    args[key].append(path)
args = [(key, paths) for key,paths in args.items()]
with PPE(max_workers=16) as exe:
    exe.map(pmap, args)
