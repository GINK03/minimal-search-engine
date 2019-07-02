from pathlib import Path
import pickle
import json
from hashlib import sha256

def hashing(x):
    hashing = sha256(bytes(x, 'utf8')).hexdigest()[:16]
    return hashing

href_urls = {}
for path in list(Path().glob('./tmp/inverted/*')):
    print(path)
    _href_urls = pickle.load(path.open('rb'))
    for href, urls in _href_urls.items():
        if href not in href_urls:
            href_urls[href] = set()
        href_urls[href] |= urls


href_refnum = {}
for href, urls in sorted(href_urls.items(), key=lambda x: len(x[1])*-1):
    # print(href, len(urls), urls)
    href_refnum[href] = len(urls)
json.dump(href_refnum, fp=open('tmp/href_refnum.json', 'w'),
          indent=2, ensure_ascii=False)

fp = open('tmp/to_pagerank.txt', 'w')
for href, urls in sorted(href_urls.items(), key=lambda x: len(x[1])*-1):
    for url in urls:
        fp.write(f'{hashing(href)} {hashing(url)}\n')