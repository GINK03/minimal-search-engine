from pathlib import Path
import pickle
import json
from hashlib import sha256
import urllib.parse


def hashing(x):
    hashing = sha256(bytes(x, 'utf8')).hexdigest()[:16]
    return hashing


href_urls = {}
shref_urls = {}
for path in list(Path().glob('./tmp/inverted/*')):
    print(path)
    _href_urls = pickle.load(path.open('rb'))
    for href, urls in _href_urls.items():
        if href not in href_urls:
            href_urls[href] = set()
        href_urls[href] |= urls
        try:
            href_netloc = urllib.parse.urlparse(href).netloc
            # print(href_netloc)
        except ValueError as ex:
            print(ex)
            continue
        if href_netloc not in href_urls:
            shref_urls[href_netloc] = set()
        shref_urls[href_netloc] |= urls

href_refnum = {}
for href, urls in sorted(href_urls.items(), key=lambda x: len(x[1])*-1):
    # print(href, len(urls), urls)
    href_refnum[href] = len(urls)
json.dump(href_refnum, fp=open('tmp/href_refnum.json', 'w'),
          indent=2, ensure_ascii=False)

fp = open('tmp/to_pagerank.txt', 'w')
for href_netloc, urls in sorted(shref_urls.items(), key=lambda x: len(x[1])*-1):
    for url in urls:
        fp.write(f'{hashing(url)} {hashing(href_netloc)}\n')
