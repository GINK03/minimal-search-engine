import requests
import re
from bs4 import BeautifulSoup
from hashlib import sha256
from FFDB import FFDB
import urllib.parse
import datetime
from concurrent.futures import ProcessPoolExecutor as PPE
from concurrent.futures import ThreadPoolExecutor as TPE
import pickle
import glob
from pathlib import Path
import time
import os
from collections import namedtuple
ffdb = FFDB(tar_path='tmp/htmls')

DELAY_TIME = float(os.environ['DELAY_TIME']) if os.environ.get('DELAY_TIME') else 0.0

CPU_SIZE = int(os.environ['CPU_SIZE']) if os.environ.get('CPU_SIZE') else 32

HTML_TIME_ROW = namedtuple('HTML_TIME_ROW', ['html', 'time', 'url'])
def scrape(arg):
    key, urls = arg
    
    ret = set()
    for url in urls:
        if ffdb.exists(url) is True:
            continue
        try:
            urlp = urllib.parse.urlparse(url)
            scheme, netloc = (urlp.scheme, urlp.netloc)
            r = requests.get(url, timeout=30.0, \
                    headers = {'User-agent':'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.100 Safari/537.36'})
            r.encoding = r.apparent_encoding
            if r.status_code not in {200, 404}:
                print(r.status_code)
                raise Exception('there is error code')
            
            soup = BeautifulSoup(r.text, features='html5')
            if soup.find('html').get('lang') != 'ja':
                ffdb.save(key=url, val=None)
                continue
        
            ffdb.save(key=url, val= [HTML_TIME_ROW(html=r.text, time=datetime.datetime.now(), url=url)] )
            for a in soup.find_all('a', {'href':True}):
                urlpsub = urllib.parse.urlparse(a.get('href'))
                urlpsub = urlpsub._replace(scheme=scheme, netloc=netloc, query='')
                if ffdb.exists(urlpsub.geturl()) is True:
                    continue
                #print(urlpsub)
                #print(url, urlpsub.geturl())
                ret.add(urlpsub.geturl())
            time.sleep(DELAY_TIME) 
            print('done', url)
        except Exception as ex:
            print(ex)  
    return ret

def chunk_urls(urls):
    args = {}
    for idx, url in enumerate(urls):
        key = idx%CPU_SIZE
        if args.get(key) is None:
            args[key] = []
        args[key].append(url)
    args = [(key,urls) for key,urls in args.items()]
    return args

if __name__ == '__main__':
    urls = set()
    urls |= scrape((1, ['https://news.yahoo.co.jp/']))
    urls |= scrape((2, ['https://www.msn.com/ja-jp/news']))
    urls |= scrape((3, ['https://news.goo.ne.jp/']))
    urls |= scrape((4, ['https://www3.nhk.or.jp/news/']))
    Path('tmp/snapshots').mkdir(exist_ok=True, parents=True)
    snapshots = sorted(glob.glob('tmp/snapshots/snapshot_*'))
    for snapshot in snapshots:
        urls |= pickle.loads(open(snapshot, 'rb').read())
    while True:
        urltmp = set()
        with PPE(max_workers=32) as exe:
            for _urlret in exe.map(scrape, chunk_urls(urls)):
                if _urlret is not None:
                    urltmp |= _urlret
        urls = urltmp
        now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        print('finish one iteration.')
        with open(f'tmp/snapshots/snapshot_{now}.pkl', 'wb') as fp:
            fp.write(pickle.dumps(urls))
        if len(urls) == 0:
            break
