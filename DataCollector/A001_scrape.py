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
ffdb = FFDB(tar_path='tmp/htmls')

DELAY_TIME = float(os.environ['DELAY_TIME']) if os.environ.get('DELAY_TIME') else 0.0
def scrape(arg):
    url = arg
    if ffdb.exists(url) is True:
        return set()
    try:
        urlp = urllib.parse.urlparse(url)
        scheme, netloc = (urlp.scheme, urlp.netloc)
        r = requests.get(url, timeout=60.0, \
                headers = {'User-agent':'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.100 Safari/537.36'})
        r.encoding = r.apparent_encoding
        if r.status_code not in {200, 404}:
            print(r.status_code)
            raise Exception('there is error code')
        
        ffdb.save(key=url, val=r.text)
        soup = BeautifulSoup(r.text, features='html5')

        ret = set()
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
        return ret
    except Exception as ex:
        print(ex)
        return set()
        
if __name__ == '__main__':
    urls = set()
    urls |= scrape('https://news.yahoo.co.jp/')
    urls |= scrape('https://www.msn.com/ja-jp/news')
    urls |= scrape('https://news.goo.ne.jp/')
    urls |= scrape('https://www3.nhk.or.jp/news/')
    Path('tmp/snapshots').mkdir(exist_ok=True, parents=True)
    snapshots = sorted(glob.glob('tmp/snapshots/snapshot_*'))
    for snapshot in snapshots:
        urls |= pickle.loads(open(snapshot, 'rb').read())
    while True:
        urltmp = set()
        with PPE(max_workers=32) as exe:
            for _urlret in exe.map(scrape, urls):
                if _urlret is not None:
                    urltmp |= _urlret
        urls = urltmp
        now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        with open(f'tmp/snapshots/snapshot_{now}.pkl', 'wb') as fp:
            fp.write(pickle.dumps(urls))
        if len(urls) == 0:
            break
