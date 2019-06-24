import requests
import re
from bs4 import BeautifulSoup
from hashlib import sha256
from FFDB import FFDB
import urllib.parse
from concurrent.futures import ProcessPoolExecutor as PPE
from concurrent.futures import ThreadPoolExecutor as TPE

def scrape(arg):
    ffdb = FFDB(tar_path='tmp/htmls')
    url = arg
    if ffdb.exists(url) is True:
        return set()
    try:
        urlp = urllib.parse.urlparse(url)
        scheme, netloc = (urlp.scheme, urlp.netloc)
        r = requests.get(url, timeout=1.0)
        r.encoding = r.apparent_encoding
        ffdb.save(key=url, val=r.text)
        soup = BeautifulSoup(r.text, features='html5')

        ret = set()
        for a in soup.find_all('a', {'href':True}):
            urlpsub = urllib.parse.urlparse(a.get('href'))
            urlpsub = urlpsub._replace(scheme=scheme, netloc=netloc)
            if ffdb.exists(urlpsub.geturl()) is True:
                continue
            print(url, urlpsub.geturl())
            ret.add(urlpsub.geturl())
        return ret
    except Exception as ex:
        ffdb.save(key=url, val={'err': str(ex)})
        return set()
        
if __name__ == '__main__':
    url = 'https://news.yahoo.co.jp/pickup/6327894'
    urls = scrape(url)
    while True:
        urltmp = set()
        with PPE(max_workers=16) as exe:
            for _urlret in exe.map(scrape, urls):
                urltmp |= _urlret
        urls = urltmp
        if len(urls) == 0:
            break
