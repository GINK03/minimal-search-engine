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

if '/usr/bin/nkf' not in os.popen('which nkf').read():
    raise Exception('there is no nkf')
ffdb = FFDB(tar_path='tmp/htmls')

DELAY_TIME = float(os.environ['DELAY_TIME']) if os.environ.get(
    'DELAY_TIME') else 0.0

CPU_SIZE = int(os.environ['CPU_SIZE']) if os.environ.get('CPU_SIZE') else 32

HTML_TIME_ROW = namedtuple(
    'HTML_TIME_ROW', ['html', 'time', 'url', 'status_code'])

qos = []


def QOS(netloc):  # print('qos', qos.count(netloc))
    if len(qos) >= 10:
        if qos.count(netloc) >= 3:
            return False
        else:
            qos.pop(0)
            qos.append(netloc)
            return True
    else:
        qos.append(netloc)
        return True


def blackList(url):
    if 'twitter.com' in url:
        return False
    elif 'wikipedia' in url:
        return False
    elif 'rakuten.co.jp' in url:
        return False
    elif 'amazon.co.jp' in url:
        return False
    elif 'dmm.co.jp' in url:
        return False
    elif re.search(r'.jpg$', url):
        return False
    else:
        return True


def path_paramter_sanitize(url):
    urlp = urllib.parse.urlparse(url)
    path = urlp.path
    path = re.sub(r'/-/.*?$', '', path)
    # print(path)
    url = urlp._replace(path=path).geturl()
    return url


Path('tmp/local_char_change').mkdir(exist_ok=True)


def local_char_change(x):
    hashed = sha256(x).hexdigest()[:16]
    with open(f'tmp/local_char_change/{hashed}', 'wb') as fp:
        fp.write(x)
    html_utf8 = os.popen(f'nkf -w tmp/local_char_change/{hashed}').read()
    Path(f'tmp/local_char_change/{hashed}').unlink()
    return html_utf8


def scrape(arg):
    key, urls = arg
    ret = set()
    for url in urls:
        try:
            url = path_paramter_sanitize(url)
            if blackList(url) is False:
                continue
            if ffdb.exists(url) is True:
                continue
            urlp = urllib.parse.urlparse(url)
            scheme, netloc = (urlp.scheme, urlp.netloc)
            # if QOS(netloc=netloc) is False:
            # print('conflict QOS control', netloc)
            #    continue
            r = requests.get(url, timeout=30.0,
                             headers={'User-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.100 Safari/537.36'}, stream=True)
            # r.encoding = r.apparent_encoding
            status_code = r.status_code
            if status_code not in {200, 404}:
                print(r.status_code)
                raise Exception('there is error code')

            html = local_char_change(r.content)
            soup = BeautifulSoup(html, features='lxml')
            if not (soup.find('html').get('lang') == 'ja' or
                    (soup.find('meta', {'name': "content-language"}) and soup.find('meta', {'name': "content-language"}).get('content') == "ja") or
                    (soup.find('meta', {'http-equiv': "Content-Type"}) and 'jp' in soup.find('meta', {'http-equiv': "Content-Type"}).get('content')) or
                    ('.jp' in netloc)):
                ffdb.save(key=url, val=None)
                continue
            ffdb.save(key=url, val=[HTML_TIME_ROW(
                html=html, time=datetime.datetime.now(), url=url, status_code=status_code)])
            for a in soup.find_all('a', {'href': True}):
                urlpsub = urllib.parse.urlparse(a.get('href'))
                #print('before', urlpsub)
                try:
                    if urlpsub.netloc == '':
                        urlpsub = urlpsub._replace(
                            scheme=scheme, netloc=netloc, query='')
                    if urlpsub.scheme == '':
                        urlpsub = urlpsub._replace(scheme=scheme)
                    urlpsub = urlpsub._replace(query='')
                except Exception as ex:
                    print(ex)
                    continue
                # if ffdb.exists(urlpsub.geturl()) is True:
                #    continue
                #print('after', urlpsub)
                #print(url, urlpsub.geturl())
                ret.add(path_paramter_sanitize(urlpsub.geturl()))

            # retがメモリを消費しすぎるので10000件にリンクを限定
            ret = set(list(ret)[-1000:])
            time.sleep(DELAY_TIME)
            print('done', url, soup.title.text)
        except Exception as ex:
            print('err', url)
            ffdb.save(key=url, val=None)
            print(ex)

    now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print('finish batch-forked iteration.')
    Path('tmp/snapshots').mkdir(exist_ok=True, parents=True)
    with open(f'tmp/snapshots/snapshot_{now}.pkl', 'wb') as fp:
        fp.write(pickle.dumps(ret))

    return ret


def chunk_urls(urls):
    args = {}
    CHUNK = len(urls)//10000
    for idx, url in enumerate(urls):
        key = idx % CHUNK
        if args.get(key) is None:
            args[key] = []
        args[key].append(url)
    args = [(key, urls) for key, urls in args.items()]
    return args


def main():
    urls = set()
    # urls |= scrape((1, ['https://news.yahoo.co.jp/']))
    #urls |= scrape((2, ['https://www.msn.com/ja-jp/news']))
    urls |= scrape(
        (3, ['http://blog.livedoor.jp/geek/archives/cat_10022560.html']))
    #urls |= scrape((4, ['https://www3.nhk.or.jp/news/']))
    print(urls)
    snapshots = sorted(glob.glob('tmp/snapshots/*'))
    for snapshot in snapshots:
        urls |= pickle.loads(open(snapshot, 'rb').read())
    while True:
        urltmp = set()
        with PPE(max_workers=CPU_SIZE) as exe:
            for _urlret in exe.map(scrape, chunk_urls(urls)):
                if _urlret is not None:
                    urltmp |= _urlret
        urls = urltmp
        if len(urls) == 0:
            break


if __name__ == '__main__':
    main()
