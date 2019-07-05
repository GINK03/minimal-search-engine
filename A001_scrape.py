import itertools
import random
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
import urllib.request

if '/usr/bin/nkf' not in os.popen('which nkf').read():
    raise Exception('there is no nkf')
ffdb = FFDB(tar_path='tmp/htmls')
DELAY_TIME = float(os.environ['DELAY_TIME']) if os.environ.get(
    'DELAY_TIME') else 0.0

CPU_SIZE = int(os.environ['CPU_SIZE']) if os.environ.get('CPU_SIZE') else 16
#CPU_SIZE = 16

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


def content_get(url):
    r = requests.get(url, timeout=30.0,
                     headers={'User-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.100 Safari/537.36'}, stream=True)
    # r.encoding = r.apparent_encoding
    status_code = r.status_code
    if status_code not in {200, 404}:
        print(r.status_code)
        raise Exception('there is error code')
    return r.content, status_code


def content_get2(url):
    import urllib.request
    r = urllib.request.Request(url)
    r.add_header('User-Agent', 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.100 Safari/537.36')
    r.add_header('Referer', url)
    with urllib.request.urlopen(r, timeout=10) as response:
        try:
            content = response.read()  # return byte-obj
        except Exception as ex:
            #print(ex, response.status)
            content = None
            raise Exception(f'Error in response {url} {response.status}')
        #print(content, response)
        status_code = response.status
        #print(url, status_code)
    return content, status_code


instance_holder = {}


def content_get3(url, key):
    if instance_holder.get(key) is None:
        from selenium import webdriver
        from selenium.webdriver.common.keys import Keys
        from selenium.webdriver.chrome.options import Options
        from selenium.webdriver.support.ui import WebDriverWait
        from selenium.webdriver.support import expected_conditions as EC
        from selenium.webdriver.common.action_chains import ActionChains
        from selenium.webdriver.common.by import By
        options = Options()
        options.add_argument('--headless')
        options.add_argument(
            "user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/64.0.3282.186 Safari/537.36")
        driver = webdriver.Chrome(chrome_options=options,
                                  executable_path='/usr/bin/chromedriver')
        instance_holder[key] = driver
    else:
        driver = instance_holder[key]
    driver.get(url)
    html = driver.page_source
    status_code = 'selenium cannot detect status code!'
    return html, status_code


Path('/tmp/local_char_change').mkdir(exist_ok=True)


def local_char_change(x):
    hashed = sha256(x).hexdigest()[:16]
    with open(f'/tmp/local_char_change/{hashed}', 'wb') as fp:
        fp.write(x)
    html_utf8 = os.popen(f'nkf -w /tmp/local_char_change/{hashed}').read()
    Path(f'/tmp/local_char_change/{hashed}').unlink()
    return html_utf8


def thmap(arg):
    key, url = arg
    for url in [url]:
        try:
            start_time = time.time()
            url = path_paramter_sanitize(url)
            if blackList(url) is False:
                continue
            if ffdb.exists(url) is True:
                continue
            urlp = urllib.parse.urlparse(url)
            scheme, netloc = (urlp.scheme, urlp.netloc)
            content, status_code = content_get2(url)
            html = local_char_change(content)
            soup = BeautifulSoup(html, features='lxml')
            if not (soup.find('html').get('lang') == 'ja' or
                    (soup.find('meta', {'name': "content-language"}) and soup.find('meta', {'name': "content-language"}).get('content') == "ja") or
                    (soup.find('meta', {'http-equiv': "Content-Type"}) and 'jp' in soup.find('meta', {'http-equiv': "Content-Type"}).get('content')) or
                    ('.jp' in netloc)):
                ffdb.save(key=url, val=None)
                continue
            ffdb.save(key=url, val=[HTML_TIME_ROW(
                html=html, time=datetime.datetime.now(), url=url, status_code=status_code)])
            time.sleep(DELAY_TIME)
            print('done', url, soup.title.text,
                  f'elapsed={time.time() - start_time:0.04f}')
        except Exception as ex:
            print('err', url, ex)
            try:
                ffdb.save(key=url, val=ex)
            except Exception as ex:
                continue


def thscrape(arg):
    key, urls = arg
    try:
        with TPE(max_workers=4) as exe:
            exe.map(thmap, zip(itertools.cycle(range(4)), urls),
                    timeout=15, chunksize=1)
    except Exception as ex:
        print(ex)


def scrape(arg):
    key, urls = arg

    ret = set()
    for url in urls:
        try:
            start_time = time.time()
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
            content, status_code = content_get2(url)
            html = local_char_change(content)
            soup = BeautifulSoup(html, features='lxml')
            if not (soup.find('html').get('lang') == 'ja' or
                    (soup.find('meta', {'name': "content-language"}) and soup.find('meta', {'name': "content-language"}).get('content') == "ja") or
                    (soup.find('meta', {'http-equiv': "Content-Type"}) and 'jp' in soup.find('meta', {'http-equiv': "Content-Type"}).get('content')) or
                    ('.jp' in netloc)):
                ffdb.save(key=url, val=None)
                continue
            ffdb.save(key=url, val=[HTML_TIME_ROW(
                html=html, time=datetime.datetime.now(), url=url, status_code=status_code)])
            for href in set([a.get('href') for a in soup.find_all('a', {'href': True})][:10]):
                urlpsub = urllib.parse.urlparse(href)
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
                ret.add(path_paramter_sanitize(urlpsub.geturl()))
                # print(path_paramter_sanitize(urlpsub.geturl()))
            # retがメモリを消費しすぎるので100件にリンクを限定
            ret = set(list(ret)[-100:])
            time.sleep(DELAY_TIME)
            print(f'done@{key:03d}', url, soup.title.text,
                  f'elapsed={time.time() - start_time:0.04f}')
        except Exception as ex:
            print('err', url, ex)
            # save Exception as it is
            # but, some exception cannot be serialized.
            try:
                ffdb.save(key=url, val=ex)
            except Exception as ex:
                continue
    now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print('finish batch-forked iteration.')
    Path('tmp/snapshots').mkdir(exist_ok=True, parents=True)
    with open(f'tmp/snapshots/snapshot_{now}.pkl', 'wb') as fp:
        fp.write(pickle.dumps(ret))
    return ret


def chunk_urls(urls):
    args = {}
    # あまり引数が多いと、メモリに乗らない
    urls = list(urls)[:3000000]
    CHUNK = len(urls)//min(CPU_SIZE, max(len(urls),1))
    random.shuffle(urls)
    for idx, url in enumerate(urls):
        key = idx % CHUNK
        if args.get(key) is None:
            args[key] = []
        args[key].append(url)
    args = [(key, urls) for key, urls in args.items()]
    return args


def main():
    urls = set()
    urls |= scrape(
        (3, ['http://blog.livedoor.jp/geek/archives/cat_10022560.html']))
    print(urls)
    snapshots = sorted(glob.glob('tmp/snapshots/*'))
    for snapshot in snapshots:
        try:
            urls |= pickle.loads(open(snapshot, 'rb').read())
        except EOFError as ex:
            continue
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
