from pathlib import Path
import pickle
import gzip
from collections import namedtuple
import datetime
from datetime import timedelta
from bs4 import BeautifulSoup
import urllib.parse
import re
import FFDB
from concurrent.futures import ProcessPoolExecutor as PPE
from concurrent.futures import ThreadPoolExecutor as TPE

ffdb = FFDB.FFDB(tar_path='tmp/parsed')
HTML_TIME_ROW = namedtuple(
    'HTML_TIME_ROW', ['html', 'time', 'url', 'status_code'])
PARSED = namedtuple(
    'PARSED', ['url', 'time', 'title', 'description', 'body', 'hrefs'])

def pmap(arg):
    key, paths = arg
    for idx, path in enumerate(paths):
        try:
            last_fn = str(path).split('/')[-1]
            if Path(f'tmp/parsed/{last_fn}').exists():
                print('passed', idx, path)
                continue
            now = datetime.datetime.now()
            arow = pickle.loads(gzip.decompress(path.open('rb').read()))
            if arow is None:
                continue
            html = arow[-1].html
            time = arow[-1].time
            url = arow[-1].url
            urlp = urllib.parse.urlparse(url)
            scheme, netloc = (urlp.scheme, urlp.netloc)

            status_code = arow[-1].status_code
            if status_code != 200:
                print('skip', status_code)
                ffdb.save(key=url, val=None)
                continue
            if ffdb.exists(key=url) is True:
                continue
            #print(path)
            #continue
            print(path)
            soup = BeautifulSoup(html, features='html5lib')

            for script in soup(['script', 'style']):
                script.decompose()
            title = soup.title.text
            description = soup.find('head').find(
                'meta', {'name': 'description'})
            if description is None:
                description = ''
            else:
                description = description.get('content')
            body = soup.find('body').get_text()
            body = re.sub('\n', ' ', body)
            body = re.sub(r'\s{1,}', ' ', body)

            hrefs = set()
            for a in soup.find_all('a', {'href': True}):
                urlpsub = urllib.parse.urlparse(a.get('href'))
                if urlpsub.netloc == '':
                    urlpsub = urlpsub._replace(
                        scheme=scheme, netloc=netloc, query='')
                if urlpsub.scheme == '':
                    urlpsub = urlpsub._replace(
                        scheme=scheme)

                hrefs.add(urlpsub.geturl())
                #print(url, urlpsub.geturl())
                parsed = PARSED(url=url, time=time, title=title,
                                description=description, body=body, hrefs=hrefs)
                ffdb.save(key=url, val=parsed)
        except Exception as ex:
            print(ex)
            ffdb.save(key=url, val=None)
            try:
                del soup
            except:
                ...
            continue


args = {}
for idx, path in enumerate(Path().glob('./tmp/htmls/*')):
    key = idx % 100000
    if args.get(key) is None:
        args[key] = []
    args[key].append(path)
args = [(key, paths) for key, paths in args.items()]
#[pmap(arg) for arg in args]
with PPE(max_workers=16) as exe:
    exe.map(pmap, args)
