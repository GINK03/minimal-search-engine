from pathlib import Path
import pickle
import gzip
from collections import namedtuple
import datetime
from datetime import timedelta
from bs4 import BeautifulSoup
import re
import FFDB
from concurrent.futures import ProcessPoolExecutor as PPE

ffdb = FFDB.FFDB(tar_path='tmp/parsed')
HTML_TIME_ROW = namedtuple('HTML_TIME_ROW', ['html', 'time', 'url'])
PARSED = namedtuple('PARSED', ['url', 'time', 'title', 'description', 'body'])

def pmap(arg):
    key, paths = arg
    for path in paths:
        print(path)
        try:
            now = datetime.datetime.now()
            arow = pickle.loads(gzip.decompress(path.open('rb').read()))
            html = arow[-1].html
            time = arow[-1].time
            url = arow[-1].url
            if ffdb.exists(key=url) is True:
                continue
            soup = BeautifulSoup(html, features='html5')
            if soup.find('html').get('lang') != 'ja':
                ffdb.save(key=url, val=None)

            for script in soup(['script', 'style']):
                script.decompose()
            title = soup.title.text
            description = soup.find('meta', attrs={'name': 'description'}).text if soup.find(
                'meta', attrs={'name': 'description'}) else ''
            body = soup.find('body').get_text()
            body = re.sub('\n', ' ', body)
            body = re.sub(r'\s{1,}', ' ', body)
            
            parsed = PARSED(url=url, time=time, title=title,
                            description=description, body=body)
            ffdb.save(key=url, val=parsed)
        except Exception as ex:
            print(ex)
            ffdb.save(key=url, val=None)
            continue

args = {}
for idx,path in enumerate(Path().glob('./tmp/htmls/*')):
    key = idx%16
    if args.get(key) is None:
        args[key] = []
    args[key].append(path)
args = [(key,paths) for key,paths in args.items()]
with PPE(max_workers=16) as exe:
    exe.map(pmap, args)
