
import gzip
import pickle

from pathlib import Path
from collections import namedtuple
PARSED = namedtuple('PARSED', ['url', 'time', 'title', 'description', 'body', 'hrefs'])

hrefs = set()
for path in Path().glob('../tmp/parsed/*'):
    try:
        a = pickle.loads(gzip.decompress(path.open('rb').read()))
    except:
        continue
    if a is None:
        continue
    #print(a.hrefs)
    hrefs |= set(a.hrefs)
    if len(hrefs) > 10000000:
        break

with open('../tmp/snapshots/hand.pkl', 'wb') as fp:
    fp.write( pickle.dumps(hrefs) )

    
