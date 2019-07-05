import pandas as pd
import MeCab
from hashlib import sha256
import json
import glob
import numpy as np
import math
import urllib.parse
pd.set_option('display.width', None)
pd.set_option('max_colwidth', 300)

hash_url = {}
for fn in glob.glob('tmp/hash_url/*.json'):
    hash_url.update(json.load(open(fn)))


def hashing(x):
    hashed = sha256(bytes(x, 'utf8')).hexdigest()[:16]
    return hashed


pagerank = json.load(open('tmp/pagerank.json'))


def get_pagerank_weight(x):
    hashed = hashing(urllib.parse.urlparse(x).netloc)
    return pagerank[hashed] if pagerank.get(hashed) else 0


def cutoff(x):
    if x >= 15:
        return 15
    else:
        return x


if __name__ == '__main__':
    m = MeCab.Tagger(
        '-Owakati -d /usr/lib/x86_64-linux-gnu/mecab/dic/mecab-ipadic-neologd')

    text = input()
    df = None
    for term in m.parse(text).strip().split():
        hterm = hashing(term)
        print(term, hterm)
        adf = pd.read_csv(f'tmp/inverted-index/{hterm}', sep='\t')
        adf.columns = ['hurl', 'weight', 'refnum']
        adf.drop_duplicates(subset='hurl', keep=False, inplace=True)
        if df is None:
            df = adf
        else:
            df = df.join(adf.set_index('hurl'), rsuffix='_r',
                         on='hurl', how='inner')
            df['weight'] = df['weight'] + df['weight_r']
            df['refnum'] = df['refnum'] + df['refnum_r']
            df = df.drop(['weight_r', 'refnum_r'], axis=1)

    df['weight_norm'] = df['weight']/df['weight'].max() * 1
    # print(df.columns)
    df['url'] = df['hurl'].apply(lambda x: hash_url.get(x))
    # adhoc remove b.hatena
    df = df[df['url'].apply(lambda x: 'b.hatena.ne.jp' not in x)]
    df['pagerank'] = df['url'].apply(get_pagerank_weight)
    df['pagerank'] = df['pagerank']/df['pagerank'].max()
    df['weight*refnum_score+pagerank'] = df['weight_norm'] * \
        df['refnum'].apply(lambda x: math.log(
            30 + cutoff(x), 30)) * np.log(np.e + df['pagerank'])

    df = df.sort_values(by=['weight*refnum_score+pagerank'], ascending=False)
    dfRes = df.head(10).copy()
    print(dfRes)
