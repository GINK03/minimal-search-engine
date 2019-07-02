import pandas as pd
import MeCab
from hashlib import sha256
import json
import glob
import numpy as np
import math
pd.set_option('display.width', 10000)
pd.set_option('max_colwidth', 300)

hash_url = {}
for fn in glob.glob('tmp/hash_url/*.json'):
    hash_url.update(json.load(open(fn)))


def hashing(x):
    hashed = sha256(bytes(x, 'utf8')).hexdigest()[:16]
    return hashed

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
        if df is None:
            df = adf
        else:
            df = df.join(adf.set_index('hurl'), rsuffix='_r', on='hurl', how='inner')
            df['weight'] = df['weight'] + df['weight_r']
            df['refnum'] = df['refnum'] + df['refnum_r']
            print(df.columns)
            df = df.drop(['weight_r', 'refnum_r'], axis=1)
            #print(df)
            
    # print(df.columns)
    df['weight*page_score'] = df['weight'] * df['refnum'].apply(lambda x: math.log(10 + cutoff(x), 10))
    df = df.sort_values(by=['weight*page_score'], ascending=False)
    
    dfRes = df.head(10).copy()
    dfRes['url'] = dfRes['hurl'].apply(lambda x: hash_url.get(x))
    print(dfRes)
