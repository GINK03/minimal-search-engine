
from hashlib import sha256
import pickle
import gzip
from pathlib import Path
import requests
import bs4
import json
import pickle
import datetime
from concurrent.futures import ProcessPoolExecutor as PPE
import itertools
import pandas as pd
import re

class FFDB(object):
    def __init__(self, tar_path='tmp/ffdb'):
        self.tar_path = tar_path
        Path(self.tar_path).mkdir(exist_ok=True, parents=True)

    def get_hashed_fs(self, key):
        hashed = sha256(bytes(key, 'utf8')).hexdigest()[:16]
        fn = f'{self.tar_path}/{hashed}'
        return fn

    def exists(self, key):
        fn = self.get_hashed_fs(key)
        if Path(fn).exists():
            return True
        return False

    def save(self, key, val):
        fn = self.get_hashed_fs(key)
        with open(fn, 'wb') as fs:
            fs.write(gzip.compress(pickle.dumps(val)))
    
    def get(self, key):
        fn = self.get_hashed_fs(key)
        if not Path(fn).exists():
            return None
        obj = None
        with open(fn, 'rb') as fs:
            obj = pickle.loads(gzip.decompress(fs.read()))
        return obj


